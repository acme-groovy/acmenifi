package groovyx.acme.nifi;

import java.io.*;
//import java.util.Map;
import java.util.Set;

import groovy.lang.Closure;
import groovy.lang.Writable;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.StreamCallback;

/**
 * Created by dm on 16.02.2019.
 */
class ParseTransformWriteContext implements Runnable, StreamCallback{
    /*must be assigned in constructor*/
    protected Closure transform;
    protected FlowFile flowFile;
    protected ProcessSession session;
    protected Relationship REL_SUCCESS;

    //private object to store intermediate data between processing stages
    //should be accessed only from run() method
    private Object flowData = null;
    private ControlMap flowAttr = null;

    //protected Object data=null;

    ParseTransformWriteContext(ProcessSession session, FlowFile flowFile, Relationship REL_SUCCESS, Closure transform){
        this.session = session;
        this.flowFile = flowFile;
        this.REL_SUCCESS = REL_SUCCESS;
        this.transform = transform;
    }

    ParseTransformWriteContext(ProcessSession session, FlowFile flowFile, Relationship REL_SUCCESS){
        this.session = session;
        this.flowFile = flowFile;
        this.REL_SUCCESS = REL_SUCCESS;
        this.transform = null;
    }

    /** takes input stream and deserializes it if necessary.
     * by default transfers to the next step (transform) the stream itself without parsing.
     * for example at this step we could parse the input stream to json object
     */
    Object parse(InputStream in) throws Exception{
        return in;
    }

    /** takes data just after parsing, transforms it if needed, and returns a new representation of data to be used on the next stage (write).
     * by default calls `transform` closure if it not null. attributes could be changed during this call.
     */
    Object transform(Object data, ControlMap attr) throws Exception{
        if(transform!=null) {
            if (transform.getMaximumNumberOfParameters() == 1) {
                data = transform.call(data);
            } else {
                data = transform.call(data,attr);
            }
        }
        return data;
    }

    /*calls asWritable(data) and then writes data to the output*/
    void write(Object data, OutputStream out) throws Exception{
        if(data instanceof InputStream) org.codehaus.groovy.runtime.IOGroovyMethods.leftShift(out, (InputStream)data);
        else ((StreamWritable)data).streamTo(out);
    }

    /**
     * main method used for transforming input stream to output stream
     * @param sin flow file input stream
     * @param sout flow file output stream
     * @param attr attributes map
     * @return true if we should transfer the file, false to drop
     */
    boolean processContent(InputStream sin, OutputStream sout, ControlMap attr) throws Exception{
        //read & parse
        if (flowFile.getSize() > 0) {
            //we don't call `parse` for an empty content. and flowData remains null.
            flowData = parse(sin);
        }
        //transform
        flowData = transform(flowData, attr);
        //write
        if (flowData != null) {
            //got some data to write
            write(flowData, sout);
            return true;
        }
        return false;
    }

    /**
     * final internal method used by flowFile.write
     */
    @Override
    public final void process(InputStream sin, OutputStream sout) throws IOException {
        try {
            flowData = processContent(sin,sout,flowAttr);
            sout.flush();
            sout.close();
            sin.close();
        } catch (Throwable t) {
            if(t instanceof IOException)throw (IOException)t;
            if(t instanceof RuntimeException)throw (RuntimeException)t;
            throw new IOException(t.toString(),t);
        }
    }

    /**
     * applies changes on attributes captured by `attr` parameter. happened after process content and streams closed.
     * @param attr
     */
    void updateAttributes(ControlMap attr){
        Set<String> removed = attr.getRemovedKeys();
        if(removed.size()>0)flowFile = session.removeAllAttributes(flowFile,removed);
        for(String key: attr.getModifiedKeys()){
            Object value = attr.get(key);
            if(value!=null)flowFile = session.putAttribute(flowFile, key, value.toString());
        }
    }

	/**finalize context. called just before file transfer or drop.*/
	void finit(){}

    @Override
    public void run(){
        if(flowFile==null)return;

        flowAttr = new ControlMap(flowFile.getAttributes());
        flowData = null;

        flowFile = session.write(flowFile, this);
        updateAttributes(flowAttr);
        finit();
        // drop or transfer
        if( Boolean.FALSE.equals(flowData) || flowData==null ){
            //if there was no data to write - just drop the file
            session.remove(flowFile);
        }else{
            session.transfer(flowFile,REL_SUCCESS);
        }

    }

    static Reader toReader(InputStream in, String encoding) throws UnsupportedEncodingException {
        return new BufferedReader(new InputStreamReader(in, encoding));
    }
    static Writer toWriter(OutputStream out, String encoding) throws UnsupportedEncodingException {
        return new BufferedWriter(new OutputStreamWriter(out, encoding));
    }
}
