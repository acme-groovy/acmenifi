package groovyx.acme.nifi;

import java.io.*;
//import java.util.Map;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import groovy.lang.Closure;
import groovy.lang.Writable;
import groovy.text.Template;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.StreamCallback;

/**
 * flow file worker base class used by all workers. containt the most re-usable transforming logic.
 */
public class ParseTransformWriteContext implements Runnable, StreamCallback{
    private Object transformerDelegate = null;
    /*must be assigned in constructor*/
    protected Closure transform;
    protected ProcessSession session;
    protected FlowFile flowFile;
    protected Relationship REL_SUCCESS;

    //private object to store intermediate data between processing stages
    //should be accessed only from run() method
    private Object flowData = null;
    private ControlMap flowAttr = null;

    /**
     * default constructor the method init() must be called to initialize the instance
     */
    public ParseTransformWriteContext(){}
    public final void init(ProcessSession session, FlowFile flowFile, Relationship REL_SUCCESS, Closure transform){
        if(session==null)throw  new IllegalArgumentException("The session could not be null");
        if(this.session!=null)throw  new IllegalArgumentException("The session already has been initialized");
        this.session = session;
        this.flowFile = flowFile;
        this.REL_SUCCESS = REL_SUCCESS;
        this.transform = transform;
    }

    protected ParseTransformWriteContext(ProcessSession session, FlowFile flowFile, Relationship REL_SUCCESS, Closure transform){
        this.init(session, flowFile, REL_SUCCESS, transform);
    }

    /** takes input stream and deserializes it if necessary.
     * by default transfers to the next step (transform) the stream itself without parsing.
     * for example at this step we could parse the input stream to json object.
     * @param in flowfile input stream
     * @return modified/parsed input stream. by default returns inputstream itself.
     * @throws Exception to mitimize try-catch in implementation
     */
    protected Object parse(InputStream in) throws Exception{
        return in;
    }

    /** takes data just after parsing, transforms it if needed, and returns a new representation of data to be used on the next stage (write).
     * by default calls `transform` closure if it not null. attributes could be changed during this call.
     * @param data current flowfile content. by default flowfile input stream.
     * @param attr modifiable flowfile attributes map
     * @return transformed flowfile content
     * @throws Exception to minimize try-catch inside this methods. all exceptions handled with `process` method.
     */
    protected Object transform(Object data, ControlMap attr) throws Exception{
        if(transform!=null) {
            if (transform.getMaximumNumberOfParameters() == 1) {
                data = delegated(transform).call(data);
            } else {
                data = delegated(transform).call(data,attr);
            }
        }
        return data;
    }

    /*calls asWritable(data) and then writes data to the output*/
    protected void write(Object data, OutputStream out) throws Exception{
        if(data instanceof StreamWritable) ((StreamWritable)data).streamTo(out);
        else if(data instanceof InputStream) org.codehaus.groovy.runtime.IOGroovyMethods.leftShift(out, (InputStream)data);
        else if(data instanceof Writable) {
            try(Writer w=IOUtils.toWriter(out,"UTF-8")){
                ((Writable)data).writeTo(w);
                w.flush();
            }
        }else if(data instanceof CharSequence){
            try(Writer w=IOUtils.toWriter(out,"UTF-8")){
                w.append((CharSequence)data);
                w.flush();
            }
        } else throw new IllegalArgumentException("Unsupported returned value type to write: "+data.getClass());
    }

    /**
     * main method used for transforming input stream to output stream
     * @param sin flow file input stream
     * @param sout flow file output stream
     * @param attr attributes map
     * @return true if we should transfer the file, false to drop
     * @throws Exception to minimize try-catch inside this methods. all exceptions handled with `process` method.
     */
    protected boolean processContent(InputStream sin, OutputStream sout, ControlMap attr) throws Exception{
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

    /**
     * method used for external transformers implementation calls.
     * @param args arguments provided by script for this transformation.
     */
    protected void invoke(Object[]args) {
        throw new RuntimeException("Not implemented");
    }

    /**
     * method that should return the delegate object used to initialize transform closure
     * @return Object that will be used as a delegate for current transform
     */
    protected Object createTransformerDelegate(){
        return new TransformerDelegate(this);
    }

    /**
     * sets transformerDelegate for the closure and returns closure
     * @param c closure to be modified
     * @return closure with new delegate object
     */
    final protected Closure delegated(Closure c){
        if(transformerDelegate==null)transformerDelegate = createTransformerDelegate();
        c.setDelegate( transformerDelegate );
        return c;
    }

}
