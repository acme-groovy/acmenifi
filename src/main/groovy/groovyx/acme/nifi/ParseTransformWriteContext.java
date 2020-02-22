package groovyx.acme.nifi;

import groovy.lang.Closure;
import groovy.lang.Writable;
import groovy.util.Node;
import groovy.util.slurpersupport.GPathResult;
import groovy.xml.XmlUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
//import java.util.Map;
import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.Set;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.w3c.dom.Element;

/**
 * Created by dm on 16.02.2019.
 */
abstract class ParseTransformWriteContext implements Runnable{
    /**must be assigned in constructor*/
    protected Closure transform;
    private int transformArgCount;
    private FlowFile flowFile;
    private ProcessSession session;
    private Relationship REL_SUCCESS;


    //protected Object data=null;

    ParseTransformWriteContext(ProcessSession session, FlowFile flowFile, Relationship REL_SUCCESS, Closure transform){
        transformArgCount = transform.getMaximumNumberOfParameters();
        if(transformArgCount>2)throw new RuntimeException("Number of parameters for transformer must be 1 or 2");
        this.transform = transform;
        this.session = session;
        this.flowFile = flowFile;
        this.REL_SUCCESS = REL_SUCCESS;
    }

    /** must parse input stream and return parsed object*/
    abstract Object parse(InputStream in) throws Exception;
    /**called before writing data returned after transformer to convert it to a writable object*/
    AcmeWritable _writable(Object data){
		throw new RuntimeException("Only `asWriter{}` or `asStream{}` are supported as return value. got: "+data.getClass());
    }
	
	/**finalize context. override it and it will be called just before file transfer or drop.*/
	void finit(){}

    @Override
    public void run(){
        if(flowFile==null)return;

        final ControlMap attr = new ControlMap(flowFile.getAttributes());
        final Object[] data = new Object[1];

        flowFile = session.write(flowFile, new StreamCallback() {
            @Override
            public void process(InputStream sin, OutputStream sout) throws IOException {
                //read & parse
                //Object data = null;
                if(flowFile.getSize()>0){
                    //we don't call `parse` for an empty content. and data remains null.
                    try {
                        data[0] = parse(sin);
                    }catch(Throwable t){
                        throw new IOException(t.getMessage(), t);
                    }
                }
                //transform
                if( transformArgCount==1 ){
                    data[0] = transform.call(data[0]);
                }else{
                    data[0] = transform.call(data[0],attr);
                }
                //write
                if(data[0]!=null){
                    //got some data to write
                    AcmeWritable writable = data[0] instanceof AcmeWritable ? (AcmeWritable)data[0] : _writable(data[0]);
                    writable.writeTo( sout );
                }

            }
        });

        //update attributes.
        Set<String> removed = attr.getRemovedKeys();
        if(removed.size()>0)flowFile = session.removeAllAttributes(flowFile,removed);
        for(String key: attr.getModifiedKeys()){
            Object value = attr.get(key);
            if(value!=null)flowFile = session.putAttribute(flowFile, key, value.toString());
        }
        //finalize
        finit();
        // drop or transfer
        if(data[0]==null){
            //if no data to write - just drop the file
            session.remove(flowFile);
        }else{
            session.transfer(flowFile,REL_SUCCESS);
        }

    }
}
