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
    abstract AcmeWritable writable(Object data);

    @Override
    public void run(){
        if(flowFile==null)return;

        ControlMap attr = null;
        Object data = null;

        try(InputStream sin = session.read(flowFile)) {
            data = parse(sin);
        }catch(Exception e){
            throw new RuntimeException(e.toString(),e);
        }

        if( this.transformArgCount==1 ){
            data = transform.call(data);
        }else{
            attr = new ControlMap(flowFile.getAttributes());
            data = transform.call(data,attr);
        }

        if(data==null){
            session.remove(flowFile);
        }else{
            AcmeWritable writable = null;
            if(data instanceof AcmeWritable)writable=(AcmeWritable)data;
            else writable = writable(data);

            flowFile = session.write(flowFile,writable); //calls this.process(OutputStream out){} method
            if(attr!=null){
                //update attributes.
                Set<String> removed = attr.getRemovedKeys();
                if(removed.size()>0)flowFile = session.removeAllAttributes(flowFile,removed);
                for(String key: attr.getModifiedKeys()){
                    Object value = attr.get(key);
                    if(value!=null)flowFile = session.putAttribute(flowFile, key, value.toString());
                }
            }
            session.transfer(flowFile,REL_SUCCESS);
        }

    }
}
