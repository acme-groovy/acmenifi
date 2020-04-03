package groovyx.acme.nifi;

import groovy.lang.Closure;
import groovy.lang.GroovyObjectSupport;
import groovy.lang.MissingMethodException;
import groovy.text.Template;
import org.apache.nifi.components.PropertyValue;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * class used as a delegate object for transform closures.
 * below you can find the methods you could use in default flowFile workers (FlowFileWorkers).
 *
 * Example:
 *
 * <pre>{@code
 *     withFlowFile(this).withReader{r->     //worker start
 *         //at this moment the `delegate` is an instance of class below (unless redefined by ParseTransformWriteContext descendant)
 *         asWriter{w->                      //one of the transformer context methods that returns writable
 *             w >> r                        //transfer reader to writer without transforming
 *         }
 *     }
 * }</pre>
 */
public class TransformerDelegate extends GroovyObjectSupport {
    protected final ParseTransformWriteContext transformer$context;
    public TransformerDelegate(ParseTransformWriteContext context){
        this.transformer$context = context;
    }

    /** prevent access to hidden properties from groovy. don't use this method. */
    @Override
    public Object getProperty(String property) {
        if("transformer$context".equals(property))throw new IllegalArgumentException("restricted access to " + property);
        return getMetaClass().getProperty(this, property);
    }

    /** helper to return alternate serializer of the parsed flowfile object that requires writer.
     * <pre>{@code return asWriter("UTF-8"){out-> out.write(stringContent)}}</pre>
     * @param args parameters of closure: `encoding` the encoding to use to write the flow file
     * @param c closure with one parameter (Writable) to write flowfile
     * @return object ready to write flowfile
     * */
    public StreamWritable asWriter(Map<String,Object> args, final Closure c){
        return new StreamWritable(args){
            @Override
            protected Writer writeTo(Writer out)throws IOException {
                c.call(out);
                return out;
            }
        };
    }
    /** helper to return alternate serializer of the parsed flowfile object that requires writer.
     * <pre>{@code return asWriter{out-> out.write(stringContent)}}</pre>
     * @param c closure with one parameter (Writable) to write flowfile with default encoding `UTF-8`
     * @return object ready to write flowfile
     * */
    @SuppressWarnings("unchecked")
    public StreamWritable asWriter(Closure c){
        return asWriter(Collections.EMPTY_MAP,c);
    }

    /** helper to return alternate serializer of the parsed flowfile object.
     * <pre>{@code return asStream{out-> out.write(bytesContent)}}</pre>
     * @param c closure that receives one parameter `OutputStream` that could be used to write flow file
     * @return object ready to write to flow file
     **/
    public StreamWritable asStream(final Closure c){
        return new StreamWritable("stream"){
            @Override
            public OutputStream streamTo(OutputStream out)throws IOException{
                c.call(out);
                return out;
            }
        };
    }

    @SuppressWarnings("unchecked")
    public FlowFileWorker createFlowFile() {
        return createFlowFile(Collections.EMPTY_MAP);
    }
    /**
     * creates flowFile from current flow file with cloning only attributes or if `parms.copyContent==true` with cloning attributes and content.
     * @param parms `cloneContent` if true clones attributes and content of current flow file; otherwise clones only attributes (default=false)
     * @return new new flow file worker
     */
    public FlowFileWorker createFlowFile(final Map<String,Object> parms){
        Boolean content    = (Boolean)parms.getOrDefault("copyContent", Boolean.FALSE);
        return new FlowFileWorker(
                content?transformer$context.session.clone(transformer$context.flowFile):transformer$context.session.create(transformer$context.flowFile),
                transformer$context.session, transformer$context.REL_SUCCESS);
    }

    private Map<String, Class<StreamWritable>> methodsCache = new HashMap<>();
    /**
     * method to support external `asXXX` commands implementation. normally called by groovy.
     * searches for <code>groovyx.acme.nifi.writer.`name`.`Name`</code> class that implements StreamWritable
     * @param name method name
     * @param arg arguments provided by caller
     * @return initialized class instance that implements StreamWritable ready to write output in specific format
     */
    @SuppressWarnings("unchecked")
    public Object methodMissing(String name, Object arg){
        Object[] args = null;

        if(name==null || name.length()<1) throw new RuntimeException("Unsupported method name: `"+name+"`");
        if( arg instanceof Object[] )args = (Object[])arg;
        else throw new RuntimeException("Unsupported argument list: "+arg+" for `"+name+"`");

        Class<StreamWritable> methodClass = methodsCache.get(name);
        if(methodClass==null) {
            String className = "groovyx.acme.nifi.writer." + name + "." + Character.toUpperCase(name.charAt(0)) + name.substring(1);
            try {
                methodClass = (Class<StreamWritable>) this.getClass().getClassLoader().loadClass(className);
            } catch (Throwable e) {
                MissingMethodException me = new MissingMethodException( name, this.getClass(), args );
                throw new RuntimeException(me.getMessage(),e);
            }
            methodsCache.put(name,methodClass);
        }

        StreamWritable writable = null;
        try {
            writable = methodClass.newInstance();
        } catch (Throwable e) {
            throw new RuntimeException("Failed to instantiate "+methodClass,e);
        }
        writable.init(args);
        return writable;
    }

}
