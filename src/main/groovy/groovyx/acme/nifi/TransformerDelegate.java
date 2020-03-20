package groovyx.acme.nifi;

import groovy.lang.Closure;
import groovy.lang.GroovyObjectSupport;
import groovy.text.Template;
import org.apache.nifi.components.PropertyValue;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Collections;
import java.util.Map;

/**
 * class used as a delegate object for transform closures.
 * below you can find the methods you could use in default flowFile workers (FlowFileWorkers).
 *
 * Example:
 * <code>
 *     withFlowFile(this).withReader{r-&gt;     //worker start
 *         asWriter{w-&gt;                      //one of the transformer context methods that returns writable
 *             w &lt;&lt; r                           //transfer reader to writer without transforming
 *         }
 *     }
 * </code>
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
     * <code>return asWriter("UTF-8"){out-&gt; out.write(stringContent)}</code>
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
     * <code>return asWriter{out-&gt; out.write(stringContent)}</code>
     * @param c closure with one parameter (Writable) to write flowfile with default encoding `UTF-8`
     * @return object ready to write flowfile
     * */
    @SuppressWarnings("unchecked")
    public StreamWritable asWriter(Closure c){
        return asWriter(Collections.EMPTY_MAP,c);
    }

    /** helper to return alternate serializer of the parsed flowfile object.
     * <code>return asStream{out-&gt; out.write(bytesContent)}</code>
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



    /** helper to return alternate serializer based on GSP-like template.
     * <code>return asTemplate([var_json:json], 'value from json: &lt;%= var_json.key1.key2 %&gt;' )</code>
     * @param args parameters to be used in template including `encoding` used to write output
     * @param template the template body. could be a processor property name that holds the template.
     * @return object ready to write flow file
     * */
    public StreamWritable asTemplate(final Map<String,Object> args, final String template){
        String encoding = (String)args.getOrDefault("encoding", "UTF-8");
        return new StreamWritable(encoding){
            @Override
            protected Writer writeTo(Writer out) throws IOException {
                Template t = Templates.get(template);
                t.make(args).writeTo(out);
                return out;
            }
        };
    }

    public StreamWritable asTemplate(final Map<String,Object> args, final PropertyValue template) {
        return  asTemplate(args, template.getValue());
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

}
