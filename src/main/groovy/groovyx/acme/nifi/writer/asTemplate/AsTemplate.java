package groovyx.acme.nifi.writer.asTemplate;

import groovy.text.Template;
import groovyx.acme.nifi.StreamWritable;
import groovyx.acme.text.AcmeTemplateEngine;
import org.apache.nifi.components.PropertyValue;
import org.codehaus.groovy.runtime.InvokerHelper;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * helper to create alternate serializer based on GSP-like template.
 * <pre>{@code return asTemplate(binding: [var_json:json], template:'value from json: <%= var_json.key1.key2 %>' )}</pre>
 * {@code asTemplate(...)} options:
 * <table summary="">
 * <tr class="rowColor"><td>encoding</td><td>encoding to use to write flow-file out stream (default=UTF-8)</td></tr>
 * <tr class="rowColor"><td>template</td><td>mandatory parameter that defines a template. could be a string or the parameter of current processor, or any standard groovy templates</td></tr>
 * <tr class="rowColor"><td>mode</td><td>template processing mode: <ul>
 *     <li> {@code '%'} - JSP like template that supports {@code <% ... %>} code injection and {@code <%= ... %>} value injection </li>
 *     <li> {@code '$'} - GroovyString like template  that supports {@code ${...}} value injections </li>
 *     <li> {@code '&'} - mode that supports both modes above </li>
 *     </ul></td></tr>
 * </table>
 * */

public class AsTemplate extends StreamWritable {
    //private String encoding;
    private Map binding;
    private Template template;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Object[] args) {
        if(args.length==1){
            if(args[0] instanceof Map){
                init((Map<Object,Object>)args[0]);
                return;
            }
        }
        throw new IllegalArgumentException( "Unsupported arguments for `asTemplate` method: " + InvokerHelper.toTypeString(args)+". Expected (Map)." );
    }

    /**
     * initialize StreamWritable as a template. valid options: template, encoding, binding, mode.
     * @param opts
     */
    private void init(Map<Object,Object> opts){
        this.encoding   = (String)opts.getOrDefault("encoding", "UTF-8");
        this.binding    = (Map) opts.get("binding");
        Object template = opts.get("template");

        if(template instanceof Template){
            this.template = (Template)template;
        }else{
            String mode = (String)opts.getOrDefault("mode", "%");  //%, $, &
            if(template instanceof String){
            }else if(template instanceof CharSequence) {
                template = template.toString();
            }if(template instanceof PropertyValue){
                template = ((PropertyValue)template).getValue() ;
            }else throw new IllegalArgumentException("Unsupported template type: "+(template==null?"null":template.getClass())+". Expected: String, NiFi-Property");
            this.template = get(mode, (String)template);
        }
    }

    @Override
    protected Writer writeTo(Writer out) throws IOException {
        template.make(this.binding).writeTo(out);
        return out;
    }

    //assume that the same template normally processed with the same mode as previously
    private static WeakHashMap<String, Template> cache = new WeakHashMap<>();

    private static Template get(String mode, String template){
        Template t = cache.get(template);
        if(t==null){
            try {
                t = new AcmeTemplateEngine().setMode(mode).createTemplate(template);
            } catch (Exception e) {
                throw new RuntimeException(e.toString(),e);
            }
            cache.put(template, t);
        }
        return t;
    }

}
