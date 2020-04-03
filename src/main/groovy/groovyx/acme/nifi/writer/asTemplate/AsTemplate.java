package groovyx.acme.nifi.writer.asTemplate;

import groovy.lang.Closure;
import groovy.text.Template;
import groovy.text.TemplateEngine;
import groovyx.acme.nifi.StreamWritable;
import groovyx.acme.nifi.Templates;
import groovyx.acme.text.AcmeTemplateEngine;
import org.apache.nifi.components.PropertyValue;
import org.codehaus.groovy.runtime.InvokerHelper;

import java.io.IOException;
import java.io.Writer;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

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
