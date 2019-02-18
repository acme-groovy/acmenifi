package groovyx.acme.nifi;

import groovy.text.Template;
import groovyx.acme.text.AcmeTemplateEngine;

import java.io.IOException;
import java.util.WeakHashMap;

public class Templates {
    private static WeakHashMap<String, Template> cache = new WeakHashMap<>();

    public static Template get(String template) throws IOException {
        Template t = cache.get(template);
        if(t==null){
            try {
                t = new AcmeTemplateEngine().setMode(AcmeTemplateEngine.MODE_JSP).createTemplate(template);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e.toString(),e);
            }
            cache.put(template, t);
        }
        return t;
    }
}
