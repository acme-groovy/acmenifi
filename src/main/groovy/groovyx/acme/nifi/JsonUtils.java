package groovyx.acme.nifi;

import groovy.json.JsonOutput;
import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.Map;

/**
 * Json utilities
 */
public class JsonUtils {

    private static void newLine(Writer w, int indent) throws IOException {
        if(indent!=-1) {
            w.append('\n');
            for (int i = 0; i < indent; i++)
                w.append(' ');
        }
    }

    /**
     * Writes map, array, or simple value to output writer as json.
     * TODO: validate performance and try to reimplement without recurse.
     * @param o map, array or simple value
     * @param w writer where to write output
     * @param indent should we indent (pretty print) output
     * @throws IOException if io error occurred
     */
    @SuppressWarnings("unchecked")
    public static void writeJson(Object o, Writer w, int indent) throws IOException {
        int cnt=0;
        if(o instanceof Collection){
            Collection<Object> a = (Collection)o;
            w.append('[');
            if(indent!=-1)indent+=2;
            for(Object e : a){
                if(cnt>0)w.append(',');
                newLine(w,indent);
                writeJson( e, w, indent );
                cnt++;
            }
            if(indent!=-1)indent-=2;
            if(cnt>0)newLine(w,indent);
            w.append(']');


        }else if(o instanceof Map){
            Map<Object,Object> m=(Map)o;
            w.append('{');
            if(indent!=-1)indent+=2;
            for(Map.Entry e : m.entrySet()){
                if(cnt>0)w.append(',');
                newLine(w,indent);
                Object key = e.getKey();
                if(key==null)throw new RuntimeException("null as map key not supported");
                w.append( JsonOutput.toJson(key.toString()) );
                w.append( ':' );
                writeJson( e.getValue(), w, indent );
                cnt++;
            }
            if(indent!=-1)indent-=2;
            if(cnt>0)newLine(w,indent);
            w.append('}');
        }else{
            w.append( JsonOutput.toJson(o) );
        }
    }

}
