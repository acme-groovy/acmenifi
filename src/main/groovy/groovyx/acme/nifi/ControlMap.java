package groovyx.acme.nifi;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * class to hold modifiable flowfile attributes. monitors all changes done to map and later could return modified and removed keys. used almost in all workers.
 */
public class ControlMap extends HashMap<String,Object> {
    private Set<String> removedKeys=new HashSet<>(); //items removed from map
    private Set<String> modifiedKeys=new HashSet<>(); //items removed from map

    ControlMap(Map<String,String> base){
        super(base);
    }

    Set<String> getRemovedKeys(){
        return removedKeys;
    }
    Set<String> getModifiedKeys(){
        return modifiedKeys;
    }

    @Override
    public Object put(String key, Object value) {
        if(value==null){
            return remove(key);
        }
        modifiedKeys.add(key);
        return super.put(key,value);
    }

    @Override
    public Object remove(Object key) {
        if(key instanceof String){
            removedKeys.add((String)key);
            modifiedKeys.remove(key);
        }
        return super.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ? extends Object> o) {
        for( Map.Entry<? extends String, ? extends Object> e : o.entrySet() ){
            this.put(e.getKey(), e.getValue());
        }
    }

    @Override
    public void clear() {
        removedKeys.addAll( super.keySet() );
        modifiedKeys.clear();
        super.clear();
    }

}
