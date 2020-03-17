package groovyx.acme.nifi.worker.withJsonReader;

import groovy.lang.Closure;
import groovy.lang.MissingMethodException;
import groovyx.acme.json.*;
import groovyx.acme.nifi.*;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.codehaus.groovy.runtime.InvokerHelper;


import java.io.*;
import java.util.Collections;
import java.util.Map;


public class WithJsonReader extends ParseTransformWriteContext {
    private String encoding;
    private AcmeJsonParser parser;
    private AcmeJsonFilterHandler jsonFilter;
    private Closure parserConfig;
    private Writer contentWriter = null;
    private Closure eventOnEOF = null;

    @Override
    @SuppressWarnings("unchecked")
    protected void invoke(Object[] args) {
        if(args.length==1){
            if(args[0] instanceof Closure){
                invoke(Collections.EMPTY_MAP ,(Closure)args[0]);
                return;
            }
        }else if(args.length==2){
            if(args[0] instanceof Map && args[1] instanceof Closure){
                invoke((Map)args[0] ,(Closure)args[1]);
                return;
            }
        }
        throw new IllegalArgumentException( "Unsupported arguments for `withJsonReader` method: " + InvokerHelper.toTypeString(args)+". Expected (Map,Closure) or (Closure)." );
    }

    private void invoke(Map<Object,Object> args, Closure c) {
        this.encoding = (String)args.getOrDefault("encoding","UTF-8");
        this.parserConfig = c;
        //initialize parser
        this.parser = new AcmeJsonParser();
        this.jsonFilter = new AcmeJsonFilterHandler();
        if( ((Boolean)args.getOrDefault("relax",Boolean.FALSE)).booleanValue()==true )this.parser.setLenient(true);
        parser.setHandler( this.jsonFilter );
        this.run();
    }

    /**
     * configure json reader, parse content, write output if defined
     * @param sin flow file input stream
     * @param sout flow file output stream
     * @param attr attributes map
     * @return true if write json called
     * @throws IOException
     */
    @Override
    protected boolean processContent(InputStream sin, OutputStream sout, ControlMap attr) throws IOException {
        //configure

        Object ret = null;
        try(Reader r = IOUtils.toReader(sin,encoding)){
            try(Writer w = IOUtils.toWriter(sout,encoding)){
                this.contentWriter = w;

                parserConfig.setDelegate( new TransformerDelegateLocal() );
                ret = parserConfig.call(attr);

                if(ret==null){
                    jsonFilter.setDelegate( new AcmeJsonNullHandler() );
                }else if(ret instanceof AcmeJsonHandler){
                    jsonFilter.setDelegate( (AcmeJsonHandler) ret);
                }else{
                    throw new IllegalStateException("the return value for JsonReader must be null (to drop file) or `asJsonWriter(indent:true)`");
                }
                parser.parse( r ); //main call to read/write json
                w.flush();
            }
            this.contentWriter = null;
        }
        if(eventOnEOF!=null){
            eventOnEOF.call();
        }
        return ret!=null; //transfer
    }

    class TransformerDelegateLocal {
        /**
         * register event listener for the json reader
         * @param jPath simple json path. supported tokens:
         *              `$` - root.
         *              `.key` or `["key"]` - obj access by key.
         *              `[NUM]` - exact array item access.
         *              `[*]` or `.*` - any item in object or array.
         *              `..` - any nest level of any item - if it's the last token then only simple values triggered.
         * @param jProc closure that will be triggered when `jPath` matched with two params:
         *              `value` - the value from json that corresponds to `jPath`.
         *              `path` - the real json path of current json value
         */
        public void onValue(String jPath, Closure jProc){
            //register processor for json event delegated to current default context
            jsonFilter.addValueFilter(jPath, delegated(jProc) );
        }

        /**
         * defines event listener thar will be triggered on end-of-file - after all json events.
         * @param c closure without parameters.
         */
        public void onEOF(Closure c){
            eventOnEOF = delegated(c);
        }

        /**
         * creates json write handler for json reader. this could be one of returned methods in `JsonReader{ }` closure
         * @param args `indent` pretty print the output json (default=false)
         */
        public AcmeJsonHandler asJsonWriter(Map<String,Object>args){
            boolean indent = (Boolean)args.getOrDefault("indent",Boolean.FALSE);
            if(contentWriter==null)throw new IllegalStateException("json writer not yet defined");
            return new AcmeJsonWriteHandler(contentWriter,indent);
        }

        /**
         * closure `c` must create AcmeJsonHandler that could process json events in own way and write it to out writer (the first closure parameter)
         */
        public AcmeJsonHandler asJsonHandler(Closure c){
            if(contentWriter==null)throw new IllegalStateException("json writer not yet defined");
            return (AcmeJsonHandler)c.call(contentWriter);
        }
    }

}
