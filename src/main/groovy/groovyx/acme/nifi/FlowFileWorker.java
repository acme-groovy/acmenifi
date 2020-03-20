package groovyx.acme.nifi;

import groovy.json.JsonParserType;
import groovy.json.JsonSlurper;
import groovy.lang.Closure;
import groovy.lang.MissingMethodException;
import groovy.lang.Script;
import groovy.util.Node;
import groovy.util.XmlParser;
import groovy.util.slurpersupport.GPathResult;
import groovy.xml.XmlUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;


/**
 * class that provides methods to process a flow file. could be created using `AcmeNiFi.withFlowFile` or `AcmeNiFi.newFlowFile`.
 * <pre>{@code
 *     withFlowFile(this).withJson(encoding: "UTF-8"){json->   // worker that process flowfile content as json
 *         return json.findAll{k,v-> k.startsWith("a")}        // produces json from source with keys that starts with 'a'
 *     }
 * }</pre>
 */
public class FlowFileWorker {
    private FlowFile flowFile;
    private ProcessSession session;
    private Relationship REL_SUCCESS;

    FlowFileWorker(Script script, boolean isNew){
        this.session     = (ProcessSession) script.getBinding().getProperty("session");
        this.REL_SUCCESS = (Relationship)script.getBinding().getProperty("REL_SUCCESS");
        if(isNew){
            this.flowFile    = session.create();
        }else{
            this.flowFile    = session.get();
        }
    }

    FlowFileWorker( FlowFile flowFile, ProcessSession session, Relationship REL_SUCCESS){
        this.flowFile    = flowFile;
        this.session     = session;
        this.REL_SUCCESS = REL_SUCCESS;
    }

    @SuppressWarnings("unchecked")
    public void withJson(Closure transform) {
        withJson(Collections.EMPTY_MAP, transform);
    }

    /**
     * treats input data as json - the result of JsonSlurper.parse(inStream) and passes the parsed json into `transform` closure.
     * closure must accept one (json) or two (json,attrs) parameters, process/transform it, and return json groovy object.
     * The result of closure will be written to output flow file as json unless the return value is rusult of `asWriter{}` or `asStream{}`.
     * @param parms `encoding` - encoding user to parse/write json (default=UTF-8).
     *              `indent` -  true if you want to pretty print the output json (default=false).
     *              `relax` - true if reLAX parser must be applied to accept unquoted strings (default=false).
     * @param transform closure with one parameter - GPathResult
     */
    public void withJson(final Map<String,Object> parms, Closure transform){
        new ParseTransformWriteContext(session, flowFile, REL_SUCCESS, transform){
            final String encoding = (String)parms.getOrDefault("encoding","UTF-8");
            final Boolean indent = (Boolean)parms.getOrDefault("indent",   Boolean.FALSE);
            final Boolean relax = (Boolean)parms.getOrDefault("relax",   Boolean.FALSE);
            @Override
            protected Object parse(InputStream in) throws Exception {
                try( Reader r = IOUtils.toReader(in, encoding)){
                    JsonSlurper parser = new JsonSlurper();
                    if(relax)parser.setType(JsonParserType.LAX);
                    return parser.parse(r);
                }
            }
            @Override
            protected void write(Object data, OutputStream out) throws Exception {
                if(data instanceof Map || data instanceof Collection || data instanceof CharSequence || data instanceof Boolean || data instanceof Number){
                    try( Writer w = IOUtils.toWriter(out, encoding)){
                        JsonUtils.writeJson(data,w,indent?0:-1);
                    }
                }else super.write(data, out);
            }

        }.run();
    }

    @SuppressWarnings("unchecked")
    public void withXml(Closure transform){
        withXml(Collections.EMPTY_MAP, transform);
    }

    /**
     * treats input data as xml (groovy.util.Node) - the result of XmlParser.parse(inStream) and passes the result into `transform` closure.
     * closure must return groovy.util.Node, GPathResult, or StreamWriter to write output or null to drop output file.
     * @param parms `validating` - true if the parser should validate documents as they are parsed (default=false, ignored if `parser` defined).
     *              `namespaceAware` -  true if the parser should provide support for XML namespaces (default=true, ignored if `parser` defined).
     *              `parser` - XmlSlurper or XmlParser object that has a method `parse(InputStream)` - if provided then `validating` and `namespaceAware` parameters are ignored.
     *              `indent` - should the xml be pretty printed (default=true) (only for groovy.util.Node).
     *              `xmlDeclaration` - prepend xml declaration (default=false) (only for groovy.util.Node).
     * @param transform closure with one parameter - GPathResult
     */
    public void withXml(final Map<String,Object> parms, Closure transform){
        new ParseTransformWriteContext(session, flowFile, REL_SUCCESS, transform){
            @Override
            protected Object parse(InputStream in) throws Exception {
                Object parser = parms.get("parser");
                if(parser!=null){
                    Method m = parser.getClass().getMethod("parse", InputStream.class);
                    return m.invoke(parser, in);
                }else {
                    Boolean validating = (Boolean) parms.getOrDefault("validating", Boolean.FALSE);
                    Boolean namespaceAware = (Boolean) parms.getOrDefault("namespaceAware", Boolean.TRUE);
                    return new XmlParser( validating, namespaceAware).parse(in);
                }
            }
            @Override
            protected void write(Object o, OutputStream out) throws Exception {
                Boolean xmlDeclaration = (Boolean) parms.getOrDefault("xmlDeclaration", Boolean.FALSE);
                Boolean indent = (Boolean) parms.getOrDefault("indent", Boolean.TRUE);
                if(o instanceof Node) {
                    XmlUtils.toStream((Node) o, out, "UTF-8", xmlDeclaration, indent);
                }else if(o instanceof GPathResult){
                    XmlUtil.serialize((GPathResult) o, out);
                }else super.write(o, out);
            }
        }.run();
    }
	
    @SuppressWarnings("unchecked")
    public void withReader(Closure transform){
		withReader(Collections.EMPTY_MAP, transform);
	}

    /**
     * runs closure `transform` passing one (Reader sin) or two (Reader sin,Map attr) parameters.
     * closure should process input data, optionally change the attributes, and could return the StreamWritable object or null to drop flow file.
     * @param parms `encoding` - the encoding to read the input stream (default UTF-8)
     * @param transform transformer
     */
    public void withReader(final Map<String,Object> parms, Closure transform){
        new ParseTransformWriteContext(session, flowFile, REL_SUCCESS, transform){
            final String encoding = (String)parms.getOrDefault("encoding","UTF-8");
			Reader reader;
            @Override
            protected Object parse(InputStream in) throws Exception {
				reader = IOUtils.toReader(in, encoding);
				return reader;
            }
            @Override
            void finit() {
				IOUtils.closeQuietly(reader);
            }
        }.run();
    }

    /**
     * the same as another asReadWriter but with default parameters (encoding=UTF-8)
     * @param c see description in withReadWriter(Map,Closure)
     */
    @SuppressWarnings("unchecked")
    public void withReadWriter(Closure c) {
        withReadWriter(Collections.EMPTY_MAP, c);
    }

    /**
     * runs `transform` closure passing two (Reader r,Writer w) or three (Reader r,Writer w,Map attr) parameters.
     * closure should process reader data, write output to writer, and optionaly change the attributes. use only if you don't need to drop file.
     * note, that return value of the closure ignored.
     * @param parms additional parameter(s): `encoding` - encoding used for reader and writer (default=UTF-8)
     * @param transform closure
     */
    public void withReadWriter(final Map<String,Object> parms, final Closure transform){
        final String encoding = (String)parms.getOrDefault("encoding","UTF-8");
        new ParseTransformWriteContext(session, flowFile, REL_SUCCESS, null){
            @Override
            public boolean processContent(InputStream sin, OutputStream sout, ControlMap attr) throws IOException {
                Object ret = null;
                try(Reader r = IOUtils.toReader(sin,encoding)){
                    try(Writer w = IOUtils.toWriter(sout,encoding)){
                        if(transform.getMaximumNumberOfParameters()==2){
                            ret = delegated(transform).call(r,w);
                        }else{
                            ret = delegated(transform).call(r,w,attr);
                        }
                        w.flush();
                    }
                }
                return true; //(ret!=null); //transfer file
            }
        }.run();
    }

    /**
     * runs closure `transform` passing two (InputStream sin,OutputStream sout) or three (InputStream sin,OutputStream sout,Map attr) parameters.
     * closure should process input data, write output, and optionally change the attributes. use only when you don't need to drop flow file.
     * note, that return value of the closure ignored.
     * @param transform closure
     */
    public void withStreams(final Closure transform){
        new ParseTransformWriteContext(session, flowFile, REL_SUCCESS,null){
            @Override
            public boolean processContent(InputStream sin, OutputStream sout, ControlMap attr) throws IOException {
                Object ret = null;
                if(transform.getMaximumNumberOfParameters()==2){
                    ret = this.delegated(transform).call(sin,sout);
                }else{
                    ret = this.delegated(transform).call(sin,sout,attr);
                }
                return true; //(ret!=null); //transfer file
            }
        }.run();
    }


    /**
     * runs closure `transform` passing one (InputStream sin) or two (InputStream sin,Map attr) parameters.
     * closure should process input data, optionally change the attributes, and could return the StreamWritable object or null to drop flow file.
     * @param transform the transformer to apply to a flowfile content
     */
    public void withStream(Closure transform){
        new ParseTransformWriteContext(session, flowFile, REL_SUCCESS, transform).run();
    }

    /**
     * writes content and attributes to current flow file without processing current file content.
     * @param transform closure that could accept one (attributes) or zero parameters. must return one of the:
     * `asStream{}` , `asWriter{}` , `CharSequence` , or `groovy.lang.Writable`
     */
    public void write(final Closure transform){
        new ParseTransformWriteContext(session, flowFile, REL_SUCCESS, transform){
            @Override
            public Object transform(Object data, ControlMap attr) throws IOException {
                if(transform.getMaximumNumberOfParameters()==1){
                    return delegated(transform).call(attr);
                }else{
                    return delegated(transform).call();
                }
            }
        }.run();
    }

    private Map<String, Class<ParseTransformWriteContext>> methodsCache = new HashMap<>();
    /**
     * method to support external `with` commands implementation. normally called by groovy.
     * searches for <code>groovyx.acme.nifi.worker.`name`.`Name`</code> class that implements flowfile transforming method
     * @param name method name
     * @param arg arguments provided by caller
     * @return null
     */
    @SuppressWarnings("unchecked")
    public Object methodMissing(String name, Object arg){
        Object[] args = null;
        if( arg instanceof Object[] )args = (Object[])arg;
        else throw new RuntimeException("Unsupported argument list: "+arg+" for `"+name+"`");
        if(name==null || name.length()<1) throw new RuntimeException("Unsupported method: `"+arg+"`");

        Class<ParseTransformWriteContext> methodClass = methodsCache.get(name);
        if(methodClass==null) {
            String className = "groovyx.acme.nifi.worker." + name + "." + Character.toUpperCase(name.charAt(0)) + name.substring(1);
            try {
                methodClass = (Class<ParseTransformWriteContext>) this.getClass().getClassLoader().loadClass(className);
            } catch (Throwable e) {
                MissingMethodException me = new MissingMethodException( name, this.getClass(), args );
                throw new RuntimeException(me.getMessage(),e);
            }
            methodsCache.put(name,methodClass);
        }

        ParseTransformWriteContext ctx = null;
        try {
            ctx = methodClass.newInstance();
        } catch (Throwable e) {
            throw new RuntimeException("Failed to instantiate "+methodClass,e);
        }
        ctx.init(this.session, this.flowFile, this.REL_SUCCESS, null);
        ctx.invoke(args);
        return null;
    }


}
