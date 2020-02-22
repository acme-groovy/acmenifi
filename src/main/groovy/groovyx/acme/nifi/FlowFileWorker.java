package groovyx.acme.nifi;

import groovy.json.JsonSlurper;
import groovy.lang.Closure;
import groovy.lang.Script;
import groovy.util.Node;
import groovy.util.XmlSlurper;
import groovy.util.slurpersupport.GPathResult;
import groovy.xml.XmlUtil;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.util.Collections;
import java.util.Map;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;


/**
 * Created by dm on 17.02.2019.
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

    @SuppressWarnings("unchecked")
    public void withJson(Closure transform) {
        withJson(Collections.EMPTY_MAP, transform);
    }

    public void withJson(final Map<String,Object> parms, Closure transform){
        new ParseTransformWriteContext(session, flowFile, REL_SUCCESS, transform){
            final String encoding = (String)parms.getOrDefault("encoding","UTF-8");
            final Boolean indent = (Boolean)parms.getOrDefault("indent", Boolean.FALSE);
            @Override
            Object parse(InputStream in) throws Exception {
                try( Reader r = new BufferedReader(new InputStreamReader(in, encoding))){
                    return new JsonSlurper().parse(r);
                }
            }
            @Override
            AcmeWritable writable(final Object data) {
                return new AcmeWritable(encoding){
                    @Override protected Writer writeTo(Writer w)throws IOException{
                        AcmeJsonOutput.writeJson(data, w, indent?0:-1);
                        return w;
                    }
                };
            }
        }.run();
    }

    public void withXml(Closure transform){
        new ParseTransformWriteContext(session, flowFile, REL_SUCCESS, transform){
            @Override Object parse(InputStream in) throws Exception {
                return new XmlSlurper().parse(in);
            }
            @Override
            AcmeWritable writable(final Object o) {
                if(o instanceof GPathResult)
                    return new AcmeWritable(null) {
                        @Override protected OutputStream writeTo(OutputStream out) throws IOException {
                            XmlUtil.serialize((GPathResult) o, out);
                            return out;
                        }
                    };
                if(o instanceof Node)
                    return new AcmeWritable(null) {
                        @Override protected OutputStream writeTo(OutputStream out) throws IOException {
                            XmlUtil.serialize((Node) o, out);
                            return out;
                        }
                    };
                throw new RuntimeException("Unsupported writable object: "+o.getClass()+". Expected: GPathResult or groovy.util.Node");
            }
        }.run();
    }
	
    @SuppressWarnings("unchecked")
    public void withReader(Closure transform){
		withReader(Collections.EMPTY_MAP, transform);
	}
	
    public void withReader(final Map<String,Object> parms, Closure transform){
        new ParseTransformWriteContext(session, flowFile, REL_SUCCESS, transform){
            final String encoding = (String)parms.getOrDefault("encoding","UTF-8");
			Reader reader;
            @Override
            Object parse(InputStream in) throws Exception {
                reader = new BufferedReader(new InputStreamReader(in, encoding));
				return reader;
            }
            @Override
            void finit() {
				IOUtils.closeQuietly(reader);
            }
        }.run();
    }

    public void withReadWriter(final Map<String,Object> parms, Closure rw){
        final String encoding = (String)parms.getOrDefault("encoding","UTF-8");
        Closure transform=new Closure(null){
            public Object doCall(Reader r, Map attr){
                return new AcmeWritable(encoding){
                    @Override
                    protected Writer writeTo(Writer w)throws IOException {
                        if(rw.getMaximumNumberOfParameters()==2) {
                            rw.call(r, w);
                        }else{
                            rw.call(r, w, attr);
                        }
                        return w;
                    }
                };
            }
        };
        new ParseTransformWriteContext(session, flowFile, REL_SUCCESS, transform){
			Reader reader;
            @Override
            Object parse(InputStream in) throws Exception {
                reader = new BufferedReader(new InputStreamReader(in, encoding));
				return reader;
            }
            @Override
            void finit() {
				IOUtils.closeQuietly(reader);
            }
        }.run();
    }

	
    public void withStream(Closure transform){
        new ParseTransformWriteContext(session, flowFile, REL_SUCCESS, transform){
			InputStream stream;
            @Override
            Object parse(InputStream in) throws Exception {
                stream = in;
				return stream;
            }
            @Override
            void finit() {
				IOUtils.closeQuietly(stream);
            }
        }.run();
    }
}
