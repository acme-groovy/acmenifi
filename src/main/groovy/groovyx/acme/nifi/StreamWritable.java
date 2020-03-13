package groovyx.acme.nifi;


import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;

import org.apache.nifi.processor.io.OutputStreamCallback;


/**
 * Created by dm on 17.02.2019.
 */
abstract public class StreamWritable { // implements OutputStreamCallback {
    private String encoding;

    StreamWritable(String encoding){
        this.encoding=encoding;
    }

    StreamWritable(Map<String,Object> args){
        this.encoding = (String)args.getOrDefault("encoding", "UTF-8");
    }

    /** overwrite this method if you need to write to a writer */
    protected Writer writeTo(Writer out) throws IOException {
        throw new RuntimeException("The `writeTo(Writer)` not implemented.");
    }

    /** main method used for writing data to stream through writer with encoding specified */
    public OutputStream streamTo(OutputStream out) throws IOException {
        Writer w = new BufferedWriter(new OutputStreamWriter(out, encoding));
        writeTo(w);
        w.flush();
        return out;
    }
}
