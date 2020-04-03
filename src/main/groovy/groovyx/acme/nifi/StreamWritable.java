package groovyx.acme.nifi;


import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;

import org.apache.nifi.processor.io.OutputStreamCallback;


/**
 * class used to stream/write data to output. used by FlowFileWorker and TransformerDelegate.
 * almost all workers supporting this object to be returned by transformers (closures).
 * this class could be instantiated with one of the transformer delegate methods: {@code asWriter{w->...}} or {@code asStream{outStream->...}}   (see TransformerDelegate)
 */
abstract public class StreamWritable { // implements OutputStreamCallback {
    protected String encoding;

    /**
     * constructor used for dynamic implementations. after this constructor the method {@code init()} will be called for dynamic implementations.
     */
    public StreamWritable(){}

    /**
     * for dynamic delegate writable methods implementation.
     * @param args arguments passed for implementation method in script.
     */
    public void init(Object []args){
        throw new IllegalStateException("Not implemented");
    }

    public StreamWritable(String encoding){
        this.encoding=encoding;
    }

    StreamWritable(Map<String,Object> args){
        this.encoding = (String)args.getOrDefault("encoding", "UTF-8");
    }

    /** overwrite this method if you need to write to a writer
     * @param out writer to write flow file with encoding specified in constructor.
     * @return out
     * @throws IOException when io exception occurs
     **/
    protected Writer writeTo(Writer out) throws IOException {
        throw new RuntimeException("The `writeTo(Writer)` not implemented.");
    }

    /** main method used for writing data to stream through writer with encoding specified.
     * method could be redefined by descendants to write directly to stream without writer.
     * default implementation creates buffered writer with encoding defined in constructor and writes it using `writeTo` method.
     * @param out outputstream used to write the output
     * @return out
     * @throws IOException when io exception occurs
     **/
    public OutputStream streamTo(OutputStream out) throws IOException {
        Writer w = new BufferedWriter(new OutputStreamWriter(out, encoding));
        writeTo(w);
        w.flush();
        return out;
    }
}
