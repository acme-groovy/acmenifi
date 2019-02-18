package groovyx.acme.nifi;


import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import org.apache.nifi.processor.io.OutputStreamCallback;


/**
 * Created by dm on 17.02.2019.
 */
abstract public class AcmeWritable implements OutputStreamCallback {
    private String encoding;

    AcmeWritable(String encoding){
        this.encoding=encoding;
    }

    /**overwrite this method if you need to write to writer*/
    protected Writer writeTo(Writer out) throws IOException {
        throw new RuntimeException("The `writeTo(Writer)` not implemented.");
    }

    /**overwrite this method if you need to write to stream */
    protected OutputStream writeTo(OutputStream out) throws IOException {
        Writer w = new BufferedWriter(new OutputStreamWriter(out, encoding));
        writeTo(w);
        w.flush();
        return out;
    }

    /**OutputStreamCallback implementation to write data to flow file stream*/
    @Override
    public final void process(OutputStream out){
        try {
            writeTo(out);
            out.flush();
            out.close();
        }catch (Exception e){
            throw new RuntimeException(e.toString(),e);
        }
    }
}
