package groovyx.acme.nifi;

import groovy.util.IndentPrinter;
import groovy.util.Node;
import groovy.util.XmlNodePrinter;

import java.io.*;

public class AcmeXmlOutput {
    public static void toStream(Node node, OutputStream out, String encoding, boolean xmlDeclaration, boolean indent) throws IOException {
        Writer w = new BufferedWriter(new OutputStreamWriter(out,encoding));
        if(xmlDeclaration)w.append("<?xml version=\"1.0\" encoding=\""+encoding+"\"?>\n");
        IndentPrinter pw = new IndentPrinter(w, indent?"  ":"", indent){
            private boolean noNewLines = true;
            @Override
            public void println() {
                //this trick to avoid last new line after xml
                if (indent && (noNewLines || super.getIndentLevel()>0)) {
                    super.print('\n');
                    noNewLines = false;
                }
            }

        };
        XmlNodePrinter nodePrinter = new XmlNodePrinter(pw);
        nodePrinter.setPreserveWhitespace(true);
        nodePrinter.print(node);
        w.flush();
    }
}
