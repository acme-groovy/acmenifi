package groovyx.acme.nifi;

import groovy.util.IndentPrinter;
import groovy.util.Node;
import groovy.util.XmlNodePrinter;

import java.io.*;

/**
 * Xml utils
 */

public class XmlUtils {
    /**
     * serialize groovy Node to stream with parameters: encoding, indentation, and xml-declaration
     * @param node xml node to write to output
     * @param out out stream to serialize xml to
     * @param encoding encoding to use during serialization
     * @param xmlDeclaration should we prepend xml-declaration to output
     * @param indent true if should we pretty-print the xml or false to linerize it
     * @throws IOException on io error
     */
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
