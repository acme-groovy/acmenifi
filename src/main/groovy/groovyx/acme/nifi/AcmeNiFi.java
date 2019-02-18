/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package groovyx.acme.nifi;


import groovy.lang.Closure;
import groovy.lang.Script;
import groovy.text.Template;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Map;

/**
 * nifi helpers
 * by dlukyanov@ukr.net 
 */
public class AcmeNiFi{

	public static FlowFileWorker withFlowFile(Script script){
		return new FlowFileWorker(script,false);
	}

	public static FlowFileWorker newFlowFile(Script script){
		return new FlowFileWorker(script,true);
	}

    /** helper to return alternate serializer of the parsed flowfile object that requires writer.
     * <code>return asWriter("UTF-8"){out-> out.write(stringContent)}</code>
     * */
	public static AcmeWritable asWriter(String encoding, final Closure c){
        return new AcmeWritable(encoding){
            @Override
            protected Writer writeTo(Writer out)throws IOException {
                c.call(out);
                return out;
            }
        };
	}
    /** helper to return alternate serializer of the parsed flowfile object that requires writer.
     * <code>return asWriter{out-> out.write(stringContent)}</code>
     * */
    public static AcmeWritable asWriter(Closure c){
        return asWriter("UTF-8",c);
    }

    /** helper to return alternate serializer of the parsed flowfile object.
     * <code>return asStream{out-> out.write(bytesContent)}</code>
     * */
	public static AcmeWritable asStream(final Closure c){
        return new AcmeWritable(null){
            @Override
            protected OutputStream writeTo(OutputStream out)throws IOException{
                c.call(out);
                return out;
            }
        };
	}

    /** helper to return alternate serializer based on GSP-like template.
     * <code>return asTemplate([var_json:json], 'value from json: <%= var_json.key1.key2 %>' )</code>
     * */
    /*
	public static AcmeWritable asTemplate(final Map<String,Object> args, final String template){
	    String encoding = (String)args.getOrDefault("encoding", "UTF-8");
        return new AcmeWritable(encoding){
            @Override
            protected Writer writeTo(Writer out) throws IOException {
                Template t = Templates.get(template);
                t.make(args).writeTo(out);
                return out;
            }
        };
	}
	*/
}
