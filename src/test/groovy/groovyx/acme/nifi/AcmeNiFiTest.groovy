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
package groovyx.acme.nifi

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

class AcmeNiFiTest extends GroovyTestCase {
	public void testJson(){
        def w,o
		def s = '{"a":{"b":[1,2,3,"xx"]},"C":"qwerty",'+((33..11).collect{'"aaa'+it+'":'+it}.join(','))+'}';
        o=new JsonSlurper().parseText(s);
        w=new StringWriter()
        AcmeJsonOutput.writeJson(o,w,-1)
        assert w.toString()==JsonOutput.toJson(o)
	}
    public void testAcmeWritable1(){
        def s = "привет"
        def b = new ByteArrayOutputStream()
        def w = AcmeNiFi.asStream{OutputStream out->
            out.write(s.getBytes("UTF-8"))
        }
        w.streamTo(b)
        assert b.toString("UTF-8")==s

    }
    public void testAcmeWritable2(){
        def s = "привет"
        def b = new ByteArrayOutputStream()
        def w = AcmeNiFi.asWriter("UTF-8"){out->
            assert out instanceof Writer
            out.write(s)
        }
        w.streamTo(b)
        assert b.toString("UTF-8")==s

    }
    public void testAcmeTemplate(){
        def b = new ByteArrayOutputStream()
        def w = AcmeNiFi.asTemplate([name:'мир'],'привет <%= name %>')
        w.streamTo(b)
        assert b.toString("UTF-8")=='привет мир';
    }
	public void testFakeMethod(){}
}
