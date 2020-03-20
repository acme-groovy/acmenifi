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
import org.apache.nifi.components.PropertyValue;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Map;

/**
 * The root nifi ExecuteGroovyScript helpers to process input file or create new one.
 * Normally methods below included statically in script:
 * <pre>{@code
 *     import static groovyx.acme.nifi.AcmeNiFi.*
 *
 *     withFlowFile(this).withSomeWorker{  // check `FlowFileWorker` class for different workers
 *         ...
 *     }
 * }</pre>
 */
public class AcmeNiFi{
	//no need to instantiate
	private AcmeNiFi(){}

	/**
	 * get flowfile from input queue for processing
	 * @param script the script where this file worker instantiated from. normally passed as `this` - <code>withFlowFile(this)</code>
	 * @return returns `FlowFileWorker` class where default workers defined
	 */
	public static FlowFileWorker withFlowFile(Script script){
		return new FlowFileWorker(script,false);
	}

	/**
	 * creates new flowfile without getting new one from input queue.
	 * @param script the script where this file worker instantiated from. normally passed as `this` - <code>newFlowFile(this)</code>
	 * @return returns `FlowFileWorker` class where default workers defined
	 */
	public static FlowFileWorker newFlowFile(Script script){
		return new FlowFileWorker(script,true);
	}

}
