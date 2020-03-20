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


import java.io.*;

/**
 * io helpers
 */
public class IOUtils{

	public static void closeQuietly(java.io.Reader in){
		if(in != null){
			try { in.close(); } catch (IOException e) {}
		}
	}
	public static void closeQuietly(java.io.InputStream in){
		if(in != null){
			try { in.close(); } catch (IOException e) {}
		}
	}
	public static void closeQuietly(java.io.Writer out){
		if(out != null){
			try { out.flush(); } catch (IOException e) {}
			try { out.close(); } catch (IOException e) {}
		}
	}
	public static void closeQuietly(java.io.OutputStream out){
		if(out != null){
			try { out.flush(); } catch (IOException e) {}
			try { out.close(); } catch (IOException e) {}
		}
	}

	public static Reader toReader(InputStream in, String encoding) throws UnsupportedEncodingException {
		return new BufferedReader(new InputStreamReader(in, encoding));
	}

	public static Writer toWriter(OutputStream out, String encoding) throws UnsupportedEncodingException {
		return new BufferedWriter(new OutputStreamWriter(out, encoding));
	}



}
