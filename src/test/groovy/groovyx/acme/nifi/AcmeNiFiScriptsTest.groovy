package groovyx.acme.nifi

import groovy.xml.XmlUtil
import junit.framework.TestCase
import junit.framework.TestResult
import org.apache.nifi.processors.groovyx.ExecuteGroovyScript
import org.apache.nifi.util.MockFlowFile
import org.apache.nifi.util.MockProcessContext
import org.apache.nifi.util.MockProcessorInitializationContext

//import org.junit.Ignore
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

/**
 * uses simple algorithm to parse EXAMPLES.md and run all examples as a test cases
 */
class AcmeNiFiScriptsTest extends GroovyTestCase {
    @Override
    void run(TestResult result){
        Map<String,String> parms=null
        StringBuilder code = new StringBuilder(1024);
        String state  = null; //null:default, else - state name
        String format = null;

        new File("./EXAMPLES.md").eachLine("UTF-8"){line->
            if(state==null) {
                if (line == '----'){
                    //end of test definition. let's run it
                    result.run( new NiFiTC(parms) )
                }else if(line.startsWith("### ")){
                    parms = [:]
                    parms.name=line.substring(4)
                }else{
                    if(line.startsWith("##### ")){
                        state=line.substring(6)
                    }
                }
            } else {
                if(line=~/^```\w+/){
                    format = line.substring(3)
                }else if(line=='```'){
                    parms[ state ] = code.toString()
                    if(format=='xml'){
                        //canonize xml?
                        //parms[ state ] = XmlUtil.serialize( parms[ state ] )
                    }
                    state=null;
                    code.setLength(0)
                }else{
                    if(code.length()>0)code.append("\n");
                    code.append(line);
                }
            }
        }
    }

    void testFakeMethod(){}

    //@Ignore
    private class NiFiTC extends TestCase{
        Map<String,String>parms

        private NiFiTC(Map<String,String>parms){
            super((String)parms.name)
            this.parms=parms
        }
        @Override
        protected void runTest(){
            ExecuteGroovyScript processor = new ExecuteGroovyScript()
            MockProcessContext context = new MockProcessContext(processor)
            MockProcessorInitializationContext initContext = new MockProcessorInitializationContext(processor, context)
            processor.initialize(initContext)
            assert processor.getSupportedPropertyDescriptors()

            TestRunner runner = TestRunners.newTestRunner(processor)
            runner.setValidateExpressionUsage(false)
            //runner.addControllerService("dbcp", dbcp, new HashMap<>())
            //runner.enableControllerService(dbcp)

            runner.setProperty(processor.SCRIPT_BODY, parms.script)
            //set dynamic properties
            parms.each {k,v-> if(k.startsWith("property "))runner.setProperty(k.substring(9), v) }

            runner.assertValid()

            runner.enqueue(parms.source.getBytes("UTF-8"))
            runner.run()
            List<MockFlowFile> success = runner.getFlowFilesForRelationship(processor.REL_SUCCESS)
            def results = parms.findAll {k,v->k.startsWith("result")}
            if(results.size()>0){
                assert success.size()==results.size()
                results.eachWithIndex{ key,expected, int i ->
                    String content = new String(success.get(i).toByteArray(),"UTF-8")
                    assert content==expected
                }
            }
            //validate attributes
            results = parms.findAll {k,v->k.startsWith("attributes")}
            if(results.size()>0){
                assert success.size()==results.size()
                results.eachWithIndex{ key,attrDef, int i ->
                    Properties attributes = new ConfigSlurper().parse(attrDef).toProperties()
                    attributes.each {k,v->
                        success.get(0).assertAttributeEquals((String)k, (String)v)
                    }
                }
            }

            //success.get(0).assertAttributeEquals("testAttr", "test content");
        }

    }


}
