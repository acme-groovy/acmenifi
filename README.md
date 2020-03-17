# AcmeNiFi

Helpers for groovy script processor in NiFi.

Actually it's a try to minimize scripting required to work with some standard formats in apache-nifi

## how to use it in nifi

there are two ways of usage

### grab it directly from your groovy script

```groovy
@Grab(group='acme.groovy', module='acmenifi', version='20190218', transitive=false)
import static groovyx.acme.nifi.AcmeNiFi.*
```

### or download jar and point it from groovy script classpath

download from maven repository:

https://dl.bintray.com/acme-groovy/repo/acme/groovy/acmenifi/

Create a special folder in apache-nifi root directory. For example `./glib` and put there the library `acmenifi-XYZ.jar`

for each ExecuteGroovyScript processor that uses this library you have to set it in `Additional classpath` property:
```
Additional classpath = ./glib/*.jar
```

> *NOTE:* You can't use `./lib` directory because in this case the library will be loaded into a memory before `groovy` processor and as a result there will be a failure like: class not found `groovy/lang/ClassLoader`...

## EXAMPLES

I met a lot of cases in nifi when standard processors are not effective in transformation. In this case you start writing scripts and I realized that to write even a simple JSON transformation, you have to write repeating code like:

```groovy
import groovy.json.JsonSlurper

def flowFile = session.get()
if(!flowFile)return

def json = session.read(flowFile).withReader("UTF-8"){ new JsonSlurper().parse(it) }
//here some transformation. for example adding current date into json:
json.today = new Date().format("yyyy-MM-dd")
//write json back to flowfile and transfer:
flowFile = session.write(flowFile,{out->
    out.withWriter("UTF-8"){w-> w << JsonOutput.toJson() }
  } as OutputStreamCallback)
session.transfer(flowFile, REL_SUCCESS)
```

I imagine a closure where I can say: want to work with flow file as with json and returned value must be a json written back into a flow file.

So, the same code as above will look like:

```groovy
import static groovyx.acme.nifi.AcmeNiFi.*

withFlowFile(this).withJson(encoding:"UTF-8", indent:true){json,attr->
  json.today = new Date().format("yyyy-MM-dd")
  return json
}
```

set flow file attributes from json content

```groovy
import static groovyx.acme.nifi.AcmeNiFi.*

withFlowFile(this).withJson(encoding:"UTF-8", indent:true){json,attr->
  //assume json root is an object (map). let's iterate through it and extract  
  json.each{k,v->                      //standard groovy iterator through java map (json object)
    attr[k] = v                        //put value into attribute by corresponding key
  }
  json.today = attr."the current date" //write attribute value into json object value
  return json                          //this will write json back to flow file and transfer to success 
}
```

See more examples: [EXAMPLES.md](./EXAMPLES.md)
