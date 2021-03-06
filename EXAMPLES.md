# AcmeNiFi groovy DSL Examples


### simple json transform
change the time format of one of the fields in json content and assign the `filename` attribute
##### script
```groovy
import static groovyx.acme.nifi.AcmeNiFi.*
//transform milliseconds to date format
withFlowFile(this).withJson(indent:true){obj,attr->
    obj.event.timestamp = new Date(obj.event.timestamp).format("yyyy-MM-dd HH:mm:ss")
    attr.filename = "event-"+obj.event.timestamp.tr(':','-')+".json"
    return obj
}
```
##### source
```json
{
  "event":{
    "timestamp":1583104614621,
    "message":"ceteris paribus"
  }
}
```
##### result
```json
{
  "event":{
    "timestamp":"2020-03-02 01:16:54",
    "message":"ceteris paribus"
  }
}
```
##### attributes
```groovy
filename="event-2020-03-02 01-16-54.json"
```
----

### simple xml transform
change the time format of one of the tags in xml content
##### script
```groovy
import static groovyx.acme.nifi.AcmeNiFi.*
withFlowFile(this).withXml{xml,attr->
    def ts = new Date(xml.timestamp[0].text() as Long).format("yyyy-MM-dd HH:mm:ss.SSS")
    xml.timestamp[0].value=ts
    return xml
}
```
##### source
```xml
<event>
  <timestamp>1583104614621</timestamp>
  <message>ceteris paribus</message>
</event>
```
##### result
```xml
<event>
  <timestamp>2020-03-02 01:16:54.621</timestamp>
  <message>ceteris paribus</message>
</event>
```
----

### work with streams - asStream
example to append data to a stream
##### script
```groovy
import static groovyx.acme.nifi.AcmeNiFi.*
withFlowFile(this).withStream{inStream,attr->
    return asStream{outStream->
        outStream << inStream
        outStream << " galaxy!".getBytes("UTF-8")
    }
}
```
##### source
```text
hello
```
##### result
```text
hello galaxy!
```
----

### work with writer - asWriter
example to append data to a stream
##### script
```groovy
import static groovyx.acme.nifi.AcmeNiFi.*
withFlowFile(this).withReader(encoding:"UTF-8"){reader,attr->
    return asWriter(encoding:"UTF-8"){writer->
        writer << reader << " galaxy!"
    }
}
```
##### source
```text
hello
```
##### result
```text
hello galaxy!
```
----

### write each line into a new file
read incoming file as reader and write each line into a new file except the first line
##### script
```groovy
import static groovyx.acme.nifi.AcmeNiFi.*
withFlowFile(this).withReader(encoding:"UTF-8"){reader,attr->
    reader.eachLine{line,lineNum->
        if(lineNum>1){
            //create new flow file, rename flow file, and write content
            createFlowFile(content:false).write{newAttr->
                newAttr.filename="line-number ${lineNum}"       //set filename attribute for new file
                asWriter(encoding:"UTF-8"){w-> w.write(line) }  //write line into a new file
            }
        }
    }
    return null //drop original flow file
}
```
##### source
```text
Ad astra per aspera
Carpe vinum
Dulce periculum
```
##### result 1
```text
Carpe vinum
```
##### result 2
```text
Dulce periculum
```
----

### use json reader to process large json files
if incoming json file is really large there is a possibility to process it in event-like mode
##### script
```groovy
import static groovyx.acme.nifi.AcmeNiFi.*
withFlowFile(this).withJsonReader(encoding:"UTF-8"){attr->
    def i = 0
    onValue('$.message.data.*'){item->
        //closure triggered when json path matches
        item.index = i++ 
        item.txt = item.txt.capitalize()
        return item
    }
    onEOF{
        //write total number of items into flowfile attribute
        attr.TotalCount = i
    }
    return asJsonWriter(indent:true)
}
```

##### source
```json
{
  "message": {
    "data": [
      {
        "id": 123,
        "txt": "carpe vinum"
      },
      {
        "id": 124,
        "txt": "dulce periculum"
      },
      {
        "id": 125,
        "txt": "ad astra per aspera"
      }
    ]
  }
}
```
##### result
```json
{
  "message": {
    "data": [
      {
        "id": 123,
        "txt": "Carpe vinum",
        "index": 0
      },
      {
        "id": 124,
        "txt": "Dulce periculum",
        "index": 1
      },
      {
        "id": 125,
        "txt": "Ad astra per aspera",
        "index": 2
      }
    ]
  }
}
```
##### attributes
```groovy
TotalCount="3"
```

----



### transform data with template
to transform data with a template declare a new property with name MY_TEMPLATE (for example)

then you could use `asTemplate` to transform incoming data  

##### property MY_TEMPLATE
```jsp
name : <%= myJson.message.name %>
<% myJson.message.data.each{ item -> %>
  <%= item.id %>, <%= item.txt %>
<% } %>
count: <%= myJson.message.data.size() %>
```

##### script
```groovy
import static groovyx.acme.nifi.AcmeNiFi.*
withFlowFile(this).withJson(encoding:"UTF-8"){json, attr->
    return asTemplate(template: MY_TEMPLATE, encoding: "UTF-8", mode:"%", binding:[myJson:json])
}
```

##### source
```json
{
  "message": {
    "name": "strange phrases",
    "data": [
      {
        "id": 123,
        "txt": "carpe vinum"
      },
      {
        "id": 124,
        "txt": "dulce periculum"
      },
      {
        "id": 125,
        "txt": "ad astra per aspera"
      }
    ]
  }
}
```
##### result
```text
name : strange phrases
  123, carpe vinum
  124, dulce periculum
  125, ad astra per aspera
count: 3
```

----
