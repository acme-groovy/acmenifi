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
