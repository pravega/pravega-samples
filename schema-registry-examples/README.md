<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
This module has applications to demonstrate how pravega applications can work with schema registry for different 
serialization formats. 

### Avro
Avro package has two applications, namely AvroDemo.java and a Message bus sample application.
AvroDemo.java shows sample code for registering and evolving avro schemas and using these schemas in pravega appliations.
This demo showcases using avro serializer library APIs usage with avro schemas, avro generated java classes and POJOs.

Avro package also includes another sample application for message bus which demonstrates writing multiple type of events 
into same pravega stream and reading and processing them either as Typed objects or generic objects using different 
avro deserializer library APIs. 

The schema is registered with the registry service and an encoding header is included with each event that is serialized and 
written into pravega stream. 

This package has three different consumers -
1. Typed Consumer which reads typed events back from the stream using the generated objects. 
This uses a deserializer that reads the header to get the schema but only uses the type from it to identify the object type.
It then uses the respective `protobuf/avro` generated java class to deserialize the data into the read time schema.

2. Typed Consumer with fallback to generic deserialization.
This uses a deserializer that is similar to `1` for all the known types. For any event for which a deserializer is not provided, 
it uses the writer schema from the registry to deserialize the event into `avro.GenericRecord` object. 

3. Generic consumer.
This uses the generic `avro` deserializer to deserialize all events into `avro.GenericRecord`.

### Protobuf
ProtobufDemo.java shows sample code for registering and evolving protobuf schemas with registry service and using these 
schemas in pravega appliations.
This demo showcases using protobuf serializer library APIs usage with protobuf schemas (.pb files) and generated java classes.

### Json
JsonDemo.java shows sample code for registering and evolving json schemas with registry service and using these 
schemas in pravega appliations.
This demo showcases using json serializer library APIs usage with json schemas and POJOs.

### All format demo
AllFormatDemo.java is a sample application that writes all three - avro, protobuf and json - events into the same pravega stream. 
It also demonstrates how the events can be filtered during reads and then written into an output stream while keeping the 
schema information included with the output stream. 