**Work in progress**

PDX serialization is the preferred serialization format for storing objects in gemfire. PDX serialization is designed serialize data as compactly as possible, while still providing the capability to read individual fields in a serialized object for query processing. PDX is also designed for ease of use and backwards and forwards compatibility between different versions of your objects.

Bruce Schuchardt wrote up a great article on using PDX on the pivotal blog: [Data Serialization: How to Run Multiple Big Data Apps on a Single Data Store with GemFire](http://blog.pivotal.io/pivotal/products/data-serialization-how-to-run-multiple-big-data-apps-at-once-with-gemfire). In this article, we're going to dive behind the scenes and look at how PDX implements these features.

# Tell Me about Yourself

A serialized object must arrive at it's destination with enough information to deserialize the object. Self describing formats such as JSON or XML include a description of the object embedded in the object. However systems designed for efficiency tend to separate the description of an object from the serialized data itself. With thrift and protobuf, that description is defined with an IDL and turned into code. With Avro, object descriptions are defined in JSON schemas and two systems can exchange schema definitions in a handshake.

PDX takes the approach of schema exchange and cranks it up a notch by taking advantage of GemFire's data storage and distribution capabilities. With PDX, serialized object descriptions are called "types" and types are stored in in GemFire in a distributed type registry. Whenever an object is serialized, PDX creates a type and stores it in the type registry, which assigns the type a unique id. That unique ID is embedded in the serialized data. The PDX type registry takes care of ensuring that the type is available everywhere.

[Image here](image here)

Let's take a look at how the type registry works. At the most basic level, PDX types are stored in a GemFire region called PdxTypes. That region is available on all peers within a distributed system. When a new type is being defined, the type registry uses a distributed lock to ensure that it obtains a unique id, and then puts the new type in the region using a transaction. The type is now known to all peers within the distributed system. If the member is using persistence, the type registry must be persistent so that the type information is also persisted to disk.

Clients obtain types lazily when they try to deserialize an object. If a type is not known to a client, the client fetches the type from a server and caches it in it's own local type registry.

## ID generation across WAN sites
Each WAN site can independently assign ids to types. To ensure different WAN sites do not assign the same ID to different types, the type ID is prefixed with a distributed system id which is unique for each WAN site. When a type is defined in one WAN site, it is added to the queue to be sent to other WAN sites to ensure they receive the type information before the data.

[Image here](image here)

# Serialization format


