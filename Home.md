# Overview

GemFire is a data management platform that provides real-time, consistent access to data-intensive applications throughout widely distributed cloud architectures.  

GemFire pools memory, CPU, network resources, and optionally local disk across multiple processes to manage application objects and behavior. It uses dynamic replication and data partitioning techniques to implement high availability, improved performance, scalability, and fault tolerance. In addition to being a distributed data container, Pivotal GemFire is an in-memory data management system that provides reliable asynchronous event notifications and guaranteed message delivery.

GemFire is an extremely mature and robust product that can trace its legacy all the way back to one of the first Object Databases for Smalltalk: GemStone. GemFire was first deployed in the financial sector as the transactional, low-latency data engine used by multiple Wall Stree trading platforms.  Today GemFire is used by over 600 enterprise customers for high-scale, 24x7 business critical applications. An example deployment includes [China National Railways](http://pivotal.io/big-data/case-study/scaling-online-sales-for-the-largest-railway-in-the-world-china-railway-corporation) that uses Pivotal GemFire to run railway ticketing for the entire country of China with a 10 node cluster that manages 2 gigabytes "hot data" in memory, and 10 backup nodes for high availability and elastic scale.

# Main Concepts and Components

_Caches_ are an abstraction that describe a node in a GemFire distributed system.

Within each cache, you define data _regions_. Data regions are analogous to tables in a relational database and manage data in a distributed fashion as name/value pairs. A _replicated_ region stores identical copies of the data on each cache member of a distributed system. A _partitioned_ region spreads the data among cache members. After the system is configured, client applications can access the distributed data in regions without knowledge of the underlying system architecture. You can define listeners to create notifications about when data has changed, and you can define expiration criteria to delete obsolete data in a region.

_Locators_ provide both discovery and load balancing services. You configure clients with a list of locator services and the locators maintain a dynamic list of member servers. By default, GemFire clients and servers use port 40404 and multicast to discover each other.

# GemFire in 5 minutes

Clone the repository and build from source (note: currently GemFire supports jdk1.7.75):

    $ git clone git@github.com:gemfire/apache-gemfire-staging.git
    $ cd gemfire/
    $ ./gradlew build install

Start a locator and server:

    $ cd gemfire-assembly/build/install/apache-gemfire
    $ ./bin/gfsh
    gfsh> start locator --name=locator
    gfsh> start server --name=server

Create a region:

    gfsh> create region --name=region --type=REPLICATE

Write a client application:

_HelloWorld.java_

    import java.util.Map;
    import com.gemstone.gemfire.cache.Region;
    import com.gemstone.gemfire.cache.client.*;
    
    public class HelloWorld {
      public static void main(String[] args) throws Exception {
        ClientCache cache = new ClientCacheFactory()
          .addPoolLocator("localhost", 10334)
          .create();
        Region<String, String> region = cache
          .<String, String>createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
          .create("region");
    
        region.put("1", "Hello");
        region.put("2", "World");
        
        for (Map.Entry<String, String>  entry : region.entrySet()) {
          System.out.format("key = %s, value = %s\n", entry.getKey(), entry.getValue());
        }
        cache.close();
      }
    }

Compile and run `HelloWorld.java`.  The classpath should include `gemfire-core-dependencies.jar`.

    javac -cp /some/path/gemfire/open/gemfire-assembly/build/install/gemfire/lib/gemfire-core-dependencies.jar HelloWorld.java
    java -cp .:/some/path/gemfire/open/gemfire-assembly/build/install/gemfire/lib/gemfire-core-dependencies.jar HelloWorld

#Application Development

GemFire applications can be written in a number of client technologies:

* Java using the GemFire client API or embedded using the GemFire peer API
* [Spring Data GemFire](http://projects.spring.io/spring-data-gemfire/) or [Spring Cache](http://docs.spring.io/spring/docs/current/spring-framework-reference/html/cache.html)
* [Python](https://github.com/gemfire/py-gemfire-rest)
* REST
* [[memcached|Moving from memcached to gemcached]]
