**[Overview](#overview)**  
**[Main Concepts and Components](#concepts)**  
**[Location of Directions for Building from Source](#building)**  
**[Geode in 5 minutes](#started)**  
**[Application Development](#development)**  
**[Documentation](http://geode.apache.org/docs/)**
**[wiki](https://cwiki.apache.org/confluence/display/GEODE/Index)**  
**Continuous Integration** [![Build Status](https://travis-ci.org/apache/incubator-geode.svg?branch=develop)](https://travis-ci.org/apache/incubator-geode)  

## <a name="overview"></a>Overview

[Apache Geode] (http://geode.apache.org/) is a data management platform that provides real-time, consistent access to data-intensive applications throughout widely distributed cloud architectures.

Apache Geode pools memory, CPU, network resources, and optionally local disk across multiple processes to manage application objects and behavior. It uses dynamic replication and data partitioning techniques to implement high availability, improved performance, scalability, and fault tolerance. In addition to being a distributed data container, Apache Geode is an in-memory data management system that provides reliable asynchronous event notifications and guaranteed message delivery.

Apache Geode is a mature, robust technology originally developed by GemStone Systems in Beaverton, Oregon. Commercially available as GemFire™, the technology was first deployed in the financial sector as the transactional, low-latency data engine used in Wall Street trading platforms.  Today Apache Geode is used by over 600 enterprise customers for high-scale business applications that must meet low latency and 24x7 availability requirements. An example deployment includes [China National Railways](http://pivotal.io/big-data/case-study/scaling-online-sales-for-the-largest-railway-in-the-world-china-railway-corporation) that uses Geode to run railway ticketing for the entire country of China with a 10 node cluster that manages 2 terabytes of "hot data" in memory, and 10 backup nodes for high availability and elastic scale.

## <a name="concepts"></a>Main Concepts and Components

_Caches_ are an abstraction that describe a node in an Apache Geode distributed system.

Within each cache, you define data _regions_. Data regions are analogous to tables in a relational database and manage data in a distributed fashion as name/value pairs. A _replicated_ region stores identical copies of the data on each cache member of a distributed system. A _partitioned_ region spreads the data among cache members. After the system is configured, client applications can access the distributed data in regions without knowledge of the underlying system architecture. You can define listeners to receive notifications when data has changed, and you can define expiration criteria to delete obsolete data in a region.

_Locators_ provide clients with both discovery and server load balancing services. Clients are configured with locator information, and the locators maintain a dynamic list of member servers. The locators provide clients with connection information to a server. 

Apache Geode includes the following features:

* Combines redundancy, replication, and a "shared nothing" persistence architecture to deliver fail-safe reliability and performance.
* Horizontally scalable to thousands of cache members, with multiple cache topologies to meet different enterprise needs. The cache can be distributed across multiple computers.
* Asynchronous and synchronous cache update propagation.
* Delta propagation distributes only the difference between old and new versions of an object (delta) instead of the entire object, resulting in significant distribution cost savings.
* Reliable asynchronous event notifications and guaranteed message delivery through optimized, low latency distribution layer.
* Applications run 4 to 40 times faster with no additional hardware.
* Data awareness and real-time business intelligence. If data changes as you retrieve it, you see the changes immediately.
* Integration with Spring Framework to speed and simplify the development of scalable, transactional enterprise applications.
* JTA compliant transaction support.
* Cluster-wide configurations that can be persisted and exported to other clusters.
* Remote cluster management through HTTP.
* REST APIs for REST-enabled application development.
* Rolling upgrades may be possible, but they will be subject to any limitations imposed by new features.

## <a name="building"></a>Building this Release from Source

Directions to build Apache Geode from source are in the source distribution, file `BUILDING.md`.

## <a name="started"></a>Geode in 5 minutes

With a JDK version 1.8 or a more recent version installed,
start a locator and server:

    $ gfsh
    gfsh> start locator --name=locator
    gfsh> start server --name=server

Create a region:

    gfsh> create region --name=region --type=REPLICATE

Write a client application:

_HelloWorld.java_

    import java.util.Map;
    import org.apache.geode.cache.Region;
    import org.apache.geode.cache.client.*;
    
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

Compile and run `HelloWorld.java`.  The classpath should include `geode-dependencies.jar`.

    javac -cp /some/path/geode/geode-assembly/build/install/apache-geode/lib/geode-dependencies.jar HelloWorld.java
    java -cp .:/some/path/geode/geode-assembly/build/install/apache-geode/lib/geode-dependencies.jar HelloWorld

## <a name="development"></a>Application Development

Apache Geode applications can be written in these client technologies:

* Java [client](http://geode.apache.org/docs/guide/topologies_and_comm/cs_configuration/chapter_overview.html) or [peer](http://geode.apache.org/docs/guide/topologies_and_comm/p2p_configuration/chapter_overview.html)
* [REST](http://geode.apache.org/docs/guide/rest_apps/chapter_overview.html)
* [Memcached](https://cwiki.apache.org/confluence/display/GEODE/Moving+from+memcached+to+gemcached)
* [Redis](https://cwiki.apache.org/confluence/display/GEODE/Geode+Redis+Adapter)

The following libraries are available external to the Apache Geode project:

* [Spring Data GemFire](http://projects.spring.io/spring-data-gemfire/)
* [Spring Cache](http://docs.spring.io/spring/docs/current/spring-framework-reference/html/cache.html)
* [Python](https://github.com/gemfire/py-gemfire-rest)

