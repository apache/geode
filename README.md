[<img src="https://geode.apache.org/img/apache_geode_logo.png" align="center"/>](http://geode.apache.org)

[![Build Status](https://travis-ci.org/apache/geode.svg?branch=develop)](https://travis-ci.org/apache/geode) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0) [![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.geode/geode-core/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.geode%22) [![homebrew](https://img.shields.io/homebrew/v/apache-geode.svg)](http://brewformulas.org/ApacheGeode) [![Docker Pulls](https://img.shields.io/docker/pulls/apachegeode/geode.svg)](https://hub.docker.com/r/apachegeode/geode/)


## Contents
1. [Overview](#overview)
2. [How to Get Apache Geode](#obtaining)
2. [Main Concepts and Components](#concepts)
3. [Location of Directions for Building from Source](#building)
4. [Geode in 5 minutes](#started)
5. [Application Development](#development)
6. [Documentation](http://geode.apache.org/docs/)
7. [Wiki](https://cwiki.apache.org/confluence/display/GEODE/Index)


## <a name="overview"></a>Overview

[Apache Geode](http://geode.apache.org/) is
a data management platform that provides real-time, consistent access to
data-intensive applications throughout widely distributed cloud architectures.

Apache Geode pools memory, CPU, network resources, and optionally local disk
across multiple processes to manage application objects and behavior. It uses
dynamic replication and data partitioning techniques to implement high
availability, improved performance, scalability, and fault tolerance. In
addition to being a distributed data container, Apache Geode is an in-memory
data management system that provides reliable asynchronous event notifications
and guaranteed message delivery.

Apache Geode is a mature, robust technology originally developed by GemStone
Systems. Commercially available as GemFire™, it was first deployed in the
financial sector as the transactional, low-latency data engine used in Wall
Street trading platforms.  Today Apache Geode technology is used by hundreds of
enterprise customers for high-scale business applications that must meet low
latency and 24x7 availability requirements.

## <a name="obtaining"></a>How to Get Apache Geode

You can download Apache Geode from the
[website](http://geode.apache.org/releases/), run a Docker
[image](https://hub.docker.com/r/apachegeode/geode/), or install with
[homebrew](http://brewformulas.org/ApacheGeode) on OSX. Application developers
can load dependencies from [Maven
Central](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22org.apache.geode%22).

Maven
```
<dependencies>
    <dependency>
        <groupId>org.apache.geode</groupId>
        <artifactId>geode-core</artifactId>
        <version>$VERSION</version>
    </dependency>
</dependencies>
```

Gradle
```
dependencies {
  compile "org.apache.geode:geode-core:$VERSION"
}
```

## <a name="concepts"></a>Main Concepts and Components

_Caches_ are an abstraction that describe a node in an Apache Geode distributed
system.

Within each cache, you define data _regions_. Data regions are analogous to
tables in a relational database and manage data in a distributed fashion as
name/value pairs. A _replicated_ region stores identical copies of the data on
each cache member of a distributed system. A _partitioned_ region spreads the
data among cache members. After the system is configured, client applications
can access the distributed data in regions without knowledge of the underlying
system architecture. You can define listeners to receive notifications when
data has changed, and you can define expiration criteria to delete obsolete
data in a region.

_Locators_ provide clients with both discovery and server load balancing
services. Clients are configured with locator information, and the locators
maintain a dynamic list of member servers. The locators provide clients with
connection information to a server.

Apache Geode includes the following features:

* Combines redundancy, replication, and a "shared nothing" persistence
  architecture to deliver fail-safe reliability and performance.
* Horizontally scalable to thousands of cache members, with multiple cache
  topologies to meet different enterprise needs. The cache can be
  distributed across multiple computers.
* Asynchronous and synchronous cache update propagation.
* Delta propagation distributes only the difference between old and new
  versions of an object (delta) instead of the entire object, resulting in
  significant distribution cost savings.
* Reliable asynchronous event notifications and guaranteed message delivery
  through optimized, low latency distribution layer.
* Data awareness and real-time business intelligence. If data changes as
  you retrieve it, you see the changes immediately.
* Integration with Spring Framework to speed and simplify the development
  of scalable, transactional enterprise applications.
* JTA compliant transaction support.
* Cluster-wide configurations that can be persisted and exported to other
  clusters.
* Remote cluster management through HTTP.
* REST APIs for REST-enabled application development.
* Rolling upgrades may be possible, but they will be subject to any
  limitations imposed by new features.

## <a name="building"></a>Building this Release from Source

See [BUILDING.md](https://github.com/apache/geode/blob/develop/BUILDING.md) for
instructions on how to build the project.

## <a name="started"></a>Geode in 5 minutes

Geode requires installation of JDK version 1.8.  After installing Apache Geode,
start a locator and server:

    $ gfsh
    gfsh> start locator
    gfsh> start server

Create a region:

    gfsh> create region --name=hello --type=REPLICATE

Write a client application (this example uses a [Gradle](https://gradle.org)
build script):

_build.gradle_

    apply plugin: 'java'
    apply plugin: 'application'

    mainClassName = 'HelloWorld'

    repositories { mavenCentral() }
    dependencies {
      compile 'org.apache.geode:geode-core:1.1.0'
      runtime 'org.slf4j:slf4j-log4j12:1.7.24'
    }

_src/main/java/HelloWorld.java_

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
          .create("hello");

        region.put("1", "Hello");
        region.put("2", "World");

        for (Map.Entry<String, String>  entry : region.entrySet()) {
          System.out.format("key = %s, value = %s\n", entry.getKey(), entry.getValue());
        }
        cache.close();
      }
    }

Build and run the `HelloWorld` example:

    $ gradle run

The application will connect to the running cluster, create a local cache, put
some data in the cache, and print the cached data to the console:

    key = 1, value = Hello
    key = 2, value = World

Finally, shutdown the Geode server and locator:

    $ gfsh> shutdown --include-locators=true

For more information see the [Geode
Examples](https://github.com/apache/geode-examples) repository or the
[documentation](http://geode.apache.org/docs/).

## <a name="development"></a>Application Development

Apache Geode applications can be written in these client technologies:

* Java [client](http://geode.apache.org/docs/guide/topologies_and_comm/cs_configuration/chapter_overview.html)
  or [peer](http://geode.apache.org/docs/guide/topologies_and_comm/p2p_configuration/chapter_overview.html)
* [REST](http://geode.apache.org/docs/guide/rest_apps/chapter_overview.html)
* [Memcached](https://cwiki.apache.org/confluence/display/GEODE/Moving+from+memcached+to+gemcached)
* [Redis](https://cwiki.apache.org/confluence/display/GEODE/Geode+Redis+Adapter)

The following libraries are available external to the Apache Geode project:

* [Spring Data GemFire](http://projects.spring.io/spring-data-gemfire/)
* [Spring Cache](http://docs.spring.io/spring/docs/current/spring-framework-reference/html/cache.html)
* [Python](https://github.com/gemfire/py-gemfire-rest)
