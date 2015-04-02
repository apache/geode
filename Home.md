# Overview

TBD...marketing blurb

# GemFire in 5 minutes

Clone the repository and build from source:

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

    import com.gemstone.gemfire.cache.Region;
    import com.gemstone.gemfire.cache.client.*;

    public class HelloWorld {
      public static void main(String[] args) throws Exception {
        ClientCache cache = new ClientCacheFactory()
          .addPoolLocator("localhost", 10334)
          .create();
        Region region = cache
          .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
          .create("region");

        region.put("1", "Hello");
        region.put("2", "World");
    
        region.forEach((k, v) -> System.out.format("key = %s, value = %s\n", k, v));
        cache.close();
      }
    }

Compile and run `HelloWorld.java`.  The classpath should include `gemfire-core-dependencies.jar`.  Note: this example uses Java 8 syntax.

    javac -cp /some/path/gemfire/open/gemfire-assembly/build/install/gemfire/lib/gemfire-core-dependencies.jar HelloWorld.java
    java -cp .:/some/path/gemfire/open/gemfire-assembly/build/install/gemfire/lib/gemfire-core-dependencies.jar HelloWorld

#Application Development

GemFire applications can written in a number of client technologies:

* Java using the GemFire client API or embedded using the GemFire peer API
* [Spring Data GemFire](http://projects.spring.io/spring-data-gemfire/) or [Spring Cache](http://docs.spring.io/spring/docs/current/spring-framework-reference/html/cache.html)
* [Python](https://github.com/gemfire/py-gemfire-rest)
* REST
* [[memcached|Moving from memcached to gemcached]]
