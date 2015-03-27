GemFire is built using gradle. To build gemfire, just run one of these two commands to create a distribution for your platform.

    ./gradlew distTar
    ./gradlew distZip

The distribution archives will be located in gemfire-assembly/build/distributions/.

To install, unzip the distribution and add the bin directory to your path. You can start servers and examine data using the bin/gfsh script.

To embed gemfire in your application, add lib/gemfire-core-dependencies.jar to your classpath.

See the [Getting Started Guide](http://gemfire.docs.pivotal.io/latest/userguide/index.html#getting_started/book_intro.html) in the pivotal gemfire's users guide for an overview of gemfire.