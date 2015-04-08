Geode is built using the standard Gradle lifecycle model.  To build Geode, invoke

    ./gradlew build

This will create the binary artifacts and run all of the tests (if for some reason you want to skip running the tests include `-Dskip.tests=true` on the gradle invocation).  To create a distribution, invoke one of

    ./gradlew distTar
    ./gradlew distZip

The distribution archives will be located in `gemfire-assembly/build/distributions/`.  To install, extract the archive file and add the bin directory to your path. You can start servers and examine data using the `bin/gfsh` script.  You can also create an exploded distributed using the `install` task.  This will create the distribution directories in `gemfire-assembly/build/install/gemfire`.

To embed Geode in your application, add `lib/gemfire-core-dependencies.jar` to your classpath.

See the [Getting Started Guide](http://gemfire.docs.pivotal.io/latest/userguide/index.html#getting_started/book_intro.html) in the Pivotal GemFire User's Guide for an overview of Geode.
