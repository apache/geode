## Building and Testing

The spark connector is built with Scala 2.10 and sbt 0.13.5 to 0.13.7.

### Building Artifacts

To build against Apache Geode, you need to build Geode first and publish the jars
to local repository. In the root of Geode directory, run:

```
./gradlew clean build install -Dskip.tests=true
```

In the root directory of connector project, run:
```
./sbt clean package
```

The following jar files will be created:
 - `geode-spark-connector/target/scala-2.10/geode-spark-connector_2.10-0.5.0.jar`
 - `geode-functions/target/scala-2.10/geode-functions_2.10-0.5.0.jar`
 - `geode-spark-demos/target/scala-2.10/geode-spark-demos_2.10-0.5.0.jar `

### Testing
Commands to run unit and integration tests:
```
./sbt test        // unit tests
./sbt it:test     // integration tests  
```

Integration tests start a Geode cluster and Spark in local mode.
Please make sure you've done following before you run `./sbt it:test`:
 - run`./sbt package`

Next: [Quick Start](2_quick.md)
