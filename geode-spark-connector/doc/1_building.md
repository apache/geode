## Building and Testing

You will need Scala 2.10 and sbt 0.13.5 to 0.13.7.

### Building Artifacts

To build against Apache Geode, you need to build Geode first and publish the jars
to local repository. In the root of Geode directory, run:

```
./gradlew clean build -Dskip.tests=true
./gradlew publishToMavenLocal
```

In the root directory of connector project, run:
```
sbt clean package
```

The following jar files will be created:
 - `gemfire-spark-connector/target/scala-2.10/gemfire-spark-connector_2.10-0.5.0.jar`
 - `gemfire-functions/target/scala-2.10/gemfire-functions_2.10-0.5.0.jar`
 - `gemfire-spark-demos/target/scala-2.10/gemfire-spark-demos_2.10-0.5.0.jar `

### Testing
Commands to run unit and integration tests:
```
sbt test        // unit tests
sbt it:test     // integration tests  
```

Integration tests start a Geode cluster and Spark in local mode.
Please make sure you've done following before you run `sbt it:test`:
 - run`sbt package`

Next: [Quick Start](2_quick.md)
