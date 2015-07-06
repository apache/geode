## Building and Testing

You will need Scala 2.10 and sbt 0.13.5 to 0.13.7.

### Building Artifacts

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

Integration tests start up a GemFire cluster and starts up Spark in local mode.
Please make sure you've done following before you run `sbt it:test`:
 - run`sbt package`
 - set environment variable `GEMFIRE` to point to a GemFire installation.

Next: [Quick Start](2_quick.md)
