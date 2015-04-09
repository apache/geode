This guide is for people interested in working on the Geode code itself. It assumes you have basic familiarity with using Geode and standard Java tools.

## Building and Running Tests

Geode is built with [gradle 2.3](https://gradle.org). See [[Building and running|Building-and-Running-Geode-from-Source]] for basic instructions on how to build.  We recommend you use jdk1.7.0_75.

Geode uses the standard gradle lifecycle. Execute this to build and run all tests:

    ./gradlew clean build

Geode has quite a few tests, so this will take several hours.  The tests are broken into the following categories

* unit tests - run with `./gradlew test`
* integration tests - run with `./gradlew integrationTest`
* distributed integration tests  - run with `./gradlew distributedTest`

_Note - The engineering team is still in the process of migrating the test suite to the open source code. You may not see any distributedTests yet._

To run an individual test, run the test in your IDE or specify the sub-project and test type like so:

    ./gradlew -DtestType.single=testName [project:]testType

For example:
    ./gradlew -DintegrationTest.single=ArrayUtilsJUnitTest integrationTest

## Setting up your IDE
Geode uses gradle plugins to generate your IDE configuration files.

### Eclipse

Invoking `./gradlew eclipse` will generate the project and classpath files for all subprojects.  Import all projects into eclipse.  Note: run `gradle build` prior to importing the projects into Eclipse.

### IntelliJ

Invoking `/gradlew idea` will generate project files for IntelliJ.  Import the resulting project files.