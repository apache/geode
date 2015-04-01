This guide is for people interested in working on the GemFire code itself. It assumes you have basic familiarity with using GemFire and standard Java tools.

## Building and Running Tests

GemFire is built with gradle. See [[Building and running|Building-and-Running-GemFire-from-Source]] For basic instructions on how to build.

GemFire uses the standard gradle lifecycle. Execute this to build and run all tests:
    ./gradlew clean build

GemFire has quite a few tests, so this will take several hours.  The tests are broken into the following categories

* unit tests - run with `./gradlew test`
* integration tests - run with `./gradlew integrationTest`
* distributed integration tests  - run with `./gradlew distributedTest`

_Note - The Pivotal GemFire engineering team is still in the process of migrating the test suite to the open source code. You may not see any distributedTests yet._

To run an individual test, run the test in your IDE or specify the sub-project and test type like so:

    ./gradlew -D_testType_.single=_testName_ _project_:_testType_

For example:
    ./gradlew -DintegrationTest.single=ArrayUtilsJUnitTest integrationTest

## Setting up your IDE
GemFire uses gradle plugins to generate your IDE configuration files.

### Eclipse
Run 

    ./gradlew eclipse

This will generate a .classath and .project for all subprojects. Import all projects into eclipse.

### Intellij

Run

    ./gradlew idea

Import the resulting projects.