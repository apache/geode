# Running Tests

## Prerequisites
Before running tests, ensure all steps in the [BUILDING.md](https://github.com/apache/geode/blob/develop/BUILDING.md) have been performed.

## Running Tests By Type
Tests are broken up into five types - unit, integration, distributed, acceptance, and upgrade.  They can be executed using the following commands from the Geode repository root directory:
* Unit tests: generally test a single class and run quickly  
  `./gradlew test`
* Integration tests: involve inter-operation of components or subsystems  
  `./gradlew integrationTest`
* Distributed tests: involve multiple members of a distributed system.  
  `./gradlew distributedTest`
* Acceptance tests: test Geode from end user perspective  
  `./gradlew acceptanceTest`
* Upgrade tests: test compatibility between versions of Geode and rolling upgrades  
  `./gradlew upgradeTest`

## Running Individual Tests
To run an individual test, you can either
1. Run the test in your [IDE](https://github.com/apache/geode/blob/develop/BUILDING.md#setting-up-intellij)
2. Run from terminal by specifying the sub-project and test type:  
`./gradlew project:testType --tests testName`  
For example:  
    `./gradlew geode-core:test --tests ArrayUtilsTest`  
    `./gradlew geode-core:distributedTest --tests ConnectionPoolDUnitTest`

## Running Tests By Category
To run a specific category of tests (eg: GfshTest):  
`./gradlew project:testType -PtestCategory=fullyQualifiedTestClassName`  
For example:  
`./gradlew geode-core:distributedTest -PtestCategory=org.apache.geode.test.junit.categories.GfshTest`

Available categories can be found in the `geode-junit/src/main/java/org/apache/geode/test/junit/categories` in the Geode repository.

## Viewing Test Results
Test results can be viewed by navigating to
`build/reports/combined` in the Geode repository, then opening the `index.html` file in your browser.
