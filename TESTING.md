# Running Tests

## Prerequisites
Before running tests, ensure all steps in the [BUILDING.md](https://github.com/apache/geode/blob/develop/BUILDING.md) have been performed.

## Running Tests By Type
Tests are broken up into three types - unit, integration, and distributed.  They can be executed using the following commands from the Geode repository root directory:
* Unit tests
  `./gradlew test --parallel`
* Integration tests
  `./gradlew integrationTest --parallel`
* Distributed tests
  `./gradlew distributedTest --parallel`

## Running Individual Tests
To run an individual test, you can either
1. Run the test in your [IDE](https://github.com/apache/geode/blob/develop/BUILDING.md#setting-up-intellij)
2. Run the from terminal by specifying the sub-project and test type:
`./gradlew project:testType --tests testName`
For example:
`./gradlew geode-core:test --tests ArrayUtilsTest`
`./gradlew geode-core:distributedTest --tests ConnectionPoolDUnitTest`

## Running Tests By Category
To run a specific category of tests (eg: GfshTest):
`./gradlew -PtestCategory=fullyQualifiedTestClassName testType`
For example:
`./gradlew -PtestCategory=org.apache.geode.test.junit.categories.GfshTest distributedTest`

Available categories can be found in the `geode-junit/src/main/java/org/apache/geode/test/junit/categories` in the Geode repository.

## Viewing Test Results
Test results can be viewed by navigating to
`build/reports/combined` in the Geode repository, then opening the `index.html` file in your browser.