[TBD: include pointers to "good" unit tests in the project]

GemFire is tested with a combination of junit, dunit (distributed unit) and regression tests.  Junit tests are written as with any other project while dunit tests use a multi-JVM environment (see [[Distributed Unit Tests|dunit---Distributed-Unit-Tests]]).

Always write unit tests for new functionality and always ensure that all unit tests pass before submitting a pull request.

    ./gradlew check

Our ultimate goal is to have the highest quality product for our users. There are multiple ways of slicing this issue, but if we go all the way back to when the code is being developed, we find unit tests. Unit tests are the foundation in continuous software testing, with the top characteristic of being automated. Good unit testing can save a lot of time and energy later on in the software development lifecycle.

Possible goals to strive for when creating our tests: (from http://artofunittesting.com/definition-of-a-unit-test/)

- Able to be fully automated

- Has full control over all the pieces running (Use mocks or stubs to achieve this isolation when needed)

- Can be run in any order if part of many other tests

- Runs in memory (no DB or File access, for example)

- Consistently returns the same result (You always run the same test, so no random numbers, for example. save those for integration or range tests)

- Runs fast

- Tests a single logical concept in the system

- Readable

- Maintainable

- Trustworthy (when you see its result, you don't need to debug the code just to be sure)

Good sites for getting ideas:

- http://artofunittesting.com/definition-of-a-unit-test

- http://readwrite.com/2008/08/13/12_unit_testing_tips_for_software_engineers 