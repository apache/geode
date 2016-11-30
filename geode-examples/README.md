# Apache Geode examples

This is the home of Apache Geode examples that are bundled with the project. Contributions<sup>[2]</sup> and corrections are welcome. Please talk to us about your suggestions at [dev@geode.apache.org](mailto:dev@geode.apache.org) or submit a [pull request](https://github.com/apache/incubator-geode/pull/new/develop).

## Example requirements

All examples:

*  Need to be testable. Use unit tests, integration tests or whatever is applicable. Tests will run through the project's CI.
*  Should be `Gradle` projects or part of existing ones. There may be exceptions here, but the community should have a consensus to accept.
*  Have to follow code format & style from Apache Geode <sup>[1]</sup> guidelines.
*  Should contain a `README.md` file with step-by-step instruction on how to set up and run the example. *Diagrams give you extra credit.*
*  Donations need to be licensed through ASL 2.0 and contributors need to file an ICLA<sup>[3]</sup>.

## Structure

### Installation and a Tutorial for Beginners

*  [How to Install](http://geode.apache.org/docs/guide/getting_started/installation/install_standalone.html)
*  Set a `GEODE_HOME` environment variable to point to the root directory of the installation; this directory contains `bin/`. For those that have built from source, it will be the `geode-assembly/build/install/apache-geode` directory.
*  If desired run the tutorial: [Apache Geode in 15 minutes or Less](http://geode.apache.org/docs/guide/getting_started/15_minute_quickstart_gfsh.html)

### Basics

*  [Replicated Region](replicated)
*  Partitioned Region
*  Persistence
*  OQL (Querying)

### Intermediate

*  PDX & Serialization
*  Lucene Indexing
*  OQL Indexing
*  Functions
*  CacheLoader & CacheWriter
*  Listeners
*  Async Event Queues
*  Continuous Querying
*  Transactions
*  Eviction
*  Expiration
*  Overflow
*  Security
*  Off-heap

### Advanced

*  WAN Gateway
*  Durable subscriptions
*  Delta propagation
*  Network partition detection
*  D-lock
*  Compression
*  Resource manager
*  PDX Advanced

### Use cases, integrations and external examples

This section has self-contained little projects that illustrate a use case or an integration with other projects.

*  SpringBoot Application
*  HTTP Session replication
*  Redis
*  Memcached
*  Spark Connector

## References

- [1]  [https://cwiki.apache.org/confluence/display/GEODE/Criteria+for+Code+Submissions](https://cwiki.apache.org/confluence/display/GEODE/Criteria+for+Code+Submissions)
- [2]  [https://cwiki.apache.org/confluence/display/GEODE/How+to+Contribute](https://cwiki.apache.org/confluence/display/GEODE/How+to+Contribute)
- [3]  [http://www.apache.org/licenses/#clas](http://www.apache.org/licenses/#clas)
