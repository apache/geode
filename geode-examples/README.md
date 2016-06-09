# Apache Geode examples

This is the home of Apache Geode examples that are bundled with the project. Contributions<sup>[2]</sup> and corrections are as usual welcome and if you have any suggestions come talk to us at [dev@geode.incubator.apache.org](mailto:dev@geode.incubator.apache.org) or just submit a [pull request](https://github.com/apache/incubator-geode/pull/new/develop).

## Example requirements

All examples:
 * Needs to be testable. (unit tests, integration tests or what's applicable) - Tests will ran through project CI.
 * Should be `Gradle` projects or part of existing ones. <sup>There could be a few exceptions here, but community should have consensus to accept</sup>
 * Have to follow code format & style from Apache Geode <sup>[1]</sup> guidelines.
 * Should contain a `README.md` file with step-by-step instruction on how to setup and run. (Diagrams give extra credits).
 * Donations need to be licensed through ASL 2.0 and contributors need to file an ICLA<sup>[3]</sup>

## Structure

### Quick start & Installation

  * [How to Install](http://geode.docs.pivotal.io/docs/getting_started/installation/install_standalone.html)
  * [Apache Geode in 15 minutes or Less](http://geode.docs.pivotal.io/docs/getting_started/15_minute_quickstart_gfsh.html)

### Basics

  * [Replicated Region]()
  * Partitioned Region
  * Persistence
  * OQL (Querying)

### Intermediate

  * PDX & Serialization
  * Functions
  * CacheLoader & CacheWriter
  * Listeners
  * Async Event Queues
  * Continuous Querying
  * Transactions
  * Eviction
  * Expiration
  * Overflow
  * Security
  * Off-heap

### Advanced

  * WAN Gateway
  * Durable subscriptions
  * Delta propagation
  * Network partition detection
  * D-lock
  * Compression
  * Resource manager
  * PDX Advanced

### Use cases & integrations

 This section should have self contained little projects that illustrate a use case or integration with some other projects.

  * SpringBoot Application
  * HTTP Session replication
  * Redis
  * Memcached
  * Spark Connector

## References

- [1] - https://cwiki.apache.org/confluence/display/GEODE/Criteria+for+Code+Submissions
- [2] - https://cwiki.apache.org/confluence/display/GEODE/How+to+Contribute
- [3] - http://www.apache.org/licenses/#clas
