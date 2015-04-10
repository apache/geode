### When should I use Geode?

Application developers and IT architects who need extremely fast processing and consistent data using open source software often run into trouble. When their applications are required to support thousands of concurrent transactions that access hundreds of gigabytes of operational data, they start having performance problems, or problems with the integrity of data.

Geode is an in-memory distributed database designed to provide high performance, low latency, extreme scale-out concurrency and consistency for data storage.

Unlike traditional relational databases with scaling limitations, Geode scales out horizontally across many nodes to provide low latency response for thousands of concurrent read and write operations on terabytes of data in-memory. 

Unlike many in-memory data grids, Geode can maintain a high degree of data consistency across many concurrent transactions and can operate as a highly available, resilient service. This makes it possible for users to deploy mission critical applications at very high scale.

### Is Geode a mature technology?

Yes, Geode is an extremely mature and robust product that can trace its legacy all the way back to one of the first Object Databases for Smalltalk: GemStone. Geode (as GemFire™) was first deployed in the financial sector as the transactional, low-latency data engine used by multiple Wall Street trading platforms.  Today Geode is used by over 600 enterprise customers for high-scale, 24x7 business critical applications. An example deployment includes [China National Railways](http://pivotal.io/big-data/case-study/scaling-online-sales-for-the-largest-railway-in-the-world-china-railway-corporation) that uses Geode to run railway ticketing for the entire country of China with a 10 node cluster that manages 2 terabytes of "hot data" in memory, and 10 backup nodes for high availability and elastic scale.

### How big can Geode scale?

Geode has been deployed to run mission-critical applications on clusters with 100+ members managing terabytes of data in-memory.  Adding capacity is as simple as spinning up a new node.  Geode automatically configures the new member and reassigns data and loading across the cluster.

### What operating systems are supported?

Geode is supported on most JDK platforms including Linux and Windows. For more details please check the [certification matrix](http://geode-docs.cfapps.io/docs-gemfire/getting_started/system_requirements/supported_configurations.html#system_requirements). 

### How does my application connect to a Geode cluster?

Geode provides Java client APIs that can be used by any other language running in the JVM (Scala, Groovy, Javascript, etc). A REST interface is supported as well.  C++ and .NET clients are not part of the current open source product, but are available in the commercial GemFire product.

The client drivers can be configured to cache data locally for improved performance.  In addition, the driver provides single-hop network reads and writes for optimal performance.

### Does Geode support zero downtime operation?

Yes, Geode provides rolling upgrade support so a cluster can remain online even while being upgraded.  In addition, Geode undergoes strict backwards compatibility testing to ensure that existing applications will continue to function.  PDX support for forwards and backwards data versioning allows a data model to evolve seamlessly.
 
### Does Geode support transactions?

Yes, Geode provides atomic transactions (all data operations succeed or fail together). Transactions are executed on a single node to avoid expensive distributed lock operations.  When the transaction is committed the results are replicated to the other cluster members.  Data used in a transaction must be colocated.  Within the context of a transaction updates will not be seen until they are committed.

### What happens if a member runs out of memory?

Geode works to prevent resource issues by supporting LRU (least recently used) eviction.  Eviction can be configured to overflow an entry to disk or remove it altogether.  Expiration is also supported.  The [[Resource Manager]] can be configured to generate alerts at eviction and critical memory usage thresholds.  When a member is in a critical state further writes are blocked to allow the GC and eviction activities to restore the member to normal operation.

### What happens if a node fails?

Geode provides data redundancy to ensure zero data loss when a node fails.  In addition, availability zones can be configured to ensure that redundant data copies are hosted on different racks. Because Geode guarantees data consistency the failover to the redundant copies is seamless.

### If I shutdown all members in a cache will I lose data?

You can configure a Geode region to store it's data to local disk.  When the cluster is restarted all members restore the in-memory data from disk.  Keys are recovered first and values are recovered asynchronously.  Members ensure consistency by exchanging version information.

### How does Geode ensure strong consistency?

Cache updates are synchronously replicated to ensure consistency prior to acknowledging success.  Replicates employ a type of version vector as an extension to the entry versioning scheme to validate data liveness.

### How does Geode partition data?

Keys are hash-partitioned over a fixed number of buckets (the default bucket count is 113).  Buckets are automatically balanced across the cluster members based on data size, redundancy, and availability zones.
 
### How Geode handle a network partition?

The new network partition (aka split brain) detection system will declare a partition event if membership is reduced by a certain percent due to abnormal loss of members within a single membership view change.

Each non-admin member will have a weight of 10 except the lead-member, which will have a weight of 15. Locators will have a weight of 3. The weights of members prior to the view change are added together and compared to the weight of members lost between the last view and completion of installation of the new view.

The default setting will be 51%, so if you have a distributed system with 10 members (not considering locators for the moment) and membership drops to 5 you would be okay if one of the remaining members was the lead member in the last view. If you lost the lead member when the count went from 10 to 5 a partition would be declared and the 5 members would shut down. Any locator that could see those 5 members would also shut down. The presumption would be that the lead and the other 4 members are doing okay.

New members being added in the view are not considered in the weight calculations. They don't hold any data yet and aren't known by anyone but the coordinator until the view is sent out. They may initiate surprise connections to other members but will soon block if there's a real network partition.

Locators add a small amount of weight so that if there are two locators to show a preference for continuing to run with multiple locators present over having a surviving quorum with the lead member but no locators present. We need to do this to account for the needs of the original 2 locator 2 server configuration. Loss of the lead member in that configuration would cause the other member to shut down unless locators are given enough weight to keep the non-lead alive.

When we count locators in the mix with the 10 server example, the loss of the first 5 cache servers would cause disconnect (weight=55) unless the group of 5 servers seeing this loss retained two or more locators (weight=50 + 3 + 3).

This approach acts to preserve the largest amount of servers in the distributed system, handles 50/50 splits and gets rid of the possibility of all members shutting down if there are no locators. It handles the original case that the coordinator/lead-member system addressed where there were two locators and two cache servers, though it does not eliminate the possibility of the whole system going down in that configuration.

For more information see [[Core Distributed System Concepts]].

### Does Geode support [[JSR-107|https://jcp.org/en/jsr/detail?id=107]]?

While Geode does not directly support [[JSR-107|https://jcp.org/en/jsr/detail?id=107]] (JCache) with an API implementation, it does provide indirect support via Spring.  Spring Data GemFire's Caching [[feature|http://docs.spring.io/spring-data-gemfire/docs/1.6.0.RELEASE/reference/html/#apis:spring-cache-abstraction]] and support for Geode is built on the core Spring Framework's [[Cache Abstraction|http://docs.spring.io/spring/docs/current/spring-framework-reference/htmlsingle/#cache]], which added [[support for JCache|http://docs.spring.io/spring/docs/current/spring-framework-reference/htmlsingle/#_caching_improvements]] annotations in 4.1.  Spring gives Geode developers the best part of JCache without requiring unnecessary or invasive coding patterns.

### How can I contribute?

Please check the [[How to Contribute]] page.
