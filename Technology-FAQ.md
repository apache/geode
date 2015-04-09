## Does Geode directly support [[JSR-107|https://jcp.org/en/jsr/detail?id=107]] AKA JCache ?

While GemFire does not directly support [[JSR-107|https://jcp.org/en/jsr/detail?id=107]] (JCache), e.g. with a API implementation, we do have indirect support via Spring.  Spring Data GemFire's Caching [[feature|http://docs.spring.io/spring-data-gemfire/docs/1.6.0.RELEASE/reference/html/#apis:spring-cache-abstraction]] and support for Geode is built on the core Spring Framework's [[Cache Abstraction|http://docs.spring.io/spring/docs/current/spring-framework-reference/htmlsingle/#cache]], which added [[support for JCache|http://docs.spring.io/spring/docs/current/spring-framework-reference/htmlsingle/#_caching_improvements]] annotations in 4.1.

Therefore, as usual and always, Spring gives Geode/Java Developers the best part of JCache without all the boilerplate fluff, ceremony and cruft leading to invasive code, and therefore puts caching in the application exactly where it belongs!

## What APIs are Available

## What is the largest single data element Geode can handle?

## What happens if a member run out of memory?

The JVM will run into an non-deterministic state and in order to avoid such situations the recommendation is to configure [[Resource Manager]] settings properly.

## What happens if a node fails?

A node can fails for many different reasons, such as out of memory errors, loss of connectivity or just being unresponsive due to running out of resources such as file descriptors.

If the node crashes and the system is setup with redundancy there is no data loss because the secondary copies will now become primaries.  For more details please check [[Redundancy and Recovery]].

## What platforms are supported?

Geode can run in pure Java mode in pretty much any JDK supported platform. Native libraries are available for Linux and for more details please check the [certification matrix](http://gemfire.docs.pivotal.io/latest/userguide/index.html#getting_started/system_requirements/supported_configurations.html#system_requirements). 

## How can I contribute?

Please check the [[How to Contribute]] page.

## Does Geode run in the cloud?

## Does Geode support distributed transactions? 

Transaction data needs to be co-located, which means the transaction execution will happen atomically and in the same node and the results will be distributed to the other members of the system. During the transaction execution updated data will not be available outside of the transaction context until it completes. 

## When should I use Geode versus other technologies?

## Do you have Scala/Groovy…. C++, .NET clients ? Available in the OSS ? 

Geode provides Java APIs that can be used by any other language running in the JVM. C++ and .NET clients are not part of the current open source product, but available in the commercial offering.

## What’s the biggest Geode cluster in production today ? (data)

## What’s the largest Geode cluster in production today ? (number of members)

## How Geode's achieve consistency between Replicated regions ?

Geode's leverage [[Region Version Vectors]] as an extension to the versioning scheme that aid in synchronization of replicated regions. These use a scheme outlined in [[Concise Version Vectors in WinFS]] and Peer to Peer replication in WinFS.  
 
## How Geode deals with Network Partitioning ?

The new network partition detection system will declare a partition event if membership is reduced by a certain percent due to abnormal loss of members within a single membership view change.

Each non-admin member will have a weight of 10 except the lead-member, which will have a weight of 15. Locators will have a weight of 3. The weights of members prior to the view change are added together and compared to the weight of members lost between the last view and completion of installation of the new view.

The default setting will be 51%, so if you have a distributed system with 10 members (not considering locators for the moment) and membership drops to 5 you would be okay if one of the remaining members was the lead member in the last view. If you lost the lead member when the count went from 10 to 5 a partition would be declared and the 5 members would shut down. Any locator that could see those 5 members would also shut down. The presumption would be that the lead and the other 4 members are doing okay.

New members being added in the view are not considered in the weight calculations. They don't hold any data yet and aren't known by anyone but the coordinator until the view is sent out. They may initiate surprise connections to other members but will soon block if there's a real network partition.

Locators add a small amount of weight so that if there are two locators to show a preference for continuing to run with multiple locators present over having a surviving quorum with the lead member but no locators present. We need to do this to account for the needs of the original 2 locator 2 server configuration. Loss of the lead member in that configuration would cause the other member to shut down unless locators are given enough weight to keep the non-lead alive.

When we count locators in the mix with the 10 server example, the loss of the first 5 cache servers would cause disconnect (weight=55) unless the group of 5 servers seeing this loss retained two or more locators (weight=50 + 3 + 3).

This approach acts to preserve the largest amount of servers in the distributed system, handles 50/50 splits and gets rid of the possibility of all members shutting down if there are no locators. It handles the original case that the coordinator/lead-member system addressed where there were two locators and two cache servers, though it does not eliminate the possibility of the whole system going down in that configuration.

For more information check [[Network Partition Detection]]

## What’s our distributed transaction algorithm/choices/etc.. ? (2PC, Paxos)

Currently Geode supports optimistic lock with repeatable reads. 
 
## What’s Geode leader election algorithm ?

The oldest non-admin member in the cluster is the leader or in Geode's terminology the coordinator. If this node crash or leave the system, the next oldest non-admin member becomes the coordinator. 