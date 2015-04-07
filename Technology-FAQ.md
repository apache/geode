## Does Geode directly support [[JSR-107|https://jcp.org/en/jsr/detail?id=107]] AKA JCache ?

While GemFire does not directly support [[JSR-107|https://jcp.org/en/jsr/detail?id=107]] (JCache), e.g. with a API implementation, we do have indirect support via Spring.  Spring Data GemFire's Caching [[feature|http://docs.spring.io/spring-data-gemfire/docs/1.6.0.RELEASE/reference/html/#apis:spring-cache-abstraction]] and support for Geode is built on the core Spring Framework's [[Cache Abstraction|http://docs.spring.io/spring/docs/current/spring-framework-reference/htmlsingle/#cache]], which added [[support for JCache|http://docs.spring.io/spring/docs/current/spring-framework-reference/htmlsingle/#_caching_improvements]] annotations in 4.1.

Therefore, as usual and always, Spring gives Geode/Java Developers the best part of JCache without all the boilerplate fluff, ceremony and cruft leading to invasive code, and therefore puts caching in the application exactly where it belongs!

## What APIs are Available

## What is the largest single data element Geode can handle?

## What is the largest cluster size for Geode?

## What happens if I run out of memory?

## What happens if a node fails?

## What platforms are supported?

## How can I contribute?

## Does Geode run in the cloud?

## Do you support distributed transactions? 

## When should I use Geode versus other technologies?

## Do you have Scala/Groovy…. C++, .NET clients ? Available in the OSS ? 

## What’s the biggest GemFire cluster in production today ? (data)

## What’s the largest GemFire cluster in production today ? (number of members)

## What’s our distributed transaction algorithm/choices/etc.. ? (2PC, Paxos)

## What’s GemFire leader election algorithm ?

## What’s Geode’s relation with GemFireXD ? Is GemFireXD going OSS soon ?



