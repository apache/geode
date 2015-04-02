[Under construction]

Here are the different projects:

- [[gemfire-core|Projects-and-Packages#gemfire-core]], the core product and unit tests
- gemfire-jgroups, a modified form of JGroups used for cluster membership and multicast
- gemfire-joptsimple
- gemfire-json
- gemfire-junit, unit test types
- gemfire-web
- gemfire-web-api


## gemfire-core

This project contains the bulk of GemFire.  Here are the main packages:

* admin: the administrative API
* cache: cache API
* cache/client: additional client cache API
* cache/control: resource manager API (rebalancing, etc)
* cache/execute: distributed function execution
* cache/operations
* cache/partition
* cache/persistence
* cache/query
* cache/server
* cache/snapshot
* cache/util
* cache/wan
* compression
* distributed
* i18n
* lang
* management
* management/cli
* pdx
* ra
* security

Internal packages of note in the gemfire-core project are

* cache/query/internal
* distributed/internal
* internal/cache
* internal/cache/*

