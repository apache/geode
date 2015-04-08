Most of the functionality of Geode is implemented in the subproject gemfire-core.
There, under com/gemstone/gemfire you will find an array of packages that
hold the public API and also the implementation of those APIs.

Here are the main architectural components:

- storage (in memory and on disk)
- distribution engine (peer to peer messaging)
- replication messages and algorithms
- clustering service
- query engine
- transaction service
- distributed lock service
- function execution service
- client/server communication and subscriptions

Here are some good starting areas for each component:

* **Storage: Region implementation**: `internal/cache`

Notable classes are `GemFireCacheImpl`, `LocalRegion`, `DistributedRegion`,
`AbstractRegionMap`, and `AbstractRegionEntry`.

For partitioned regions start with `PartitionRegion`, `BucketRegion`,
`PartitionedRegionDataStore` and `PartitionedRegionHelper`.

* **Storage: Persistence**: `internal/cache` and `internal/cache/persistence`

Start with `DiskStoreImpl` and `Oplog`.

* **Replication Messages**: `internal/cache`

Replication is primarly implemented in messaging classes in this
package.  There are lots of messaging classes here, such as 
`DistributedCacheOperation` & subclasses,
`RemoteOperationMessage` & subclasses, `PartitionMessage` & subclasses,
`SearchLoadAndWriteProcessor`, `GetInitialImageOperation`
and `StateFlushOperation`.

Also look in `CacheDistributionAdvisor` and `RegionAdvisor`.  These hold
profiles of peers and are used to determine who should receive replication
messages.

* **Clustering Service**: `distributed/internal`

Notable classes in these packages for message distribution are
`InternalDistributedSystem`, `DistribuitonManager`, `DistributionMessage`,
`ReplyProcessor21`, `JGroupMembershipManager`.  All peer-to-peer messages
are implemented as subclasses of `DistributionMessage`.

* **Query Engine**: `cache/internal/query`

Start with `DefaultQueryService`.

* **Transaction Service**: `internal/cache`

The primary classes are `TXState` and `TXCommitMessage`.

* **Distributed Lock Service**: `distributed/internal/locks`

`DLockService` and `DLockGrantor` are good classes to begin with.

* **Function Execution Engine**: `cache/execute/internal`

Start with `FunctionServiceManager` and the subclasses of `AbstractExecution`.

* **Client/Server**: `cache/client/internal` and `internal/cache/tier/sockets`

`cache/client/internal` contains the client-side code.  Look at `ServerRegionProxy`,
which hooks into `LocalRegion` (in the `internal/cache` package) to turn it
into a client-side `Region`.  Classes ending with `Op` perform the actual
messaging interaction with servers using the class `OpExecutorImpl`.

Subscription feeds are received and handled by `CacheClientUpdater`.

On the server side the classes in `internal/cache/tier/sockets` come into
play.  `AcceptorImpl` accepts connections from clients and `ServerConnection`
threads handle individual requests.  `CacheClientProxy` sends subscription
messages to clients.  `CacheClientNotifier` receives events from the `Cache`
and hands them to the appropriate proxies.




