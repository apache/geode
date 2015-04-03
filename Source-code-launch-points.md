[Please add to this document when you find gaps or bad advice.]

Most of the functionality of GemFire is implemented in the project gemfire-core.
There, under com/gemstone/gemfire you will find an array of packages that
hold the public API and also the implementation of those APIs.

Here are some good areas to look for the following functionality:

* **Region implementation**: `internal/cache`

Notable classes are `GemFireCacheImpl`, `LocalRegion`, `DistributedRegion`,
`AbstractRegionMap`, and `AbstractRegionEntry`.

For partitioned regions start with `PartitionRegion`, `BucketRegion`,
`PartitionedRegionDataStore` and `PartitionedRegionHelper`.

* **Replication**: `internal/cache`

There are lots of messaging classes here, such as 
`DistributedCacheOperation` & subclasses,
`RemoteOperationMessage` & subclasses, `PartitionMessage` & subclasses,
`SearchLoadAndWriteProcessor`, `GetInitialImageOperation`, `StateFlushOperation`,
and `EntryEventImpl`.

Also look in `CacheDistributionAdvisor` and `RegionAdvisor`.  These hold
profiles of peers and are used to determine who should receive replication
messages.

* **Persistence**: `internal/cache` and `internal/cache/persistence`

Start with `DiskStoreImpl` and `Oplog`.

* **Transactions**: `internal/cache`

The primary classes are `TXState` and `TXCommitMessage`.

* **Querying**: `cache/internal/query`

Start with `DefaultQueryService`.

* **Distributed Locking**: `distributed/internal/locks`

`DLockService` and `DLockGrantor` are good classes to begin with.

* **Clients**: `cache/client/internal` and `internal/cache/tier/sockets`

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

* **Function Execution**: `cache/execute/internal`

Start with `FunctionServiceManager` and the subclasses of `AbstractExecution`.

* **Clustering**: `distributed/internal`

Notable classes in these packages for message distribution are
`InternalDistributedSystem`, `DistribuitonManager`, `DistributionMessage`,
`ReplyProcessor21`, `JGroupMembershipManager`.  All peer-to-peer messages
are implemented as subclasses of `DistributionMessage`.


