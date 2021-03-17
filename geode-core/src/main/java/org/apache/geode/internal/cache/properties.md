<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->
# Geode System Properties

## Table of contents

1. Miscellaneous
2. Properties list
3. Properties used by other Jars
   1. commons-logging-1.1.1.jar
   2. jsse.jar 

 
> These are the methods found that reference the system properties table.
>
> - `Boolean#getBoolean(String)`
> - `Integer#getInteger(String)`
> - `Integer#getInteger(String, Integer)`
> - `Integer#getInteger(String, int)`
> - `Long#getLong(String)`
> - `Long#getLong(String, Long)`
> - `Long#getLong(String, long)`
> - `System#getProperties`
> - `System#getProperty(String)`
> - `System#getProperty(String, String)`


## Miscellaneous

DistributionConfigImpl constructor prepends `gemfire.` to each valid attribute name, then looks for a System property with that value.  If such a property name exists, it overrides any read from a property file or any properties passed into the caller.

`org.apache.geode.internal.cache.Oplog` looks for properties of the form:

- fullRegionName + `_UNIT_BUFF_SIZE` -- Integer
  > "Asif: The minimum unit size of the Pool created. The ByteBuffer pools present at different indexes will be multiple of this size. Default unit buffer size is 1024"

- fullRegionName + `_MAX_POOL_SIZE` -- Integer
  >"The initial pool size . Default pool size is zero"

- fullRegionName + `_WAIT_TIME` -- Integer
  >"Asif: MAX time in milliseconds for which a thread will wait for a buffer to get freed up. If not available in that duration a buffer will be created on the fly. The timeout has no meaning if the max pool size is -1( no upper bound)"


## Properties list, in Alphabetical Order

| Name | Type | Default value | References |
|---|---|---|---|
| AdminDistributedSystemImpl.TIMEOUT_MS | Integer | `60000` | See `org.apache.geode.admin.internal.AdminDistributedSystemImpl#TIMEOUT_MS`. |
| AvailablePort.fastRandom | Boolean | `false` | See `org.apache.geode.internal.AvailablePort`.<p>If true, an instance of `java.util.Random` is used instead of `java.security.SecureRandom` to randomly select a port.</p><p>This property is available in case there is a performance issue involved with random port selection.</p>|
| AvailablePort.timeout | Integer | `2000` | See `org.apache.geode.internal.AvailablePort#isPortAvailable`.<p>When establishing a locator, this sets the `SO_TIMEOUT` characteristic on the UDP port that we attempt to test.</p><p>Units are in milliseconds.</p> |
| BridgeServer.HANDSHAKE_POOL_SIZE | Integer | `4` | See `org.apache.geode.internal.cache.tier.sockets.AcceptorImpl#HANDSHAKE_POOL_SIZE`. |
| BridgeServer.MAXIMUM_CHUNK_SIZE | Integer | `100` | See `org.apache.geode.internal.cache.tier.sockets.BaseCommand#MAXIMUM_CHUNK_SIZE`. |
| BridgeServer.MAX_INCOMING_DATA | Integer | `-1` | See `org.apache.geode.internal.cache.tier.sockets.BaseCommand#MAX_INCOMING_DATA`.<p> Maximum number of concurrent incoming client message bytes that a cache server will allow. Once a server is working on this number additional incoming client messages will wait until one of them completes or fails. The bytes are computed based in the size sent in the incoming msg header.</p> |
| BridgeServer.MAX_INCOMING_MSGS | Integer | `-1` | See `org.apache.geode.internal.cache.tier.sockets.BaseCommand#MAX_INCOMING_MSGS`.<p> Maximum number of concurrent incoming client messages that a cache server will allow. Once a server is working on this number additional incoming client messages will wait until one of them completes or fails. |
| BridgeServer.SELECTOR | Boolean | `false` | See `org.apache.geode.internal.cache.tier.sockets.AcceptorImpl#DEPRECATED_SELECTOR`.<p>Only used if `max-threads == 0`. This is for 5.0.2 backwards compatibility.</p><p>**Deprecated**, since 5.1 use cache-server max-threads instead.</p> |
| BridgeServer.SELECTOR_POOL_SIZE | Integer | `16` | See `org.apache.geode.internal.cache.tier.sockets.AcceptorImpl#DEPRECATED_SELECTOR_POOL_SIZE`.<p>Only used if `max-threads == 0`. This is for 5.0.2 backwards compatibility.</p><p>**Deprecated**, since 5.1 use cache-server max-threads instead.</p> |
| BridgeServer.SOCKET_BUFFER_SIZE | Integer | `32768` | See `org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier#socketBufferSize`.<br/>See `org.apache.geode.internal.cache.tier.sockets.CacheClientUpdater#CacheClietnUpdater(String, EndpointImpl, List, LogWriter, boolean, DistributedSystem)`.<p>The size of the server-to-client communication socket buffers.</p> |
| BridgeServer.acceptTimeout | Integer | `2900` | See `org.apache.geode.internal.cache.tier.sockets.AcceptorImpl#acceptTimeout`.<p>Sets the accept timeout (in milliseconds). This is how long a server will wait to get its first byte from a client it has just accepted.</p> |
| BridgeServer.backlog | Integer | `1280` | See `org.apache.geode.internal.cache.tier.sockets.AcceptorImpl` constructor.<p>This is the TCP/IP "accept" backlog for client/server communications.</p> | 
| BridgeServer.handShakeTimeout | Integer | `59000` | See `org.apache.geode.internal.cache.tier.sockets.AcceptorImpl#handshakeTimeout`.<p>Sets the hand shake timeout (in milliseconds). This is how long a client will wait to hear back from a server.</p> |
| DistributionManager.DISCONNECT_WAIT | Long | `10 * 1000` | See `org.apache.geode.distributed.internal.InternalDistributedSystem#MAX_DISCONNECT_WAIT`.<p>This is how much time, in milliseconds to allow a disconnect listener to run before we interrupt it.</p> |
| DistributionManager.INCOMING_QUEUE_LIMIT | Integer | `80000` | See `org.apache.geode.distributed.internal.ClusterOperationExecutor#INCOMING_QUEUE_LIMIT`. |
| DistributionManager.MAX_FE_THREADS | Integer | Max of `100` and `16 * number of processors` | See `org.apache.geode.distributed.internal.OperationExecutors#MAX_FE_THREADS`.<p>Maximum function execution threads.</p> |
| DistributionManager.MAX_PR_THREADS | Integer | Max of `200` and `32 * number of processors` | See `org.apache.geode.distributed.internal.ClusterOperationExecutors#MAX_PR_THREADS`. |
| DistributionManager.MAX_SERIAL_QUEUE_THREAD | Integer | `20` | See `org.apache.geode.distributed.internal.ClusterOperationExecutors#MAX_SERIAL_QUEUE_THREAD`.<p>Max number of serial Queue executors, in case of multi-serial-queue executor</p> |
| DistributionManager.MAX_THREADS | Integer | `1000` | See `org.apache.geode.distributed.internal.OperationExecutors#MAX_THREADS`. |
| DistributionManager.MAX_WAITING_THREADS | Integer | `Integer.MAX_VALUE` | See `org.apache.geode.distributed.internal.ClusterOperationExecutors#MAX_WAITING_THREADS`. |
| DistributionManager.OptimizedUpdateByteLimit | Integer | `2000` | See `org.apache.geode.internal.cache.SearchLoadAndWriteProcessor#SMALL_BLOB_SIZE`. |
| DistributionManager.SERIAL_QUEUE_BYTE_LIMIT | Integer | `40 * 1024 * 1024` | See `org.apache.geode.distributed.internal.ClusterOperationExecutors#SERIAL_QUEUE_BYTE_LIMIT`. |
| DistributionManager.SERIAL_QUEUE_SIZE_LIMIT | Integer | `20000` | See `org.apache.geode.distributed.internal.ClusterOperationExecutors#SERIAL_QUEUE_SIZE_LIMIT`. |
| DistributionManager.SERIAL_QUEUE_SIZE_THROTTLE | Integer | `SERIAL_QUEUE_SIZE_LIMIT * THROTTLE_PERCENT` | See `org.apache.geode.distributed.internal.ClusterOperationExecutors#SERIAL_QUEUE_THROTTLE`. |
| DistributionManager.SERIAL_QUEUE_THROTTLE_PERCENT | Integer | `75` | See `org.apache.geode.distributed.internal.ClusterOperationExecutors#THROTTLE_PERCENT`. |
| DistributionManager.TOTAL_SERIAL_QUEUE_BYTE_LIMIT | Integer | `80 * 1024 * 1024` | See `org.apache.geode.distributed.internal.ClusterOperationExecutors#TOTAL_SERIAL_QUEUE_BYTE_LIMIT`. |
| DistributionManager.TOTAL_SERIAL_QUEUE_THROTTLE | Integer | `SERIAL_QUEUE_BYTE_LIMIT * THROTTLE_PERCENT` | See `org.apache.geode.distributed.internal.ClusterOperationExecutors#TOTAL_SERIAL_QUEUE_THROTTLE`. |
| DistributionManager.enqueueOrderedMessages | Boolean | `false` | See `org.apache.geode.distributed.internal.DistributionMessage#INLINE_PROCESS`. |
| DistributionManager.requireAllStartupResponses | Boolean | `false` | See `org.apache.geode.distributed.internal.ClusterDistributionManager#sendStartupMessage`.<p>Fail distributed system join unless a startup response is received from all peers.</p> |
| DistributionManager.singleSerialExecutor | Boolean | `false` |  See `org.apache.geode.distributed.internal.ClusterOperationExecutors#MULTI_SERIAL_EXECUTORS`.<p>Flag indicating whether to use single Serial-Executor thread or Multiple Serial-executor thread.</p> |
| DistributionManager.syncEvents | Boolean | `false` | See `org.apache.geode.distributed.internal.ClusterDistributionManager#SYNC_EVENTS`. |
| DynamicRegionFactory.disablePersistence | Boolean | `false` | See `org.apache.geode.cache.DynamicRegionFactory#DISABLE_PERSIST_BACKUP`. |
| DynamicRegionFactory.disableRegisterInterest | Boolean | `false` | See `org.apache.geode.cache.DynamicRegionFactory#DISABLE_REGISTER_INTEREST`. |
| DynamicRegionFactory.msDelay | Long | `250` | See `org.apache.geode.cache.DynamicRegionFactory#regionCreateSleepMillis`.<p>This controls the delay introduced to try and avoid any race conditions between propagation of newly created Dynamic Regions and the Entries put into them.</p>| 
| Gateway.EVENT_TIMEOUT | Integer | `5 * 60 * 1000` | See `org.apache.geode.internal.cache.wan.AbstractGatewaySender#EVENT_TIMEOUT`.<p>Units are in milliseconds.</p>| 
| GatewaySender.QUEUE_SIZE_THRESHOLD | Integer | `5000` | See `org.apache.geode.internal.cache.wan.AbstractGatewaySender#QUEUE_SIZE_THRESHOLD`.<p>The queue size threshold used to warn the user. If the queue reaches this size, log a warning.</p>|
| GatewaySender.TOKEN_TIMEOUT | Integer | `15000` | See `org.apache.geode.internal.cachewan.AbstractGatewaySender#TOKEN_TIMEOUT`.<p>Timeout tokens in the unprocessedEvents map after this many milliseconds.</p> |
| GetInitialImage.chunkSize | Integer | `500 * 1024` | See `org.apache.geode.internal.cache.InitialImageOperation#CHUNK_SIZE_IN_BYTES`.<p>Maximum number of bytes to put in a single message</p>|
| GrantorRequestProcessor.ELDER_CHANGE_SLEEP | Long | `100` | See `org.apache.geode.distributed.internal.locks.GrantorRequestProcessor#ELDER_CHANGE_SLEEP`.<p>The number of milliseconds to sleep for elder change if current elder is departing (and already sent shutdown msg) but is still in the View.</p>|
| Locator.forceLocatorDMType | Boolean | `false` | See `org.apache.geode.distributed.internal.InternalLocator#FORCE_LOCATOR_DM_TYPE`<p>Used internally by the locator. It sets it to true to tell other code that the member type should be LOCATOR. As of 7.0.</p>|
| Locator.inhibitDMBanner | Boolean | `false` | See `org.apache.geode.distributed.internal.InternalLocator#INHIBIT_DM_BANNER`.|
| StatArchiveReader.dump | Boolean | `false` | See `org.apache.geode.internal.StatArchiveReader` constructor. |
| StatArchiveReader.dumpall | Boolean | `false` | See `org.apache.geode.internal.StatArchiveReader`.<p>See `org.apache.geode.internal.StatArchiveReader.SimpleValue.dump`</p>|
| ack-threshold-exception | Boolean | `false` | See `org.apache.geode.distributed.internal.ReplyProcessor21#THROW_EXCEPTION_ON_TIMEOUT`.|
| org.apache.geode.logging.internal.OSProcess.trace | String | No default value | See `org.apache.geode.logging.internal.OSProcess#bgexec(String[], File, File, boolean)`.<p>If this property exists and has non-zero length (string content is not checked), additional information about the executed command is printed to `System.out`.</p>|
| GemFire.ALWAYS_REPLICATE_UPDATES | Boolean | `false` | See `org.apache.geode.internal.cache.AbstractUpdateOperation#ALWAYS_REPLICATE_UPDATES`.<p>If true then non-replicate regions will turn a remote update they receive on an entry they do not have into a local create. By default, these updates would have been ignored.</p>|
| gemfire.ALLOW_PERSISTENT_TRANSACTIONS | Boolean | `false` | See `org.apache.geode.internal.cache.TxManagerImpl#ALLOW_PERSISTENT_TRANSACTIONS`<p>A flag to allow persistent transactions.</p>|
| gemfire.ASCII_STRINGS | Boolean | `false` | See `org.apache.geode.internal.tcp.MsgStreamer#ASCII_STRINGS`.<p>See `org.apache.geode.internal.BufferDataOutputStream#ASCII_STRINGS`.</p><p>Causes GemFire's implementation of writeUTF to only work for Strings that use the ASCII character set. So Strings that use the international characters will be serialized incorrectly. If you know your Strings only use ASCII setting this to true can improve your performance if you are using writeUTF frequently. Most Strings are serialized using DataSerializer.writeString which does not use writeUTF.</p>|
| gemfire.AutoSerializer.SAFE | Boolean | `false` | See `apache.geode.pdx.internal.AutoSerializableManager`.<p>If set to `true` forces the `ReflectionBasedAutoSerializer` to not use the `sun.misc.Unsafe` code.<p>Using `Unsafe` optimizes performance but reduces portablity.<p>By default, `ReflectionBasedAutoSerializer` will attempt to use `Unsafe` but silently not use it if it is not available.|
| gemfire.AutoSerializer.UNSAFE | Boolean | `false` | See `apache.geode.pdx.internal.AutoSerializableManager`.<p>If set to `true` then the `ReflectionBasedAutoSerializer` will throw an exception if it is not able to use the `sun.misc.Unsafe` code.<p>Using `Unsafe` optimizes performance but reduces portablity.<p>By default, `ReflectionBasedAutoSerializer` will attempt to use `Unsafe` but silently not use it if it is not available.|
| gemfire.BucketAdvisor.getPrimaryTimeout | Long | `15000L` | See `org.apache.geode.internal.cache.BucketAdvisor#waitForNewPrimary`.<p>Add its value to the timeout for a new member to become primary. Units are in milliseconds.|
| gemfire.Cache.ASYNC_EVENT_LISTENERS | Boolean | `false` | See `org.apache.geode.internal.cache.InternalCacheBuilder#USE_ASYNC_EVENT_LISTENERS_PROPERTY`.<p>If true then cache event listeners will be invoked by a background thread.<p>By default, they are invoked by the same thread that is doing the cache operation.|
| gemfire.Cache.EVENT_QUEUE_LIMIT | Integer | `4096` | See `org.apache.geode.internal.cache.GemFireCacheImpl#EVENT_QUEUE_LIMIT`.|
| gemfire.Cache.defaultLockLease | Integer | `120` | See `org.apache.geode.internal.cache.GemFireCacheImpl#DEFAULT_LOCK_LEASE`.<p>The default duration (in seconds) of a lease on a distributed lock.</p>|
| gemfire.Cache.defaultLockTimeout | Integer | `60` | See `org.apache.geode.internal.cache.GemFireCacheImpl#DEFAULT_LOCK_TIMEOUT`.<p>The default number of seconds to wait for a distributed lock.</p>|
| gemfire.Cache.defaultSearchTimeout | Integer | `300` | See `org.apache.geode.internal.cache.GemFireCacheImpl#DEFAULT_SEARCH_TIMEOUT`.<p>The default amount of time to wait for a `netSearch` to complete.<p>Units are in seconds.|
| gemfire.Cache.startSerialNumber | Integer | `1` | See `org.apache.geode.distributed.internal.DistributionAdvisor#START_SERIAL_NUMBER`.<p>Specifies the starting serial number for the `serialNumberSequencer`.|
| gemfire.CacheDistributionAdvisor.rolloverThreshold | Integer | `1000` | See `org.apache.geode.distributed.internal.DistributionAdvisor#ROLLOVER_THRESHOLD`.<p>Used to compare profile versioning numbers against `Integer.MAX_VALUE` `Integer.MIN_VALUE` to determine if a rollover has occurred.| 
| gemfire.Capacity | Integer | `230000` | See `org.apache.geode.internal.cache.ha.HARegionQueueAttributes#BLOCKING_QUEUE_CAPACITY`.|
| gemfire.DEFAULT_MAX_OPLOG_SIZE | Long | `1024L` | See `org.apache.geode.cache.DiskStoreFactory#DEFAULT_MAX_OPLOG_SIZE`.<p>See `org.apache.geode.internal.cache.DiskWriteAttributesImpl#DEFAULT_MAX_OPLOG_SIZE`.<p>Default max in bytes|
| gemfire.DLockService.automateFreeResources | Boolean | `false` | See `org.apache.geode.distributed.internal.locks.DLockService#AUTOMATE_FREE_RESOURCES`|
| gemfire.DLockService.LockGrantorId.rolloverMargin | Integer | `10000` | See `org.apache.geode.distributed.internal.locks.LockGrantorId#ROLLOVER_MARGIN`.|
| gemfire.DLockService.debug.nonGrantorDestroyLoop | Boolean | `false` | See `org.apache.geode.distributed.internal.locks.DLockService#DEBUG_NONGRANTOR_DESTROY_LOOP`.|
| gemfire.DLockService.debug.nonGrantorDestroyLoopCount | Integer | `20` | See `org.apache.geode.distributed.internal.locks.DLockService#DEBUG_NONGRANTOR_DESTROY_LOOP_COUNT`.|
| gemfire.DLockService.notGrantorSleep | Long | `100` | See `org.apache.geode.distributed.internal.locks.DLockService#NOT_GRANTOR_SLEEP`.<p>Units are in milliseconds.|
| gemfire.DistributedLockService.startSerialNumber | Integer | `1` | See `org.apache.geode.distributed.internal.locks.DLockService#START_SERIAL_NUMBER`.<p>Specifies the starting serial number for the `serialNumberSequencer`.|
| gemfire.DO_EXPENSIVE_VALIDATIONS | Boolean | `false` | See `org.apache.geode.internal.cache.LocalRegion#DO_EXPENSIVE_VALIDATIONS`.|
| gemfire.DistributionAdvisor.startVersionNumber | Integer | `1` | See `org.apache.geode.distributed.internal.DistributionAdvisor#START_VERSION_NUMBER`.<p>Specifies the starting version number for the `profileVersionSequencer`.|
| gemfire.EXPIRY_THREADS | Integer | `0` | See `org.apache.geode.internal.cache.ExpiryTask` class init|
| gemfire.EXPIRY_UNITS_MS | Boolean | `false` | See `org.apache.geode.internal.cache.LocalRegion#EXPIRY_MS_PROPERTY`.<p>Used by unit tests to set expiry to milliseconds instead of the default seconds. Used in ExpiryTask.|
| gemfire.IDLE_THREAD_TIMEOUT | Integer | `30000 * 60` | See `org.apache.geode.distributed.internal.FunctionExecutionPooledExecutor` constructor.<p>See `org.apache.geode.internal.logging.CoreLoggingExecutors#getIdleThreadTimeoutMillis`<p>Units are in milliseconds.|
| gemfire.locator-load-imbalance-threshold | Float | `10.0` | See `org.apache.geode.distributed.internal.LocatorLoadSnapshot#LOAD_IMBALANCE_THRESHOLD_PROPERTY_NAME`<p>Sets the connection count threshold for rebalancing clients.  When a client asks the locator whether it should switch to a less loaded server the locator will respond "no" if the connection-count gap between the highest-loaded server and the least-loaded server is within this threshold. If the threshold is reached the locator will aggressivley reassign clients until balance is re-established.|
| gemfire.memoryEventTolerance | Integer | `0` | See `org.apache.geode.internal.cache.control.MemoryThresholds#memoryStateChangeTolerance`<p>Number of eviction or critical state changes that have to occur before the event is delivered.<p>The default is `0` so we will change states immediately by default.|
| gemfire.MAX_PENDING_CANCELS | Integer | `10000` | See `org.apache.geode.internal.cache.ExpirationScheduler#MAX_PENDING_CANCELS`.|
| gemfire.MAXIMUM_SHUTDOWN_PEEKS | Integer | `50` | See `org.apache.geode.internal.cache.tier.sockets.CacheClientProxy#MAXIMUM_SHUTDOWN_PEEKS`.<p>The number of times to peek on shutdown before giving up and shutting down.</p>|
| gemfire.MIN_BUCKET_SIZE | Integer | `1` | See `org.apache.geode.internal.cache.partitioned.SizedBasedLoadProbe.#MIN_BUCKET_SIZE`<p>Allows setting the minimum bucket size to be used during rebalance|
| gemfire.DISABLE_MOVE_PRIMARIES_ON_STARTUP | Boolean | `false` | See `org.apache.geode.internal.cache.PRHARedundancyProvider#scheduleRedundancyRecovery`<p>See `org.apache.geode.internal.cache.control.RebalanceOperationImpl#scheduleRebalance`<p>If true then pr primary buckets will not be balanced when a new member is started.|
| gemfire.MessageTimeToLive | Integer | `180` | See `org.apache.geode.internal.cache.ha.HARegionQueue#getHARegionQueueInstance`.<p>Constant used to set region entry expiry time using system property.<p>Units are in seconds.|
| gemfire.ON_DISCONNECT_CLEAR_PDXTYPEIDS | Boolean | `false` | See `org.apache.geode.cache.client.internal.PoolImpl#ON_DISCONNECT_CLEAR_PDXTYPEIDS`<p>Clear pdxType ids when client disconnects from servers|
| gemfire.PRDebug | Boolean | `false` | See `org.apache.geode.internal.admin.remote.RemoteCacheInfo` constructor.<p>See `org.apache.geode.internal.admin.remote.RootRegionResponse#create(DistributionManager, InternalDistributedMember)`.<p>See `org.apache.geode.internal.cache.FixedPartitionAttributesImpl#toString`.<p>See `org.apache.geode.internal.cache.PartitionedRegionDataStore#createBucketRegion(int)`.<p>See `org.apache.geode.internal.cache.PartitionedRegionHelper#getPRRoot(InternalCache, boolean)`.<p>See `org.apache.geode.internal.cache.PartitionedRegionHelper#logForDataLoss(PartitionedRegion,int,String)`.|
| gemfire.PREFER_SERIALIZED | Boolean | `false` | See `org.apache.geode.internal.cache.CachedDeserializableFactory#PREFER_DESERIALIZED`.<p>Enable storing the values in serialized form|
| gemfire.PRSanityCheckDisabled | Boolean | `false` | See `org.apache.geode.internal.cache.partitioned.PRSanityCheckMessage#schedule`.|
| gemfire.PRSanityCheckInterval | Integer | `5000` | See `org.apache.geode.internal.cache.partitioned.PRSanityCheckMessage#schedule`.<p>Units are in milliseconds.|
| gemfire.PartitionedRegionRandomSeed | Long | `NanoTimer.getTime()` | See `org.apache.geode.internal.cache.PartitionedRegion#RANDOM`.<p>Seed for the random number generator in this class.|
| gemfire.Query.COMPILED_QUERY_CLEAR_TIME | Integer | `10 * 60 * 1000` | See `org.apache.geode.cache.query.internal.DefaultQuery.#COMPILED_QUERY_CLEAR_TIME`<p>Frequency of clean up compiled queries|
| gemfire.Query.VERBOSE | Boolean | `false` | See `org.apache.geode.cache.query.internal.DefaultQuery.#QUERY_VERBOSE`<p>Enable verbose logging in the query execution|
| gemfire.QueryService.QueryHeterogeneousObjects | Boolean | `true` | See `org.apache.geode.cache.query.internal.DefaultQueryService.#QUERY_HETEROGENEOUS_OBJECTS`<p>Allow query on region with heterogeneous objects|
| gemfire.randomizeOnMember | Boolean | `false` | See `org.apache.geode.internal.cache.execute.InternalFunctionExecutionServiceImpl.#RANDOM_onMember`<p>When set, onMember execution will be executed on a random member.|
| gemfire.RegionAdvisor.volunteeringThreadCount | Integer | `1` | See `org.apache.geode.internal.cache.partitioned.RegionAdvisor#VOLUNTERING_THREAD_COUNT`.<p>Number of threads allowed to concurrently volunteer for bucket primary.|
| gemfire.VM_OWNERSHIP_WAIT_TIME | Long | `Long.MAX_VALUE` | See `org.apache.geode.internal.cache.PartitionedRegion#VM_OWNERSHIP_WAIT_TIME`<p>Time to wait for for acquiring distributed lock ownership. Time is specified in milliseconds.|
| gemfire.bridge.disableShufflingOfEndpoints | Boolean | `false` | See `org.apache.geode.cache.cient.internal.ExplicitConnectionSourceImpl#DISABLE_SHUFFLING`.<p>A debug flag, which can be toggled by tests to disable/enable shuffling of the endpoints list.|
| gemfire.bridge.suppressIOExceptionLogging | Boolean | `false` | See `org.apache.geode.internal.cache.tier.sockets.BaseCommand#SUPPRESS_IO_EXCEPTION_LOGGING`.<p>Whether to suppress logging of IOExceptions.|
| gemfire.BridgeServer.FORCE_LOAD_UPDATE_FREQUENCY | Integer | `10` | See `org.apache.geode.internal.cache.CacheServerImpl.#FORCE_LOAD_UPDATE_FREQUENCY`<p>How often to force a CacheServer load message to be sent|
| gemfire.BucketRegion.alwaysFireLocalListeners | Boolean | `false` | See `org.apache.geode.internal.cache.BucketRegion.#FORCE_LOCAL_LISTENERS_INVOCATION`<p>Enable invocation of listeners in both primary and secondary buckets|
| gemfire.Cache.MAX_QUERY_EXECUTION_TIME | Integer | `-1` | See `org.apache.geode.internal.cache.GemFireCacheImpl.#MAX_QUERY_EXECUTION_TIME`<p>Limit the max query execution time (ms)|
| gemfire.CLIENT_FUNCTION_TIMEOUT | Integer | `0` | See `org.apache.geode.internal.cache.execute.AbstractExecution#CLIENT_FUNCTION_TIMEOUT_SYSTEM_PROPERTY`<p>Timeout to set for client function execution|
| gemfire.clientSocketFactory | String | empty | See `org.apache.geode.internal.net.SocketCreator#initializeClientSocketFactory`<p>Non-standard Socket creator|
| gemfire.cq.EXECUTE_QUERY_DURING_INIT | Boolean | `true` | See `org.apache.geode.cache.query.cq.internal.CqServiceImpl.#EXECUTE_QUERY_DURING_INIT`<p>When set to false, avoid query execution during CQ when initial results are not required|
| gemfire.disableAccessTimeUpdateOnPut | Boolean | `false` | See `org.apache.geode.internal.cache.entries.AbstractRegionEntry.#DISABLE_ACCESS_TIME_UPDATE_ON_PUT`<p>Whether to disable last access time update when a put occurs.|
| gemfire.disable-event-old-value | Boolean | Default: `false` | See `org.apache.geode.internal.cache.EntryEventImpl#EVENT_OLD_VALUE`.<p>Discussing EVENT_OLD_VALUE = !Boolean.getBoolean():<p>- If true (the default) then preserve old values in events.<p>- If false then mark non-null values as being NOT_AVAILABLE.|
| gemfire.disablePartitionedRegionBucketAck | Boolean | `false` | See `org.apache.geode.internal.cache.PartitionedRegion.#DISABLE_SECONDARY_BUCKET_ACK`<p>Enable no-ack replication in bucket regions|
| gemfire.disableShutdownHook | Boolean | `false` | See `org.apache.geode.distributed.internal.InternalDistributedSystem#DISABLE_SHUTDOWN_HOOK_PROPERTY`<p>If true then the shutdown hooks of the DistributedSystem, Locator, and Agent are not run on shutdown. This was added for bug 38407.|
| gemfire.disallowMcastDefaults | Boolean | `false` | See `org.apache.geode.distributed.internal.DistributionConfigImpl#checkForDisallowedDefaults`<p>Used by unit tests to make sure the Geode mcast-port has been configured to a non-default value.|
| gemfire.disk.recoverValues | Boolean | `true` | See `org.apache.geode.internal.cache.DiskStoreImpl#RECOVER_VALUE_PROPERTY_NAME`.<p>Whether to get the values from disk to memory on recovery|
| gemfire.enableCpuTime | Boolean | `false` | See `org.apache.geode.internal.stats50.VMStats50`<p>This property causes the per thread stats to also measure cpu time.<p>This property is ignored unless `gemfire.enableThreadStats` is also set to true. See `java.lang.management.ThreadMXBean.setThreadCpuTimeEnabled(boolean)` for more information.|
| gemfire.enableContentionTime | Boolean | `false` | See `org.apache.geode.internal.stats50.VMStats50`<p>This property causes the per thread stats to also measure contention. This property is ignored unless `gemfire.enableThreadStats` is also set to true. See `java.lang.management.ThreadMXBean.setThreadContentionMonitoringEnabled(boolean)` for more information.|
| gemfire.enableThreadStats | Boolean | `false` | See `org.apache.geode.internal.stats50.VMStats50`<p>This property causes the per thread stats to be collected. See `java.lang.management.ThreadMXBean` for more information.|
| gemfire.gateway-queue-no-ack | Boolean | `false` | See `org.apache.geode.internal.cache.wan.serial.SerialGatewaySenderQueue#NO_ACK`.<p>Whether the Gateway queue should be no-ack instead of ack.|
| gemfire.GatewayReceiver.ApplyRetries | Boolean | `false` | See `org.apache.geode.cache.wan.GatewayReceiver#APPLY_RETRIES`<p>If true causes the GatewayReceiver will apply batches it has already received.|
| gemfire.GetInitialImage.CHUNK_PERMITS | Integer | `16` | See `org.apache.geode.internal.cache.InitialImageOperation.#CHUNK_PERMITS`<p>Allowed number of in-flight initial image chunks. This property controls how many requests for GII chunks can be handled simultaneously.|
| gemfire.GetInitialImage.MAX_PARALLEL_GIIS | Integer | `5` | See `org.apache.geode.internal.cache.InitialImageOperation.#MAX_PARALLEL_GIIS `<p>Allowed number of GIIs in parallel. This property controls how many regions can do GII simultaneously. Each replicated region and partitioned region bucket counts against this number.|
| gemfire.haltOnAssertFailure | Boolean | `false` | See `org.apache.geode.internal.Assert#debug`.<p>Causes VM to hang on assertion failure (to allow a debugger to be attached) instead of exiting the process.|
| gemfire.launcher.registerSignalHandlers | Boolean | `false` | See `org.apache.geode.distributed.AbstractLauncher.SIGNAL_HANDLER_REGISTRATION_SYSTEM_PROPERTY`<p>Causes the code used by gfsh to launch a server or locator to install signal handlers using `sun.misc.Signal`.|
| gemfire.locators | String | | See `org.apache.geode.distributed.internal.InternalLocator#startDistributedSystem`.<p>If this property is not found in gemfire.properties, the system property of the same name is used.|
| gemfire.lru.maxSearchEntries | Integer | `-1` | See `org.apache.geode.internal.lang.SystemPropertyHelper#EVICTION_SEARCH_MAX_ENTRIES`.<p>This is the maximum number of "good enough" entries to pass over for eviction before settling on the next acceptable entry.  This prevents excessive cache processing to find a candidate for eviction.|
| gemfire.order-pr-gets | Boolean | `false` | See `org.apache.geode.internal.cache.partitioned.GetMessage#ORDER_PR_GETS`.|
| gemfire.partitionedRegionRetryTimeout | Integer | `60 * 60 * 1000` | See `org.apache.geode.internal.cache.PartitionedRegion#retryTimeout`<p>The maximum milliseconds for retrying operations|
| gemfire.PRQueryProcessor.numThreads | Integer | `1` | See `org.apache.geode.internal.cache.PRQueryProcessor#NUM_THREADS`<p>The number of concurrent threads to use within a single VM to execute queries on a Partitioned Region. If set to 1 (or less) then queries are run sequentially with a single thread.|
| gemfire.SPECIAL_DURABLE | Boolean | `false` | See `org.apache.geode.cache.client.internal.PoolImpl` constructor.<p>See `org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID` constructor.<p>Makes multiple durable subscriptions from same client unique. on java client ensures only a single server proxy is created but will limit the client to a single active client pool at a time|
| gemfire.statsDisabled | Boolean | `false` | See `org.apache.geode.distributed.internal.InternalDistributedSystem#statsDisabled`.|
| gemfire.syncWrites | Boolean | `false` | See `org.apache.geode.internal.cache.Oplog#SYNC_WRITES`.<p>This system property instructs that writes be synchronously written to disk and not to file system. (Use rwd instead of rw - RandomAccessFile property)|
| gemfire.TcpServer.MAX_POOL_SIZE | Integer | `100` | See `org.apache.geode.distributed.internal.InternalLocator#MAX_POOL_SIZE`<p>This property limits the number of threads that the locator will use for processing messages. |
| gemfire.TcpServer.BACKLOG | Integer | Value of `p2p.backlog` property | See `org.apache.geode.distributed.internal.tcpserver.TcpServer#backlogLimit`<p>This property establishes a Locator's TCP/IP "accept" backlog for locator communications.|
| gemfire.validateMessageSize | Boolean | `false` | See `org.apache.geode.distributed.internal.DistributionConfig#VALIDATE`.|
| gemfirePropertyFile | String | `gemfire.properties` | See `org.apache.geode.distributed.DistributedSystem#PROPERTIES_FILE_PROPERTY`.<p>The `PROPERTIES_FILE_PROPERTY` is the system property that can be used to specify the name of the properties file that the connect method will check for when it looks for a properties file. Unless the value specifies the fully qualified path to the file, the file will be searched for, in order, in the following directories:<p>- the current directory<br>- the home directory<br>- the class path<p>Only the first file found will be used. The default value is `gemfire.properties`. However if the `PROPERTIES_FILE_PROPERTY` is set then its value will be used instead of the default. If this value is a relative file system path then the above search is done. If it is an absolute file system path then that file must exist; no search for it is done.|
| gfAgentDebug | Boolean | `false` | See `org.apache.geode.admin.jmx.internal.AgentImpl#checkDebug`.<p>Enables `mx4j` tracing if Agent debugging is enabled.|
| gfAgentPropertyFile | String | `agent.properties` | See `org.apache.geode.admin.jmx.internal.AgentConfigImpl#retrievePropertyFile`.<p>The `propertyFile` is the name of the property file that will be loaded on startup of the Agent. The file will be searched for, in order, in the following directories:<p>-  the current directory<br>-  the home directory<br>-  the class path<p>Only the first file found will be used. The default value of propertyFile is `"agent.properties"`. However if the "gfAgentPropertyFile" system property is set then its value is the value of propertyFile. If this value is a relative file system path then the above search is done. If its an absolute file system path then that file must exist; no search for it is done.|
| jta.VERBOSE | Boolean | `false` | See `org.apache.geode.internal.jta.GlobalTransaction#VERBOSE`.<p>See `org.apache.geode.internal.jta.TransactionManagerImpl#VERBOSE`.|
| jta.defaultTimeout | Integer | `600` | See `org.apache.geode.internal.jta.TransactionManagerImpl#DEFAULT_TRANSACTION_TIMEOUT`.<p>Units are in seconds.|
| mergelogs.TRIM_TIMESTAMPS | Boolean | `false` | See `org.apache.geode.internal.LogFileParser#TRIM_TIMESTAMPS`.|
| org.apache.commons.logging.log | String | `org.apache.commons.logging.impl.SimpleLog` | See `org.apache.geode.admin.jmx.internal.AgentImpl` class init.<p>This is the name of a class.<p>This property is also used by commons-logging.jar (and discussed below).  It is called out here because of its explicit use in the JMX Agent.|
| org.apache.geode.internal.net.ssl.config | String | | See `org.apache.geode.management.internal.ContextAwareSSLRMIClientSocketFactory#createSocket()`<p>In gfsh the ssl config is stored within the `org.apache.geode.internal.net.ssl.config` system property|
| osStatsDisabled | Boolean | `false` | See `org.apache.geode.internal.HostStatSampler#osStatsDisabled`.|
| p2p.backlog | Integer | `1000` (but limited by OS somaxconn setting) | See `org.apache.geode.distributed.internal.tcpserver.TcpServer#p2pBacklog`.<p>backlog is the TCP/IP "accept" backlog configuration parameter for cluster communications|
| p2p.batchBufferSize | Integer | `1024 * 1024` | See `org.apache.geode.internal.tcp.Connection#BATCH_BUFFER_SIZE`.|
| p2p.batchFlushTime | Integer | `50` | See `org.apache.geode.internal.tcp.Connection#BATCH_FLUSH_MS`.<p>Max number of milliseconds until queued messages are sent. Messages are sent when max_bundle_size or max_bundle_timeout has been exceeded (whichever occurs faster)|
| p2p.batchSends | Boolean | `false` | See `org.apache.geode.internal.tcp.Connection#BATCH_SENDS`.|
| p2p.disableSocketWrite | Boolean | `false` | See `org.apache.geode.internal.tcp.Connection#SOCKET_WRITE_DISABLED`.<p>Use to test message prep overhead (no socket write).<br>WARNING: turning this on completely disables distribution of batched sends|
| p2p.disconnectDelay | Integer | `3000` | See `org.apache.geode.distributed.internal.DistributionImpl#destroyMember`.<p>Workaround for bug 34010: small pause inserted before closing reader threads for a departed member.<p>Units are milliseconds.|
| p2p.handshakeTimeoutMs | Integer | `59000` | See `org.apache.geode.internal.tcp.Connection#HANDSHAKE_TIMEOUT_MS`.|
| p2p.joinTimeout | Long | `60000` for a server and `24000` for a locator | See `org.apache.geode.distributed.internal.membership.adapter.ServiceConfig` constructor.<p>Establishes the timeout for waiting for a join response when connecting to the cluster. Units are in milliseconds.|
| p2p.listenerCloseTimeout | Integer | `60000` | See `org.apache.geode.internal.tcp.TCPConduit#LISTENER_CLOSE_TIMEOUT`.<p>Max amount of time (ms) to wait for listener threads to stop|
| gemfire.BufferPool.useHeapBuffers | Boolean | `false` | See `org.apache.geode.internal.net.BufferPool#useDirectBuffers`.<p>Use java "heap" ByteBuffers instead of direct ByteBuffers for NIO operations. Recommended if TLSv1 is being used or if you find you are running out of direct-memory and do not want to increase the amount of direct-memory available to the JVM. Use of heap buffers can reduce performance in some cases.|
| p2p.oldIO | Boolean | `false` | See `org.apache.geode.internal.tcp.TCPConduit#init`.<p>Deprecated. If set, a warning message is logged saying it is currently not supported. This property was used for not using java.nio.|
| p2p.tcpBufferSize | Integer | `32768` | See `org.apache.geode.internal.tcp.TCPConduit#parseProperties`.<p>Any value smaller than `gemfire.SMALL_BUFFER_SIZE` will be set to `gemfire.SMALL_BUFFER_SIZE`.<p>If the gemfire property socket-buffer-size is set to a value other than 32768 then this system property will be ignored. Otherwise this system property sets the p2p socket-buffer-size.<p>Units are are bytes.|
| p2p.test.inhibitAcceptor | Boolean | `false` | See `org.apache.geode.internal.tcp.TCPConduit#startAcceptor`.|
| query.disableIndexes | Boolean | `false` | See org.apache.geode.cache.query.internal.index.IndexUtils#indexesEnabled.|
| remote.call.timeout | Integer | `1800` | See org.apache.geode.internal.admin.remote.AdminWaiters#getWaitTimeout.<p>Units are in seconds.|
| skipConnection | Boolean | `false` | Removed in Geode 1.0 with removal of deprecated Bridge classes.|
| slowStartTimeForTesting | Long | `5000` | See `org.apache.geode.internal.cache.tier.sockets.MessageDispatcher#run()`.<p>Units are in milliseconds.|
| stats.archive-file | String | `stats.gfs` | See `org.apache.geode.internal.statistics.SimpleStatSampler#archiveFileName`. |
| stats.disable | Boolean | `false` | See `org.apache.geode.internal.statistics.LocalStatisticsFactory#statsDisabled`.|
| stats.disk-space-limit | Long | `0` | See `org.apache.geode.internal.statistics.SimpleStatSampler#archiveDiskSpaceLimit`.|
| stats.file-size-limit | Long | `0` | See `org.apache.geode.internal.SimpleStatSampler#archiveFileSizeLimit`.<p>Units are in megabytes|
| stats.name | String | `Thread.currentThread().getName()` | See `org.apache.geode.internal.LocalStatisticsFactory#initName`.|
| stats.sample-rate | Integer | `1000` | See `org.apache.geode.internal.SimpleStatSampler#sampleRate`.<p>Units are in milliseconds.|
| tcpServerPort | Integer | tcp-port property in the distribution config | See `org.apache.geode.distributed.internal.direct.DirectChannel#DirectChannel(MembershipManager, DistributedMembershipListener, DistributionConfig, LogWriter, Properties)`.|


## Properties used by other Jars

ToDo: Following jars were in the previous version if this document, they should be reviewed:
- jta-1_0_1B.jar
- rt.jar
- sunrsasign.jar
- jce.jar
- charsets.jar 
- localedata.jar
- ldapsec.jar
- sunjce_provider.jar

### commons-logging-1.1.1.jar

Miscellaneous: org.apache.commons.logging.log.logName, where
the name is the String passed to the SimpleLog constructor:  This is a String
used to set the logging level of the logger.

See `http://commons.apache.org/proper/commons-logging/apidocs/org/apache/commons/logging/impl/SimpleLog.html` for more information .

| Name | Type | Default value | References |
|---|---|---|---|
| org.apache.commons.logging.log | String | `org.apache.commons.logging.impl.SimpleLog` | See `org.apache.geode.admin.jmx.internal.AgentImpl`<p>See `org.apache.commons.logging.impl.LogFactoryImpl#LOG_PROPERTY`<p>The name of the system property identifying our Log implementation class.|
| org.apache.commons.logging.simplelog.dateTimeFormat | String | `yyyy/MM/dd HH:mm:ss:SSS zzz` | The date and time format to be used in the output messages. The pattern describing the date and time format is the same that is used in `java.text.SimpleDateFormat`.|
| org.apache.commons.logging.simplelog.showdatetime | Boolean | `false` | Set to true if you want the current date and time to be included in output messages|
| org.apache.commons.logging.simplelog.showlogname | Boolean | `false` | Set to true if you want the Log instance name to be included in output messages|
| org.apache.commons.logging.simplelog.showShortLogname | String | `true` | Set to true if you want the last component of the name to be included in output messages. Default to true - otherwise we'll be lost in a flood of messages without knowing who sends them.|

### jsse.jar

See the JSSE documentation for more information.

| Name | Type | References |
|---|---|---|
| cert.provider.x509v1security | String | X509Certificate implementation.|  
| java.protocol.handler.pkgs | String | HTTPS protocol implementation.|  
| java.security.debug | String | Generic dynamic debug tracing support.|  
| javax.net.debug | String | JSSE-specific dynamic debug tracing support.|  
| javax.net.ssl.keyStore | String | See `org.apache.geode.internal.net.SSLConfigurationFactory#JAVAX_KEYSTORE`<p>Default keystore.|  
| javax.net.ssl.keyStoreType | String | See `org.apache.geode.internal.net.SSLConfigurationFactory#JAVAX_KEYSTORE_TYPE`<p>Default keystore type.|  
| javax.net.ssl.keyStorePassword | String | See `org.apache.geode.internal.net.SSLConfigurationFactory#JAVAX_KEYSTORE_PASSWORD`<p>Default keystore password.|  
| javax.net.ssl.trustStore | String | See `org.apache.geode.internal.net.SSLConfigurationFactory#JAVAX_TRUSTSTORE`<p>If the system property `javax.net.ssl.trustStore` is defined, then the `TrustManagerFactory` attempts to find a file using the filename specified by that system property, and uses that file for the KeyStore. If that file does not exist, then a default `TrustManager` using an empty keystore is created.|
| javax.net.ssl.trustStorePassword | String | See `org.apache.geode.internal.net.SSLConfigurationFactory#JAVAX_TRUSTSTORE_PASSWORD`<p>Default truststore password.|  
