/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toSet;
import static org.apache.geode.internal.lang.SystemUtils.getLineSeparator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheStatistics;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.InterestRegistrationEvent;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.PartitionedRegionDistributionException;
import org.apache.geode.cache.PartitionedRegionStorageException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.RegionMembershipListener;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.execute.EmptyRegionFunctionException;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.cache.partition.PartitionNotAvailableException;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.persistence.PartitionOfflineException;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.cache.query.FunctionDomainException;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexCreationException;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.MultiIndexCreationException;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.internal.Bag;
import org.apache.geode.cache.query.internal.CompiledSelect;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.QCompiler;
import org.apache.geode.cache.query.internal.QueryExecutor;
import org.apache.geode.cache.query.internal.ResultsBag;
import org.apache.geode.cache.query.internal.ResultsCollectionWrapper;
import org.apache.geode.cache.query.internal.ResultsSet;
import org.apache.geode.cache.query.internal.index.AbstractIndex;
import org.apache.geode.cache.query.internal.index.IndexCreationData;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.internal.index.IndexUtils;
import org.apache.geode.cache.query.internal.index.PartitionedIndex;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.LockServiceDestroyedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.FunctionExecutionPooledExecutor;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem.DisconnectListener;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ProfileListener;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.locks.DLockRemoteToken;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.distributed.internal.membership.api.MemberDataBuilder;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.BucketAdvisor.ServerBucketProfile;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile;
import org.apache.geode.internal.cache.DestroyPartitionedRegionMessage.DestroyPartitionedRegionResponse;
import org.apache.geode.internal.cache.PutAllPartialResultException.PutAllPartialResult;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.control.MemoryEvent;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.eviction.HeapEvictor;
import org.apache.geode.internal.cache.execute.AbstractExecution;
import org.apache.geode.internal.cache.execute.BucketMovedException;
import org.apache.geode.internal.cache.execute.FunctionExecutionNodePruner;
import org.apache.geode.internal.cache.execute.FunctionRemoteContext;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;
import org.apache.geode.internal.cache.execute.LocalResultCollector;
import org.apache.geode.internal.cache.execute.PartitionedRegionFunctionExecutor;
import org.apache.geode.internal.cache.execute.PartitionedRegionFunctionResultSender;
import org.apache.geode.internal.cache.execute.PartitionedRegionFunctionResultWaiter;
import org.apache.geode.internal.cache.execute.RegionFunctionContextImpl;
import org.apache.geode.internal.cache.execute.ServerToClientFunctionResultSender;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.partitioned.ContainsKeyValueMessage;
import org.apache.geode.internal.cache.partitioned.ContainsKeyValueMessage.ContainsKeyValueResponse;
import org.apache.geode.internal.cache.partitioned.DestroyMessage;
import org.apache.geode.internal.cache.partitioned.DestroyMessage.DestroyResponse;
import org.apache.geode.internal.cache.partitioned.DumpAllPRConfigMessage;
import org.apache.geode.internal.cache.partitioned.DumpB2NRegion;
import org.apache.geode.internal.cache.partitioned.DumpB2NRegion.DumpB2NResponse;
import org.apache.geode.internal.cache.partitioned.DumpBucketsMessage;
import org.apache.geode.internal.cache.partitioned.FetchBulkEntriesMessage;
import org.apache.geode.internal.cache.partitioned.FetchBulkEntriesMessage.FetchBulkEntriesResponse;
import org.apache.geode.internal.cache.partitioned.FetchEntriesMessage;
import org.apache.geode.internal.cache.partitioned.FetchEntriesMessage.FetchEntriesResponse;
import org.apache.geode.internal.cache.partitioned.FetchEntryMessage;
import org.apache.geode.internal.cache.partitioned.FetchEntryMessage.FetchEntryResponse;
import org.apache.geode.internal.cache.partitioned.FetchKeysMessage;
import org.apache.geode.internal.cache.partitioned.FetchKeysMessage.FetchKeysResponse;
import org.apache.geode.internal.cache.partitioned.GetMessage;
import org.apache.geode.internal.cache.partitioned.GetMessage.GetResponse;
import org.apache.geode.internal.cache.partitioned.IdentityRequestMessage;
import org.apache.geode.internal.cache.partitioned.IdentityRequestMessage.IdentityResponse;
import org.apache.geode.internal.cache.partitioned.IdentityUpdateMessage;
import org.apache.geode.internal.cache.partitioned.IdentityUpdateMessage.IdentityUpdateResponse;
import org.apache.geode.internal.cache.partitioned.IndexCreationMsg;
import org.apache.geode.internal.cache.partitioned.InterestEventMessage;
import org.apache.geode.internal.cache.partitioned.InterestEventMessage.InterestEventResponse;
import org.apache.geode.internal.cache.partitioned.InvalidateMessage;
import org.apache.geode.internal.cache.partitioned.InvalidateMessage.InvalidateResponse;
import org.apache.geode.internal.cache.partitioned.PREntriesIterator;
import org.apache.geode.internal.cache.partitioned.PRLocallyDestroyedException;
import org.apache.geode.internal.cache.partitioned.PRSanityCheckMessage;
import org.apache.geode.internal.cache.partitioned.PRUpdateEntryVersionMessage;
import org.apache.geode.internal.cache.partitioned.PRUpdateEntryVersionMessage.UpdateEntryVersionResponse;
import org.apache.geode.internal.cache.partitioned.PartitionMessage.PartitionResponse;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionObserver;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionObserverHolder;
import org.apache.geode.internal.cache.partitioned.PutAllPRMessage;
import org.apache.geode.internal.cache.partitioned.PutMessage;
import org.apache.geode.internal.cache.partitioned.PutMessage.PutResult;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor;
import org.apache.geode.internal.cache.partitioned.RegionAdvisor.PartitionProfile;
import org.apache.geode.internal.cache.partitioned.RemoveAllPRMessage;
import org.apache.geode.internal.cache.partitioned.RemoveIndexesMessage;
import org.apache.geode.internal.cache.partitioned.SizeMessage;
import org.apache.geode.internal.cache.partitioned.SizeMessage.SizeResponse;
import org.apache.geode.internal.cache.partitioned.colocation.ColocationLogger;
import org.apache.geode.internal.cache.partitioned.colocation.ColocationLoggerFactory;
import org.apache.geode.internal.cache.persistence.PRPersistentConfig;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.tier.sockets.command.Get70;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.AbstractGatewaySenderEventProcessor;
import org.apache.geode.internal.cache.wan.AsyncEventQueueConfigurationException;
import org.apache.geode.internal.cache.wan.GatewaySenderConfigurationException;
import org.apache.geode.internal.cache.wan.GatewaySenderException;
import org.apache.geode.internal.cache.wan.parallel.ConcurrentParallelGatewaySenderQueue;
import org.apache.geode.internal.cache.wan.parallel.ParallelGatewaySenderQueue;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.sequencelog.RegionLogger;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.size.Sizeable;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.util.TransformUtils;
import org.apache.geode.internal.util.concurrent.StoppableCountDownLatch;
import org.apache.geode.logging.internal.executors.LoggingExecutors;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * A Region whose total storage is split into chunks of data (partitions) which are copied up to a
 * configurable level (for high availability) and placed on multiple VMs for improved performance
 * and increased storage capacity.
 */
public class PartitionedRegion extends LocalRegion
    implements CacheDistributionAdvisee, QueryExecutor {

  @Immutable
  public static final Random RANDOM =
      new Random(Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "PartitionedRegionRandomSeed",
          NanoTimer.getTime()));

  @MakeNotStatic
  private static final AtomicInteger SERIAL_NUMBER_GENERATOR = new AtomicInteger();

  /**
   * getNetworkHopType byte indicating this was the bucket owner for the last operation
   */
  public static final int NETWORK_HOP_NONE = 0;

  /**
   * getNetworkHopType byte indicating this was not the bucket owner and a message had to be sent to
   * a primary in the same server group
   */
  private static final int NETWORK_HOP_TO_SAME_GROUP = 1;

  /**
   * getNetworkHopType byte indicating this was not the bucket owner and a message had to be sent to
   * a primary in a different server group
   */
  public static final int NETWORK_HOP_TO_DIFFERENT_GROUP = 2;
  public static final String DATA_MOVED_BY_REBALANCE =
      "Transactional data moved, due to rebalancing.";

  private final DiskRegionStats diskRegionStats;

  /**
   * Changes scope of replication to secondary bucket to SCOPE.DISTRIBUTED_NO_ACK
   */
  static final boolean DISABLE_SECONDARY_BUCKET_ACK =
      Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "disablePartitionedRegionBucketAck");

  /**
   * A debug flag used for testing calculation of starting bucket id
   */
  @MutableForTesting
  public static boolean BEFORE_CALCULATE_STARTING_BUCKET_FLAG = false;

  /**
   * Thread specific random number
   */
  private static final ThreadLocal threadRandom = new ThreadLocal() {
    @Override
    protected Object initialValue() {
      int i = RANDOM.nextInt();
      if (i < 0) {
        i = -1 * i;
      }
      return i;
    }
  };

  /**
   * Global Region for storing PR config ( PRName->PRConfig). This region would be used to resolve
   * PR name conflict.*
   */
  private volatile Region<String, PartitionRegionConfig> prRoot;

  /**
   *
   * PartitionedRegionDataStore class takes care of data storage for the PR. This will contain the
   * bucket Regions to store data entries for PR*
   */
  protected PartitionedRegionDataStore dataStore;

  /**
   * The advisor that hold information about this partitioned region
   */
  private final RegionAdvisor distAdvisor;
  private final SenderIdMonitor senderIdMonitor;

  /** Logging mechanism for debugging */
  private static final Logger logger = LogService.getLogger();

  /** cleanup flags * */
  private boolean cleanPRRegistration = false;

  /** Time to wait for for acquiring distributed lock ownership */
  private static final long VM_OWNERSHIP_WAIT_TIME = PRSystemPropertyGetter.parseLong(
      System.getProperty(PartitionedRegionHelper.VM_OWNERSHIP_WAIT_TIME_PROPERTY),
      PartitionedRegionHelper.VM_OWNERSHIP_WAIT_TIME_DEFAULT);

  /**
   * default redundancy level is 0.
   */
  final int redundantCopies;

  /**
   * The miminum amount of redundancy needed for a write operation
   */
  final int minimumWriteRedundancy;

  /**
   * The miminum amount of redundancy needed for a read operation
   */
  final int minimumReadRedundancy;

  /**
   * Ratio of currently allocated memory to maxMemory that triggers rebalance activity.
   */
  static final float rebalanceThreshold = 0.75f;

  /** The maximum memory allocated for this node in Mb */
  final int localMaxMemory;

  /** The maximum milliseconds for retrying operations */
  private final int retryTimeout;

  /**
   * The statistics for this PR
   */
  public final PartitionedRegionStats prStats;

  // private Random random = new Random(System.currentTimeMillis());

  /** Number of initial buckets */
  private final int totalNumberOfBuckets;

  // private static final boolean throwIfNoNodesLeft = true;

  public static final int DEFAULT_RETRY_ITERATIONS = 3;

  /**
   * Flag to indicate if a cache loader is present
   */
  private volatile boolean haveCacheLoader;

  /**
   * Region identifier used for DLocks (Bucket and Region)
   */
  private final String regionIdentifier;

  /**
   * Maps each PR to a prId. This prId will uniquely identify the PR.
   */
  @MakeNotStatic
  private static final PRIdMap prIdToPR = new PRIdMap();

  /**
   * Flag to indicate whether region is closed
   *
   */
  public volatile boolean isClosed = false;

  /**
   * a flag indicating that the PR is destroyed in this VM
   */
  public volatile boolean isLocallyDestroyed = false;

  /**
   * the thread locally destroying this pr. not volatile, so always check isLocallyDestroyed before
   * checking locallyDestroyingThread
   *
   * Concurrency: {@link #isLocallyDestroyed} is volatile
   */
  Thread locallyDestroyingThread;

  // TODO someone please add a javadoc for this
  private volatile boolean hasPartitionedIndex = false;

  /**
   * regionMembershipListener notification requires this to be plugged into a PR's RegionAdvisor
   */
  private final AdvisorListener advisorListener = new AdvisorListener();

  /*
   * Map containing <IndexTask, FutureTask<IndexTask> or Index>. IndexTask represents an index thats
   * completely created or one thats in create phase. This is done in order to avoid synchronization
   * on the indexes.
   */
  private final ConcurrentMap indexes = new ConcurrentHashMap();

  private volatile boolean recoveredFromDisk;

  public static final int RUNNING_MODE = -1;
  public static final int PRIMARY_BUCKETS_LOCKED = 1;
  public static final int DISK_STORE_FLUSHED = 2;
  public static final int OFFLINE_EQUAL_PERSISTED = 3;

  private volatile int shutDownAllStatus = RUNNING_MODE;

  private final long birthTime = System.currentTimeMillis();

  public void setShutDownAllStatus(int newStatus) {
    this.shutDownAllStatus = newStatus;
  }

  private final PartitionedRegion colocatedWithRegion;

  private final ColocationLoggerFactory colocationLoggerFactory;

  private final AtomicReference<ColocationLogger> missingColocatedRegionLogger =
      new AtomicReference<>();

  private List<BucketRegion> sortedBuckets;

  private ScheduledExecutorService bucketSorter;

  private final ConcurrentMap<String, Integer[]> partitionsMap = new ConcurrentHashMap<>();

  public ConcurrentMap<String, Integer[]> getPartitionsMap() {
    return this.partitionsMap;
  }

  /**
   * for wan shadowPR
   */
  private boolean enableConflation;

  private final Object indexLock = new Object();

  private final Set<ColocationListener> colocationListeners = ConcurrentHashMap.newKeySet();

  public void addColocationListener(ColocationListener colocationListener) {
    if (colocationListener != null) {
      colocationListeners.add(colocationListener);
    }
  }

  public void removeColocationListener(ColocationListener colocationListener) {
    colocationListeners.remove(colocationListener);
  }

  ScheduledExecutorService getBucketSorter() {
    return bucketSorter;
  }

  static PRIdMap getPrIdToPR() {
    return prIdToPR;
  }

  /**
   * Byte 0 = no NWHOP Byte 1 = NWHOP to servers in same server-grp Byte 2 = NWHOP tp servers in
   * other server-grp
   */
  private static final ThreadLocal<Byte> networkHopType = new ThreadLocal<Byte>() {
    @Override
    protected Byte initialValue() {
      return (byte) NETWORK_HOP_NONE;
    }
  };

  public void clearNetworkHopData() {
    networkHopType.remove();
    metadataVersion.remove();
  }

  private void setNetworkHopType(Byte value) {
    networkHopType.set(value);
  }

  /**
   * If the last operation in the current thread required a one-hop to another server who held the
   * primary bucket for the operation then this will return something other than NETWORK_HOP_NONE.
   * <p>
   * see NETWORK_HOP_NONE, NETWORK_HOP_TO_SAME_GROUP and NETWORK_HOP_TO_DIFFERENT_GROUP
   */
  public byte getNetworkHopType() {
    return networkHopType.get();
  }

  private static final ThreadLocal<Byte> metadataVersion = new ThreadLocal<Byte>() {
    @Override
    protected Byte initialValue() {
      return ClientMetadataService.INITIAL_VERSION;
    }
  };

  private void setMetadataVersion(Byte value) {
    metadataVersion.set(value);
  }

  public byte getMetadataVersion() {
    return metadataVersion.get();
  }

  /**
   * Returns the EvictionController for this PR. This is needed to find the single instance of
   * EvictionController created early for a PR when it is recovered from disk. This fixes bug 41938
   */
  public EvictionController getPREvictionControllerFromDiskInitialization() {
    EvictionController result = null;
    if (getDiskStore() != null) {
      result = getDiskStore().getExistingPREvictionContoller(this);
    }
    return result;
  }

  @Override
  public boolean remove(Object key, Object value, Object callbackArg) {
    final long startTime = prStats.getTime();
    try {
      return super.remove(key, value, callbackArg);
    } finally {
      this.prStats.endDestroy(startTime);
    }
  }

  public PartitionListener[] getPartitionListeners() {
    return this.partitionListeners;
  }

  /**
   * Return canonical representation for a bucket (for logging)
   *
   * @param bucketId the bucket
   * @return a String representing this PR and the bucket
   */
  public String bucketStringForLogs(int bucketId) {
    return getPRId() + BUCKET_ID_SEPARATOR + bucketId;
  }

  /** Separator between PRId and bucketId for creating bucketString */
  public static final String BUCKET_ID_SEPARATOR = ":";

  /**
   * Clear the prIdMap, typically used when disconnecting from the distributed system or clearing
   * the cache
   */
  public static void clearPRIdMap() {
    synchronized (prIdToPR) {
      prIdToPR.clear();
    }
  }

  @Immutable
  private static final DisconnectListener dsPRIdCleanUpListener = new DisconnectListener() {
    @Override
    public String toString() {
      return "Shutdown listener for PartitionedRegion";
    }

    @Override
    public void onDisconnect(InternalDistributedSystem sys) {
      clearPRIdMap();
    }
  };

  PartitionedRegionRedundancyTracker getRedundancyTracker() {
    return redundancyTracker;
  }

  public void computeWithPrimaryLocked(Object key, Runnable r) throws PrimaryBucketLockException {
    int bucketId = PartitionedRegionHelper.getHashKey(this, null, key, null, null);

    BucketRegion br;
    try {
      br = this.dataStore.getInitializedBucketForId(key, bucketId);
    } catch (ForceReattemptException e) {
      throw new BucketMovedException(e);
    }

    try {
      br.doLockForPrimary(false);
    } catch (PrimaryBucketException e) {
      throw new PrimaryBucketLockException("retry since primary lock failed: " + e);
    }

    try {
      r.run();
    } finally {
      br.doUnlockForPrimary();
    }
  }


  public static class PRIdMap extends HashMap {
    private static final long serialVersionUID = 3667357372967498179L;
    public static final String DESTROYED = "Partitioned Region Destroyed";

    static final String LOCALLY_DESTROYED = "Partitioned Region Is Locally Destroyed";

    static final String FAILED_REGISTRATION = "Partitioned Region's Registration Failed";

    public static final String NO_PATH_FOUND = "NoPathFound";

    private volatile boolean cleared = true;

    @Override
    public Object get(Object key) {
      throw new UnsupportedOperationException(
          "PRIdMap.get not supported - use getRegion instead");
    }

    public Object getRegion(Object key) throws PRLocallyDestroyedException {
      Assert.assertTrue(key instanceof Integer);

      Object o = super.get(key);
      if (o == DESTROYED) {
        throw new RegionDestroyedException(
            String.format("Region for prId= %s is destroyed",
                key),
            NO_PATH_FOUND);
      }
      if (o == LOCALLY_DESTROYED) {
        throw new PRLocallyDestroyedException(
            String.format("Region with prId= %s is locally destroyed on this node",
                key));
      }
      if (o == FAILED_REGISTRATION) {
        throw new PRLocallyDestroyedException(
            String.format("Region with prId= %s failed initialization on this node",
                key));
      }
      return o;
    }

    @Override
    public Object remove(final Object key) {
      return put(key, DESTROYED, true);
    }

    @Override
    public Object put(final Object key, final Object value) {
      return put(key, value, true);
    }

    public Object put(final Object key, final Object value, boolean sendIdentityRequestMessage) {
      if (cleared) {
        cleared = false;
      }

      if (key == null) {
        throw new NullPointerException(
            "null key not allowed for prIdToPR Map");
      }
      if (value == null) {
        throw new NullPointerException(
            "null value not allowed for prIdToPR Map");
      }
      Assert.assertTrue(key instanceof Integer);
      if (sendIdentityRequestMessage)
        IdentityRequestMessage.setLatestId((Integer) key);
      if ((super.get(key) == DESTROYED) && (value instanceof PartitionedRegion)) {
        throw new PartitionedRegionException(
            String.format("Can NOT reuse old Partitioned Region Id= %s",
                key));
      }
      return super.put(key, value);
    }

    @Override
    public void clear() {
      this.cleared = true;
      super.clear();
    }

    public synchronized String dump() {
      StringBuilder sb = new StringBuilder("prIdToPR Map@");
      sb.append(System.identityHashCode(prIdToPR)).append(':').append(getLineSeparator());
      Map.Entry mapEntry;
      for (Iterator iterator = prIdToPR.entrySet().iterator(); iterator.hasNext();) {
        mapEntry = (Map.Entry) iterator.next();
        sb.append(mapEntry.getKey()).append("=>").append(mapEntry.getValue());
        if (iterator.hasNext()) {
          sb.append(getLineSeparator());
        }
      }
      return sb.toString();
    }
  }

  private int partitionedRegionId = -3;

  /** Node description */
  private final Node node;

  /** Helper Object for redundancy Management of PartitionedRegion */
  private final PRHARedundancyProvider redundancyProvider;

  /**
   * flag saying whether this VM needs cache operation notifications from other members
   */
  private boolean requiresNotification;

  /**
   * Latch that signals when the Bucket meta-data is ready to receive updates
   */
  private final StoppableCountDownLatch initializationLatchAfterBucketIntialization;

  public static final String RETRY_TIMEOUT_PROPERTY =
      GeodeGlossary.GEMFIRE_PREFIX + "partitionedRegionRetryTimeout";

  private final PartitionRegionConfigValidator validator;

  final List<FixedPartitionAttributesImpl> fixedPAttrs;

  private byte fixedPASet;

  private final List<PartitionedRegion> colocatedByList =
      new CopyOnWriteArrayList<PartitionedRegion>();

  private final PartitionListener[] partitionListeners;

  private boolean isShadowPR = false;

  private AbstractGatewaySender parallelGatewaySender = null;

  private final PartitionedRegionRedundancyTracker redundancyTracker;

  private boolean regionCreationNotified;

  /**
   * Constructor for a PartitionedRegion. This has an accessor (Region API) functionality and
   * contains a datastore for actual storage. An accessor can act as a local cache by having a local
   * storage enabled. A PartitionedRegion can be created by a factory method of RegionFactory.java
   * and also by invoking Cache.createRegion(). (Cache.xml etc to be added)
   */
  public PartitionedRegion(String regionName,
      RegionAttributes regionAttributes,
      LocalRegion parentRegion,
      InternalCache cache,
      InternalRegionArguments internalRegionArgs,
      StatisticsClock statisticsClock,
      ColocationLoggerFactory colocationLoggerFactory) {
    super(regionName, regionAttributes, parentRegion, cache, internalRegionArgs,
        new PartitionedRegionDataView(), statisticsClock);

    this.colocationLoggerFactory = colocationLoggerFactory;
    this.node = initializeNode();
    this.prStats = new PartitionedRegionStats(cache.getDistributedSystem(), getFullPath(),
        statisticsClock);
    this.regionIdentifier = getFullPath().replace(Region.SEPARATOR_CHAR, '#');

    if (logger.isDebugEnabled()) {
      logger.debug("Constructing Partitioned Region {}", regionName);
    }

    // By adding this disconnect listener we ensure that the pridmap is cleaned
    // up upon
    // distributed system disconnect even this (or other) PRs are destroyed
    // (which prevents pridmap cleanup).
    cache.getInternalDistributedSystem().addDisconnectListener(dsPRIdCleanUpListener);

    this.partitionAttributes = regionAttributes.getPartitionAttributes();
    this.localMaxMemory = this.partitionAttributes.getLocalMaxMemory();
    this.retryTimeout = Integer.getInteger(RETRY_TIMEOUT_PROPERTY,
        PartitionedRegionHelper.DEFAULT_TOTAL_WAIT_RETRY_ITERATION);
    this.totalNumberOfBuckets = this.partitionAttributes.getTotalNumBuckets();
    this.prStats.incTotalNumBuckets(this.totalNumberOfBuckets);

    // Warning: potential early escape of instance
    this.distAdvisor = RegionAdvisor.createRegionAdvisor(this);
    senderIdMonitor = createSenderIdMonitor();
    // Warning: potential early escape of instance
    this.redundancyProvider = new PRHARedundancyProvider(this, cache.getInternalResourceManager());

    // localCacheEnabled = ra.getPartitionAttributes().isLocalCacheEnabled();
    // This is to make sure that local-cache get and put works properly.
    // getScope is overridden to return the correct scope.
    // this.scope = Scope.LOCAL;
    this.redundantCopies = regionAttributes.getPartitionAttributes().getRedundantCopies();
    this.redundancyTracker = new PartitionedRegionRedundancyTracker(this.totalNumberOfBuckets,
        this.redundantCopies, this.prStats, getFullPath());
    this.prStats.setConfiguredRedundantCopies(
        regionAttributes.getPartitionAttributes().getRedundantCopies());
    this.prStats.setLocalMaxMemory(
        regionAttributes.getPartitionAttributes().getLocalMaxMemory() * 1024L * 1024);

    // No redundancy required for writes
    this.minimumWriteRedundancy = Integer.getInteger(
        GeodeGlossary.GEMFIRE_PREFIX + "mimimumPartitionedRegionWriteRedundancy", 0);

    // No redundancy required for reads
    this.minimumReadRedundancy = Integer.getInteger(
        GeodeGlossary.GEMFIRE_PREFIX + "mimimumPartitionedRegionReadRedundancy", 0);

    this.haveCacheLoader = regionAttributes.getCacheLoader() != null;

    this.initializationLatchAfterBucketIntialization =
        new StoppableCountDownLatch(this.getCancelCriterion(), 1);

    this.validator = new PartitionRegionConfigValidator(this);
    this.partitionListeners = this.partitionAttributes.getPartitionListeners();

    this.colocatedWithRegion = ColocationHelper.getColocatedRegion(this);

    if (colocatedWithRegion != null) {
      colocatedWithRegion.getColocatedByList().add(this);
    }

    if (colocatedWithRegion != null && !internalRegionArgs.isUsedForParallelGatewaySenderQueue()) {
      // In a colocation chain, the child region inherits the fixed partition attributes from parent
      // region.
      this.fixedPAttrs = colocatedWithRegion.getFixedPartitionAttributesImpl();
      this.fixedPASet = colocatedWithRegion.fixedPASet;
    } else {
      this.fixedPAttrs = this.partitionAttributes.getFixedPartitionAttributes();
      this.fixedPASet = 0;
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Partitioned Region {} constructed {}", regionName,
          (this.haveCacheLoader ? "with a cache loader" : ""));
    }
    if (this.getEvictionAttributes() != null
        && this.getEvictionAttributes().getAlgorithm().isLRUHeap()) {
      this.sortedBuckets = new ArrayList<BucketRegion>();
      this.bucketSorter = LoggingExecutors.newScheduledThreadPool(1, "BucketSorterThread");
    }
    // If eviction is on, Create an instance of PartitionedRegionLRUStatistics
    if ((this.getEvictionAttributes() != null
        && !this.getEvictionAttributes().getAlgorithm().isNone()
        && this.getEvictionAttributes().getAction().isOverflowToDisk())
        || this.getDataPolicy().withPersistence()) {
      StatisticsFactory sf = this.getCache().getDistributedSystem();
      this.diskRegionStats = new DiskRegionStats(sf, getFullPath());
    } else {
      this.diskRegionStats = null;
    }
    if (internalRegionArgs.isUsedForParallelGatewaySenderQueue()) {
      this.isShadowPR = true;
      this.parallelGatewaySender = internalRegionArgs.getParallelGatewaySender();
    }
    this.regionCreationNotified = false;

    /*
     * Start persistent profile logging if we are a persistent region.
     */
    if (getDataPolicy().withPersistence()) {
      startPersistenceProfileLogging();
    }
  }

  /**
   * Monitors when other members that participate in this persistent region are removed and creates
   * a log entry marking the event.
   */
  private void startPersistenceProfileLogging() {
    this.distAdvisor.addProfileChangeListener(new ProfileListener() {
      @Override
      public void profileCreated(Profile profile) {}

      @Override
      public void profileUpdated(Profile profile) {}

      @Override
      public void profileRemoved(Profile profile, boolean destroyed) {
        /*
         * Don't bother logging membership activity if our region isn't ready.
         */
        if (isInitialized()) {
          CacheProfile cacheProfile = (CacheProfile) profile;
          Set<String> onlineMembers = new HashSet<>();

          TransformUtils.transform(
              PartitionedRegion.this.distAdvisor.advisePersistentMembers().values(), onlineMembers,
              TransformUtils.persistentMemberIdToLogEntryTransformer);

          logger
              .info(
                  "The following persistent member has gone offline for region {}:{}.  Remaining participating members for the region include: {}",
                  new Object[] {PartitionedRegion.this.getName(),
                      TransformUtils.persistentMemberIdToLogEntryTransformer
                          .transform(cacheProfile.persistentID),
                      onlineMembers});
        }
      }
    });
  }

  public boolean isShadowPR() {
    return isShadowPR;
  }

  public AbstractGatewaySender getParallelGatewaySender() {
    return parallelGatewaySender;
  }

  public Set<String> getParallelGatewaySenderIds() {
    Set<String> regionGatewaySenderIds = this.getAllGatewaySenderIds();
    if (regionGatewaySenderIds.isEmpty()) {
      return Collections.emptySet();
    }
    Set<GatewaySender> cacheGatewaySenders = getCache().getAllGatewaySenders();
    Set<String> parallelGatewaySenderIds = new HashSet<String>();
    for (GatewaySender sender : cacheGatewaySenders) {
      if (regionGatewaySenderIds.contains(sender.getId()) && sender.isParallel()) {
        parallelGatewaySenderIds.add(sender.getId());
      }
    }
    return parallelGatewaySenderIds;
  }

  public List<PartitionedRegion> getColocatedByList() {
    return this.colocatedByList;
  }

  public boolean isColocatedBy() {
    return !this.colocatedByList.isEmpty();
  }

  private void createAndValidatePersistentConfig() {
    DiskStoreImpl diskStore = this.getDiskStore();
    if (this.getDataPolicy().withPersistence() && !this.getConcurrencyChecksEnabled()
        && supportsConcurrencyChecks()) {
      logger.info(
          "Turning on concurrency checks for region: {} since it has been configured to persist data to disk.",
          this.getFullPath());
      this.setConcurrencyChecksEnabled(true);
    }
    if (diskStore != null && this.getDataPolicy().withPersistence()) {
      String colocatedWith = colocatedWithRegion == null ? "" : colocatedWithRegion.getFullPath();
      PRPersistentConfig config = diskStore.getPersistentPRConfig(this.getFullPath());
      if (config != null) {
        if (config.getTotalNumBuckets() != this.getTotalNumberOfBuckets()) {
          Object[] prms = new Object[] {this.getFullPath(), this.getTotalNumberOfBuckets(),
              config.getTotalNumBuckets()};
          throw new IllegalStateException(
              String.format(
                  "For partition region %s,total-num-buckets %s should not be changed. Previous configured number is %s.",
                  prms));
        }
        // Make sure we don't change to be colocated with a different region
        // We also can't change from colocated to not colocated without writing
        // a record to disk, so we won't allow that right now either.
        if (!colocatedWith.equals(config.getColocatedWith())) {
          Object[] prms =
              new Object[] {this.getFullPath(), colocatedWith, config.getColocatedWith()};
          DiskAccessException dae = new DiskAccessException(
              String.format(
                  "A DiskAccessException has occurred while writing to the disk for region %s. The region will be closed.",
                  this.getFullPath()),
              null, diskStore);
          diskStore.handleDiskAccessException(dae);
          throw new IllegalStateException(
              String.format(
                  "For partition region %s, cannot change colocated-with to \"%s\" because there is persistent data with different colocation. Previous configured value is \"%s\".",
                  prms));
        }
      } else {

        config = new PRPersistentConfig(this.getTotalNumberOfBuckets(), colocatedWith);
        diskStore.addPersistentPR(this.getFullPath(), config);
        // Fix for support issue 7870 - the parent region needs to be able
        // to discover that there is a persistent colocated child region. So
        // if this is a child region, persist its config to the parent disk store
        // as well.
        if (colocatedWithRegion != null && colocatedWithRegion.getDiskStore() != null
            && colocatedWithRegion.getDiskStore() != diskStore) {
          colocatedWithRegion.getDiskStore().addPersistentPR(this.getFullPath(), config);
        }
      }

    }
  }

  /**
   * Initializes the PartitionedRegion meta data, adding this Node and starting the service on this
   * node (if not already started). Made this synchronized for bug 41982
   *
   * @return true if initialize was done; false if not because it was destroyed
   */
  private synchronized boolean initPRInternals(InternalRegionArguments internalRegionArgs) {

    if (this.isLocallyDestroyed) {
      // don't initialize if we are already destroyed for bug 41982
      return false;
    }
    /* Initialize the PartitionRegion */
    if (cache.isCacheAtShutdownAll()) {
      throw cache.getCacheClosedException("Cache is shutting down");
    }
    validator.validateColocation();

    // Do this after the validation, to avoid creating a persistent config
    // for an invalid PR.
    createAndValidatePersistentConfig();
    initializePartitionedRegion();

    // If localMaxMemory is set to 0, do not initialize Data Store.
    final boolean storesData = this.localMaxMemory > 0;
    if (storesData) {
      initializeDataStore(this.getAttributes());
    }

    // register this PartitionedRegion, Create a PartitionRegionConfig and bind
    // it into the allPartitionedRegion system wide Region.
    // IMPORTANT: do this before advising peers that we have this region
    registerPartitionedRegion(storesData);

    getRegionAdvisor().initializeRegionAdvisor(); // must be BEFORE initializeRegion call
    getRegionAdvisor().addMembershipListener(this.advisorListener); // fix for bug 38719

    // 3rd part of eviction attributes validation, after eviction attributes
    // have potentially been published (by the first VM) but before buckets are created
    validator.validateEvictionAttributesAgainstLocalMaxMemory();
    validator.validateFixedPartitionAttributes();

    // Register with the other Nodes that have this region defined, this
    // allows for an Advisor profile exchange, also notifies the Admin
    // callbacks that this Region is created.
    try {
      new CreateRegionProcessor(this).initializeRegion();
    } catch (IllegalStateException e) {
      // If this is a PARTITION_PROXY then retry region creation
      // after toggling the concurrencyChecksEnabled flag. This is
      // required because for persistent regions, we enforce concurrencyChecks
      if (!this.isDataStore() && supportsConcurrencyChecks()) {
        this.setConcurrencyChecksEnabled(!this.getConcurrencyChecksEnabled());
        new CreateRegionProcessor(this).initializeRegion();
      } else {
        throw e;
      }
    }

    if (!this.isDestroyed && !this.isLocallyDestroyed) {
      // Register at this point so that other members are known
      this.cache.getInternalResourceManager().addResourceListener(ResourceType.MEMORY, this);
    }

    // Create OQL indexes before starting GII.
    createOQLIndexes(internalRegionArgs);

    // if any other services are dependent on notifications from this region,
    // then we need to make sure that in-process ops are distributed before
    // releasing the GII latches
    if (this.isAllEvents()) {
      StateFlushOperation sfo = new StateFlushOperation(getDistributionManager());
      try {
        sfo.flush(this.distAdvisor.adviseAllPRNodes(), getDistributionManager().getId(),
            OperationExecutors.HIGH_PRIORITY_EXECUTOR, false);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        getCancelCriterion().checkCancelInProgress(ie);
      }
    }

    releaseBeforeGetInitialImageLatch(); // moved to this spot for bug 36671

    // requires prid assignment mthomas 4/3/2007
    getRegionAdvisor().processProfilesQueuedDuringInitialization();

    releaseAfterBucketMetadataSetupLatch();

    try {
      if (storesData) {
        if (this.redundancyProvider.recoverPersistentBuckets()) {
          // Mark members as recovered from disk recursively, starting
          // with the leader region.
          PartitionedRegion leaderRegion = ColocationHelper.getLeaderRegion(this);
          markRecoveredRecursively(leaderRegion);
        }
      }
    } catch (RegionDestroyedException rde) {
      // Do nothing.
      if (logger.isDebugEnabled()) {
        logger.debug("initPRInternals: failed due to exception", rde);
      }
    }

    releaseAfterGetInitialImageLatch();

    try {
      if (storesData) {
        this.redundancyProvider.scheduleCreateMissingBuckets();
        this.redundancyProvider.startRedundancyRecovery();
      }
    } catch (RegionDestroyedException rde) {
      // Do nothing.
      if (logger.isDebugEnabled()) {
        logger.debug("initPRInternals: failed due to exception", rde);
      }
    }

    return true;
  }

  private void markRecoveredRecursively(PartitionedRegion region) {
    region.setRecoveredFromDisk();
    for (PartitionedRegion colocatedRegion : ColocationHelper.getColocatedChildRegions(region)) {
      markRecoveredRecursively(colocatedRegion);
    }
  }

  @Override
  public void postCreateRegion() {
    super.postCreateRegion();
    CacheListener[] listeners = fetchCacheListenersField();
    if (listeners != null && listeners.length > 0) {
      Set others = getRegionAdvisor().adviseGeneric();
      for (int i = 0; i < listeners.length; i++) {
        if (listeners[i] instanceof RegionMembershipListener) {
          RegionMembershipListener rml = (RegionMembershipListener) listeners[i];
          try {
            DistributedMember[] otherDms = new DistributedMember[others.size()];
            others.toArray(otherDms);
            rml.initialMembers(this, otherDms);
          } catch (VirtualMachineError err) {
            SystemFailure.initiateFailure(err);
            // If this ever returns, rethrow the error. We're poisoned
            // now, so don't let this thread continue.
            throw err;
          } catch (Throwable t) {
            // Whenever you catch Error or Throwable, you must also
            // catch VirtualMachineError (see above). However, there is
            // _still_ a possibility that you are dealing with a cascading
            // error condition, so you also need to check to see if the JVM
            // is still usable:
            SystemFailure.checkFailure();
            logger.error("Exception occurred in RegionMembershipListener",
                t);
          }
        }
      }
    }

    PartitionListener[] partitionListeners = this.getPartitionListeners();
    if (partitionListeners != null && partitionListeners.length != 0) {
      for (int i = 0; i < partitionListeners.length; i++) {
        PartitionListener listener = partitionListeners[i];
        if (listener != null) {
          listener.afterRegionCreate(this);
        }
      }
    }

    Set<String> allGatewaySenderIds = getAllGatewaySenderIds();
    if (!allGatewaySenderIds.isEmpty()) {
      for (GatewaySender sender : cache.getAllGatewaySenders()) {
        if (sender.isParallel() && allGatewaySenderIds.contains(sender.getId())) {
          /*
           * get the ParallelGatewaySender to create the colocated partitioned region for this
           * region.
           */
          if (sender.isRunning()) {
            AbstractGatewaySender senderImpl = (AbstractGatewaySender) sender;
            ((ConcurrentParallelGatewaySenderQueue) senderImpl.getQueues()
                .toArray(new RegionQueue[1])[0]).addShadowPartitionedRegionForUserPR(this);
          }
        }
      }
    }
  }

  /*
   * Initializes the PartitionedRegion. OVERRIDES
   */
  @Override
  public void initialize(InputStream snapshotInputStream, InternalDistributedMember imageTarget,
      InternalRegionArguments internalRegionArgs) throws TimeoutException, ClassNotFoundException {
    if (logger.isDebugEnabled()) {
      logger.debug("PartitionedRegion#initialize {}", getName());
    }
    RegionLogger.logCreate(getName(), getDistributionManager().getDistributionManagerId());

    this.requiresNotification = this.cache.requiresNotificationFromPR(this);
    initPRInternals(internalRegionArgs);

    if (logger.isDebugEnabled()) {
      logger.debug("PartitionRegion#initialize: finished with {}", this);
    }
    this.cache.addPartitionedRegion(this);

  }

  @Override
  protected RegionVersionVector createRegionVersionVector() {
    return null;
  }

  /**
   * Initializes the Node for this Map.
   */
  private Node initializeNode() {
    return new Node(getDistributionManager().getId(), SERIAL_NUMBER_GENERATOR.getAndIncrement());
  }

  /**
   * receive notification that a cache server or wan gateway has been created that requires
   * notification of cache events from this region
   */
  public void cacheRequiresNotification() {
    if (!this.requiresNotification && !(this.isClosed || this.isLocallyDestroyed)) {
      // tell others of the change in status
      this.requiresNotification = true;
      new UpdateAttributesProcessor(this).distribute(false);
    }
  }

  @Override
  void distributeUpdatedProfileOnSenderCreation() {
    if (!(this.isClosed || this.isLocallyDestroyed)) {
      // tell others of the change in status
      requiresNotification = true;
      new UpdateAttributesProcessor(this).distribute(false);
    }
  }

  @Override
  public void addGatewaySenderId(String gatewaySenderId) {
    super.addGatewaySenderId(gatewaySenderId);
    new UpdateAttributesProcessor(this).distribute();
    GatewaySender sender = getCache().getGatewaySender(gatewaySenderId);
    if (sender != null && !sender.isParallel()) {
      distributeUpdatedProfileOnSenderCreation();
    }
    if (sender != null && sender.isParallel() && sender.isRunning()) {
      AbstractGatewaySender senderImpl = (AbstractGatewaySender) sender;
      ((ConcurrentParallelGatewaySenderQueue) senderImpl.getQueues().toArray(new RegionQueue[1])[0])
          .addShadowPartitionedRegionForUserPR(this);
    }
    updateSenderIdMonitor();
  }

  public void updatePRConfigWithNewSetOfAsynchronousEventDispatchers(
      Set<String> asynchronousEventDispatchers) {
    updatePartitionRegionConfig(prConfig -> {
      prConfig.setGatewaySenderIds(asynchronousEventDispatchers);
    });
  }

  public void updatePRConfigWithNewGatewaySenderAfterAssigningBuckets(String aeqId) {
    PartitionRegionHelper.assignBucketsToPartitions(this);
    updatePartitionRegionConfig(prConfig -> {
      Set<String> newGateWayIds;
      if (prConfig.getGatewaySenderIds() != null) {
        newGateWayIds = new HashSet<>(prConfig.getGatewaySenderIds());
      } else {
        newGateWayIds = new HashSet<>();
      }
      newGateWayIds.add(aeqId);
      prConfig.setGatewaySenderIds(newGateWayIds);
    });
  }

  @Override
  public void removeGatewaySenderId(String gatewaySenderId) {
    super.removeGatewaySenderId(gatewaySenderId);
    new UpdateAttributesProcessor(this).distribute();
    updateSenderIdMonitor();
  }

  @Override
  public void addAsyncEventQueueId(String asyncEventQueueId) {
    addAsyncEventQueueId(asyncEventQueueId, false);
  }

  @Override
  public void addAsyncEventQueueId(String asyncEventQueueId, boolean isInternal) {
    super.addAsyncEventQueueId(asyncEventQueueId, isInternal);
    GatewaySender sender = getCache()
        .getGatewaySender(AsyncEventQueueImpl.getSenderIdFromAsyncEventQueueId(asyncEventQueueId));
    if (sender != null && !sender.isParallel()) {
      distributeUpdatedProfileOnSenderCreation();
    }
    if (sender != null && sender.isParallel() && sender.isRunning()) {
      AbstractGatewaySender senderImpl = (AbstractGatewaySender) sender;
      ((ConcurrentParallelGatewaySenderQueue) senderImpl.getQueues().toArray(new RegionQueue[1])[0])
          .addShadowPartitionedRegionForUserPR(this);
    }
    updateSenderIdMonitor();
  }

  @Override
  public void removeAsyncEventQueueId(String asyncEventQueueId) {
    super.removeAsyncEventQueueId(asyncEventQueueId);
    new UpdateAttributesProcessor(this).distribute();
    updateSenderIdMonitor();
  }

  private SenderIdMonitor createSenderIdMonitor() {
    return SenderIdMonitor.createSenderIdMonitor(this, this.distAdvisor);
  }

  private void updateSenderIdMonitor() {
    this.senderIdMonitor.update();
  }

  @Override
  void checkSameSenderIdsAvailableOnAllNodes() {
    this.senderIdMonitor.checkSenderIds();
  }

  /**
   * Initializes the PartitionedRegion - create the Global regions for storing the
   * PartitiotnedRegion configs.
   */
  private void initializePartitionedRegion() {
    this.prRoot = PartitionedRegionHelper.getPRRoot(getCache());
  }

  // Used for testing purposes
  Region<String, PartitionRegionConfig> getPRRoot() {
    return prRoot;
  }

  @Override
  public void remoteRegionInitialized(CacheProfile profile) {
    if (isInitialized() && hasListener()) {
      Object callback = DistributedRegion.TEST_HOOK_ADD_PROFILE ? profile : null;
      RegionEventImpl event = new RegionEventImpl(PartitionedRegion.this, Operation.REGION_CREATE,
          callback, true, profile.peerMemberId);
      dispatchListenerEvent(EnumListenerEvent.AFTER_REMOTE_REGION_CREATE, event);
    }
  }

  /**
   * This method initializes the partitionedRegionDataStore for this PR.
   *
   * @param ra Region attributes
   */
  private void initializeDataStore(RegionAttributes ra) {

    this.dataStore =
        PartitionedRegionDataStore.createDataStore(cache, this, ra.getPartitionAttributes(),
            getStatisticsClock());
  }

  protected DistributedLockService getPartitionedRegionLockService() {
    return getGemFireCache().getPartitionedRegionLockService();
  }

  /**
   * Register this PartitionedRegion by: 1) Create a PartitionRegionConfig and 2) Bind it into the
   * allPartitionedRegion system wide Region.
   *
   * @param storesData which indicates whether the instance in this cache stores data, effecting the
   *        Nodes PRType
   *
   * @see Node#setPRType(int)
   */
  private void registerPartitionedRegion(boolean storesData) {
    // Register this ParitionedRegion. First check if the ParitionedRegion
    // entry already exists globally.
    PartitionRegionConfig prConfig = null;
    PartitionAttributes prAttribs = getAttributes().getPartitionAttributes();
    if (storesData) {
      if (this.fixedPAttrs != null) {
        this.node.setPRType(Node.FIXED_PR_DATASTORE);
      } else {
        this.node.setPRType(Node.ACCESSOR_DATASTORE);
      }
      this.node.setPersistence(getAttributes().getDataPolicy() == DataPolicy.PERSISTENT_PARTITION);
      this.node.setLoaderAndWriter(getAttributes().getCacheLoader(),
          getAttributes().getCacheWriter());
    } else {
      if (this.fixedPAttrs != null) {
        this.node.setPRType(Node.FIXED_PR_ACCESSOR);
      } else {
        this.node.setPRType(Node.ACCESSOR);
      }
    }
    final RegionLock rl = getRegionLock();
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("registerPartitionedRegion: obtaining lock");
      }
      rl.lock();
      checkReadiness();

      prConfig = getPRRoot().get(getRegionIdentifier());

      if (prConfig == null) {
        validateParallelAsynchronousEventDispatcherIds();
        this.partitionedRegionId = generatePRId(getSystem());
        prConfig = new PartitionRegionConfig(this.partitionedRegionId, this.getFullPath(),
            prAttribs, this.getScope(), getAttributes().getEvictionAttributes(),
            getAttributes().getRegionIdleTimeout(), getAttributes().getRegionTimeToLive(),
            getAttributes().getEntryIdleTimeout(), getAttributes().getEntryTimeToLive(),
            this.getAllGatewaySenderIds());
        logger.info("Partitioned Region {} is born with prId={} ident:{}",
            new Object[] {getFullPath(), this.partitionedRegionId, getRegionIdentifier()});

        PRSanityCheckMessage.schedule(this);
      } else {
        validator.validatePartitionAttrsFromPRConfig(prConfig);
        if (storesData) {
          validator.validatePersistentMatchBetweenDataStores(prConfig);
          validator.validateCacheLoaderWriterBetweenDataStores(prConfig);
          validator.validateFixedPABetweenDataStores(prConfig);
        }

        this.partitionedRegionId = prConfig.getPRId();
        logger.info("Partitioned Region {} is created with prId={}",
            new Object[] {getFullPath(), this.partitionedRegionId});
      }

      synchronized (prIdToPR) {
        prIdToPR.put(this.partitionedRegionId, this); // last
      }

      prConfig.addNode(this.node);
      if (this.getFixedPartitionAttributesImpl() != null) {
        calculateStartingBucketIDs(prConfig);
      }
      updatePRConfig(prConfig, false);
      /*
       * try { if (this.redundantCopies > 0) { if (storesData) {
       * this.dataStore.grabBackupBuckets(false); } } } catch (RegionDestroyedException rde) { if
       * (!this.isClosed) throw rde; }
       */
      this.cleanPRRegistration = true;
    } catch (LockServiceDestroyedException lsde) {
      if (logger.isDebugEnabled()) {
        logger.debug("registerPartitionedRegion: unable to obtain lock for {}", this);
      }
      cleanupFailedInitialization();
      throw new PartitionedRegionException(
          "Can not create PartitionedRegion (failed to acquire RegionLock).",
          lsde);
    } catch (IllegalStateException ill) {
      cleanupFailedInitialization();
      throw ill;
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      String registerErrMsg =
          String.format(
              "An exception was caught while registering PartitionedRegion %s. dumpPRId: %s",
              getFullPath(), prIdToPR.dump());
      try {
        synchronized (prIdToPR) {
          if (prIdToPR.containsKey(this.partitionedRegionId)) {
            prIdToPR.put(this.partitionedRegionId, PRIdMap.FAILED_REGISTRATION, false);
            logger.info("FAILED_REGISTRATION prId={} named {}",
                new Object[] {this.partitionedRegionId, this.getName()});
          }
        }
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable e) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        if (logger.isDebugEnabled()) {
          logger.debug("Partitioned Region creation, could not clean up after caught exception", e);
        }
      }
      throw new PartitionedRegionException(registerErrMsg, t);
    } finally {
      try {
        rl.unlock();
        if (logger.isDebugEnabled()) {
          logger.debug("registerPartitionedRegion: released lock");
        }
      } catch (Exception es) {
        if (logger.isDebugEnabled()) {
          logger.debug(es.getMessage(), es);
        }
      }
    }
  }

  public void validateParallelAsynchronousEventDispatcherIds() throws PRLocallyDestroyedException {
    validateParallelAsynchronousEventDispatcherIds(this.getParallelGatewaySenderIds());
  }

  /**
   * Filters out non parallel GatewaySenders.
   *
   * @param senderIds set of gateway sender IDs.
   * @return set of parallel gateway senders present in the input set.
   */
  public Set<String> filterOutNonParallelGatewaySenders(Set<String> senderIds) {
    Set<String> allParallelSenders = cache.getAllGatewaySenders()
        .parallelStream()
        .filter(GatewaySender::isParallel)
        .map(GatewaySender::getId)
        .collect(toSet());

    Set<String> parallelSenders = new HashSet<>(senderIds);
    parallelSenders.retainAll(allParallelSenders);

    return parallelSenders;
  }

  /**
   * Filters out non parallel AsyncEventQueues.
   *
   * @param queueIds set of async-event-queue IDs.
   * @return set of parallel async-event-queues present in the input set.
   */
  public Set<String> filterOutNonParallelAsyncEventQueues(Set<String> queueIds) {
    Set<String> allParallelQueues = cache.getAsyncEventQueues()
        .parallelStream()
        .filter(AsyncEventQueue::isParallel)
        .map(asyncEventQueue -> AsyncEventQueueImpl
            .getAsyncEventQueueIdFromSenderId(asyncEventQueue.getId()))
        .collect(toSet());

    Set<String> parallelAsyncEventQueues = new HashSet<>(queueIds);
    parallelAsyncEventQueues.retainAll(allParallelQueues);

    return parallelAsyncEventQueues;
  }

  public void validateParallelAsynchronousEventDispatcherIds(
      Set<String> asynchronousEventDispatcherIds) throws PRLocallyDestroyedException {
    for (String dispatcherId : asynchronousEventDispatcherIds) {
      GatewaySender sender = getCache().getGatewaySender(dispatcherId);
      AsyncEventQueue asyncEventQueue = getCache()
          .getAsyncEventQueue(AsyncEventQueueImpl.getAsyncEventQueueIdFromSenderId(dispatcherId));

      // Can't attach a non persistent parallel gateway / async-event-queue to a persistent
      // partitioned region.
      if (getDataPolicy().withPersistence()) {
        if ((sender != null) && (!sender.isPersistenceEnabled())) {
          throw new GatewaySenderConfigurationException(String.format(
              "Non persistent gateway sender %s can not be attached to persistent region %s",
              dispatcherId, getFullPath()));
        } else if ((asyncEventQueue != null) && (!asyncEventQueue.isPersistent())) {
          throw new AsyncEventQueueConfigurationException(String.format(
              "Non persistent asynchronous event queue %s can not be attached to persistent region %s",
              dispatcherId, getFullPath()));
        }
      }

      for (PartitionRegionConfig config : this.prRoot.values()) {
        if (config.getGatewaySenderIds().contains(dispatcherId)) {
          if (this.getFullPath().equals(config.getFullPath())) {
            // The sender is already attached to this region
            continue;
          }

          Map<String, PartitionedRegion> colocationMap =
              ColocationHelper.getAllColocationRegions(this);
          if (!colocationMap.isEmpty()) {
            if (colocationMap.containsKey(config.getFullPath())) {
              continue;
            } else {
              int prID = config.getPRId();
              PartitionedRegion colocatedPR = PartitionedRegion.getPRFromId(prID);
              PartitionedRegion leader = ColocationHelper.getLeaderRegion(colocatedPR);

              if (colocationMap.containsValue(leader)) {
                continue;
              } else {
                throw new IllegalStateException(String.format(
                    "Non colocated regions %s, %s cannot have the same parallel %s id %s configured.",
                    this.getFullPath(), config.getFullPath(),
                    (asyncEventQueue != null ? "async event queue" : "gateway sender"),
                    dispatcherId));
              }
            }
          } else {
            throw new IllegalStateException(String.format(
                "Non colocated regions %s, %s cannot have the same parallel %s id %s configured.",
                this.getFullPath(), config.getFullPath(),
                (asyncEventQueue != null ? "async event queue" : "gateway sender"), dispatcherId));
          }
        }
      }
    }
  }

  /**
   * @return whether this region requires event notification for all cache content changes from
   *         other nodes
   */
  public boolean getRequiresNotification() {
    return this.requiresNotification;
  }

  /**
   * Get the Partitioned Region identifier used for DLocks (Bucket and Region)
   */
  public String getRegionIdentifier() {
    return this.regionIdentifier;
  }

  void setRecoveredFromDisk() {
    this.recoveredFromDisk = true;
    new UpdateAttributesProcessor(this).distribute(false);
  }

  /**
   * Throw an exception if persistent data recovery from disk is not complete for this region.
   */
  public void checkPROffline() throws PartitionOfflineException {
    if (getDataPolicy().withPersistence() && !recoveredFromDisk) {
      Set<PersistentID> persistIds =
          new HashSet(getRegionAdvisor().advisePersistentMembers().values());
      persistIds.removeAll(getRegionAdvisor().adviseInitializedPersistentMembers().values());
      throw new PartitionOfflineException(persistIds,
          String.format("Partitioned Region %s is offline due to unrecovered persistent data, %s",
              new Object[] {getFullPath(), persistIds}));
    }
  }

  /**
   *
   * Set the global PartitionedRegionConfig metadata for this region.
   *
   * This method should *only* be called while holding the partitioned region lock. For code
   * that is mutating the config, use
   * {@link #updatePartitionRegionConfig(PartitionRegionConfigModifier)}
   * instead.
   */
  void updatePRConfig(PartitionRegionConfig prConfig, boolean putOnlyIfUpdated) {
    final PartitionedRegion colocatedRegion = ColocationHelper.getColocatedRegion(this);
    RegionLock colocatedLock = null;
    boolean colocatedLockAcquired = false;
    try {
      boolean colocationComplete = false;
      if (colocatedRegion != null && !prConfig.isColocationComplete()) {
        colocatedLock = colocatedRegion.getRegionLock();
        colocatedLock.lock();
        colocatedLockAcquired = true;
        final PartitionRegionConfig parentConf =
            getPRRoot().get(colocatedRegion.getRegionIdentifier());
        if (parentConf.isColocationComplete() && parentConf.hasSameDataStoreMembers(prConfig)) {
          colocationComplete = true;
          prConfig.setColocationComplete(this);
        }
      }

      if (isDataStore() && !prConfig.isFirstDataStoreCreated()) {
        prConfig.setDatastoreCreated(getEvictionAttributes());
      }
      // N.B.: this put could fail with a CacheClosedException:
      if (!putOnlyIfUpdated || colocationComplete) {
        getPRRoot().put(getRegionIdentifier(), prConfig);
      }
    } finally {
      if (colocatedLockAcquired) {
        colocatedLock.unlock();
      }
    }
  }

  void executeColocationCallbacks() {
    colocationListeners.stream().forEach(ColocationListener::afterColocationCompleted);
  }

  /**
   * @param access true if caller wants last accessed time updated
   * @param allowTombstones - whether a tombstone can be returned
   */
  @Override
  protected Region.Entry<?, ?> nonTXGetEntry(KeyInfo keyInfo, boolean access,
      boolean allowTombstones) {
    final long startTime = prStats.getTime();
    final Object key = keyInfo.getKey();
    try {
      int bucketId = keyInfo.getBucketId();
      if (bucketId == KeyInfo.UNKNOWN_BUCKET) {
        bucketId = PartitionedRegionHelper.getHashKey(this, Operation.GET_ENTRY, key, null, null);
        keyInfo.setBucketId(bucketId);
      }
      InternalDistributedMember targetNode = getOrCreateNodeForBucketRead(bucketId);
      return getEntryInBucket(targetNode, bucketId, key, access, allowTombstones);
    } finally {
      this.prStats.endGetEntry(startTime);
    }
  }

  protected EntrySnapshot getEntryInBucket(final DistributedMember targetNode, final int bucketId,
      final Object key, boolean access, final boolean allowTombstones) {
    final int retryAttempts = calcRetry();
    if (logger.isTraceEnabled()) {
      logger.trace("getEntryInBucket: " + "Key key={} ({}) from: {} bucketId={}", key,
          key.hashCode(), targetNode, bucketStringForLogs(bucketId));
    }
    int bucketIdInt = bucketId;
    EntrySnapshot ret = null;
    int count = 0;
    RetryTimeKeeper retryTime = null;
    InternalDistributedMember retryNode = (InternalDistributedMember) targetNode;
    while (count <= retryAttempts) {
      // Every continuation should check for DM cancellation
      if (retryNode == null) {
        checkReadiness();
        if (retryTime == null) {
          retryTime = new RetryTimeKeeper(this.retryTimeout);
        }
        if (retryTime.overMaximum()) {
          break;
        }
        retryNode = getOrCreateNodeForBucketRead(bucketId);

        // No storage found for bucket, early out preventing hot loop, bug 36819
        if (retryNode == null) {
          checkShutdown();
          return null;
        }
        continue;
      }
      try {
        final boolean loc = (this.localMaxMemory > 0) && retryNode.equals(getMyId());
        if (loc) {
          ret = this.dataStore.getEntryLocally(bucketId, key, access, allowTombstones);
        } else {
          ret = getEntryRemotely(retryNode, bucketIdInt, key, access, allowTombstones);
          // TODO:Suranjan&Yogesh : there should be better way than this one
          String name = Thread.currentThread().getName();
          if (name.startsWith("ServerConnection") && !getMyId().equals(targetNode)) {
            setNetworkHopType(bucketIdInt, (InternalDistributedMember) targetNode);
          }
        }

        return ret;
      } catch (PRLocallyDestroyedException pde) {
        if (logger.isDebugEnabled()) {
          logger.debug("getEntryInBucket: Encountered PRLocallyDestroyedException", pde);
        }
        checkReadiness();
      } catch (EntryNotFoundException ignore) {
        return null;
      } catch (ForceReattemptException prce) {
        prce.checkKey(key);
        if (logger.isDebugEnabled()) {
          logger.debug("getEntryInBucket: retrying, attempts so far: {}", count, prce);
        }
        checkReadiness();
        InternalDistributedMember lastNode = retryNode;
        retryNode = getOrCreateNodeForBucketRead(bucketIdInt);
        if (lastNode.equals(retryNode)) {
          if (retryTime == null) {
            retryTime = new RetryTimeKeeper(this.retryTimeout);
          }
          if (retryTime.overMaximum()) {
            break;
          }
          retryTime.waitToRetryNode();
        }
      } catch (PrimaryBucketException notPrimary) {
        if (logger.isDebugEnabled()) {
          logger.debug("Bucket {} on Node {} not primary", notPrimary.getLocalizedMessage(),
              retryNode);
        }
        getRegionAdvisor().notPrimary(bucketIdInt, retryNode);
        retryNode = getOrCreateNodeForBucketRead(bucketIdInt);
      }

      // It's possible this is a GemFire thread e.g. ServerConnection
      // which got to this point because of a distributed system shutdown or
      // region closure which uses interrupt to break any sleep() or wait()
      // calls
      // e.g. waitForPrimary
      checkShutdown();

      count++;
      if (count == 1) {
        this.prStats.incContainsKeyValueOpsRetried();
      }
      this.prStats.incContainsKeyValueRetries();

    }

    PartitionedRegionDistributionException e = null; // Fix for bug 36014
    if (logger.isDebugEnabled()) {
      e = new PartitionedRegionDistributionException(
          String.format("No VM available for getEntry in %s attempts",
              count));
    }
    logger.warn(String.format("No VM available for getEntry in %s attempts", count), e);
    return null;
  }

  /**
   * Check for region closure, region destruction, cache closure as well as distributed system
   * disconnect. As of 6/21/2007, there were at least four volatile variables reads and one
   * synchonrization performed upon completion of this method.
   */
  private void checkShutdown() {
    checkReadiness();
    this.cache.getCancelCriterion().checkCancelInProgress(null);
  }

  /**
   * Checks if a key is contained remotely.
   *
   * @param targetNode the node where bucket region for the key exists.
   * @param bucketId the bucket id for the key.
   * @param key the key, whose value needs to be checks
   * @param access true if caller wants last access time updated
   * @param allowTombstones whether tombstones should be returned
   * @throws EntryNotFoundException if the entry doesn't exist
   * @throws ForceReattemptException if the peer is no longer available
   * @return true if the passed key is contained remotely.
   */
  public EntrySnapshot getEntryRemotely(InternalDistributedMember targetNode, Integer bucketId,
      Object key, boolean access, boolean allowTombstones)
      throws EntryNotFoundException, PrimaryBucketException, ForceReattemptException {
    FetchEntryResponse r = FetchEntryMessage.send(targetNode, this, key, access);
    this.prStats.incPartitionMessagesSent();
    EntrySnapshot entry = r.waitForResponse();
    if (entry != null && entry.getRawValue() == Token.TOMBSTONE) {
      if (!allowTombstones) {
        return null;
      }
    }
    return entry;
  }

  // /////////////////////////////////////////////////////////////////
  // Following methods would throw, operation Not Supported Exception
  // /////////////////////////////////////////////////////////////////

  /**
   * @since GemFire 5.0
   * @throws UnsupportedOperationException OVERRIDES
   */
  @Override
  public void becomeLockGrantor() {
    throw new UnsupportedOperationException();
  }

  /**
   * @since GemFire 5.0
   * @throws UnsupportedOperationException OVERRIDES
   */
  @Override
  public Region createSubregion(String subregionName, RegionAttributes regionAttributes)
      throws RegionExistsException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  /**
   * @since GemFire 5.0
   * @throws UnsupportedOperationException OVERRIDES
   */
  @Override
  public Lock getDistributedLock(Object key) throws IllegalStateException {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized long getLastModifiedTime() {
    if (!this.canStoreDataLocally()) {
      return 0;
    }
    Set<BucketRegion> buckets = this.dataStore.getAllLocalBucketRegions();
    long lastModifiedTime =
        buckets.stream().map(x -> x.getLastModifiedTime()).reduce(0L, (a, b) -> a > b ? a : b);
    return lastModifiedTime;
  }

  @Override
  public synchronized long getLastAccessedTime() {
    if (!this.canStoreDataLocally()) {
      return 0;
    }
    Set<BucketRegion> buckets = this.dataStore.getAllLocalBucketRegions();
    long lastAccessedTime =
        buckets.stream().map(x -> x.getLastAccessedTime()).reduce(0L, (a, b) -> a > b ? a : b);
    return lastAccessedTime;
  }

  @Override
  public long getMissCount() {
    if (!this.canStoreDataLocally()) {
      return 0;
    }
    Set<BucketRegion> buckets = this.dataStore.getAllLocalBucketRegions();
    return buckets.stream().map(x -> x.getMissCount()).reduce(0L, (a, b) -> a + b);
  }

  @Override
  public long getHitCount() {
    if (!this.canStoreDataLocally()) {
      return 0;
    }
    Set<BucketRegion> buckets = this.dataStore.getAllLocalBucketRegions();
    return buckets.stream().map(x -> x.getHitCount()).reduce(0L, (a, b) -> a + b);
  }

  @Override
  public void resetCounts() {
    if (!this.canStoreDataLocally()) {
      return;
    }
    Set<BucketRegion> buckets = this.dataStore.getAllLocalBucketRegions();
    for (BucketRegion bucket : buckets) {
      bucket.resetCounts();
    }
  }

  /**
   * @since GemFire 5.0
   * @throws UnsupportedOperationException OVERRIDES
   */
  public Region getSubregion() {
    throw new UnsupportedOperationException();
  }

  /**
   * @since GemFire 5.0
   * @throws UnsupportedOperationException OVERRIDES
   */
  @Override
  public Lock getRegionDistributedLock() throws IllegalStateException {

    throw new UnsupportedOperationException();
  }

  /**
   * @since GemFire 5.0
   * @throws UnsupportedOperationException OVERRIDES
   */
  @Override
  public void loadSnapshot(InputStream inputStream)
      throws IOException, ClassNotFoundException, CacheWriterException, TimeoutException {
    throw new UnsupportedOperationException();
  }

  /**
   * Should it destroy entry from local accessor????? OVERRIDES
   */
  @Override
  public void localDestroy(Object key, Object aCallbackArgument) throws EntryNotFoundException {

    throw new UnsupportedOperationException();
  }

  /**
   * @since GemFire 5.0
   * @throws UnsupportedOperationException OVERRIDES
   */
  @Override
  public void localInvalidate(Object key, Object aCallbackArgument) throws EntryNotFoundException {

    throw new UnsupportedOperationException();
  }

  /**
   * @since GemFire 5.0
   * @throws UnsupportedOperationException OVERRIDES
   */
  @Override
  public void localInvalidateRegion(Object aCallbackArgument) {
    getDataView().checkSupportsRegionInvalidate();
    throw new UnsupportedOperationException();
  }

  /**
   * Executes a query on this PartitionedRegion. The restrictions have already been checked. The
   * query is a SELECT expression, and the only region it refers to is this region.
   *
   * @see DefaultQuery#execute()
   *
   * @since GemFire 5.1
   */
  @Override
  public Object executeQuery(final DefaultQuery query,
      final ExecutionContext executionContext,
      final Object[] parameters,
      final Set buckets)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException {
    for (;;) {
      try {
        return doExecuteQuery(query, executionContext, parameters, buckets);
      } catch (ForceReattemptException ignore) {
        // fall through and loop
      }
    }
  }

  /**
   * If ForceReattemptException is thrown then the caller must loop and call us again.
   *
   * @throws ForceReattemptException if one of the buckets moved out from under us
   */
  private Object doExecuteQuery(final DefaultQuery query,
      final ExecutionContext executionContext,
      final Object[] parameters,
      final Set buckets)
      throws FunctionDomainException, TypeMismatchException, NameResolutionException,
      QueryInvocationTargetException, ForceReattemptException {
    if (logger.isDebugEnabled()) {
      logger.debug("Executing query :{}", query);
    }

    HashSet<Integer> allBuckets = new HashSet<Integer>();

    if (buckets == null) { // remote buckets
      final Iterator remoteIter = getRegionAdvisor().getBucketSet().iterator();
      try {
        while (remoteIter.hasNext()) {
          allBuckets.add((Integer) remoteIter.next());
        }
      } catch (NoSuchElementException ignore) {
      }
    } else { // local buckets
      Iterator localIter = null;
      if (this.dataStore != null) {
        localIter = buckets.iterator();
      } else {
        localIter = Collections.emptySet().iterator();
      }
      try {
        while (localIter.hasNext()) {
          allBuckets.add((Integer) localIter.next());
        }
      } catch (NoSuchElementException ignore) {
      }
    }

    if (allBuckets.size() == 0) {
      if (logger.isDebugEnabled()) {
        logger.debug("No bucket storage allocated. PR has no data yet.");
      }
      ResultsSet resSet = new ResultsSet();
      resSet.setElementType(new ObjectTypeImpl(
          this.getValueConstraint() == null ? Object.class : this.getValueConstraint()));
      return resSet;
    }

    CompiledSelect selectExpr = query.getSimpleSelect();
    if (selectExpr == null) {
      throw new IllegalArgumentException(
          "query must be a select expression only");
    }

    // this can return a BAG even if it's a DISTINCT select expression,
    // since the expectation is that the duplicates will be removed at the end
    SelectResults results = selectExpr.getEmptyResultSet(parameters, getCache(), query);

    PartitionedRegionQueryEvaluator prqe = new PartitionedRegionQueryEvaluator(this.getSystem(),
        this, query, executionContext, parameters, results, allBuckets);
    for (;;) {
      this.getCancelCriterion().checkCancelInProgress(null);
      boolean interrupted = Thread.interrupted();
      try {
        results = prqe.queryBuckets(null);
        break;
      } catch (InterruptedException ignore) {
        interrupted = true;
      } catch (FunctionDomainException e) {
        throw e;
      } catch (TypeMismatchException e) {
        throw e;
      } catch (NameResolutionException e) {
        throw e;
      } catch (QueryInvocationTargetException e) {
        throw e;
      } catch (QueryException qe) {
        throw new QueryInvocationTargetException(
            String.format("Unexpected query exception occurred during query execution %s",
                qe.getMessage()),
            qe);
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } // for

    // Drop Duplicates if this is a DISTINCT query
    boolean allowsDuplicates = results.getCollectionType().allowsDuplicates();
    // No need to apply the limit to the SelectResults.
    // We know that even if we do not apply the limit,
    // the results will satisfy the limit
    // as it has been evaluated in the iteration of List to
    // populate the SelectsResuts
    // So if the results is instance of ResultsBag or is a StructSet or
    // a ResultsSet, if the limit exists, the data set size will
    // be exactly matching the limit
    if (selectExpr.isDistinct()) {
      // don't just convert to a ResultsSet (or StructSet), since
      // the bags can convert themselves to a Set more efficiently
      ObjectType elementType = results.getCollectionType().getElementType();
      if (selectExpr.getOrderByAttrs() != null) {
        // Set limit also, its not applied while building the final result set as order by is
        // involved.
      } else if (allowsDuplicates) {
        results = new ResultsCollectionWrapper(elementType, results.asSet());
      }
      if (selectExpr.isCount() && (results.isEmpty() || selectExpr.isDistinct())) {
        // Constructor with elementType not visible.
        SelectResults resultCount = new ResultsBag(getCachePerfStats());
        resultCount.setElementType(new ObjectTypeImpl(Integer.class));
        ((Bag) resultCount).addAndGetOccurence(results.size());
        return resultCount;
      }
    }
    return results;
  }

  /**
   * @since GemFire 5.0
   * @throws UnsupportedOperationException OVERRIDES
   */
  @Override
  public void saveSnapshot(OutputStream outputStream) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * @since GemFire 5.0
   * @throws UnsupportedOperationException OVERRIDES
   */
  @Override
  public void writeToDisk() {
    throw new UnsupportedOperationException();
  }

  /**
   * @since GemFire 5.0
   * @throws UnsupportedOperationException OVERRIDES
   */
  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  void basicClear(RegionEventImpl regionEvent, boolean cacheWrite) {
    throw new UnsupportedOperationException();
  }

  @Override
  void basicLocalClear(RegionEventImpl event) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean virtualPut(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed, boolean invokeCallbacks, boolean throwConcurrentModificaiton)
      throws TimeoutException, CacheWriterException {
    final long startTime = prStats.getTime();
    boolean result = false;
    final DistributedPutAllOperation putAllOp_save = event.setPutAllOperation(null);

    if (event.getEventId() == null) {
      event.setNewEventId(this.cache.getDistributedSystem());
    }
    boolean bucketStorageAssigned = true;
    try {
      final Integer bucketId = event.getKeyInfo().getBucketId();
      assert bucketId != KeyInfo.UNKNOWN_BUCKET;
      // check in bucket2Node region
      InternalDistributedMember targetNode = getNodeForBucketWrite(bucketId, null);
      // force all values to be serialized early to make size computation cheap
      // and to optimize distribution.
      if (logger.isDebugEnabled()) {
        logger.debug("PR.virtualPut putting event={}", event);
      }

      if (targetNode == null) {
        try {
          bucketStorageAssigned = false;
          targetNode = createBucket(bucketId, event.getNewValSizeForPR(), null);
        } catch (PartitionedRegionStorageException e) {
          // try not to throw a PRSE if the cache is closing or this region was
          // destroyed during createBucket() (bug 36574)
          this.checkReadiness();
          if (this.cache.isClosed()) {
            throw new RegionDestroyedException(toString(), getFullPath());
          }
          throw e;
        }
      }

      if (event.isBridgeEvent() && bucketStorageAssigned) {
        setNetworkHopType(bucketId, targetNode);
      }
      if (putAllOp_save == null) {
        result = putInBucket(targetNode, bucketId, event, ifNew, ifOld, expectedOldValue,
            requireOldValue, (ifNew ? 0L : lastModified));
        if (logger.isDebugEnabled()) {
          logger.debug("PR.virtualPut event={} ifNew={} ifOld={} result={}", event, ifNew, ifOld,
              result);
        }
      } else {
        checkIfAboveThreshold(event); // fix for 40502
        // putAll: save the bucket id into DPAO, then wait for postPutAll to send msg
        // at this time, DPAO's PutAllEntryData should be empty, we should add entry here with
        // bucket id
        // the message will be packed in postPutAll, include the one to local bucket, because the
        // buckets
        // could be changed at that time
        putAllOp_save.addEntry(event, bucketId);
        if (logger.isDebugEnabled()) {
          logger.debug("PR.virtualPut PutAll added event={} into bucket {}", event, bucketId);
        }
        result = true;
      }
    } catch (RegionDestroyedException rde) {
      if (!rde.getRegionFullPath().equals(getFullPath())) {
        throw new RegionDestroyedException(toString(), getFullPath(), rde);
      }
    } finally {
      if (putAllOp_save == null) {
        // only for normal put
        if (ifNew) {
          this.prStats.endCreate(startTime);
        } else {
          this.prStats.endPut(startTime);
        }
      }
    }
    if (!result) {
      checkReadiness();
      if (!ifNew && !ifOld && !this.getConcurrencyChecksEnabled()) {
        // may fail due to concurrency conflict
        // failed for unknown reason
        // throw new PartitionedRegionStorageException("unable to execute operation");
        logger.warn("PR.virtualPut returning false when ifNew and ifOld are both false",
            new Exception("stack trace"));
      }
    }
    return result;
  }

  @Override
  void performPutAllEntry(EntryEventImpl event) {
    /*
     * force shared data view so that we just do the virtual op, accruing things in the put all
     * operation for later
     */
    getSharedDataView().putEntry(event, false, false, null, false, 0L, false);
  }

  @Override
  void performRemoveAllEntry(EntryEventImpl event) {
    // do not call basicDestroy directly since it causes bug 51715
    // basicDestroy(event, true, null);
    // force shared data view so that we just do the virtual op
    getSharedDataView().destroyExistingEntry(event, true, null);
  }

  @Override
  public void checkIfAboveThreshold(EntryEventImpl entryEvent) throws LowMemoryException {
    getRegionAdvisor().checkIfBucketSick(entryEvent.getKeyInfo().getBucketId(),
        entryEvent.getKey());
  }

  public boolean isFixedPartitionedRegion() {
    if (this.fixedPAttrs != null || this.fixedPASet == 1) {
      // We are sure that its a FixedFPA
      return true;
    }
    // We know that it's a normal PR
    if (this.fixedPASet == 2) {
      return false;
    }
    // Now is the case for accessor with fixedPAttrs null
    // and we don't know if it is a FPR
    // We will find out once and return that value whenever we have check again.
    this.fixedPASet = hasRemoteFPAttrs();
    return this.fixedPASet == 1;
  }

  private byte hasRemoteFPAttrs() {
    List<FixedPartitionAttributesImpl> fpaList =
        this.getRegionAdvisor().adviseAllFixedPartitionAttributes();
    Set<InternalDistributedMember> remoteDataStores = this.getRegionAdvisor().adviseDataStore();
    if (!fpaList.isEmpty() || (this.fixedPAttrs != null && !this.fixedPAttrs.isEmpty())) {
      return 1;
    }
    if (isDataStore() || !remoteDataStores.isEmpty()) {
      return 2;
    }

    // This is an accessor and we need to wait for a datastore to see if there
    // are any fixed PRs.
    return 0;
  }

  @Override
  public void postPutAllFireEvents(DistributedPutAllOperation putAllOp,
      VersionedObjectList successfulPuts) {
    /*
     * No op on pr, will happen in the buckets etc.
     */
  }

  @Override
  public void postRemoveAllFireEvents(DistributedRemoveAllOperation removeAllOp,
      VersionedObjectList successfulOps) {
    /*
     * No op on pr, will happen in the buckets etc.
     */
  }

  /**
   * Create PutAllPRMsgs for each bucket, and send them.
   *
   * @param putAllOp DistributedPutAllOperation object.
   * @param successfulPuts not used in PartitionedRegion.
   */
  @Override
  public long postPutAllSend(DistributedPutAllOperation putAllOp,
      VersionedObjectList successfulPuts) {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    if (cache.isCacheAtShutdownAll()) {
      throw cache.getCacheClosedException("Cache is shutting down");
    }

    final long startTime = prStats.getTime();
    // build all the msgs by bucketid
    HashMap prMsgMap = putAllOp.createPRMessages();
    PutAllPartialResult partialKeys = new PutAllPartialResult(putAllOp.putAllDataSize);

    // clear the successfulPuts list since we're actually doing the puts here
    // and the basicPutAll work was just a way to build the DPAO object
    Map<Object, VersionTag> keyToVersionMap =
        new HashMap<Object, VersionTag>(successfulPuts.size());
    successfulPuts.clearVersions();
    Iterator itor = prMsgMap.entrySet().iterator();
    while (itor.hasNext()) {
      Map.Entry mapEntry = (Map.Entry) itor.next();
      Integer bucketId = (Integer) mapEntry.getKey();
      PutAllPRMessage prMsg = (PutAllPRMessage) mapEntry.getValue();
      checkReadiness();
      long then = 0;
      if (isDebugEnabled) {
        then = System.currentTimeMillis();
      }
      try {
        VersionedObjectList versions = sendMsgByBucket(bucketId, prMsg);
        if (versions.size() > 0) {
          partialKeys.addKeysAndVersions(versions);
          versions.saveVersions(keyToVersionMap);
        } else if (!this.getConcurrencyChecksEnabled()) { // no keys returned if not versioned
          Set keys = prMsg.getKeys();
          partialKeys.addKeys(keys);
        }
      } catch (PutAllPartialResultException pre) {
        // sendMsgByBucket applied partial keys
        if (isDebugEnabled) {
          logger.debug("PR.postPutAll encountered PutAllPartialResultException, ", pre);
        }
        partialKeys.consolidate(pre.getResult());
      } catch (Exception ex) {
        // If failed at other exception
        if (isDebugEnabled) {
          logger.debug("PR.postPutAll encountered exception at sendMsgByBucket, ", ex);
        }
        @Released
        EntryEventImpl firstEvent = prMsg.getFirstEvent(this);
        try {
          partialKeys.saveFailedKey(firstEvent.getKey(), ex);
        } finally {
          firstEvent.release();
        }
      }
      if (isDebugEnabled) {
        long now = System.currentTimeMillis();
        if ((now - then) >= 10000) {
          logger.debug("PR.sendMsgByBucket took " + (now - then) + " ms");
        }
      }
    }
    this.prStats.endPutAll(startTime);
    if (!keyToVersionMap.isEmpty()) {
      for (Iterator it = successfulPuts.getKeys().iterator(); it.hasNext();) {
        successfulPuts.addVersion(keyToVersionMap.get(it.next()));
      }
      keyToVersionMap.clear();
    }

    if (partialKeys.hasFailure()) {
      logger.info("Region {} putAll: {}",
          new Object[] {getFullPath(), partialKeys});
      if (putAllOp.isBridgeOperation()) {
        if (partialKeys.getFailure() instanceof CancelException) {
          throw (CancelException) partialKeys.getFailure();
        } else {
          throw new PutAllPartialResultException(partialKeys);
        }
      } else {
        if (partialKeys.getFailure() instanceof RuntimeException) {
          throw (RuntimeException) partialKeys.getFailure();
        } else {
          throw new RuntimeException(partialKeys.getFailure());
        }
      }
    }
    return -1;
  }

  @Override
  public long postRemoveAllSend(DistributedRemoveAllOperation op,
      VersionedObjectList successfulOps) {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    if (cache.isCacheAtShutdownAll()) {
      throw cache.getCacheClosedException("Cache is shutting down");
    }

    final long startTime = prStats.getTime();
    // build all the msgs by bucketid
    HashMap<Integer, RemoveAllPRMessage> prMsgMap = op.createPRMessages();
    PutAllPartialResult partialKeys = new PutAllPartialResult(op.removeAllDataSize);

    // clear the successfulOps list since we're actually doing the removes here
    // and the basicRemoveAll work was just a way to build the "op" object
    Map<Object, VersionTag> keyToVersionMap = new HashMap<Object, VersionTag>(successfulOps.size());
    successfulOps.clearVersions();
    Iterator<Map.Entry<Integer, RemoveAllPRMessage>> itor = prMsgMap.entrySet().iterator();
    while (itor.hasNext()) {
      Map.Entry<Integer, RemoveAllPRMessage> mapEntry = itor.next();
      Integer bucketId = (Integer) mapEntry.getKey();
      RemoveAllPRMessage prMsg = mapEntry.getValue();
      checkReadiness();
      long then = 0;
      if (isDebugEnabled) {
        then = System.currentTimeMillis();
      }
      try {
        VersionedObjectList versions = sendMsgByBucket(bucketId, prMsg);
        if (versions.size() > 0) {
          partialKeys.addKeysAndVersions(versions);
          versions.saveVersions(keyToVersionMap);
        } else if (!this.getConcurrencyChecksEnabled()) { // no keys returned if not versioned
          Set keys = prMsg.getKeys();
          partialKeys.addKeys(keys);
        }
      } catch (PutAllPartialResultException pre) {
        // sendMsgByBucket applied partial keys
        if (isDebugEnabled) {
          logger.debug("PR.postRemoveAll encountered BulkOpPartialResultException, ", pre);
        }
        partialKeys.consolidate(pre.getResult());
      } catch (Exception ex) {
        // If failed at other exception
        if (isDebugEnabled) {
          logger.debug("PR.postRemoveAll encountered exception at sendMsgByBucket, ", ex);
        }
        @Released
        EntryEventImpl firstEvent = prMsg.getFirstEvent(this);
        try {
          partialKeys.saveFailedKey(firstEvent.getKey(), ex);
        } finally {
          firstEvent.release();
        }
      }
      if (isDebugEnabled) {
        long now = System.currentTimeMillis();
        if ((now - then) >= 10000) {
          logger.debug("PR.sendMsgByBucket took {} ms", (now - then));
        }
      }
    }
    this.prStats.endRemoveAll(startTime);
    if (!keyToVersionMap.isEmpty()) {
      for (Object o : successfulOps.getKeys()) {
        successfulOps.addVersion(keyToVersionMap.get(o));
      }
      keyToVersionMap.clear();
    }

    if (partialKeys.hasFailure()) {
      logger.info("Region {} putAll: {}",
          new Object[] {getFullPath(), partialKeys});
      if (op.isBridgeOperation()) {
        if (partialKeys.getFailure() instanceof CancelException) {
          throw (CancelException) partialKeys.getFailure();
        } else {
          throw new PutAllPartialResultException(partialKeys);
        }
      } else {
        if (partialKeys.getFailure() instanceof RuntimeException) {
          throw (RuntimeException) partialKeys.getFailure();
        } else {
          throw new RuntimeException(partialKeys.getFailure());
        }
      }
    }
    return -1;
  }

  /*
   * If failed after retries, it will throw PartitionedRegionStorageException, no need for return
   * value
   */
  private VersionedObjectList sendMsgByBucket(final Integer bucketId, PutAllPRMessage prMsg) {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    // retry the put remotely until it finds the right node managing the bucket
    @Released
    EntryEventImpl event = prMsg.getFirstEvent(this);
    try {
      RetryTimeKeeper retryTime = null;
      InternalDistributedMember currentTarget = getNodeForBucketWrite(bucketId, null);
      if (isDebugEnabled) {
        logger.debug("PR.sendMsgByBucket:bucket {}'s currentTarget is {}", bucketId, currentTarget);
      }

      long timeOut = 0;
      int count = 0;
      for (;;) {
        switch (count) {
          case 0:
            // Note we don't check for DM cancellation in common case.
            // First time. Assume success, keep going.
            break;
          case 1:
            this.cache.getCancelCriterion().checkCancelInProgress(null);
            // Second time (first failure). Calculate timeout and keep going.
            timeOut = System.currentTimeMillis() + this.retryTimeout;
            break;
          default:
            this.cache.getCancelCriterion().checkCancelInProgress(null);
            // test for timeout
            long timeLeft = timeOut - System.currentTimeMillis();
            if (timeLeft < 0) {
              PRHARedundancyProvider.timedOut(this, null, null, "update an entry",
                  this.retryTimeout);
              // NOTREACHED
            }

            // Didn't time out. Sleep a bit and then continue
            boolean interrupted = Thread.interrupted();
            try {
              Thread.sleep(PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION);
            } catch (InterruptedException ignore) {
              interrupted = true;
            } finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
            break;
        } // switch
        count++;

        if (currentTarget == null) { // pick target
          checkReadiness();
          if (retryTime == null) {
            retryTime = new RetryTimeKeeper(this.retryTimeout);
          }

          currentTarget = waitForNodeOrCreateBucket(retryTime, event, bucketId);
          if (isDebugEnabled) {
            logger.debug("PR.sendMsgByBucket: event size is {}, new currentTarget is {}",
                getEntrySize(event), currentTarget);
          }

          // It's possible this is a GemFire thread e.g. ServerConnection
          // which got to this point because of a distributed system shutdown or
          // region closure which uses interrupt to break any sleep() or wait() calls
          // e.g. waitForPrimary or waitForBucketRecovery in which case throw exception
          checkShutdown();
          continue;
        } // pick target

        try {
          return tryToSendOnePutAllMessage(prMsg, currentTarget);
        } catch (ForceReattemptException prce) {
          checkReadiness();
          InternalDistributedMember lastTarget = currentTarget;
          if (retryTime == null) {
            retryTime = new RetryTimeKeeper(this.retryTimeout);
          }
          currentTarget = getNodeForBucketWrite(bucketId, retryTime);
          if (isDebugEnabled) {
            logger.debug("PR.sendMsgByBucket: Old target was {}, Retrying {}", lastTarget,
                currentTarget);
          }
          if (lastTarget.equals(currentTarget)) {
            if (isDebugEnabled) {
              logger.debug("PR.sendMsgByBucket: Retrying at the same node:{} due to {}",
                  currentTarget, prce.getMessage());
            }
            if (retryTime.overMaximum()) {
              PRHARedundancyProvider.timedOut(this, null, null, "update an entry",
                  this.retryTimeout);
              // NOTREACHED
            }
            retryTime.waitToRetryNode();
          }
          event.setPossibleDuplicate(true);
          prMsg.setPossibleDuplicate(true);
        } catch (PrimaryBucketException notPrimary) {
          if (isDebugEnabled) {
            logger.debug("Bucket {} on Node {} not primnary", notPrimary.getLocalizedMessage(),
                currentTarget);
          }
          getRegionAdvisor().notPrimary(bucketId, currentTarget);
          if (retryTime == null) {
            retryTime = new RetryTimeKeeper(this.retryTimeout);
          }
          currentTarget = getNodeForBucketWrite(bucketId, retryTime);
        } catch (DataLocationException dle) {
          if (isDebugEnabled) {
            logger.debug("DataLocationException processing putAll", dle);
          }
          throw new TransactionException(dle);
        }

        // It's possible this is a GemFire thread e.g. ServerConnection
        // which got to this point because of a distributed system shutdown or
        // region closure which uses interrupt to break any sleep() or wait()
        // calls
        // e.g. waitForPrimary or waitForBucketRecovery in which case throw
        // exception
        checkShutdown();

        // If we get here, the attempt failed...
        if (count == 1) {
          this.prStats.incPutAllMsgsRetried();
        }
        this.prStats.incPutAllRetries();
      } // for
    } finally {
      if (event != null) {
        event.release();
      }
    }
    // NOTREACHED
  }

  /*
   * If failed after retries, it will throw PartitionedRegionStorageException, no need for return
   * value
   */
  private VersionedObjectList sendMsgByBucket(final Integer bucketId, RemoveAllPRMessage prMsg) {
    // retry the put remotely until it finds the right node managing the bucket
    @Released
    EntryEventImpl event = prMsg.getFirstEvent(this);
    try {
      RetryTimeKeeper retryTime = null;
      InternalDistributedMember currentTarget = getNodeForBucketWrite(bucketId, null);
      if (logger.isDebugEnabled()) {
        logger.debug("PR.sendMsgByBucket:bucket {}'s currentTarget is {}", bucketId, currentTarget);
      }

      long timeOut = 0;
      int count = 0;
      for (;;) {
        switch (count) {
          case 0:
            // Note we don't check for DM cancellation in common case.
            // First time. Assume success, keep going.
            break;
          case 1:
            this.cache.getCancelCriterion().checkCancelInProgress(null);
            // Second time (first failure). Calculate timeout and keep going.
            timeOut = System.currentTimeMillis() + this.retryTimeout;
            break;
          default:
            this.cache.getCancelCriterion().checkCancelInProgress(null);
            // test for timeout
            long timeLeft = timeOut - System.currentTimeMillis();
            if (timeLeft < 0) {
              PRHARedundancyProvider.timedOut(this, null, null, "update an entry",
                  this.retryTimeout);
              // NOTREACHED
            }

            // Didn't time out. Sleep a bit and then continue
            boolean interrupted = Thread.interrupted();
            try {
              Thread.sleep(PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION);
            } catch (InterruptedException ignore) {
              interrupted = true;
            } finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
            break;
        } // switch
        count++;

        if (currentTarget == null) { // pick target
          checkReadiness();
          if (retryTime == null) {
            retryTime = new RetryTimeKeeper(this.retryTimeout);
          }

          currentTarget = waitForNodeOrCreateBucket(retryTime, event, bucketId);
          if (logger.isDebugEnabled()) {
            logger.debug("PR.sendMsgByBucket: event size is {}, new currentTarget is {}",
                getEntrySize(event), currentTarget);
          }

          // It's possible this is a GemFire thread e.g. ServerConnection
          // which got to this point because of a distributed system shutdown or
          // region closure which uses interrupt to break any sleep() or wait() calls
          // e.g. waitForPrimary or waitForBucketRecovery in which case throw exception
          checkShutdown();
          continue;
        } // pick target

        try {
          return tryToSendOneRemoveAllMessage(prMsg, currentTarget);
        } catch (ForceReattemptException prce) {
          checkReadiness();
          InternalDistributedMember lastTarget = currentTarget;
          if (retryTime == null) {
            retryTime = new RetryTimeKeeper(this.retryTimeout);
          }
          currentTarget = getNodeForBucketWrite(bucketId, retryTime);
          if (logger.isTraceEnabled()) {
            logger.trace("PR.sendMsgByBucket: Old target was {}, Retrying {}", lastTarget,
                currentTarget);
          }
          if (lastTarget.equals(currentTarget)) {
            if (logger.isDebugEnabled()) {
              logger.debug("PR.sendMsgByBucket: Retrying at the same node:{} due to {}",
                  currentTarget, prce.getMessage());
            }
            if (retryTime.overMaximum()) {
              PRHARedundancyProvider.timedOut(this, null, null, "update an entry",
                  this.retryTimeout);
              // NOTREACHED
            }
            retryTime.waitToRetryNode();
          }
          event.setPossibleDuplicate(true);
          if (prMsg != null) {
            prMsg.setPossibleDuplicate(true);
          }
        } catch (PrimaryBucketException notPrimary) {
          if (logger.isDebugEnabled()) {
            logger.debug("Bucket {} on Node {} not primary", notPrimary.getLocalizedMessage(),
                currentTarget);
          }
          getRegionAdvisor().notPrimary(bucketId, currentTarget);
          if (retryTime == null) {
            retryTime = new RetryTimeKeeper(this.retryTimeout);
          }
          currentTarget = getNodeForBucketWrite(bucketId, retryTime);
        } catch (DataLocationException dle) {
          if (logger.isDebugEnabled()) {
            logger.debug("DataLocationException processing putAll", dle);
          }
          throw new TransactionException(dle);
        }

        // It's possible this is a GemFire thread e.g. ServerConnection
        // which got to this point because of a distributed system shutdown or
        // region closure which uses interrupt to break any sleep() or wait()
        // calls
        // e.g. waitForPrimary or waitForBucketRecovery in which case throw
        // exception
        checkShutdown();

        // If we get here, the attempt failed...
        if (count == 1) {
          this.prStats.incRemoveAllMsgsRetried();
        }
        this.prStats.incRemoveAllRetries();
      } // for
      // NOTREACHED
    } finally {
      event.release();
    }
  }

  public VersionedObjectList tryToSendOnePutAllMessage(PutAllPRMessage prMsg,
      InternalDistributedMember currentTarget) throws DataLocationException {
    boolean putResult = false;
    VersionedObjectList versions = null;
    final boolean isLocal = (this.localMaxMemory > 0) && currentTarget.equals(getMyId());
    if (isLocal) { // local
      // It might throw retry exception when one key failed
      // InternalDS has to be set for each msg
      prMsg.initMessage(this, null, false, null);
      putResult =
          prMsg.doLocalPutAll(this, this.getDistributionManager().getDistributionManagerId(), 0L);
      versions = prMsg.getVersions();
    } else {
      PutAllPRMessage.PutAllResponse response =
          (PutAllPRMessage.PutAllResponse) prMsg.send(currentTarget, this);
      PutAllPRMessage.PutAllResult pr = null;
      if (response != null) {
        this.prStats.incPartitionMessagesSent();
        try {
          pr = response.waitForResult();
          putResult = pr.returnValue;
          versions = pr.versions;
        } catch (RegionDestroyedException rde) {
          if (logger.isDebugEnabled()) {
            logger.debug("prMsg.send: caught RegionDestroyedException", rde);
          }
          throw new RegionDestroyedException(toString(), getFullPath());
        } catch (CacheException ce) {
          // Fix for bug 36014
          throw new PartitionedRegionDistributionException(
              "prMsg.send on " + currentTarget + " failed", ce);
        }
      } else {
        putResult = true; // follow the same behavior of putRemotely()
      }
    }

    if (!putResult) {
      // retry exception when msg failed in waitForResult()
      ForceReattemptException fre =
          new ForceReattemptException("false result in PutAllMessage.send - retrying");
      fre.setHash(0);
      throw fre;
    }
    return versions;
  }

  public VersionedObjectList tryToSendOneRemoveAllMessage(RemoveAllPRMessage prMsg,
      InternalDistributedMember currentTarget) throws DataLocationException {
    boolean putResult = false;
    VersionedObjectList versions = null;
    final boolean isLocal = (this.localMaxMemory > 0) && currentTarget.equals(getMyId());
    if (isLocal) { // local
      // It might throw retry exception when one key failed
      // InternalDS has to be set for each msg
      prMsg.initMessage(this, null, false, null);
      putResult = prMsg.doLocalRemoveAll(this,
          this.getDistributionManager().getDistributionManagerId(), true);
      versions = prMsg.getVersions();
    } else {
      RemoveAllPRMessage.RemoveAllResponse response =
          (RemoveAllPRMessage.RemoveAllResponse) prMsg.send(currentTarget, this);
      RemoveAllPRMessage.RemoveAllResult pr = null;
      if (response != null) {
        this.prStats.incPartitionMessagesSent();
        try {
          pr = response.waitForResult();
          putResult = pr.returnValue;
          versions = pr.versions;
        } catch (RegionDestroyedException rde) {
          if (logger.isDebugEnabled()) {
            logger.debug("prMsg.send: caught RegionDestroyedException", rde);
          }
          throw new RegionDestroyedException(toString(), getFullPath());
        } catch (CacheException ce) {
          // Fix for bug 36014
          throw new PartitionedRegionDistributionException(
              "prMsg.send on " + currentTarget + " failed", ce);
        }
      } else {
        putResult = true; // follow the same behavior of putRemotely()
      }
    }

    if (!putResult) {
      // retry exception when msg failed in waitForResult()
      ForceReattemptException fre =
          new ForceReattemptException("false result in PutAllMessage.send - retrying");
      fre.setHash(0);
      throw fre;
    }
    return versions;
  }

  /**
   * Performs the {@link Operation#UPDATE put} operation in the bucket on the remote VM and invokes
   * "put" callbacks
   *
   * @param targetNode the VM in whose {@link PartitionedRegionDataStore} contains the bucket
   * @param bucketId the identity of the bucket
   * @param event the event that contains the key and value
   * @param lastModified the timestamp that the entry was modified
   * @param ifNew true=can create a new key false=can't create a new key
   * @param ifOld true=can update existing entry false=can't update existing entry
   * @param expectedOldValue only succeed if old value is equal to this value. If null, then doesn't
   *        matter what old value is. If INVALID token, must be INVALID.
   * @see LocalRegion#virtualPut(EntryEventImpl, boolean, boolean, Object, boolean, long, boolean)
   * @return false if ifNew is true and there is an existing key, or ifOld is true and there is no
   *         existing entry; otherwise return true.
   *
   */
  private boolean putInBucket(final InternalDistributedMember targetNode, final Integer bucketId,
      final EntryEventImpl event, final boolean ifNew, boolean ifOld, Object expectedOldValue,
      boolean requireOldValue, final long lastModified) {
    if (logger.isDebugEnabled()) {
      logger.debug("putInBucket: {} ({}) to {} to bucketId={} retry={} ms", event.getKey(),
          event.getKey().hashCode(), targetNode, bucketStringForLogs(bucketId), retryTimeout);
    }
    // retry the put remotely until it finds the right node managing the bucket

    RetryTimeKeeper retryTime = null;
    boolean result = false;
    InternalDistributedMember currentTarget = targetNode;
    long timeOut = 0;
    int count = 0;
    for (;;) {
      switch (count) {
        case 0:
          // Note we don't check for DM cancellation in common case.
          // First time. Assume success, keep going.
          break;
        case 1:
          this.cache.getCancelCriterion().checkCancelInProgress(null);
          // Second time (first failure). Calculate timeout and keep going.
          timeOut = System.currentTimeMillis() + this.retryTimeout;
          break;
        default:
          this.cache.getCancelCriterion().checkCancelInProgress(null);
          // test for timeout
          long timeLeft = timeOut - System.currentTimeMillis();
          if (timeLeft < 0) {
            PRHARedundancyProvider.timedOut(this, null, null, "update an entry", this.retryTimeout);
            // NOTREACHED
          }

          // Didn't time out. Sleep a bit and then continue
          boolean interrupted = Thread.interrupted();
          try {
            Thread.sleep(PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION);
          } catch (InterruptedException ignore) {
            interrupted = true;
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
          break;
      } // switch
      count++;

      if (currentTarget == null) { // pick target
        checkReadiness();
        if (retryTime == null) {
          retryTime = new RetryTimeKeeper(this.retryTimeout);
        }
        currentTarget = waitForNodeOrCreateBucket(retryTime, event, bucketId);

        // It's possible this is a GemFire thread e.g. ServerConnection
        // which got to this point because of a distributed system shutdown or
        // region closure which uses interrupt to break any sleep() or wait() calls
        // e.g. waitForPrimary or waitForBucketRecovery in which case throw exception
        checkShutdown();
        continue;
      } // pick target

      try {
        final boolean isLocal = (this.localMaxMemory > 0) && currentTarget.equals(getMyId());
        if (logger.isDebugEnabled()) {
          logger.debug("putInBucket: currentTarget = {}; ifNew = {}; ifOld = {}; isLocal = {}",
              currentTarget, ifNew, ifOld, isLocal);
        }
        checkIfAboveThreshold(event);
        if (isLocal) {
          event.setInvokePRCallbacks(true);
          long start = this.prStats.startPutLocal();
          try {
            final BucketRegion br =
                this.dataStore.getInitializedBucketForId(event.getKey(), bucketId);
            // Local updates should insert a serialized (aka CacheDeserializable) object
            // given that most manipulation of values is remote (requiring serialization to send).
            // But... function execution always implies local manipulation of
            // values so keeping locally updated values in Object form should be more efficient.
            if (!FunctionExecutionPooledExecutor.isFunctionExecutionThread()) {
              // TODO: this condition may not help since BucketRegion.virtualPut calls
              // forceSerialized
              br.forceSerialized(event);
            }
            if (ifNew) {
              result = this.dataStore.createLocally(br, event, ifNew, ifOld, requireOldValue,
                  lastModified);
            } else {
              result = this.dataStore.putLocally(br, event, ifNew, ifOld, expectedOldValue,
                  requireOldValue, lastModified);
            }
          } finally {
            this.prStats.endPutLocal(start);
          }
        } // local
        else { // remote
          // no need to perform early serialization (and create an un-necessary byte array)
          // sending the message performs that work.
          long start = this.prStats.startPutRemote();
          try {
            if (ifNew) {
              result = createRemotely(currentTarget, bucketId, event, requireOldValue);
            } else {
              result = putRemotely(currentTarget, event, ifNew, ifOld, expectedOldValue,
                  requireOldValue);
              if (!requireOldValue) {
                // make sure old value is set to NOT_AVAILABLE token
                event.oldValueNotAvailable();
              }
            }
          } finally {
            this.prStats.endPutRemote(start);
          }
        } // remote

        if (!result && !ifOld && !ifNew) {
          Assert.assertTrue(!isLocal);
          ForceReattemptException fre = new ForceReattemptException(
              "false result when !ifNew and !ifOld is unacceptable - retrying");
          fre.setHash(event.getKey().hashCode());
          throw fre;
        }

        return result;
      } catch (ConcurrentCacheModificationException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("putInBucket: caught concurrent cache modification exception", e);
        }
        event.isConcurrencyConflict(true);

        if (logger.isTraceEnabled()) {
          logger.trace(
              "ConcurrentCacheModificationException received for putInBucket for bucketId: {}{}{} for event: {}  No reattampt is done, returning from here",
              getPRId(), BUCKET_ID_SEPARATOR, bucketId, event);
        }
        return result;
      } catch (ForceReattemptException prce) {
        prce.checkKey(event.getKey());
        if (logger.isDebugEnabled()) {
          logger.debug(
              "putInBucket: Got ForceReattemptException for {} on VM {} for node {}{}{} for bucket = {}",
              this, this.getMyId(), currentTarget, getPRId(), BUCKET_ID_SEPARATOR, bucketId, prce);
          logger.debug("putInBucket: count={}", count);
        }
        checkReadiness();
        InternalDistributedMember lastTarget = currentTarget;
        if (retryTime == null) {
          retryTime = new RetryTimeKeeper(this.retryTimeout);
        }
        currentTarget = getNodeForBucketWrite(bucketId, retryTime);
        if (lastTarget.equals(currentTarget)) {
          if (retryTime.overMaximum()) {
            PRHARedundancyProvider.timedOut(this, null, null, "update an entry", this.retryTimeout);
            // NOTREACHED
          }
          retryTime.waitToRetryNode();
        }
        event.setPossibleDuplicate(true);
      } catch (PrimaryBucketException notPrimary) {
        if (logger.isDebugEnabled()) {
          logger.debug("Bucket {} on Node {} not primary", notPrimary.getLocalizedMessage(),
              currentTarget);
        }
        getRegionAdvisor().notPrimary(bucketId, currentTarget);
        if (retryTime == null) {
          retryTime = new RetryTimeKeeper(this.retryTimeout);
        }
        currentTarget = getNodeForBucketWrite(bucketId, retryTime);
      }

      // It's possible this is a GemFire thread e.g. ServerConnection
      // which got to this point because of a distributed system shutdown or
      // region closure which uses interrupt to break any sleep() or wait()
      // calls
      // e.g. waitForPrimary or waitForBucketRecovery in which case throw
      // exception
      checkShutdown();

      // If we get here, the attempt failed...
      if (count == 1) {
        if (ifNew) {
          this.prStats.incCreateOpsRetried();
        } else {
          this.prStats.incPutOpsRetried();
        }
      }
      if (event.getOperation().isCreate()) {
        this.prStats.incCreateRetries();
      } else {
        this.prStats.incPutRetries();
      }

      if (logger.isDebugEnabled()) {
        logger.debug(
            "putInBucket for bucketId = {} failed (attempt # {} ({} ms left), retrying with node {}",
            bucketStringForLogs(bucketId), count, (timeOut - System.currentTimeMillis()),
            currentTarget);
      }
    } // for

    // NOTREACHED
  }


  /**
   * Find an existing live Node that owns the bucket, or create the bucket and return one of its
   * owners.
   *
   * @param retryTime the RetryTimeKeeper to track retry times
   * @param event the event used to get the entry size in the event a new bucket should be created
   * @param bucketId the identity of the bucket should it be created
   * @return a Node which contains the bucket, potentially null
   */
  private InternalDistributedMember waitForNodeOrCreateBucket(RetryTimeKeeper retryTime,
      EntryEventImpl event, Integer bucketId) {
    InternalDistributedMember newNode;
    if (retryTime.overMaximum()) {
      PRHARedundancyProvider.timedOut(this, null, null, "allocate a bucket",
          retryTime.getRetryTime());
      // NOTREACHED
    }

    retryTime.waitForBucketsRecovery();
    newNode = getNodeForBucketWrite(bucketId, retryTime);
    if (newNode == null) {
      newNode = createBucket(bucketId, getEntrySize(event), retryTime);
    }

    return newNode;
  }

  /**
   * Get the serialized size of an {@code EntryEventImpl}
   *
   * @param eei the entry from whcih to fetch the size
   * @return the size of the serialized entry
   */
  private static int getEntrySize(EntryEventImpl eei) {
    @Unretained
    final Object v = eei.getRawNewValue();
    if (v instanceof CachedDeserializable) {
      return ((Sizeable) v).getSizeInBytes();
    }
    return 0;
  }

  public InternalDistributedMember getOrCreateNodeForBucketWrite(int bucketId,
      final RetryTimeKeeper snoozer) {
    InternalDistributedMember targetNode = getNodeForBucketWrite(bucketId, snoozer);
    if (targetNode != null) {
      return targetNode;
    }

    try {
      return createBucket(bucketId, 0, null);
    } catch (PartitionedRegionStorageException e) {
      // try not to throw a PRSE if the cache is closing or this region was
      // destroyed during createBucket() (bug 36574)
      this.checkReadiness();
      if (this.cache.isClosed()) {
        throw new RegionDestroyedException(toString(), getFullPath());
      }
      throw e;
    }
  }

  /**
   * Fetch the primary Node returning if the redundancy for the Node is satisfied
   *
   * @param bucketId the identity of the bucket
   * @param snoozer a RetryTimeKeeper to track how long we may wait until the bucket is ready
   * @return the primary member's id or null if there is no storage
   */
  public InternalDistributedMember getNodeForBucketWrite(int bucketId,
      final RetryTimeKeeper snoozer) {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    RetryTimeKeeper localSnoozer = snoozer;
    // Prevent early access to buckets that are not completely created/formed
    // and
    // prevent writing to a bucket whose redundancy is sub par
    while (minimumWriteRedundancy > 0
        && getRegionAdvisor().getBucketRedundancy(bucketId) < this.minimumWriteRedundancy) {
      this.cache.getCancelCriterion().checkCancelInProgress(null);

      // First check to see if there is any storage assigned TODO: redundant check to while
      // condition
      if (!getRegionAdvisor().isStorageAssignedForBucket(bucketId, this.minimumWriteRedundancy,
          false)) {
        if (isDebugEnabled) {
          logger.debug("No storage assigned for bucket ({}{}{}) writer", getPRId(),
              BUCKET_ID_SEPARATOR, bucketId);
        }
        return null; // No bucket for this key
      }

      if (localSnoozer == null) {
        localSnoozer = new RetryTimeKeeper(this.retryTimeout);
      }

      if (!localSnoozer.overMaximum()) {
        localSnoozer.waitForBucketsRecovery();
      } else {
        int red = getRegionAdvisor().getBucketRedundancy(bucketId);
        final TimeoutException noTime = new TimeoutException(
            String.format(
                "Attempt to acquire primary node for write on bucket %s timed out in %s ms. Current redundancy [ %s ] does not satisfy minimum [ %s ]",
                new Object[] {bucketStringForLogs(bucketId),
                    localSnoozer.getRetryTime(), red, this.minimumWriteRedundancy}));
        checkReadiness();
        throw noTime;
      }
    }
    // Possible race with loss of redundancy at this point.
    // This loop can possibly create a soft hang if no primary is ever selected.
    // This is preferable to returning null since it will prevent obtaining the
    // bucket lock for bucket creation.
    return waitForNoStorageOrPrimary(bucketId, "write");
  }

  /**
   * wait until there is a primary or there is no storage
   *
   * @param bucketId the id of the target bucket
   * @param readOrWrite a string used in log messages
   * @return the primary member's id or null if there is no storage
   */
  private InternalDistributedMember waitForNoStorageOrPrimary(int bucketId, String readOrWrite) {
    boolean isInterrupted = false;
    try {
      for (;;) {
        isInterrupted = Thread.interrupted() || isInterrupted;
        InternalDistributedMember d = getBucketPrimary(bucketId);
        if (d != null) {
          return d; // success!
        } else {
          // go around the loop again
          if (logger.isDebugEnabled()) {
            logger.debug("No primary node found for bucket ({}{}{}) {}", getPRId(),
                BUCKET_ID_SEPARATOR, bucketId, readOrWrite);
          }
        }
        if (!getRegionAdvisor().isStorageAssignedForBucket(bucketId)) {
          if (logger.isDebugEnabled()) {
            logger.debug("No storage while waiting for primary for bucket ({}{}{}) {}", getPRId(),
                BUCKET_ID_SEPARATOR, bucketId, readOrWrite);
          }
          return null; // No bucket for this key
        }
        checkShutdown();
      }
    } finally {
      if (isInterrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  /**
   * override the one in LocalRegion since we don't need to do getDeserialized.
   */
  @Override
  public Object get(Object key, Object aCallbackArgument, boolean generateCallbacks,
      boolean disableCopyOnRead, boolean preferCD, ClientProxyMembershipID requestingClient,
      EntryEventImpl clientEvent, boolean returnTombstones)
      throws TimeoutException, CacheLoaderException {
    validateKey(key);
    checkReadiness();
    checkForNoAccess();
    discoverJTA();
    boolean miss = true;
    Object value = getDataView().findObject(getKeyInfo(key, aCallbackArgument), this,
        true/* isCreate */, generateCallbacks, null /* no local value */, disableCopyOnRead,
        preferCD, requestingClient, clientEvent, returnTombstones);
    if (value != null && !Token.isInvalid(value)) {
      miss = false;
    }
    return value;
  }

  @Override
  protected long startGet() {
    return 0;
  }

  @Override
  protected void endGet(long start, boolean isMiss) {
    // get stats are recorded by the BucketRegion
    // so nothing is needed on the PartitionedRegion.
  }

  public InternalDistributedMember getOrCreateNodeForBucketRead(int bucketId) {
    InternalDistributedMember targetNode = getNodeForBucketRead(bucketId);
    if (targetNode != null) {
      return targetNode;
    }
    try {
      return createBucket(bucketId, 0, null);
    } catch (PartitionedRegionStorageException e) {
      // try not to throw a PRSE if the cache is closing or this region was
      // destroyed during createBucket() (bug 36574)
      this.checkReadiness();
      if (this.cache.isClosed()) {
        throw new RegionDestroyedException(toString(), getFullPath());
      }
      throw e;
    }
  }

  /**
   * Gets the Node for reading a specific bucketId. This method gives priority to local node to
   * speed up operation and avoid remote calls.
   *
   * @param bucketId identifier for bucket
   *
   * @return the primary member's id or null if there is no storage
   */
  public InternalDistributedMember getNodeForBucketRead(int bucketId) {
    // Wait until there is storage
    InternalDistributedMember primary = waitForNoStorageOrPrimary(bucketId, "read");
    if (primary == null) {
      return null;
    }
    if (isTX()) {
      return getNodeForBucketWrite(bucketId, null);
    }
    return getRegionAdvisor().getPreferredNode(bucketId);
  }

  /**
   * Gets the Node for reading or performing a load from a specific bucketId.
   *
   * @return the member from which to read or load
   * @since GemFire 5.7
   */
  private InternalDistributedMember getNodeForBucketReadOrLoad(int bucketId) {
    InternalDistributedMember targetNode;
    if (!this.haveCacheLoader) {
      targetNode = getNodeForBucketRead(bucketId);
    } else {
      targetNode = getNodeForBucketWrite(bucketId, null /* retryTimeKeeper */);
    }
    if (targetNode == null) {
      this.checkShutdown(); // Fix for bug#37207
      targetNode = createBucket(bucketId, 0 /* size */, null /* retryTimeKeeper */);
    }
    return targetNode;
  }

  /**
   * Puts the key/value pair into the remote target that is managing the key's bucket.
   *
   * @param recipient the member to receive the message
   * @param event the event prompting this action
   * @param ifNew the mysterious ifNew parameter
   * @param ifOld the mysterious ifOld parameter
   * @return whether the operation succeeded
   * @throws PrimaryBucketException if the remote bucket was not the primary
   * @throws ForceReattemptException if the peer is no longer available
   */
  public boolean putRemotely(final DistributedMember recipient, final EntryEventImpl event,
      boolean ifNew, boolean ifOld, Object expectedOldValue, boolean requireOldValue)
      throws PrimaryBucketException, ForceReattemptException {
    // boolean forceAck = basicGetWriter() != null
    // || getDistributionAdvisor().adviseNetWrite().size() > 0;
    long eventTime = event.getEventTime(0L);
    PutMessage.PutResponse response = (PutMessage.PutResponse) PutMessage.send(recipient, this,
        event, eventTime, ifNew, ifOld, expectedOldValue, requireOldValue);
    PutResult pr = null;
    if (response != null) {
      this.prStats.incPartitionMessagesSent();
      try {
        pr = response.waitForResult();
        event.setOperation(pr.op);
        event.setVersionTag(pr.versionTag);
        if (requireOldValue) {
          event.setOldValue(pr.oldValue, true);
        }
        return pr.returnValue;
      } catch (RegionDestroyedException rde) {
        if (logger.isDebugEnabled()) {
          logger.debug("putRemotely: caught RegionDestroyedException", rde);
        }
        throw new RegionDestroyedException(toString(), getFullPath());
      } catch (TransactionException te) {
        throw te;
      } catch (CacheException ce) {
        // Fix for bug 36014
        throw new PartitionedRegionDistributionException(
            String.format("Putting entry on %s failed",
                recipient),
            ce);
      }
    }
    return true;// ???:ezoerner:20080728 why return true if response was null?
  }

  /**
   * Create a bucket for the provided bucket identifier in an atomic fashion.
   *
   * @param bucketId the bucket identifier for the bucket that needs creation
   * @param snoozer tracking object used to determine length of time t for bucket creation
   * @return a selected Node on which to perform bucket operations
   */
  public InternalDistributedMember createBucket(int bucketId, int size,
      final RetryTimeKeeper snoozer) {

    InternalDistributedMember ret = getNodeForBucketWrite(bucketId, snoozer);
    if (ret != null) {
      return ret;
    }
    // In the current co-location scheme, we have to create the bucket for the
    // colocatedWith region, before we create bucket for this region
    final PartitionedRegion colocatedWith = ColocationHelper.getColocatedRegion(this);
    if (colocatedWith != null) {
      colocatedWith.createBucket(bucketId, size, snoozer);
    }

    // THis is for FPR.if the given bucket id is not starting bucket id then
    // create bucket for starting bucket id
    String partitionName = null;
    if (this.isFixedPartitionedRegion()) {
      FixedPartitionAttributesImpl fpa =
          PartitionedRegionHelper.getFixedPartitionAttributesForBucket(this, bucketId);
      partitionName = fpa.getPartitionName();
      int startBucketId = fpa.getStartingBucketID();
      if (startBucketId == -1) {
        throw new PartitionNotAvailableException(
            String.format(
                "For FixedPartitionedRegion %s, Partition %s is not yet initialized on datastore",
                new Object[] {getName(), partitionName}));
      }
      if (startBucketId != bucketId) {
        createBucket(startBucketId, size, snoozer);
      }
    }
    // Potentially no storage assigned, start bucket creation, be careful of race
    // conditions
    final long startTime = prStats.getTime();
    if (isDataStore()) {
      ret = this.redundancyProvider.createBucketAtomically(bucketId, size, false,
          partitionName);
    } else {
      ret = this.redundancyProvider.createBucketOnDataStore(bucketId, size, snoozer);
    }
    return ret;
  }

  @Override
  Object findObjectInSystem(KeyInfo keyInfo, boolean isCreate, TXStateInterface tx,
      boolean generateCallbacks, Object localValue, boolean disableCopyOnRead, boolean preferCD,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) throws CacheLoaderException, TimeoutException {
    Object obj = null;
    final Object key = keyInfo.getKey();
    final Object aCallbackArgument = keyInfo.getCallbackArg();
    final long startTime = prStats.getTime();
    try {
      int bucketId = keyInfo.getBucketId();
      if (bucketId == KeyInfo.UNKNOWN_BUCKET) {
        bucketId = PartitionedRegionHelper.getHashKey(this, isCreate ? Operation.CREATE : null, key,
            null, aCallbackArgument);
        keyInfo.setBucketId(bucketId);
      }
      InternalDistributedMember targetNode = null;
      TXStateProxy txState = getTXState();
      boolean allowRetry;
      if (txState != null) {
        if (txState.isRealDealLocal()) {
          targetNode = getMyId();
        } else {
          targetNode = (InternalDistributedMember) txState.getTarget();
          assert targetNode != null;
        }
        allowRetry = false;
      } else {
        targetNode = getBucketNodeForReadOrWrite(bucketId, clientEvent);
        allowRetry = true;
      }
      if (targetNode == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("No need to create buckets on get(), no CacheLoader configured.");
        }
        return null;
      }

      obj = getFromBucket(targetNode, bucketId, key, aCallbackArgument, disableCopyOnRead, preferCD,
          requestingClient, clientEvent, returnTombstones, allowRetry);
    } finally {
      this.prStats.endGet(startTime);
    }
    return obj;
  }

  InternalDistributedMember getBucketNodeForReadOrWrite(int bucketId,
      EntryEventImpl clientEvent) {
    InternalDistributedMember targetNode;
    if (clientEvent != null && clientEvent.getOperation() != null
        && clientEvent.getOperation().isGetForRegisterInterest()) {
      targetNode = getNodeForBucketWrite(bucketId, null);
    } else {
      targetNode = getNodeForBucketReadOrLoad(bucketId);
    }
    return targetNode;
  }

  /**
   * Execute the provided named function in all locations that contain the given keys. So function
   * can be executed on just one fabric node, executed in parallel on a subset of nodes in parallel
   * across all the nodes.
   *
   * @since GemFire 6.0
   */
  public ResultCollector executeFunction(final Function function,
      final PartitionedRegionFunctionExecutor execution, ResultCollector rc,
      boolean executeOnBucketSet) {
    if (execution.isPrSingleHop()) {
      if (!executeOnBucketSet) {
        switch (execution.getFilter().size()) {
          case 1:
            if (logger.isDebugEnabled()) {
              logger.debug("Executing Function: (Single Hop) {} on single node.", function.getId());
            }
            return executeOnSingleNode(function, execution, rc, true, false);
          default:
            if (logger.isDebugEnabled()) {
              logger.debug("Executing Function: (Single Hop) {} on multiple nodes.",
                  function.getId());
            }
            return executeOnMultipleNodes(function, execution, rc, true, false);
        }
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug("Executing Function: (Single Hop) {} on a set of buckets nodes.",
              function.getId());
        }
        return executeOnBucketSet(function, execution, rc, execution.getFilter());
      }
    } else {
      switch (execution.getFilter().size()) {
        case 0:
          if (logger.isDebugEnabled()) {
            logger.debug("Executing Function: {} setArguments={} on all buckets.", function.getId(),
                execution.getArguments());
          }
          return executeOnAllBuckets(function, execution, rc, false);
        case 1:
          if (logger.isDebugEnabled()) {
            logger.debug("Executing Function: {} setArguments={} on single node.", function.getId(),
                execution.getArguments());
          }
          return executeOnSingleNode(function, execution, rc, false, executeOnBucketSet);
        default:
          if (logger.isDebugEnabled()) {
            logger.debug("Executing Function: {} setArguments={} on multiple nodes.",
                function.getId(), execution.getArguments());
          }
          return executeOnMultipleNodes(function, execution, rc, false, executeOnBucketSet);
      }
    }
  }

  /**
   * Executes function on multiple nodes
   */
  private ResultCollector executeOnMultipleNodes(final Function function,
      final PartitionedRegionFunctionExecutor execution, ResultCollector rc, boolean isPRSingleHop,
      boolean isBucketSetAsFilter) {
    final Set routingKeys = execution.getFilter();
    final boolean primaryMembersNeeded = function.optimizeForWrite();
    Map<Integer, Set> bucketToKeysMap = FunctionExecutionNodePruner.groupByBucket(this,
        routingKeys, primaryMembersNeeded, false, isBucketSetAsFilter);
    HashMap<InternalDistributedMember, HashSet> memberToKeysMap =
        new HashMap<InternalDistributedMember, HashSet>();
    HashMap<InternalDistributedMember, int[]> memberToBuckets =
        FunctionExecutionNodePruner.groupByMemberToBuckets(this, bucketToKeysMap.keySet(),
            primaryMembersNeeded);

    if (isPRSingleHop && (memberToBuckets.size() > 1)) {
      // memberToBuckets.remove(getMyId()); // don't remove
      for (InternalDistributedMember targetNode : memberToBuckets.keySet()) {
        if (!targetNode.equals(getMyId())) {
          int[] bucketArray = memberToBuckets.get(targetNode);
          int length = BucketSetHelper.length(bucketArray);
          if (length == 0) {
            continue;
          }
          for (int i = 0; i < length; i++) {
            int bucketId = BucketSetHelper.get(bucketArray, i);
            Set<ServerBucketProfile> profiles =
                this.getRegionAdvisor().getClientBucketProfiles(bucketId);
            if (profiles != null) {
              for (ServerBucketProfile profile : profiles) {
                if (profile.getDistributedMember().equals(targetNode)) {
                  if (logger.isDebugEnabled()) {
                    logger.debug("FunctionServiceSingleHop: Found multiple nodes.{}", getMyId());
                  }
                  throw new InternalFunctionInvocationTargetException(
                      "Multiple target nodes found for single hop operation");
                }
              }
            }
          }
        }
      }
    }

    while (!execution.getFailedNodes().isEmpty()) {
      Set memberKeySet = memberToBuckets.keySet();
      RetryTimeKeeper retryTime = new RetryTimeKeeper(this.retryTimeout);
      Iterator iterator = memberKeySet.iterator();

      boolean hasRemovedNode = false;

      while (iterator.hasNext()) {
        if (execution.getFailedNodes().contains(((DistributedMember) iterator.next()).getId())) {
          hasRemovedNode = true;
        }
      }

      if (hasRemovedNode) {
        if (retryTime.overMaximum()) {
          PRHARedundancyProvider.timedOut(this, null, null, "doing function execution",
              this.retryTimeout);
          // NOTREACHED
        }
        retryTime.waitToRetryNode();
        memberToBuckets = FunctionExecutionNodePruner.groupByMemberToBuckets(this,
            bucketToKeysMap.keySet(), primaryMembersNeeded);

      } else {
        execution.clearFailedNodes();
      }
    }

    for (Map.Entry entry : memberToBuckets.entrySet()) {
      InternalDistributedMember member = (InternalDistributedMember) entry.getKey();
      int[] buckets = (int[]) entry.getValue();
      int length = BucketSetHelper.length(buckets);
      if (length == 0) {
        continue;
      }
      int bucket;
      for (int i = 0; i < length; i++) {
        bucket = BucketSetHelper.get(buckets, i);
        HashSet keys = memberToKeysMap.get(member);
        if (keys == null) {
          keys = new HashSet();
        }
        keys.addAll(bucketToKeysMap.get(bucket));
        memberToKeysMap.put(member, keys);
      }
    }
    // memberToKeysMap.keySet().retainAll(memberToBuckets.keySet());
    if (memberToKeysMap.isEmpty()) {
      throw new FunctionException(String.format("No target node found for KEY, %s",
          routingKeys));
    }
    Set<InternalDistributedMember> dest = memberToKeysMap.keySet();
    execution.validateExecution(function, dest);
    // added for the data aware procedure.
    execution.setExecutionNodes(dest);
    // end

    final HashSet localKeys = memberToKeysMap.remove(getMyId());
    int[] localBucketSet = null;
    boolean remoteOnly = false;
    if (localKeys == null) {
      remoteOnly = true;
    } else {
      localBucketSet = FunctionExecutionNodePruner.getBucketSet(PartitionedRegion.this, localKeys,
          false, isBucketSetAsFilter);

      remoteOnly = false;
    }
    final LocalResultCollector<?, ?> localResultCollector =
        execution.getLocalResultCollector(function, rc);
    final DistributionManager dm = getDistributionManager();
    final PartitionedRegionFunctionResultSender resultSender =
        new PartitionedRegionFunctionResultSender(dm, this, 0L, localResultCollector,
            execution.getServerResultSender(), memberToKeysMap.isEmpty(), remoteOnly,
            execution.isForwardExceptions(), function, localBucketSet);

    if (localKeys != null) {
      final RegionFunctionContextImpl prContext =
          new RegionFunctionContextImpl(cache, function.getId(), PartitionedRegion.this,
              execution.getArgumentsForMember(getMyId().getId()),
              localKeys, ColocationHelper
                  .constructAndGetAllColocatedLocalDataSet(PartitionedRegion.this, localBucketSet),
              localBucketSet, resultSender, execution.isReExecute());
      if (logger.isDebugEnabled()) {
        logger.debug("FunctionService: Executing on local node with keys.{}", localKeys);
      }
      execution.executeFunctionOnLocalPRNode(function, prContext, resultSender, dm, isTX());
    }

    if (!memberToKeysMap.isEmpty()) {
      HashMap<InternalDistributedMember, FunctionRemoteContext> recipMap =
          new HashMap<InternalDistributedMember, FunctionRemoteContext>();
      for (Map.Entry me : memberToKeysMap.entrySet()) {
        InternalDistributedMember recip = (InternalDistributedMember) me.getKey();
        HashSet memKeys = (HashSet) me.getValue();
        FunctionRemoteContext context = new FunctionRemoteContext(function,
            execution.getArgumentsForMember(recip.getId()), memKeys,
            FunctionExecutionNodePruner.getBucketSet(this, memKeys, false, isBucketSetAsFilter),
            execution.isReExecute(), execution.isFnSerializationReqd(), getPrincipal());
        recipMap.put(recip, context);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("FunctionService: Executing on remote nodes with member to keys map.{}",
            memberToKeysMap);
      }
      PartitionedRegionFunctionResultWaiter resultReceiver =
          new PartitionedRegionFunctionResultWaiter(getSystem(), this.getPRId(),
              localResultCollector, function, resultSender);
      return resultReceiver.getPartitionedDataFrom(recipMap, this, execution);
    }
    return localResultCollector;
  }

  private Object getPrincipal() {
    return cache.getSecurityService().getPrincipal();
  }

  /**
   * Single key execution on single node
   *
   * @since GemFire 6.0
   */
  private ResultCollector executeOnSingleNode(final Function function,
      final PartitionedRegionFunctionExecutor execution, ResultCollector rc, boolean isPRSingleHop,
      boolean isBucketSetAsFilter) {
    final Set routingKeys = execution.getFilter();
    final Object key = routingKeys.iterator().next();
    final Integer bucketId;
    if (isBucketSetAsFilter) {
      bucketId = (Integer) key;
    } else {
      bucketId =
          PartitionedRegionHelper.getHashKey(this, Operation.FUNCTION_EXECUTION, key, null, null);
    }
    InternalDistributedMember targetNode = null;
    if (function.optimizeForWrite()) {
      targetNode = createBucket(bucketId, 0, null /* retryTimeKeeper */);
      cache.getInternalResourceManager().getHeapMonitor().checkForLowMemory(function, targetNode);
    } else {
      targetNode = getOrCreateNodeForBucketRead(bucketId);
    }
    final DistributedMember localVm = getMyId();
    if (targetNode != null && isPRSingleHop && !localVm.equals(targetNode)) {
      Set<ServerBucketProfile> profiles = this.getRegionAdvisor().getClientBucketProfiles(bucketId);
      if (profiles != null) {
        for (ServerBucketProfile profile : profiles) {
          if (profile.getDistributedMember().equals(targetNode)) {
            if (logger.isDebugEnabled()) {
              logger.debug("FunctionServiceSingleHop: Found remote node.{}", localVm);
            }
            throw new InternalFunctionInvocationTargetException(
                "Multiple target nodes found for single hop operation");
          }
        }
      }
    }

    if (targetNode == null) {
      throw new FunctionException(
          String.format("No target node found for KEY, %s", key));
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Executing Function: {} setArguments={} on {}", function.getId(),
          execution.getArguments(), targetNode);
    }
    while (!execution.getFailedNodes().isEmpty()) {
      RetryTimeKeeper retryTime = new RetryTimeKeeper(this.retryTimeout);
      if (execution.getFailedNodes().contains(targetNode.getId())) {
        /*
         * if (retryTime.overMaximum()) { PRHARedundancyProvider.timedOut(this, null, null,
         * "doing function execution", this.retryTimeout); // NOTREACHED }
         */
        // Fix for Bug # 40083
        targetNode = null;
        while (targetNode == null) {
          if (retryTime.overMaximum()) {
            PRHARedundancyProvider.timedOut(this, null, null, "doing function execution",
                this.retryTimeout);
            // NOTREACHED
          }
          retryTime.waitToRetryNode();
          if (function.optimizeForWrite()) {
            targetNode = getOrCreateNodeForBucketWrite(bucketId, retryTime);
          } else {
            targetNode = getOrCreateNodeForBucketRead(bucketId);
          }
        }
      } else {
        execution.clearFailedNodes();
      }
    }

    final int[] buckets = new int[2];
    buckets[0] = 0;
    BucketSetHelper.add(buckets, bucketId);
    final Set<InternalDistributedMember> singleMember = Collections.singleton(targetNode);
    execution.validateExecution(function, singleMember);
    execution.setExecutionNodes(singleMember);
    LocalResultCollector<?, ?> localRC = execution.getLocalResultCollector(function, rc);
    if (targetNode.equals(localVm)) {
      final DistributionManager dm = getDistributionManager();
      PartitionedRegionFunctionResultSender resultSender =
          new PartitionedRegionFunctionResultSender(dm, PartitionedRegion.this, 0, localRC,
              execution.getServerResultSender(), true, false, execution.isForwardExceptions(),
              function, buckets);
      final FunctionContext context =
          new RegionFunctionContextImpl(cache, function.getId(), PartitionedRegion.this,
              execution.getArgumentsForMember(localVm.getId()), routingKeys, ColocationHelper
                  .constructAndGetAllColocatedLocalDataSet(PartitionedRegion.this, buckets),
              buckets, resultSender, execution.isReExecute());
      execution.executeFunctionOnLocalPRNode(function, context, resultSender, dm, isTX());
      return localRC;
    } else {
      return executeFunctionOnRemoteNode(targetNode, function,
          execution.getArgumentsForMember(targetNode.getId()), routingKeys,
          function.isHA() ? rc : localRC, buckets, execution.getServerResultSender(), execution);
    }
  }

  public ResultCollector executeOnBucketSet(final Function function,
      PartitionedRegionFunctionExecutor execution, ResultCollector rc, Set<Integer> bucketSet) {
    Set<Integer> actualBucketSet = this.getRegionAdvisor().getBucketSet();
    try {
      bucketSet.retainAll(actualBucketSet);
    } catch (NoSuchElementException ignore) {
      // done
    }
    HashMap<InternalDistributedMember, int[]> memberToBuckets =
        FunctionExecutionNodePruner.groupByMemberToBuckets(this, bucketSet,
            function.optimizeForWrite());

    if (memberToBuckets.isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Executing on bucketset : {} executeOnBucketSet Member to buckets map is : {} bucketSet is empty",
            bucketSet, memberToBuckets);
      }
      throw new EmptyRegionFunctionException(
          "Region is empty and the function cannot be executed");
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("Executing on bucketset : {} executeOnBucketSet Member to buckets map is : {}",
            bucketSet, memberToBuckets);

      }
    }

    if (memberToBuckets.size() > 1) {
      for (InternalDistributedMember targetNode : memberToBuckets.keySet()) {
        if (!targetNode.equals(getMyId())) {
          int[] bucketArray = memberToBuckets.get(targetNode);
          int length = BucketSetHelper.length(bucketArray);
          if (length == 0) {
            continue;
          }
          for (int i = 0; i < length; i++) {
            int bucketId = BucketSetHelper.get(bucketArray, i);
            Set<ServerBucketProfile> profiles =
                this.getRegionAdvisor().getClientBucketProfiles(bucketId);
            if (profiles != null) {
              for (ServerBucketProfile profile : profiles) {
                if (profile.getDistributedMember().equals(targetNode)) {
                  if (logger.isDebugEnabled()) {
                    logger.debug(
                        "FunctionServiceSingleHop: Found multiple nodes for executing on bucket set.{}",
                        getMyId());
                  }
                  throw new InternalFunctionInvocationTargetException(
                      "Multiple target nodes found for single hop operation");
                }
              }
            }
          }
        }
      }
    }

    execution = (PartitionedRegionFunctionExecutor) execution.withFilter(new HashSet());
    while (!execution.getFailedNodes().isEmpty()) {
      Set memberKeySet = memberToBuckets.keySet();
      RetryTimeKeeper retryTime = new RetryTimeKeeper(this.retryTimeout);
      Iterator iterator = memberKeySet.iterator();
      boolean hasRemovedNode = false;

      while (iterator.hasNext()) {
        if (execution.getFailedNodes().contains(((DistributedMember) iterator.next()).getId())) {
          hasRemovedNode = true;
        }
      }

      if (hasRemovedNode) {
        if (retryTime.overMaximum()) {
          PRHARedundancyProvider.timedOut(this, null, null, "doing function execution",
              this.retryTimeout);
          // NOTREACHED
        }
        retryTime.waitToRetryNode();
        memberToBuckets = FunctionExecutionNodePruner.groupByMemberToBuckets(this, bucketSet,
            function.optimizeForWrite());
      } else {
        execution.clearFailedNodes();
      }
    }

    Set<InternalDistributedMember> dest = memberToBuckets.keySet();
    cache.getInternalResourceManager().getHeapMonitor().checkForLowMemory(function,
        Collections.unmodifiableSet(dest));

    boolean isSelf = false;
    execution.setExecutionNodes(dest);
    final int[] localBucketSet = memberToBuckets.remove(getMyId());
    if (BucketSetHelper.length(localBucketSet) > 0) {
      isSelf = true;
    }
    final HashMap<InternalDistributedMember, FunctionRemoteContext> recipMap =
        new HashMap<InternalDistributedMember, FunctionRemoteContext>();
    for (InternalDistributedMember recip : dest) {
      FunctionRemoteContext context = new FunctionRemoteContext(function,
          execution.getArgumentsForMember(recip.getId()), null, memberToBuckets.get(recip),
          execution.isReExecute(), execution.isFnSerializationReqd(), getPrincipal());
      recipMap.put(recip, context);
    }
    final LocalResultCollector<?, ?> localRC = execution.getLocalResultCollector(function, rc);

    final DistributionManager dm = getDistributionManager();
    final PartitionedRegionFunctionResultSender resultSender =
        new PartitionedRegionFunctionResultSender(dm, this, 0L, localRC,
            execution.getServerResultSender(), recipMap.isEmpty(), !isSelf,
            execution.isForwardExceptions(), function, localBucketSet);

    // execute locally and collect the result
    if (isSelf && this.dataStore != null) {
      final RegionFunctionContextImpl prContext =
          new RegionFunctionContextImpl(cache, function.getId(), PartitionedRegion.this,
              execution.getArgumentsForMember(getMyId().getId()), null, ColocationHelper
                  .constructAndGetAllColocatedLocalDataSet(PartitionedRegion.this, localBucketSet),
              localBucketSet, resultSender, execution.isReExecute());
      execution.executeFunctionOnLocalNode(function, prContext, resultSender, dm, isTX());
    }
    PartitionedRegionFunctionResultWaiter resultReceiver =
        new PartitionedRegionFunctionResultWaiter(getSystem(), this.getPRId(), localRC, function,
            resultSender);

    return resultReceiver.getPartitionedDataFrom(recipMap, this, execution);
  }

  /**
   * Executes function on all bucket nodes
   *
   * @since GemFire 6.0
   */
  private ResultCollector executeOnAllBuckets(final Function function,
      final PartitionedRegionFunctionExecutor execution, ResultCollector rc,
      boolean isPRSingleHop) {
    Set<Integer> bucketSet = new HashSet<Integer>();
    Iterator<Integer> itr = this.getRegionAdvisor().getBucketSet().iterator();
    while (itr.hasNext()) {
      try {
        bucketSet.add(itr.next());
      } catch (NoSuchElementException ignore) {
      }
    }
    HashMap<InternalDistributedMember, int[]> memberToBuckets =
        FunctionExecutionNodePruner.groupByMemberToBuckets(this, bucketSet,
            function.optimizeForWrite());

    if (memberToBuckets.isEmpty()) {
      throw new EmptyRegionFunctionException(
          "Region is empty and the function cannot be executed");
    }

    while (!execution.getFailedNodes().isEmpty()) {
      Set memberKeySet = memberToBuckets.keySet();
      RetryTimeKeeper retryTime = new RetryTimeKeeper(this.retryTimeout);

      Iterator iterator = memberKeySet.iterator();

      boolean hasRemovedNode = false;

      while (iterator.hasNext()) {
        if (execution.getFailedNodes().contains(((DistributedMember) iterator.next()).getId())) {
          hasRemovedNode = true;
        }
      }

      if (hasRemovedNode) {
        if (retryTime.overMaximum()) {
          PRHARedundancyProvider.timedOut(this, null, null, "doing function execution",
              this.retryTimeout);
          // NOTREACHED
        }
        retryTime.waitToRetryNode();
        memberToBuckets = FunctionExecutionNodePruner.groupByMemberToBuckets(this, bucketSet,
            function.optimizeForWrite());
      } else {
        execution.clearFailedNodes();
      }
    }

    Set<InternalDistributedMember> dest = memberToBuckets.keySet();
    execution.validateExecution(function, dest);
    execution.setExecutionNodes(dest);

    boolean isSelf = false;
    final int[] localBucketSet = memberToBuckets.remove(getMyId());
    if (BucketSetHelper.length(localBucketSet) > 0) {
      isSelf = true;
    }
    final HashMap<InternalDistributedMember, FunctionRemoteContext> recipMap =
        new HashMap<InternalDistributedMember, FunctionRemoteContext>();
    for (InternalDistributedMember recip : memberToBuckets.keySet()) {
      FunctionRemoteContext context = new FunctionRemoteContext(function,
          execution.getArgumentsForMember(recip.getId()), null, memberToBuckets.get(recip),
          execution.isReExecute(), execution.isFnSerializationReqd(), getPrincipal());
      recipMap.put(recip, context);
    }
    final LocalResultCollector<?, ?> localResultCollector =
        execution.getLocalResultCollector(function, rc);
    final DistributionManager dm = getDistributionManager();
    final PartitionedRegionFunctionResultSender resultSender =
        new PartitionedRegionFunctionResultSender(dm, this, 0L, localResultCollector,
            execution.getServerResultSender(), recipMap.isEmpty(), !isSelf,
            execution.isForwardExceptions(), function, localBucketSet);

    // execute locally and collect the result
    if (isSelf && this.dataStore != null) {
      final RegionFunctionContextImpl prContext =
          new RegionFunctionContextImpl(cache, function.getId(), PartitionedRegion.this,
              execution.getArgumentsForMember(getMyId().getId()), null, ColocationHelper
                  .constructAndGetAllColocatedLocalDataSet(PartitionedRegion.this, localBucketSet),
              localBucketSet, resultSender, execution.isReExecute());
      execution.executeFunctionOnLocalPRNode(function, prContext, resultSender, dm, isTX());
    }
    PartitionedRegionFunctionResultWaiter resultReceiver =
        new PartitionedRegionFunctionResultWaiter(getSystem(), this.getPRId(), localResultCollector,
            function, resultSender);

    return resultReceiver.getPartitionedDataFrom(recipMap, this, execution);
  }

  /**
   * @param requestingClient the client requesting the object, or null if not from a client
   * @param allowRetry if false then do not retry
   */
  private Object getFromBucket(final InternalDistributedMember targetNode, int bucketId,
      final Object key, final Object aCallbackArgument, boolean disableCopyOnRead, boolean preferCD,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones, boolean allowRetry) {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    final int retryAttempts = calcRetry();
    Object obj;
    // retry the get remotely until it finds the right node managing the bucket
    int count = 0;
    RetryTimeKeeper retryTime = null;
    InternalDistributedMember retryNode = targetNode;
    while (count <= retryAttempts) {
      // Every continuation should check for DM cancellation
      if (retryNode == null) {
        checkReadiness();
        if (retryTime == null) {
          retryTime = new RetryTimeKeeper(this.retryTimeout);
        }
        retryNode = getNodeForBucketReadOrLoad(bucketId);

        // No storage found for bucket, early out preventing hot loop, bug 36819
        if (retryNode == null) {
          checkShutdown();
          return null;
        }
        continue;
      }
      final boolean isLocal = this.localMaxMemory > 0 && retryNode.equals(getMyId());

      try {
        if (isLocal) {
          obj = this.dataStore.getLocally(bucketId, key, aCallbackArgument, disableCopyOnRead,
              preferCD, requestingClient, clientEvent, returnTombstones, false);
        } else {
          if (this.haveCacheLoader) {
            // If the region has a cache loader,
            // the target node is the primary server of the bucket. But, if the
            // value can be found in a local bucket, we should first try there.

            /* MergeGemXDHDFSToGFE -readoing from local bucket was disabled in GemXD */
            if (null != (obj = getFromLocalBucket(bucketId, key, aCallbackArgument,
                disableCopyOnRead, preferCD, requestingClient, clientEvent, returnTombstones))) {
              return obj;
            }
          }

          obj = getRemotely(retryNode, bucketId, key, aCallbackArgument, preferCD, requestingClient,
              clientEvent, returnTombstones);

          // TODO: there should be better way than this one
          String name = Thread.currentThread().getName();
          if (name.startsWith("ServerConnection") && !getMyId().equals(retryNode)) {
            setNetworkHopType(bucketId, (InternalDistributedMember) retryNode);
          }
        }
        return obj;
      } catch (PRLocallyDestroyedException pde) {
        if (isDebugEnabled) {
          logger.debug("getFromBucket Encountered PRLocallyDestroyedException", pde);
        }
        checkReadiness();
        if (allowRetry) {
          retryNode = getNodeForBucketReadOrLoad(bucketId);
        } else {
          // Only transactions set allowRetry to false,
          // fail the transaction here as region is destroyed.
          Throwable cause = pde.getCause();
          if (cause != null && cause instanceof RegionDestroyedException) {
            throw (RegionDestroyedException) cause;
          } else {
            // Should not see it currently, all current constructors of PRLocallyDestroyedException
            // set the cause to RegionDestroyedException.
            throw new RegionDestroyedException(toString(), getFullPath());
          }
        }
      } catch (ForceReattemptException prce) {
        prce.checkKey(key);
        checkReadiness();

        if (allowRetry) {
          InternalDistributedMember lastNode = retryNode;
          if (isDebugEnabled) {
            logger.debug("getFromBucket: retry attempt: {} of {}", count, retryAttempts, prce);
          }
          retryNode = getNodeForBucketReadOrLoad(bucketId);
          if (lastNode.equals(retryNode)) {
            if (retryTime == null) {
              retryTime = new RetryTimeKeeper(this.retryTimeout);
            }
            if (retryTime.overMaximum()) {
              break;
            }
            if (isDebugEnabled) {
              logger.debug("waiting to retry node {}", retryNode);
            }
            retryTime.waitToRetryNode();
          }
        } else {
          // with transaction
          handleForceReattemptExceptionWithTransaction(prce);
        }
      } catch (PrimaryBucketException notPrimary) {
        if (allowRetry) {
          if (isDebugEnabled) {
            logger.debug("getFromBucket: {} on Node {} not primary",
                notPrimary.getLocalizedMessage(), retryNode);
          }
          getRegionAdvisor().notPrimary(bucketId, retryNode);
          retryNode = getNodeForBucketReadOrLoad(bucketId);
        } else {
          throw notPrimary;
        }
      }

      // It's possible this is a GemFire thread e.g. ServerConnection
      // which got to this point because of a distributed system shutdown or
      // region closure which uses interrupt to break any sleep() or wait()
      // calls
      // e.g. waitForPrimary
      checkShutdown();

      count++;
      if (count == 1) {
        this.prStats.incGetOpsRetried();
      }
      this.prStats.incGetRetries();
      if (isDebugEnabled) {
        logger.debug("getFromBucket: Attempting to resend get to node {} after {} failed attempts",
            retryNode, count);
      }
    } // While

    PartitionedRegionDistributionException e = null; // Fix for bug 36014
    if (logger.isDebugEnabled()) {
      e = new PartitionedRegionDistributionException(
          String.format("No VM available for get in %s attempts",
              count));
    }
    logger.warn(String.format("No VM available for get in %s attempts", count), e);
    return null;
  }

  void handleForceReattemptExceptionWithTransaction(
      ForceReattemptException forceReattemptException) {
    if (forceReattemptException instanceof BucketNotFoundException) {
      throw new TransactionDataRebalancedException(DATA_MOVED_BY_REBALANCE,
          forceReattemptException);
    }
    Throwable cause = forceReattemptException.getCause();
    if (cause instanceof PrimaryBucketException) {
      throw (PrimaryBucketException) cause;
    } else if (cause instanceof TransactionDataRebalancedException) {
      throw (TransactionDataRebalancedException) cause;
    } else if (cause instanceof RegionDestroyedException) {
      throw new TransactionDataRebalancedException(DATA_MOVED_BY_REBALANCE, cause);
    } else {
      throw new TransactionDataRebalancedException(DATA_MOVED_BY_REBALANCE,
          forceReattemptException);
    }
  }

  /**
   * If a bucket is local, try to fetch the value from it
   */
  public Object getFromLocalBucket(int bucketId, final Object key, final Object aCallbackArgument,
      boolean disableCopyOnRead, boolean preferCD, ClientProxyMembershipID requestingClient,
      EntryEventImpl clientEvent, boolean returnTombstones)
      throws ForceReattemptException, PRLocallyDestroyedException {
    Object obj;
    // try reading locally.
    InternalDistributedMember readNode = getNodeForBucketRead(bucketId);
    if (readNode == null) {
      return null; // fixes 51657
    }
    if (readNode.equals(getMyId())
        && null != (obj = this.dataStore.getLocally(bucketId, key, aCallbackArgument,
            disableCopyOnRead, preferCD, requestingClient, clientEvent, returnTombstones, true))) {
      if (logger.isTraceEnabled()) {
        logger.trace("getFromBucket: Getting key {} ({}) locally - success", key, key.hashCode());
      }
      return obj;
    }
    return null;
  }

  /**
   * This invokes a cache writer before a destroy operation. Although it has the same method
   * signature as the method in LocalRegion, it is invoked in a different code path. LocalRegion
   * invokes this method via its "entries" member, while PartitionedRegion invokes this method in
   * its region operation methods and messages.
   */
  @Override
  boolean cacheWriteBeforeRegionDestroy(RegionEventImpl event)
      throws CacheWriterException, TimeoutException {

    if (event.getOperation().isDistributed()) {
      serverRegionDestroy(event);
      CacheWriter localWriter = basicGetWriter();
      Set netWriteRecipients = localWriter == null ? this.distAdvisor.adviseNetWrite() : null;

      if (localWriter == null && (netWriteRecipients == null || netWriteRecipients.isEmpty())) {
        return false;
      }

      final long start = getCachePerfStats().startCacheWriterCall();
      try {
        SearchLoadAndWriteProcessor processor = SearchLoadAndWriteProcessor.getProcessor();
        processor.initialize(this, "preDestroyRegion", null);
        processor.doNetWrite(event, netWriteRecipients, localWriter,
            SearchLoadAndWriteProcessor.BEFOREREGIONDESTROY);
        processor.release();
      } finally {
        getCachePerfStats().endCacheWriterCall(start);
      }
      return true;
    }
    return false;
  }

  /**
   * Test Method: Get the DistributedMember identifier for the vm containing a key
   *
   * @param key the key to look for
   * @return The ID of the DistributedMember holding the key, or null if there is no current mapping
   *         for the key
   */
  public DistributedMember getMemberOwning(Object key) {
    int bucketId = PartitionedRegionHelper.getHashKey(this, null, key, null, null);
    return getNodeForBucketRead(bucketId);
  }

  /**
   * Test Method: Investigate the local cache to determine if it contains a the key
   *
   * @param key The key
   * @return true if the key exists
   * @see LocalRegion#containsKey(Object)
   */
  public boolean localCacheContainsKey(Object key) {
    return getRegionMap().containsKey(key);
  }

  /**
   * Test Method: Fetch a value from the local cache
   *
   * @param key The kye
   * @return the value associated with that key
   * @see LocalRegion#get(Object, Object, boolean, EntryEventImpl)
   */
  public Object localCacheGet(Object key) {
    RegionEntry re = getRegionMap().getEntry(key);
    if (re == null || re.isDestroyedOrRemoved()) {
      return null;
    } else {
      return re.getValue(this); // OFFHEAP: spin until we can copy into a heap cd?
    }
  }

  /**
   * Test Method: Fetch the local cache's key set
   *
   * @return A set of keys
   * @see LocalRegion#keys()
   */
  public Set localCacheKeySet() {
    return super.keys();
  }

  /**
   * Test Method: Get all entries for all copies of a bucket
   *
   * This method will not work correctly if membership in the distributed system changes while the
   * result is being calculated.
   *
   * @return a List of HashMaps, each map being a copy of the entries in a bucket
   */
  public List<BucketDump> getAllBucketEntries(final int bucketId) throws ForceReattemptException {
    if (bucketId >= getTotalNumberOfBuckets()) {
      return Collections.emptyList();
    }
    ArrayList<BucketDump> ret = new ArrayList<BucketDump>();
    HashSet<InternalDistributedMember> collected = new HashSet<InternalDistributedMember>();
    for (;;) {
      // Collect all the candidates by re-examining the advisor...
      Set<InternalDistributedMember> owners = getRegionAdvisor().getBucketOwners(bucketId);
      // Remove ones we've already polled...
      owners.removeAll(collected);

      // Terminate if no more entries
      if (owners.isEmpty()) {
        break;
      }
      // Get first entry
      Iterator<InternalDistributedMember> ownersI = owners.iterator();
      InternalDistributedMember owner = (InternalDistributedMember) ownersI.next();
      // Remove it from our list
      collected.add(owner);

      // If it is ourself, answer directly
      if (owner.equals(getMyId())) {
        BucketRegion br = this.dataStore.handleRemoteGetEntries(bucketId);
        Map<Object, Object> m = new HashMap<Object, Object>() {
          // TODO: clean this up -- outer class is not serializable
          private static final long serialVersionUID = 0L;

          @Override
          public String toString() {
            return "Bucket id = " + bucketId + " from local member = "
                + getDistributionManager().getDistributionManagerId() + ": " + super.toString();
          }
        };

        Map<Object, VersionTag> versions = new HashMap<Object, VersionTag>();

        for (Iterator<Map.Entry> it = br.entrySet().iterator(); it.hasNext();) {
          NonTXEntry entry = (NonTXEntry) it.next();
          RegionEntry re = entry.getRegionEntry();
          Object value = re.getValue(br); // OFFHEAP: incrc, deserialize, decrc
          VersionStamp versionStamp = re.getVersionStamp();
          VersionTag versionTag = versionStamp != null ? versionStamp.asVersionTag() : null;
          if (versionTag != null) {
            versionTag.replaceNullIDs(br.getVersionMember());
          }
          if (Token.isRemoved(value)) {
            continue;
          } else if (Token.isInvalid(value)) {
            value = null;
          } else if (value instanceof CachedDeserializable) {
            value = ((CachedDeserializable) value).getDeserializedForReading();
          }
          m.put(re.getKey(), value);
          versions.put(re.getKey(), versionTag);
        }
        RegionVersionVector rvv = br.getVersionVector();
        rvv = rvv != null ? rvv.getCloneForTransmission() : null;
        ret.add(new BucketDump(bucketId, owner, rvv, m, versions));
        continue;
      }

      // Send a message
      try {
        final FetchEntriesResponse r;
        r = FetchEntriesMessage.send(owner, this, bucketId);
        ret.add(r.waitForEntries());
      } catch (ForceReattemptException ignore) {
        // node has departed? Ignore.
      }
    } // for

    return ret;
  }

  /**
   * Fetch the keys for the given bucket identifier, if the bucket is local or remote.
   *
   * @return A set of keys from bucketNum or {@link Collections#EMPTY_SET}if no keys can be found.
   */
  public Set getBucketKeys(int bucketNum) {
    return getBucketKeys(bucketNum, false);
  }

  /**
   * Fetch the keys for the given bucket identifier, if the bucket is local or remote. This version
   * of the method allows you to retrieve Tombstone entries as well as undestroyed entries.
   *
   * @param allowTombstones whether to include destroyed entries in the result
   * @return A set of keys from bucketNum or {@link Collections#EMPTY_SET}if no keys can be found.
   */
  public Set getBucketKeys(int bucketNum, boolean allowTombstones) {
    int buck = bucketNum;
    final int retryAttempts = calcRetry();
    Set ret = null;
    int count = 0;
    InternalDistributedMember nod = getOrCreateNodeForBucketRead(bucketNum);
    RetryTimeKeeper snoozer = null;
    while (count <= retryAttempts) {
      // It's possible this is a GemFire thread e.g. ServerConnection
      // which got to this point because of a distributed system shutdown or
      // region closure which uses interrupt to break any sleep() or wait()
      // calls
      // e.g. waitForPrimary or waitForBucketRecovery
      checkShutdown();

      if (nod == null) {
        if (snoozer == null) {
          snoozer = new RetryTimeKeeper(this.retryTimeout);
        }
        nod = getOrCreateNodeForBucketRead(bucketNum);

        // No storage found for bucket, early out preventing hot loop, bug 36819
        if (nod == null) {
          checkShutdown();
          break;
        }
        count++;
        continue;
      }

      try {
        if (nod.equals(getMyId())) {
          ret = this.dataStore.getKeysLocally(buck, allowTombstones);
        } else {
          FetchKeysResponse r = FetchKeysMessage.send(nod, this, buck, allowTombstones);
          ret = r.waitForKeys();
        }
        if (ret != null) {
          return ret;
        }
      } catch (PRLocallyDestroyedException ignore) {
        if (logger.isDebugEnabled()) {
          logger.debug("getBucketKeys: Encountered PRLocallyDestroyedException");
        }
        checkReadiness();
      } catch (ForceReattemptException prce) {
        if (logger.isDebugEnabled()) {
          logger.debug("getBucketKeys: attempt:{}", (count + 1), prce);
        }
        checkReadiness();
        if (snoozer == null) {
          snoozer = new RetryTimeKeeper(this.retryTimeout);
        }
        InternalDistributedMember oldNode = nod;
        nod = getNodeForBucketRead(buck);
        if (nod != null && nod.equals(oldNode)) {
          if (snoozer.overMaximum()) {
            checkReadiness();
            throw new TimeoutException(
                String.format(
                    "Attempt to acquire primary node for read on bucket %s timed out in %s ms",
                    new Object[] {getBucketName(buck), snoozer.getRetryTime()}));
          }
          snoozer.waitToRetryNode();
        }

      }
      count++;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("getBucketKeys: no keys found returning empty set");
    }
    return Collections.emptySet();
  }

  /**
   * Fetches entries from local and remote nodes and appends these to register-interest response.
   */
  public void fetchEntries(HashMap<Integer, HashSet<Object>> bucketKeys, VersionedObjectList values,
      ServerConnection servConn) throws IOException {
    int retryAttempts = calcRetry();
    RetryTimeKeeper retryTime = null;
    HashMap<Integer, HashSet> failures = new HashMap<Integer, HashSet>(bucketKeys);
    HashMap<InternalDistributedMember, HashMap<Integer, HashSet>> nodeToBuckets =
        new HashMap<InternalDistributedMember, HashMap<Integer, HashSet>>();

    while (--retryAttempts >= 0 && !failures.isEmpty()) {
      nodeToBuckets.clear();
      updateNodeToBucketMap(nodeToBuckets, failures);
      failures.clear();

      HashMap<Integer, HashSet> localBuckets = nodeToBuckets.remove(getMyId());
      if (localBuckets != null && !localBuckets.isEmpty()) {
        Set keys = new HashSet();
        for (Integer id : localBuckets.keySet()) {
          keys.addAll(localBuckets.get(id));
        }
        if (!keys.isEmpty()) {
          BaseCommand.appendNewRegisterInterestResponseChunkFromLocal(this, values, "keyList", keys,
              servConn);
        }
      }

      // Handle old nodes for Rolling Upgrade support
      Set<Integer> failedSet = handleOldNodes(nodeToBuckets, values, servConn);
      // Add failed buckets to nodeToBuckets map so that these will be tried on
      // remote nodes.
      if (!failedSet.isEmpty()) {
        for (Integer bId : failedSet) {
          failures.put(bId, bucketKeys.get(bId));
        }
        updateNodeToBucketMap(nodeToBuckets, failures);
        failures.clear();
      }

      fetchRemoteEntries(nodeToBuckets, failures, values, servConn);
      if (!failures.isEmpty()) {
        if (retryTime == null) {
          retryTime = new RetryTimeKeeper(this.retryTimeout);
        }
        if (!waitForFetchRemoteEntriesRetry(retryTime)) {
          break;
        }
      }
    }
    if (!failures.isEmpty()) {
      throw new InternalGemFireException("Failed to fetch entries from " + failures.size()
          + " buckets of region " + getName() + " for register interest.");
    }
  }

  void updateNodeToBucketMap(
      HashMap<InternalDistributedMember, HashMap<Integer, HashSet>> nodeToBuckets,
      HashMap<Integer, HashSet> bucketKeys) {
    for (int id : bucketKeys.keySet()) {
      InternalDistributedMember node = getOrCreateNodeForBucketWrite(id, null);
      if (nodeToBuckets.containsKey(node)) {
        nodeToBuckets.get(node).put(id, bucketKeys.get(id));
      } else {
        HashMap<Integer, HashSet> map = new HashMap<Integer, HashSet>();
        map.put(id, bucketKeys.get(id));
        nodeToBuckets.put(node, map);
      }
    }
  }

  /**
   * @return set of bucket-ids that could not be read from.
   */
  private Set<Integer> handleOldNodes(HashMap nodeToBuckets, VersionedObjectList values,
      ServerConnection servConn) throws IOException {
    Set<Integer> failures = new HashSet<Integer>();
    HashMap oldFellas = filterOldMembers(nodeToBuckets);
    for (Iterator it = oldFellas.entrySet().iterator(); it.hasNext();) {
      Map.Entry e = (Map.Entry) it.next();
      InternalDistributedMember member = (InternalDistributedMember) e.getKey();
      Object bucketInfo = e.getValue();

      HashMap<Integer, HashSet> bucketKeys = null;
      Set<Integer> buckets = null;

      if (bucketInfo instanceof Set) {
        buckets = (Set<Integer>) bucketInfo;
      } else {
        bucketKeys = (HashMap<Integer, HashSet>) bucketInfo;
        buckets = bucketKeys.keySet();
      }

      fetchKeysAndValues(values, servConn, failures, member, bucketKeys, buckets);
    }
    return failures;
  }

  void fetchKeysAndValues(VersionedObjectList values, ServerConnection servConn,
      Set<Integer> failures, InternalDistributedMember member,
      HashMap<Integer, HashSet> bucketKeys, Set<Integer> buckets)
      throws IOException {
    for (Integer bucket : buckets) {
      Set keys = null;
      if (bucketKeys == null) {
        try {
          FetchKeysResponse fetchKeysResponse = getFetchKeysResponse(member, bucket);
          keys = fetchKeysResponse.waitForKeys();
        } catch (ForceReattemptException ignore) {
          failures.add(bucket);
        }
      } else {
        keys = bucketKeys.get(bucket);
      }
      if (keys != null) {
        getValuesForKeys(values, servConn, keys);
      }
    }
  }

  FetchKeysResponse getFetchKeysResponse(InternalDistributedMember member,
      Integer bucket)
      throws ForceReattemptException {
    return FetchKeysMessage.send(member, this, bucket, true);
  }

  void getValuesForKeys(VersionedObjectList values, ServerConnection servConn, Set keys)
      throws IOException {
    // TODO (ashetkar) Use single Get70 instance for all?
    for (Object key : keys) {
      Get70 command = (Get70) Get70.getCommand();
      Get70.Entry ge = command.getValueAndIsObject(this, key, null, servConn);

      if (ge.keyNotPresent) {
        values.addObjectPartForAbsentKey(key, ge.value, ge.versionTag);
      } else {
        values.addObjectPart(key, ge.value, ge.isObject, ge.versionTag);
      }

      if (values.size() == BaseCommand.MAXIMUM_CHUNK_SIZE) {
        BaseCommand.sendNewRegisterInterestResponseChunk(this, "keyList", values, false,
            servConn);
        values.clear();
      }
    }
  }

  /**
   * @param nodeToBuckets A map with InternalDistributedSystem as key and either HashSet or
   *        HashMap<Integer, HashSet> as value.
   * @return Map of <old members, set/map of bucket ids they host>.
   */
  private HashMap filterOldMembers(HashMap nodeToBuckets) {
    ClusterDistributionManager dm = (ClusterDistributionManager) getDistributionManager();
    HashMap oldGuys = new HashMap();

    Set<InternalDistributedMember> oldMembers =
        new HashSet<InternalDistributedMember>(nodeToBuckets.keySet());
    dm.removeMembersWithSameOrNewerVersion(oldMembers, KnownVersion.CURRENT);
    Iterator<InternalDistributedMember> oldies = oldMembers.iterator();
    while (oldies.hasNext()) {
      InternalDistributedMember old = oldies.next();
      if (nodeToBuckets.containsKey(old)) {
        oldGuys.put(old, nodeToBuckets.remove(old));
      } else {
        oldies.remove();
      }
    }

    return oldGuys;
  }

  /**
   * Fetches entries from local and remote nodes and appends these to register-interest response.
   */
  public void fetchEntries(String regex, VersionedObjectList values, ServerConnection servConn)
      throws IOException {
    int retryAttempts = calcRetry();
    RetryTimeKeeper retryTime = null;
    HashSet<Integer> failures = new HashSet<Integer>(getRegionAdvisor().getBucketSet());
    HashMap<InternalDistributedMember, HashSet<Integer>> nodeToBuckets =
        new HashMap<InternalDistributedMember, HashSet<Integer>>();

    while (--retryAttempts >= 0 && !failures.isEmpty()) {
      nodeToBuckets.clear();
      updateNodeToBucketMap(nodeToBuckets, failures);
      failures.clear();

      HashSet<Integer> localBuckets = nodeToBuckets.remove(getMyId());

      if (localBuckets != null && !localBuckets.isEmpty()) {
        for (Integer id : localBuckets) {
          Set keys = fetchAllLocalKeys(id, failures, regex);
          if (!keys.isEmpty()) {
            BaseCommand.appendNewRegisterInterestResponseChunkFromLocal(this, values,
                regex != null ? regex : "ALL_KEYS", keys, servConn);
          }
        }
      }
      // Add failed buckets to nodeToBuckets map so that these will be tried on
      // remote nodes.
      updateNodeToBucketMap(nodeToBuckets, failures);
      failures.clear();

      // Handle old nodes for Rolling Upgrade support
      Set<Integer> ret = handleOldNodes(nodeToBuckets, values, servConn);
      if (!ret.isEmpty()) {
        failures.addAll(ret);
        updateNodeToBucketMap(nodeToBuckets, failures);
        failures.clear();
      }

      localBuckets = nodeToBuckets.remove(getMyId());
      if (localBuckets != null && !localBuckets.isEmpty()) {
        failures.addAll(localBuckets);
        // updateNodeToBucketMap(nodeToBuckets, failures);
        // failures.clear();
      }

      fetchAllRemoteEntries(nodeToBuckets, failures, regex, values, servConn);
      if (!failures.isEmpty()) {
        if (retryTime == null) {
          retryTime = new RetryTimeKeeper(this.retryTimeout);
        }
        if (!waitForFetchRemoteEntriesRetry(retryTime)) {
          break;
        }
      }
    }
    if (!failures.isEmpty()) {
      throw new InternalGemFireException("Failed to fetch entries from " + failures.size()
          + " buckets of region " + getName() + " for register interest.");
    }
  }

  void updateNodeToBucketMap(
      HashMap<InternalDistributedMember, HashSet<Integer>> nodeToBuckets, Set<Integer> buckets) {
    for (int id : buckets) {
      InternalDistributedMember node = getOrCreateNodeForBucketWrite(id, null);
      if (nodeToBuckets.containsKey(node)) {
        nodeToBuckets.get(node).add(id);
      } else {
        HashSet<Integer> set = new HashSet<Integer>();
        set.add(id);
        nodeToBuckets.put(node, set);
      }
    }
  }

  /**
   * @return boolean False indicates caller should stop re-trying.
   */
  private boolean waitForFetchRemoteEntriesRetry(RetryTimeKeeper retryTime) {
    if (retryTime.overMaximum()) {
      return false;
    }
    retryTime.waitToRetryNode();
    return true;
  }

  public Set fetchAllLocalKeys(Integer id, Set<Integer> failures, String regex) {
    Set result = new HashSet();
    try {
      Set keys = null;
      if (regex != null) {
        keys = this.dataStore.handleRemoteGetKeys(id, InterestType.REGULAR_EXPRESSION, regex, true);
      } else {
        keys = this.dataStore.getKeysLocally(id, true);
      }
      result.addAll(keys);
    } catch (ForceReattemptException ignore) {
      failures.add(id);
    } catch (PRLocallyDestroyedException ignore) {
      failures.add(id);
    }
    return result;
  }

  /**
   * Sends FetchBulkEntriesMessage to each of the nodes hosting the buckets, unless the nodes are
   * older than 8.0
   */
  public void fetchRemoteEntries(
      HashMap<InternalDistributedMember, HashMap<Integer, HashSet>> nodeToBuckets,
      HashMap<Integer, HashSet> failures, VersionedObjectList values, ServerConnection servConn)
      throws IOException {
    Set result = null;
    HashMap<Integer, HashSet> oneBucketKeys = new HashMap<Integer, HashSet>();

    for (Iterator<Map.Entry<InternalDistributedMember, HashMap<Integer, HashSet>>> itr =
        nodeToBuckets.entrySet().iterator(); itr.hasNext();) {
      Map.Entry<InternalDistributedMember, HashMap<Integer, HashSet>> entry = itr.next();
      HashMap<Integer, HashSet> bucketKeys = entry.getValue();
      FetchBulkEntriesResponse fber = null;
      result = new HashSet();

      // Fetch one bucket-data at a time to avoid this VM running out of memory.
      // See #50647
      for (Map.Entry<Integer, HashSet> e : bucketKeys.entrySet()) {
        result.clear();
        oneBucketKeys.clear();
        oneBucketKeys.put(e.getKey(), e.getValue());
        try {
          if (entry.getKey().getVersion().isOlderThan(KnownVersion.GFE_80)) {
            failures.putAll(nodeToBuckets.get(entry.getKey()));
            continue;
          }
          fber =
              FetchBulkEntriesMessage.send(entry.getKey(), this, oneBucketKeys, null, null, true);

          BucketDump[] bds = fber.waitForEntries();
          if (fber.getFailedBucketIds() != null && !fber.getFailedBucketIds().isEmpty()) {
            for (int id : fber.getFailedBucketIds()) {
              failures.put(id, nodeToBuckets.get(entry.getKey()).get(id));
            }
          }
          for (BucketDump bd : bds) {
            result.addAll(bd.getValuesWithVersions().entrySet());
          }

          BaseCommand.appendNewRegisterInterestResponseChunk(this, values, "keyList", result,
              servConn);

        } catch (ForceReattemptException ignore) {
          failures.put(e.getKey(), e.getValue());
        }
      }
    }
  }

  /**
   * Sends FetchBulkEntriesMessage to each of the nodes hosting the buckets, unless the nodes are
   * older than 8.0
   */
  public void fetchAllRemoteEntries(
      HashMap<InternalDistributedMember, HashSet<Integer>> nodeToBuckets, HashSet<Integer> failures,
      String regex, VersionedObjectList values, ServerConnection servConn) throws IOException {
    Set result = null;
    HashSet<Integer> bucketId = new HashSet<Integer>();

    for (Iterator<Map.Entry<InternalDistributedMember, HashSet<Integer>>> itr =
        nodeToBuckets.entrySet().iterator(); itr.hasNext();) {
      Map.Entry<InternalDistributedMember, HashSet<Integer>> entry = itr.next();
      HashSet<Integer> buckets = new HashSet<Integer>(entry.getValue()); // Is it needed to copy the
                                                                         // set here?
      FetchBulkEntriesResponse fber = null;
      result = new HashSet();

      // Fetch one bucket-data at a time to avoid this VM running out of memory.
      // See #50647
      for (int bucket : buckets) {
        result.clear();
        bucketId.clear();
        bucketId.add(bucket);
        try {
          if (entry.getKey().getVersion().isOlderThan(KnownVersion.GFE_80)) {
            failures.addAll(nodeToBuckets.get(entry.getKey()));
            continue;
          }
          fber = FetchBulkEntriesMessage.send(entry.getKey(), this, null, bucketId, regex, true);

          BucketDump[] bds = fber.waitForEntries();
          if (fber.getFailedBucketIds() != null) {
            failures.addAll(fber.getFailedBucketIds());
          }
          for (BucketDump bd : bds) {
            result.addAll(bd.getValuesWithVersions().entrySet());
          }
          BaseCommand.appendNewRegisterInterestResponseChunk(this, values,
              regex != null ? regex : "ALL_KEYS", result, servConn);

        } catch (ForceReattemptException ignore) {
          failures.add(bucket);
        }
      }
    }
  }

  /**
   * Test Method: Get all {@link InternalDistributedMember}s known by this instance of the
   * PartitionedRegion. Note: A member is recognized as such when it partiticpates as a "data
   * store".
   *
   * @return a {@code HashSet} of {@link InternalDistributedMember}s or an empty {@code HashSet}
   */
  public Set<InternalDistributedMember> getAllNodes() {
    Set<InternalDistributedMember> result = getRegionAdvisor().adviseDataStore(true);
    if (this.isDataStore()) {
      result.add(getDistributionManager().getId());
    }
    return result;
  }

  /**
   * Test Method: Get the number of entries in the local data store.
   */
  public long getLocalSizeForTest() {
    if (this.dataStore == null) {
      return 0L;
    }
    long ret = 0L;
    Integer i;
    for (Iterator si = this.dataStore.getSizeLocally().values().iterator(); si.hasNext();) {
      i = (Integer) si.next();
      ret += i;
    }
    return ret;
  }

  /**
   * Gets the remote object with the given key.
   *
   * @param targetNode the Node hosting the key
   * @param bucketId the id of the bucket the key hashed into
   * @param key the key
   * @param requestingClient the client that made this request
   * @param clientEvent client event for carrying version information. Null if not a client
   *        operation
   * @param returnTombstones TODO
   * @return the value
   * @throws PrimaryBucketException if the peer is no longer the primary
   * @throws ForceReattemptException if the peer is no longer available
   */
  public Object getRemotely(InternalDistributedMember targetNode, int bucketId, final Object key,
      final Object aCallbackArgument, boolean preferCD, ClientProxyMembershipID requestingClient,
      EntryEventImpl clientEvent, boolean returnTombstones)
      throws PrimaryBucketException, ForceReattemptException {
    Object value;
    if (logger.isDebugEnabled()) {
      logger.debug("PartitionedRegion#getRemotely: getting value from bucketId={}{}{} for key {}",
          getPRId(), BUCKET_ID_SEPARATOR, bucketId, key);
    }
    GetResponse response = GetMessage.send(targetNode, this, key, aCallbackArgument,
        requestingClient, returnTombstones);
    this.prStats.incPartitionMessagesSent();
    value = response.waitForResponse(preferCD);
    if (clientEvent != null) {
      clientEvent.setVersionTag(response.getVersionTag());
    }
    if (logger.isDebugEnabled()) {
      logger.debug("getRemotely: got value {} for key {}", value, key);
    }
    return value;
  }

  private ResultCollector executeFunctionOnRemoteNode(InternalDistributedMember targetNode,
      final Function function, final Object object, final Set routingKeys, ResultCollector rc,
      int[] bucketArray, ServerToClientFunctionResultSender sender, AbstractExecution execution) {
    PartitionedRegionFunctionResultSender resultSender =
        new PartitionedRegionFunctionResultSender(null, this, 0, rc, sender, false, true,
            execution.isForwardExceptions(), function, bucketArray);

    PartitionedRegionFunctionResultWaiter resultReceiver =
        new PartitionedRegionFunctionResultWaiter(getSystem(), this.getPRId(), rc, function,
            resultSender);

    FunctionRemoteContext context = new FunctionRemoteContext(function, object, routingKeys,
        bucketArray, execution.isReExecute(), execution.isFnSerializationReqd(), getPrincipal());

    HashMap<InternalDistributedMember, FunctionRemoteContext> recipMap =
        new HashMap<InternalDistributedMember, FunctionRemoteContext>();

    recipMap.put(targetNode, context);

    return resultReceiver.getPartitionedDataFrom(recipMap, this, execution);
  }

  /**
   * This method returns Partitioned Region data store associated with this Partitioned Region
   *
   */
  public PartitionedRegionDataStore getDataStore() {
    return this.dataStore;
  }

  @Override
  boolean canStoreDataLocally() {
    return getDataStore() != null;
  }

  /**
   * Grab the PartitionedRegionID Lock, this MUST be done in a try block since it may throw an
   * exception
   *
   * @return true if the lock was acquired
   */
  private static boolean grabPRIDLock(final DistributedLockService lockService) {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    boolean ownership = false;
    int n = 0;
    while (!ownership) {
      if (isDebugEnabled) {
        logger.debug("grabPRIDLock: Trying to get the dlock in allPartitionedRegions for {}: {}",
            PartitionedRegionHelper.MAX_PARTITIONED_REGION_ID, (n + 1));
      }
      ownership = lockService.lock(PartitionedRegionHelper.MAX_PARTITIONED_REGION_ID,
          VM_OWNERSHIP_WAIT_TIME, -1);
    }
    return ownership;
  }

  private static void releasePRIDLock(final DistributedLockService lockService) {
    try {
      lockService.unlock(PartitionedRegionHelper.MAX_PARTITIONED_REGION_ID);
      if (logger.isDebugEnabled()) {
        logger.debug("releasePRIDLock: Released the dlock in allPartitionedRegions for {}",
            PartitionedRegionHelper.MAX_PARTITIONED_REGION_ID);
      }
    } catch (Exception es) {
      logger.warn(String.format("releasePRIDLock: unlocking %s caught an exception",
          PartitionedRegionHelper.MAX_PARTITIONED_REGION_ID), es);
    }
  }

  /**
   * generates new partitioned region ID globally.
   */
  public int generatePRId(InternalDistributedSystem sys) {
    final DistributedLockService lockService = getPartitionedRegionLockService();
    return _generatePRId(sys, lockService);
  }

  private static int _generatePRId(InternalDistributedSystem sys,
      DistributedLockService lockService) {
    boolean ownership = false;
    int prid = 0;

    try {
      ownership = grabPRIDLock(lockService);
      if (ownership) {
        if (logger.isDebugEnabled()) {
          logger.debug("generatePRId: Got the dlock in allPartitionedRegions for {}",
              PartitionedRegionHelper.MAX_PARTITIONED_REGION_ID);
        }

        Set parMembers = sys.getDistributionManager().getOtherDistributionManagerIds();

        Integer currentPRID;

        IdentityResponse pir = IdentityRequestMessage.send(parMembers, sys);
        currentPRID = pir.waitForId();

        if (currentPRID == null) {
          currentPRID = 0;
        }
        prid = currentPRID + 1;
        currentPRID = prid;

        try {
          IdentityUpdateResponse pr = IdentityUpdateMessage.send(parMembers, sys, currentPRID);
          pr.waitForRepliesUninterruptibly();
        } catch (ReplyException e) {
          if (logger.isDebugEnabled()) {
            logger.debug("generatePRId: Ignoring exception", e);
          }
        }
      }
    } finally {
      if (ownership) {
        releasePRIDLock(lockService);
      }
    }
    return prid;
  }

  @Override
  public DistributionAdvisor getDistributionAdvisor() {
    return this.distAdvisor;
  }

  @Override
  public CacheDistributionAdvisor getCacheDistributionAdvisor() {
    return this.distAdvisor;
  }

  public RegionAdvisor getRegionAdvisor() {
    return this.distAdvisor;
  }

  /** Returns the distribution profile; lazily creates one if needed */
  @Override
  public Profile getProfile() {
    return this.distAdvisor.createProfile();
  }

  @Override
  public void fillInProfile(Profile p) {
    CacheProfile profile = (CacheProfile) p;
    // set fields on CacheProfile...
    profile.isPartitioned = true;
    profile.isPersistent = getDataPolicy().withPersistence();
    profile.dataPolicy = getDataPolicy();
    profile.hasCacheLoader = basicGetLoader() != null;
    profile.hasCacheWriter = basicGetWriter() != null;
    profile.hasCacheListener = hasListener();
    Assert.assertTrue(getScope().isDistributed());
    profile.scope = getScope();
    profile.setSubscriptionAttributes(getSubscriptionAttributes());
    // fillInProfile MUST set serialNumber
    profile.serialNumber = getSerialNumber();

    // TODO - prpersist - this is a bit of a hack, but we're
    // reusing this boolean to indicate that this member has finished disk recovery.
    profile.regionInitialized = recoveredFromDisk;

    profile.hasCacheServer = ((this.cache.getCacheServers().size() > 0) ? true : false);
    profile.filterProfile = getFilterProfile();
    profile.gatewaySenderIds = getGatewaySenderIds();
    profile.asyncEventQueueIds = getVisibleAsyncEventQueueIds();

    if (getDataPolicy().withPersistence()) {
      profile.persistentID = getDiskStore().generatePersistentID();
    }

    fillInProfile((PartitionProfile) profile);

    for (CacheServiceProfile csp : getCacheServiceProfiles().values()) {
      profile.addCacheServiceProfile(csp);
    }

    profile.isOffHeap = getOffHeap();
  }

  /** set fields that are only in PartitionProfile... */
  public void fillInProfile(PartitionProfile profile) {
    // both isDataStore and numBuckets are not required for sending purposes,
    // but nice to have for toString debugging
    profile.isDataStore = getLocalMaxMemory() > 0;
    if (this.dataStore != null) {
      profile.numBuckets = this.dataStore.getBucketsManaged();
    }

    profile.requiresNotification = this.requiresNotification;
    profile.localMaxMemory = getLocalMaxMemory();
    profile.fixedPAttrs = this.fixedPAttrs;
    // shutdownAll
    profile.shutDownAllStatus = shutDownAllStatus;
  }

  @Override
  void initialized() {
    // PartitionedRegions do not send out a profile at the end of
    // initialization. It is not currently needed by other members
    // since no GII is done on a PartitionedRegion
  }

  @Override
  protected void cacheListenersChanged(boolean nowHasListener) {
    if (nowHasListener) {
      this.advisorListener.initRMLWrappers();
    }
    new UpdateAttributesProcessor(this).distribute();
  }

  // propagate the new writer to the data store
  @Override
  protected void cacheWriterChanged(CacheWriter p_oldWriter) {
    super.cacheWriterChanged(p_oldWriter);
    if (p_oldWriter == null ^ basicGetWriter() == null) {
      updatePRNodeInformation();
      new UpdateAttributesProcessor(this).distribute();
    }
  }

  // propagate the new loader to the data store
  @Override
  protected void cacheLoaderChanged(CacheLoader oldLoader) {
    this.dataStore.cacheLoaderChanged(basicGetLoader(), oldLoader);
    super.cacheLoaderChanged(oldLoader);
    if (oldLoader == null ^ basicGetLoader() == null) {
      updatePRNodeInformation();
      new UpdateAttributesProcessor(this).distribute();
    }
  }

  /**
   * This method returns PartitionedRegion associated with a PartitionedRegion ID from prIdToPR map.
   *
   * @param prid Partitioned Region ID
   */
  public static PartitionedRegion getPRFromId(int prid) throws PRLocallyDestroyedException {
    final Object o;
    synchronized (prIdToPR) {
      o = prIdToPR.getRegion(prid);
    }
    return (PartitionedRegion) o;
  }

  /**
   * Verify that the given prId is correct for the given region name in this vm
   *
   * @param sender the member requesting validation
   * @param prId the ID being used for the pr by the sender
   * @param regionId the regionIdentifier used for prId by the sender
   */
  public static void validatePRID(InternalDistributedMember sender, int prId, String regionId) {
    try {
      PartitionedRegion pr = null;
      synchronized (prIdToPR) {
        // first do a quick probe
        pr = (PartitionedRegion) prIdToPR.getRegion(prId);
      }
      if (pr != null && !pr.isLocallyDestroyed && pr.getRegionIdentifier().equals(regionId)) {
        return;
      }
    } catch (RegionDestroyedException ignore) {
      // ignore and do full pass over prid map
    } catch (PartitionedRegionException ignore) {
      // ditto
    } catch (PRLocallyDestroyedException ignore) {
      // ignore and do full check
    }
    synchronized (prIdToPR) {
      for (Iterator it = prIdToPR.values().iterator(); it.hasNext();) {
        Object o = it.next();
        if (o instanceof String) {
          continue;
        }
        PartitionedRegion pr = (PartitionedRegion) o;
        if (pr.getPRId() == prId) {
          if (!pr.getRegionIdentifier().equals(regionId)) {
            logger.warn("{} is using PRID {} for {} but this process maps that PRID to {}",
                new Object[] {sender.toString(), prId, pr.getRegionIdentifier()});
          }
        } else if (pr.getRegionIdentifier().equals(regionId)) {
          logger.warn("{} is using PRID {} for {} but this process is using PRID {}",
              new Object[] {sender, prId, pr.getRegionIdentifier(), pr.getPRId()});
        }
      }
    }
  }

  public static String dumpPRId() {
    return prIdToPR.dump();
  }

  public String dumpAllPartitionedRegions() {
    StringBuilder sb = new StringBuilder(getPRRoot().getFullPath());
    sb.append("\n");
    Object key = null;
    for (Iterator i = getPRRoot().keySet().iterator(); i.hasNext();) {
      key = i.next();
      sb.append(key).append("=>").append(getPRRoot().get(key));
      if (i.hasNext()) {
        sb.append("\n");
      }
    }
    return sb.toString();
  }

  /**
   * This method returns prId
   *
   */
  public int getPRId() {
    return this.partitionedRegionId;
  }

  /**
   * Updates local cache with a new value.
   *
   * @param key the key
   * @param value the value
   * @param newVersion the new version of the key
   */
  void updateLocalCache(Object key, Object value, long newVersion) {

  }

  /**
   * This method returns total number of buckets for this PR
   *
   */
  public int getTotalNumberOfBuckets() {

    return this.totalNumberOfBuckets;
  }

  @Override
  public void basicDestroy(final EntryEventImpl event, final boolean cacheWrite,
      final Object expectedOldValue)
      throws TimeoutException, EntryNotFoundException, CacheWriterException {

    final long startTime = prStats.getTime();
    try {
      if (event.getEventId() == null) {
        event.setNewEventId(this.cache.getDistributedSystem());
      }
      discoverJTA();
      getDataView().destroyExistingEntry(event, cacheWrite, expectedOldValue);
    } catch (RegionDestroyedException rde) {
      if (!rde.getRegionFullPath().equals(getFullPath())) {
        // Handle when a bucket is destroyed
        throw new RegionDestroyedException(toString(), getFullPath(), rde);
      }
    } finally {
      this.prStats.endDestroy(startTime);
    }
  }

  /**
   * @param expectedOldValue only succeed if current value is equal to expectedOldValue
   * @throws EntryNotFoundException if entry not found or if expectedOldValue not null and current
   *         value was not equal to expectedOldValue
   */
  public void destroyInBucket(final EntryEventImpl event, Object expectedOldValue)
      throws EntryNotFoundException, CacheWriterException {
    // Get the bucket id for the key
    final Integer bucketId = event.getKeyInfo().getBucketId();
    assert bucketId != KeyInfo.UNKNOWN_BUCKET;
    // check in bucket2Node region
    final InternalDistributedMember targetNode = getOrCreateNodeForBucketWrite(bucketId, null);

    if (logger.isDebugEnabled()) {
      logger.debug("destroyInBucket: key={} ({}) in node {} to bucketId={} retry={} ms",
          event.getKey(), event.getKey().hashCode(), targetNode, bucketStringForLogs(bucketId),
          retryTimeout);
    }

    // retry the put remotely until it finds the right node managing the bucket
    RetryTimeKeeper retryTime = null;
    InternalDistributedMember currentTarget = targetNode;
    long timeOut = 0;
    int count = 0;
    for (;;) {
      switch (count) {
        case 0:
          // Note we don't check for DM cancellation in common case.
          // First time, keep going
          break;
        case 1:
          // First failure
          this.cache.getCancelCriterion().checkCancelInProgress(null);
          timeOut = System.currentTimeMillis() + this.retryTimeout;
          break;
        default:
          this.cache.getCancelCriterion().checkCancelInProgress(null);
          // test for timeout
          long timeLeft = timeOut - System.currentTimeMillis();
          if (timeLeft < 0) {
            PRHARedundancyProvider.timedOut(this, null, null, "destroy an entry",
                this.retryTimeout);
            // NOTREACHED
          }

          // Didn't time out. Sleep a bit and then continue
          boolean interrupted = Thread.interrupted();
          try {
            Thread.sleep(PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION);
          } catch (InterruptedException ignore) {
            interrupted = true;
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
          break;
      }
      count++;

      if (currentTarget == null) { // pick target
        checkReadiness();

        if (retryTime == null) {
          retryTime = new RetryTimeKeeper(this.retryTimeout);
        }
        if (retryTime.overMaximum()) {
          // if (this.getNodeList(bucketId) == null
          // || this.getNodeList(bucketId).size() == 0) {
          // throw new EntryNotFoundException("Entry not found for key "
          // + event.getKey());
          // }
          if (getRegionAdvisor().getBucket(bucketId).getBucketAdvisor()
              .basicGetPrimaryMember() == null) {
            throw new EntryNotFoundException(
                String.format("Entry not found for key %s",
                    event.getKey()));
          }
          TimeoutException e = new TimeoutException(
              String.format("Time out looking for target node for destroy; waited %s ms",
                  retryTime.getRetryTime()));
          if (logger.isDebugEnabled()) {
            logger.debug(e.getMessage(), e);
          }
          checkReadiness();
          throw e;
        }

        currentTarget = getOrCreateNodeForBucketWrite(bucketId, retryTime);

        // No storage found for bucket, early out preventing hot loop, bug 36819
        if (currentTarget == null) {
          checkEntryNotFound(event.getKey());
        }
        continue;
      } // pick target

      final boolean isLocal = (this.localMaxMemory > 0) && currentTarget.equals(getMyId());
      try {

        DistributedRemoveAllOperation savedOp = event.setRemoveAllOperation(null);
        if (savedOp != null) {
          savedOp.addEntry(event, bucketId);
          return;
        }
        if (isLocal) {
          // doCacheWriteBeforeDestroy(event);
          event.setInvokePRCallbacks(true);
          this.dataStore.destroyLocally(bucketId, event, expectedOldValue);
        } else {
          if (event.isBridgeEvent()) {
            setNetworkHopType(bucketId, currentTarget);
          }
          destroyRemotely(currentTarget, bucketId, event, expectedOldValue);
        }
        return;

        // NOTREACHED (success)
      } catch (ConcurrentCacheModificationException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("destroyInBucket: caught concurrent cache modification exception", e);
        }
        event.isConcurrencyConflict(true);

        if (logger.isTraceEnabled()) {
          logger.trace(
              "ConcurrentCacheModificationException received for destroyInBucket for bucketId: {}{}{} for event: {} No reattempt is done, returning from here",
              getPRId(), BUCKET_ID_SEPARATOR, bucketId, event);
        }
        return;
      } catch (ForceReattemptException e) {
        e.checkKey(event.getKey());
        // We don't know if the destroy took place or not at this point.
        // Assume that if the next destroy throws EntryDestroyedException, the
        // previous destroy attempt was a success
        checkReadiness();
        InternalDistributedMember lastNode = currentTarget;
        if (retryTime == null) {
          retryTime = new RetryTimeKeeper(this.retryTimeout);
        }
        currentTarget = getOrCreateNodeForBucketWrite(bucketId, retryTime);
        event.setPossibleDuplicate(true);
        if (lastNode.equals(currentTarget)) {
          if (retryTime.overMaximum()) {
            PRHARedundancyProvider.timedOut(this, null, null, "destroy an entry",
                retryTime.getRetryTime());
          }
          retryTime.waitToRetryNode();
        }
      } catch (PrimaryBucketException notPrimary) {
        if (logger.isDebugEnabled()) {
          logger.debug("destroyInBucket: {} on Node {} not primary",
              notPrimary.getLocalizedMessage(), currentTarget);
        }
        getRegionAdvisor().notPrimary(bucketId, currentTarget);
        if (retryTime == null) {
          retryTime = new RetryTimeKeeper(this.retryTimeout);
        }
        currentTarget = getOrCreateNodeForBucketWrite(bucketId, retryTime);
      }

      // If we get here, the attempt failed.
      if (count == 1) {
        this.prStats.incDestroyOpsRetried();
      }
      this.prStats.incDestroyRetries();

      if (logger.isDebugEnabled()) {
        logger.debug(
            "destroyInBucket: Attempting to resend destroy to node {} after {} failed attempts",
            currentTarget, count);
      }
    } // for
  }

  private void setNetworkHopType(final Integer bucketId,
      final InternalDistributedMember targetNode) {

    if (this.isDataStore() && !getMyId().equals(targetNode)) {
      Set<ServerBucketProfile> profiles = this.getRegionAdvisor().getClientBucketProfiles(bucketId);

      if (profiles != null) {
        for (ServerBucketProfile profile : profiles) {
          if (profile.getDistributedMember().equals(targetNode)) {

            if (isProfileFromSameGroup(profile)) {
              if (this.getNetworkHopType() != NETWORK_HOP_TO_SAME_GROUP
                  && logger.isDebugEnabled()) {
                logger.debug(
                    "one-hop: cache op meta data staleness observed.  Message is in same server group (byte 1)");
              }
              this.setNetworkHopType((byte) NETWORK_HOP_TO_SAME_GROUP);
            } else {
              if (this.getNetworkHopType() != NETWORK_HOP_TO_DIFFERENT_GROUP
                  && logger.isDebugEnabled()) {
                logger.debug(
                    "one-hop: cache op meta data staleness observed.  Message is to different server group (byte 2)");
              }
              this.setNetworkHopType((byte) NETWORK_HOP_TO_DIFFERENT_GROUP);
            }
            this.setMetadataVersion((byte) profile.getVersion());
            break;
          }
        }
      }
    }
  }

  public boolean isProfileFromSameGroup(ServerBucketProfile profile) {
    Set<String> localServerGroups = getLocalServerGroups();
    if (localServerGroups.isEmpty()) {
      return true;
    }

    Set<BucketServerLocation66> locations = profile.getBucketServerLocations();

    for (BucketServerLocation66 sl : locations) {
      String[] groups = sl.getServerGroups();
      if (groups.length == 0) {
        return true;
      } else {
        for (String s : groups) {
          if (localServerGroups.contains(s))
            return true;
        }
      }
    }
    return false;
  }

  public Set<String> getLocalServerGroups() {
    Set<String> localServerGroups = new HashSet();
    InternalCache cache = getCache();
    List servers;

    servers = cache.getCacheServers();

    Collections.addAll(localServerGroups, MemberDataBuilder.parseGroups(null,
        cache.getInternalDistributedSystem().getConfig().getGroups()));

    for (Object object : servers) {
      CacheServerImpl server = (CacheServerImpl) object;
      if (server.isRunning() && (server.getExternalAddress() != null)) {
        Collections.addAll(localServerGroups, server.getGroups());
      }
    }
    return localServerGroups;
  }

  /**
   * Destroy the entry on the remote node.
   *
   * @param recipient the member id of the receiver of the message
   * @param bucketId the idenity of the bucket
   * @param event the event prompting this request
   * @param expectedOldValue if not null, then destroy only if entry exists and current value is
   *        equal to expectedOldValue
   * @throws EntryNotFoundException if entry not found OR if expectedOldValue is non-null and
   *         doesn't equal the current value
   * @throws PrimaryBucketException if the bucket on that node is not the primary copy
   * @throws ForceReattemptException if the peer is no longer available
   */
  public void destroyRemotely(DistributedMember recipient, Integer bucketId, EntryEventImpl event,
      Object expectedOldValue)
      throws EntryNotFoundException, PrimaryBucketException, ForceReattemptException {
    DestroyResponse response = DestroyMessage.send(recipient, this, event, expectedOldValue);
    if (response != null) {
      this.prStats.incPartitionMessagesSent();
      try {
        response.waitForCacheException();
        event.setVersionTag(response.getVersionTag());
      } catch (EntryNotFoundException enfe) {
        throw enfe;
      } catch (TransactionDataNotColocatedException enfe) {
        throw enfe;
      } catch (TransactionDataRebalancedException e) {
        throw e;
      } catch (CacheException ce) {
        throw new PartitionedRegionException(
            String.format("Destroy of entry on %s failed",
                recipient),
            ce);
      } catch (RegionDestroyedException ignore) {
        throw new RegionDestroyedException(toString(), getFullPath());
      }
    }
  }

  /**
   * This is getter method for local max memory.
   *
   * @return local max memory for this PartitionedRegion
   */
  public int getLocalMaxMemory() {
    return this.localMaxMemory;
  }

  /**
   * This is getter method for redundancy.
   *
   * @return redundancy for this PartitionedRegion
   */
  public int getRedundantCopies() {
    return this.redundantCopies;
  }

  @Override
  public VersionTag findVersionTagForEvent(EventID eventId) {
    if (this.dataStore != null) {
      Set<Map.Entry<Integer, BucketRegion>> bucketMap = this.dataStore.getAllLocalBuckets();
      for (Map.Entry<Integer, BucketRegion> entry : bucketMap) {
        VersionTag result = entry.getValue().findVersionTagForEvent(eventId);
        if (result != null) {
          return result;
        }
      }
    }
    return null;
  }

  @Override
  public VersionTag findVersionTagForClientBulkOp(EventID eventId) {
    Map<ThreadIdentifier, VersionTag> results = new HashMap<ThreadIdentifier, VersionTag>();
    if (this.dataStore != null) {
      Set<Map.Entry<Integer, BucketRegion>> bucketMap = this.dataStore.getAllLocalBuckets();
      for (Map.Entry<Integer, BucketRegion> entry : bucketMap) {
        VersionTag bucketResult = entry.getValue().findVersionTagForClientBulkOp(eventId);
        if (bucketResult != null) {
          return bucketResult;
        }
      }
    }
    return null;
  }

  /*
   * This method cleans the Partioned region structures if the the creation of Partition region
   * fails OVERRIDES
   */
  @Override
  public void cleanupFailedInitialization() {
    super.cleanupFailedInitialization();
    // Fix for 44551 - make sure persistent buckets
    // are done recoverying from disk before sending the
    // destroy region message.
    this.redundancyProvider.waitForPersistentBucketRecovery();
    this.cache.removePartitionedRegion(this);
    this.cache.getInternalResourceManager(false).removeResourceListener(this);
    this.redundancyProvider.shutdown(); // see bug 41094
    int serials[] = getRegionAdvisor().getBucketSerials();
    RegionEventImpl event = new RegionEventImpl(this, Operation.REGION_CLOSE, null, false,
        getMyId(), generateEventID()/* generate EventID */);
    try {
      sendDestroyRegionMessage(event, serials);
    } catch (Exception ex) {
      logger.warn(
          "PartitionedRegion#cleanupFailedInitialization(): Failed to clean the PartionRegion data store",
          ex);
    }
    if (null != this.dataStore) {
      try {
        this.dataStore.cleanUp(true, false);
      } catch (Exception ex) {
        logger.warn(
            "PartitionedRegion#cleanupFailedInitialization(): Failed to clean the PartionRegion data store",
            ex);
      }
    }

    if (this.cleanPRRegistration) {
      try {
        synchronized (prIdToPR) {
          if (prIdToPR.containsKey(this.partitionedRegionId)) {
            prIdToPR.put(this.partitionedRegionId, PRIdMap.FAILED_REGISTRATION, false);
            if (logger.isDebugEnabled()) {
              logger.debug("cleanupFailedInitialization: set failed for prId={} named {}",
                  this.partitionedRegionId, this.getName());
            }
          }
        }

        PartitionedRegionHelper.removeGlobalMetadataForFailedNode(this.node,
            this.getRegionIdentifier(), getGemFireCache(), true);
      } catch (Exception ex) {
        logger.warn(
            "PartitionedRegion#cleanupFailedInitialization: Failed to clean the PartionRegion allPartitionedRegions",
            ex);
      }
    }
    this.distAdvisor.close();
    getPrStats().close();
    if (getDiskStore() != null && getDiskStore().getOwnedByRegion()) {
      getDiskStore().close();
    }
    if (logger.isDebugEnabled()) {
      logger.debug("cleanupFailedInitialization: end of {}", getName());
    }
  }

  /**
   * Called after the cache close has closed all regions. This clean up static pr resources.
   *
   * @since GemFire 5.0
   */
  static void afterRegionsClosedByCacheClose(InternalCache cache) {
    PRQueryProcessor.shutdown();
    clearPRIdMap();
  }

  @Override
  void basicInvalidateRegion(RegionEventImpl event) {
    if (!event.isOriginRemote()) {
      sendInvalidateRegionMessage(event);
    }
    for (BucketRegion br : getDataStore().getAllLocalPrimaryBucketRegions()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Invalidating bucket {}", br);
      }
      br.basicInvalidateRegion(event);
    }
    super.basicInvalidateRegion(event);
  }

  @Override
  void invalidateAllEntries(RegionEvent rgnEvent) {
    // do nothing
  }

  private void sendInvalidateRegionMessage(RegionEventImpl event) {
    final int retryAttempts = calcRetry();
    Throwable thr = null;
    int count = 0;
    while (count <= retryAttempts) {
      try {
        count++;
        Set recipients = getRegionAdvisor().adviseDataStore();
        ReplyProcessor21 response =
            InvalidatePartitionedRegionMessage.send(recipients, this, event);
        response.waitForReplies();
        thr = null;
        break;
      } catch (ReplyException e) {
        thr = e;
        if (!this.isClosed && !this.isDestroyed) {
          if (logger.isDebugEnabled()) {
            logger.debug("Invalidating partitioned region caught exception", e);
          }
        }
      } catch (InterruptedException e) {
        thr = e;
        if (!cache.getCancelCriterion().isCancelInProgress()) {
          if (logger.isDebugEnabled()) {
            logger.debug("Invalidating partitioned region caught exception", e);
          }
        }
      }
    }
    if (thr != null) {
      PartitionedRegionDistributionException e = new PartitionedRegionDistributionException(
          String.format("Invalidating partitioned region caught exception %s",
              count));
      if (logger.isDebugEnabled()) {
        logger.debug(e.getMessage(), e);
      }
      throw e;
    }
  }

  @Override
  public void basicInvalidate(EntryEventImpl event) throws EntryNotFoundException {
    final long startTime = prStats.getTime();
    try {
      if (event.getEventId() == null) {
        event.setNewEventId(this.cache.getDistributedSystem());
      }
      discoverJTA();
      getDataView().invalidateExistingEntry(event, isInitialized(), false);
    } catch (RegionDestroyedException rde) {
      if (!rde.getRegionFullPath().equals(getFullPath())) {
        // Handle when a bucket is destroyed
        throw new RegionDestroyedException(toString(), getFullPath(), rde);
      }
    } finally {
      this.prStats.endInvalidate(startTime);
    }
    return;
  }

  @Override
  void basicUpdateEntryVersion(EntryEventImpl event) throws EntryNotFoundException {

    try {
      if (event.getEventId() == null) {
        event.setNewEventId(this.cache.getDistributedSystem());
      }
      getDataView().updateEntryVersion(event);
    } catch (RegionDestroyedException rde) {
      if (!rde.getRegionFullPath().equals(getFullPath())) {
        // Handle when a bucket is destroyed
        throw new RegionDestroyedException(toString(), getFullPath(), rde);
      }
    }
    return;
  }

  /**
   * Invalidate the entry in the bucket identified by the key
   */
  void invalidateInBucket(final EntryEventImpl event) throws EntryNotFoundException {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    final int bucketId = event.getKeyInfo().getBucketId();
    assert bucketId != KeyInfo.UNKNOWN_BUCKET;
    final InternalDistributedMember targetNode = getOrCreateNodeForBucketWrite(bucketId, null);

    final int retryAttempts = calcRetry();
    int count = 0;
    RetryTimeKeeper retryTime = null;
    InternalDistributedMember retryNode = targetNode;
    while (count <= retryAttempts) {
      // It's possible this is a GemFire thread e.g. ServerConnection
      // which got to this point because of a distributed system shutdown or
      // region closure which uses interrupt to break any sleep() or wait()
      // calls
      // e.g. waitForPrimary or waitForBucketRecovery
      checkShutdown();

      if (retryNode == null) {
        checkReadiness();
        if (retryTime == null) {
          retryTime = new RetryTimeKeeper(this.retryTimeout);
        }
        try {
          retryNode = getOrCreateNodeForBucketWrite(bucketId, retryTime);
        } catch (TimeoutException ignore) {
          if (getRegionAdvisor().isStorageAssignedForBucket(bucketId)) {
            // bucket no longer exists
            throw new EntryNotFoundException(
                String.format("Entry not found for key %s",
                    event.getKey()));
          }
          break; // fall out to failed exception
        }

        if (retryNode == null) {
          checkEntryNotFound(event.getKey());
        }
        continue;
      }
      final boolean isLocal = (this.localMaxMemory > 0) && retryNode.equals(getMyId());
      try {
        if (isLocal) {
          event.setInvokePRCallbacks(true);
          this.dataStore.invalidateLocally(bucketId, event);
        } else {
          invalidateRemotely(retryNode, bucketId, event);
        }
        return;
      } catch (ConcurrentCacheModificationException e) {
        if (isDebugEnabled) {
          logger.debug("invalidateInBucket: caught concurrent cache modification exception", e);
        }
        event.isConcurrencyConflict(true);

        if (isDebugEnabled) {
          logger.debug(
              "ConcurrentCacheModificationException received for invalidateInBucket for bucketId: {}{}{} for event: {}  No reattampt is done, returning from here",
              getPRId(), BUCKET_ID_SEPARATOR, bucketId, event);
        }
        return;
      } catch (ForceReattemptException prce) {
        prce.checkKey(event.getKey());
        if (isDebugEnabled) {
          logger.debug("invalidateInBucket: retry attempt:{} of {}", count, retryAttempts, prce);
        }
        checkReadiness();

        InternalDistributedMember lastNode = retryNode;
        retryNode = getOrCreateNodeForBucketWrite(bucketId, retryTime);
        if (lastNode.equals(retryNode)) {
          if (retryTime == null) {
            retryTime = new RetryTimeKeeper(this.retryTimeout);
          }
          if (retryTime.overMaximum()) {
            break;
          }
          retryTime.waitToRetryNode();
        }
        event.setPossibleDuplicate(true);
      } catch (PrimaryBucketException notPrimary) {
        if (isDebugEnabled) {
          logger.debug("invalidateInBucket {} on Node {} not primary",
              notPrimary.getLocalizedMessage(), retryNode);
        }
        getRegionAdvisor().notPrimary(bucketId, retryNode);
        retryNode = getOrCreateNodeForBucketWrite(bucketId, retryTime);
      }

      count++;
      if (count == 1) {
        this.prStats.incInvalidateOpsRetried();
      }
      this.prStats.incInvalidateRetries();
      if (isDebugEnabled) {
        logger.debug(
            "invalidateInBucket: Attempting to resend invalidate to node {} after {} failed attempts",
            retryNode, count);
      }
    } // while

    // No target was found
    PartitionedRegionDistributionException e = new PartitionedRegionDistributionException(
        String.format("No VM available for invalidate in %s attempts.",
            count)); // Fix for bug 36014
    if (!isDebugEnabled) {
      logger.warn("No VM available for invalidate in {} attempts.", count);
    } else {
      logger.warn(e.getMessage(), e);
    }
    throw e;
  }

  /**
   * invalidates the remote object with the given key.
   *
   * @param recipient the member id of the recipient of the operation
   * @param bucketId the id of the bucket the key hashed into
   * @throws EntryNotFoundException if the entry does not exist in this region
   * @throws PrimaryBucketException if the bucket on that node is not the primary copy
   * @throws ForceReattemptException if the peer is no longer available
   */
  public void invalidateRemotely(DistributedMember recipient, Integer bucketId,
      EntryEventImpl event)
      throws EntryNotFoundException, PrimaryBucketException, ForceReattemptException {
    InvalidateResponse response = InvalidateMessage.send(recipient, this, event);
    if (response != null) {
      this.prStats.incPartitionMessagesSent();
      try {
        response.waitForResult();
        event.setVersionTag(response.versionTag);
        return;
      } catch (EntryNotFoundException ex) {
        throw ex;
      } catch (TransactionDataNotColocatedException ex) {
        throw ex;
      } catch (TransactionDataRebalancedException e) {
        throw e;
      } catch (CacheException ce) {
        throw new PartitionedRegionException(
            String.format("Invalidation of entry on %s failed",
                recipient),
            ce);
      }
    }
  }

  /**
   * Calculate the number of times we attempt to commumnicate with a data store. Beware that this
   * method is called very frequently so it absolutely must perform well.
   *
   * @return the number of times to attempt to communicate with a data store
   */
  private int calcRetry() {
    return (this.retryTimeout / PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION) + 1;
  }

  /**
   * Creates the key/value pair into the remote target that is managing the key's bucket.
   *
   * @param recipient member id of the recipient of the operation
   * @param bucketId the id of the bucket that the key hashed to
   * @param event the event prompting this request
   * @throws PrimaryBucketException if the bucket on that node is not the primary copy
   * @throws ForceReattemptException if the peer is no longer available
   */
  private boolean createRemotely(DistributedMember recipient, Integer bucketId,
      EntryEventImpl event, boolean requireOldValue)
      throws PrimaryBucketException, ForceReattemptException {
    boolean ret = false;
    long eventTime = event.getEventTime(0L);
    PutMessage.PutResponse reply = (PutMessage.PutResponse) PutMessage.send(recipient, this, event,
        eventTime, true, false, null, // expectedOldValue
        requireOldValue);
    PutResult pr = null;
    if (reply != null) {
      this.prStats.incPartitionMessagesSent();
      try {
        pr = reply.waitForResult();
        event.setOperation(pr.op);
        event.setVersionTag(pr.versionTag);
        if (requireOldValue) {
          event.setOldValue(pr.oldValue, true);
        }
        ret = pr.returnValue;
      } catch (EntryExistsException ignore) {
        // This might not be necessary and is here for safety sake
        ret = false;
      } catch (TransactionDataNotColocatedException tdnce) {
        throw tdnce;
      } catch (TransactionDataRebalancedException e) {
        throw e;
      } catch (CacheException ce) {
        throw new PartitionedRegionException(
            String.format("Create of entry on %s failed",
                recipient),
            ce);
      } catch (RegionDestroyedException rde) {
        if (logger.isDebugEnabled()) {
          logger.debug("createRemotely: caught exception", rde);
        }
        throw new RegionDestroyedException(toString(), getFullPath());
      }
    }
    return ret;
  }

  // ////////////////////////////////
  // ////// Set Operations /////////
  // ///////////////////////////////

  /**
   * This method returns set of all the entries of this PartitionedRegion(locally or remotely).
   * Currently, it throws UnsupportedOperationException
   *
   * @param recursive boolean flag to indicate whether subregions should be considered or not.
   * @return set of all the entries of this PartitionedRegion
   *
   *         OVERRIDES
   */
  @Override
  public Set entrySet(boolean recursive) {
    checkReadiness();
    if (!restoreSetOperationTransactionBehavior) {
      discoverJTA();
    }
    return Collections.unmodifiableSet(new PREntriesSet());
  }

  public Set<Region.Entry> entrySet(Set<Integer> bucketIds) {
    return new PREntriesSet(bucketIds);
  }

  /**
   * Set view of entries. This currently extends the keySet iterator and performs individual
   * getEntry() operations using the keys
   *
   * @since GemFire 5.1
   */
  protected class PREntriesSet extends KeysSet {

    boolean allowTombstones;

    private class EntriesSetIterator extends KeysSetIterator {

      /** reusable KeyInfo */
      private final KeyInfo key = new KeyInfo(null, null, null);

      public EntriesSetIterator(Set bucketSet, boolean allowTombstones) {
        super(bucketSet, allowTombstones);
        PREntriesSet.this.allowTombstones = allowTombstones;
      }

      @Override
      public Object next() {
        this.key.setKey(super.next());
        this.key.setBucketId(this.currentBucketId);

        Object entry =
            view.getEntryForIterator(this.key, PartitionedRegion.this, true, allowTombstones);
        return entry != null ? entry : new DestroyedEntry(key.getKey().toString());
      }
    }

    public PREntriesSet() {
      super();
    }

    public PREntriesSet(Set<Integer> bucketSet) {
      super(bucketSet);
    }

    @Override
    public Iterator iterator() {
      checkTX();
      return new EntriesSetIterator(this.bucketSet, allowTombstones);
    }
  }

  /**
   * This method returns set of all the keys of this PartitionedRegion(locally or remotely).
   *
   * @return set of all the keys of this PartitionedRegion
   *
   *         OVERRIDES
   */
  @Override
  public Set keys() {
    checkReadiness();
    if (!restoreSetOperationTransactionBehavior) {
      discoverJTA();
    }
    return Collections.unmodifiableSet(new KeysSet());
  }

  @Override
  public Set keySet(boolean allowTombstones) {
    checkReadiness();
    return Collections.unmodifiableSet(new KeysSet(allowTombstones));
  }

  /**
   * Get a keyset of the given buckets
   */
  public Set keySet(Set<Integer> bucketSet) {
    return new KeysSet(bucketSet);
  }

  public Set keysWithoutCreatesForTests() {
    checkReadiness();
    Set<Integer> availableBuckets = new HashSet<Integer>();
    for (int i = 0; i < getTotalNumberOfBuckets(); i++) {
      if (distAdvisor.isStorageAssignedForBucket(i)) {
        availableBuckets.add(i);
      }
    }
    return Collections.unmodifiableSet(new KeysSet(availableBuckets));
  }

  /** Set view of entries */
  protected class KeysSet extends EntriesSet {
    class KeysSetIterator implements PREntriesIterator<Object> {
      final Iterator<Integer> bucketSetI;
      volatile Iterator currentBucketI = null;
      int currentBucketId = -1;
      volatile Object currentKey = null;
      protected final Set<Integer> bucketSet;
      boolean allowTombstones;

      public KeysSetIterator(Set<Integer> bucketSet, boolean allowTombstones) {
        PartitionedRegion.this.checkReadiness();
        this.bucketSet = bucketSet;
        this.allowTombstones = allowTombstones;
        this.bucketSetI = createBucketSetI();
        this.currentBucketI = getNextBucketIter(false /* no throw */);
      }

      protected Iterator<Integer> createBucketSetI() {
        if (this.bucketSet != null) {
          return this.bucketSet.iterator();
        }
        return getRegionAdvisor().getBucketSet().iterator();
      }

      @Override
      public boolean hasNext() {
        PartitionedRegion.this.checkReadiness();
        if (this.currentBucketI.hasNext()) {
          return true;
        } else {
          while (!this.currentBucketI.hasNext() && this.bucketSetI.hasNext()) {
            PartitionedRegion.this.checkReadiness();
            this.currentBucketI = getNextBucketIter(false);
          }
          return this.currentBucketI.hasNext();
        }
      }

      @Override
      public Object next() {
        if (myTX != null) {
          checkTX();
        }
        PartitionedRegion.this.checkReadiness();
        if (this.currentBucketI.hasNext()) {
          this.currentKey = this.currentBucketI.next();
        } else {
          this.currentKey = null;
          while (!this.currentBucketI.hasNext() && this.bucketSetI.hasNext()) {
            PartitionedRegion.this.checkReadiness();
            this.currentBucketI = getNextBucketIter(true);
          }
          // Next line may throw NoSuchElementException... this is expected.
          this.currentKey = this.currentBucketI.next();
        }
        return this.currentKey;
      }

      protected Iterator getNextBucketIter(boolean canThrow) {
        try {
          this.currentBucketId = this.bucketSetI.next();
          // TODO: optimize this code by implementing getBucketKeysIterator.
          // Instead of creating a Set to return it can just create an ArrayList
          // and return an iterator on it. This would cut down on garbage and
          // cpu usage.
          return view
              .getBucketKeys(PartitionedRegion.this, this.currentBucketId, this.allowTombstones)
              .iterator();
        } catch (NoSuchElementException endOfTheLine) {
          if (canThrow) {
            throw endOfTheLine;
          } else {
            // Logically pass the NoSuchElementException to the caller
            // Can't throw here because it is called in the contructor context
            return Collections.emptySet().iterator();
          }
        }
      }

      @Override
      public void remove() {
        if (this.currentKey == null) {
          throw new IllegalStateException();
        }
        try {
          PartitionedRegion.this.destroy(this.currentKey);
        } catch (EntryNotFoundException e) {
          if (logger.isDebugEnabled()) {
            logger.debug("Caught exception during KeySetIterator remove", e);
          }
        } finally {
          this.currentKey = null;
        }
      }

      @Override
      public PartitionedRegion getPartitionedRegion() {
        return PartitionedRegion.this;
      }

      @Override
      public int getBucketId() {
        return this.currentBucketId;
      }
    }

    protected final Set<Integer> bucketSet;

    public KeysSet() {
      super(PartitionedRegion.this, false, IteratorType.KEYS, false);
      this.bucketSet = null;
    }

    public KeysSet(boolean allowTombstones) {
      super(PartitionedRegion.this, false, IteratorType.KEYS, allowTombstones);
      this.bucketSet = null;
    }

    public KeysSet(Set<Integer> bucketSet) {
      super(PartitionedRegion.this, false, IteratorType.KEYS, false);
      this.bucketSet = bucketSet;
    }

    @Override
    public int size() {
      checkTX();
      return PartitionedRegion.this.entryCount(this.bucketSet);
    }

    @Override
    public Object[] toArray() {
      return toArray((Object[]) null);
    }

    @Override
    public Object[] toArray(Object[] array) {
      List temp = new ArrayList(this.size());
      for (Iterator iter = this.iterator(); iter.hasNext();) {
        temp.add(iter.next());
      }
      if (array == null) {
        return temp.toArray();
      } else {
        return temp.toArray(array);
      }
    }

    @Override
    public Iterator iterator() {
      checkTX();
      return new KeysSetIterator(this.bucketSet, this.allowTombstones);
    }
  }

  /**
   * This method returns collection of all the values of this PartitionedRegion(locally or
   * remotely).
   *
   * @return collection of all the values of this PartitionedRegion
   */
  @Override
  public Collection values() {
    checkReadiness();
    if (!restoreSetOperationTransactionBehavior) {
      discoverJTA();
    }
    return Collections.unmodifiableSet(new ValuesSet());
  }

  /**
   * Set view of values. This currently extends the keySet iterator and performs individual get()
   * operations using the keys
   *
   * @since GemFire 5.1
   */
  protected class ValuesSet extends KeysSet {

    private class ValuesSetIterator extends KeysSetIterator {

      Object nextValue = null;

      /** reusable KeyInfo */
      private final KeyInfo key = new KeyInfo(null, null, null);

      public ValuesSetIterator(Set bucketSet) {
        super(bucketSet, false);
      }

      @Override
      public boolean hasNext() {
        if (nextValue != null) {
          return true;
        }
        while (nextValue == null) {
          if (!super.hasNext()) {
            return false;
          }
          this.key.setKey(super.next());
          this.key.setBucketId(this.currentBucketId);
          Region.Entry re = (Region.Entry) view.getEntryForIterator(key, PartitionedRegion.this,
              rememberReads, allowTombstones);
          if (re != null) {
            nextValue = re.getValue();
          }
        }
        return true;
      }

      @Override
      public Object next() {
        if (!this.hasNext()) {
          throw new NoSuchElementException();
        }
        Assert.assertTrue(nextValue != null, "nextValue found to be null");
        Object result = nextValue;
        nextValue = null;
        return result;
      }

      @Override
      public void remove() {
        super.remove();
        nextValue = null;
      }
    }

    public ValuesSet() {
      super();
    }

    public ValuesSet(Set<Integer> bucketSet) {
      super(bucketSet);
    }

    @Override
    public Iterator iterator() {
      checkTX();
      return new ValuesSetIterator(this.bucketSet);
    }
  }

  /**
   * @since GemFire 6.6
   */
  @Override
  public boolean containsValue(final Object value) {
    if (value == null) {
      throw new NullPointerException(
          "Value for containsValue(value) cannot be null");
    }
    checkReadiness();
    // First check the values is present locally.
    if (this.getDataStore() != null) {
      ValuesSet vSet = new ValuesSet(this.getDataStore().getAllLocalPrimaryBucketIds());
      Iterator itr = vSet.iterator();
      while (itr.hasNext()) {
        Object v = itr.next();
        if (v.equals(value)) {
          return true;
        }
      }
    }

    ResultCollector rc = null;
    try {
      rc = FunctionService.onRegion(this).setArguments((Serializable) value)
          .execute(PRContainsValueFunction.class.getName());
      List<Boolean> results = ((List<Boolean>) rc.getResult());
      for (Boolean r : results) {
        if (r) {
          return true;
        }
      }
    } catch (FunctionException fe) {
      checkShutdown();
      logger.warn("PR containsValue warning. Got an exception while executing function",
          fe.getCause());
    }
    return false;
  }

  @Override
  public boolean containsKey(Object key) {
    checkReadiness();
    validateKey(key);
    return getDataView().containsKey(getKeyInfo(key), this);
  }

  @Override
  boolean nonTXContainsKey(KeyInfo keyInfo) {
    final long startTime = prStats.getTime();
    boolean contains = false;
    try {
      int bucketId = keyInfo.getBucketId();
      if (bucketId == KeyInfo.UNKNOWN_BUCKET) {
        bucketId = PartitionedRegionHelper.getHashKey(this, Operation.CONTAINS_KEY,
            keyInfo.getKey(), keyInfo.getValue(), keyInfo.getCallbackArg());
        keyInfo.setBucketId(bucketId);
      }
      Integer bucketIdInt = bucketId;
      InternalDistributedMember targetNode = getOrCreateNodeForBucketRead(bucketId);
      // targetNode null means that this key is not in the system.
      if (targetNode != null) {
        contains = containsKeyInBucket(targetNode, bucketIdInt, keyInfo.getKey(), false);
      }
    } finally {
      this.prStats.endContainsKey(startTime);
    }
    return contains;
  }

  boolean containsKeyInBucket(final InternalDistributedMember targetNode, final Integer bucketIdInt,
      final Object key, boolean valueCheck) {
    final int retryAttempts = calcRetry();
    if (logger.isDebugEnabled()) {
      logger.debug("containsKeyInBucket: {}{} ({}) from: {} bucketId={}",
          (valueCheck ? "ValueForKey key=" : "Key key="), key, key.hashCode(), targetNode,
          bucketStringForLogs(bucketIdInt));
    }
    boolean ret;
    int count = 0;
    RetryTimeKeeper retryTime = null;
    InternalDistributedMember retryNode = targetNode;
    while (count <= retryAttempts) {
      // Every continuation should check for DM cancellation
      if (retryNode == null) {
        checkReadiness();
        if (retryTime == null) {
          retryTime = new RetryTimeKeeper(this.retryTimeout);
        }
        if (retryTime.overMaximum()) {
          break;
        }
        retryNode = getOrCreateNodeForBucketRead(bucketIdInt);
        // No storage found for bucket, early out preventing hot loop, bug 36819
        if (retryNode == null) {
          checkShutdown(); // Prefer closed style exceptions over empty result
          return false;
        }

        continue;
      } // retryNode != null
      try {
        final boolean loc = retryNode.equals(getMyId());
        if (loc) {
          if (valueCheck) {
            ret = this.dataStore.containsValueForKeyLocally(bucketIdInt, key);
          } else {
            ret = this.dataStore.containsKeyLocally(bucketIdInt, key);
          }
        } else {
          if (valueCheck) {
            ret = containsValueForKeyRemotely(retryNode, bucketIdInt, key);
          } else {
            ret = containsKeyRemotely(retryNode, bucketIdInt, key);
          }
        }
        return ret;
      } catch (PRLocallyDestroyedException pde) {
        if (logger.isDebugEnabled()) {
          logger.debug("containsKeyInBucket: Encountered PRLocallyDestroyedException", pde);
        }
        checkReadiness();
      } catch (ForceReattemptException prce) {
        prce.checkKey(key);
        if (logger.isDebugEnabled()) {
          logger.debug("containsKeyInBucket: retry attempt:{} of {}", count, retryAttempts, prce);
        }
        checkReadiness();

        InternalDistributedMember lastNode = retryNode;
        retryNode = getOrCreateNodeForBucketRead(bucketIdInt);
        if (lastNode.equals(retryNode)) {
          if (retryTime == null) {
            retryTime = new RetryTimeKeeper(this.retryTimeout);
          }
          if (retryTime.overMaximum()) {
            break;
          }
          retryTime.waitToRetryNode();
        }
      } catch (PrimaryBucketException notPrimary) {
        if (logger.isDebugEnabled()) {
          logger.debug("containsKeyInBucket {} on Node {} not primary",
              notPrimary.getLocalizedMessage(), retryNode);
        }
        getRegionAdvisor().notPrimary(bucketIdInt, retryNode);
        retryNode = getOrCreateNodeForBucketRead(bucketIdInt);
      } catch (RegionDestroyedException rde) {
        if (!rde.getRegionFullPath().equals(getFullPath())) {
          throw new RegionDestroyedException(toString(), getFullPath(), rde);
        }
      }

      // It's possible this is a GemFire thread e.g. ServerConnection
      // which got to this point because of a distributed system shutdown or
      // region closure which uses interrupt to break any sleep() or wait()
      // calls
      // e.g. waitForPrimary
      checkShutdown();

      count++;
      if (count == 1) {
        this.prStats.incContainsKeyValueOpsRetried();
      }
      this.prStats.incContainsKeyValueRetries();

    }

    String msg;
    if (valueCheck) {
      msg =
          "No VM available for contains value for key in %s attempts";
    } else {
      msg = "No VM available for contains key in %s attempts";
    }

    Integer countInteger = count;
    PartitionedRegionDistributionException e = null; // Fix for bug 36014
    if (logger.isDebugEnabled()) {
      e = new PartitionedRegionDistributionException(String.format(msg, countInteger));
    }
    logger.warn(String.format(msg, countInteger), e);
    return false;
  }

  /**
   * Checks if a key is contained remotely.
   *
   * @param targetNode the node where bucket region for the key exists.
   * @param bucketId the bucket id for the key.
   * @param key the key, whose value needs to be checks
   * @return true if the passed key is contained remotely.
   * @throws PrimaryBucketException if the remote bucket was not the primary
   * @throws ForceReattemptException if the peer is no longer available
   */
  public boolean containsKeyRemotely(InternalDistributedMember targetNode, Integer bucketId,
      Object key) throws PrimaryBucketException, ForceReattemptException {
    ContainsKeyValueResponse r =
        ContainsKeyValueMessage.send(targetNode, this, key, bucketId, false);
    this.prStats.incPartitionMessagesSent();
    return r.waitForContainsResult();
  }

  /**
   * Returns whether there is a valid (non-null) value present for the specified key, locally or
   * remotely. This method is equivalent to:
   *
   * <pre>
   * Entry e = getEntry(key);
   * return e != null &amp;&amp; e.getValue() != null;
   * </pre>
   *
   * This method does not consult localCache, even if enabled.
   *
   * @param key the key to check for a valid value
   * @return true if there is an entry in this region for the specified key and it has a valid value
   *         OVERRIDES
   */
  @Override
  public boolean containsValueForKey(Object key) {
    // checkClosed();
    checkReadiness();
    validateKey(key);
    final long startTime = prStats.getTime();
    boolean containsValueForKey = false;
    try {
      containsValueForKey = getDataView().containsValueForKey(getKeyInfo(key), this);
    } finally {
      this.prStats.endContainsValueForKey(startTime);
    }
    return containsValueForKey;
  }

  @Override
  boolean nonTXContainsValueForKey(KeyInfo keyInfo) {
    boolean containsValueForKey = false;
    int bucketId = keyInfo.getBucketId();
    if (bucketId == KeyInfo.UNKNOWN_BUCKET) {
      bucketId = PartitionedRegionHelper.getHashKey(this, Operation.CONTAINS_VALUE_FOR_KEY,
          keyInfo.getKey(), keyInfo.getValue(), keyInfo.getCallbackArg());
      keyInfo.setBucketId(bucketId);
    }
    InternalDistributedMember targetNode = getOrCreateNodeForBucketRead(bucketId);
    // targetNode null means that this key is not in the system.
    if (targetNode != null) {
      containsValueForKey = containsKeyInBucket(targetNode, bucketId, keyInfo.getKey(), true);
    }
    return containsValueForKey;
  }

  /**
   * Checks if this instance contains a value for the key remotely.
   *
   * @param targetNode the node where bucket region for the key exists.
   * @param bucketId the bucket id for the key.
   * @param key the key, whose value needs to be checks
   * @return true if there is non-null value for the given key
   * @throws PrimaryBucketException if the remote bucket was not the primary
   * @throws ForceReattemptException if the peer is no longer available
   */
  public boolean containsValueForKeyRemotely(InternalDistributedMember targetNode, Integer bucketId,
      Object key) throws PrimaryBucketException, ForceReattemptException {
    if (logger.isDebugEnabled()) {
      logger.debug("containsValueForKeyRemotely: key={}", key);
    }
    ContainsKeyValueResponse r =
        ContainsKeyValueMessage.send(targetNode, this, key, bucketId, true);
    this.prStats.incPartitionMessagesSent();
    return r.waitForContainsResult();
  }

  /**
   * Get the VSD statistics type
   *
   * @return the statistics instance specific to this Partitioned Region
   */
  public PartitionedRegionStats getPrStats() {
    return this.prStats;
  }

  /* non-transactional size calculation */
  @Override
  public int getRegionSize() {
    return entryCount(null);
  }

  @Override
  public int getLocalSize() {
    if (dataStore == null) {
      return 0;
    }

    return dataStore.getLocalBucket2RegionMap().values().stream()
        .mapToInt(BucketRegion::getLocalSize)
        .sum();
  }

  public int entryCount(boolean localOnly) {
    if (localOnly) {
      if (this.isDataStore()) {
        return entryCount(this.dataStore.getAllLocalBucketIds());
      } else {
        return 0;
      }
    } else {
      return entryCount(null);
    }
  }

  @Override
  int entryCount(Set<Integer> buckets) {
    return entryCount(buckets, false);
  }

  @Override
  public int entryCount(Set<Integer> buckets, boolean estimate) {
    Map<Integer, SizeEntry> bucketSizes = null;
    if (buckets != null) {
      if (this.dataStore != null) {
        List<Integer> list = new ArrayList<Integer>(buckets);
        bucketSizes = this.dataStore.getSizeLocallyForBuckets(list);
      }
    } else {
      if (this.dataStore != null) {
        bucketSizes = this.dataStore.getSizeForLocalBuckets();
      }
      HashSet recips = (HashSet) getRegionAdvisor().adviseDataStore(true);
      recips.remove(getMyId());
      if (!recips.isEmpty()) {
        Map<Integer, SizeEntry> remoteSizes = getSizeRemotely(recips, false);
        if (logger.isDebugEnabled()) {
          logger.debug("entryCount: {} remoteSizes={}", this, remoteSizes);
        }
        if (bucketSizes != null && !bucketSizes.isEmpty()) {
          for (Map.Entry<Integer, SizeEntry> me : remoteSizes.entrySet()) {
            Integer k = me.getKey();
            if (!bucketSizes.containsKey(k) || !bucketSizes.get(k).isPrimary()) {
              bucketSizes.put(k, me.getValue());
            }
          }
        } else {
          bucketSizes = remoteSizes;
        }
      }
    }

    int size = 0;
    if (bucketSizes != null) {
      for (SizeEntry entry : bucketSizes.values()) {
        size += entry.getSize();
      }
    }
    return size;
  }

  @Override
  long getEstimatedLocalSize() {
    final PartitionedRegionDataStore ds = this.dataStore;
    if (ds != null) {
      return ds.getEstimatedLocalBucketSize(false);
    } else {
      return 0;
    }
  }

  /**
   * This method gets a PartitionServerSocketConnection to targetNode and sends size request to the
   * node. It returns size of all the buckets "primarily" hosted on that node. Here "primarily"
   * means that for a given bucketID that node comes first in the node list. This selective counting
   * ensures that redundant bucket size is added only once.
   *
   * @return the size of all the buckets hosted on the target node.
   */
  private Map<Integer, SizeEntry> getSizeRemotely(Set targetNodes, boolean estimate) {
    SizeResponse r = SizeMessage.send(targetNodes, this, null, estimate);
    this.prStats.incPartitionMessagesSent();
    Map retVal = null;
    try {
      retVal = r.waitBucketSizes();
    } catch (CacheException e) {
      checkReadiness();
      throw e;
    }
    return retVal;
  }

  /**
   * Returns the lockname used by Distributed Lock service to clean the
   * {@code allPartitionedRegions}.
   *
   */
  private String getLockNameForBucket2NodeModification(int bucketID) {
    return (getRegionIdentifier() + ":" + bucketID);
  }

  /**
   * A simple container class that holds the lock name and the service that created the lock,
   * typically used for locking buckets, but not restricted to that usage.
   *
   * @since GemFire 5.0
   */
  static class BucketLock {

    protected final DLockService lockService;

    protected final String lockName;

    private boolean lockOwned = false;

    private final InternalCache cache;

    private final boolean enableAlerts;

    protected BucketLock(String lockName, InternalCache cache, boolean enableAlerts) {
      this.lockService = (DLockService) cache.getPartitionedRegionLockService();
      this.cache = cache;
      this.lockName = lockName;
      this.enableAlerts = enableAlerts;
    }

    /**
     * Locks the given name (provided during construction) uninterruptibly or throws an exception.
     */
    public void lock() {
      try {
        basicLock();
      } catch (LockServiceDestroyedException e) {
        cache.getCancelCriterion().checkCancelInProgress(null);
        throw e;
      }
    }

    /**
     * Attempts to lock the given name (provided during construction) uninterruptibly
     *
     * @return true if the lock was acquired, otherwise false.
     */
    public boolean tryLock() {
      try {
        cache.getCancelCriterion().checkCancelInProgress(null);
        basicTryLock(PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION);
      } catch (LockServiceDestroyedException e) {
        cache.getCancelCriterion().checkCancelInProgress(null);
        throw e;
      }
      return this.lockOwned;
    }

    private void basicLock() {
      if (enableAlerts) {
        ReplyProcessor21.forceSevereAlertProcessing();
      }
      try {
        DistributionManager dm = cache.getInternalDistributedSystem().getDistributionManager();

        long ackWaitThreshold = 0;
        long ackSAThreshold = dm.getConfig().getAckSevereAlertThreshold() * 1000L;
        boolean suspected = false;
        boolean severeAlertIssued = false;
        DistributedMember lockHolder = null;

        long waitInterval;
        long startTime;

        if (!enableAlerts) {
          // Make sure we only attempt the lock long enough not to
          // get a 15 second warning from the reply processor.
          ackWaitThreshold = dm.getConfig().getAckWaitThreshold() * 1000L;
          waitInterval = ackWaitThreshold - 1;
          startTime = System.currentTimeMillis();
        } else if (ackSAThreshold > 0) {
          ackWaitThreshold = dm.getConfig().getAckWaitThreshold() * 1000L;
          waitInterval = ackWaitThreshold;
          startTime = System.currentTimeMillis();
        } else {
          waitInterval = PartitionedRegion.VM_OWNERSHIP_WAIT_TIME;
          startTime = 0;
        }

        while (!this.lockOwned) {
          cache.getCancelCriterion().checkCancelInProgress(null);
          this.lockOwned =
              this.lockService.lock(this.lockName, waitInterval, -1, false, false, !enableAlerts);
          if (!this.lockOwned && ackSAThreshold > 0 && enableAlerts) {
            long elapsed = System.currentTimeMillis() - startTime;
            if (elapsed > ackWaitThreshold && enableAlerts) {
              if (!suspected) {
                suspected = true;
                severeAlertIssued = false;
                waitInterval = ackSAThreshold;
                DLockRemoteToken remoteToken = this.lockService.queryLock(this.lockName);
                lockHolder = remoteToken.getLessee();
                if (lockHolder != null) {
                  dm.getDistribution().suspectMember((InternalDistributedMember) lockHolder,
                      "Has not released a partitioned region lock in over "
                          + ackWaitThreshold / 1000 + " sec");
                }
              } else if (elapsed > ackSAThreshold && enableAlerts) {
                DLockRemoteToken remoteToken = this.lockService.queryLock(this.lockName);
                if (lockHolder != null && remoteToken.getLessee() != null
                    && lockHolder.equals(remoteToken.getLessee())) {
                  if (!severeAlertIssued) {
                    severeAlertIssued = true;
                    logger.fatal(
                        "{} seconds have elapsed waiting for the partitioned region lock held by {}",
                        new Object[] {(ackWaitThreshold + ackSAThreshold) / 1000,
                            lockHolder});
                  }
                } else {
                  // either no lock holder now, or the lock holder has changed
                  // since the ackWaitThreshold last elapsed
                  suspected = false;
                  waitInterval = ackWaitThreshold;
                  lockHolder = null;
                }
              }
            }
          }
        }
      } finally {
        if (enableAlerts) {
          ReplyProcessor21.unforceSevereAlertProcessing();
        }
      }
    }

    private void basicTryLock(long time) {
      final Object key = this.lockName;

      final DistributionManager dm = cache.getInternalDistributedSystem().getDistributionManager();

      long start = System.currentTimeMillis();
      long end;
      long timeoutMS = time;
      if (timeoutMS < 0) {
        timeoutMS = Long.MAX_VALUE;
        end = Long.MAX_VALUE;
      } else {
        end = start + timeoutMS;
      }

      long ackSAThreshold =
          cache.getInternalDistributedSystem().getConfig().getAckSevereAlertThreshold() * 1000L;
      boolean suspected = false;
      boolean severeAlertIssued = false;
      DistributedMember lockHolder = null;

      long waitInterval;
      long ackWaitThreshold;

      if (ackSAThreshold > 0) {
        ackWaitThreshold =
            cache.getInternalDistributedSystem().getConfig().getAckWaitThreshold() * 1000L;
        waitInterval = ackWaitThreshold;
        start = System.currentTimeMillis();
      } else {
        waitInterval = timeoutMS;
        ackWaitThreshold = 0;
        start = 0;
      }

      do {
        try {
          waitInterval = Math.min(end - System.currentTimeMillis(), waitInterval);
          ReplyProcessor21.forceSevereAlertProcessing();
          this.lockOwned = this.lockService.lock(key, waitInterval, -1, true, false);
          if (this.lockOwned) {
            return;
          }
          if (ackSAThreshold > 0) {
            long elapsed = System.currentTimeMillis() - start;
            if (elapsed > ackWaitThreshold) {
              if (!suspected) {
                // start suspect processing on the holder of the lock
                suspected = true;
                severeAlertIssued = false; // in case this is a new lock holder
                waitInterval = ackSAThreshold;
                DLockRemoteToken remoteToken = this.lockService.queryLock(key);
                lockHolder = remoteToken.getLessee();
                if (lockHolder != null) {
                  dm.getDistribution().suspectMember((InternalDistributedMember) lockHolder,
                      "Has not released a global region entry lock in over "
                          + ackWaitThreshold / 1000 + " sec");
                }
              } else if (elapsed > ackSAThreshold) {
                DLockRemoteToken remoteToken = this.lockService.queryLock(key);
                if (lockHolder != null && remoteToken.getLessee() != null
                    && lockHolder.equals(remoteToken.getLessee())) {
                  if (!severeAlertIssued) {
                    severeAlertIssued = true;
                    logger.fatal(
                        "{} seconds have elapsed waiting for global region entry lock held by {}",
                        (ackWaitThreshold + ackSAThreshold) / 1000,
                        lockHolder);
                  }
                } else {
                  // the lock holder has changed
                  suspected = false;
                  waitInterval = ackWaitThreshold;
                  lockHolder = null;
                }
              }
            }
          } // ackSAThreshold processing
        } catch (IllegalStateException ex) {
          cache.getCancelCriterion().checkCancelInProgress(null);
          throw ex;
        } finally {
          ReplyProcessor21.unforceSevereAlertProcessing();
        }
      } while (System.currentTimeMillis() < end);
    }

    /**
     * Ask the grantor who has the lock
     *
     * @return the ID of the member holding the lock
     */
    public DistributedMember queryLock() {
      try {
        DLockRemoteToken remoteToken = this.lockService.queryLock(this.lockName);
        return remoteToken.getLessee();
      } catch (LockServiceDestroyedException e) {
        cache.getCancelCriterion().checkCancelInProgress(null);
        throw e;
      }
    }

    public void unlock() {
      if (this.lockOwned) {
        try {
          this.lockService.unlock(this.lockName);
        } catch (LockServiceDestroyedException e) {
          // cache was probably closed which destroyed this lock service
          // note: destroyed lock services release all held locks
          cache.getCancelCriterion().checkCancelInProgress(null);
          if (logger.isDebugEnabled()) {
            logger.debug("BucketLock#unlock: Lock service {} was destroyed", this.lockService, e);
          }
        } finally {
          this.lockOwned = false;
        }
      }
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof BucketLock)) {
        return false;
      }

      BucketLock other = (BucketLock) obj;
      if (!this.lockName.equals(other.lockName))
        return false;

      DLockService ls1 = lockService;
      DLockService ls2 = other.lockService;
      if (ls1 == null || ls2 == null) {
        // If both are null, return true, if only one is null, return false
        return ls1 == ls2;
      }
      return ls1.equals(ls2);
    }

    @Override
    public int hashCode() {
      return this.lockName.hashCode();
    }

    @Override
    public String toString() {
      return "BucketLock@" + System.identityHashCode(this) + "lockName=" + this.lockName
          + " lockService=" + this.lockService;
    }
  } // end class BucketLock

  public BucketLock getBucketLock(int bucketID) {
    String lockName = getLockNameForBucket2NodeModification(bucketID);
    return new BucketLock(lockName, getCache(), false);
  }

  public static class RegionLock extends BucketLock {
    protected RegionLock(String regionIdentifier, InternalCache cache) {
      super(regionIdentifier, cache, true);
    }

    @Override
    public String toString() {
      return "RegionLock@" + System.identityHashCode(this) + "lockName=" + super.lockName
          + " lockService=" + super.lockService;
    }
  }

  public class RecoveryLock extends BucketLock {
    protected RecoveryLock() {
      super(PartitionedRegion.this.getRegionIdentifier() + "-RecoveryLock", getCache(), false);
    }

    @Override
    public String toString() {
      return "RecoveryLock@" + System.identityHashCode(this) + "lockName=" + super.lockName
          + " lockService=" + super.lockService;
    }
  }

  @Override
  public String toString() {
    return new StringBuffer().append("Partitioned Region ").append("@")
        .append(Integer.toHexString(hashCode())).append(" [").append("path='").append(getFullPath())
        .append("'; dataPolicy=").append(this.getDataPolicy()).append("; prId=")
        .append(this.partitionedRegionId).append("; isDestroyed=").append(this.isDestroyed)
        .append("; isClosed=").append(this.isClosed).append("; retryTimeout=")
        .append(this.retryTimeout).append("; serialNumber=").append(getSerialNumber())

        .append("; partition attributes=").append(getPartitionAttributes().toString())
        .append("; on VM ").append(getMyId()).append("]").toString();
  }

  public RegionLock getRegionLock() {
    return getRegionLock(getRegionIdentifier(), getGemFireCache());
  }

  public static RegionLock getRegionLock(String regionIdentifier, InternalCache cache) {
    return new RegionLock(regionIdentifier, cache);
  }

  public RecoveryLock getRecoveryLock() {
    return new RecoveryLock();
  }

  public Node getNode() {
    return this.node;
  }

  @Override
  public LoaderHelper createLoaderHelper(Object key, Object callbackArgument,
      boolean netSearchAllowed, boolean netLoadAllowed, SearchLoadAndWriteProcessor searcher) {
    return new LoaderHelperImpl(this, key, callbackArgument, netSearchAllowed, netLoadAllowed,
        searcher);
  }

  public PRHARedundancyProvider getRedundancyProvider() {
    return this.redundancyProvider;
  }

  /**
   * This method checks if region is closed.
   */
  public void checkClosed() {
    if (this.isClosed) {
      throw new RegionDestroyedException(
          String.format("PR %s is locally closed", this),
          getFullPath());
    }
  }

  /**
   * This method checks if region is closed or destroyed.
   */
  public void checkClosedOrDestroyed() {
    checkClosed();
    if (isDestroyed()) {
      throw new RegionDestroyedException(toString(), getFullPath());
    }
  }

  public void setHaveCacheLoader() {
    if (!this.haveCacheLoader) {
      this.haveCacheLoader = true;
    }
  }

  /**
   * This method closes the partitioned region locally. It is invoked from the postDestroyRegion
   * method of LocalRegion {@link LocalRegion}, which is overridden in PartitionedRegion This method
   * <br>
   * Updates this prId in prIDMap <br>
   * Cleanups the data store (Removes the bucket mapping from b2n region, locally destroys b2n and
   * destroys the bucket regions. <br>
   * sends destroyPartitionedRegionMessage to other VMs so that they update their RegionAdvisor <br>
   * Sends BackupBucketMessage to other nodes
   *
   * @param event the region event
   */
  private void closePartitionedRegion(RegionEventImpl event) {
    final boolean isClose = event.getOperation().isClose();
    if (isClose) {
      this.isClosed = true;
    }
    final RegionLock rl = getRegionLock();
    try {
      rl.lock();
      logger.info("Partitioned Region {} with prId={} closing.",
          new Object[] {getFullPath(), getPRId()});
      if (!checkIfAlreadyDestroyedOrOldReference()) {
        PartitionedRegionHelper.removeGlobalMetadataForFailedNode(getNode(),
            this.getRegionIdentifier(), getGemFireCache(), false);
      }
      int serials[] = getRegionAdvisor().getBucketSerials();

      if (!event.getOperation().isClose() && getDiskStore() != null && getDataStore() != null) {
        for (BucketRegion bucketRegion : getDataStore().getAllLocalBucketRegions()) {
          bucketRegion.isDestroyingDiskRegion = true;
          bucketRegion.getDiskRegion().beginDestroy(bucketRegion);
        }
      }

      sendDestroyRegionMessage(event, serials); // Notify other members that this VM
      // no longer has the region

      // Must clean up pridToPR map so that messages do not get access to buckets
      // which should be destroyed
      synchronized (prIdToPR) {
        prIdToPR.put(getPRId(), PRIdMap.LOCALLY_DESTROYED, false);
      }

      redundancyProvider.shutdown();

      if (this.dataStore != null) {
        this.dataStore.cleanUp(true, !isClose);
      }
    } finally {
      // Make extra sure that the static is cleared in the event
      // a new cache is created
      synchronized (prIdToPR) {
        prIdToPR.put(getPRId(), PRIdMap.LOCALLY_DESTROYED, false);
      }

      rl.unlock();
    }

    logger.info("Partitioned Region {} with prId={} closed.",
        new Object[] {getFullPath(), getPRId()});
  }

  public void checkForColocatedChildren() {
    List<PartitionedRegion> listOfChildRegions = ColocationHelper.getColocatedChildRegions(this);
    if (listOfChildRegions.size() != 0) {
      List<String> childRegionList = new ArrayList<String>();
      for (PartitionedRegion childRegion : listOfChildRegions) {
        if (!childRegion.getName().contains(ParallelGatewaySenderQueue.QSTRING)) {
          childRegionList.add(childRegion.getFullPath());
        }
      }
      if (!childRegionList.isEmpty()) {
        throw new IllegalStateException(String.format(
            "The parent region [%s] in colocation chain cannot "
                + "be destroyed, unless all its children [%s] are destroyed",
            this.getFullPath(), childRegionList));
      }
    }
  }

  @Override
  public void destroyRegion(Object aCallbackArgument)
      throws CacheWriterException, TimeoutException {
    this.cache.invokeBeforeDestroyed(this);
    checkForColocatedChildren();
    getDataView().checkSupportsRegionDestroy();
    checkForLimitedOrNoAccess();

    RegionEventImpl event = new RegionEventImpl(this, Operation.REGION_DESTROY, aCallbackArgument,
        false, getMyId(), generateEventID());
    basicDestroyRegion(event, true);
  }

  private void stopMissingColocatedRegionLogger() {
    ColocationLogger colocationLogger = missingColocatedRegionLogger.getAndSet(null);
    if (colocationLogger != null) {
      colocationLogger.stop();
    }
  }

  public void addMissingColocatedRegionLogger(String childName) {
    getOrSetMissingColocatedRegionLogger().addMissingChildRegion(childName);
  }

  public void addMissingColocatedRegionLogger(PartitionedRegion childRegion) {
    getOrSetMissingColocatedRegionLogger().addMissingChildRegions(childRegion);
  }

  /**
   * Returns existing ColocationLogger or creates and sets a new instance if one does not yet exist.
   * Spins to ensure that if another thread sets ColocationLogger, we return that one instead of
   * setting another instance. Checks CancelCriterion during spin and either returns an instance or
   * throws NullPointerException. The only way to exist the loop is with an instance or by throwing
   * CancelException if the Cache closes.
   */
  private ColocationLogger getOrSetMissingColocatedRegionLogger() {
    ColocationLogger colocationLogger = missingColocatedRegionLogger.get();
    while (colocationLogger == null) {
      colocationLogger = colocationLoggerFactory.startColocationLogger(this);
      if (!missingColocatedRegionLogger.compareAndSet(null, colocationLogger)) {
        colocationLogger = missingColocatedRegionLogger.get();
      }
      getCancelCriterion().checkCancelInProgress();
    }
    requireNonNull(colocationLogger);
    return colocationLogger;
  }

  public List<String> getMissingColocatedChildren() {
    ColocationLogger colocationLogger = missingColocatedRegionLogger.get();
    if (colocationLogger != null) {
      return colocationLogger.updateAndGetMissingChildRegions();
    }
    return Collections.emptyList();
  }

  public void destroyParallelGatewaySenderRegion(Operation op, boolean cacheWrite, boolean lock,
      boolean callbackEvents) {

    if (logger.isDebugEnabled()) {
      logger.debug("Destroying parallel queue region for senders: {}",
          this.getParallelGatewaySenderIds());
    }

    boolean keepWaiting = true;

    while (true) {
      List<String> pausedSenders = new ArrayList<String>();
      List<ConcurrentParallelGatewaySenderQueue> parallelQueues =
          new ArrayList<ConcurrentParallelGatewaySenderQueue>();
      isDestroyedForParallelWAN = true;
      int countOfQueueRegionsToBeDestroyed = 0;
      for (String senderId : this.getParallelGatewaySenderIds()) {
        AbstractGatewaySender sender =
            (AbstractGatewaySender) this.cache.getGatewaySender(senderId);
        if (sender == null || sender.getEventProcessor() == null) {
          continue;
        }

        if (cacheWrite) { // in case of destroy operation senders should be
                          // resumed
          if (sender.isPaused()) {
            pausedSenders.add(senderId);
            continue;
          }
        }

        if (pausedSenders.isEmpty()) { // if there are puase sender then only
                                       // check for other pause senders instead
                                       // of creating list of shadowPR
          AbstractGatewaySenderEventProcessor ep = sender.getEventProcessor();
          if (ep == null)
            continue;
          ConcurrentParallelGatewaySenderQueue parallelQueue =
              (ConcurrentParallelGatewaySenderQueue) ep.getQueue();
          PartitionedRegion parallelQueueRegion = parallelQueue.getRegion(this.getFullPath());

          // this may be removed in previous iteration
          if (parallelQueueRegion == null || parallelQueueRegion.isDestroyed
              || parallelQueueRegion.isClosed) {
            continue;
          }

          parallelQueues.add(parallelQueue);
          countOfQueueRegionsToBeDestroyed++;
        }
      }

      if (!pausedSenders.isEmpty()) {
        String exception = null;
        if (pausedSenders.size() == 1) {
          exception =
              String.format("GatewaySender %s is paused. Resume it before destroying region %s.",
                  pausedSenders, this.getName());
        } else {
          exception =
              String.format(
                  "GatewaySenders %s are paused. Resume them before destroying region %s.",
                  pausedSenders, this.getName());
        }
        isDestroyedForParallelWAN = false;
        throw new GatewaySenderException(exception);
      }

      if (countOfQueueRegionsToBeDestroyed == 0) {
        break;
      }

      for (ConcurrentParallelGatewaySenderQueue parallelQueue : parallelQueues) {
        PartitionedRegion parallelQueueRegion = parallelQueue.getRegion(this.getFullPath());
        // CacheWrite true == distributedDestoy. So in case of false, dont wait
        // for queue to drain
        // parallelQueueRegion.size() = With DistributedDestroym wait for queue
        // to drain
        // keepWaiting : comes from the MAXIMUM_SHUTDOWN_WAIT_TIME case handled
        if (cacheWrite && parallelQueueRegion.size() != 0 && keepWaiting) {
          continue;
        } else {// In any case, destroy shadow PR locally. distributed destroy of
                // userPR will take care of detsroying shadowPR locally on other
                // nodes.
          RegionEventImpl event = null;
          if (op.isClose()) { // In case of cache close operation, we want SPR's basic destroy to go
                              // through CACHE_CLOSE condition of postDestroyRegion not
                              // closePartitionedRegion code
            event = new RegionEventImpl(parallelQueueRegion, op, null, false, getMyId(),
                generateEventID());
          } else {
            event = new RegionEventImpl(parallelQueueRegion, Operation.REGION_LOCAL_DESTROY, null,
                false, getMyId(), generateEventID());
          }
          parallelQueueRegion.basicDestroyRegion(event, false, lock, callbackEvents);
          parallelQueue.removeShadowPR(this.getFullPath());
          countOfQueueRegionsToBeDestroyed--;
          continue;
        }
      }

      if (countOfQueueRegionsToBeDestroyed == 0) {
        break;
      }

      if (cacheWrite) {
        if (AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME == -1) {
          keepWaiting = true;
          try {
            Thread.sleep(5000);
          } catch (InterruptedException ignore) {
            // interrupted
          }
        } else {
          try {
            Thread.sleep(AbstractGatewaySender.MAXIMUM_SHUTDOWN_WAIT_TIME * 1000L);
          } catch (InterruptedException ignore) {/* ignore */
            // interrupted
          }
          keepWaiting = false;
        }
      }
    }
  }

  @Override
  public void localDestroyRegion(Object aCallbackArgument) {
    getDataView().checkSupportsRegionDestroy();
    String prName = this.getColocatedWith();
    List<PartitionedRegion> listOfChildRegions = ColocationHelper.getColocatedChildRegions(this);

    List<String> childRegionsWithoutSendersList = new ArrayList<String>();
    if (listOfChildRegions.size() != 0) {
      for (PartitionedRegion childRegion : listOfChildRegions) {
        if (!childRegion.getName().contains(ParallelGatewaySenderQueue.QSTRING)) {
          childRegionsWithoutSendersList.add(childRegion.getFullPath());
        }
      }
    }

    if ((prName != null) || (!childRegionsWithoutSendersList.isEmpty())) {
      throw new UnsupportedOperationException(
          "Any Region in colocation chain cannot be destroyed locally.");
    }

    RegionEventImpl event = new RegionEventImpl(this, Operation.REGION_LOCAL_DESTROY,
        aCallbackArgument, false, getMyId(), generateEventID()/* generate EventID */);
    try {
      basicDestroyRegion(event, false);
    } catch (CacheWriterException e) {
      // not possible with local operation, CacheWriter not called
      throw new Error("CacheWriterException should not be thrown in localDestroyRegion", e);
    } catch (TimeoutException e) {
      // not possible with local operation, no distributed locks possible
      throw new Error("TimeoutException should not be thrown in localDestroyRegion", e);
    }
  }

  /**
   * This method actually destroys the local accessor and data store. This method is invoked from
   * the postDestroyRegion method of LocalRegion {@link LocalRegion}, which is overridden in
   * PartitionedRegion This method is invoked from postDestroyRegion method. If origin is local:
   * <br>
   * Takes region lock <br>
   * Removes prId from PRIDMap <br>
   * Does data store cleanup (removal of bucket regions) <br>
   * Sends destroyRegionMessage to other nodes <br>
   * Removes it from allPartitionedRegions <br>
   * Destroys bucket2node region
   *
   * @param event the RegionEvent that triggered this operation
   *
   * @see #destroyPartitionedRegionLocally(boolean)
   * @see #destroyPartitionedRegionGlobally(RegionEventImpl)
   * @see #destroyCleanUp(RegionEventImpl, int[])
   * @see DestroyPartitionedRegionMessage
   */
  private void destroyPartitionedRegion(RegionEventImpl event) {
    final RegionLock rl = getRegionLock();
    boolean isLocked = false;

    boolean removeFromDisk = !event.getOperation().isClose();
    try {
      logger.info("Partitioned Region {} with prId={} is being destroyed.",
          new Object[] {getFullPath(), getPRId()});
      if (!event.isOriginRemote()) {
        try {
          rl.lock();
          isLocked = true;
        } catch (CancelException ignore) {
          // ignore
        }
        if (!destroyPartitionedRegionGlobally(event)) {
          if (destroyPartitionedRegionLocally(removeFromDisk)) {
            sendLocalDestroyRegionMessage(event);
          }
        }
      } else {
        PartitionRegionConfig prConfig = null;
        try {
          prConfig = getPRRoot().get(getRegionIdentifier());
        } catch (CancelException ignore) {
          // ignore; metadata not accessible
        }
        if (!checkIfAlreadyDestroyedOrOldReference() && null != prConfig
            && !prConfig.getIsDestroying()) {
          try {
            rl.lock();
            isLocked = true;
          } catch (CancelException ignore) {
            // ignore
          }
          if (!destroyPartitionedRegionGlobally(event)) {
            if (destroyPartitionedRegionLocally(removeFromDisk)) {
              sendLocalDestroyRegionMessage(event);
            }
          }
        } else {
          if (destroyPartitionedRegionLocally(removeFromDisk)) {
            sendLocalDestroyRegionMessage(event);
          }
        }
      }
      logger.info("Partitioned Region {} with prId={} is destroyed.",
          new Object[] {getFullPath(), getPRId()});
    } finally {
      try {
        if (isLocked) {
          rl.unlock();
        }
      } catch (Exception es) {
        logger.info("Caught exception while trying to unlock during Region destruction.",
            es);
      }
    }
  }

  /**
   * This method destroys the PartitionedRegion globally. It sends destroyRegion message to other
   * nodes and handles cleaning up of the data stores and meta-data.
   *
   * @param event the RegionEvent which triggered this global destroy operation
   * @return true if the region was found globally.
   */
  private boolean destroyPartitionedRegionGlobally(RegionEventImpl event) {
    if (checkIfAlreadyDestroyedOrOldReference()) {
      return false;
    }
    PartitionRegionConfig prConfig;
    try {
      prConfig = getPRRoot().get(this.getRegionIdentifier());
    } catch (CancelException ignore) {
      // global data not accessible, don't try to finish global destroy.
      return false;
    }

    if (prConfig == null || prConfig.getIsDestroying()) {
      return false;
    }
    prConfig.setIsDestroying();
    try {
      getPRRoot().put(this.getRegionIdentifier(), prConfig);
    } catch (CancelException ignore) {
      // ignore; metadata not accessible
    }
    int serials[] = getRegionAdvisor().getBucketSerials();
    final boolean isClose = event.getOperation().isClose();
    destroyPartitionedRegionLocally(!isClose);
    destroyCleanUp(event, serials);
    return true;
  }

  private void sendLocalDestroyRegionMessage(RegionEventImpl event) {
    int serials[] = getRegionAdvisor().getBucketSerials();
    RegionEventImpl eventForLocalDestroy = (RegionEventImpl) event.clone();
    eventForLocalDestroy.setOperation(Operation.REGION_LOCAL_DESTROY);
    sendDestroyRegionMessage(event, serials);
  }

  /**
   * This method: <br>
   * Sends DestroyRegionMessage to other nodes <br>
   * Removes this PartitionedRegion from allPartitionedRegions <br>
   * Destroys bucket2node region <br>
   *
   * @param event the RegionEvent that triggered the region clean up
   *
   * @see DestroyPartitionedRegionMessage
   */
  private void destroyCleanUp(RegionEventImpl event, int serials[]) {
    String rId = getRegionIdentifier();
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("PartitionedRegion#destroyCleanUp: Destroying region: {}", getFullPath());
      }
      sendDestroyRegionMessage(event, serials);
      try {
        // if this event is global destruction of the region everywhere, remove
        // it from the pr root configuration
        if (null != getPRRoot()) {
          getPRRoot().destroy(rId);
        }
      } catch (EntryNotFoundException ex) {
        if (logger.isDebugEnabled()) {
          logger.debug("PartitionedRegion#destroyCleanup: caught exception", ex);
        }
      } catch (CancelException ignore) {
        // ignore; metadata not accessible
      }
    } finally {
      if (logger.isDebugEnabled()) {
        logger.debug("PartitionedRegion#destroyCleanUp: " + "Destroyed region: {}", getFullPath());
      }
    }
  }

  /**
   * Sends the partitioned region specific destroy region message and waits for any responses. This
   * message is also sent in close/localDestroyRegion operation. When it is sent in Cache
   * close/Region close/localDestroyRegion operations, it results in updating of RegionAdvisor on
   * recipient nodes.
   *
   * @param event the destruction event
   * @param serials the bucket serials hosted locally
   * @see Region#destroyRegion()
   * @see Region#close()
   * @see Region#localDestroyRegion()
   * @see GemFireCacheImpl#close()
   */
  private void sendDestroyRegionMessage(RegionEventImpl event, int serials[]) {
    boolean retry = true;
    while (retry) {
      retry = attemptToSendDestroyRegionMessage(event, serials);
    }
  }

  private boolean attemptToSendDestroyRegionMessage(RegionEventImpl event, int serials[]) {
    if (getPRRoot() == null) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Partition region {} failed to initialize. Remove its profile from remote members.",
            this);
      }
      new UpdateAttributesProcessor(this, true).distribute(false);
      return false;
    }
    final HashSet configRecipients = new HashSet(getRegionAdvisor().adviseAllPRNodes());

    // It's possible this instance has not been initialized
    // or hasn't gotten through initialize() far enough to have
    // sent a CreateRegionProcessor message, bug 36048
    try {
      final PartitionRegionConfig prConfig = getPRRoot().get(getRegionIdentifier());

      if (prConfig != null) {
        // Fix for bug#34621 by Tushar
        Iterator itr = prConfig.getNodes().iterator();
        while (itr.hasNext()) {
          InternalDistributedMember idm = ((Node) itr.next()).getMemberId();
          if (!idm.equals(getMyId())) {
            configRecipients.add(idm);
          }
        }
      }
    } catch (CancelException ignore) {
      // ignore
    }

    try {
      DestroyPartitionedRegionResponse resp =
          DestroyPartitionedRegionMessage.send(configRecipients, this, event, serials);
      resp.waitForRepliesUninterruptibly();
    } catch (ReplyException e) {
      if (e.getRootCause() instanceof ForceReattemptException) {
        return true;
      }
      logger.warn(
          "PartitionedRegion#sendDestroyRegionMessage: Caught exception during DestroyRegionMessage send and waiting for response",
          e);
    }
    return false;
  }

  /**
   * This method is used to destroy this partitioned region data and its associated store and
   * removal of its PartitionedRegionID from local prIdToPR map. This method is called from
   * destroyPartitionedRegion Method and removes the entry from prIdMap and cleans the data store
   * buckets. It first checks whether this PartitionedRegion is already locally destroyed. If it is,
   * this call returns, else if process with the removal from prIdMap and dataStore cleanup.
   *
   * @return wether local destroy happened
   *
   * @see #destroyPartitionedRegion(RegionEventImpl)
   */
  boolean destroyPartitionedRegionLocally(boolean removeFromDisk) {
    synchronized (this) {
      if (this.isLocallyDestroyed) {
        return false;
      }
      this.locallyDestroyingThread = Thread.currentThread();
      this.isLocallyDestroyed = true;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("destroyPartitionedRegionLocally: Starting destroy for PR = {}", this);
    }
    try {
      synchronized (prIdToPR) {
        prIdToPR.remove(getPRId());
      }
      this.redundancyProvider.shutdown(); // see bug 41094
      if (this.dataStore != null) {
        this.dataStore.cleanUp(false, removeFromDisk);
      }
    } finally {
      this.getRegionAdvisor().close();
      getPrStats().close();
      this.cache.getInternalResourceManager(false).removeResourceListener(this);
      this.locallyDestroyingThread = null;
      if (logger.isDebugEnabled()) {
        logger.debug("destroyPartitionedRegionLocally: Ending destroy for PR = {}", this);
      }
    }
    return true;
  }

  /**
   * This method is invoked from recursiveDestroyRegion method of LocalRegion. This method checks
   * the region type and invokes the relevant method.
   *
   * @param destroyDiskRegion - true if the contents on disk should be destroyed
   * @param event the RegionEvent
   */
  @Override
  protected void postDestroyRegion(boolean destroyDiskRegion, RegionEventImpl event) {
    if (logger.isDebugEnabled()) {
      logger.debug("PartitionedRegion#postDestroyRegion: {}", this);
    }
    Assert.assertTrue(this.isDestroyed || this.isClosed);

    // Fixes 44551 - wait for persistent buckets to finish
    // recovering before sending the destroy region message
    // any GII or wait for persistent recovery will be aborted by the destroy
    // flag being set to true, so this shouldn't take long.
    this.redundancyProvider.waitForPersistentBucketRecovery();
    // fix #39196 OOME caused by leak in GemFireCache.partitionedRegions
    this.cache.removePartitionedRegion(this);
    this.cache.getInternalResourceManager(false).removeResourceListener(this);

    final Operation op = event.getOperation();
    stopMissingColocatedRegionLogger();
    if (op.isClose() || Operation.REGION_LOCAL_DESTROY.equals(op)) {
      try {
        if (Operation.CACHE_CLOSE.equals(op) || Operation.FORCED_DISCONNECT.equals(op)) {
          int serials[] = getRegionAdvisor().getBucketSerials();

          try {
            getRegionAdvisor().closeBucketAdvisors();
            // BUGFIX for bug#34672 by Tushar Apshankar. It would update the
            // advisors on other nodes about cache closing of this PartitionedRegion
            sendDestroyRegionMessage(event, serials);

            // Because this code path never cleans up the real buckets, we need
            // to log the fact that those buckets are destroyed here
            if (RegionLogger.isEnabled()) {
              PartitionedRegionDataStore store = getDataStore();
              if (store != null) {
                for (BucketRegion bucket : store.getAllLocalBucketRegions()) {
                  RegionLogger.logDestroy(bucket.getFullPath(), getMyId(), bucket.getPersistentID(),
                      true);
                }
              }
            }
          } catch (CancelException ignore) {
            // Don't throw this; we're just trying to remove the region.
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "postDestroyRegion: failed sending DestroyRegionMessage due to cache closure");
            }
          } finally {
            // Since we are not calling closePartitionedRegion
            // we need to cleanup any diskStore we own here.
            // Why don't we call closePartitionedRegion?
            // Instead of closing it, we need to register it to be closed later
            // Otherwise, when the cache close tries to close all of the bucket regions,
            // they'll fail because their disk store is already closed.
            DiskStoreImpl dsi = getDiskStore();
            if (dsi != null && dsi.getOwnedByRegion()) {
              cache.addDiskStore(dsi);
            }
          }

          // Majority of cache close operations handled by
          // afterRegionsClosedByCacheClose(GemFireCache
          // cache) or GemFireCache.close()
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("Making closePartitionedRegion call for {} with origin = {} op= {}", this,
                event.isOriginRemote(), op);
          }
          try {
            closePartitionedRegion(event);
          } finally {
            if (Operation.REGION_LOCAL_DESTROY.equals(op)) {
              DiskStoreImpl dsi = getDiskStore();
              if (dsi != null && dsi.getOwnedByRegion()) {
                dsi.destroy();
              }
            }
          }
        }
      } finally {
        // tell other members to recover redundancy for any buckets
        this.getRegionAdvisor().close();
        getPrStats().close();
      }
    } else if (Operation.REGION_DESTROY.equals(op) || Operation.REGION_EXPIRE_DESTROY.equals(op)) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "PartitionedRegion#postDestroyRegion: Making destroyPartitionedRegion call for {} with originRemote = {}",
            this, event.isOriginRemote());
      }
      destroyPartitionedRegion(event);
    } else {
      Assert.assertTrue(false, "Unknown op" + op);
    }

    // set client routing information into the event
    // The destroy operation in case of PR is distributed differently
    // hence the FilterRoutingInfo is set here instead of
    // DistributedCacheOperation.distribute().
    if (!isUsedForMetaRegion() && !isUsedForPartitionedRegionAdmin()
        && !isUsedForPartitionedRegionBucket() && !isUsedForParallelGatewaySenderQueue()) {
      FilterRoutingInfo localCqFrInfo = getFilterProfile().getFilterRoutingInfoPart1(event,
          FilterProfile.NO_PROFILES, Collections.emptySet());
      FilterRoutingInfo localCqInterestFrInfo =
          getFilterProfile().getFilterRoutingInfoPart2(localCqFrInfo, event);
      if (localCqInterestFrInfo != null) {
        event.setLocalFilterInfo(localCqInterestFrInfo.getLocalFilterInfo());
      }
    }

    if (destroyDiskRegion) {
      DiskStoreImpl dsi = getDiskStore();
      if (dsi != null && getDataPolicy().withPersistence()) {
        dsi.removePersistentPR(getFullPath());
        // Fix for support issue 7870 - remove this regions
        // config from the parent disk store, if we are removing the region.
        if (colocatedWithRegion != null && colocatedWithRegion.getDiskStore() != null
            && colocatedWithRegion.getDiskStore() != dsi) {
          colocatedWithRegion.getDiskStore().removePersistentPR(getFullPath());

        }
      }
    }
    if (colocatedWithRegion != null) {
      colocatedWithRegion.getColocatedByList().remove(this);
    }

    if (bucketSorter != null) {
      bucketSorter.shutdown();
    }

    RegionLogger.logDestroy(getName(),
        this.cache.getInternalDistributedSystem().getDistributedMember(), null, op.isClose());
  }

  /**
   * This method checks whether this PartitionedRegion is eligible for the destruction or not. It
   * first gets the prConfig for this region, and if it NULL, it sends a
   * destroyPartitionedRegionLocally call as a pure precautionary measure. If it is not null, we
   * check if this call is intended for this region only and there is no new PartitionedRegion
   * creation with the same name. This check fixes, bug # 34621.
   *
   * @return true, if it is eligible for the region destroy
   */
  private boolean checkIfAlreadyDestroyedOrOldReference() {
    PartitionRegionConfig prConfig = null;
    try {
      prConfig = getPRRoot().get(this.getRegionIdentifier());
    } catch (CancelException ignore) {
      // ignore, metadata not accessible
    }
    boolean isAlreadyDestroyedOrOldReference = false;
    if (null == prConfig) {
      isAlreadyDestroyedOrOldReference = true;
    } else {
      // If this reference is a destroyed reference and a new PR is created
      // after destruction of the older one is complete, bail out.
      if (prConfig.getPRId() != this.partitionedRegionId) {
        isAlreadyDestroyedOrOldReference = true;
      }
    }
    return isAlreadyDestroyedOrOldReference;
  }

  @Override
  public void dispatchListenerEvent(EnumListenerEvent op, InternalCacheEvent event) {
    // don't dispatch the event if the interest policy forbids it
    if (hasListener()) {
      if (event.getOperation().isEntry()) {
        EntryEventImpl ev = (EntryEventImpl) event;
        if (!ev.getInvokePRCallbacks()) {
          if (this.getSubscriptionAttributes()
              .getInterestPolicy() == InterestPolicy.CACHE_CONTENT) {
            if (logger.isTraceEnabled(LogMarker.DM_BRIDGE_SERVER_VERBOSE)) {
              logger.trace(LogMarker.DM_BRIDGE_SERVER_VERBOSE,
                  "not dispatching PR event in this member as there is no interest in it");
            }
            return;
          }
        }
      }
      super.dispatchListenerEvent(op, event);
    }
  }

  @Override
  void generateLocalFilterRouting(InternalCacheEvent event) {
    if (isGenerateLocalFilterRoutingNeeded(event)) {
      super.generateLocalFilterRouting(event);
    }
  }

  /**
   * Invoke the cache writer before a put is performed. Each BucketRegion delegates to the
   * CacheWriter on the PartitionedRegion meaning that CacheWriters on a BucketRegion should only be
   * used for internal purposes.
   *
   * @see BucketRegion#cacheWriteBeforePut(EntryEventImpl, Set, CacheWriter, boolean, Object)
   */
  @Override
  public void cacheWriteBeforePut(EntryEventImpl event, Set netWriteRecipients,
      CacheWriter localWriter, boolean requireOldValue, Object expectedOldValue)
      throws CacheWriterException, TimeoutException {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    // return if notifications are inhibited
    if (event.inhibitAllNotifications()) {
      if (isDebugEnabled) {
        logger.debug("Notification inhibited for key {}", event.getKey());
      }

      return;
    }

    final boolean isNewKey = event.getOperation().isCreate();
    serverPut(event, requireOldValue, expectedOldValue);
    if (localWriter == null && (netWriteRecipients == null || netWriteRecipients.isEmpty())) {
      if (isDebugEnabled) {
        logger.debug(
            "cacheWriteBeforePut: beforePut empty set returned by advisor.adviseNetWrite in netWrite");
      }
      return;
    }

    final long start = getCachePerfStats().startCacheWriterCall();

    try {
      SearchLoadAndWriteProcessor processor = SearchLoadAndWriteProcessor.getProcessor();
      processor.initialize(this, "preUpdate", null);
      try {
        if (!isNewKey) {
          if (isDebugEnabled) {
            logger.debug("cacheWriteBeforePut: doNetWrite(BEFOREUPDATE)");
          }
          processor.doNetWrite(event, netWriteRecipients, localWriter,
              SearchLoadAndWriteProcessor.BEFOREUPDATE);
        } else {
          if (isDebugEnabled) {
            logger.debug("cacheWriteBeforePut: doNetWrite(BEFORECREATE)");
          }
          // sometimes the op will erroneously be UPDATE
          processor.doNetWrite(event, netWriteRecipients, localWriter,
              SearchLoadAndWriteProcessor.BEFORECREATE);
        }
      } finally {
        processor.release();
      }
    } finally {
      getCachePerfStats().endCacheWriterCall(start);
    }
  }

  /**
   * Invoke the CacheWriter before the detroy operation occurs. Each BucketRegion delegates to the
   * CacheWriter on the PartitionedRegion meaning that CacheWriters on a BucketRegion should only be
   * used for internal purposes.
   *
   * @see BucketRegion#cacheWriteBeforeDestroy(EntryEventImpl, Object)
   */
  @Override
  public boolean cacheWriteBeforeDestroy(EntryEventImpl event, Object expectedOldValue)
      throws CacheWriterException, EntryNotFoundException, TimeoutException {
    // return if notifications are inhibited
    if (event.inhibitAllNotifications()) {
      if (logger.isDebugEnabled()) {
        logger.debug("Notification inhibited for key {}", event.getKey());
      }
      return false;
    }

    if (event.isDistributed()) {
      serverDestroy(event, expectedOldValue);
      CacheWriter localWriter = basicGetWriter();
      Set netWriteRecipients = localWriter == null ? this.distAdvisor.adviseNetWrite() : null;

      if (localWriter == null && (netWriteRecipients == null || netWriteRecipients.isEmpty())) {
        return false;
      }

      final long start = getCachePerfStats().startCacheWriterCall();
      try {
        event.setOldValueFromRegion();
        SearchLoadAndWriteProcessor processor = SearchLoadAndWriteProcessor.getProcessor();
        processor.initialize(this, event.getKey(), null);
        processor.doNetWrite(event, netWriteRecipients, localWriter,
            SearchLoadAndWriteProcessor.BEFOREDESTROY);
        processor.release();
      } finally {
        getCachePerfStats().endCacheWriterCall(start);
      }
      return true;
    }
    return false;
  }

  /**
   * Send a message to all PartitionedRegion participants, telling each member of the
   * PartitionedRegion with a datastore to dump the contents of the buckets to the system.log and
   * validate that the meta-data for buckets agrees with the data store's perspective
   *
   * @param distribute true will distributed a DumpBucketsMessage to PR nodes
   * @see #validateAllBuckets()
   */
  public void dumpAllBuckets(boolean distribute) throws ReplyException {
    if (logger.isDebugEnabled()) {
      logger.debug("[dumpAllBuckets] distribute={} {}", distribute, this);
    }
    getRegionAdvisor().dumpProfiles("dumpAllBuckets");
    if (distribute) {
      PartitionResponse response = DumpBucketsMessage.send(getRegionAdvisor().adviseAllPRNodes(),
          this, false /* only validate */, false);
      response.waitForRepliesUninterruptibly();
    }
    if (this.dataStore != null) {
      this.dataStore.dumpEntries(false /* onlyValidate */);
    }
  }

  @Override
  public void dumpBackingMap() {
    dumpAllBuckets(true);
  }

  /**
   * Send a message to all PartitionedRegion participants, telling each member of the
   * PartitionedRegion with a datastore to validate that the meta-data for buckets agrees with the
   * data store's perspective
   *
   * @see #dumpAllBuckets(boolean)
   */
  public void validateAllBuckets() throws ReplyException {
    PartitionResponse response = DumpBucketsMessage.send(getRegionAdvisor().adviseAllPRNodes(),
        this, true /* only validate */, false);
    response.waitForRepliesUninterruptibly();
    if (this.dataStore != null) {
      this.dataStore.dumpEntries(true /* onlyValidate */);
    }
  }

  /**
   * Sends a message to all the {@code PartitionedRegion} participants, telling each member of the
   * PartitionedRegion to dump the nodelist in bucket2node metadata for specified bucketId.
   */
  public void sendDumpB2NRegionForBucket(int bucketId) {
    getRegionAdvisor().dumpProfiles("dumpB2NForBucket");
    try {
      PartitionResponse response =
          DumpB2NRegion.send(this.getRegionAdvisor().adviseAllPRNodes(), this, bucketId, false);
      response.waitForRepliesUninterruptibly();
      this.dumpB2NForBucket(bucketId);
    } catch (ReplyException re) {
      if (logger.isDebugEnabled()) {
        logger.debug("sendDumpB2NRegionForBucket got ReplyException", re);
      }
    } catch (CancelException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("sendDumpB2NRegionForBucket got CacheClosedException", e);
      }
    } catch (RegionDestroyedException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("sendDumpB2RegionForBucket got RegionDestroyedException", e);
      }
    }
  }

  /**
   * Logs the b2n nodelist for specified bucketId.
   */
  public void dumpB2NForBucket(int bId) {
    getRegionAdvisor().getBucket(bId).getBucketAdvisor()
        .dumpProfiles("Dumping advisor bucket meta-data for bId=" + bucketStringForLogs(bId)
            + " aka " + getBucketName(bId));
  }

  /**
   * Send a message to all PartitionedRegion participants, telling each member of the
   * PartitionedRegion with a datastore to dump the contents of the allPartitionedRegions for this
   * PartitionedRegion.
   */
  public void sendDumpAllPartitionedRegions() throws ReplyException {
    getRegionAdvisor().dumpProfiles("dumpAllPartitionedRegions");
    PartitionResponse response =
        DumpAllPRConfigMessage.send(getRegionAdvisor().adviseAllPRNodes(), this);
    response.waitForRepliesUninterruptibly();
    dumpSelfEntryFromAllPartitionedRegions();
  }

  /**
   * This method prints the content of the allPartitionedRegion's contents for this
   * PartitionedRegion.
   */
  public void dumpSelfEntryFromAllPartitionedRegions() {
    StringBuilder sb = new StringBuilder(getPRRoot().getFullPath());
    sb.append("Dumping allPartitionedRegions for ");
    sb.append(this);
    sb.append("\n");
    sb.append(getPRRoot().get(getRegionIdentifier()));
    logger.debug(sb.toString());
  }

  /**
   * A test method to get the list of all the bucket ids for the partitioned region in the data
   * Store.
   */
  public List<Integer> getLocalBucketsListTestOnly() {
    List<Integer> localBucketList = null;
    if (this.dataStore != null) {
      localBucketList = this.dataStore.getLocalBucketsListTestOnly();
    }
    return localBucketList;
  }

  /**
   * A test method to get the list of all the primary bucket ids for the partitioned region in the
   * data Store.
   */
  public List<Integer> getLocalPrimaryBucketsListTestOnly() {
    List<Integer> localPrimaryList = null;
    if (this.dataStore != null) {
      localPrimaryList = this.dataStore.getLocalPrimaryBucketsListTestOnly();
    }
    return localPrimaryList;
  }

  /** doesn't throw RegionDestroyedException, used by CacheDistributionAdvisor */
  @Override
  public DistributionAdvisee getParentAdvisee() {
    return (DistributionAdvisee) basicGetParentRegion();
  }

  /**
   * A Simple class used to track retry time for Region operations Does not provide any
   * synchronization or concurrent safety
   */
  public static class RetryTimeKeeper {
    private long totalTimeInRetry;

    private final long maxTimeInRetry;

    public RetryTimeKeeper(int maxTime) {
      this.maxTimeInRetry = maxTime;
    }

    /**
     * wait for {@link PartitionedRegionHelper#DEFAULT_WAIT_PER_RETRY_ITERATION}, updating the total
     * wait time. Use this method when the same node has been selected for consecutive attempts with
     * an operation.
     */
    public void waitToRetryNode() {
      this.waitForBucketsRecovery();
    }

    /**
     * Wait for {@link PartitionedRegionHelper#DEFAULT_WAIT_PER_RETRY_ITERATION} time and update the
     * total wait time.
     */
    public void waitForBucketsRecovery() {
      /*
       * Unfortunately, due to interrupts plus the vagaries of thread scheduling, we can't assume
       * that our sleep is for exactly the amount of time that we specify. Thus, we need to measure
       * the before/after times and increment the counter accordingly.
       */
      long start = System.currentTimeMillis();
      boolean interrupted = Thread.interrupted();
      try {
        Thread.sleep(PartitionedRegionHelper.DEFAULT_WAIT_PER_RETRY_ITERATION);
      } catch (InterruptedException ignore) {
        interrupted = true;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      long delta = System.currentTimeMillis() - start;
      if (delta < 1) {
        // I don't think this can happen, but I want to guarantee that
        // this thing will eventually time out.
        delta = 1;
      }
      this.totalTimeInRetry += delta;
    }

    public boolean overMaximum() {
      return this.totalTimeInRetry > this.maxTimeInRetry;
    }

    public long getRetryTime() {
      return this.totalTimeInRetry;
    }
  }

  public String getBucketName(int bucketId) {
    return PartitionedRegionHelper.getBucketName(getFullPath(), bucketId);
  }

  public static final String BUCKET_NAME_SEPARATOR = "_";

  public boolean isDataStore() {
    return localMaxMemory > 0;
  }

  @Override
  void enableConcurrencyChecks() {
    if (supportsConcurrencyChecks()) {
      this.setConcurrencyChecksEnabled(true);
      assert !isDataStore();
    }
  }

  /**
   * finds all the keys matching the given regular expression (or all keys if regex is ".*")
   *
   * @param regex the regular expression
   * @param allowTombstones whether to include destroyed entries
   * @param collector object that will receive the keys as they arrive
   */
  public void getKeysWithRegEx(String regex, boolean allowTombstones, SetCollector collector)
      throws IOException {
    _getKeysWithInterest(InterestType.REGULAR_EXPRESSION, regex, allowTombstones, collector);
  }

  /**
   * finds all the keys in the given list that are present in the region
   *
   * @param keyList the key list
   * @param allowTombstones whether to return destroyed entries
   * @param collector object that will receive the keys as they arrive
   */
  public void getKeysWithList(List keyList, boolean allowTombstones, SetCollector collector)
      throws IOException {
    _getKeysWithInterest(InterestType.KEY, keyList, allowTombstones, collector);
  }

  /**
   * finds all the keys matching the given interest type and passes them to the given collector
   *
   * @param allowTombstones whether to return destroyed entries
   */
  private void _getKeysWithInterest(int interestType, Object interestArg, boolean allowTombstones,
      SetCollector collector) throws IOException {
    // this could be parallelized by building up a list of buckets for each
    // vm and sending out the requests for keys in parallel. That might dump
    // more onto this vm in one swoop than it could handle, though, so we're
    // keeping it simple for now
    int totalBuckets = getTotalNumberOfBuckets();
    int retryAttempts = calcRetry();
    for (int bucket = 0; bucket < totalBuckets; bucket++) {
      Set bucketSet = null;
      int lbucket = bucket;
      final RetryTimeKeeper retryTime = new RetryTimeKeeper(Integer.MAX_VALUE);
      InternalDistributedMember bucketNode = getOrCreateNodeForBucketRead(lbucket);
      for (int count = 0; count <= retryAttempts; count++) {
        if (logger.isDebugEnabled()) {
          logger.debug("_getKeysWithInterest bucketId={} attempt={}", bucket, (count + 1));
        }
        try {
          if (bucketNode != null) {
            if (bucketNode.equals(getMyId())) {
              bucketSet = this.dataStore.handleRemoteGetKeys(lbucket, interestType, interestArg,
                  allowTombstones);
            } else {
              FetchKeysResponse r = FetchKeysMessage.sendInterestQuery(bucketNode, this, lbucket,
                  interestType, interestArg, allowTombstones);
              bucketSet = r.waitForKeys();
            }
          }
          break;
        } catch (PRLocallyDestroyedException ignore) {
          if (logger.isDebugEnabled()) {
            logger.debug("_getKeysWithInterest: Encountered PRLocallyDestroyedException");
          }
          checkReadiness();
        } catch (ForceReattemptException prce) {
          // no checkKey possible
          if (logger.isDebugEnabled()) {
            logger.debug("_getKeysWithInterest: retry attempt: {}", count, prce);
          }
          checkReadiness();

          InternalDistributedMember lastTarget = bucketNode;
          bucketNode = getOrCreateNodeForBucketRead(lbucket);
          if (lastTarget.equals(bucketNode)) {
            if (retryTime.overMaximum()) {
              break;
            }
            retryTime.waitToRetryNode();
          }
        }
      } // for(count)
      if (bucketSet != null) {
        collector.receiveSet(bucketSet);
      }
    } // for(bucket)
  }

  /**
   * EventIDs are generated for PartitionedRegion.
   */
  @Override
  public boolean generateEventID() {
    return true;
  }

  @Override
  public boolean shouldNotifyBridgeClients() {
    return true;
  }

  /**
   * SetCollector is implemented by classes that want to receive chunked results from queries like
   * getKeysWithRegEx. The implementor creates a method, receiveSet, that consumes the chunks.
   *
   * @since GemFire 5.1
   */
  public interface SetCollector {
    void receiveSet(Set theSet) throws IOException;
  }

  /**
   * Returns the index flag
   *
   * @return true if the partitioned region is indexed else false
   */
  public boolean isIndexed() {
    return this.hasPartitionedIndex;
  }

  /**
   * Returns the map containing all the indexes on this partitioned region.
   *
   * @return Map of all the indexes created.
   */
  public Map getIndex() {
    Hashtable availableIndexes = new Hashtable();
    for (final Object ind : this.indexes.values()) {
      // Check if the returned value is instance of Index (this means
      // the index is not in create phase, its created successfully).
      if (ind instanceof Index) {
        availableIndexes.put(((Index) ind).getName(), ind);
      }
    }
    return availableIndexes;
  }

  /**
   * Returns the a PartitionedIndex on this partitioned region.
   *
   */
  public PartitionedIndex getIndex(String indexName) {
    Iterator iter = this.indexes.values().iterator();
    while (iter.hasNext()) {
      Object ind = iter.next();
      // Check if the returned value is instance of Index (this means
      // the index is not in create phase, its created successfully).
      if (ind instanceof PartitionedIndex && ((Index) ind).getName().equals(indexName)) {
        return (PartitionedIndex) ind;
      }
    }
    return null;
  }

  /**
   * Gets a collecion of all the indexes created on this pr.
   *
   * @return collection of all the indexes
   */
  public Collection getIndexes() {
    if (this.indexes.isEmpty()) {
      return Collections.emptyList();
    }

    ArrayList idxs = new ArrayList();
    Iterator it = this.indexes.values().iterator();
    while (it.hasNext()) {
      Object ind = it.next();
      // Check if the returned value is instance of Index (this means
      // the index is not in create phase, its created successfully).
      if (ind instanceof Index) {
        idxs.add(ind);
      }
    }
    return idxs;
  }

  /**
   * Creates the actual index on this partitioned regions.
   *
   * @param remotelyOriginated true if the index is created because of a remote index creation call
   * @param indexType the type of index created.
   * @param indexName the name for the index to be created
   * @param indexedExpression expression for index creation.
   * @param fromClause the from clause for index creation
   * @param imports class to be imported for fromClause.
   *
   * @return Index an index created on this region.
   * @throws ForceReattemptException indicating the operation failed to create a remote index
   * @throws IndexCreationException if the index is not created properly
   * @throws IndexNameConflictException if an index exists with this name on this region
   * @throws IndexExistsException if and index already exists with the same properties as the one
   *         created
   */
  public Index createIndex(boolean remotelyOriginated, IndexType indexType, String indexName,
      String indexedExpression, String fromClause, String imports, boolean loadEntries)
      throws ForceReattemptException, IndexCreationException, IndexNameConflictException,
      IndexExistsException {
    return createIndex(remotelyOriginated, indexType, indexName, indexedExpression, fromClause,
        imports, loadEntries, true);
  }

  public Index createIndex(boolean remotelyOriginated, IndexType indexType, String indexName,
      String indexedExpression, String fromClause, String imports, boolean loadEntries,
      boolean sendMessage) throws ForceReattemptException, IndexCreationException,
      IndexNameConflictException, IndexExistsException {
    // Check if its remote request and this vm is an accessor.
    if (remotelyOriginated && dataStore == null) {
      // This check makes sure that for some region this vm cannot create
      // data store where as it should have.
      if (getLocalMaxMemory() != 0) {
        throw new IndexCreationException(
            String.format(
                "Data Store on this vm is null and the local max Memory is not zero, the data policy is %s and the localMaxMemeory is : %s",
                getDataPolicy(), (long) getLocalMaxMemory()));
      }
      // Not have to do anything since the region is just an Accessor and
      // does not store any data.
      logger.info("This is an accessor vm and doesnt contain data");
      return null;
    }

    // Create indexManager.
    if (this.indexManager == null) {
      this.indexManager = IndexUtils.getIndexManager(cache, this, true);
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Started creating index with Index Name :{} On PartitionedRegion {}, Indexfrom caluse={}, Remote Request: {}",
          indexName, this.getFullPath(), fromClause, remotelyOriginated);
    }
    IndexTask indexTask = new IndexTask(remotelyOriginated, indexType, indexName, indexedExpression,
        fromClause, imports, loadEntries);

    FutureTask<Index> indexFutureTask = new FutureTask<Index>(indexTask);

    // This will return either the Index FutureTask or Index itself, based
    // on whether the index creation is in process or completed.
    Object ind = this.indexes.putIfAbsent(indexTask, indexFutureTask);

    // Check if its instance of Index, in that the case throw index exists exception.
    if (ind instanceof Index) {
      if (remotelyOriginated) {
        return (Index) ind;
      }

      throw new IndexNameConflictException(
          String.format("Index named ' %s ' already exists.", indexName));
    }

    FutureTask<Index> oldIndexFutureTask = (FutureTask<Index>) ind;
    Index index = null;
    boolean interrupted = false;

    try {
      if (oldIndexFutureTask == null) {
        // Index doesn't exist, create index.
        indexFutureTask.run();
        index = indexFutureTask.get();
        if (index != null) {
          this.indexes.put(indexTask, index);
          PartitionedIndex prIndex = (PartitionedIndex) index;
          indexManager.addIndex(indexName, index);

          // Locally originated create index request.
          // Send create request to other PR nodes.
          if (!remotelyOriginated && sendMessage) {
            logger.info(
                "Created index locally, sending index creation message to all members, and will be waiting for response {}.",
                prIndex);
            HashSet<IndexCreationData> singleIndexDefinition = new HashSet<IndexCreationData>();
            IndexCreationData icd = new IndexCreationData(indexName);
            icd.setIndexData(indexType, fromClause, indexedExpression, imports, loadEntries);
            singleIndexDefinition.add(icd);

            IndexCreationMsg.IndexCreationResponse response = null;
            try {
              response = (IndexCreationMsg.IndexCreationResponse) IndexCreationMsg.send(null,
                  PartitionedRegion.this, singleIndexDefinition);
              if (response != null) {
                IndexCreationMsg.IndexCreationResult result = response.waitForResult();
                Map<String, Integer> indexBucketsMap = result.getIndexBucketsMap();
                if (indexBucketsMap != null && indexBucketsMap.size() > 0) {
                  prIndex.setRemoteBucketesIndexed(indexBucketsMap.values().iterator().next());
                }
              }
            } catch (UnsupportedOperationException ignore) {
              // if remote nodes are of older versions indexes will not be created there, so remove
              // index on this node as well.
              this.indexes.remove(index);
              indexManager.removeIndex(index);
              throw new IndexCreationException(
                  "Indexes should not be created when there are older versions of gemfire in the cluster.");
            }
          }
        }
      } else {
        // Some other thread is trying to create the same index.
        // Wait for index to be initialized from other thread.
        index = oldIndexFutureTask.get();
        // The Index is successfully created, throw appropriate error message from this thread.

        if (remotelyOriginated) {
          return index;
        }

        throw new IndexNameConflictException(
            String.format("Index named ' %s ' already exists.",
                indexName));
      }
    } catch (InterruptedException ignore) {
      interrupted = true;
    } catch (ExecutionException ee) {
      if (!remotelyOriginated) {
        Throwable cause = ee.getCause();
        if (cause instanceof IndexNameConflictException) {
          throw (IndexNameConflictException) cause;
        } else if (cause instanceof IndexExistsException) {
          throw (IndexExistsException) cause;
        }
        throw new IndexInvalidException(ee);
      }
    } finally {
      // If the index is not successfully created, remove IndexTask from the map.
      if (index == null) {
        ind = this.indexes.get(indexTask);
        if (ind != null && !(ind instanceof Index)) {
          this.indexes.remove(indexTask);
        }
      }

      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug(
          "Completed creating index with Index Name :{} On PartitionedRegion {}, Remote Request: {}",
          indexName, this.getFullPath(), remotelyOriginated);
    }
    return index;
  }

  public List<Index> createIndexes(boolean remotelyOriginated,
      HashSet<IndexCreationData> indexDefinitions) throws MultiIndexCreationException,
      CacheException, ForceReattemptException, IndexCreationException {
    if (remotelyOriginated && dataStore == null) {
      // This check makes sure that for some region this vm cannot create
      // data store where as it should have.
      if (getLocalMaxMemory() != 0) {
        throw new IndexCreationException(
            String.format(
                "Data Store on this vm is null and the local max Memory is not zero, the data policy is %s and the localMaxMemeory is : %s",
                getDataPolicy(), (long) getLocalMaxMemory()));
      }
      // Not have to do anything since the region is just an Accessor and
      // does not store any data.
      logger
          .info("This is an accessor vm and doesnt contain data");
      return new ArrayList<Index>();
    }

    Set<Index> indexes = new HashSet<Index>();
    boolean throwException = false;
    HashMap<String, Exception> exceptionsMap = new HashMap<String, Exception>();

    // First step is creating all the defined indexes.
    // Do not send the IndexCreationMsg to remote nodes now.
    throwException |=
        createEmptyIndexes(indexDefinitions, remotelyOriginated, indexes, exceptionsMap);

    // If same indexes are created locally and also being created by a remote index creation msg
    // populate such indexes only once.
    Set<Index> unpopulatedIndexes = null;

    if (indexes.size() > 0) {
      unpopulatedIndexes = getUnpopulatedIndexes(indexes);
    }

    // Second step is iterating over REs and populating all the created indexes
    if (unpopulatedIndexes != null && unpopulatedIndexes.size() > 0) {
      throwException |= populateEmptyIndexes(unpopulatedIndexes, exceptionsMap);
    }

    // Third step is to send the message to remote nodes
    // Locally originated create index request.
    // Send create request to other PR nodes.
    throwException |=
        sendCreateIndexesMessage(remotelyOriginated, indexDefinitions, indexes, exceptionsMap);

    // If exception is throw in any of the above steps
    if (throwException) {
      throw new MultiIndexCreationException(exceptionsMap);
    }

    // set the populate flag for all the created PR indexes
    for (Index ind : indexes) {
      ((AbstractIndex) ind).setPopulated(true);
    }

    return new ArrayList<Index>(indexes);
  }

  private boolean createEmptyIndexes(HashSet<IndexCreationData> indexDefinitions,
      boolean remotelyOriginated, Set<Index> indexes, HashMap<String, Exception> exceptionsMap) {
    boolean throwException = false;
    for (IndexCreationData icd : indexDefinitions) {
      try {
        Index ind = this.createIndex(remotelyOriginated, icd.getIndexType(), icd.getIndexName(),
            icd.getIndexExpression(), icd.getIndexFromClause(), icd.getIndexImportString(), false,
            false);
        // There could be nulls in the set if a node is accessor.
        // The index might have been created by the local node.
        if (ind != null) {
          indexes.add(ind);
        }
      } catch (Exception ex) {
        // If an index creation fails, add the exception to the map and
        // continue creating rest of the indexes.The failed indexes will
        // be removed from the IndexManager#indexes map by the createIndex
        // method so that those indexes will not be populated in the next
        // step.
        if (logger.isDebugEnabled()) {
          logger.debug("Creation failed for index: {}, {}", icd.getIndexName(), ex.getMessage(),
              ex);
        }
        exceptionsMap.put(icd.getIndexName(), ex);
        throwException = true;
      }
    }
    return throwException;
  }

  private Set<Index> getUnpopulatedIndexes(Set<Index> indexSet) {
    synchronized (indexLock) {
      HashSet<Index> unpopulatedIndexes = new HashSet<Index>();
      for (Index ind : indexSet) {
        PartitionedIndex prIndex = (PartitionedIndex) ind;
        if (!prIndex.isPopulateInProgress()) {
          prIndex.setPopulateInProgress(true);
          unpopulatedIndexes.add(prIndex);
        }
      }
      return unpopulatedIndexes;
    }
  }

  private boolean populateEmptyIndexes(Set<Index> indexes,
      HashMap<String, Exception> exceptionsMap) {
    boolean throwException = false;
    if (getDataStore() != null && indexes.size() > 0) {
      Set localBuckets = getDataStore().getAllLocalBuckets();
      Iterator it = localBuckets.iterator();
      while (it.hasNext()) {
        Map.Entry entry = (Map.Entry) it.next();
        Region bucket = (Region) entry.getValue();

        if (bucket == null) {
          continue;
        }
        IndexManager bucketIndexManager = IndexUtils.getIndexManager(cache, bucket, true);
        Set<Index> bucketIndexes = getBucketIndexesForPRIndexes(bucket, indexes);
        try {
          bucketIndexManager.populateIndexes(bucketIndexes);
        } catch (MultiIndexCreationException ex) {
          exceptionsMap.putAll(ex.getExceptionsMap());
          throwException = true;
        }
      }
    }
    return throwException;
  }

  private Set<Index> getBucketIndexesForPRIndexes(Region bucket, Set<Index> indexes) {
    Set<Index> bucketIndexes = new HashSet<Index>();
    for (Index ind : indexes) {
      bucketIndexes.addAll(((PartitionedIndex) ind).getBucketIndexes(bucket));
    }
    return bucketIndexes;
  }

  private boolean sendCreateIndexesMessage(boolean remotelyOriginated,
      HashSet<IndexCreationData> indexDefinitions, Set<Index> indexes,
      HashMap<String, Exception> exceptionsMap) throws CacheException, ForceReattemptException {
    boolean throwException = false;
    if (!remotelyOriginated) {
      logger.info(
          "Created index locally, sending index creation message to all members, and will be waiting for response.");
      IndexCreationMsg.IndexCreationResponse response;
      try {
        response = (IndexCreationMsg.IndexCreationResponse) IndexCreationMsg.send(null, this,
            indexDefinitions);

        if (response != null) {
          IndexCreationMsg.IndexCreationResult result = response.waitForResult();
          Map<String, Integer> remoteIndexBucketsMap = result.getIndexBucketsMap();
          // set the number of remote buckets indexed for each pr index
          if (remoteIndexBucketsMap != null) {
            for (Index ind : indexes) {
              if (remoteIndexBucketsMap.containsKey(ind.getName())) {
                ((PartitionedIndex) ind)
                    .setRemoteBucketesIndexed(remoteIndexBucketsMap.get(ind.getName()));
              }
            }
          }
        }
      } catch (UnsupportedOperationException ignore) {
        // if remote nodes are of older versions indexes will not be created
        // there, so remove index on this node as well.
        for (Index ind : indexes) {
          exceptionsMap.put(ind.getName(),
              new IndexCreationException(
                  "Indexes should not be created when there are older versions of gemfire in the cluster."));
          this.indexes.remove(ind);
          indexManager.removeIndex(ind);
        }
        throwException = true;
      }
    }
    return throwException;
  }

  /**
   * Explicitly sends an index creation message to a newly added node to the system on prs.
   *
   * @param idM id on the newly added node.
   */
  public void sendIndexCreationMsg(InternalDistributedMember idM) {
    if (!this.isIndexed()) {
      return;
    }

    RegionAdvisor advisor = (RegionAdvisor) getCacheDistributionAdvisor();
    final Set<InternalDistributedMember> recipients = advisor.adviseDataStore();
    if (!recipients.contains(idM)) {
      logger.info(
          "Newly added member to the PR is an accessor and will not receive index information : {}",
          idM);
      return;
    }
    // this should add the member to a synchronized set and then sent this member
    // and index creation msg latter after its completed creating the partitioned region.
    IndexCreationMsg.IndexCreationResponse response;
    IndexCreationMsg.IndexCreationResult result;

    if (this.indexes.isEmpty()) {
      return;
    }

    Iterator it = this.indexes.values().iterator();
    HashSet<IndexCreationData> indexDefinitions = new HashSet<>();
    Set<PartitionedIndex> indexes = new HashSet<>();
    while (it.hasNext()) {
      Object ind = it.next();
      // Check if the returned value is instance of Index (this means
      // the index is not in create phase, its created successfully).
      if (!(ind instanceof Index)) {
        continue;
      }
      PartitionedIndex prIndex = (PartitionedIndex) ind;
      indexes.add(prIndex);
      IndexCreationData icd = new IndexCreationData(prIndex.getName());
      icd.setIndexData(prIndex.getType(), prIndex.getFromClause(), prIndex.getIndexedExpression(),
          prIndex.getImports(), true);
      indexDefinitions.add(icd);
    }

    response =
        (IndexCreationMsg.IndexCreationResponse) IndexCreationMsg.send(idM, this, indexDefinitions);

    if (logger.isDebugEnabled()) {
      logger.debug("Sending explicitly index creation message to : {}", idM);
    }

    if (response != null) {
      try {
        result = response.waitForResult();
        Map<String, Integer> remoteIndexBucketsMap = result.getIndexBucketsMap();
        // set the number of remote buckets indexed for each pr index
        for (Index ind : indexes) {
          ((PartitionedIndex) ind)
              .setRemoteBucketesIndexed(remoteIndexBucketsMap.get(ind.getName()));
        }
      } catch (ForceReattemptException e) {
        logger.info(String.format("ForceReattempt exception : %s", e));
      }
    }
  }

  /**
   * Removes all the indexes on this partitioned regions instance and send remove index message
   */
  public int removeIndexes(boolean remotelyOriginated)
      throws CacheException, ForceReattemptException {
    int numBuckets = 0;

    if (!this.hasPartitionedIndex || this.indexes.isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug("This partitioned regions does not have any index : {}", this);
      }
      return numBuckets;
    }

    this.hasPartitionedIndex = false;

    logger.info("Removing all the indexes on this paritition region {}",
        this);

    try {
      for (Object bucketEntryObject : dataStore.getAllLocalBuckets()) {
        LocalRegion bucket = null;
        Map.Entry bucketEntry = (Map.Entry) bucketEntryObject;
        bucket = (LocalRegion) bucketEntry.getValue();
        if (bucket != null) {
          bucket.waitForData();
          IndexManager indexMang = IndexUtils.getIndexManager(cache, bucket, false);
          if (indexMang != null) {
            indexMang.removeIndexes();
            numBuckets++;
            if (logger.isDebugEnabled()) {
              logger.debug("Removed all the indexes on bucket {}", bucket);
            }
          }
        }
      } // ends while
      if (logger.isDebugEnabled()) {
        logger.debug("Removed this many indexes on the buckets : {}", numBuckets);
      }
      RemoveIndexesMessage.RemoveIndexesResponse response;

      if (!remotelyOriginated) {
        logger.info("Sending removeIndex message to all the participating prs.");

        response = (RemoveIndexesMessage.RemoveIndexesResponse) RemoveIndexesMessage.send(this,
            null, true);

        if (null != response) {
          response.waitForResults();
          logger.info("Done waiting for index removal");
          if (logger.isDebugEnabled()) {
            logger.debug(
                "Total number of buckets which removed indexes , locally : {} and remotely removed : {} and the total number of remote buckets : {}",
                numBuckets, response.getRemoteRemovedIndexes(), response.getTotalRemoteBuckets());
          }
        }
      }
      this.indexManager.removeIndexes();
      return numBuckets;

    } // outer try block
    finally {
      this.indexes.clear();
    }
  }

  /**
   * Removes a particular index on this partitioned regions instance.
   *
   * @param ind Index to be removed.
   *
   */
  public int removeIndex(Index ind, boolean remotelyOriginated)
      throws CacheException, ForceReattemptException {

    int numBuckets = 0;
    IndexTask indexTask = null;
    Object prIndex = null;

    if (ind != null) {
      indexTask = new IndexTask(ind.getName());
      prIndex = this.indexes.get(indexTask);
    }

    // Check if the returned value is instance of Index (this means the index is
    // not in create phase, its created successfully).
    if (!(prIndex instanceof Index)) {
      logger.info("This index {} is not on this partitoned region :  {}",
          ind, this);
      return numBuckets;
    }

    if (logger.isDebugEnabled()) {
      logger.debug(
          "Remove index called, IndexName: {} Index: {}  Will be removing all the bucket indexes.",
          ind.getName(), ind);
    }

    Index index1 = this.indexManager.getIndex(ind.getName());
    if (index1 != null) {
      this.indexManager.removeIndex(index1);
    }

    // After removing from region wait for removing from index manager and
    // marking the index invalid.
    PartitionedIndex index = (PartitionedIndex) prIndex;
    index.acquireIndexWriteLockForRemove();

    this.indexes.remove(indexTask);

    // For releasing the write lock after removal.
    try {
      synchronized (prIndex) {
        List allBucketIndex = ((PartitionedIndex) prIndex).getBucketIndexes();
        Iterator it = allBucketIndex.iterator();

        if (logger.isDebugEnabled()) {
          logger.debug("Will be removing indexes on : {} buckets", allBucketIndex.size());
        }

        while (it.hasNext()) {
          Index in = (Index) it.next();
          LocalRegion region = ((LocalRegion) in.getRegion());
          region.waitForData();
          IndexManager indMng = region.getIndexManager();
          indMng.removeIndex(in);

          if (logger.isDebugEnabled()) {
            logger.debug("Removed index : {} on bucket {}", in, region);
          }
          numBuckets++;
          ((PartitionedIndex) prIndex).removeFromBucketIndexes(region, in);
        } // while
      }
    } finally {
      ((PartitionedIndex) prIndex).releaseIndexWriteLockForRemove();
    }

    if (!remotelyOriginated) {
      // send remove index message.
      RemoveIndexesMessage.RemoveIndexesResponse response;
      logger.info("Sending removeIndex message to all the participating prs.");
      response =
          (RemoveIndexesMessage.RemoveIndexesResponse) RemoveIndexesMessage.send(this, ind, false);

      if (response != null) {
        response.waitForResults();
        logger.info("Done waiting for index removal");
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Total number of buckets which removed indexs , locally : {} and remotely removed : {} and the total number of remote buckets : {}",
              numBuckets, response.getRemoteRemovedIndexes(), response.getTotalRemoteBuckets());
        }
      }
    }
    return numBuckets;
  }

  /**
   * Gets and removes index by name.
   *
   * @param indexName name of the index to be removed.
   */
  public int removeIndex(String indexName) throws CacheException, ForceReattemptException {
    int numBuckets = 0;
    // remotely originated removeindex
    Object ind = this.indexes.get(indexName);

    // Check if the returned value is instance of Index (this means the index is
    // not in create phase, its created successfully).
    if (ind instanceof Index) {
      numBuckets = removeIndex((Index) this.indexes.get(indexName), true);
    }
    return numBuckets;
  }

  @Override
  public Object getValueInVM(Object key) throws EntryNotFoundException {
    if (this.dataStore == null) {
      throw new EntryNotFoundException(key.toString());
    }
    final int bucketId = PartitionedRegionHelper.getHashKey(this, null, key, null, null);
    return this.dataStore.getLocalValueInVM(key, bucketId);
  }

  /**
   * This method is intended for testing purposes only.
   */
  @Override
  public Object getValueOnDisk(Object key) throws EntryNotFoundException {
    final int bucketId = PartitionedRegionHelper.getHashKey(this, null, key, null, null);
    if (this.dataStore == null) {
      throw new EntryNotFoundException(key.toString());
    }
    return this.dataStore.getLocalValueOnDisk(key, bucketId);
  }

  /**
   * This method is intended for testing purposes only.
   */
  @Override
  public Object getValueOnDiskOrBuffer(Object key) throws EntryNotFoundException {
    final int bucketId = PartitionedRegionHelper.getHashKey(this, null, key, null, null);
    if (this.dataStore == null) {
      throw new EntryNotFoundException(key.toString());
    }
    return this.dataStore.getLocalValueOnDiskOrBuffer(key, bucketId);
  }

  /**
   * Test Method: Fetch the given bucket's meta-data from each member hosting buckets
   *
   * @param bucketId the identity of the bucket
   * @return list of arrays, each array element containing a {@link DistributedMember} and a
   *         {@link Boolean} the boolean denotes if the member is hosting the bucket and believes it
   *         is the primary
   * @throws ForceReattemptException if the caller should reattempt this request
   */
  public List<Object[]> getBucketOwnersForValidation(int bucketId) throws ForceReattemptException {
    // bucketid 1 => "vm A", false | "vm B", false | "vm C", true | "vm D", false
    // bucketid 2 => List< Tuple(MemberId mem, Boolean isPrimary) >

    // remotely fetch each VM's bucket meta-data (versus looking at the bucket
    // advisor's data
    RuntimeException rte = null;
    List<Object[]> remoteInfos = new LinkedList<>();
    for (int i = 0; i < 3; i++) {
      rte = null;
      DumpB2NResponse response =
          DumpB2NRegion.send(getRegionAdvisor().adviseDataStore(), this, bucketId, true);
      try {
        remoteInfos.addAll(response.waitForPrimaryInfos());
        break;
      } catch (TimeoutException e) {
        rte = e;
        logger.info("DumpB2NRegion failed to get PR {}, bucket id {}'s info due to {}, retrying...",
            this.getFullPath(), bucketId, e.getMessage());
      }
    }
    if (rte != null) {
      logger.info("DumpB2NRegion retried 3 times", rte);
      throw rte;
    }

    // Include current VM in the status...
    if (getRegionAdvisor().getBucket(bucketId).isHosting()) {
      if (getRegionAdvisor().isPrimaryForBucket(bucketId)) {
        remoteInfos.add(new Object[] {getSystem().getDM().getId(), Boolean.TRUE, ""});
      } else {
        remoteInfos.add(new Object[] {getSystem().getDM().getId(), Boolean.FALSE, ""});
      }
    }
    return remoteInfos;
  }

  /**
   * Return the primary for the local bucket. Returns null if no primary can be found within
   * {@link DistributionConfig#getMemberTimeout}.
   *
   * @return the primary bucket member
   */
  public InternalDistributedMember getBucketPrimary(int bucketId) {
    return getRegionAdvisor().getPrimaryMemberForBucket(bucketId);
  }

  /**
   * Wait until the bucket meta-data has been built and is ready to receive messages and/or updates
   */
  public void waitOnBucketMetadataInitialization() {
    waitOnInitialization(this.initializationLatchAfterBucketIntialization);
  }

  private void releaseAfterBucketMetadataSetupLatch() {
    this.initializationLatchAfterBucketIntialization.countDown();
  }

  @Override
  void releaseLatches() {
    super.releaseLatches();
    releaseAfterBucketMetadataSetupLatch();
  }

  /**
   * get the total retry interval, in milliseconds, for operations concerning this partitioned
   * region
   *
   * @return millisecond retry timeout interval
   */
  public int getRetryTimeout() {
    return this.retryTimeout;
  }

  public long getBirthTime() {
    return birthTime;
  }

  public PartitionResolver getPartitionResolver() {
    return this.partitionAttributes.getPartitionResolver();
  }

  public String getColocatedWith() {
    return this.partitionAttributes.getColocatedWith();
  }

  /**
   * Used to get membership events from our advisor to implement RegionMembershipListener
   * invocations. This is copied almost in whole from DistributedRegion
   *
   * @since GemFire 5.7
   */
  protected class AdvisorListener implements MembershipListener {
    protected synchronized void initRMLWrappers() {
      if (PartitionedRegion.this.isInitialized() && hasListener()) {
        initPostCreateRegionMembershipListeners(getRegionAdvisor().adviseAllPRNodes());
      }
    }

    @Override
    public synchronized void memberJoined(DistributionManager distributionManager,
        InternalDistributedMember id) {
      // bug #44684 - this notification has been moved to a point AFTER the
      // other member has finished initializing its region

      // required-roles functionality is not implemented for partitioned regions,
      // or it would be done here
    }

    @Override
    public void quorumLost(DistributionManager distributionManager,
        Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}

    @Override
    public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
        InternalDistributedMember whoSuspected, String reason) {}

    @Override
    public synchronized void memberDeparted(DistributionManager distributionManager,
        InternalDistributedMember id, boolean crashed) {
      if (PartitionedRegion.this.isInitialized() && hasListener()) {
        RegionEventImpl event =
            new RegionEventImpl(PartitionedRegion.this, Operation.REGION_CLOSE, null, true, id);
        if (crashed) {
          dispatchListenerEvent(EnumListenerEvent.AFTER_REMOTE_REGION_CRASH, event);
        } else {
          // TODO: it would be nice to know if what actual op was done
          // could be close, local destroy, or destroy (or load snap?)
          if (DestroyRegionOperation.isRegionDepartureNotificationOk()) {
            dispatchListenerEvent(EnumListenerEvent.AFTER_REMOTE_REGION_DEPARTURE, event);
          }
        }
      }
      // required-roles functionality is not implemented for partitioned regions,
      // or it would be done here
    }
  }

  @Override
  RegionEntry basicGetTXEntry(KeyInfo keyInfo) {
    int bucketId = keyInfo.getBucketId();
    if (bucketId == KeyInfo.UNKNOWN_BUCKET) {
      bucketId = PartitionedRegionHelper.getHashKey(this, null, keyInfo.getKey(),
          keyInfo.getValue(), keyInfo.getCallbackArg());
      keyInfo.setBucketId(bucketId);
    }
    if (keyInfo.isCheckPrimary()) {
      DistributedMember primary = getRegionAdvisor().getPrimaryMemberForBucket(bucketId);
      if (!primary.equals(getMyId())) {
        throw new PrimaryBucketException(
            "Bucket " + bucketId + " is not primary. Current primary holder is " + primary);
      }
    }
    BucketRegion br = this.dataStore.getLocalBucketById(bucketId);
    RegionEntry re = br.basicGetEntry(keyInfo.getKey());
    if (re != null && re.isRemoved()) {
      re = null;
    }
    return re;
  }

  @Override
  boolean usesDiskStore(RegionAttributes regionAttributes) {
    if (regionAttributes.getPartitionAttributes().getLocalMaxMemory() <= 0) {
      return false; // see bug 42055
    }
    return super.usesDiskStore(regionAttributes);
  }

  @Override
  DiskStoreImpl findDiskStore(RegionAttributes regionAttributes,
      InternalRegionArguments internalRegionArgs) {
    DiskStoreImpl store = super.findDiskStore(regionAttributes, internalRegionArgs);
    if (store != null && store.getOwnedByRegion()) {
      store.initializeIfNeeded();
    }
    return store;
  }

  @Override
  DiskRegion createDiskRegion(InternalRegionArguments internalRegionArgs)
      throws DiskAccessException {
    if (internalRegionArgs.getDiskRegion() != null) {
      return internalRegionArgs.getDiskRegion();
    } else {
      return null;
    }
  }

  @Override
  public void handleInterestEvent(InterestRegistrationEvent event) {
    if (logger.isDebugEnabled()) {
      logger.debug("PartitionedRegion {} handling {}", getFullPath(), event);
    }
    // Process event in remote data stores by sending message
    Set<InternalDistributedMember> allRemoteStores = getRegionAdvisor().adviseDataStore(true);
    if (logger.isDebugEnabled()) {
      logger.debug("PartitionedRegion {} sending InterestEvent message to:{}", getFullPath(),
          allRemoteStores);
    }
    InterestEventResponse response = null;
    if (!allRemoteStores.isEmpty()) {
      try {
        response = InterestEventMessage.send(allRemoteStores, this, event);
      } catch (ForceReattemptException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("PartitionedRegion {} caught exception", getFullPath(), e);
        }
      }
    }

    // Process event in the local data store if necessary
    if (this.dataStore != null) {
      // Handle the interest event in the local data store
      this.dataStore.handleInterestEvent(event);
    }

    // Wait for replies
    if (response != null) {
      try {
        if (logger.isDebugEnabled()) {
          logger.debug("PartitionedRegion {} waiting for response from {}", getFullPath(),
              allRemoteStores);
        }
        response.waitForResponse();
      } catch (ForceReattemptException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("PartitionedRegion {} caught exception", getFullPath(), e);
        }
      }
    }
  }

  @Override
  public AttributesMutator getAttributesMutator() {
    checkReadiness();
    return this;
  }

  /**
   * Changes the timeToLive expiration attributes for the partitioned region as a whole
   *
   * @param timeToLive the expiration attributes for the region timeToLive
   * @return the previous value of region timeToLive
   * @throws IllegalArgumentException if timeToLive is null or if the ExpirationAction is
   *         LOCAL_INVALIDATE and the region is {@link DataPolicy#withReplication replicated}
   * @throws IllegalStateException if statistics are disabled for this region.
   */
  @Override
  public ExpirationAttributes setRegionTimeToLive(ExpirationAttributes timeToLive) {
    ExpirationAttributes attr = super.setRegionTimeToLive(timeToLive);
    // Set to Bucket regions as well
    if (this.getDataStore() != null) { // not for accessors
      for (Object o : this.getDataStore().getAllLocalBuckets()) {
        Map.Entry entry = (Map.Entry) o;
        Region bucketRegion = (Region) entry.getValue();
        bucketRegion.getAttributesMutator().setRegionTimeToLive(timeToLive);
      }
    }
    return attr;
  }

  /**
   * Changes the idleTimeout expiration attributes for the region as a whole. Resets the
   * {@link CacheStatistics#getLastAccessedTime} for the region.
   *
   * @param idleTimeout the ExpirationAttributes for this region idleTimeout
   * @return the previous value of region idleTimeout
   * @throws IllegalArgumentException if idleTimeout is null or if the ExpirationAction is
   *         LOCAL_INVALIDATE and the region is {@link DataPolicy#withReplication replicated}
   * @throws IllegalStateException if statistics are disabled for this region.
   */
  @Override
  public ExpirationAttributes setRegionIdleTimeout(ExpirationAttributes idleTimeout) {
    ExpirationAttributes attr = super.setRegionIdleTimeout(idleTimeout);
    // Set to Bucket regions as well
    if (this.getDataStore() != null) { // not for accessors
      for (Object o : this.getDataStore().getAllLocalBuckets()) {
        Map.Entry entry = (Map.Entry) o;
        Region bucketRegion = (Region) entry.getValue();
        bucketRegion.getAttributesMutator().setRegionIdleTimeout(idleTimeout);
      }
    }
    return attr;
  }

  /**
   * Changes the timeToLive expiration attributes for values in this region.
   *
   * @param timeToLive the timeToLive expiration attributes for entries
   * @return the previous value of entry timeToLive
   * @throws IllegalArgumentException if timeToLive is null or if the ExpirationAction is
   *         LOCAL_DESTROY and the region is {@link DataPolicy#withReplication replicated} or if the
   *         ExpirationAction is LOCAL_INVALIDATE and the region is
   *         {@link DataPolicy#withReplication replicated}
   * @throws IllegalStateException if statistics are disabled for this region.
   */
  @Override
  public ExpirationAttributes setEntryTimeToLive(ExpirationAttributes timeToLive) {
    ExpirationAttributes attr = super.setEntryTimeToLive(timeToLive);

    /*
     * All buckets must be created to make this change, otherwise it is possible for
     * updatePRConfig(...) to make changes that cause bucket creation to live lock
     */
    PartitionRegionHelper.assignBucketsToPartitions(this);
    if (dataStore != null) {
      dataStore.lockBucketCreationAndVisit(
          (bucketId, r) -> r.getAttributesMutator().setEntryTimeToLive(timeToLive));
    }
    updatePartitionRegionConfig(config -> config.setEntryTimeToLive(timeToLive));
    return attr;
  }

  /**
   * Changes the custom timeToLive for values in this region
   *
   * @param custom the new CustomExpiry
   * @return the old CustomExpiry
   */
  @Override
  public CustomExpiry setCustomEntryTimeToLive(CustomExpiry custom) {
    CustomExpiry expiry = super.setCustomEntryTimeToLive(custom);
    // Set to Bucket regions as well
    if (dataStore != null) {
      dataStore.lockBucketCreationAndVisit(
          (bucketId, r) -> r.getAttributesMutator().setCustomEntryTimeToLive(custom));
    }
    return expiry;
  }

  /**
   * Changes the idleTimeout expiration attributes for values in the region.
   *
   * @param idleTimeout the idleTimeout expiration attributes for entries
   * @return the previous value of entry idleTimeout
   * @throws IllegalArgumentException if idleTimeout is null or if the ExpirationAction is
   *         LOCAL_DESTROY and the region is {@link DataPolicy#withReplication replicated} or if the
   *         the ExpirationAction is LOCAL_INVALIDATE and the region is
   *         {@link DataPolicy#withReplication replicated}
   * @see AttributesFactory#setStatisticsEnabled
   * @throws IllegalStateException if statistics are disabled for this region.
   */
  @Override
  public ExpirationAttributes setEntryIdleTimeout(ExpirationAttributes idleTimeout) {
    ExpirationAttributes attr = super.setEntryIdleTimeout(idleTimeout);
    /*
     * All buckets must be created to make this change, otherwise it is possible for
     * updatePRConfig(...) to make changes that cause bucket creation to live lock
     */
    PartitionRegionHelper.assignBucketsToPartitions(this);
    // Set to Bucket regions as well
    if (dataStore != null) {
      dataStore.lockBucketCreationAndVisit(
          (bucketId, r) -> r.getAttributesMutator().setEntryIdleTimeout(idleTimeout));
    }
    updatePartitionRegionConfig(config -> config.setEntryIdleTimeout(idleTimeout));
    return attr;
  }

  /**
   * Changes the CustomExpiry for idleTimeout for values in the region
   *
   * @param custom the new CustomExpiry
   * @return the old CustomExpiry
   */
  @Override
  public CustomExpiry setCustomEntryIdleTimeout(CustomExpiry custom) {
    CustomExpiry expiry = super.setCustomEntryIdleTimeout(custom);
    // Set to Bucket regions as well
    if (dataStore != null) {
      dataStore.lockBucketCreationAndVisit(
          (bucketId, r) -> r.getAttributesMutator().setCustomEntryIdleTimeout(custom));
    }
    return expiry;
  }

  @Override
  void setMemoryThresholdFlag(MemoryEvent event) {
    if (event.getState().isCritical() && !event.getPreviousState().isCritical()
        && (event.getType() == ResourceType.HEAP_MEMORY
            || (event.getType() == ResourceType.OFFHEAP_MEMORY && getOffHeap()))) {
      // update proxy bucket, so that we can reject operations on those buckets.
      getRegionAdvisor().markBucketsOnMember(event.getMember(), true/* sick */);
    } else if (!event.getState().isCritical() && event.getPreviousState().isCritical()
        && (event.getType() == ResourceType.HEAP_MEMORY
            || (event.getType() == ResourceType.OFFHEAP_MEMORY && getOffHeap()))) {
      getRegionAdvisor().markBucketsOnMember(event.getMember(), false/* not sick */);
    }
  }

  @Override
  void initialCriticalMembers(boolean localMemoryIsCritical,
      Set<InternalDistributedMember> criticalMembers) {
    for (InternalDistributedMember idm : criticalMembers) {
      getRegionAdvisor().markBucketsOnMember(idm, true/* sick */);
    }
  }

  public DiskRegionStats getDiskRegionStats() {
    return diskRegionStats;
  }

  @Override
  public void removeCriticalMember(DistributedMember member) {
    if (logger.isDebugEnabled()) {
      logger.debug("PR: removing member {} from critical member list", member);
    }
    getRegionAdvisor().markBucketsOnMember(member, false/* sick */);
  }

  public PartitionedRegion getColocatedWithRegion() {
    return this.colocatedWithRegion;
  }

  private final AtomicBoolean bucketSorterStarted = new AtomicBoolean(false);

  private final AtomicBoolean bucketSortedOnce = new AtomicBoolean(false);

  public List<BucketRegion> getSortedBuckets() {
    if (!bucketSorterStarted.get()) {
      bucketSorterStarted.set(true);
      this.bucketSorter.scheduleAtFixedRate(new BucketSorterRunnable(), 0,
          HeapEvictor.BUCKET_SORTING_INTERVAL, TimeUnit.MILLISECONDS);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Started BucketSorter to sort the buckets according to number of entries in each bucket for every {} milliseconds",
            HeapEvictor.BUCKET_SORTING_INTERVAL);
      }
    }
    if (!bucketSortedOnce.get()) {
      while (bucketSortedOnce.get() == false);
    }
    List<BucketRegion> bucketList = new ArrayList<>(this.sortedBuckets);
    return bucketList;
  }

  class BucketSorterRunnable implements Runnable {
    @Override
    public void run() {
      try {
        List<BucketRegion> bucketList = new ArrayList<BucketRegion>();
        Set<BucketRegion> buckets = dataStore.getAllLocalBucketRegions();
        for (BucketRegion br : buckets) {
          if (HeapEvictor.MINIMUM_ENTRIES_PER_BUCKET < br.getSizeForEviction()) {
            bucketList.add(br);
          }
        }
        if (!bucketList.isEmpty()) {
          Collections.sort(bucketList, new Comparator<BucketRegion>() {
            @Override
            public int compare(BucketRegion buk1, BucketRegion buk2) {
              long buk1NumEntries = buk1.getSizeForEviction();
              long buk2NumEntries = buk2.getSizeForEviction();
              if (buk1NumEntries > buk2NumEntries) {
                return -1;
              } else if (buk1NumEntries < buk2NumEntries) {
                return 1;
              }
              return 0;
            }
          });
        }
        sortedBuckets = bucketList;
        if (!bucketSortedOnce.get()) {
          bucketSortedOnce.set(true);
        }
      } catch (Exception e) {
        if (logger.isDebugEnabled()) {
          logger.debug("BucketSorterRunnable : encountered Exception ", e);
        }
      }
    }
  }

  @Override
  public LocalRegion getDataRegionForRead(final KeyInfo keyInfo) {
    final Object entryKey = keyInfo.getKey();
    BucketRegion br;
    try {
      PartitionedRegionDataStore ds = getDataStore();
      if (ds == null) {
        throw new TransactionException(
            "PartitionedRegion Transactions cannot execute on nodes with local max memory zero");
      }
      // TODO provide appropriate Operation and arg
      int bucketId = keyInfo.getBucketId();
      if (bucketId == KeyInfo.UNKNOWN_BUCKET) {
        bucketId = PartitionedRegionHelper.getHashKey(this, null, entryKey, keyInfo.getValue(),
            keyInfo.getCallbackArg());
        keyInfo.setBucketId(bucketId);
      }
      br = ds.getInitializedBucketWithKnownPrimaryForId(null, bucketId);
      if (keyInfo.isCheckPrimary()) {
        try {
          br.checkForPrimary();
        } catch (PrimaryBucketException pbe) {
          throw new TransactionDataRebalancedException(DATA_MOVED_BY_REBALANCE, pbe);
        }
      }
    } catch (RegionDestroyedException ignore) {
      // TODO: why is this purposely not wrapping the original cause?
      throw new TransactionDataNotColocatedException(
          String.format("Key %s is not colocated with transaction",
              entryKey));
    } catch (ForceReattemptException ignore) {
      br = null;
    }
    return br;
  }

  @Override
  public LocalRegion getDataRegionForWrite(KeyInfo keyInfo) {
    BucketRegion br = null;
    final Object entryKey = keyInfo.getKey();
    try {
      final int retryAttempts = calcRetry();
      // TODO provide appropriate Operation and arg
      int bucketId = keyInfo.getBucketId();
      if (bucketId == KeyInfo.UNKNOWN_BUCKET) {
        bucketId = PartitionedRegionHelper.getHashKey(this, null, entryKey, keyInfo.getValue(),
            keyInfo.getCallbackArg());
        keyInfo.setBucketId(bucketId);
      }
      int count = 0;
      while (count <= retryAttempts) {
        try {
          PartitionedRegionDataStore ds = getDataStore();
          if (ds == null) {
            throw new TransactionException(
                "PartitionedRegion Transactions cannot execute on nodes with local max memory zero");
          }
          br = ds.getInitializedBucketWithKnownPrimaryForId(entryKey, bucketId);
          break;
        } catch (ForceReattemptException ignore) {
          // create a new bucket
          InternalDistributedMember member = createBucket(bucketId, 0, null);
          if (!getMyId().equals(member) && keyInfo.isCheckPrimary()) {
            throw new PrimaryBucketException(
                "Bucket " + bucketId + " is not primary. Current primary holder is " + member);
          }
          count++;
        }
      }
      Assert.assertNotNull(br, "Could not create storage for Entry");
      if (keyInfo.isCheckPrimary()) {
        br.checkForPrimary();
      }
    } catch (PrimaryBucketException | RegionDestroyedException exception) {
      throw new TransactionDataRebalancedException(DATA_MOVED_BY_REBALANCE, exception);
    }
    return br;
  }

  @Override
  public DistributedMember getOwnerForKey(KeyInfo key) {
    if (key == null) {
      return super.getOwnerForKey(null);
    }
    // TODO provide appropriate Operation and arg
    int bucketId = key.getBucketId();
    if (bucketId == KeyInfo.UNKNOWN_BUCKET) {
      bucketId = PartitionedRegionHelper.getHashKey(this, null, key.getKey(), key.getValue(),
          key.getCallbackArg());
      key.setBucketId(bucketId);
    }
    return createBucket(bucketId, 0, null);
  }

  @Override
  public KeyInfo getKeyInfo(Object key) {
    return getKeyInfo(key, null);
  }

  @Override
  public KeyInfo getKeyInfo(Object key, Object callbackArg) {
    return getKeyInfo(key, null, callbackArg);
  }

  @Override
  public KeyInfo getKeyInfo(Object key, Object value, Object callbackArg) {
    final int bucketId;
    if (key == null) {
      // key is null for putAll
      bucketId = KeyInfo.UNKNOWN_BUCKET;
    } else {
      bucketId = PartitionedRegionHelper.getHashKey(this, null, key, value, callbackArg);
    }
    return new KeyInfo(key, callbackArg, bucketId);
  }

  public static class SizeEntry implements Serializable {
    private final int size;
    private final boolean isPrimary;

    public SizeEntry(int size, boolean isPrimary) {
      this.size = size;
      this.isPrimary = isPrimary;
    }

    public int getSize() {
      return size;
    }

    public boolean isPrimary() {
      return isPrimary;
    }

    @Override
    public String toString() {
      return "SizeEntry(" + size + ", primary=" + isPrimary + ")";
    }
  }

  /**
   * Index Task used to create the index. This is used along with the FutureTask to take care of,
   * same index creation request from multiple threads. At any time only one thread succeeds and
   * other threads waits for the completion of the index creation. This avoids usage of
   * synchronization which could block any index creation.
   */
  public class IndexTask implements Callable<Index> {

    public String indexName;

    public boolean remotelyOriginated;

    private IndexType indexType;

    private String indexedExpression;

    private String fromClause;

    public String imports;

    public boolean loadEntries;

    IndexTask(boolean remotelyOriginated, IndexType indexType, String indexName,
        String indexedExpression, String fromClaus, String imports, boolean loadEntries) {
      this.indexName = indexName;
      this.remotelyOriginated = remotelyOriginated;
      this.indexType = indexType;
      this.indexName = indexName;
      this.indexedExpression = indexedExpression;
      this.fromClause = fromClaus;
      // this.p_list = p_list;
      this.imports = imports;
      this.loadEntries = loadEntries;

    }

    IndexTask(String indexName) {
      this.indexName = indexName;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof IndexTask)) {
        return false;
      }
      IndexTask otherIndexTask = (IndexTask) other;
      if (this.indexName.equals(otherIndexTask.indexName)) {
        return true;
      }
      return false;
    }

    @Override
    public int hashCode() {
      return this.indexName.hashCode();
    }

    /**
     * This starts creating the index.
     */
    @Override
    public PartitionedIndex call() throws IndexCreationException, IndexNameConflictException,
        IndexExistsException, ForceReattemptException {
      PartitionedIndex prIndex;

      if (dataStore != null) {
        prIndex = createIndexOnPRBuckets();
      } else {
        if (getLocalMaxMemory() != 0) {
          throw new IndexCreationException(
              String.format("Data Store on this vm is null and the local max Memory is not zero %s",
                  (long) getLocalMaxMemory()));
        }
        logger.info("This is an accessor vm and doesnt contain data");

        prIndex = new PartitionedIndex(cache, indexType, indexName, PartitionedRegion.this,
            indexedExpression, fromClause, imports);
      }

      hasPartitionedIndex = true;
      return prIndex;
    }

    /**
     * This creates indexes on PR buckets.
     */
    private PartitionedIndex createIndexOnPRBuckets()
        throws IndexNameConflictException, IndexExistsException, IndexCreationException {

      Set localBuckets = getDataStore().getAllLocalBuckets();
      Iterator it = localBuckets.iterator();
      QCompiler compiler = new QCompiler();
      if (imports != null) {
        compiler.compileImports(imports);
      }

      // imports can be null
      PartitionedIndex parIndex = new PartitionedIndex(cache, indexType, indexName,
          PartitionedRegion.this, indexedExpression, fromClause, imports);

      // In cases where we have no data yet (creation from cache xml), it would leave the populated
      // flag to false Not really an issue as a put will trigger bucket index creation which should
      // set this the flag to true However if the region is empty, we should set this flag to true
      // so it will be reported as used even though there is no data in the region

      if (!it.hasNext()) {
        parIndex.setPopulated(true);
      }
      while (it.hasNext()) {
        Map.Entry entry = (Map.Entry) it.next();
        Region bucket = (Region) entry.getValue();

        if (bucket == null) {
          continue;
        }

        ExecutionContext externalContext = new ExecutionContext(null, cache);
        externalContext.setBucketRegion(PartitionedRegion.this, (BucketRegion) bucket);
        IndexManager indMng = IndexUtils.getIndexManager(cache, bucket, true);
        try {
          Index bucketIndex = indMng.createIndex(indexName, indexType, indexedExpression,
              fromClause, imports, externalContext, parIndex, loadEntries);
          // parIndex.addToBucketIndexes(bucketIndex);
        } catch (IndexNameConflictException ince) {
          if (!remotelyOriginated) {
            throw ince;
          }
        } catch (IndexExistsException iee) {
          if (!remotelyOriginated) {
            throw iee;
          }
        }
      } // End of bucket list
      parIndex.markValid(true);
      return parIndex;
    }
  }

  public List<FixedPartitionAttributesImpl> getFixedPartitionAttributesImpl() {
    return fixedPAttrs;
  }

  public List<FixedPartitionAttributesImpl> getPrimaryFixedPartitionAttributes_TestsOnly() {
    List<FixedPartitionAttributesImpl> primaryFixedPAttrs =
        new LinkedList<FixedPartitionAttributesImpl>();
    if (this.fixedPAttrs != null) {
      for (FixedPartitionAttributesImpl fpa : this.fixedPAttrs) {
        if (fpa.isPrimary()) {
          primaryFixedPAttrs.add(fpa);
        }
      }
    }
    return primaryFixedPAttrs;
  }

  public List<FixedPartitionAttributesImpl> getSecondaryFixedPartitionAttributes_TestsOnly() {
    List<FixedPartitionAttributesImpl> secondaryFixedPAttrs =
        new LinkedList<FixedPartitionAttributesImpl>();
    if (this.fixedPAttrs != null) {
      for (FixedPartitionAttributesImpl fpa : this.fixedPAttrs) {
        if (!fpa.isPrimary()) {
          secondaryFixedPAttrs.add(fpa);
        }
      }
    }
    return secondaryFixedPAttrs;
  }

  /**
   * For the very first member, FPR's first partition has starting bucket id as 0 and other
   * partitions have starting bucket id as the sum of previous partition's starting bucket id and
   * previous partition's num-buckets.
   *
   * For other members, all partitions defined previously with assigned starting bucket ids are
   * fetched from the metadata PartitionRegionConfig. Now for each partition defined for this
   * member, if this partition is already available in the list of the previously defined partitions
   * then starting bucket id is directly assigned from the same previously defined partition.
   *
   * And if the partition on this member is not available in the previously defined partitions then
   * new starting bucket id is calculated as the sum of the largest starting bucket id from
   * previously defined partitions and corresponding num-buckets of the partition.
   *
   * This data of the partitions (FixedPartitionAttributes with starting bucket id for the Fixed
   * Partitioned Region) is stored in metadata for each member.
   */
  private void calculateStartingBucketIDs(PartitionRegionConfig prConfig) {
    if (BEFORE_CALCULATE_STARTING_BUCKET_FLAG) {
      PartitionedRegionObserver pro = PartitionedRegionObserverHolder.getInstance();
      pro.beforeCalculatingStartingBucketId();
    }
    List<FixedPartitionAttributesImpl> fpaList = getFixedPartitionAttributesImpl();

    if (this.getColocatedWith() == null) {
      Set<FixedPartitionAttributesImpl> elderFPAs = prConfig.getElderFPAs();
      int startingBucketID = 0;
      if (elderFPAs != null && !elderFPAs.isEmpty()) {
        int largestStartBucId = -1;
        for (FixedPartitionAttributesImpl fpa : elderFPAs) {
          if (fpa.getStartingBucketID() > largestStartBucId) {
            largestStartBucId = fpa.getStartingBucketID();
            startingBucketID = largestStartBucId + fpa.getNumBuckets();
          }
        }
      }
      for (FixedPartitionAttributesImpl fpaImpl : fpaList) {
        if (elderFPAs != null && elderFPAs.contains(fpaImpl)) {
          for (FixedPartitionAttributesImpl remotefpa : elderFPAs) {
            if (remotefpa.equals(fpaImpl)) {
              fpaImpl.setStartingBucketID(remotefpa.getStartingBucketID());
            }
          }
        } else {
          fpaImpl.setStartingBucketID(startingBucketID);
          startingBucketID += fpaImpl.getNumBuckets();
        }
      }
    }
    prConfig.addFPAs(fpaList);
    for (FixedPartitionAttributesImpl fxPrAttr : fpaList) {
      this.partitionsMap.put(fxPrAttr.getPartitionName(),
          new Integer[] {fxPrAttr.getStartingBucketID(), fxPrAttr.getNumBuckets()});
    }
  }

  /**
   * Returns the local BucketRegion given the key. Returns null if no BucketRegion exists.
   *
   * @since GemFire 6.1.2.9
   */
  public BucketRegion getBucketRegion(Object key) {
    if (this.dataStore == null)
      return null;
    Integer bucketId = PartitionedRegionHelper.getHashKey(this, null, key, null, null);
    return this.dataStore.getLocalBucketById(bucketId);
  }

  /**
   * Returns the local BucketRegion given the key and value. Returns null if no BucketRegion exists.
   */
  public BucketRegion getBucketRegion(Object key, Object value) {
    if (this.dataStore == null) {
      return null;
    }
    final Integer bucketId = PartitionedRegionHelper.getHashKey(this, null, key, value, null);
    return this.dataStore.getLocalBucketById(bucketId);
  }

  /**
   * Test hook to return the per entry overhead for a bucket region. Returns -1 if no buckets exist
   * in this vm.
   *
   * @since GemFire 6.1.2.9
   */
  public int getPerEntryLRUOverhead() {
    if (this.dataStore == null) { // this is an accessor
      return -1;
    }
    try {
      return this.dataStore.getPerEntryLRUOverhead();
    } catch (NoSuchElementException ignore) { // no buckets available
      return -1;
    }
  }

  @Override
  boolean isEntryIdleExpiryPossible() {
    // false always as this is a partitionedRegion,
    // its the BucketRegion that does the expiry
    return false;
  }

  @Override
  public boolean hasSeenEvent(EntryEventImpl ev) {
    // [bruce] PRs don't track events - their buckets do that
    if (this.dataStore == null) {
      return false;
    }
    return this.dataStore.hasSeenEvent(ev);
  }

  public void enableConflation(boolean conflation) {
    this.enableConflation = conflation;
  }

  public boolean isConflationEnabled() {
    return this.enableConflation;
  }

  @Override
  public CachePerfStats getRegionPerfStats() {
    PartitionedRegionDataStore ds = getDataStore();
    CachePerfStats result = null;
    if (ds != null) {
      // If we don't have a data store (we are an accessor)
      // then we do not have per region stats.
      // This is not good. We should still have stats even for accessors.
      result = ds.getCachePerfStats(); // fixes 46692
    }
    return result;
  }

  public void updateEntryVersionInBucket(EntryEventImpl event) {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    final int bucketId = event.getKeyInfo().getBucketId();
    assert bucketId != KeyInfo.UNKNOWN_BUCKET;
    final InternalDistributedMember targetNode = getOrCreateNodeForBucketWrite(bucketId, null);

    final int retryAttempts = calcRetry();
    int count = 0;
    RetryTimeKeeper retryTime = null;
    InternalDistributedMember retryNode = targetNode;
    while (count <= retryAttempts) {
      // It's possible this is a GemFire thread e.g. ServerConnection
      // which got to this point because of a distributed system shutdown or
      // region closure which uses interrupt to break any sleep() or wait()
      // calls
      // e.g. waitForPrimary or waitForBucketRecovery
      checkShutdown();

      if (retryNode == null) {
        checkReadiness();
        if (retryTime == null) {
          retryTime = new RetryTimeKeeper(this.retryTimeout);
        }
        try {
          retryNode = getOrCreateNodeForBucketWrite(bucketId, retryTime);
        } catch (TimeoutException ignore) {
          if (getRegionAdvisor().isStorageAssignedForBucket(bucketId)) {
            // bucket no longer exists
            throw new EntryNotFoundException(
                String.format("Entry not found for key %s",
                    event.getKey()));
          }
          break; // fall out to failed exception
        }

        if (retryNode == null) {
          checkEntryNotFound(event.getKey());
        }
        continue;
      }
      final boolean isLocal = (this.localMaxMemory > 0) && retryNode.equals(getMyId());
      try {
        if (isLocal) {
          this.dataStore.updateEntryVersionLocally(bucketId, event);
        } else {
          updateEntryVersionRemotely(retryNode, bucketId, event);
        }
        return;
      } catch (ConcurrentCacheModificationException e) {
        if (isDebugEnabled) {
          logger.debug("updateEntryVersionInBucket: caught concurrent cache modification exception",
              e);
        }
        event.isConcurrencyConflict(true);

        if (isDebugEnabled) {
          logger.debug(
              "ConcurrentCacheModificationException received for updateEntryVersionInBucket for bucketId: {}{}{} for event: {}  No reattampt is done, returning from here",
              getPRId(), BUCKET_ID_SEPARATOR, bucketId, event);
        }
        return;
      } catch (ForceReattemptException prce) {
        prce.checkKey(event.getKey());
        if (isDebugEnabled) {
          logger.debug("updateEntryVersionInBucket: retry attempt:{} of {}", count, retryAttempts,
              prce);
        }
        checkReadiness();

        InternalDistributedMember lastNode = retryNode;
        retryNode = getOrCreateNodeForBucketWrite(bucketId, retryTime);
        if (lastNode.equals(retryNode)) {
          if (retryTime == null) {
            retryTime = new RetryTimeKeeper(this.retryTimeout);
          }
          if (retryTime.overMaximum()) {
            break;
          }
          retryTime.waitToRetryNode();
        }
      } catch (PrimaryBucketException notPrimary) {
        if (isDebugEnabled) {
          logger.debug("updateEntryVersionInBucket {} on Node {} not primary",
              notPrimary.getLocalizedMessage(), retryNode);
        }
        getRegionAdvisor().notPrimary(bucketId, retryNode);
        retryNode = getOrCreateNodeForBucketWrite(bucketId, retryTime);
      }

      count++;

      if (isDebugEnabled) {
        logger.debug(
            "updateEntryVersionInBucket: Attempting to resend update version to node {} after {} failed attempts",
            retryNode, count);
      }
    } // while

    // No target was found
    PartitionedRegionDistributionException e = new PartitionedRegionDistributionException(
        String.format("No VM available for update-version in %s attempts.",
            count)); // Fix for bug 36014
    if (!isDebugEnabled) {
      logger.warn("No VM available for update-version in {} attempts.",
          count);
    } else {
      logger.warn(e.getMessage(), e);
    }
    throw e;
  }

  /**
   * Updates the entry version timestamp of the remote object with the given key.
   *
   * @param recipient the member id of the recipient of the operation
   * @param bucketId the id of the bucket the key hashed into
   * @throws EntryNotFoundException if the entry does not exist in this region
   * @throws PrimaryBucketException if the bucket on that node is not the primary copy
   * @throws ForceReattemptException if the peer is no longer available
   */
  private void updateEntryVersionRemotely(InternalDistributedMember recipient, Integer bucketId,
      EntryEventImpl event)
      throws EntryNotFoundException, PrimaryBucketException, ForceReattemptException {

    UpdateEntryVersionResponse response = PRUpdateEntryVersionMessage.send(recipient, this, event);
    if (response != null) {
      this.prStats.incPartitionMessagesSent();
      try {
        response.waitForResult();
        return;
      } catch (EntryNotFoundException ex) {
        throw ex;
      } catch (TransactionDataNotColocatedException ex) {
        throw ex;
      } catch (TransactionDataRebalancedException e) {
        throw e;
      } catch (CacheException ce) {
        throw new PartitionedRegionException(
            String.format("Update version of entry on %s failed.",
                recipient),
            ce);
      }
    }
  }

  public void shadowPRWaitForBucketRecovery() {
    assert this.isShadowPR();
    PartitionedRegion userPR = ColocationHelper.getLeaderRegion(this);
    boolean isAccessor = (userPR.getLocalMaxMemory() == 0);
    if (isAccessor)
      return; // return from here if accessor node

    // Before going ahead, make sure all the buckets of shadowPR are
    // loaded
    // and primary nodes have been decided.
    // This is required in case of persistent PR and sender.
    Set<Integer> allBuckets = userPR.getDataStore().getAllLocalBucketIds();
    Set<Integer> allBucketsClone = new HashSet<Integer>(allBuckets);
    while (allBucketsClone.size() != 0) {
      logger.debug(
          "Need to wait until partitionedRegionQueue <<{}>> is loaded with all the buckets",
          this.getName());
      Iterator<Integer> itr = allBucketsClone.iterator();
      while (itr.hasNext()) {
        InternalDistributedMember node = this.getNodeForBucketWrite(itr.next(), null);
        if (node != null) {
          itr.remove();
        }
      }
      // after the iteration is over, sleep for sometime before trying
      // again
      try {
        Thread.sleep(ParallelGatewaySenderQueue.WAIT_CYCLE_SHADOW_BUCKET_LOAD);
      } catch (InterruptedException e) {
        logger.error(e.getMessage(), e);
      }
    }
  }

  @Override
  void preDestroyChecks() {
    try {
      // if there are colocated children throw an exception
      checkForColocatedChildren();
    } catch (CancelException ignore) {
      // If cache is being closed, ignore and go
      // ahead with the destroy
    }
  }

  @Override
  boolean hasStorage() {
    return this.getLocalMaxMemory() != 0;
  }

  @Override
  public EntryExpiryTask getEntryExpiryTask(Object key) {
    BucketRegion br = this.getDataStore().getLocalBucketByKey(key);
    if (br == null) {
      throw new EntryNotFoundException("Bucket for key " + key + " does not exist.");
    }
    return br.getEntryExpiryTask(key);
  }

  void updatePRNodeInformation() {
    updatePartitionRegionConfig(prConfig -> {
      CacheLoader cacheLoader = basicGetLoader();
      CacheWriter cacheWriter = basicGetWriter();
      if (prConfig != null) {

        for (Node node : prConfig.getNodes()) {

          if (node.getMemberId().equals(getMyId())) {
            node.setLoaderAndWriter(cacheLoader, cacheWriter);
          }
        }
      }
    });
  }

  public Logger getLogger() {
    return logger;
  }

  interface PartitionRegionConfigModifier {
    void modify(PartitionRegionConfig prConfig);
  }

  void updatePartitionRegionConfig(
      PartitionRegionConfigModifier partitionRegionConfigModifier) {
    RegionLock rl = getRegionLock();
    rl.lock();
    try {
      if (getPRRoot() == null) {
        if (logger.isDebugEnabled()) {
          logger.debug(PartitionedRegionHelper.PR_ROOT_REGION_NAME + " is null");
        }
        return;
      }
      PartitionRegionConfig prConfig = getPRRoot().get(getRegionIdentifier());
      partitionRegionConfigModifier.modify(prConfig);
      updatePRConfig(prConfig, false);
    } finally {
      rl.unlock();
    }
  }

  @VisibleForTesting
  public SenderIdMonitor getSenderIdMonitor() {
    return senderIdMonitor;
  }

  @Override
  public boolean isRegionCreateNotified() {
    return this.regionCreationNotified;
  }

  @Override
  public void setRegionCreateNotified(boolean notified) {
    this.regionCreationNotified = notified;
  };

  void notifyRegionCreated() {
    if (regionCreationNotified)
      return;
    this.getSystem().handleResourceEvent(ResourceEvent.REGION_CREATE, this);
    this.regionCreationNotified = true;
  }
}
