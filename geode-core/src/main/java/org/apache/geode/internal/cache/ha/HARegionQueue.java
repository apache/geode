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
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.internal.lang.SystemPropertyHelper.HA_REGION_QUEUE_EXPIRY_TIME_PROPERTY;
import static org.apache.geode.internal.lang.SystemPropertyHelper.THREAD_ID_EXPIRY_TIME_PROPERTY;
import static org.apache.geode.internal.lang.SystemPropertyHelper.getProductIntegerProperty;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.query.internal.CqQueryVsdStats;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.InternalCqQuery;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.Conflatable;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientMarkerMessageImpl;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl.CqNameToOp;
import org.apache.geode.internal.cache.tier.sockets.HAEventWrapper;
import org.apache.geode.internal.cache.tier.sockets.Handshake;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.internal.util.concurrent.StoppableCondition;
import org.apache.geode.internal.util.concurrent.StoppableReentrantLock;
import org.apache.geode.internal.util.concurrent.StoppableReentrantReadWriteLock;
import org.apache.geode.internal.util.concurrent.StoppableReentrantReadWriteLock.StoppableWriteLock;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * An implementation of Queue using Gemfire Region as the underlying datastructure. The key will be
 * a counter(long) and value will be the offered Object in the queue. For example, an entry in this
 * region (key = 5; value = obj1) would mean that the Object obj1 is at the 5th position in the
 * queue.
 *
 * This class has a field idsAvailable which is guraded by a ReentrantReadWriteLock. The peek
 * threads which do not modify the idsAvailable LinkedhashSet take read lock , thereby increasing
 * the concurrency of peek operations. The threads like take, remove, QRM ,put& expiry take a write
 * lock while operating on the set. <BR>
 * <B>This class is performant for multiple dispatchers that are trying to do non blocking peek </B>
 * <br>
 * For Blocking operations the object should be of type BlockingHARegionQueue. This class has just a
 * ReentrantLock . Condition object of this lock is used to issue signal to the waiting peek & take
 * threads for put operation. As a result , concurrency of peek operations is not possible. This
 * class is optimized for a single peek thread .
 *
 * 30 May 2008: 5.7 onwards the underlying GemFire Region will continue to have key as counter(long)
 * but the value will be a wrapper object(HAEventWrapper) which will be a key in a separate data
 * structure called haContainer (an implementation of Map). The value against this wrapper will be
 * the offered object in the queue. The purpose of this modification is to allow multiple
 * ha-region-queues share their offered values without storing separate copies in memory, upon GII.
 *
 * (See BlockingHARegionQueue)
 *
 * @since GemFire 4.3
 */
public class HARegionQueue implements RegionQueue {
  private static final Logger logger = LogService.getLogger();

  /** The {@code Region} backing this queue */
  protected HARegion region;

  /**
   * The key into the {@code Region} used when putting entries onto the queue. The counter uses
   * incrementAndGet so counter will always be started from 1
   */
  protected final AtomicLong tailKey = new AtomicLong(0);

  /**
   * Map whose key is the ThreadIdentifier object value is the LastDispatchedAndCurrentEvents
   * object. Every add operation will be identified by the ThreadIdentifier object & the position
   * recorded in the LastDispatchedAndCurrentEvents object.
   */
  protected final ConcurrentMap eventsMap = new ConcurrentHashMap();

  /**
   * The {@code Map} mapping the regionName->key to the queue key. This index allows fast updating
   * of entries in the queue for conflation.
   */
  protected volatile Map indexes = Collections.unmodifiableMap(new HashMap());

  private StoppableReentrantReadWriteLock rwLock;

  private StoppableReentrantReadWriteLock.StoppableReadLock readLock;

  private StoppableWriteLock writeLock;

  /** The name of the {@code Region} backing this queue */
  private String regionName;

  /** The ClientProxyMembershipID associated with the ha queue */
  private ClientProxyMembershipID clientProxyID;

  /**
   * The statistics for this queue
   */
  public HARegionQueueStats stats;

  /**
   * Accesses to this set must be protected via the rwLock.
   */
  protected LinkedHashSet idsAvailable;

  /**
   * Map of HA queue region-name and value as a MapWrapper object (whose underlying map contains
   * ThreadIdentifier as key & value as the last dispatched sequence ID)
   */
  @MakeNotStatic
  static ConcurrentMap dispatchedMessagesMap;

  /**
   * A MapWrapper object whose underlying map contains ThreadIdentifier as key & value as the last
   * dispatched sequence ID
   */
  protected MapWrapper threadIdToSeqId;

  /**
   * A sequence violation can occur , if an HARegionQueue receives events thru GII & the same event
   * also arrives via Gemfire Put in that local VM. If the HARegionQueue does not receive any data
   * via GII , then there should not be any violation. If there is data arriving thru GII, such
   * violations can be expected , but should be analyzed thoroughly.
   */
  protected boolean puttingGIIDataInQueue;

  /**
   * flag indicating whether interest has been registered for this queue. This is used to prevent
   * race conditions between secondaries and primary for interest registration.
   */
  private boolean hasRegisteredInterest;

  /**
   * a thread local to store the counters corresponding to the events peeked by a particular thread.
   * When {@code remove()} will be called, these events stored in thread-local will be destroyed.
   */
  protected static final ThreadLocal peekedEventsContext = new ThreadLocal();

  /**
   * Thread which creates the {@code QueueRemovalMessage} and sends it to other nodes in the system
   */
  @MakeNotStatic
  private static QueueRemovalThread qrmThread;

  /** protects from modification during GII chunking */
  private StoppableReentrantReadWriteLock giiLock;

  /** the number of concurrent GII requests being served */
  private volatile int giiCount;

  /** queue to hold events during GII transfer so we do not modify the queue during chunking */
  private Queue giiQueue = new ConcurrentLinkedQueue();

  /**
   * Constant used to indicate the instance of BlockingHARegionQueue. The static function used for
   * creating the queue instance should be passed this as parameter for creating
   * BlockingHARegionQueue
   */
  public static final int BLOCKING_HA_QUEUE = 1;

  /**
   * Constant used to indicate the instance of HARegionQueue. The static function used for creating
   * the queue instance should be passed this as parameter for creating HARegionQueue
   */
  public static final int NON_BLOCKING_HA_QUEUE = 2;

  public static final String HA_EVICTION_POLICY_NONE = "none";

  public static final String HA_EVICTION_POLICY_MEMORY = "mem";

  public static final String HA_EVICTION_POLICY_ENTRY = "entry";

  public static final long INIT_OF_SEQUENCEID = -1L;

  /**
   * The default frequency (in seconds) at which a message will be sent by the primary to all the
   * secondary nodes to remove the events which have already been dispatched from the queue.
   */
  public static final int DEFAULT_MESSAGE_SYNC_INTERVAL = 1;

  /**
   * The frequency (in seconds) at which a message will be sent by the primary to all the secondary
   * nodes to remove the events which have already been dispatched from the queue.
   */
  @MakeNotStatic
  protected static volatile int messageSyncInterval = DEFAULT_MESSAGE_SYNC_INTERVAL;

  /**
   * The underlying map (may hold reference to a Region or a ConcurrentHashMap) for this
   * HARegionQueue instance (and also shared by all the HARegionQueue instances associated with the
   * same CacheClientNotifier).
   */
  protected Map haContainer;

  /**
   * Boolean to indicate whether this HARegionQueue is having active dispatcher or not( primary node
   * or the secondary node). This will be used to prevent the events from expiry if the node is
   * primary. This was introduced for fixing the bug#36853
   */
  private volatile boolean isPrimary = false;

  /** Boolean to indicate whether destruction of queue is in progress. */
  protected volatile boolean destroyInProgress = false;

  private CancelCriterion stopper;

  /** @since GemFire 5.7 */
  protected byte clientConflation = Handshake.CONFLATION_DEFAULT;

  /**
   * Boolean to indicate whether client is a slow receiver
   *
   * @since GemFire 6.0
   */
  public boolean isClientSlowReceiver = false;

  /**
   * initialization flag - when true the queue has fully initialized
   */
  final AtomicBoolean initialized = new AtomicBoolean();

  /**
   * Test hooks for periodic ack
   *
   * @since GemFire 6.0
   */
  @MutableForTesting
  static boolean testMarkerMessageReceived = false;
  @MutableForTesting
  static boolean isUsedByTest = false;

  /**
   * Used by durable queues to maintain acked events by client
   */
  protected Map ackedEvents;

  /**
   * Indicates how many times this ha queue has hit the max queue size limit.
   */
  protected long maxQueueSizeHitCount = 0;

  /**
   * Processes the given string and returns a string which is allowed for region names
   *
   * @return legal region name
   */
  public static String createRegionName(String regionName) {
    return regionName.replace('/', '#');
  }

  /**
   * @param isPrimary whether this is the primary queue for a client
   */
  protected HARegionQueue(String regionName, InternalCache cache, Map haContainer,
      ClientProxyMembershipID clientProxyId, final byte clientConflation, boolean isPrimary,
      StatisticsClock statisticsClock)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {

    String processedRegionName = createRegionName(regionName);

    // Initialize the statistics
    StatisticsFactory factory = cache.getInternalDistributedSystem().getStatisticsManager();

    AttributesFactory af = new AttributesFactory();
    af.setMirrorType(MirrorType.KEYS_VALUES);
    af.addCacheListener(createCacheListenerForHARegion());
    af.setStatisticsEnabled(true);
    RegionAttributes ra = af.create();

    this.region = HARegion.getInstance(processedRegionName, cache, this, ra, statisticsClock);

    if (this.isPrimary) {// fix for 41878
      // since it's primary queue, we will disable the EntryExpiryTask
      // this should be done after region creation
      disableEntryExpiryTasks();
    }

    this.regionName = processedRegionName;
    this.threadIdToSeqId = new MapWrapper();
    this.idsAvailable = new LinkedHashSet();
    setClientConflation(clientConflation);
    this.isPrimary = isPrimary;
    // Initialize the statistics
    this.stats = new HARegionQueueStats(factory, processedRegionName);
    this.haContainer = haContainer;
    this.giiLock = new StoppableReentrantReadWriteLock(cache.getCancelCriterion());
    this.clientProxyID = clientProxyId;

    this.stopper = this.region.getCancelCriterion();
    this.rwLock = new StoppableReentrantReadWriteLock(region.getCancelCriterion());
    this.readLock = this.rwLock.readLock();
    this.writeLock = this.rwLock.writeLock();

    putGIIDataInRegion();

    if (this.getClass() == HARegionQueue.class) {
      initialized.set(true);
    }
  }

  @VisibleForTesting
  HARegionQueue(String regionName, HARegion haRegion, InternalCache cache, Map haContainer,
      ClientProxyMembershipID clientProxyId, final byte clientConflation, boolean isPrimary,
      HARegionQueueStats stats, StoppableReentrantReadWriteLock giiLock,
      StoppableReentrantReadWriteLock rwLock, CancelCriterion cancelCriterion,
      boolean puttingGIIDataInQueue, StatisticsClock statisticsClock)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {
    this.regionName = regionName;
    this.region = haRegion;
    this.threadIdToSeqId = new MapWrapper();
    this.idsAvailable = new LinkedHashSet();
    setClientConflation(clientConflation);
    this.isPrimary = isPrimary;
    // Initialize the statistics
    this.stats = stats;
    this.haContainer = haContainer;
    this.giiLock = giiLock;
    this.clientProxyID = clientProxyId;

    this.stopper = cancelCriterion;
    this.rwLock = rwLock;
    this.readLock = this.rwLock.readLock();
    this.writeLock = this.rwLock.writeLock();

    // false specifically set in tests only
    if (puttingGIIDataInQueue) {
      putGIIDataInRegion();
    }
    if (this.getClass() == HARegionQueue.class) {
      initialized.set(true);
    }
  }

  /**
   * install DACE information from an initial image provider
   */
  @SuppressWarnings("synthetic-access")
  public void recordEventState(InternalDistributedMember sender, Map eventState) {
    StringBuffer sb = null;
    final boolean isDebugEnabled_BS = logger.isTraceEnabled(LogMarker.BRIDGE_SERVER_VERBOSE);
    if (isDebugEnabled_BS) {
      sb = new StringBuffer(500);
      sb.append("Recording initial event state for ").append(this.regionName).append(" from ")
          .append(sender);
    }
    for (Iterator it = eventState.entrySet().iterator(); it.hasNext();) {
      Map.Entry entry = (Map.Entry) it.next();
      DispatchedAndCurrentEvents giiDace = (DispatchedAndCurrentEvents) entry.getValue();
      if (giiDace != null) { // fix for bug #41789: npe during state transfer
        giiDace.owningQueue = this;
        giiDace.isGIIDace = true;
        if (giiDace.QRM_LOCK == null) {
          // this has happened a number of times with JDK 1.6, even when QRM_LOCK
          // was a "final" field
          giiDace.QRM_LOCK = new Object();
        }
        if (isDebugEnabled_BS) {
          sb.append("\n  ").append(((ThreadIdentifier) entry.getKey()).expensiveToString())
              .append("; sequenceID=").append(giiDace.lastSequenceIDPut);
        }
        // use putIfAbsent to avoid overwriting newer dispatch information
        Object o = this.eventsMap.putIfAbsent(entry.getKey(), giiDace);
        if (o != null && isDebugEnabled_BS) {
          sb.append(" -- could not store.  found ").append(o);
        }
      }
    }
    if (isDebugEnabled_BS) {
      logger.trace(LogMarker.BRIDGE_SERVER_VERBOSE, sb.toString());
    }
  }

  /**
   * Repopulates the HARegion after the GII is over so as to reset the counters and populate the
   * DACE objects for the thread identifiers . This method should be invoked as the last method in
   * the constructor . Thus while creating BlockingQueue this method should be invoked lastly in the
   * derived class constructor , after the HARegionQueue contructor is complete. Otherwise, the
   * ReentrantLock will be null.
   */
  void putGIIDataInRegion() throws CacheException, InterruptedException {
    Set entrySet = this.region.entrySet(false);
    // check if the region is not empty. if there is
    // data, then the relevant data structures have to
    // be populated
    if (!entrySet.isEmpty()) {
      this.puttingGIIDataInQueue = true;
      final boolean isDebugEnabled = logger.isDebugEnabled();
      try {
        Region.Entry entry = null;
        Map orderedMap = new TreeMap();
        Iterator iterator = entrySet.iterator();
        Object key = null;
        while (iterator.hasNext()) {
          entry = (Region.Entry) iterator.next();
          key = entry.getKey();
          if (isDebugEnabled) {
            logger.debug("{} processing queue key {} and value {}", this.regionName, key,
                entry.getValue());
          }
          if (key instanceof Long) {
            if (!(entry.getValue() instanceof ClientMarkerMessageImpl)) {
              orderedMap.put(key, entry.getValue());
            }
          }
          this.region.localDestroy(key);
        }
        long max = 0;
        long counterInRegion = 0;
        entrySet = orderedMap.entrySet();
        if (!entrySet.isEmpty()) {
          Map.Entry mapEntry = null;
          iterator = entrySet.iterator();
          while (iterator.hasNext()) {
            mapEntry = (Map.Entry) iterator.next();
            Conflatable val = (Conflatable) mapEntry.getValue();
            if (val != null && val.getEventId() != null) {
              counterInRegion = ((Long) mapEntry.getKey()).intValue();
              // TODO: remove this assertion
              Assert.assertTrue(counterInRegion > max);
              max = counterInRegion;
              this.put(val);
            } else if (isDebugEnabled) {
              logger.debug(
                  "{} bug 44959 encountered: HARegion.putGIIDataInRegion found null eventId in {}",
                  this.regionName, val);
            }
          }
        }
        this.tailKey.set(max);
      } finally {
        this.puttingGIIDataInQueue = false;
        if (isDebugEnabled) {
          logger.debug("{} done putting GII data into queue", this);
        }
      }
    }
    // TODO:Asif: Avoid invocation of this method
    startHAServices(this.region.getCache());
  }

  /**
   * Puts the GII'd entry into the ha region, if it was GII'd along with its ClientUpdateMessageImpl
   * instance.
   *
   * @since GemFire 5.7
   */
  protected void putInQueue(Object val) throws InterruptedException {
    if (val instanceof HAEventWrapper && ((HAEventWrapper) val).getClientUpdateMessage() == null) {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "{} HARegionQueue.putGIIDataInRegion(): key={} was removed at sender side, so not putting it into the ha queue.",
            this.regionName, ((Conflatable) val).getKeyToConflate());
      }
    } else {
      this.put(val);
    }
  }

  /**
   * Check whether to conflate an event
   *
   * @since GemFire 5.7
   */
  protected boolean shouldBeConflated(Conflatable event) {
    boolean retVal = event.shouldBeConflated();
    // don't apply the client conflation override on durable markers
    if (event instanceof ClientMarkerMessageImpl) {
      return retVal;
    }
    switch (this.clientConflation) {
      case Handshake.CONFLATION_OFF:
        return false; // always disable
      case Handshake.CONFLATION_ON:
        if (event instanceof HAEventWrapper) {
          ClientUpdateMessage cum = (ClientUpdateMessage) this.haContainer.get(event);
          if (cum != null) {
            retVal = cum.isUpdate();
          }
          break;
        }
        if (event instanceof ClientUpdateMessage) {
          // Does this ever happen now?
          retVal = ((ClientUpdateMessage) event).isUpdate();
          break;
        }
        // Oddness
        break;
      case Handshake.CONFLATION_DEFAULT:
        return retVal;
      default:
        throw new InternalGemFireError("Invalid clientConflation");
    }
    return retVal;
  }

  /**
   * Adds an object at the queue's tail. The implementation supports concurrent put operations in a
   * performant manner. This is done in following steps: <br>
   * 1)Check if the Event being added has a sequence ID less than the Last Dispatched SequenceId. If
   * yes do not add the Event to the Queue <br>
   * 2)If no then Do a "put-if-absent" on the eventsMap ,inserting the
   * LastDispatchedAndCurrentEvents object or adding the position to the existing object for the
   * ThreadIdentifier
   *
   * It is possible that DispatchedAnCurrentEvents object just retrieved by the put thread has
   * expired thus a one level recursion can occur to do a valid put
   *
   * The operation is thread safe & is guarded by taking a lock on LastDispatchedAndCurrentEvents
   * object & SIZE Lock
   *
   * @param object object to put onto the queue
   */
  @Override
  public boolean put(Object object) throws CacheException, InterruptedException {
    this.giiLock.readLock().lock(); // fix for bug #41681 - durable client misses event
    try {
      if (this.giiCount > 0) {
        if (logger.isDebugEnabled()) {
          logger.debug("{}: adding message to GII queue of size {}: {}", this.regionName,
              giiQueue.size(), object);
        }
        HAEventWrapper haContainerKey = null;

        if (object instanceof HAEventWrapper) {
          HAEventWrapper wrapper = (HAEventWrapper) object;
          wrapper.incrementPutInProgressCounter("GII queue");
        }

        this.giiQueue.add(object);
      } else {
        if (logger.isTraceEnabled()) {
          logger.trace("{}: adding message to HA queue: {}", this.regionName, object);
        }
        basicPut(object);
      }
    } finally {
      this.giiLock.readLock().unlock();
    }

    // basicPut() invokes dace.putObject() to put onto HARegionQueue
    // However, dace.putObject could return true even though
    // the event is not put onto the HARegionQueue due to eliding events etc.
    // So it is not reliable to be used whether offheap ref ownership is passed over to
    // the queue (if and when HARegionQueue uses offheap). The probable
    // solution could be that to let dace.putObject() to increase offheap REF count
    // when it puts the event onto the region queue. Also always release (dec)
    // the offheap REF count from the caller.
    return true;
  }


  private void basicPut(Object object) throws CacheException, InterruptedException {
    // optmistically decrease the put count
    try {
      this.checkQueueSizeConstraint();
      // this.region.checkReadiness(); // throws CacheClosed or RegionDestroyed
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      this.region.getCache().getCancelCriterion().checkCancelInProgress(ie);
    }

    Conflatable event = (Conflatable) object;
    // Get the EventID object & from it obtain the ThreadIdentifier
    EventID eventId = event.getEventId();
    ThreadIdentifier ti = getThreadIdentifier(eventId);
    long sequenceID = eventId.getSequenceID();
    // Check from Events Map if the put operation should proceed or not
    DispatchedAndCurrentEvents dace = (DispatchedAndCurrentEvents) this.eventsMap.get(ti);
    if (dace != null && dace.isGIIDace && this.puttingGIIDataInQueue) {
      // we only need to retain DACE for which there are no entries in the queue.
      // for other thread identifiers we build up a new DACE
      dace = null;
    }
    if (dace != null) {
      // check the last dispatched sequence Id
      if (this.puttingGIIDataInQueue || (sequenceID > dace.lastDispatchedSequenceId)) {
        // Asif:Insert the Event into the Region with proper locking.
        // It is possible that by the time put operation proceeds , the
        // Last dispatched id has changed so it is possible that the object at
        // this point
        // also does not get added
        if (!dace.putObject(event, sequenceID)) {
          // dace encountered a DESTROYED token - stop adding GII data
          if (!this.puttingGIIDataInQueue) {
            this.put(object);
          }
        } else {
          if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER_VERBOSE)) {
            logger.trace(LogMarker.BRIDGE_SERVER_VERBOSE, "{}: Adding message to queue: {}", this,
                object);
          }
        }

      } else {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "{}: This queue has already seen this event.  The highest sequence number in the queue for {} is {}, but this event's sequence number is {}",
              this.regionName, ti, dace.lastDispatchedSequenceId, sequenceID);
        }
        incrementTakeSidePutPermits();
      }
    } else {
      dace = new DispatchedAndCurrentEvents(this);
      DispatchedAndCurrentEvents oldDace =
          (DispatchedAndCurrentEvents) this.eventsMap.putIfAbsent(ti, dace);
      if (oldDace != null) {
        dace = oldDace;
      } else {
        // Add the recently added ThreadIdentifier to the RegionQueue for expiry
        this.region.put(ti, dace.lastDispatchedSequenceId);
        // update the stats
        this.stats.incThreadIdentifiers();
      }
      if (!dace.putObject(event, sequenceID)) {
        this.put(object);
      } else {
        if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER_VERBOSE)) {
          logger.trace(LogMarker.BRIDGE_SERVER_VERBOSE, "{}: Adding message to queue: {}", this,
              object);
        }
      }
    }
  }

  /**
   * while serving a GII request we queue put()s in a list so that the GII image is not modified.
   * This keeps the chunking iteration from sometimes seeing a later event but missing an older
   * event. Bug #41681
   */
  public void startGiiQueueing() {
    this.giiLock.writeLock().lock();
    this.giiCount++; // TODO: non-atomic operation on volatile!
    if (logger.isDebugEnabled()) {
      logger.debug("{}: startGiiQueueing count is now {}", this.regionName, this.giiCount);
    }
    this.giiLock.writeLock().unlock();
  }

  /**
   * at the end of a GII image request we decrement the in-process count and, if it falls to zero we
   * empty the list of messages that put() has been building. This is done with the lock held to
   * prevent newer events from being queued before the list is drained.
   */
  public void endGiiQueueing() {
    this.giiLock.writeLock().lock();
    final boolean isDebugEnabled = logger.isDebugEnabled();
    try {
      this.giiCount--; // TODO: non-atomic operation on volatile!
      if (isDebugEnabled) {
        logger.debug("{}: endGiiQueueing count is now {}", this.regionName, this.giiCount);
      }
      if (this.giiCount < 0) {
        if (isDebugEnabled) {
          logger.debug("{} found giiCount to be {}", this.regionName, this.giiCount);
        }
        this.giiCount = 0;
      }
      if (this.giiCount == 0) {
        if (isDebugEnabled) {
          logger.debug("{} all GII requests completed - draining {} messages", this.regionName,
              this.giiQueue.size());
        }
        boolean interrupted = false;
        int expectedCount = this.giiQueue.size();
        int actualCount = 0;
        while (true) {
          Object value;
          try {
            value = this.giiQueue.remove();
          } catch (NoSuchElementException ignore) {
            break;
          }
          actualCount++;
          try {
            if (isDebugEnabled) {
              logger.debug("{} draining #{}: {}", this.regionName, (actualCount + 1), value);
            }
            if (value instanceof HAEventWrapper) {
              if (((HAEventWrapper) value).getClientUpdateMessage() == null) {
                if (isDebugEnabled) {
                  logger.debug(
                      "{} ATTENTION: found gii queued event with null event message.  Please see bug #44852: {}",
                      this.regionName, value);
                }
                continue;
              }
            }

            basicPut(value);

            // The HAEventWrapper putInProgressCounter must be decremented because it was
            // incremented when it was queued in giiQueue.
            if (value instanceof HAEventWrapper) {
              ((HAEventWrapper) value).decrementPutInProgressCounter();
            }
          } catch (NoSuchElementException ignore) {
            break;
          } catch (InterruptedException ignore) {
            // complete draining while holding the write-lock so nothing else
            // can get into the queue
            interrupted = true;
          }
        }
        if (interrupted) {
          this.region.getCache().getCancelCriterion()
              .checkCancelInProgress(new InterruptedException());
          Thread.currentThread().interrupt();
        }
      }
    } catch (RuntimeException t) {
      logger.fatal("endGiiQueueing terminating due to uncaught runtime exception", t);
      throw t;
    } catch (Error t) {
      logger.fatal("endGiiQueueing terminating due to uncaught error", t);
      throw t;
    } finally {
      if (logger.isTraceEnabled()) {
        logger.trace("{} endGiiQueueing completed", this.regionName);
      }
      this.giiLock.writeLock().unlock();
    }
  }


  /**
   * this method is for transmission of DACE information with initial image state in HARegions. It
   * should not be used for other purposes. The map contains only those entries that have no items
   * queued. This is used to prevent replay of events in the new queue that have already been
   * removed from this queue.
   */
  public Map getEventMapForGII() {
    // fix for bug #41621 - concurrent modification exception while serializing event map
    final boolean isDebugEnabled = logger.isDebugEnabled();
    do {
      HashMap result = new HashMap();
      try {
        for (Map.Entry<ThreadIdentifier, DispatchedAndCurrentEvents> entry : ((Map<ThreadIdentifier, DispatchedAndCurrentEvents>) this.eventsMap)
            .entrySet()) {
          if (entry.getValue().isCountersEmpty()) {
            result.put(entry.getKey(), entry.getValue());
          }
        }
        return result;
      } catch (ConcurrentModificationException ignore) {
        // TODO:WTF: bad practice but eventsMap is ConcurrentHashMap
        if (isDebugEnabled) {
          logger.debug(
              "HARegion encountered concurrent modification exception while analysing event state - will try again");
        }
      }
    } while (true);
  }

  /**
   * Implementation in BlockingHARegionQueue class
   */
  void checkQueueSizeConstraint() throws InterruptedException {
    if (Thread.interrupted())
      throw new InterruptedException();
  }

  /**
   * Implementation in BlokcingHARegionQueue class
   *
   */
  void incrementTakeSidePutPermitsWithoutNotify() {

  }

  /**
   * Creates the static dispatchedMessagesMap (if not present) and starts the QueuRemovalThread if
   * not running
   */
  static synchronized void startHAServices(InternalCache cache) {
    if (qrmThread == null) {
      dispatchedMessagesMap = new ConcurrentHashMap();
      qrmThread = new QueueRemovalThread(cache);
      qrmThread.setName("Queue Removal Thread");
      qrmThread.start();
    }
  }

  /**
   * Clears the dispatchedMessagesMap and shuts down the QueueRemovalThread.
   *
   */
  public static synchronized void stopHAServices() {
    if (qrmThread != null) {
      qrmThread.shutdown();
      qrmThread = null;
      dispatchedMessagesMap.clear();
      dispatchedMessagesMap = null;

    }
  }

  /**
   * Used for testing purposes only
   *
   * @return the frequency (in seconds) at which a message will be sent by the primary to all the
   *         secondary nodes to remove the events which have already been dispatched from the queue.
   */
  public static int getMessageSyncInterval() {
    return messageSyncInterval;
  }

  /**
   *
   * The internal method used for setting the message synch interval time via the Cache API
   *
   * @param seconds time to wait between two synch messages
   */
  public static void setMessageSyncInterval(int seconds) {
    messageSyncInterval = seconds;
  }

  /**
   * Returns the previous counter if any for the Conflatable object
   *
   * @param event Object to be conflated
   * @param newPosition New Conflatable object's position
   * @return Long object denoting the position of the previous conflatable object
   */
  protected Long addToConflationMap(Conflatable event, Long newPosition) {
    String r = event.getRegionToConflate();
    ConcurrentMap latestIndexesForRegion = (ConcurrentMap) this.indexes.get(r);
    if (latestIndexesForRegion == null) {
      synchronized (HARegionQueue.this) {
        if ((latestIndexesForRegion = (ConcurrentMap) this.indexes.get(r)) == null) {
          latestIndexesForRegion = createConcurrentMap();
          Map newMap = new HashMap(this.indexes);
          newMap.put(r, latestIndexesForRegion);
          this.indexes = Collections.unmodifiableMap(newMap);
        }
      }
    }
    Object key = event.getKeyToConflate();
    return (Long) latestIndexesForRegion.put(key, newPosition);
  }

  /**
   * Creates and returns a ConcurrentMap. This method is over-ridden in test classes to test some
   * functionality
   *
   * @return new ConcurrentMap
   */
  ConcurrentMap createConcurrentMap() {
    return new ConcurrentHashMap();
  }

  /**
   *
   * @return CacheListener object having the desired functionility of expiry of Thread Identifier &
   *         Events for the HAregion. This method is appropriately over ridden in the test classes.
   */
  CacheListener createCacheListenerForHARegion() {
    return new CacheListenerAdapter() {
      @Override
      public void afterInvalidate(EntryEvent event) {
        try {
          // Fix 39175
          // Fix for #48879. As the expiry-tasks are disabled either in
          // constructor or when the queue becomes primary, we no longer need to
          // check if the queue is primary here. It'll allow ThreadIdentifiers
          // to expire.
          // if (!HARegionQueue.this.isPrimary()) {
          HARegionQueue.this.expireTheEventOrThreadIdentifier(event);
          // }
        } catch (CancelException ignore) {
          // ignore, we're done
        } catch (CacheException ce) {
          if (!destroyInProgress) {
            logger.error("HAREgionQueue::createCacheListner::Exception in the expiry thread",
                ce);
          }
        }
      }
    };
  }

  /**
   * This function is invoked by the createCacheListenerForHARegion for theHARegionQueue & also by
   * overridden function createCacheListenerForHARegion of the HARegionQueueJUnitTest class for the
   * test testConcurrentEventExpiryAndTake. This function provides the meaningful functionality for
   * expiry of the Event object as well as ThreadIdentifier
   *
   * @param event event object representing the data being expired
   */
  void expireTheEventOrThreadIdentifier(EntryEvent event) throws CacheException {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug(
          "HARegionQueue::afterInvalidate. Entry Event being invalidated:{}, isPrimaryQueue:{}",
          event, HARegionQueue.this.isPrimary());
    }
    Object key = event.getKey();
    if (key instanceof ThreadIdentifier) {
      // Check if the sequenceID present as value against this key is same
      // as
      // the last dispatched sequence & the size of set containing the
      // counters
      // is 0. If yes the Dace should be removed
      // Get DACE

      DispatchedAndCurrentEvents dace =
          (DispatchedAndCurrentEvents) HARegionQueue.this.eventsMap.get(key);
      Assert.assertTrue(dace != null);
      Long expirySequenceID = (Long) event.getOldValue();
      boolean expired = dace.expireOrUpdate(expirySequenceID, (ThreadIdentifier) key);
      if (isDebugEnabled) {
        logger.debug(
            "HARegionQueue::afterInvalidate:Size of the region after expiring or updating the ThreadIdentifier={}",
            HARegionQueue.this.region.keys().size());
        logger.debug("HARegionQueue::afterInvalidate:ThreadIdentifier expired={}", expired);
      }
    } else if (key instanceof Long) {
      // if (destroyFromAvailableIDsAndRegion((Long)key)) {
      destroyFromQueue(key);
      Conflatable cf = (Conflatable) event.getOldValue();
      EventID id = cf.getEventId();
      byte[] memID = id.getMembershipID();
      long threadId = id.getThreadID();
      DispatchedAndCurrentEvents dace =
          (DispatchedAndCurrentEvents) eventsMap.get(new ThreadIdentifier(memID, threadId));
      if (shouldBeConflated(cf)) {
        dace.destroy((Long) key, cf.getKeyToConflate(), cf.getRegionToConflate());
      } else {
        dace.destroy((Long) key);
      }
      // }
    } else {
      // unexpected condition, throw exception?
    }

  }

  /**
   * This method adds the position of newly added object to the List of available IDs so that it is
   * available for peek or take. This method is called from DispatchedAndCurrentEvents object. This
   * method is invoked in a write lock for non blocking queue & in a reentrant lock in a blocking
   * queue. In case of blocking queue , this method also signals the waiting take & peek threads to
   * awake.
   *
   * @param position The Long position of the object which has been added
   */
  void publish(Long position) throws InterruptedException {
    acquireWriteLock();
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("Adding position " + position + " to available IDs. Region: " + regionName);
      }

      this.idsAvailable.add(position);
      // Notify the waiting peek threads or take threads of blocking queue
      // A void operation for the non blocking queue operations
      notifyPeekAndTakeThreads();
    } finally {
      releaseWriteLock();
    }
  }

  protected boolean removeFromOtherLists(Long position) {
    return false;
  }

  /**
   * @param position Long value present in the Available IDs map against which Event object is
   *        present in HARegion. This function is directly invoked from the basicInvalidate function
   *        where
   *        expiry is aborted if this function returns false
   * @return boolean true if the position could be removed from the Set
   * @throws InterruptedException *
   */
  public boolean destroyFromAvailableIDs(Long position) throws InterruptedException {
    if (logger.isDebugEnabled()) {
      logger.debug("Removing position " + position + " from available IDs. Region: " + regionName);
    }

    boolean removedOK = false;
    acquireWriteLock();
    try {
      removedOK = this.idsAvailable.remove(position);
      if (!removedOK) {
        removedOK = this.removeFromOtherLists(position);
      }
      if (removedOK) {
        this.incrementTakeSidePutPermits();
      }
    } finally {
      releaseWriteLock();
    }
    return removedOK;
  }

  /**
   * Destroys the entry at the position from the Region. It checks for the presence of the position
   * in the AvailableID Set. If the position existed in the Set, then only it is removed from the
   * Set & the underlying Region
   *
   * @param position Long position counter for entry in the Region
   * @return true if the entry with <br>
   *         position <br>
   *         specified was removed from the Set
   */
  protected boolean destroyFromAvailableIDsAndRegion(Long position) throws InterruptedException {
    if (logger.isDebugEnabled()) {
      logger.debug("Removing position " + position + " from available IDs and region. Region: "
          + this.regionName);
    }

    boolean removedOK = this.destroyFromAvailableIDs(position);

    if (removedOK) {
      try {
        this.destroyFromQueue(position);
      } catch (EntryNotFoundException ignore) {
        if (!HARegionQueue.this.destroyInProgress) {
          if (!this.region.isDestroyed()) {
            Assert.assertTrue(false, "HARegionQueue::remove: The position " + position
                + "existed in availableIDs set but not in Region object is not expected");
          }
        }
      }
    }
    return removedOK;
  }

  /*
   * Removes from the local region and decrements stats GII does not call this because it messes
   * with the cq stats Expiry can call this for now as durable client expires would shut down the cq
   * anyways if anything goes wrong
   *
   */
  private void destroyFromQueue(Object key) {
    Object event = this.region.get(key);
    this.region.localDestroy(key);

    maintainCqStats(event, -1);
  }

  /** Returns the {@code toString} for this RegionQueue object */
  @Override
  public String toString() {
    return "RegionQueue on " + this.regionName + "(" + (this.isPrimary ? "primary" : "backup")
        + ")";
  }

  /**
   * Returns the underlying region that backs this queue.
   */
  @Override
  public HARegion getRegion() {
    return this.region;
  }


  /**
   * This method is invoked by the take function . For non blocking queue it returns null or a valid
   * long position while for blocking queue it waits for data in the queue or throws Exception if
   * the thread encounters exception while waiting.
   */
  protected Long getAndRemoveNextAvailableID() throws InterruptedException {
    Long next = null;
    acquireWriteLock();
    try {
      if (this.idsAvailable.isEmpty()) {
        if (waitForData()) {
          Iterator itr = this.idsAvailable.iterator();
          next = (Long) itr.next();
          itr.remove();
          this.incrementTakeSidePutPermits();
        }
      } else {
        Iterator itr = this.idsAvailable.iterator();
        next = (Long) itr.next();
        itr.remove();
        this.incrementTakeSidePutPermits();
      }
    } finally {
      releaseWriteLock();
    }
    return next;
  }

  /**
   * Returns the next position counter present in idsAvailable set. This method is invoked by the
   * peek function. In case of BlockingQueue, this method waits till a valid ID is available.
   *
   * @return valid Long poistion or null depending upon the nature of the queue
   * @throws TimeoutException if operation is interrupted (unfortunately)
   */
  private Long getNextAvailableID() throws InterruptedException {
    Long next = null;
    acquireReadLock();
    try {
      if (this.idsAvailable.isEmpty()) {
        // Asif:Wait in case it is a blocking thread
        if (waitForData()) {
          next = (Long) this.idsAvailable.iterator().next();
        }
      } else {
        next = (Long) this.idsAvailable.iterator().next();
      }
    } finally {
      releaseReadLock();
    }
    return next;
  }

  /**
   * For non blocking queue , this method either returns null or an Object. For blocking queue it
   * will always return with an Object or wait for queue to be populated.
   *
   * @throws CacheException The exception can be thrown by BlockingQueue if it encounters
   *         InterruptedException while waiting for data
   */
  @Override
  public Object take() throws CacheException, InterruptedException {
    Conflatable object = null;
    Long next = null;
    if ((next = this.getAndRemoveNextAvailableID()) != null) {
      object = (Conflatable) this.region.get(next);
      Assert.assertTrue(object != null);

      object = this.getAndRemoveFromHAContainer(object);
      Assert.assertTrue(object != null);
      EventID eventid = object.getEventId();
      long sequenceId = eventid.getSequenceID();
      ThreadIdentifier threadid = getThreadIdentifier(eventid);
      DispatchedAndCurrentEvents dace = (DispatchedAndCurrentEvents) this.eventsMap.get(threadid);
      Assert.assertTrue(dace != null);
      Object keyToConflate = null;
      if (shouldBeConflated(object)) {
        keyToConflate = object.getKeyToConflate();
      }
      dace.removeEventAndSetSequenceID(
          new RemovedEventInfo(next, object.getRegionToConflate(), keyToConflate), sequenceId);

      // Periodic ack from the client will add to the addDispatchMessage Map.
      // This method gets called from cacheClientNotifier upon receiving the ack from client.
      // addDispatchedMessage(threadid, sequenceId);

      // update the stats
      // if (logger.isDebugEnabled()) {
      this.stats.incEventsTaken();
      // }
    }
    // since size is zero, return null
    if (object == null && logger.isDebugEnabled()) {
      logger.debug("RegionQueue is EMPTY, returning null for take()");
    }
    return object;
  }

  @Override
  public List take(int batchSize) throws CacheException, InterruptedException {
    List batch = new ArrayList(batchSize * 2);
    for (int i = 0; i < batchSize; i++) {
      Object obj = take();
      if (obj != null) {
        batch.add(obj);
      } else {
        break;
      }
    }
    if (logger.isTraceEnabled()) {
      logger.trace("{}: Took a batch of {} entries", this, batch.size());
    }
    return batch;
  }

  protected boolean checkPrevAcks() {
    // ARB: Implemented in DurableHARegionQueue.
    return true;
  }

  protected boolean checkEventForRemoval(Long counter, ThreadIdentifier threadid, long sequenceId) {
    return true;
  }

  protected void setPeekedEvents() {
    HARegionQueue.peekedEventsContext.set(null);
  }

  /**
   * Removes the events that were peeked by this thread. The events are destroyed from the queue and
   * conflation map and DispatchedAndCurrentEvents are updated accordingly.
   */
  @Override
  public void remove() throws InterruptedException {
    List peekedIds = (List) HARegionQueue.peekedEventsContext.get();

    if (peekedIds == null) {
      if (logger.isDebugEnabled()) {
        logger.debug("Remove() called before peek(), nothing to remove.");
      }
      return;
    }

    if (!this.checkPrevAcks()) {
      return;
    }

    Map groupedThreadIDs = new HashMap();

    for (Iterator iter = peekedIds.iterator(); iter.hasNext();) {
      Long counter = (Long) iter.next();

      Conflatable event = (Conflatable) this.region.get(counter);
      if (event != null) {
        EventID eventid = event.getEventId();
        long sequenceId = eventid.getSequenceID();
        ThreadIdentifier threadid = getThreadIdentifier(eventid);

        if (!checkEventForRemoval(counter, threadid, sequenceId)) {
          continue;
        }

        Object key = null;
        String r = null;
        if (shouldBeConflated(event)) {
          key = event.getKeyToConflate();
          r = event.getRegionToConflate();
        }
        RemovedEventInfo info = new RemovedEventInfo(counter, r, key);

        List countersList;
        if ((countersList = (List) groupedThreadIDs.get(threadid)) != null) {
          countersList.add(info);
          countersList.set(0, sequenceId);
        } else {
          countersList = new ArrayList();
          countersList.add(sequenceId);
          countersList.add(info);
          groupedThreadIDs.put(threadid, countersList);
        }
        event = null;
        info = null;
      } else {
        // if (logger.isDebugEnabled()) {
        HARegionQueue.this.stats.incNumVoidRemovals();
        // }
      }
    }

    for (Iterator iter = groupedThreadIDs.entrySet().iterator(); iter.hasNext();) {
      Map.Entry element = (Map.Entry) iter.next();
      ThreadIdentifier tid = (ThreadIdentifier) element.getKey();
      List removedEvents = (List) element.getValue();
      long lastDispatchedId = (Long) removedEvents.remove(0);
      DispatchedAndCurrentEvents dace = (DispatchedAndCurrentEvents) this.eventsMap.get(tid);
      if (dace != null && dace.lastDispatchedSequenceId < lastDispatchedId) {
        try {
          dace.setLastDispatchedIDAndRemoveEvents(removedEvents, lastDispatchedId);
        } catch (CacheException e) {
          // ignore and log
          logger.error("Exception occurred while trying to set the last dispatched id",
              e);
        }
      }

      // Periodic ack from the client will add to the addDispatchMessage Map.
      // This method gets called from cacheClientNotifier upon receiving the ack from client.
      // addDispatchedMessage(tid, lastDispatchedId);
    }
    groupedThreadIDs = null;
    // removed the events from queue, now clear the peekedEventsContext

    setPeekedEvents();
  }

  protected Object getNextAvailableIDFromList() throws InterruptedException {
    return this.getNextAvailableID();
  }

  protected void storePeekedID(Long id) {
    // ARB: Implemented in DurableHARegionQueue.
  }

  @Override
  public Object peek() throws InterruptedException {
    if (Thread.interrupted()) {
      throw new InterruptedException();
    }
    Conflatable object = null;
    Long next = null;

    while (true) {
      try {
        next = (Long) this.getNextAvailableIDFromList();
        if (next == null) {
          break;
        }
      } catch (TimeoutException ignore) {
        throw new InterruptedException();
      }

      object = (Conflatable) this.region.get(next);

      // It is possible for the object to be null if a queue removal
      // occurred between getting the next available ID and getting the object
      // from the region. If this happens, on the next iteration of this loop we will
      // get a different available ID to process
      if (object != null) {
        object = (object instanceof HAEventWrapper) ? (Conflatable) this.haContainer.get(object)
            : object;

        if (object != null) {
          List peekedEvents;
          if ((peekedEvents = (List) HARegionQueue.peekedEventsContext.get()) != null) {
            peekedEvents.add(next);
          } else {
            peekedEvents = new LinkedList();
            peekedEvents.add(next);
            HARegionQueue.peekedEventsContext.set(peekedEvents);
          }
          this.storePeekedID(next);
          break;
        }
      }
    }
    // since size is zero, return null
    if (logger.isTraceEnabled()) {
      logger.trace("HARegionQueue::peek: Returning object from head = {}", object);
    }
    return object;
  }

  @Override
  public List peek(int batchSize) throws InterruptedException {
    return peek(batchSize, -1);
  }

  /**
   * Return a batch of minimum specified size
   *
   * @param minSize minimum number to return
   * @return null if minimum was not present
   */
  private List doReturn(int minSize, int maxSize) {
    acquireReadLock();
    try {
      int numToReturn = this.idsAvailable.size();
      if (numToReturn < minSize) {
        return null;
      }
      if (numToReturn > maxSize) {
        numToReturn = maxSize;
      }
      return getBatchAndUpdateThreadContext(numToReturn);
    } finally {
      releaseReadLock();
    }

  }

  /**
   * Peeks either a batchSize number of elements from the queue or until timeToWait milliseconds
   * have elapsed (whichever comes first). If it has peeked batchSize number of elements from the
   * queue before timeToWait milliseconds have elapsed, it stops peeking. If timeToWait milliseconds
   * elapse before batchSize number of elements has been peeked, it stops. All the counters peeked
   * for the batch are added to the thread-context, so that upon calling of remove(), all the peeked
   * events of the batch are removed from the queue.
   *
   * If the Queue is non blocking multiple peek operations can proceed but if it is of type non
   * blocking only one peek operation will proceed as the blocking queue does not use ReadWriteLock
   *
   * @param batchSize The number of objects to peek from the queue
   * @param timeToWait The number of milliseconds to attempt to peek
   *
   * @return The list of events peeked
   */
  @Override
  public List peek(int batchSize, int timeToWait) throws InterruptedException {
    long start = System.currentTimeMillis();
    long end = start + timeToWait;
    if (logger.isTraceEnabled()) {
      logger.trace("{}: Peek start time={} end time={} time to wait={}", this, start, end,
          timeToWait);
    }

    // If we're full, just return the results.
    List result = doReturn(batchSize, batchSize);
    if (result != null) {
      return result;
    }

    // Need to wait for a while.
    for (;;) {
      region.getCache().getCancelCriterion().checkCancelInProgress(null);
      result = doReturn(batchSize, batchSize);
      if (result != null) {
        return result; // full now
      }

      // If time to wait is -1 (don't wait) or time interval has elapsed
      long currentTime = System.currentTimeMillis();
      if (logger.isTraceEnabled()) {
        logger.trace("{}: Peek current time: {}", this, currentTime);
      }
      if (timeToWait == -1 || currentTime >= end) {
        if (logger.isTraceEnabled()) {
          logger.trace("{}: Peek timed out", this);
        }
        // Return whatever's there.
        result = doReturn(0, batchSize);
        Assert.assertTrue(result != null);
        return result;
      }

      // Sleep a bit before trying again.
      if (logger.isTraceEnabled()) {
        logger.trace("{}: Peek continuing", this);
      }
      boolean interrupted = Thread.interrupted();
      try {
        Thread.sleep(50); // TODO this seems kinda busy IMNSHO -- jason
      } catch (InterruptedException ignore) {
        interrupted = true;
        this.region.getCancelCriterion().checkCancelInProgress(null);
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    } // for
  }

  /**
   * This method prepares the batch of events and updates the thread-context with corresponding
   * counters, so that when remove is called by this thread, these events are destroyed from the
   * queue.This method should always be invoked within the {@code rwLock}.
   *
   * @param batchSize - number of events to be peeked
   * @return - list of events peeked
   */
  private List getBatchAndUpdateThreadContext(int batchSize) {
    Iterator itr = this.idsAvailable.iterator();
    int currSize = this.idsAvailable.size();
    int limit = currSize >= batchSize ? batchSize : currSize;
    List batch = new ArrayList(limit);

    List peekedEventsThreadContext;
    if ((peekedEventsThreadContext = (List) HARegionQueue.peekedEventsContext.get()) == null) {
      peekedEventsThreadContext = new LinkedList();
    }
    for (int i = 0; i < limit; i++) {
      Long counter = (Long) itr.next();
      Object eventOrWrapper = this.region.get(counter);
      Object event;
      if (eventOrWrapper instanceof HAEventWrapper) {
        event = haContainer.get(eventOrWrapper);
        if (event == null) {
          event = ((HAEventWrapper) eventOrWrapper).getClientUpdateMessage();
        }
      } else {
        event = eventOrWrapper;
      }
      if (event != null) {
        batch.add(event);
      }
      peekedEventsThreadContext.add(counter);
    }

    HARegionQueue.peekedEventsContext.set(peekedEventsThreadContext);
    return batch;
  }

  @Override
  public void addCacheListener(CacheListener listener) {
    // nothing
  }

  @Override
  public void removeCacheListener() {
    // nothing
  }

  /**
   * It adds the entry to a static data structure dispatchedMessagesMap which is periodically
   * operated upon by the QRM thread.
   *
   * @param tid - the ThreadIdentifier object for this event
   * @param sequenceId - the sequence id for this event
   */
  public void addDispatchedMessage(ThreadIdentifier tid, long sequenceId) {
    Long lastSequenceNumber = sequenceId;
    boolean wasEmpty = false;
    Long oldvalue = null;
    Map internalMap = null;
    while (true) {
      internalMap = this.threadIdToSeqId.map;
      synchronized (internalMap) {
        if (internalMap != this.threadIdToSeqId.map) {
          continue;
        } else {
          wasEmpty = internalMap.isEmpty();
          oldvalue = (Long) internalMap.put(tid, lastSequenceNumber);
          if (ackedEvents != null) {
            ackedEvents.put(tid, lastSequenceNumber);
          }
          if (oldvalue != null && oldvalue.compareTo(lastSequenceNumber) > 0) {
            internalMap.put(tid, oldvalue);
            if (ackedEvents != null) {
              ackedEvents.put(tid, oldvalue);
            }
          }
          break;
        }
      }
    }
    if (wasEmpty) {
      // Asif: If the wasEmpty flag is true this would mean that the
      // dispatchedMessagesMap
      // should not contain the threadIdToSeqID Map as it should have been
      // removed by QRM thread
      // Also because unless the map is actually put by the thread experiencing
      // the
      // wasEmpty flag true, the QRM thread cannot find it & hence cannot toggle
      // it.
      // Thus a condition where the internalMap being put by the dispatcher
      // thread is stale ( that
      // is already replaced by QRM thread ) cannot arise. Once the dispatcher
      // thread
      // has put the Map , then the QRM thread may empty it & replace it with a
      // new Map
      // Assert.assertTrue(!dispatchedMessagesMap.containsKey(this.regionName));
      // dispatchedMessagesMap.put(this.regionName, this.threadIdToSeqId);
      Map tempDispatchedMessagesMap = dispatchedMessagesMap;
      if (tempDispatchedMessagesMap != null) {
        Object old = ((ConcurrentMap) tempDispatchedMessagesMap).putIfAbsent(this.regionName,
            this.threadIdToSeqId);
        if (isUsedByTest) {
          testMarkerMessageReceived = true;
          if (logger.isDebugEnabled()) {
            logger.debug("testIsAckReceived: {}", testMarkerMessageReceived);
          }
        }
        Assert.assertTrue(old == null);
      }

    }
  }

  public void createAckedEventsMap() {

  }

  public void setAckedEvents() {

  }

  /**
   * Used for testing purposes only
   *
   * @return Map object
   */
  public static Map getDispatchedMessagesMapForTesting() {
    return Collections.unmodifiableMap(dispatchedMessagesMap);
  }

  /**
   * Used for testing purposes only
   *
   * @return Map object
   */
  Map getConflationMapForTesting() {
    return Collections.unmodifiableMap(this.indexes);
  }

  public HARegionQueueStats getStatistics() {
    return this.stats;
  }

  /**
   * Used for testing purposes only
   *
   * @return Map object containing DispatchedAndCurrentEvents object for a ThreadIdentifier
   */
  Map getEventsMapForTesting() {
    return Collections.unmodifiableMap(this.eventsMap);
  }



  /**
   * Used for testing purposes only. Returns the set of current counters for the given
   * ThreadIdentifier
   *
   * @param id - the EventID object
   * @return - the current counters set
   */
  Set getCurrentCounterSet(EventID id) {
    Set counters = null;
    ThreadIdentifier tid = getThreadIdentifier(id);
    DispatchedAndCurrentEvents wrapper = (DispatchedAndCurrentEvents) this.eventsMap.get(tid);
    if (wrapper != null) {
      synchronized (wrapper) {
        if (wrapper.isCountersEmpty()) {
          counters = Collections.emptySet();
        } else {
          counters = Collections.unmodifiableSet(wrapper.counters.keySet());
        }
      }
    }
    return counters;
  }

  /**
   * Used for testing purposes only. Returns the last dispatched sequenceId for the given
   * ThreadIdentifier
   *
   * @param id - the EventID object
   * @return - the current counters set
   */
  long getLastDispatchedSequenceId(EventID id) {
    ThreadIdentifier tid = getThreadIdentifier(id);
    DispatchedAndCurrentEvents wrapper = (DispatchedAndCurrentEvents) this.eventsMap.get(tid);
    return wrapper.lastDispatchedSequenceId;
  }

  /**
   * Used for testing purposes only
   *
   */
  Set getAvailableIds() {
    acquireReadLock();
    try {
      return Collections.unmodifiableSet(this.idsAvailable);
    } finally {
      releaseReadLock();
    }
  }

  /**
   * This method is invoked by the QRM to remove the IDs which have already been dispatched by the
   * primary node. It sets the last dispatched sequence ID in the DACE iff the sequenceID received
   * is more than the current. If the renoval emssage arrives before the DACE was created, it
   * creates a DACE. Only one QRM operates at a time on a DACE & any other mesasge will be waiting
   * for the current thread to exit. This is accomplished by taking a lock on QRM_LOCK object in the
   * DACE.
   *
   * @param lastDispatched EventID containing the ThreadIdentifier and the last dispatched sequence
   *        Id
   */
  protected void removeDispatchedEvents(EventID lastDispatched)
      throws CacheException, InterruptedException {
    ThreadIdentifier ti = getThreadIdentifier(lastDispatched);
    long sequenceID = lastDispatched.getSequenceID();
    // get the DispatchedAndCurrentEvents object for this threadID
    DispatchedAndCurrentEvents dace = (DispatchedAndCurrentEvents) this.eventsMap.get(ti);
    if (dace != null && dace.lastDispatchedSequenceId < sequenceID) {
      dace.setLastDispatchedIDAndRemoveEvents(sequenceID);
    } else if (dace == null) {
      dace = new DispatchedAndCurrentEvents(this);
      dace.lastDispatchedSequenceId = sequenceID;
      DispatchedAndCurrentEvents oldDace =
          (DispatchedAndCurrentEvents) this.eventsMap.putIfAbsent(ti, dace);
      if (oldDace != null) {
        dace = oldDace;
        if (dace.lastDispatchedSequenceId < sequenceID) {
          dace.setLastDispatchedIDAndRemoveEvents(sequenceID);
        }
      } else {
        // Add the recently added ThreadIdentifier to the RegionQueue for
        // expiry
        this.region.put(ti, dace.lastDispatchedSequenceId);
        // update the stats
        this.stats.incThreadIdentifiers();
      }
    }
  }

  /**
   * Returns the size of the queue
   *
   * @return the size of the queue
   */
  @Override
  public int size() {
    acquireReadLock();
    try {
      return this.idsAvailable.size();
    } finally {
      releaseReadLock();
    }
  }

  void incrementTakeSidePutPermits() {
    // nothing
  }

  // called from dace on a put.
  // Increment ha queue stats and cq stats
  void entryEnqueued(Conflatable event) {
    stats.incEventsEnqued();
    maintainCqStats(event, 1);
  }

  private void maintainCqStats(Object event, long incrementAmount) {
    CqService cqService = region.getGemFireCache().getCqService();
    if (cqService != null) {
      try {
        if (event instanceof HAEventWrapper) {
          HAEventWrapper hw = (HAEventWrapper) event;
          if (hw.getClientUpdateMessage() != null) {
            event = hw.getClientUpdateMessage();
          } else {
            event = (Conflatable) this.haContainer.get(event);
          }


          if (event instanceof ClientUpdateMessage) {
            if (((ClientUpdateMessage) event).hasCqs()
                && ((ClientUpdateMessage) event).hasCqs(clientProxyID)) {
              CqNameToOp cqNames = ((ClientUpdateMessage) event).getClientCq(clientProxyID);
              if (cqNames != null) {
                for (String cqName : cqNames.getNames()) {
                  InternalCqQuery cq =
                      ((InternalCqQuery) cqService.getClientCqFromServer(clientProxyID, cqName));
                  CqQueryVsdStats cqStats = cq.getVsdStats();
                  if (cq != null && cqStats != null) {
                    cqStats.incNumHAQueuedEvents(incrementAmount);
                  }
                }
              }
            }
          }
        }
      } catch (Exception e) {
        // catch exceptions that arise due to maintaining cq stats
        // as maintaining cq stats should not affect the system.
        if (logger.isTraceEnabled()) {
          logger.trace("Exception while maintaining cq events stats.", e);
        }
      }
    }
  }

  /**
   * Caller must hold the rwLock.
   *
   * @return true if the queue contains objects
   */
  boolean internalIsEmpty() {
    return this.idsAvailable.isEmpty();
  }

  /**
   * test hook to see if the queue is empty
   *
   * @return true if the queue is empty, false if not
   */
  public boolean isEmpty() {
    acquireReadLock();
    try {
      return internalIsEmpty();
    } finally {
      releaseReadLock();
    }
  }

  /**
   * Acquires the write Lock for the non blocking class. This method is overridden in the
   * BlockingHARegionQueue class which acquires the lock on a ReentrantLock instead of
   * ReentrantReadWriteLock of this class. A write lock is aquired by any thread which intends to
   * modify the idsAvailable HashSet , which can be either a put ,remove , take , QRM message or
   * expiry thread
   *
   * All invocations of this method need to have {@link #releaseWriteLock()} in a matching finally
   * block.
   *
   * <p>
   * author Asif
   */
  void acquireWriteLock() {
    this.writeLock.lock();
  }

  /**
   * Acquires the read Lock for the non blocking class. This method is overridden in the
   * BlockingHARegionQueue class which acquires the lock on a ReentrantLock instead of
   * ReentrantReadWriteLock of this class. A read lock is aquired by a non blocking peek while
   * operating on the idsAvailable LinkedHashSet without structurally modifying it.
   *
   * All invocations of this method must have {@link #releaseReadLock()} in a matching finally
   * block.
   *
   * <p>
   * author Asif
   */
  void acquireReadLock() {
    // TODO should this be interruptible?
    this.readLock.lock();
  }

  /**
   * Releases the Read lock. Overridden in the BlockingHARegionQueue class.
   *
   */
  void releaseReadLock() {
    this.readLock.unlock();
  }

  /**
   * Releases the write lock. Overridden in the BlockingHARegionQueue class.
   *
   */
  void releaseWriteLock() {
    this.writeLock.unlock();
  }

  /**
   * A no op. Overridden in the BlockingHARegionQueue class.
   *
   * <p>
   * author Asif
   */
  void notifyPeekAndTakeThreads() {
    // NO Op for blocking queue
  }

  /**
   * Always returns false for a HARegionQueue class. Suitably overridden in BlockingHARegionQueue
   * class.
   *
   * @return false for HAREgionQueue as this is a non blocking class
   */
  boolean waitForData() throws InterruptedException {
    return false;
  }

  /**
   * Utility method which extracts ThreadIdentifier from an EventID object
   *
   * @param eventId EventID object
   * @return ThreadIdentifier object
   */
  protected static ThreadIdentifier getThreadIdentifier(EventID eventId) {
    return new ThreadIdentifier(eventId.getMembershipID(), eventId.getThreadID());
  }

  /**
   * get the qrm thread for testing purposes
   *
   */
  static void stopQRMThread() {
    qrmThread.shutdown();
  }

  /**
   * Calls the createMessageList method of QueueRemovalThread for testing purposes
   *
   * @return message list for testing
   */
  static List createMessageListForTesting() {
    return qrmThread.createMessageList();
  }

  /**
   * Creates a HARegionQueue object with default attributes Used by tests
   *
   * @param regionName uniquely identifies the HARegionQueue in the VM.For HARegionQueues across the
   *        VM to communicate with each other , the name should be identical
   * @param cache Gemfire Cache instance
   * @param haRgnQType int identifying whether the HARegionQueue is of type blocking or non blocking
   * @return an instance of HARegionQueue
   */
  public static HARegionQueue getHARegionQueueInstance(String regionName, InternalCache cache,
      final int haRgnQType, final boolean isDurable, StatisticsClock statisticsClock)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {
    Map container = null;
    if (haRgnQType == HARegionQueue.BLOCKING_HA_QUEUE) {
      container = new HAContainerMap(new ConcurrentHashMap());
    } else {
      // Should actually be HAContainerRegion, but ok if only JUnits using this
      // method.
      container = new HashMap();
    }

    return getHARegionQueueInstance(regionName, cache,
        HARegionQueueAttributes.DEFAULT_HARQ_ATTRIBUTES, haRgnQType, isDurable, container, null,
        Handshake.CONFLATION_DEFAULT, false, Boolean.FALSE, statisticsClock);
  }

  /**
   * Creates a HARegionQueue object with default attributes
   *
   * @param regionName uniquely identifies the HARegionQueue in the VM.For HARegionQueues across the
   *        VM to communicate with each other , the name should be identical
   * @param cache Gemfire Cache instance
   * @param hrqa HARegionQueueAttribute instance used for configuring the HARegionQueue
   * @param haRgnQType int identifying whether the HARegionQueue is of type blocking or non blocking
   * @param isPrimary whether this is the primary queue for the client
   * @param canHandleDelta boolean indicating whether the HARegionQueue can handle delta or not
   * @return an instance of HARegionQueue
   */
  public static HARegionQueue getHARegionQueueInstance(String regionName, InternalCache cache,
      HARegionQueueAttributes hrqa, final int haRgnQType, final boolean isDurable, Map haContainer,
      ClientProxyMembershipID clientProxyId, final byte clientConflation, boolean isPrimary,
      boolean canHandleDelta, StatisticsClock statisticsClock)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {

    HARegionQueue hrq = null;
    switch (haRgnQType) {
      case BLOCKING_HA_QUEUE:
        if (!isDurable && !canHandleDelta) {
          hrq = new BlockingHARegionQueue(regionName, cache, hrqa, haContainer, clientProxyId,
              clientConflation, isPrimary, statisticsClock);
        } else {
          hrq = new DurableHARegionQueue(regionName, cache, hrqa, haContainer, clientProxyId,
              clientConflation, isPrimary, statisticsClock);
        }
        break;
      case NON_BLOCKING_HA_QUEUE:
        hrq = new HARegionQueue(regionName, cache, haContainer, clientProxyId, clientConflation,
            isPrimary, statisticsClock);
        break;
      default:
        throw new IllegalArgumentException(
            String.format("haRgnQType can either be BLOCKING ( %s ) or NON BLOCKING ( %s )",
                new Object[] {BLOCKING_HA_QUEUE, NON_BLOCKING_HA_QUEUE}));
    }
    if (!isDurable) {
      Optional<Integer> expiryTime =
          getProductIntegerProperty(HA_REGION_QUEUE_EXPIRY_TIME_PROPERTY);
      hrqa.setExpiryTime(expiryTime.orElseGet(hrqa::getExpiryTime));
      ExpirationAttributes ea =
          new ExpirationAttributes(hrqa.getExpiryTime(), ExpirationAction.LOCAL_INVALIDATE);
      hrq.region.getAttributesMutator().setEntryTimeToLive(ea);
    }
    return hrq;
  }

  /**
   * Creates a HARegionQueue object with default attributes. used by tests
   *
   * @return an instance of HARegionQueue
   * @since GemFire 5.7
   */
  public static HARegionQueue getHARegionQueueInstance(String regionName, InternalCache cache,
      HARegionQueueAttributes hrqa, final int haRgnQType, final boolean isDurable,
      StatisticsClock statisticsClock)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {
    Map container = null;
    if (haRgnQType == HARegionQueue.BLOCKING_HA_QUEUE) {
      container = new HAContainerMap(new ConcurrentHashMap());
    } else {
      // Should actually be HAContainerRegion, but ok if only JUnits using this
      // method.
      container = new HashMap();
    }

    return getHARegionQueueInstance(regionName, cache, hrqa, haRgnQType, isDurable, container, null,
        Handshake.CONFLATION_DEFAULT, false, Boolean.FALSE, statisticsClock);
  }

  public boolean isEmptyAckList() {
    synchronized (this.threadIdToSeqId.list) {
      return this.threadIdToSeqId.list.isEmpty();
    }
  }

  public void closeClientCq(ClientProxyMembershipID clientId, InternalCqQuery cqToClose) {
    acquireReadLock();
    try {
      // Get all available Ids for the HA Region Queue
      Object[] availableIds = this.availableIDsArray();
      int currSize = availableIds.length;

      Object event = null;
      for (int i = 0; i < currSize; i++) {
        Long counter = (Long) availableIds[i];
        event = this.region.get(counter);
        HAEventWrapper wrapper = null;
        if (event instanceof HAEventWrapper) {
          wrapper = (HAEventWrapper) event;
          event = this.haContainer.get(event);
        }

        // Since this method is invoked in a readlock , the entry in HARegion
        // cannot be null
        if (event == null) {
          Assert.assertTrue(this.destroyInProgress,
              "Got event null when queue was not being destroyed");
        }

        if (event instanceof ClientUpdateMessageImpl) {
          ClientUpdateMessageImpl updateEvent = (ClientUpdateMessageImpl) event;
          updateEvent.removeClientCq(clientId, cqToClose);
          // If no more interest and no more cqs remove from available ids and backing region
          if (!updateEvent.hasCqs(clientId) && !updateEvent.isClientInterested(clientId)) {
            if (wrapper != null) {
              try {
                if (this.destroyFromAvailableIDsAndRegion(counter)) {
                  stats.incEventsRemoved();
                }
              } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
              }
            }
          }
        }
      }
    } finally {
      releaseReadLock();
    }
  }

  public Object updateHAEventWrapper(InternalDistributedMember sender,
      CachedDeserializable newValueCd, String regionName) {
    Object inputValue;
    try {
      inputValue = BlobHelper.deserializeBlob(newValueCd.getSerializedValue(),
          sender.getVersionObject(), null);
      newValueCd = new VMCachedDeserializable(inputValue, newValueCd.getSizeInBytes());
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("Unable to deserialize HA event for region " + regionName);
    }
    if (inputValue instanceof HAEventWrapper) {
      HAEventWrapper inputHaEventWrapper = (HAEventWrapper) inputValue;
      // Key was removed at sender side so not putting it into the HARegion
      if (inputHaEventWrapper.getClientUpdateMessage() == null) {
        return null;
      }
      // Getting the instance from singleton CCN..This assumes only one bridge
      // server in the VM
      HAContainerWrapper haContainer =
          (HAContainerWrapper) CacheClientNotifier.getInstance().getHaContainer();
      if (haContainer == null) {
        return null;
      }
      HAEventWrapper entryHaEventWrapper = null;
      do {
        ClientUpdateMessageImpl entryMessage = (ClientUpdateMessageImpl) haContainer
            .putIfAbsent(inputHaEventWrapper, inputHaEventWrapper.getClientUpdateMessage());
        if (entryMessage != null) {
          entryHaEventWrapper = (HAEventWrapper) haContainer.getKey(inputHaEventWrapper);
          if (entryHaEventWrapper == null) {
            continue;
          }
          synchronized (entryHaEventWrapper) {
            if (entryHaEventWrapper == (HAEventWrapper) haContainer.getKey(entryHaEventWrapper)) {
              entryHaEventWrapper.incAndGetReferenceCount();
              addClientCQsAndInterestList(entryMessage, inputHaEventWrapper, haContainer,
                  regionName);
              inputHaEventWrapper.setClientUpdateMessage(null);
              newValueCd =
                  new VMCachedDeserializable(entryHaEventWrapper, newValueCd.getSizeInBytes());
              if (logger.isDebugEnabled()) {
                logger.debug("GII Update of Event ID hash code: " + entryHaEventWrapper.hashCode()
                    + "; System ID hash code: " + System.identityHashCode(entryHaEventWrapper)
                    + "; Wrapper details: " + entryHaEventWrapper);
              }
            } else {
              entryHaEventWrapper = null;
            }
          }
        } else { // putIfAbsent successful
          synchronized (inputHaEventWrapper) {
            inputHaEventWrapper.incAndGetReferenceCount();
            inputHaEventWrapper.setHAContainer(haContainer);
            inputHaEventWrapper.setClientUpdateMessage(null);
            if (logger.isDebugEnabled()) {
              logger.debug("GII Add of Event ID hash code: " + inputHaEventWrapper.hashCode()
                  + "; System ID hash code: " + System.identityHashCode(inputHaEventWrapper)
                  + "; Wrapper details: " + entryHaEventWrapper);
            }
          }
          break;
        }
        // try until we either get a reference to HAEventWrapper from
        // HAContainer or successfully put one into it.
      } while (entryHaEventWrapper == null);
    }
    return newValueCd;
  }

  /**
   * This is an implementation of RegionQueue where peek() & take () are blocking operation and will
   * not return unless it gets some legitimate value The Lock object used by this class is a
   * ReentrantLock & not a ReadWriteLock as in the base class. This reduces the concurrency of peek
   * operations, but it enables the condition object of the ReentrantLock used to guard the
   * idsAvailable Set for notifying blocking peek & take operations. Previously a separate Lock
   * object was used by the BlockingQueue for wait notify. This class will be performant if there is
   * a single peek thread.
   */
  private static class BlockingHARegionQueue extends HARegionQueue {
    /**
     * Guards the Put permits
     */
    private final Object putGuard = new Object();

    private final int capacity;

    /**
     * Current put permits available
     */
    private int putPermits;

    /**
     * Current take permits available
     */
    private int takeSidePutPermits = 0;

    /**
     * Lock on which the put thread waits for permit & on which take/remove thread issue notify
     */
    private final Object permitMon = new Object();

    // Lock on which the take & remove threads block awaiting data from put
    // operations
    private final StoppableReentrantLock lock;

    /**
     * Condition object on which peek & take threads will block
     */
    protected final StoppableCondition blockCond;

    /**
     * @param hrqa HARegionQueueAttributes through which expiry time etc for the HARegionQueue can
     *        be set
     * @param isPrimary whether this is the primary queue for a client
     */
    protected BlockingHARegionQueue(String regionName, InternalCache cache,
        HARegionQueueAttributes hrqa, Map haContainer, ClientProxyMembershipID clientProxyId,
        final byte clientConflation, boolean isPrimary, StatisticsClock statisticsClock)
        throws IOException, ClassNotFoundException, CacheException, InterruptedException {
      super(regionName, cache, haContainer, clientProxyId, clientConflation, isPrimary,
          statisticsClock);
      this.capacity = hrqa.getBlockingQueueCapacity();
      this.putPermits = this.capacity;
      this.lock = new StoppableReentrantLock(this.region.getCancelCriterion());
      this.blockCond = lock.newCondition();

      super.putGIIDataInRegion();
      if (this.getClass() == BlockingHARegionQueue.class) {
        initialized.set(true);
      }
    }

    @Override
    public void destroy() throws CacheWriterException {
      try {
        super.destroy();
      } finally {
        synchronized (this.permitMon) {
          this.permitMon.notifyAll(); // fix for bug 37581
        }
      }
    }

    /**
     * Checks whether a put operation should block or proceed based on the capacity constraint of
     * the Queue. Initially it checks only via Put Side Put Permits. If it is exhausted it checks
     * the take side put permits using the appropriate lock. If the queue's capacity is exhausted
     * then the thread goes into a finite wait state holding the Put Guard lock for the duration of
     * EVENT_ENQUEUE_WAIT_TIME after which the thread proceeds to enqueue the event, allowing the
     * putPermits go negative, if needed. Thus, no put thread is blocked for more than
     * EVENT_ENQUEUE_WAIT_TIME.
     * <p>
     * This effectively makes the blocking queue behave like a non-blocking queue which throttles
     * puts if it reaches its capacity. This was changed in 8.1, see #51400. This function is NOOP
     * in the HARegionQueue.
     */
    @Override
    @edu.umd.cs.findbugs.annotations.SuppressWarnings("TLW_TWO_LOCK_WAIT")
    void checkQueueSizeConstraint() throws InterruptedException {
      if (this.haContainer instanceof HAContainerMap && isPrimary()) { // Fix for bug 39413
        if (Thread.interrupted())
          throw new InterruptedException();
        synchronized (this.putGuard) {
          if (putPermits <= 0) {
            synchronized (this.permitMon) {
              if (reconcilePutPermits() <= 0) {
                if (region.getSystem().getConfig().getRemoveUnresponsiveClient()) {
                  isClientSlowReceiver = true;
                } else {
                  try {
                    long logFrequency = CacheClientNotifier.DEFAULT_LOG_FREQUENCY;
                    CacheClientNotifier ccn = CacheClientNotifier.getInstance();
                    if (ccn != null) { // check needed for junit tests
                      logFrequency = ccn.getLogFrequency();
                    }
                    if ((this.maxQueueSizeHitCount % logFrequency) == 0) {
                      logger.warn("Client queue for {} client is full.",
                          new Object[] {region.getName()});
                      this.maxQueueSizeHitCount = 0;
                    }
                    ++this.maxQueueSizeHitCount;
                    this.region.checkReadiness(); // fix for bug 37581
                    // TODO: wait called while holding two locks
                    this.permitMon.wait(CacheClientNotifier.eventEnqueueWaitTime);
                    this.region.checkReadiness(); // fix for bug 37581
                    // Fix for #51400. Allow the queue to grow beyond its
                    // capacity/maxQueueSize, if it is taking a long time to
                    // drain the queue, either due to a slower client or the
                    // deadlock scenario mentioned in the ticket.
                    reconcilePutPermits();
                    if ((this.maxQueueSizeHitCount % logFrequency) == 1) {
                      logger.info("Resuming with processing puts ...");
                    }
                  } catch (InterruptedException ex) {
                    // TODO: The line below is meaningless. Comment it out later
                    this.permitMon.notifyAll();
                    throw ex;
                  }
                }
              }
            } // synchronized (this.permitMon)
          } // if (putPermits <= 0)
          --putPermits;
        } // synchronized (this.putGuard)
      }
    }

    /**
     * This function should always be called under a lock on putGuard & permitMon obejct
     *
     * @return int current Put permits
     */
    private int reconcilePutPermits() {
      putPermits += takeSidePutPermits;
      takeSidePutPermits = 0;
      return putPermits;
    }

    /**
     * Implemented to reduce contention between concurrent take/remove operations and put . The
     * reconciliation between take side put permits & put side put permits happens only if theput
     * side put permits are exhausted. In HARehionQueue base class this is a NOOP function. This was
     * added in case a put operation which has reduced the put permit optmistically but due to some
     * reason ( most likely because of duplicate event) was not added in the queue. In such case it
     * will increment take side permit without notifying any waiting thread
     */
    @Override
    void incrementTakeSidePutPermitsWithoutNotify() {
      synchronized (this.permitMon) {
        ++this.takeSidePutPermits;
      }
    }

    /**
     * Implemented to reduce contention between concurrent take/remove operations and put . The
     * reconciliation between take side put permits & put side put permits happens only if theput
     * side put permits are exhausted. In HARehionQueue base class this is a NOOP function
     */
    @Override
    void incrementTakeSidePutPermits() {
      if (this.haContainer instanceof HAContainerMap && isPrimary()) { // Fix for bug 39413
        synchronized (this.permitMon) {
          ++this.takeSidePutPermits;
          this.permitMon.notifyAll();
        }
      }
    }

    /**
     * Identical to the acquireReadLock as there is only one type of Lock object in this class.
     */
    @Override
    void acquireWriteLock() {
      this.lock.lock();
    }

    /**
     * Identical to the acquireWriteLock as there is only one type of Lock object in this class.
     */
    @Override
    void acquireReadLock() {
      this.lock.lock();
    }

    /**
     * This method is called by the publish method when a valid Long position is added to the
     * idsAvailable set. It should always be called after acquiring the ReentrantLock. It notifies
     * the waiting peek & take threads.
     *
     * <p>
     * author Asif
     */
    @Override
    void notifyPeekAndTakeThreads() {
      blockCond.signalAll();
    }

    /**
     * Returns true if data is available in the queue. This method should always be invoked after
     * acquiring the lock on ReentrantLock object. It blocks the thread if the queue is empty or
     * returns true otherwise . This will always return true indicating that data is available for
     * retrieval or throw an Exception.It can never return false.
     */
    @Override
    boolean waitForData() throws InterruptedException {
      while (this.internalIsEmpty()) {
        region.getCache().getCancelCriterion().checkCancelInProgress(null);
        boolean interrupted = Thread.currentThread().isInterrupted();
        try {
          blockCond.await(StoppableCondition.TIME_TO_WAIT);
        } catch (InterruptedException ie) {
          interrupted = true;
          region.getCache().getCancelCriterion().checkCancelInProgress(ie);
          throw new TimeoutException(ie);
        } finally {
          if (interrupted)
            Thread.currentThread().interrupt();
        }
      }
      return true;
    }

    /**
     * Noop method to prevent HARegionQueue population before the constructor of BlockingQueue is
     * complete.
     *
     * <p>
     * author Asif
     */
    @Override
    void putGIIDataInRegion() {

    }

    /**
     * Identical to the releaseWriteLock as there is only one type of Lock object in this class.
     *
     */

    @Override
    void releaseReadLock() {
      this.lock.unlock();
    }

    /**
     * Identical to the releaseReadLock as there is only one type of Lock object in this class.
     *
     * <p>
     * author Asif
     *
     */

    @Override
    void releaseWriteLock() {
      this.lock.unlock();
    }

  }

  private static class DurableHARegionQueue extends BlockingHARegionQueue {

    private LinkedHashSet durableIDsList = null;
    LinkedList unremovedElements = null;
    HashMap currDurableMap = null;

    protected DurableHARegionQueue(String regionName, InternalCache cache,
        HARegionQueueAttributes hrqa, Map haContainer, ClientProxyMembershipID clientProxyId,
        final byte clientConflation, boolean isPrimary, StatisticsClock statisticsClock)
        throws IOException, ClassNotFoundException, CacheException, InterruptedException {
      super(regionName, cache, hrqa, haContainer, clientProxyId, clientConflation, isPrimary,
          statisticsClock);

      this.threadIdToSeqId.keepPrevAcks = true;
      this.durableIDsList = new LinkedHashSet();
      this.ackedEvents = new HashMap();
      this.initialized.set(true);

    }

    @Override
    boolean waitForData() throws InterruptedException {
      region.getCache().getCancelCriterion().checkCancelInProgress(null);
      boolean interrupted = Thread.currentThread().isInterrupted();
      try {
        blockCond.await(StoppableCondition.TIME_TO_WAIT);
      } catch (InterruptedException ie) {
        interrupted = true;
        region.getCache().getCancelCriterion().checkCancelInProgress(ie);
        throw new TimeoutException(ie);
      } finally {
        if (interrupted)
          Thread.currentThread().interrupt();
      }
      return !this.internalIsEmpty();
    }

    @Override
    protected Object getNextAvailableIDFromList() throws InterruptedException {
      return this.getAndRemoveNextAvailableID();
    }

    /**
     * It is different from its super implementation only in not invoking
     * incrementTakeSidePutPermits(). Fix for #41521.
     */
    @Override
    protected Long getAndRemoveNextAvailableID() throws InterruptedException {
      Long next = null;
      acquireWriteLock();
      try {
        if (this.idsAvailable.isEmpty()) {
          if (waitForData()) {
            Iterator itr = this.idsAvailable.iterator();
            next = (Long) itr.next();
            itr.remove();
          }
        } else {
          Iterator itr = this.idsAvailable.iterator();
          next = (Long) itr.next();
          itr.remove();
        }
      } finally {
        releaseWriteLock();
      }
      return next;
    }

    @Override
    protected void storePeekedID(Long id) {
      acquireWriteLock();
      try {
        this.durableIDsList.add(id);
      } finally {
        releaseWriteLock();
      }
    }

    @Override
    protected boolean checkPrevAcks() {
      this.unremovedElements = new LinkedList();
      this.currDurableMap = new HashMap();
      synchronized (this.threadIdToSeqId.list) {
        // ARB: Construct map of threadIdToSeqId from the list of maps made by
        // QRM.
        // This list of maps is combined into one map for efficiency.
        while (!this.threadIdToSeqId.list.isEmpty()) {
          this.currDurableMap.putAll((Map) this.threadIdToSeqId.list.remove(0));
        }
      }
      return !this.currDurableMap.isEmpty();
    }

    @Override
    protected boolean checkEventForRemoval(Long counter, ThreadIdentifier threadid,
        long sequenceId) {
      if (this.currDurableMap.isEmpty()) {
        // ARB: If no acked events are found, do not remove peeked counter from
        // HARegion.
        this.unremovedElements.add(counter);
        return false;
      }

      Long seqId = (Long) this.currDurableMap.get(threadid);
      if (seqId != null) {
        if ((Long.valueOf(sequenceId)).compareTo(seqId) > 0) {
          // ARB: The acked event is older than the current peeked event.
          this.unremovedElements.add(counter);
          return false;
        }
      } else {
        // ARB: No events from current threadID were acked.
        this.unremovedElements.add(counter);
        return false;
      }

      return true;
    }

    @Override
    protected void setPeekedEvents() {
      // ARB: Set peeked events list to the events that were not acked by client.
      HARegionQueue.peekedEventsContext.set(unremovedElements.isEmpty() ? null : unremovedElements);
      this.unremovedElements = null;
      this.currDurableMap = null;
    }

    /**
     * Caller must hold the rwLock
     */
    @Override
    protected boolean removeFromOtherLists(Long position) {
      return this.durableIDsList.remove(position);
    }

    @Override
    public void initializeTransients() {
      // ARB: Durable client specific data structures for dispatcher.
      // durableIDsList: list of counters from which dispatcher peeks. Used instead of availableIds
      // list,
      // because the iterator for peeks requires a list that is not modified during iteration.
      // threadIdToSeqId.list: list of threadIdToSeqId maps. These maps are constructed from the
      // acks that are sent by client - each map corresponds to an ack message.

      if (!this.durableIDsList.isEmpty()) {
        this.acquireWriteLock();
        try {

          // ARB: The following addAll() op is expensive. @todo
          // Can be optimized by removing addAll and changing usage of idsAvailable:
          // Use durableIDsList before idsAvailable, i.e. first peek() from durableIDsList,
          // followed by idsAvailable. Similarly, destroy() operations should remove id from
          // either durableIDsList or idsAvailable.

          long start = System.currentTimeMillis();
          this.durableIDsList.addAll(this.idsAvailable);
          this.idsAvailable = this.durableIDsList;
          this.durableIDsList = new LinkedHashSet();
          long end = System.currentTimeMillis();
          if ((end - start) > 3000) {
            logger.warn("Durable client queue initialization took {} ms.",
                Long.toString(end - start));
          }
        } finally {
          this.releaseWriteLock();
        }
      }
      /*
       * Setting this threadlocal variable to null has no use as the current thread never uses it.
       * Instead it should really be set null by message dispatcher thread while starting or
       * resuming. This was added in revision 20914. Need to check if it really needs to be thread
       * local.
       */
      peekedEventsContext.set(null);
      this.threadIdToSeqId.list.clear();
    }

    /**
     * Returns the size of the idsAvailable Set
     *
     * Caller must hold the rwLock
     */
    @Override
    protected int availableIDsSize() {
      return this.idsAvailable.size() + this.durableIDsList.size();
    }

    /**
     * Caller must hold rwLock
     */
    @Override
    protected Object[] availableIDsArray() {
      // ARB: potentially expensive operation.
      LinkedList retVal = new LinkedList();
      retVal.addAll(this.durableIDsList);
      retVal.addAll(idsAvailable);
      return retVal.toArray();
    }

    @Override
    public int size() {
      acquireReadLock();
      try {
        return this.idsAvailable.size() + this.durableIDsList.size();
      } finally {
        releaseReadLock();
      }
    }

    @Override
    public void createAckedEventsMap() {
      ackedEvents = new HashMap();
    }

    @Override
    public void setAckedEvents() {
      if (threadIdToSeqId.keepPrevAcks) {
        synchronized (threadIdToSeqId.list) {
          threadIdToSeqId.list.add(ackedEvents);
        }
      }
    }

  }

  /**
   * Use caution while using it!
   */
  public void clearPeekedIDs() {
    peekedEventsContext.set(null);
  }

  /**
   * This thread will check for messages which have been dispatched. After a configurable time or
   * size is reached, it will create a new {@code QueueRemovalMessage} and send it to all the nodes
   * in the DistributedSystem
   */
  private static class QueueRemovalThread extends Thread {

    /**
     * boolean to make a shutdown request
     */
    private volatile boolean shutdown = false;

    private final InternalCache cache;

    /**
     * Constructor : Creates and initializes the thread
     */
    public QueueRemovalThread(InternalCache cache) {
      this.setDaemon(true);
      this.cache = cache;
    }

    private boolean checkCancelled() {
      if (shutdown) {
        return true;
      }
      if (cache.getCancelCriterion().isCancelInProgress()) {
        return true;
      }
      return false;
    }

    /**
     * The thread will check the dispatchedMessages map for messages that have been dispatched. It
     * will create a new {@code QueueRemovalMessage} and send it to the other nodes
     */
    @Override
    public void run() {
      InternalDistributedSystem ids = cache.getInternalDistributedSystem();
      DistributionManager dm = ids.getDistributionManager();

      try { // ensure exit message is printed
        // Long waitTime = Long.getLong(QUEUE_REMOVAL_WAIT_TIME, 1000);
        for (;;) {
          try { // be somewhat tolerant of failures
            if (checkCancelled()) {
              break;
            }

            // TODO : make the thread running time configurable
            boolean interrupted = Thread.interrupted();
            try {
              synchronized (this) {
                this.wait(1000 * (long) messageSyncInterval);
              }
            } catch (InterruptedException e) {
              interrupted = true;
              if (checkCancelled()) {
                break;
              }
              logger.warn("InterruptedException occurred in QueueRemovalThread  while waiting ",
                  e);
              break; // desperation...we must be trying to shut down...?
            } finally {
              // Not particularly important since we're exiting the thread,
              // but following the pattern is still good practice...
              if (interrupted)
                Thread.currentThread().interrupt();
            }

            if (logger.isTraceEnabled()) {
              logger.trace("QueueRemovalThread about to query the message list");
            }
            List queueRemovalMessageList = this.createMessageList();
            if (queueRemovalMessageList != null && !queueRemovalMessageList.isEmpty()) { // messages
                                                                                         // exist
              QueueRemovalMessage qrm = new QueueRemovalMessage();
              qrm.resetRecipients();
              List<CacheServer> servers = this.cache.getCacheServers();
              List<DistributedMember> recipients = new LinkedList();
              for (CacheServer server : servers) {
                recipients.addAll(CacheServerImpl.class.cast(server).getCacheServerAdvisor()
                    .adviseBridgeServers());
              }
              qrm.setRecipients(recipients);
              qrm.setMessagesList(queueRemovalMessageList);
              dm.putOutgoing(qrm);
            } // messages exist
          } // be somewhat tolerant of failures
          catch (CancelException ignore) {
            if (logger.isDebugEnabled()) {
              logger.debug("QueueRemovalThread is exiting due to cancellation");
            }
            break;
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
            if (checkCancelled()) {
              break;
            }
            if (logger.isDebugEnabled()) {
              logger.debug("QueueRemovalThread: ignoring exception", t);
            }
          }
        } // for
      } // ensure exit message is printed
      catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("QueueRemovalThread exiting due to cancellation: ", e);
        }
      } finally {
        logger.info("The QueueRemovalThread is done.");
      }
    }

    /**
     * Creates a list containing the eventIds that have been dispatched by the clients. The QRM
     * thread while operating on the MapWrapper for a given region , sets a new map object so that
     * put operations are not blocked while the QRM thread is iterating over the map contained in
     * MapWrapper & the put operations will continue using the new internal Ma.
     *
     */
    protected List createMessageList() {
      Map.Entry entry = null;
      Map.Entry internalEntry = null;
      MapWrapper threadIdToSequenceIdMap = null;
      String regionName = null;
      ThreadIdentifier tid = null;
      Long sequenceId = null;
      EventID eventId = null;
      List queueRemovalMessageList = new LinkedList();
      Iterator internalIterator = null;
      Iterator iterator = dispatchedMessagesMap.entrySet().iterator();
      while (iterator.hasNext()) {
        entry = (Map.Entry) iterator.next();
        regionName = (String) entry.getKey(); // key will be the string
        // containing
        // the region name
        queueRemovalMessageList.add(regionName);// add region name to the list
        // then add the number of event ids that will follow and finally the
        // event
        // ids themselves
        threadIdToSequenceIdMap = (MapWrapper) entry.getValue();
        // take a lock since dispatcher can be adding at the same time
        // Asif: After taking the lock , change the underlying Map object
        // So that the dispatcher thread does not see the old Map object
        Map internalMap = threadIdToSequenceIdMap.map;
        synchronized (internalMap) {
          // remove the current threadID toSequenceMap entry from the
          // dispatchedEvenstMap
          // within the lock
          iterator.remove();
          threadIdToSequenceIdMap.map = new HashMap();
        }
        // ARB: Add map to removal list for durable client queues.
        /*
         * if (threadIdToSequenceIdMap.keepPrevAcks) { synchronized (threadIdToSequenceIdMap.list) {
         * threadIdToSequenceIdMap.list.add(internalMap); } }
         */
        // first add the size within the lock
        queueRemovalMessageList.add(internalMap.size());
        internalIterator = internalMap.entrySet().iterator();
        // then add the event ids to the message list within the lock
        while (internalIterator.hasNext()) {
          internalEntry = (Map.Entry) internalIterator.next();
          tid = (ThreadIdentifier) internalEntry.getKey();
          sequenceId = (Long) internalEntry.getValue();
          eventId = new EventID(tid.getMembershipID(), tid.getThreadID(), sequenceId);
          queueRemovalMessageList.add(eventId);
        }
      }
      return queueRemovalMessageList;
    }

    /**
     * shutdown this thread and the caller thread will join this thread
     *
     */
    public void shutdown() {
      this.shutdown = true;
      this.interrupt();

      // Asif:Do not exit till QRM thread is dead , else the QRM thread
      // may operate on Null dispatchedMessagesMap //Bug 37046
      boolean interrupted = Thread.interrupted();
      try {
        this.join(15 * 1000);
      } catch (InterruptedException ignore) {
        interrupted = true;
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
      if (this.isAlive()) {
        logger.warn("QueueRemovalThread ignored cancellation");
      }
    }
  }

  /**
   * Class which keeps track of the positions ( keys) of underlying Region object for the events
   * placed in the Queue. It also keeps track of the last sequence ID dispatched. Thus all the
   * events with sequence ID less than that dispatched are eligible for removal
   */
  public static class DispatchedAndCurrentEvents implements DataSerializableFixedID, Serializable {
    /**
     * Keeps the track of last dispatched sequence ID. This field should be updated by the
     * Dispatcher thread or the QRM message
     */
    protected transient volatile long lastDispatchedSequenceId = -1L;

    /** Indicates if this object has expired* */
    private static final int TOKEN_DESTROYED = -2;

    /**
     * Counters corresponding to this ThreadIdentifier. Note that LinkedHashMap is used instead of
     * LinkedHashSet to save some memory. All we really put into this map is the key. This field is
     * null until the first add.
     */
    protected transient LinkedHashMap<Long, Object> counters;

    private transient volatile Object QRM_LOCK = new Object();

    /** the queue that owns this DACE */
    transient HARegionQueue owningQueue;

    /** set to true if this was transferred from another VM during GII */
    transient boolean isGIIDace;

    public DispatchedAndCurrentEvents(HARegionQueue owner) {
      this.owningQueue = owner;
    }

    /**
     * for deserialization. To be usable, the owningQueue of a DACE created with this method must be
     * established. That isn't done by deserialization.
     */
    public DispatchedAndCurrentEvents() {}

    /**
     * Used for debugging purpose to ensure that in no situation , for a given ThreadIdentifier the
     * order gets violated
     */
    protected volatile long lastSequenceIDPut = INIT_OF_SEQUENCEID;

    /**
     * This method adds to the conflation map & counters set if the add operation has not yet been
     * dispatched. Also it is the responsibility of this method to remove the old conflated entry.
     * As such we can assume only one thread can enter this function at a time because this DACE
     * object gets created for every ThreadIdentifier . And a given thread ( corresponding to this
     * thread identifier) is doing operation in sequence & a new add operation in DACE cannot happen
     * till the old one is done.
     *
     * @param event Object to be added to the queue
     * @param sequenceID Sequence ID of the event originating from a unqiue thread identified by its
     *        ThreadIdentifier
     */
    protected boolean putObject(Conflatable event, long sequenceID)
        throws CacheException, InterruptedException {
      Long oldPosition = null;
      final boolean isDebugEnabled_BS = logger.isTraceEnabled(LogMarker.BRIDGE_SERVER_VERBOSE);
      if (isDebugEnabled_BS && this.lastSequenceIDPut >= sequenceID
          && !owningQueue.puttingGIIDataInQueue) {
        logger.trace(LogMarker.BRIDGE_SERVER_VERBOSE,
            "HARegionQueue::DACE:putObject: Given sequence ID is already present ({}).\nThis may be a recovered operation via P2P or a GetInitialImage.\nlastSequenceIDPut = {} ; event = {};\n",
            sequenceID, lastSequenceIDPut, event);
      }

      boolean rejected = false;
      Conflatable eventInHARegion = null;

      synchronized (this) {
        if (sequenceID > this.lastSequenceIDPut) {
          if (logger.isTraceEnabled()) {
            logger.trace("HARegionQueue.putObject: adding {}", event);
          }
          this.lastSequenceIDPut = sequenceID;
        } else if (!owningQueue.puttingGIIDataInQueue) {
          if (isDebugEnabled_BS) {
            logger.trace(LogMarker.BRIDGE_SERVER_VERBOSE,
                "{} eliding event with ID {}, because it is not greater than the last sequence ID ({}). The rejected event has key <{}> and value <{}>",
                this, event.getEventId(), this.lastSequenceIDPut, event.getKeyToConflate(),
                event.getValueToConflate());
          }
          owningQueue.stats.incNumSequenceViolated();

          // increase take side put permits instead of increasing put side permits
          owningQueue.incrementTakeSidePutPermits();// WithoutNotify();
          CacheClientNotifier ccn = CacheClientNotifier.getInstance();
          if (ccn != null) {
            ccn.getClientProxy(owningQueue.clientProxyID).getStatistics().incMessagesFailedQueued();
          }
          return true;
        }

        if (lastDispatchedSequenceId == TOKEN_DESTROYED) {
          return false;
        }

        if (sequenceID > lastDispatchedSequenceId || owningQueue.puttingGIIDataInQueue) {
          // Insert the object into the Region
          Long position = owningQueue.tailKey.incrementAndGet();

          eventInHARegion = owningQueue.putEventInHARegion(event, position);

          // Add the position counter to the LinkedHashSet
          if (this.counters == null) {
            this.counters = new LinkedHashMap<Long, Object>();
          }
          this.counters.put(position, null);

          // Check if the event is conflatable
          if (owningQueue.shouldBeConflated(eventInHARegion)) {
            // Add to the conflation map & get the position of the
            // old conflatable entry. The old entry may have inserted by the
            // same
            // ThreadIdentifier or different one.
            oldPosition = owningQueue.addToConflationMap(eventInHARegion, position);
          }

          // Take the size lock & add to the list of availabelIds
          // TODO: Asif : To implement blocking peek & take , ideally notify
          // should be issued
          // from this block & hence this function should be appropriately
          // overridden
          owningQueue.publish(position);
        } else {
          rejected = true;
        }
      }
      if (rejected) {
        owningQueue.incrementTakeSidePutPermits();// WithoutNotify();
        CacheClientNotifier ccn = CacheClientNotifier.getInstance();
        if (ccn != null) {
          ccn.getClientProxy(owningQueue.clientProxyID).getStatistics().incMessagesFailedQueued();
        }
      } else {
        owningQueue.entryEnqueued(eventInHARegion);
      }
      // Remove the old conflated position
      if (oldPosition != null) {
        // Obtain the DispatchedAndCurrentEvents object
        Conflatable old = (Conflatable) owningQueue.region.get(oldPosition);
        if (old != null) {
          ThreadIdentifier oldTi = HARegionQueue.getThreadIdentifier(old.getEventId());
          DispatchedAndCurrentEvents oldDace =
              (DispatchedAndCurrentEvents) owningQueue.eventsMap.get(oldTi);
          if (oldDace != null) {
            oldDace.removeOldConflatedEntry(oldPosition);
          }
        }
      }
      return true;
    }

    /**
     * Destroys the the old entry ( which got replaced by the new entry due to conflation) from the
     * availableIDs , Region & Counters set. Since this is executed within a sync block by the new
     * entry thread, it is guaranteed that the old entry thread will exit first , placing the
     * position etc in the available IDs set. Also the new entry thread & old entry thread are
     * belonging to different ThreadIdentifier objects & hence hold different
     * DispatchedAndCurrentEvents object.
     */
    private void removeOldConflatedEntry(Long oldPosition)
        throws CacheException, InterruptedException {
      synchronized (this) {
        Conflatable conflatable = (Conflatable) owningQueue.region.get(oldPosition);
        if (owningQueue.destroyFromAvailableIDsAndRegion(oldPosition)) {
          if (this.counters != null) {
            this.counters.remove(oldPosition);
          }
          // <HA overflow>
          if (conflatable instanceof HAEventWrapper) {
            owningQueue.decAndRemoveFromHAContainer((HAEventWrapper) conflatable,
                "Remove Old Conflated Entry");
          }
          // </HA overflow>
          // update statistics

          // Fix for bug 39291:
          // Since markers are always conflated regardless of the conflation
          // setting and they are not normal (are internal) events, we should
          // not bump the events-conflated stat for markers.
          if (!(conflatable instanceof ClientMarkerMessageImpl)) {
            owningQueue.stats.incEventsConflated();
          } else {
            owningQueue.stats.incMarkerEventsConflated();
          }
        }
      }
    }

    /**
     * Removes the Entry from the Counters Set contained in DACE & from the conflation Map. This
     * method should be invoked only if the removal from available ID set returns true.
     *
     * @param position Long position to be removed from the Counter Set
     * @param key Object used as the key in the conflation Map
     * @param rName String region name against which the conflation map is stored
     */
    protected void destroy(Long position, Object key, String rName) {
      this.destroy(position);
      // Remove from conflation Map if the position in the conflation map is the
      // position
      // that is passed
      ConcurrentMap conflationMap = (ConcurrentMap) owningQueue.indexes.get(rName);
      Assert.assertTrue(conflationMap != null);
      conflationMap.remove(key, position);
    }

    /**
     * Removes the Entry from the Counters Set contained in DACE
     */
    protected synchronized void destroy(Long position) {
      if (this.counters != null) {
        this.counters.remove(position);
      }
    }

    private synchronized boolean isCountersEmpty() {
      return this.counters == null || this.counters.isEmpty();
    }

    /**
     * Invoked by the Cache Listner attached on the HARegion when the entries experience expiry.
     * This callabck is used to check whether DispatchedAndCurrentEvens object for a
     * ThreadIdentifier is eligible for removal or not. A DACE object is removed if the last
     * dispatched sequenec Id matches the expVal & the size of the counters set is 0
     *
     * @param expVal long value indicating the sequence with which the ThreadIdentifier was last
     *        updated for expiry.
     * @param ti ThreadIdentifier object corresponding to the thread which is being expired ( whose
     *        DispatchedAndCurrent Events object is being removed)
     * @return boolean true if the ThreadIdentifier object for a given DACE was expired .
     */
    protected boolean expireOrUpdate(long expVal, ThreadIdentifier ti) {
      // Check if the object is a candidate for expiry
      boolean expired = false;
      synchronized (this) {
        if (expVal == this.lastDispatchedSequenceId && isCountersEmpty()) {
          try {
            // Remove the ThreadIdentifier from the Region which was added for
            // expiry
            owningQueue.destroyFromQueue(ti);
            this.lastDispatchedSequenceId = TOKEN_DESTROYED;
            owningQueue.eventsMap.remove(ti);
            expired = true;
            this.owningQueue.getStatistics().decThreadIdentifiers();
          } catch (RegionDestroyedException e) {
            if (!owningQueue.destroyInProgress && logger.isDebugEnabled()) {
              logger.debug(
                  "DispatchedAndCurrentEvents::expireOrUpdate: Queue found destroyed while removing expiry entry for ThreadIdentifier={} and expiry value={}",
                  ti, expVal, e);
            }
          } catch (EntryNotFoundException enfe) {
            if (!owningQueue.destroyInProgress) {
              logger.error(
                  "DispatchedAndCurrentEvents::expireOrUpdate: Unexpectedly encountered exception while removing expiry entry for ThreadIdentifier={} and expiry value={}",
                  new Object[] {ti, expVal, enfe});
            }
          }
        }
      }
      if (!expired) {
        try {
          // Update the entry with latest sequence ID
          owningQueue.region.put(ti, this.lastDispatchedSequenceId);
        } catch (CancelException e) {
          throw e;
        } catch (Exception e) {
          if (!owningQueue.destroyInProgress) {
            logger.error(String.format(
                "DispatchedAndCurrentEvents::expireOrUpdate: Unexpectedly encountered exception while updating expiry ID for ThreadIdentifier=%s",
                ti),
                e);
          }
        }
      }
      return expired;
    }

    /**
     * Invoked by the by the QRM message . This method sets the LastDispatched sequence ID in the
     * DACE & destroys all the sequence Ids which are less than the last dispatched sequence ID. At
     * a time only one thread operates on it which is accomplished by the QRM_LOCK. The lock on the
     * DACE is minimized by copying the Counters Set & then identifying the positions which need to
     * be removed
     *
     * @param lastDispatchedSeqId long indicating the last dispatched ID which gets set in a DACE
     */
    protected void setLastDispatchedIDAndRemoveEvents(long lastDispatchedSeqId)
        throws CacheException, InterruptedException {
      Long[] countersCopy = null;
      synchronized (this.QRM_LOCK) {
        synchronized (this) {
          if (this.lastDispatchedSequenceId > lastDispatchedSeqId) {
            // If the current last dispatched ID is greater than the new id ,
            // then do not set it
            return;
          }
          this.lastDispatchedSequenceId = lastDispatchedSeqId;
          if (this.counters != null) {
            countersCopy = new Long[this.counters.size()];
            countersCopy = this.counters.keySet().toArray(countersCopy);
          }
        } // synchronized this

        if (countersCopy != null) {
          for (int i = 0; i < countersCopy.length; i++) {
            Long counter = countersCopy[i];
            Conflatable event = (Conflatable) owningQueue.region.get(counter);
            if (event == null) {
              // this.destroy(counter); event already destroyed?
              continue;
            }

            long seqId = event.getEventId().getSequenceID();
            if (seqId > this.lastDispatchedSequenceId) {
              break; // we're done
            }

            if (!owningQueue.destroyFromAvailableIDsAndRegion(counter)) {
              continue; // still valid
            }

            if (event instanceof HAEventWrapper) {
              if (((HAEventWrapper) event).getReferenceCount() == 0 && logger.isDebugEnabled()) {
                logger.debug("Reference count is already zero for event {}", event.getEventId());
              }

              owningQueue.decAndRemoveFromHAContainer((HAEventWrapper) event,
                  "Queue Removal Message");
            }

            // At this point we know we're going to remove the event,
            // so increment the statistic
            owningQueue.stats.incEventsRemovedByQrm();

            if (!owningQueue.shouldBeConflated(event)) {
              // Just update the counters set
              this.destroy(counter);
              continue; // we're done
            }

            Object key = event.getKeyToConflate();
            String r = event.getRegionToConflate();
            // Update the counters set and the conflation map.
            this.destroy(counter, key, r);
          } // for
        }
      } // synchronized
    }

    /**
     * Invoked by the by the remove method . This method sets the LastDispatched sequence ID in the
     * DACE & destroys all the sequence Ids which are less than the last dispatched sequence ID. The
     * lock on the DACE is minimized by copying the Counters Set & then identifying the positions
     * which need to be removed
     *
     * @param removedEventInfoList List containing objects of RemovedEventInfo class representing
     *        Events which have been peeked & are now candidate for removal. It has to be guaranteed
     *        that the sequence IDs of all the other counters is less than the last dispatched
     * @param lastDispatchedSeqId long indicating the last dispatched ID which gets set in a DACE
     */
    protected void setLastDispatchedIDAndRemoveEvents(List removedEventInfoList,
        long lastDispatchedSeqId) throws CacheException, InterruptedException {

      synchronized (this) {
        if (this.lastDispatchedSequenceId > lastDispatchedSeqId) {
          // If the current last dispatched ID is greater than the new id ,
          // then do not set it
          return;
        }
        this.lastDispatchedSequenceId = lastDispatchedSeqId;

      }
      Iterator it = removedEventInfoList.iterator();
      while (it.hasNext()) {
        RemovedEventInfo info = (RemovedEventInfo) it.next();
        Long counter = info.counter;
        Object key = info.key;
        String r = info.regionName;
        Conflatable wrapper = (Conflatable) owningQueue.region.get(counter);
        if (owningQueue.destroyFromAvailableIDsAndRegion(counter)) {
          if (key != null) {
            this.destroy(counter, key, r);
          } else {
            this.destroy(counter);
          }
          // <HA overflow>
          if (wrapper instanceof HAEventWrapper) {
            owningQueue.decAndRemoveFromHAContainer((HAEventWrapper) wrapper, "Message Dispatcher");
          }
          // </HA overflow>
          owningQueue.stats.incEventsRemoved();
        } else {
          owningQueue.stats.incNumVoidRemovals();
        }
      }
    }

    /**
     * This method is invoked by the take function. Before invoking it , the take has already
     * removed the poistion from the available IDs set.
     *
     * @param info Data object of type RemovedEventInfo which contains info like position countre,
     *        key & region name
     * @param sequenceID sequence ID of the event being removed from HARegionQueue
     */
    protected void removeEventAndSetSequenceID(RemovedEventInfo info, long sequenceID) {
      synchronized (this) {
        if (this.lastDispatchedSequenceId < sequenceID) {
          this.lastDispatchedSequenceId = sequenceID;
        }
      }

      Long counter = info.counter;
      Object key = info.key;
      String r = info.regionName;
      try {
        owningQueue.destroyFromQueue(counter);
      } catch (EntryNotFoundException enfe) {
        if (!owningQueue.destroyInProgress) {
          logger.error(
              "DACE::removeEventAndSetSequenceID: Since the event was successuly removed by a take operation, it should have existed in the region",
              enfe);
        }
      }
      if (key == null) {
        this.destroy(counter);
      } else {
        this.destroy(counter, key, r);
      }

    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.geode.internal.serialization.DataSerializableFixedID#fromData(java.io.DataInput)
     */
    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      synchronized (this) {
        this.lastDispatchedSequenceId = in.readLong();
        this.lastSequenceIDPut = in.readLong();
      }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.internal.serialization.DataSerializableFixedID#getDSFID()
     */
    @Override
    public int getDSFID() {
      return DISPATCHED_AND_CURRENT_EVENTS;
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.geode.internal.serialization.DataSerializableFixedID#toData(java.io.DataOutput)
     */
    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      synchronized (this) { // fix for bug #41621
        out.writeLong(this.lastDispatchedSequenceId);
        out.writeLong(this.lastSequenceIDPut);
      }
    }

    @Override
    public String toString() {
      return "DACE(put=" + this.lastSequenceIDPut + "sent=" + this.lastDispatchedSequenceId + ")";
    }

    @Override
    public Version[] getSerializationVersions() {
      // TODO Auto-generated method stub
      return null;
    }
  }

  // TODO:Asif : Remove this method
  @Override
  public void remove(int top) {
    throw new UnsupportedOperationException(
        "HARegionQueue and its derived class do not support this operation ");

  }

  /**
   * destroys the underlying HARegion and removes its reference from the dispatched messages map
   */
  public void destroy() throws CacheWriterException {
    this.destroyInProgress = true;
    Map tempDispatchedMessagesMap = dispatchedMessagesMap;
    if (tempDispatchedMessagesMap != null) {
      tempDispatchedMessagesMap.remove(this.regionName);
    }
    try {
      try {
        updateHAContainer();
      } catch (RegionDestroyedException ignore) {
        // keep going
      } catch (CancelException ignore) {
        // keep going
        if (logger.isDebugEnabled()) {
          logger.debug("HARegionQueue#destroy: ignored cancellation!!!!");
        }
      }

      try {
        this.region.destroyRegion();
      } catch (RegionDestroyedException | CancelException ignore) {
        // keep going
      }
      ((HAContainerWrapper) haContainer).removeProxy(regionName);
    } finally {
      this.stats.close();
    }
  }

  /**
   * If the event is an instance of HAEventWrapper, put it into the haContainer and then into the ha
   * region. Otherwise, simply put it into the ha region.
   *
   * @since GemFire 5.7
   */
  protected Conflatable putEventInHARegion(Conflatable event, Long position) {
    if (event instanceof HAEventWrapper) {
      HAEventWrapper inputHaEventWrapper = (HAEventWrapper) event;
      HAEventWrapper haContainerKey = null;

      if (this.isQueueInitialized()) {
        haContainerKey = putEntryConditionallyIntoHAContainer(inputHaEventWrapper);
      } else {
        haContainerKey = inputHaEventWrapper;
      }

      if (logger.isDebugEnabled()) {
        logger.debug("adding haContainerKey to HARegion at " + position + ":"
            + haContainerKey + " for " + this.regionName);
      }
      this.region.put(position, haContainerKey);

      return haContainerKey;
    } else { // (event instanceof ClientMarkerMessageImpl OR ConflatableObject OR
             // ClientInstantiatorMessage)
      if (logger.isDebugEnabled()) {
        logger.debug("adding ClientUpdateMessage to HARegion at " + position + ":" + event + " for "
            + this.regionName);
      }
      this.region.put(position, event);

      return event;
    }
  }

  private void addClientCQsAndInterestList(ClientUpdateMessageImpl msg,
      HAEventWrapper haEventWrapper, Map haContainer, String regionName) {

    ClientProxyMembershipID proxyID = ((HAContainerWrapper) haContainer).getProxyID(regionName);
    if (haEventWrapper.getClientCqs() != null) {
      CqNameToOp clientCQ = haEventWrapper.getClientCqs().get(proxyID);
      if (clientCQ != null) {
        msg.addClientCqs(proxyID, clientCQ);
      }
    }

    // This is a remote HAEventWrapper.
    // Add new Interested client lists.
    ClientUpdateMessageImpl clientMsg =
        (ClientUpdateMessageImpl) haEventWrapper.getClientUpdateMessage();
    if (clientMsg != null) {
      if (clientMsg.isClientInterestedInUpdates(proxyID)) {
        msg.addClientInterestList(proxyID, true);
      } else if (clientMsg.isClientInterestedInInvalidates(proxyID)) {
        msg.addClientInterestList(proxyID, false);
      }
    }
  }

  /**
   * If the wrapper's referenceCount becomes 1 after increment, then set this haEventWrapper and its
   * clientUpdateMessage into the haContainer as <key, value>.
   *
   * @param inputHaEventWrapper An instance of {@code HAEventWrapper}
   * @since GemFire 5.7
   */
  protected HAEventWrapper putEntryConditionallyIntoHAContainer(
      HAEventWrapper inputHaEventWrapper) {
    HAEventWrapper haContainerKey = null;

    while (haContainerKey == null) {
      ClientUpdateMessageImpl haContainerEntry =
          (ClientUpdateMessageImpl) ((HAContainerWrapper) this.haContainer)
              .putIfAbsent(inputHaEventWrapper, inputHaEventWrapper.getClientUpdateMessage());

      if (haContainerEntry != null) {
        haContainerKey = (HAEventWrapper) ((HAContainerWrapper) this.haContainer)
            .getKey(inputHaEventWrapper);

        // Key was already removed from the container, so continue
        if (haContainerKey == null) {
          continue;
        }

        synchronized (haContainerKey) {
          // assert the entry is still present and we still have the same reference
          if (haContainerKey == ((HAContainerWrapper) this.haContainer).getKey(haContainerKey)) {
            haContainerKey.incAndGetReferenceCount();

            addClientCQsAndInterestList(haContainerEntry, inputHaEventWrapper,
                this.haContainer, this.regionName);

            if (logger.isDebugEnabled()) {
              logger.debug("Putting updated event in haContainer with Event ID hash code: "
                  + haContainerKey.hashCode() + "; System ID hash code: "
                  + System.identityHashCode(haContainerKey)
                  + "; Wrapper details: " + haContainerKey);
            }
          } else {
            haContainerKey = null;
          }
        }
      } else {
        synchronized (inputHaEventWrapper) {
          inputHaEventWrapper.incAndGetReferenceCount();
          inputHaEventWrapper.setHAContainer(this.haContainer);

          if (!inputHaEventWrapper.getPutInProgress()) {
            // This means that this is a GII'ed event. Hence we must
            // explicitly set 'clientUpdateMessage' to null.
            inputHaEventWrapper.setClientUpdateMessage(null);
          }

          if (logger.isDebugEnabled()) {
            logger.debug("Putting new event in haContainer with Event ID hash code: "
                + inputHaEventWrapper.hashCode()
                + "; System ID hash code: " + System.identityHashCode(inputHaEventWrapper)
                + "; Wrapper details: " + inputHaEventWrapper);
          }
        }

        haContainerKey = inputHaEventWrapper;
      }
    }

    return haContainerKey;
  }

  /**
   * Caller must hold the rwLock
   *
   * @return size of idsAvailable
   */
  protected int availableIDsSize() {
    return this.idsAvailable.size();
  }

  /**
   * Caller must hold the rwLock
   *
   * @return the idsAvailable Set as an array
   */
  protected Object[] availableIDsArray() {
    return this.idsAvailable.toArray();
  }

  /**
   * whether the primary queue for the client has registered interest
   */
  public boolean noPrimaryOrHasRegisteredInterest() {
    return this.region.noPrimaryOrHasRegisteredInterest();
  }

  /**
   * set whether this queue has had interest registered for it
   */
  public void setHasRegisteredInterest(boolean flag) {
    boolean old = this.hasRegisteredInterest;
    this.hasRegisteredInterest = flag;
    if (old != flag) {
      this.region.sendProfileUpdate();
    }
  }

  /**
   * hs this queue had interest registered for it?
   */
  public boolean getHasRegisteredInterest() {
    return this.hasRegisteredInterest;
  }

  /**
   * Called from destroy(), this method decrements the referenceCount of all the HAEventWrapper
   * instances held by this queue. Also, removes those instances whose referenceCount becomes zero.
   *
   * @since GemFire 5.7
   */
  private void updateHAContainer() {
    try {
      Object[] availableIdsArray = null;
      acquireReadLock();
      try {
        if (this.availableIDsSize() != 0) {
          availableIdsArray = this.availableIDsArray();
        }
      } finally {
        releaseReadLock();
      }
      if (availableIdsArray != null) {
        final Set wrapperSet = new HashSet();

        for (int i = 0; i < availableIdsArray.length; i++) {
          if (destroyFromAvailableIDs((long) availableIdsArray[i])) {
            wrapperSet.add(this.region.get(availableIdsArray[i]));
          }
        }

        // Start a new thread which will update the clientMessagesRegion for
        // each of the HAEventWrapper instances present in the wrapperSet
        Thread regionCleanupTask =
            new LoggingThread("HA Region Cleanup for " + regionName, false, () -> {
              try {
                Iterator iter = wrapperSet.iterator();
                while (iter.hasNext()) {
                  Conflatable conflatable = (Conflatable) iter.next();
                  if (conflatable instanceof HAEventWrapper) {
                    decAndRemoveFromHAContainer((HAEventWrapper) conflatable, "Destroy");
                  }
                }
              } catch (CancelException ignore) {
                return; // we're done
              } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                  logger.debug(
                      "Exception in regionCleanupTask thread of HARegionQueue.updateHAContainer$run()",
                      e);
                }
              }
            });
        regionCleanupTask.start();
      }
    } catch (CancelException e) {
      throw e;
    } catch (RegionDestroyedException e) {
      // TODO does this really generate a CancelException, or do we still
      // get a warning in the logs?

      // Odds are we're shutting down...
      this.getRegion().getCache().getCancelCriterion().checkCancelInProgress(e);

      // If we get back, this is Really Weird. Treat it like the
      // Exception case below.
      logger.warn("HARegionQueue.updateHAContainer: underlying region has been destroyed", e);
    } catch (Exception e) {
      logger.warn(
          "Exception in HARegionQueue.updateHAContainer(). The task to decrement the ref count by one for all the HAEventWrapper instances of this queue present in the haContainer may not have been started",
          e);
    }
  }

  /**
   * If the conflatable is an instance of HAEventWrapper, and if the corresponding entry is present
   * in the haContainer, then decrements its reference count. If the decremented ref count is zero
   * and put is not in progress, removes the entry from the haContainer, before returning the
   * {@code ClientUpdateMessage} instance.
   *
   * @return An instance of {@code ClientUpdateMessage}
   * @since GemFire 5.7
   */
  public Conflatable getAndRemoveFromHAContainer(Conflatable conflatable) {
    Conflatable msg = null;
    if (conflatable instanceof HAEventWrapper) {
      HAEventWrapper wrapper = (HAEventWrapper) conflatable;
      msg = (Conflatable) HARegionQueue.this.haContainer.get(wrapper);
      if (msg != null) {
        decAndRemoveFromHAContainer(wrapper, "GetAndRemoveFromHAContainer");
      }
    } else {
      msg = conflatable;
    }
    return msg;
  }

  /**
   * Decrements reference count for the wrapper in the container by one. If the decremented ref
   * count is zero and put is not in progress, removes the entry from the haContainer.
   *
   * @since GemFire 5.7
   */
  public void decAndRemoveFromHAContainer(HAEventWrapper wrapper) {
    decAndRemoveFromHAContainer(wrapper, "");
  }

  public void decAndRemoveFromHAContainer(HAEventWrapper wrapper, String caller) {
    boolean decAndRemovePerformed = false;

    while (!decAndRemovePerformed) {
      HAEventWrapper haContainerKey =
          (HAEventWrapper) ((HAContainerWrapper) haContainer).getKey(wrapper);

      if (haContainerKey == null) {
        break;
      }

      synchronized (haContainerKey) {
        if (haContainerKey == (HAEventWrapper) ((HAContainerWrapper) haContainer).getKey(wrapper)) {
          if (logger.isDebugEnabled()) {
            logger.debug(caller + " decremented Event ID hash code: " + haContainerKey.hashCode()
                + "; System ID hash code: " + System.identityHashCode(haContainerKey)
                + "; Wrapper details: " + haContainerKey);
          }
          if (haContainerKey.decAndGetReferenceCount() == 0L) {
            HARegionQueue.this.haContainer.remove(haContainerKey);
            if (logger.isDebugEnabled()) {
              logger.debug(
                  caller + " removed Event ID hash code: " + haContainerKey.hashCode()
                      + "; System ID hash code: "
                      + System.identityHashCode(haContainerKey)
                      + "; Wrapper details: " + haContainerKey);
            }
          }
          decAndRemovePerformed = true;
        }
      }
    }
  }

  /**
   * Returns true if the dispatcher for this HARegionQueue is active.
   *
   * @return the true if dispatcher for this HARegionQueue is active(primary node)
   */
  public boolean isPrimary() {
    return isPrimary;
  }

  /**
   * returns true if the queue has been fully initialized
   */
  public boolean isQueueInitialized() {
    return this.initialized.get();
  }

  /**
   * Set whether the dispatcher of this node is active or not (i.e. primary or secondary node). If
   * {@code flag} is set to {@code true}, disables Entry Expiry Tasks.
   *
   * @param flag the value to set isPrimary to
   */
  public void setPrimary(boolean flag) {
    boolean old = this.isPrimary;
    this.isPrimary = flag;
    if (flag) {// fix for #41878
      // since it's primary queue now, we will disable the EntryExpiryTask
      disableEntryExpiryTasks();
    }
    if (old != isPrimary) {
      this.region.sendProfileUpdate();
    }
  }

  /**
   * Disables EntryExpiryTask for the HARegion ({@code this.region}).
   *
   */
  private void disableEntryExpiryTasks() {
    int oldTimeToLive = this.region.getEntryTimeToLive().getTimeout();
    if (oldTimeToLive > 0) {
      ExpirationAttributes ea = new ExpirationAttributes(0, // disables expiration
          ExpirationAction.LOCAL_INVALIDATE);
      this.region.setEntryTimeToLive(ea);
      this.region.setCustomEntryTimeToLive(new ThreadIdentifierCustomExpiry());
      logger.info(
          "Entry expiry tasks disabled because the queue became primary. Old messageTimeToLive was: {}",
          oldTimeToLive);
    }
  }

  /**
   * Set client conflation override
   *
   * @since GemFire 5.7
   */
  public void setClientConflation(byte value) {
    if (value != Handshake.CONFLATION_OFF && value != Handshake.CONFLATION_ON
        && value != Handshake.CONFLATION_DEFAULT) {
      throw new IllegalArgumentException("illegal conflation value");
    }
    this.clientConflation = value;
  }

  public void initializeTransients() {}

  public static boolean isTestMarkerMessageReceived() {
    return testMarkerMessageReceived;
  }

  public static void setUsedByTest(boolean isUsedByTest) {
    HARegionQueue.isUsedByTest = isUsedByTest;
    if (!isUsedByTest) {
      HARegionQueue.testMarkerMessageReceived = isUsedByTest;
    }
  }

  public boolean isClientSlowReceiver() {
    return isClientSlowReceiver;
  }

  @Override
  public void close() {
    Region r = getRegion();
    if (r != null && !r.isDestroyed()) {
      try {
        r.close();
      } catch (RegionDestroyedException ignore) {
      }
    }
  }

  /**
   * A simple check to validate that the peek() method has been executed as it initializes some
   * structures used by other methods.
   *
   * @return true if peeking returns a non-null value
   */
  public boolean isPeekInitialized() {
    return HARegionQueue.peekedEventsContext.get() != null;
  }

  /**
   * A wrapper class whose underlying map gets replaced with a fresh one when QRM thread is
   * operating on it. This wrapper acts as a means of communication between the QRM thread & the
   * MapWrapper object contained in the HARegionQueue
   */
  static class MapWrapper {
    Map map;

    List list;

    boolean keepPrevAcks = false;

    public MapWrapper() {
      super();
      map = new HashMap();
      list = new LinkedList();
    }

    void put(Object key, Object o) {
      synchronized (this.map) {
        this.map.put(key, o);
      }
    }
  }

  /**
   * A wrapper class that has counter, key and the region-name for an event which was peeked and
   * needs to be removed. The key and regionName fields will be set only if conflation is true for
   * the event.
   */
  static class RemovedEventInfo {
    Long counter;

    String regionName;

    Object key;

    public RemovedEventInfo(Long counter, String regionName, Object key) {
      this.counter = counter;
      this.regionName = regionName;
      this.key = key;
    }
  }

  /** this is used to expire thread identifiers, even in primary queues */
  static class ThreadIdentifierCustomExpiry implements CustomExpiry {

    /**
     * expiry time for ThreadIdentifiers. In seconds.
     */
    static final int DEFAULT_THREAD_ID_EXPIRY_TIME = 300;

    @Immutable
    private static final ExpirationAttributes DEFAULT_THREAD_ID_EXP_ATTS =
        new ExpirationAttributes(DEFAULT_THREAD_ID_EXPIRY_TIME, ExpirationAction.LOCAL_INVALIDATE);

    @MutableForTesting
    private static volatile ExpirationAttributes testExpAtts;

    private final int expTime;

    ThreadIdentifierCustomExpiry() {
      expTime = calculateThreadIdExpiryTime();
    }

    @Override
    public ExpirationAttributes getExpiry(Region.Entry entry) {
      // Use key to determine expiration.
      Object key = entry.getKey();
      if (key instanceof ThreadIdentifier) {
        // TODO: inject subclass of ThreadIdentifierCustomExpiry and move next block to test class
        if (expTime != DEFAULT_THREAD_ID_EXPIRY_TIME) {
          // This should only happen in unit test code
          ExpirationAttributes result = testExpAtts;
          if (result == null || result.getTimeout() != expTime) {
            result = new ExpirationAttributes(expTime, ExpirationAction.LOCAL_INVALIDATE);
            // save the expiration attributes in a static to prevent tests from creating lots of
            // instances.
            testExpAtts = result;
          }
          return result;
        } else {
          return DEFAULT_THREAD_ID_EXP_ATTS;
        }
      } else {
        return null;
      }
    }

    @Override
    public void close() {
      // nothing
    }

    private static int calculateThreadIdExpiryTime() {
      Optional<Integer> expiryTime = getProductIntegerProperty(THREAD_ID_EXPIRY_TIME_PROPERTY);
      return expiryTime.orElse(DEFAULT_THREAD_ID_EXPIRY_TIME);
    }
  }

  public Queue getGiiQueue() {
    return this.giiQueue;
  }
}
