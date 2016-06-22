/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.control.MemoryEvent;
import com.gemstone.gemfire.internal.cache.control.ResourceListener;
import com.gemstone.gemfire.internal.cache.versions.CompactVersionHolder;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.size.ReflectionSingleObjectSizer;
import com.gemstone.gemfire.internal.util.concurrent.StoppableReentrantLock;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tombstones are region entries that have been destroyed but are held
 * for future concurrency checks.  They are timed out after a reasonable
 * period of time when there is no longer the possibility of concurrent
 * modification conflicts.
 * <p>
 * The cache holds a tombstone service that is responsible for tracking
 * and timing out tombstones.
 * 
 */
public class TombstoneService  implements ResourceListener<MemoryEvent> {
  private static final Logger logger = LogService.getLogger();
  
  /**
   * The default tombstone expiration period, in milliseconds for replicated
   * regions.<p>  This is the period over which the destroy operation may
   * conflict with another operation.  After this timeout elapses the tombstone
   * is put into a GC set for removal.  Removal is typically triggered by
   * the size of the GC set, but could be influenced by resource managers.
   * 
   * The default is 600,000 milliseconds (10 minutes).
   */
  public static long REPLICATED_TOMBSTONE_TIMEOUT = Long.getLong(
      DistributionConfig.GEMFIRE_PREFIX + "tombstone-timeout", 600000L).longValue();
  
  /**
   * The default tombstone expiration period in millis for non-replicated
   * regions.  This tombstone timeout should be shorter than the one for
   * replicated regions and need not be excessively long.  Making it longer
   * than the replicated timeout can cause non-replicated regions to issue
   * revisions based on the tombstone that could overwrite modifications made
   * by others that no longer have the tombstone.<p>
   * The default is 480,000 milliseconds (8 minutes)
   */
  public static long CLIENT_TOMBSTONE_TIMEOUT = Long.getLong(
      DistributionConfig.GEMFIRE_PREFIX + "non-replicated-tombstone-timeout", 480000);
  
  /**
   * The max number of tombstones in an expired batch.  This covers
   * all replicated regions, including PR buckets.  The default is
   * 100,000 expired tombstones.
   */
  public static long EXPIRED_TOMBSTONE_LIMIT = Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "tombstone-gc-threshold", 100000);
  
  /**
   * The interval to scan for expired tombstones in the queues
   */
  public static long DEFUNCT_TOMBSTONE_SCAN_INTERVAL = Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "tombstone-scan-interval", 60000);
  
  /**
   * The threshold percentage of free max memory that will trigger tombstone GCs.
   * The default percentage is somewhat less than the LRU Heap evictor so that
   * we evict tombstones before we start evicting cache data.
   */
  public static double GC_MEMORY_THRESHOLD = Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "tombstone-gc-memory-threshold",
      30 /*100-HeapLRUCapacityController.DEFAULT_HEAP_PERCENTAGE*/) * 0.01;
  
  /** this is a test hook for causing the tombstone service to act as though free memory is low */
  public static boolean FORCE_GC_MEMORY_EVENTS = false;

  public final static Object debugSync = new Object();
  public final static boolean DEBUG_TOMBSTONE_COUNT = Boolean
      .getBoolean(DistributionConfig.GEMFIRE_PREFIX + "TombstoneService.DEBUG_TOMBSTONE_COUNT"); // TODO:LOG:replace TombstoneService.DEBUG_TOMBSTONE_COUNT

  public static boolean IDLE_EXPIRATION = false; // dunit test hook for forced batch expiration
  
  /**
   * two sweepers, one for replicated regions (including PR buckets) and one for
   * other regions.  They have different timeout intervals.
   */
  private final TombstoneSweeper replicatedTombstoneSweeper;
  private final TombstoneSweeper nonReplicatedTombstoneSweeper;

  public final Object blockGCLock = new Object();
  private int progressingDeltaGIICount; 
  
  public static TombstoneService initialize(GemFireCacheImpl cache) {
    TombstoneService instance = new TombstoneService(cache);
//    cache.getResourceManager().addResourceListener(instance);  experimental
    return instance;
  }
  
  private TombstoneService(GemFireCacheImpl cache) {
    this.replicatedTombstoneSweeper = new TombstoneSweeper(cache, new ConcurrentLinkedQueue<Tombstone>(),
        REPLICATED_TOMBSTONE_TIMEOUT, true, new AtomicLong());
    this.nonReplicatedTombstoneSweeper = new TombstoneSweeper(cache, new ConcurrentLinkedQueue<Tombstone>(),
        CLIENT_TOMBSTONE_TIMEOUT, false, new AtomicLong());
    this.replicatedTombstoneSweeper.start();
    this.nonReplicatedTombstoneSweeper.start();
  }

  /**
   * this ensures that the background sweeper thread is stopped
   */
  public void stop() {
    this.replicatedTombstoneSweeper.stop();
    this.nonReplicatedTombstoneSweeper.stop();
  }
  
 /**
   * Tombstones are markers placed in destroyed entries in order to keep the
   * entry around for a while so that it's available for concurrent modification
   * detection.
   * 
   * @param r  the region holding the entry
   * @param entry the region entry that holds the tombstone
   * @param destroyedVersion the version that was destroyed
   */
  public void scheduleTombstone(LocalRegion r, RegionEntry entry, VersionTag destroyedVersion) {
    if (entry.getVersionStamp() == null) {
      logger.warn("Detected an attempt to schedule a tombstone for an entry that is not versioned in region " + r.getFullPath(), new Exception("stack trace"));
      return;
    }
    Tombstone ts = new Tombstone(entry, r, destroyedVersion);
    this.getSweeper(r).scheduleTombstone(ts);
  }
  
  
  private TombstoneSweeper getSweeper(LocalRegion r)  {
    if (r.getScope().isDistributed() && r.getServerProxy() == null && r.dataPolicy.withReplication()) {
      return this.replicatedTombstoneSweeper;
    } else {
      return this.nonReplicatedTombstoneSweeper;
    }
  }
  
  
  /**
   * remove all tombstones for the given region.  Do this when the region is
   * cleared or destroyed.
   * @param r
   */
  public void unscheduleTombstones(LocalRegion r) {
    TombstoneSweeper sweeper = this.getSweeper(r);
    Queue<Tombstone> queue = sweeper.getQueue();
    long removalSize = 0;
    for (Iterator<Tombstone> it=queue.iterator(); it.hasNext(); ) {
      Tombstone t = it.next();
      if (t.region == r) {
        it.remove();
        removalSize += t.getSize();
      }
    }
    sweeper.incQueueSize(-removalSize);
  }
  
  public int getGCBlockCount() {
    synchronized(this.blockGCLock) {
      return this.progressingDeltaGIICount;
    }
  }
   
  public int incrementGCBlockCount() {
    synchronized(this.blockGCLock) {
      return ++this.progressingDeltaGIICount;
    }
  }
  
  public int decrementGCBlockCount() {
    synchronized(this.blockGCLock) {
      return --this.progressingDeltaGIICount;
    }
  }
  
  /**
   * remove tombstones from the given region that have region-versions <= those in the given removal map
   * @return a collection of keys removed (only if the region is a bucket - empty otherwise)
   */
  public Set<Object> gcTombstones(LocalRegion r, Map<VersionSource, Long> regionGCVersions) {
    synchronized(this.blockGCLock) {
      int count = getGCBlockCount(); 
      if (count > 0) {
        // if any delta GII is on going as provider at this member, not to do tombstone GC
        if (logger.isDebugEnabled()) {
          logger.debug("gcTombstones skipped due to {} Delta GII on going", count);
        }
        return null;
      }
    long removalSize = 0;
    if (logger.isDebugEnabled()) {
      logger.debug("gcTombstones invoked for region {} and version map {}", r, regionGCVersions);
    }
    Set<Tombstone> removals = new HashSet<Tombstone>();
    VersionSource myId = r.getVersionMember();
    boolean isBucket = r.isUsedForPartitionedRegionBucket();
    final TombstoneSweeper sweeper = this.getSweeper(r);
    Tombstone currentTombstone = sweeper.lockAndGetCurrentTombstone();
    try {
      if (currentTombstone != null && currentTombstone.region == r) {
        VersionSource destroyingMember = currentTombstone.getMemberID();
        if (destroyingMember == null) {
          destroyingMember = myId;
        }
        Long maxReclaimedRV = regionGCVersions.get(destroyingMember);
        if (maxReclaimedRV != null && currentTombstone.getRegionVersion() <= maxReclaimedRV.longValue()) {
          removals.add(currentTombstone);
          removalSize += currentTombstone.getSize();
          // TODO call sweeper.clearCurrentTombstone
        }
      }
      for (Iterator<Tombstone> it=sweeper.getQueue().iterator(); it.hasNext(); ) {
        Tombstone t = it.next();
        if (t.region == r) {
          VersionSource destroyingMember = t.getMemberID();
          if (destroyingMember == null) {
            destroyingMember = myId;
          }
          Long maxReclaimedRV = regionGCVersions.get(destroyingMember);
          if (maxReclaimedRV != null && t.getRegionVersion() <= maxReclaimedRV.longValue()) {
            it.remove();
            removals.add(t);
            removalSize += t.getSize();
          }
        }
      }
      sweeper.incQueueSize(-removalSize);
    } finally {
      sweeper.unlock();
    }
    
    //Record the GC versions now, so that we can persist them
    for(Map.Entry<VersionSource, Long> entry : regionGCVersions.entrySet()) {
      r.getVersionVector().recordGCVersion(entry.getKey(), entry.getValue());
    }
    
    //Remove any exceptions from the RVV that are older than the GC version
    r.getVersionVector().pruneOldExceptions();

    //Persist the GC RVV to disk. This needs to happen BEFORE we remove
    //the entries from map, to prevent us from removing a tombstone
    //from disk that has a version greater than the persisted
    //GV RVV.
    if(r.getDataPolicy().withPersistence()) {
      //Update the version vector which reflects what has been persisted on disk.
      r.getDiskRegion().writeRVVGC(r);
    }
    
    Set<Object> removedKeys = new HashSet();
    for (Tombstone t: removals) {
      if (t.region.getRegionMap().removeTombstone(t.entry, t, false, true) && isBucket) {
        removedKeys.add(t.entry.getKey());
      }
    }
    return removedKeys;
    } // sync on deltaGIILock
  }
  
  /**
   * client tombstone removal is key-based if the server is a PR.  This is due to the
   * server having separate version vectors for each bucket.  In the client this causes
   * the version vector to make no sense, so we have to send it a collection of the
   * keys removed on the server and then we brute-force remove any of them that
   * are tombstones on the client
   *  
   * @param r the region affected
   * @param tombstoneKeys the keys removed on the server
   */
  public void gcTombstoneKeys(LocalRegion r, Set<Object> tombstoneKeys) {
    if (r.getServerProxy() == null) {
      // if the region does not have a server proxy
      // then it will not have any tombstones to gc for the server.
      return;
    }
    final TombstoneSweeper sweeper = this.getSweeper(r);
    Set<Tombstone> removals = new HashSet<Tombstone>();
    Tombstone currentTombstone = sweeper.lockAndGetCurrentTombstone();
    try {
      long removalSize = 0;
      VersionSource myId = r.getVersionMember();
      if (logger.isDebugEnabled()) {
        logger.debug("gcTombstones invoked for region {} and keys {}", r, tombstoneKeys);
      }
      if (currentTombstone != null && currentTombstone.region == r) {
        VersionSource destroyingMember = currentTombstone.getMemberID();
        if (destroyingMember == null) {
          destroyingMember = myId;
        }
        if (tombstoneKeys.contains(currentTombstone.entry.getKey())) {
          removals.add(currentTombstone);
          removalSize += currentTombstone.getSize();
          // TODO: shouldn't we call sweeper.clearTombstone()?
        }
      }
      for (Iterator<Tombstone> it=sweeper.getQueue().iterator(); it.hasNext(); ) {
        Tombstone t = it.next();
        if (t.region == r) {
          VersionSource destroyingMember = t.getMemberID();
          if (destroyingMember == null) {
            destroyingMember = myId;
          }
          if (tombstoneKeys.contains(t.entry.getKey())) {
            it.remove();
            removals.add(t);
            removalSize += t.getSize();
          }
        }
      }
      sweeper.incQueueSize(-removalSize);
    } finally {
      sweeper.unlock();
    }
    
    for (Tombstone t: removals) {
      //TODO - RVV - to support persistent client regions
      //we need to actually record this as a destroy on disk, because
      //the GCC RVV doesn't make sense on the client.
      t.region.getRegionMap().removeTombstone(t.entry, t, false, true);
    }
  }
  
  /**
   * For test purposes only, force the expiration of a number of tombstones for
   * replicated regions.
   * @throws InterruptedException
   * @return true if the expiration occurred 
   */
  public boolean forceBatchExpirationForTests(int count) throws InterruptedException {
    this.replicatedTombstoneSweeper.testHook_batchExpired = new CountDownLatch(1);
    try {
      synchronized(this.replicatedTombstoneSweeper) {
        this.replicatedTombstoneSweeper.forceExpirationCount+= count;
        this.replicatedTombstoneSweeper.notifyAll();
      }

      //Wait for 30 seconds. If we wait longer, we risk hanging the tests if
      //something goes wrong.
      return this.replicatedTombstoneSweeper.testHook_batchExpired.await(30, TimeUnit.SECONDS);
    } finally {
      this.replicatedTombstoneSweeper.testHook_batchExpired=null;
    }
  }

  /**
   * Test Hook - slow operation
   * verify whether a tombstone is scheduled for expiration
   */
  public boolean isTombstoneScheduled(LocalRegion r, RegionEntry re) {
    TombstoneSweeper sweeper = this.getSweeper(r);
    VersionSource myId = r.getVersionMember();
    VersionTag entryTag = re.getVersionStamp().asVersionTag();
    int entryVersion = entryTag.getEntryVersion();
    for (Tombstone t: sweeper.getQueue()) {
      if (t.region == r) {
        VersionSource destroyingMember = t.getMemberID();
        if (destroyingMember == null) {
          destroyingMember = myId;
        }
        if (t.region == r
            && t.entry.getKey().equals(re.getKey())
            && t.getEntryVersion() == entryVersion) {
          return true;
        }
      }
    }
    return sweeper.hasExpiredTombstone(r, re, entryTag);
  }

  @Override
  public String toString() {
    return "Destroyed entries GC service.  Replicate Queue=" + this.replicatedTombstoneSweeper.getQueue().toString()
    + " Non-replicate Queue=" + this.nonReplicatedTombstoneSweeper.getQueue().toString()
    + (this.replicatedTombstoneSweeper.expiredTombstones != null?
        " expired batch size = " + this.replicatedTombstoneSweeper.expiredTombstones.size() : "");
  }  
  private static class Tombstone extends CompactVersionHolder {
    // tombstone overhead size
    public static int PER_TOMBSTONE_OVERHEAD = ReflectionSingleObjectSizer.REFERENCE_SIZE // queue's reference to the tombstone
      + ReflectionSingleObjectSizer.REFERENCE_SIZE * 3 // entry, region, member ID
      + ReflectionSingleObjectSizer.REFERENCE_SIZE  // region entry value (Token.TOMBSTONE)
      + 18; // version numbers and timestamp
    
    
    RegionEntry entry;
    LocalRegion region;
    
    Tombstone(RegionEntry entry, LocalRegion region, VersionTag destroyedVersion) {
      super(destroyedVersion);
      this.entry = entry;
      this.region = region;
    }
    
    public int getSize() {
      return Tombstone.PER_TOMBSTONE_OVERHEAD // includes per-entry overhead
        + ObjectSizer.DEFAULT.sizeof(entry.getKey());
    }
    
    @Override
    public String toString() {
      String v = super.toString();
      StringBuilder sb = new StringBuilder();
      sb.append("(").append(entry.getKey()).append("; ")
        .append(region.getName()).append("; ").append(v)
        .append(")");
      return sb.toString();
    }
  }
  
  private static class TombstoneSweeper implements Runnable {
    /**
     * the expiration time for tombstones in this sweeper
     */
    private final long expiryTime;
    /**
     * the current tombstones.  These are queued for expiration.  When tombstones
     * are resurrected they are left in this queue and the sweeper thread
     * figures out that they are no longer valid tombstones.
     */
    private final Queue<Tombstone> tombstones;
    /**
     * The size, in bytes, of the queue
     */
    private final AtomicLong queueSize;
    /**
     * the thread that handles tombstone expiration.  It reads from the
     * tombstone queue.
     */
    private final Thread sweeperThread;
    /**
     * whether this sweeper accumulates expired tombstones for batch removal
     */
    private final boolean batchMode;
    /**
     * The sweeper thread's current tombstone.
     * Only set by the run() thread while holding the currentTombstoneLock.
     * Read by other threads while holding the currentTombstoneLock.
     */
    private Tombstone currentTombstone;
    /**
     * a lock protecting the value of currentTombstone from changing
     */
    private final StoppableReentrantLock currentTombstoneLock;
    /**
     * tombstones that have expired and are awaiting batch removal.  This
     * variable is only accessed by the sweeper thread and so is not guarded
     */
    private Set<Tombstone> expiredTombstones;
    
    /**
     * count of entries to forcibly expire due to memory events
     */
    private long forceExpirationCount = 0;
    
    /**
     * Force batch expiration
     */
    private boolean forceBatchExpiration = false;
    
    /**
     * Is a batch expiration in progress?
     */
    private volatile boolean batchExpirationInProgress;
    
    /**
     * A test hook to force expiration of tombstones.
     * See @{link {@link TombstoneService#forceBatchExpirationForTests(int)}
     */
    private CountDownLatch testHook_batchExpired;

    /**
     * the cache that owns all of the tombstones in this sweeper
     */
    private final GemFireCacheImpl cache;
    
    private volatile boolean isStopped;
    
    TombstoneSweeper(GemFireCacheImpl cache,
        Queue<Tombstone> tombstones,
        long expiryTime,
        boolean batchMode,
        AtomicLong queueSize) {
      this.cache = cache;
      this.expiryTime = expiryTime;
      this.tombstones = tombstones;
      this.queueSize = queueSize;
      this.batchMode = batchMode;
      if (batchMode) {
        this.expiredTombstones = new HashSet<Tombstone>();
      }
      this.currentTombstoneLock = new StoppableReentrantLock(cache.getCancelCriterion());
      this.sweeperThread = new Thread(LoggingThreadGroup.createThreadGroup("Destroyed Entries Processors", logger), this);
      this.sweeperThread.setDaemon(true);
      String product = "GemFire";
      String threadName = product + " Garbage Collection Thread " + (batchMode ? "1" : "2");
      this.sweeperThread.setName(threadName);
    }

  synchronized void start() {
    this.sweeperThread.start();
  }

  synchronized void stop() {
    this.isStopped = true;
    if (this.sweeperThread != null) {
      notifyAll();
    }
    try {
      this.sweeperThread.join(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    getQueue().clear();
  }

    public Tombstone lockAndGetCurrentTombstone() {
      this.currentTombstoneLock.lock();
      return this.currentTombstone;
    }

    public void unlock() {
      this.currentTombstoneLock.unlock();
    }

    public void incQueueSize(long delta) {
      this.queueSize.addAndGet(delta);
    }

    public Queue<Tombstone> getQueue() {
      return this.tombstones;
    }

    /** force a batch GC */
    void forceBatchExpiration() {
      this.forceBatchExpiration = true;
      //this.forceExpirationCount = EXPIRED_TOMBSTONE_LIMIT - this.expiredTombstones.size() + 1;
    }
    
    void scheduleTombstone(Tombstone ts) {
      this.tombstones.add(ts);
      this.queueSize.addAndGet(ts.getSize());
    }
    
    /** if we should GC the batched tombstones, this method will initiate the operation */
    private void processBatch() {
      if (this.forceBatchExpiration 
          || this.expiredTombstones.size() >= EXPIRED_TOMBSTONE_LIMIT
          || testHook_batchExpired != null) {
        this.forceBatchExpiration = false;
        expireBatch();
      }
    }
    
    /** test hook - unsafe since not synchronized */
    boolean hasExpiredTombstone(LocalRegion r, RegionEntry re, VersionTag tag) {
      if (this.expiredTombstones == null) {
        return false;
      }
      int entryVersion = tag.getEntryVersion();
      boolean retry;
      do {
        retry = false;
        try {
          for (Tombstone t: this.expiredTombstones) {
            if (t.region == r
                && t.entry.getKey().equals(re.getKey())
                && t.getEntryVersion() == entryVersion) {
              return true;
            }
          }
        } catch (ConcurrentModificationException e) {
          retry = true;
        }
      } while (retry);
      return false;
    }
    
    
    
    /** expire a batch of tombstones */
    private void expireBatch() {
      // fix for bug #46087 - OOME due to too many GC threads
      if (this.batchExpirationInProgress) {
        // incorrect return due to race between this and waiting-pool GC thread is okay
        // because the sweeper thread will just try again after its next sleep (max sleep is 10 seconds)
        return;
      }
      synchronized(cache.getTombstoneService().blockGCLock) {
        int count = cache.getTombstoneService().getGCBlockCount();
        if (count > 0) {
          // if any delta GII is on going as provider at this member, not to do tombstone GC
          if (logger.isDebugEnabled()) {
            logger.debug("expireBatch skipped due to {} Delta GII on going", count);
          }
          return;
        }

      this.batchExpirationInProgress = true;
      boolean batchScheduled = false;
      try {
        Set<Tombstone> expired = expiredTombstones;
        if (expired.isEmpty()) {
          return;
        }
        expiredTombstones = new HashSet<Tombstone>();
        final Set<DistributedRegion> regionsAffected = new HashSet<DistributedRegion>();
        long removalSize = 0;

        //Update the GC RVV for all of the affected regions.
        //We need to do this so that we can persist the GC RVV before
        //we start removing entries from the map.
        for (Tombstone t: expired) {
          t.region.getVersionVector().recordGCVersion(t.getMemberID(), t.getRegionVersion());
          regionsAffected.add((DistributedRegion)t.region);
        }
        
        for (DistributedRegion r: regionsAffected) {
          //Remove any exceptions from the RVV that are older than the GC version
          r.getVersionVector().pruneOldExceptions();

          //Persist the GC RVV to disk. This needs to happen BEFORE we remove
          //the entries from map, to prevent us from removing a tombstone
          //from disk that has a version greater than the persisted
          //GV RVV.
          if(r.getDataPolicy().withPersistence()) {
            r.getDiskRegion().writeRVVGC(r);
          }
        }

        final Map<LocalRegion, Set<Object>> reapedKeys = new HashMap<LocalRegion, Set<Object>>();
        
        //Remove the tombstones from the in memory region map.
        for (Tombstone t: expired) {
          // for PR buckets we have to keep track of the keys removed because clients have
          // them all lumped in a single non-PR region
          if (t.region.getRegionMap().removeTombstone(t.entry, t, false, true) && t.region.isUsedForPartitionedRegionBucket()) {
            Set<Object> keys = reapedKeys.get(t.region);
            if (keys == null) {
              keys = new HashSet<Object>();
              reapedKeys.put(t.region, keys);
            }
            keys.add(t.entry.getKey());
          }
          removalSize += t.getSize();
        }

        this.queueSize.addAndGet(-removalSize);
        // do messaging in a pool so this thread is not stuck trying to
        // communicate with other members
        cache.getDistributionManager().getWaitingThreadPool().execute(new Runnable() {
          public void run() {
            try {
              // this thread should not reference other sweeper state, which is not synchronized
              for (DistributedRegion r: regionsAffected) {
                r.distributeTombstoneGC(reapedKeys.get(r));
              }
            } finally {
              batchExpirationInProgress = false;
            }
          }
        });
        batchScheduled = true;
      } finally {
        if(testHook_batchExpired != null) {
          testHook_batchExpired.countDown();
        }
        if (!batchScheduled) {
          batchExpirationInProgress = false;
        }
      }
      } // sync on deltaGIILock
    }
    
    /**
     * The run loop picks a tombstone off of the expiration queue and waits
     * for it to expire.  It also periodically scans for resurrected tombstones
     * and handles batch expiration.  Batch expiration works by tossing the
     * expired tombstones into a set and delaying the removal of those tombstones
     * from the Region until scheduled points in the calendar.  
     */
    public void run() {
      long minimumRetentionMs = this.expiryTime / 10; // forceExpiration will not work on something younger than this
      long maximumSleepTime = 10000;
      Tombstone myTombstone = null;
      if (logger.isTraceEnabled(LogMarker.TOMBSTONE)) {
        logger.trace(LogMarker.TOMBSTONE, "Destroyed entries sweeper starting with default sleep interval={}", this.expiryTime);
      }
      // millis we need to run a scan of queue and batch set for resurrected tombstones
      long minimumScanTime = 100;
      // how often to perform the scan
      long scanInterval = Math.min(DEFUNCT_TOMBSTONE_SCAN_INTERVAL, expiryTime);
      long lastScanTime = this.cache.cacheTimeMillis();
      
      while (!isStopped && cache.getCancelCriterion().cancelInProgress() == null) {
        Throwable problem = null;
        try {
          if (this.batchMode) {
            cache.getCachePerfStats().setReplicatedTombstonesSize(queueSize.get());
          } else {
            cache.getCachePerfStats().setNonReplicatedTombstonesSize(queueSize.get());
          }
          SystemFailure.checkFailure();
          long now = this.cache.cacheTimeMillis();
          if (forceExpirationCount <= 0) {
            if (this.batchMode) {
              processBatch();
            }
            // if we're running out of memory we get a little more aggressive about
            // the size of the batch we'll expire
            if (GC_MEMORY_THRESHOLD > 0 && this.batchMode) {
              // check to see how we're doing on memory
              Runtime rt = Runtime.getRuntime();
              long freeMemory = rt.freeMemory();
              long totalMemory = rt.totalMemory();
              long maxMemory = rt.maxMemory();
              freeMemory += (maxMemory-totalMemory);
              if (FORCE_GC_MEMORY_EVENTS ||
                  freeMemory / (totalMemory * 1.0) < GC_MEMORY_THRESHOLD) {
                forceBatchExpiration = !this.batchExpirationInProgress &&
                       this.expiredTombstones.size() > (EXPIRED_TOMBSTONE_LIMIT / 4);
                if (forceBatchExpiration) {
                  if (logger.isDebugEnabled()) {
                    logger.debug("forcing batch expiration due to low memory conditions");
                  }
                }
                // forcing expiration of tombstones that have not timed out can cause inconsistencies
                // too easily
  //              if (this.batchMode) {
  //                forceExpirationCount = EXPIRED_TOMBSTONE_LIMIT - this.expiredTombstones.size();
  //              } else {
  //                forceExpirationCount = EXPIRED_TOMBSTONE_LIMIT;
  //              }
  //              maximumSleepTime = 1000;
              }
            }
          }
          if (myTombstone == null) {
            myTombstone = setCurrentToNextTombstone();
          }
          long sleepTime = 0;
          boolean expireMyTombstone = false;
          if (myTombstone == null) {
            sleepTime = expiryTime;
          } else {
            long msTillMyTombstoneExpires = myTombstone.getVersionTimeStamp() + expiryTime - now;
            if (forceExpirationCount > 0) {
              if (msTillMyTombstoneExpires > 0 && msTillMyTombstoneExpires <= minimumRetentionMs) {
                sleepTime = msTillMyTombstoneExpires;
              } else {
                forceExpirationCount--;
                expireMyTombstone = true;
              }
            } else if (msTillMyTombstoneExpires > 0) {
              sleepTime = msTillMyTombstoneExpires;
            } else {
              expireMyTombstone = true;
            }
          }
          if (expireMyTombstone) {
            try {
              if (batchMode) {
                if (logger.isTraceEnabled(LogMarker.TOMBSTONE)) {
                  logger.trace(LogMarker.TOMBSTONE, "expiring tombstone {}", myTombstone);
                }
                expiredTombstones.add(myTombstone);
              } else {
                if (logger.isTraceEnabled(LogMarker.TOMBSTONE)) {
                  logger.trace(LogMarker.TOMBSTONE, "removing expired tombstone {}", myTombstone);
                }
                queueSize.addAndGet(-myTombstone.getSize());
                myTombstone.region.getRegionMap().removeTombstone(myTombstone.entry, myTombstone, false, true);
              }
              myTombstone = null;
              clearCurrentTombstone();
            } catch (CancelException e) {
              return;
            } catch (Exception e) {
              logger.warn(LocalizedMessage.create(LocalizedStrings.GemFireCacheImpl_TOMBSTONE_ERROR), e);
              myTombstone = null;
              clearCurrentTombstone();
            }
          }
          if (sleepTime > 0) {
            // initial sleeps could be very long, so we reduce the interval to allow
            // this thread to periodically sweep up tombstones for resurrected entries
            sleepTime = Math.min(sleepTime, scanInterval);
            if (sleepTime > minimumScanTime  &&  (now - lastScanTime) > scanInterval) {
              lastScanTime = now;
              long start = now;
              // see if any have been superseded
              for (Iterator<Tombstone> it = getQueue().iterator(); it.hasNext(); ) {
                Tombstone test = it.next();
                if (it.hasNext()) {
                  if (test.region.getRegionMap().isTombstoneNotNeeded(test.entry, test.getEntryVersion())) {
                    it.remove();
                    this.queueSize.addAndGet(-test.getSize());
                    if (test == myTombstone) {
                      myTombstone = null;
                      clearCurrentTombstone();
                      sleepTime = 0;
                    }
                  } else if (batchMode && test != myTombstone && (test.getVersionTimeStamp()+expiryTime) <= now) {
                    it.remove();
                    this.queueSize.addAndGet(-test.getSize());
                    if (logger.isTraceEnabled(LogMarker.TOMBSTONE)) {
                      logger.trace(LogMarker.TOMBSTONE, "expiring tombstone {}", test);
                    }
                    expiredTombstones.add(test);
                    sleepTime = 0;
                  }
                }
              }
              // now check the batch of timed-out tombstones, if there is one
              if (batchMode) {
                for (Iterator<Tombstone> it = expiredTombstones.iterator(); it.hasNext(); ) {
                  Tombstone test = it.next();
                  if (test.region.getRegionMap().isTombstoneNotNeeded(test.entry, test.getEntryVersion())) {
                    if (logger.isTraceEnabled(LogMarker.TOMBSTONE)) {
                      logger.trace(LogMarker.TOMBSTONE, "removing obsolete tombstone: {}", test);
                    }
                    it.remove();
                    this.queueSize.addAndGet(-test.getSize());
                    if (test == myTombstone) {
                      myTombstone = null;
                      clearCurrentTombstone();
                      sleepTime = 0;
                    }
                  }
                }
              }
              if (sleepTime > 0) {
                long elapsed = this.cache.cacheTimeMillis() - start;
                sleepTime = sleepTime - elapsed;
                if (sleepTime <= 0) {
                  minimumScanTime = elapsed;
                  continue;
                }
              }
            }
            // test hook:  if there are expired tombstones and nothing else is expiring soon,
            // perform distributed tombstone GC
            if (batchMode && IDLE_EXPIRATION && sleepTime >= expiryTime) {
              if (this.expiredTombstones.size() > 0) {
                expireBatch();
              }
            }
            if (sleepTime > 0) {
              try {
                sleepTime = Math.min(sleepTime, maximumSleepTime);
                if (logger.isTraceEnabled(LogMarker.TOMBSTONE)) {
                  logger.trace(LogMarker.TOMBSTONE, "sleeping for {}", sleepTime);
                }
                synchronized(this) {
                  if(isStopped) {
                    return;
                  }
                  this.wait(sleepTime);
                }
              } catch (InterruptedException e) {
                return;
              }
            }
          } // sleepTime > 0
        } catch (CancelException e) {
          break;
        } catch (VirtualMachineError err) { // GemStoneAddition
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable e) {
          SystemFailure.checkFailure();
          problem = e;
        }
        if (problem != null) {
          logger.fatal(LocalizedMessage.create(LocalizedStrings.TombstoneService_UNEXPECTED_EXCEPTION), problem);
        }
      } // while()
    } // run()

    private void clearCurrentTombstone() {
      currentTombstoneLock.lock();
      currentTombstone = null;
      currentTombstoneLock.unlock();
    }

    /**
     * Returns the new currentTombstone taken from the tombstones queue; null if no next tombstone
     */
    private Tombstone setCurrentToNextTombstone() {
      Tombstone result;
      currentTombstoneLock.lock();
      try {
        result = tombstones.poll();
        if (result != null) {
          if (logger.isTraceEnabled(LogMarker.TOMBSTONE)) {
            logger.trace(LogMarker.TOMBSTONE, "current tombstone is {}", result);
          }
          currentTombstone = result;
        } else {
          if (logger.isTraceEnabled(LogMarker.TOMBSTONE)) {
            logger.trace(LogMarker.TOMBSTONE, "queue is empty - will sleep");
          }
          forceExpirationCount = 0;
        }
      } finally {
        currentTombstoneLock.unlock();
      }
      return result;
    }
    
  } // class TombstoneSweeper

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.control.ResourceListener#onEvent(java.lang.Object)
   */
  @Override
  public void onEvent(MemoryEvent event) {
    if (event.isLocal()) {
      if (event.getState().isEviction() && !event.getPreviousState().isEviction()) {
        this.replicatedTombstoneSweeper.forceBatchExpiration();
      }
    }
  }


}
