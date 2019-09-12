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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.distributed.internal.CacheTime;
import org.apache.geode.internal.cache.versions.CompactVersionHolder;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThread;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.size.ReflectionSingleObjectSizer;
import org.apache.geode.internal.util.concurrent.StoppableReentrantLock;

/**
 * Tombstones are region entries that have been destroyed but are held for future concurrency
 * checks. They are timed out after a reasonable period of time when there is no longer the
 * possibility of concurrent modification conflicts.
 * <p>
 * The cache holds a tombstone service that is responsible for tracking and timing out tombstones.
 */
public class TombstoneService {
  private static final Logger logger = LogService.getLogger();

  @VisibleForTesting
  public static final long REPLICATE_TOMBSTONE_TIMEOUT_DEFAULT =
      Long.getLong(GEMFIRE_PREFIX + "tombstone-timeout", 600000L);

  /**
   * The default tombstone expiration period, in milliseconds for replicates and partitions.
   * <p>
   * This is the period over which the destroy operation may conflict with another operation. After
   * this timeout elapses the tombstone is put into a GC set for removal. Removal is typically
   * triggered by the size of the GC set, but could be influenced by resource managers.
   *
   * The default is 600,000 milliseconds (10 minutes).
   */
  @MutableForTesting
  public static long REPLICATE_TOMBSTONE_TIMEOUT = REPLICATE_TOMBSTONE_TIMEOUT_DEFAULT;

  @VisibleForTesting
  public static final long NON_REPLICATE_TOMBSTONE_TIMEOUT_DEFAULT =
      Long.getLong(GEMFIRE_PREFIX + "non-replicated-tombstone-timeout", 480000);

  /**
   * The default tombstone expiration period in millis for non-replicate/partition regions. This
   * tombstone timeout should be shorter than the one for replicated regions and need not be
   * excessively long. Making it longer than the replicated timeout can cause non-replicated regions
   * to issue revisions based on the tombstone that could overwrite modifications made by others
   * that no longer have the tombstone.
   * <p>
   * The default is 480,000 milliseconds (8 minutes)
   */
  @MutableForTesting
  public static long NON_REPLICATE_TOMBSTONE_TIMEOUT = NON_REPLICATE_TOMBSTONE_TIMEOUT_DEFAULT;

  @VisibleForTesting
  public static final int EXPIRED_TOMBSTONE_LIMIT_DEFAULT =
      Integer.getInteger(GEMFIRE_PREFIX + "tombstone-gc-threshold", 100000);

  /**
   * The max number of tombstones in an expired batch. This covers all replicated regions, including
   * PR buckets. The default is 100,000 expired tombstones.
   */
  @MutableForTesting
  public static int EXPIRED_TOMBSTONE_LIMIT = EXPIRED_TOMBSTONE_LIMIT_DEFAULT;

  @VisibleForTesting
  public static final long DEFUNCT_TOMBSTONE_SCAN_INTERVAL_DEFAULT =
      Long.getLong(GEMFIRE_PREFIX + "tombstone-scan-interval", 60000);

  /**
   * The interval to scan for expired tombstones in the queues
   */
  public static final long DEFUNCT_TOMBSTONE_SCAN_INTERVAL =
      DEFUNCT_TOMBSTONE_SCAN_INTERVAL_DEFAULT;

  @VisibleForTesting
  public static final double GC_MEMORY_THRESHOLD_DEFAULT =
      Integer.getInteger(GEMFIRE_PREFIX + "tombstone-gc-memory-threshold", 30) * 0.01;

  /**
   * The threshold percentage of free max memory that will trigger tombstone GCs. The default
   * percentage is somewhat less than the LRU Heap evictor so that we evict tombstones before we
   * start evicting cache data.
   */
  @MutableForTesting
  public static double GC_MEMORY_THRESHOLD = GC_MEMORY_THRESHOLD_DEFAULT;

  @VisibleForTesting
  public static final boolean FORCE_GC_MEMORY_EVENTS_DEFAULT = false;

  /** this is a test hook for causing the tombstone service to act as though free memory is low */
  @MutableForTesting
  public static boolean FORCE_GC_MEMORY_EVENTS = FORCE_GC_MEMORY_EVENTS_DEFAULT;

  @VisibleForTesting
  public static final long MAX_SLEEP_TIME_DEFAULT = 10000;

  /** maximum time a sweeper will sleep, in milliseconds. */
  @MutableForTesting
  public static long MAX_SLEEP_TIME = MAX_SLEEP_TIME_DEFAULT;

  @VisibleForTesting
  public static final boolean IDLE_EXPIRATION_DEFAULT = false;

  /** dunit test hook for forced batch expiration */
  @MutableForTesting
  public static boolean IDLE_EXPIRATION = IDLE_EXPIRATION_DEFAULT;

  /**
   * two sweepers, one for replicated regions (including PR buckets) and one for other regions. They
   * have different timeout intervals.
   */
  private final ReplicateTombstoneSweeper replicatedTombstoneSweeper;
  private final NonReplicateTombstoneSweeper nonReplicatedTombstoneSweeper;

  public static TombstoneService initialize(InternalCache cache) {
    return new TombstoneService(cache);
  }

  private TombstoneService(InternalCache cache) {
    this.replicatedTombstoneSweeper =
        new ReplicateTombstoneSweeper(cache, cache.getCachePerfStats(), cache.getCancelCriterion(),
            cache.getDistributionManager().getExecutors().getWaitingThreadPool());
    this.nonReplicatedTombstoneSweeper = new NonReplicateTombstoneSweeper(cache,
        cache.getCachePerfStats(), cache.getCancelCriterion());
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
   * Tombstones are markers placed in destroyed entries in order to keep the entry around for a
   * while so that it's available for concurrent modification detection.
   *
   * @param r the region holding the entry
   * @param entry the region entry that holds the tombstone
   * @param destroyedVersion the version that was destroyed
   */
  public void scheduleTombstone(LocalRegion r, RegionEntry entry, VersionTag destroyedVersion) {
    if (entry.getVersionStamp() == null) {
      logger.warn(
          "Detected an attempt to schedule a tombstone for an entry that is not versioned in region "
              + r.getFullPath(),
          new Exception("stack trace"));
      return;
    }
    Tombstone ts = new Tombstone(entry, r, destroyedVersion);
    this.getSweeper(r).scheduleTombstone(ts);
  }


  private TombstoneSweeper getSweeper(LocalRegion r) {
    if (r.getScope().isDistributed() && r.getServerProxy() == null
        && r.getDataPolicy().withReplication()) {
      return this.replicatedTombstoneSweeper;
    } else {
      return this.nonReplicatedTombstoneSweeper;
    }
  }

  /**
   * remove all tombstones for the given region. Do this when the region is cleared or destroyed.
   */
  public void unscheduleTombstones(LocalRegion r) {
    getSweeper(r).unscheduleTombstones(r);
  }

  public int getGCBlockCount() {
    return replicatedTombstoneSweeper.getGCBlockCount();
  }

  public int incrementGCBlockCount() {
    return replicatedTombstoneSweeper.incrementGCBlockCount();
  }

  public int decrementGCBlockCount() {
    return replicatedTombstoneSweeper.decrementGCBlockCount();
  }

  public long getScheduledTombstoneCount() {
    long result = 0;
    result += replicatedTombstoneSweeper.getScheduledTombstoneCount();
    result += nonReplicatedTombstoneSweeper.getScheduledTombstoneCount();
    return result;
  }

  /**
   * remove tombstones from the given region that have region-versions <= those in the given removal
   * map
   *
   * @return a collection of keys removed (only if the region is a bucket - empty otherwise)
   */
  @SuppressWarnings("rawtypes")
  public Set<Object> gcTombstones(LocalRegion r, Map<VersionSource, Long> regionGCVersions,
      boolean needsKeys) {
    synchronized (getBlockGCLock()) {
      int count = getGCBlockCount();
      if (count > 0) {
        // if any delta GII is on going as provider at this member, not to do tombstone GC
        if (logger.isDebugEnabled()) {
          logger.debug("gcTombstones skipped due to {} Delta GII on going", count);
        }
        return null;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("gcTombstones invoked for region {} and version map {}", r, regionGCVersions);
      }
      final VersionSource myId = r.getVersionMember();
      final TombstoneSweeper sweeper = getSweeper(r);
      final List<Tombstone> removals = new ArrayList<Tombstone>();
      sweeper.removeUnexpiredIf(t -> {
        if (t.region == r) {
          VersionSource destroyingMember = t.getMemberID();
          if (destroyingMember == null) {
            destroyingMember = myId;
          }
          Long maxReclaimedRV = regionGCVersions.get(destroyingMember);
          if (maxReclaimedRV != null && t.getRegionVersion() <= maxReclaimedRV) {
            removals.add(t);
            return true;
          }
        }
        return false;
      });

      // Record the GC versions now, so that we can persist them
      for (Map.Entry<VersionSource, Long> entry : regionGCVersions.entrySet()) {
        r.getVersionVector().recordGCVersion(entry.getKey(), entry.getValue());
      }

      // Remove any exceptions from the RVV that are older than the GC version
      r.getVersionVector().pruneOldExceptions();

      // Persist the GC RVV to disk. This needs to happen BEFORE we remove
      // the entries from map, to prevent us from removing a tombstone
      // from disk that has a version greater than the persisted
      // GV RVV.
      if (r.getDataPolicy().withPersistence()) {
        // Update the version vector which reflects what has been persisted on disk.
        r.getDiskRegion().writeRVVGC(r);
      }

      Set<Object> removedKeys = needsKeys ? new HashSet<Object>() : Collections.emptySet();
      for (Tombstone t : removals) {
        boolean tombstoneWasStillInRegionMap =
            t.region.getRegionMap().removeTombstone(t.entry, t, false, true);
        if (needsKeys && tombstoneWasStillInRegionMap) {
          removedKeys.add(t.entry.getKey());
        }
      }
      return removedKeys;
    } // sync on deltaGIILock
  }

  /**
   * client tombstone removal is key-based if the server is a PR. This is due to the server having
   * separate version vectors for each bucket. In the client this causes the version vector to make
   * no sense, so we have to send it a collection of the keys removed on the server and then we
   * brute-force remove any of them that are tombstones on the client
   *
   * @param r the region affected
   * @param tombstoneKeys the keys removed on the server
   */
  public void gcTombstoneKeys(final LocalRegion r, final Set<Object> tombstoneKeys) {
    if (r.getServerProxy() == null) {
      // if the region does not have a server proxy
      // then it will not have any tombstones to gc for the server.
      return;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("gcTombstoneKeys invoked for region {} and keys {}", r, tombstoneKeys);
    }
    final TombstoneSweeper sweeper = this.getSweeper(r);
    final List<Tombstone> removals = new ArrayList<Tombstone>(tombstoneKeys.size());
    sweeper.removeUnexpiredIf(t -> {
      if (t.region == r) {
        if (tombstoneKeys.contains(t.entry.getKey())) {
          removals.add(t);
          return true;
        }
      }
      return false;
    });

    for (Tombstone t : removals) {
      // TODO - RVV - to support persistent client regions
      // we need to actually record this as a destroy on disk, because
      // the GCC RVV doesn't make sense on the client.
      t.region.getRegionMap().removeTombstone(t.entry, t, false, true);
    }
  }

  /**
   * For test purposes only, force the expiration of a number of tombstones for replicated regions.
   *
   * @param count Number of tombstones to expire
   *
   * @return true if the expiration occurred
   */
  public boolean forceBatchExpirationForTests(int count) throws InterruptedException {
    return this.replicatedTombstoneSweeper.testHook_forceExpiredTombstoneGC(count, 30, SECONDS);
  }

  /**
   * For test purposes only, force the expiration of a number of tombstones for replicated regions.
   *
   * @param count Number of tombstones to expire
   * @param timeout the maximum time to wait
   * @param unit the time unit of the {@code timeout} argument
   *
   * @return true if the expiration occurred
   */
  public boolean forceBatchExpirationForTests(int count, long timeout, TimeUnit unit)
      throws InterruptedException {
    return this.replicatedTombstoneSweeper.testHook_forceExpiredTombstoneGC(count, timeout, unit);
  }

  @Override
  public String toString() {
    return "Destroyed entries GC service.  Replicate Queue=" + this.replicatedTombstoneSweeper
        + " Non-replicate Queue=" + this.nonReplicatedTombstoneSweeper;
  }

  public Object getBlockGCLock() {
    return this.replicatedTombstoneSweeper.getBlockGCLock();
  }

  private static class Tombstone extends CompactVersionHolder {
    // tombstone overhead size
    public static final int PER_TOMBSTONE_OVERHEAD =
        ReflectionSingleObjectSizer.REFERENCE_SIZE // queue's reference to the tombstone
            + ReflectionSingleObjectSizer.REFERENCE_SIZE * 3 // entry, region, member ID
            + ReflectionSingleObjectSizer.REFERENCE_SIZE // region entry value (Token.TOMBSTONE)
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
      sb.append("(").append(entry.getKey()).append("; ").append(region.getName()).append("; ")
          .append(v).append(")");
      return sb.toString();
    }
  }
  private static class NonReplicateTombstoneSweeper extends TombstoneSweeper {
    NonReplicateTombstoneSweeper(CacheTime cacheTime, CachePerfStats stats,
        CancelCriterion cancelCriterion) {
      super(cacheTime, stats, cancelCriterion, NON_REPLICATE_TOMBSTONE_TIMEOUT,
          "Non-replicate Region Garbage Collector");
    }

    @Override
    protected boolean removeExpiredIf(Predicate<Tombstone> predicate) {
      return false;
    }

    @Override
    protected void updateStatistics() {
      stats.setNonReplicatedTombstonesSize(getMemoryEstimate());
    }

    @Override
    protected boolean hasExpired(long msTillHeadTombstoneExpires) {
      return msTillHeadTombstoneExpires <= 0;
    }

    @Override
    protected void expireTombstone(Tombstone tombstone) {
      if (logger.isTraceEnabled(LogMarker.TOMBSTONE_VERBOSE)) {
        logger.trace(LogMarker.TOMBSTONE_VERBOSE, "removing expired tombstone {}", tombstone);
      }
      updateMemoryEstimate(-tombstone.getSize());
      tombstone.region.getRegionMap().removeTombstone(tombstone.entry, tombstone, false, true);
    }

    @Override
    protected void checkExpiredTombstoneGC() {}

    @Override
    protected void handleNoUnexpiredTombstones() {}

    @Override
    boolean testHook_forceExpiredTombstoneGC(int count, long timeout, TimeUnit unit)
        throws InterruptedException {
      return true;
    }

    @Override
    protected void beforeSleepChecks() {}
  }

  private static class ReplicateTombstoneSweeper extends TombstoneSweeper {
    /**
     * Used to execute batch gc message execution in the background.
     */
    private final ExecutorService executor;
    /**
     * tombstones that have expired and are awaiting batch removal.
     */
    private final List<Tombstone> expiredTombstones;
    private final Object expiredTombstonesLock = new Object();

    /**
     * Force batch expiration
     */
    private boolean forceBatchExpiration = false;

    /**
     * Is a batch expiration in progress? Part of expireBatch is done in a background thread and
     * until that completes batch expiration is in progress.
     */
    private volatile boolean batchExpirationInProgress;

    private final Object blockGCLock = new Object();
    private int progressingDeltaGIICount;

    /**
     * A test hook to force a call to expireBatch. The call will only happen after
     * testHook_forceExpirationCount goes to zero. This latch is counted down at the end of
     * expireBatch. See @{link {@link TombstoneService#forceBatchExpirationForTests(int)}
     */
    private CountDownLatch testHook_forceBatchExpireCall;
    /**
     * count of tombstones to forcibly expire
     */
    private int testHook_forceExpirationCount = 0;

    ReplicateTombstoneSweeper(CacheTime cacheTime, CachePerfStats stats,
        CancelCriterion cancelCriterion, ExecutorService executor) {
      super(cacheTime, stats, cancelCriterion, REPLICATE_TOMBSTONE_TIMEOUT,
          "Replicate/Partition Region Garbage Collector");
      this.expiredTombstones = new ArrayList<Tombstone>();
      this.executor = executor;
    }

    public int decrementGCBlockCount() {
      synchronized (getBlockGCLock()) {
        return --progressingDeltaGIICount;
      }
    }

    public int incrementGCBlockCount() {
      synchronized (getBlockGCLock()) {
        return ++progressingDeltaGIICount;
      }
    }

    public int getGCBlockCount() {
      synchronized (getBlockGCLock()) {
        return progressingDeltaGIICount;
      }
    }

    public Object getBlockGCLock() {
      return blockGCLock;
    }

    @Override
    protected boolean removeExpiredIf(Predicate<Tombstone> predicate) {
      boolean result = false;
      long removalSize = 0;
      synchronized (expiredTombstonesLock) {
        // Iterate in reverse order to optimize lots of removes.
        // Since expiredTombstones is an ArrayList removing from
        // low indexes requires moving everything at a higher index down.
        for (int idx = expiredTombstones.size() - 1; idx >= 0; idx--) {
          Tombstone t = expiredTombstones.get(idx);
          if (predicate.test(t)) {
            removalSize += t.getSize();
            expiredTombstones.remove(idx);
            result = true;
          }
        }
      }
      updateMemoryEstimate(-removalSize);
      return result;
    }

    /** expire a batch of tombstones */
    private void expireBatch() {
      // fix for bug #46087 - OOME due to too many GC threads
      if (this.batchExpirationInProgress) {
        // incorrect return due to race between this and waiting-pool GC thread is okay
        // because the sweeper thread will just try again after its next sleep (max sleep is 10
        // seconds)
        return;
      }
      synchronized (getBlockGCLock()) {
        int count = getGCBlockCount();
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

          // TODO seems like no need for the value of this map to be a Set.
          // It could instead be a List, which would be nice because the per entry
          // memory overhead for a set is much higher than an ArrayList
          // BUT we send it to clients and the old
          // version of them expects it to be a Set.
          final Map<DistributedRegion, Set<Object>> reapedKeys = new HashMap<>();

          // Update the GC RVV for all of the affected regions.
          // We need to do this so that we can persist the GC RVV before
          // we start removing entries from the map.
          synchronized (expiredTombstonesLock) {
            for (Tombstone t : expiredTombstones) {
              DistributedRegion tr = (DistributedRegion) t.region;
              tr.getVersionVector().recordGCVersion(t.getMemberID(), t.getRegionVersion());
              if (!reapedKeys.containsKey(tr)) {
                reapedKeys.put(tr, Collections.emptySet());
              }
            }
          }

          for (DistributedRegion r : reapedKeys.keySet()) {
            // Remove any exceptions from the RVV that are older than the GC version
            r.getVersionVector().pruneOldExceptions();

            // Persist the GC RVV to disk. This needs to happen BEFORE we remove
            // the entries from map, to prevent us from removing a tombstone
            // from disk that has a version greater than the persisted
            // GV RVV.
            if (r.getDataPolicy().withPersistence()) {
              r.getDiskRegion().writeRVVGC(r);
            }
          }

          // Remove the tombstones from the in memory region map.
          removeExpiredIf(t -> {
            // for PR buckets we have to keep track of the keys removed because clients have
            // them all lumped in a single non-PR region
            DistributedRegion tr = (DistributedRegion) t.region;
            boolean tombstoneWasStillInRegionMap =
                tr.getRegionMap().removeTombstone(t.entry, t, false, true);
            if (tombstoneWasStillInRegionMap && hasToTrackKeysForClients(tr)) {
              Set<Object> keys = reapedKeys.get(tr);
              if (keys.isEmpty()) {
                keys = new HashSet<Object>();
                reapedKeys.put(tr, keys);
              }
              keys.add(t.entry.getKey());
            }
            return true;
          });

          // do messaging in a pool so this thread is not stuck trying to
          // communicate with other members
          executor.execute(new Runnable() {
            @Override
            public void run() {
              try {
                // this thread should not reference other sweeper state, which is not synchronized
                for (Map.Entry<DistributedRegion, Set<Object>> mapEntry : reapedKeys.entrySet()) {
                  DistributedRegion r = mapEntry.getKey();
                  Set<Object> rKeysReaped = mapEntry.getValue();
                  r.distributeTombstoneGC(rKeysReaped);
                }
              } finally {
                batchExpirationInProgress = false;
              }
            }
          });
          batchScheduled = true;
        } finally {
          if (testHook_forceBatchExpireCall != null) {
            testHook_forceBatchExpireCall.countDown();
          }
          if (!batchScheduled) {
            batchExpirationInProgress = false;
          }
        }
      } // sync on deltaGIILock
    }

    /**
     * Returns true if keys needs to be tracked for clients registering interests on PR.
     */
    private boolean hasToTrackKeysForClients(DistributedRegion r) {
      return r.isUsedForPartitionedRegionBucket()
          && ((r.getFilterProfile() != null && r.getFilterProfile().hasInterest())
              || r.getPartitionedRegion().getRegionAdvisor().hasPRServerWithInterest());
    }

    @Override
    protected void checkExpiredTombstoneGC() {
      if (shouldCallExpireBatch()) {
        this.forceBatchExpiration = false;
        expireBatch();
      }
      checkIfBatchExpirationShouldBeForced();
    }

    private boolean shouldCallExpireBatch() {
      if (testHook_forceExpirationCount > 0) {
        return false;
      }
      if (forceBatchExpiration) {
        return true;
      }
      if (testHook_forceBatchExpireCall != null) {
        return true;
      }
      if (expiredTombstones.size() >= EXPIRED_TOMBSTONE_LIMIT) {
        return true;
      }
      return false;
    }

    private void testHookIfIdleExpireBatch() {
      if (IDLE_EXPIRATION && sleepTime >= EXPIRY_TIME && !this.expiredTombstones.isEmpty()) {
        expireBatch();
      }
    }

    @Override
    protected void updateStatistics() {
      stats.setReplicatedTombstonesSize(getMemoryEstimate());
    }

    private void checkIfBatchExpirationShouldBeForced() {
      if (testHook_forceExpirationCount > 0) {
        return;
      }
      if (GC_MEMORY_THRESHOLD <= 0.0) {
        return;
      }
      if (this.batchExpirationInProgress) {
        return;
      }
      if (this.expiredTombstones.size() <= (EXPIRED_TOMBSTONE_LIMIT / 4)) {
        return;
      }
      if (FORCE_GC_MEMORY_EVENTS || isFreeMemoryLow()) {
        forceBatchExpiration = true;
        if (logger.isDebugEnabled()) {
          logger.debug("forcing batch expiration due to low memory conditions");
        }
      }
    }

    private boolean isFreeMemoryLow() {
      Runtime rt = Runtime.getRuntime();
      long unusedMemory = rt.freeMemory(); // "free" is how much space we have allocated that is
                                           // currently not used
      long totalMemory = rt.totalMemory(); // "total" is how much space we have allocated
      long maxMemory = rt.maxMemory(); // "max" is how much space we can allocate
      unusedMemory += (maxMemory - totalMemory); // "max-total" is how much space we have that has
                                                 // not yet been allocated
      return unusedMemory / (totalMemory * 1.0) < GC_MEMORY_THRESHOLD;
    }

    @Override
    protected boolean hasExpired(long msTillHeadTombstoneExpires) {
      if (testHook_forceExpirationCount > 0) {
        testHook_forceExpirationCount--;
        return true;
      }
      return msTillHeadTombstoneExpires <= 0;
    }

    @Override
    protected void expireTombstone(Tombstone tombstone) {
      if (logger.isTraceEnabled(LogMarker.TOMBSTONE_VERBOSE)) {
        logger.trace(LogMarker.TOMBSTONE_VERBOSE, "adding expired tombstone {} to batch",
            tombstone);
      }
      synchronized (expiredTombstonesLock) {
        expiredTombstones.add(tombstone);
      }
    }

    @Override
    protected void handleNoUnexpiredTombstones() {
      testHook_forceExpirationCount = 0;
    }

    @Override
    public String toString() {
      return super.toString() + " batchedExpiredTombstones[" + expiredTombstones.size() + "] = "
          + expiredTombstones.toString();
    }

    @Override
    boolean testHook_forceExpiredTombstoneGC(int count, long timeout, TimeUnit unit)
        throws InterruptedException {
      // sync on blockGCLock since expireBatch syncs on it
      synchronized (getBlockGCLock()) {
        testHook_forceBatchExpireCall = new CountDownLatch(1);
      }
      try {
        synchronized (this) {
          testHook_forceExpirationCount += count;
          notifyAll();
        }
        return testHook_forceBatchExpireCall.await(timeout, unit);
      } finally {
        testHook_forceBatchExpireCall = null;
      }
    }

    @Override
    protected void beforeSleepChecks() {
      testHookIfIdleExpireBatch();
    }

    @Override
    public long getScheduledTombstoneCount() {
      return super.getScheduledTombstoneCount() + this.expiredTombstones.size();
    }
  }

  private abstract static class TombstoneSweeper implements Runnable {
    /**
     * the expiration time for tombstones in this sweeper
     */
    protected final long EXPIRY_TIME;
    /**
     * The minimum amount of elapsed time, in millis, between purges.
     */
    private final long PURGE_INTERVAL;
    /**
     * How long the sweeper should sleep.
     */
    protected long sleepTime;
    /**
     * Estimate of how long, in millis, it will take to do a purge of obsolete tombstones.
     */
    private long minimumPurgeTime = 1;
    /**
     * Timestamp of when the last purge was done.
     */
    private long lastPurgeTimestamp;
    /**
     * the current tombstones. These are queued for expiration. When tombstones are resurrected they
     * are left in this queue and the sweeper thread figures out that they are no longer valid
     * tombstones.
     */
    private final Queue<Tombstone> tombstones;
    /**
     * Estimate of the amount of memory used by this sweeper
     */
    private final AtomicLong memoryUsedEstimate;
    /**
     * the thread that handles tombstone expiration.
     */
    private final Thread sweeperThread;
    /**
     * A lock protecting the head of the tombstones queue. Operations that may remove the head need
     * to hold this lock.
     */
    private final StoppableReentrantLock queueHeadLock;


    protected final CacheTime cacheTime;
    protected final CachePerfStats stats;
    private final CancelCriterion cancelCriterion;

    private volatile boolean isStopped;

    TombstoneSweeper(CacheTime cacheTime, CachePerfStats stats, CancelCriterion cancelCriterion,
        long expiryTime, String threadName) {
      this.cacheTime = cacheTime;
      this.stats = stats;
      this.cancelCriterion = cancelCriterion;
      this.EXPIRY_TIME = expiryTime;
      this.PURGE_INTERVAL = Math.min(DEFUNCT_TOMBSTONE_SCAN_INTERVAL, expiryTime);
      this.tombstones = new ConcurrentLinkedQueue<Tombstone>();
      this.memoryUsedEstimate = new AtomicLong();
      this.queueHeadLock = new StoppableReentrantLock(cancelCriterion);
      this.sweeperThread = new LoggingThread(threadName, this);
      this.lastPurgeTimestamp = getNow();
    }

    public void unscheduleTombstones(final LocalRegion r) {
      this.removeIf(t -> {
        if (t.region == r) {
          return true;
        }
        return false;
      });
    }

    /**
     * For each unexpired tombstone this sweeper knows about call the predicate. If the predicate
     * returns true then remove the tombstone from any storage and update the memory estimate.
     *
     * @return true if predicate ever returned true
     */
    private boolean removeUnexpiredIf(Predicate<Tombstone> predicate) {
      boolean result = false;
      long removalSize = 0;
      lockQueueHead();
      try {
        for (Iterator<Tombstone> it = getQueue().iterator(); it.hasNext();) {
          Tombstone t = it.next();
          if (predicate.test(t)) {
            removalSize += t.getSize();
            it.remove();
            result = true;
          }
        }
      } finally {
        unlockQueueHead();
      }
      updateMemoryEstimate(-removalSize);
      return result;
    }

    /**
     * For all tombstone this sweeper knows about call the predicate. If the predicate returns true
     * then remove the tombstone from any storage and update the memory estimate.
     *
     * @return true if predicate ever returned true
     */
    private boolean removeIf(Predicate<Tombstone> predicate) {
      return removeUnexpiredIf(predicate) || removeExpiredIf(predicate);
    }

    synchronized void start() {
      this.sweeperThread.start();
    }

    void stop() {
      synchronized (this) {
        this.isStopped = true;
        notifyAll();
      }
      try {
        this.sweeperThread.join(100);
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
    }

    private void lockQueueHead() {
      this.queueHeadLock.lock();
    }

    private void unlockQueueHead() {
      this.queueHeadLock.unlock();
    }

    public long getMemoryEstimate() {
      return this.memoryUsedEstimate.get();
    }

    public void updateMemoryEstimate(long delta) {
      this.memoryUsedEstimate.addAndGet(delta);
    }

    protected Queue<Tombstone> getQueue() {
      return this.tombstones;
    }

    void scheduleTombstone(Tombstone ts) {
      this.tombstones.add(ts);
      updateMemoryEstimate(ts.getSize());
    }

    @Override
    public void run() {
      if (logger.isTraceEnabled(LogMarker.TOMBSTONE_VERBOSE)) {
        logger.trace(LogMarker.TOMBSTONE_VERBOSE,
            "Destroyed entries sweeper starting with sleep interval of {} milliseconds",
            EXPIRY_TIME);
      }
      while (!isStopped && !cancelCriterion.isCancelInProgress()) {
        try {
          updateStatistics();
          SystemFailure.checkFailure();
          final long now = getNow();
          checkExpiredTombstoneGC();
          checkOldestUnexpired(now);
          purgeObsoleteTombstones(now);
          doSleep();
        } catch (CancelException ignore) {
          break;
        } catch (VirtualMachineError err) { // GemStoneAddition
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable e) {
          SystemFailure.checkFailure();
          logger.fatal("GemFire garbage collection service encountered an unexpected exception",
              e);
        }
      } // while()
    } // run()

    private long getNow() {
      return cacheTime.cacheTimeMillis();
    }

    private void doSleep() {
      if (sleepTime <= 0) {
        return;
      }
      beforeSleepChecks();
      sleepTime = Math.min(sleepTime, MAX_SLEEP_TIME);
      if (logger.isTraceEnabled(LogMarker.TOMBSTONE_VERBOSE)) {
        logger.trace(LogMarker.TOMBSTONE_VERBOSE, "sleeping for {}", sleepTime);
      }
      synchronized (this) {
        if (isStopped) {
          return;
        }
        try {
          this.wait(sleepTime);
        } catch (InterruptedException ignore) {
        }
      }
    }

    private void purgeObsoleteTombstones(final long now) {
      if (minimumPurgeTime > sleepTime) {
        // the purge might take minimumScanTime
        // and we have something to do sooner
        // than that so return
        return;
      }
      if ((now - lastPurgeTimestamp) < PURGE_INTERVAL) {
        // the time since the last purge
        // is less than the configured interval
        // so return
        return;
      }
      lastPurgeTimestamp = now;
      // see if any have been superseded
      boolean removedObsoleteTombstone = removeIf(tombstone -> {
        if (tombstone.region.getRegionMap().isTombstoneNotNeeded(tombstone.entry,
            tombstone.getEntryVersion())) {
          if (logger.isTraceEnabled(LogMarker.TOMBSTONE_VERBOSE)) {
            logger.trace(LogMarker.TOMBSTONE_VERBOSE, "removing obsolete tombstone: {}", tombstone);
          }
          return true;
        }
        return false;
      });
      if (removedObsoleteTombstone) {
        sleepTime = 0;
      } else {
        long elapsed = getNow() - now;
        sleepTime -= elapsed;
        if (sleepTime <= 0) {
          minimumPurgeTime = elapsed;
        }
      }
    }

    /**
     * See if the oldest unexpired tombstone should be expired.
     */
    private void checkOldestUnexpired(long now) {
      sleepTime = 0;
      lockQueueHead();
      Tombstone oldest = tombstones.peek();
      try {
        if (oldest == null) {
          if (logger.isTraceEnabled(LogMarker.TOMBSTONE_VERBOSE)) {
            logger.trace(LogMarker.TOMBSTONE_VERBOSE, "queue is empty - will sleep");
          }
          handleNoUnexpiredTombstones();
          sleepTime = EXPIRY_TIME;
        } else {
          if (logger.isTraceEnabled(LogMarker.TOMBSTONE_VERBOSE)) {
            logger.trace(LogMarker.TOMBSTONE_VERBOSE, "oldest unexpired tombstone is {}", oldest);
          }
          long msTillHeadTombstoneExpires = oldest.getVersionTimeStamp() + EXPIRY_TIME - now;
          if (hasExpired(msTillHeadTombstoneExpires)) {
            try {
              tombstones.remove();
              expireTombstone(oldest);
            } catch (CancelException ignore) {
              // nothing needed
            } catch (Exception e) {
              logger.warn("Unexpected exception while processing tombstones", e);
            }
          } else {
            sleepTime = msTillHeadTombstoneExpires;
          }
        }
      } finally {
        unlockQueueHead();
      }
    }

    public long getScheduledTombstoneCount() {
      return getQueue().size();
    }

    @Override
    public String toString() {
      return "[" + getQueue().size() + "] " + getQueue().toString();
    }

    /**
     * For each expired tombstone this sweeper knows about call the predicate. If the predicate
     * returns true then remove the tombstone from any storage and update the memory estimate.
     * <p>
     * Some sweepers batch up the expired tombstones to gc them later.
     *
     * @return true if predicate ever returned true
     */
    protected abstract boolean removeExpiredIf(Predicate<Tombstone> predicate);

    /** see if the already expired tombstones should be processed */
    protected abstract void checkExpiredTombstoneGC();

    protected abstract void handleNoUnexpiredTombstones();

    protected abstract boolean hasExpired(long msTillTombstoneExpires);

    protected abstract void expireTombstone(Tombstone tombstone);

    protected abstract void updateStatistics();

    /**
     * Do anything needed before the sweeper sleeps.
     */
    protected abstract void beforeSleepChecks();

    abstract boolean testHook_forceExpiredTombstoneGC(int count, long timeout, TimeUnit unit)
        throws InterruptedException;
  } // class TombstoneSweeper
}
