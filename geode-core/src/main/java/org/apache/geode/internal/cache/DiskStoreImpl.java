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

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.*;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.i18n.StringId;
import org.apache.geode.internal.FileUtil;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.DiskEntry.Helper.ValueWrapper;
import org.apache.geode.internal.cache.DiskEntry.RecoveredEntry;
import org.apache.geode.internal.cache.ExportDiskRegion.ExportWriter;
import org.apache.geode.internal.cache.lru.LRUAlgorithm;
import org.apache.geode.internal.cache.lru.LRUStatistics;
import org.apache.geode.internal.cache.persistence.*;
import org.apache.geode.internal.cache.snapshot.GFSnapshot;
import org.apache.geode.internal.cache.snapshot.GFSnapshot.SnapshotWriter;
import org.apache.geode.internal.cache.snapshot.SnapshotPacket.SnapshotRecord;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.concurrent.ConcurrentHashSet;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThreadGroup;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.util.BlobHelper;
import org.apache.geode.pdx.internal.EnumInfo;
import org.apache.geode.pdx.internal.PdxField;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.PeerTypeRegistration;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.InetAddress;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.geode.distributed.ConfigurationProperties.*;

/**
 * Represents a (disk-based) persistent store for region data. Used for both persistent recoverable
 * regions and overflow-only regions.
 * 
 * 
 * @since GemFire 3.2
 */
@SuppressWarnings("synthetic-access")
public class DiskStoreImpl implements DiskStore {
  private static final Logger logger = LogService.getLogger();

  private static final String BACKUP_DIR_PREFIX = "dir";
  public static final boolean KRF_DEBUG = Boolean.getBoolean("disk.KRF_DEBUG");

  public static final int MAX_OPEN_INACTIVE_OPLOGS = Integer
      .getInteger(DistributionConfig.GEMFIRE_PREFIX + "MAX_OPEN_INACTIVE_OPLOGS", 7).intValue();

  /*
   * If less than 20MB (default - configurable through this property) of the available space is left
   * for logging and other misc stuff then it is better to bail out.
   */
  public static final int MIN_DISK_SPACE_FOR_LOGS = Integer
      .getInteger(DistributionConfig.GEMFIRE_PREFIX + "MIN_DISK_SPACE_FOR_LOGS", 20).intValue();

  /** Represents an invalid id of a key/value on disk */
  public static final long INVALID_ID = 0L; // must be zero

  public static final String COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME =
      DistributionConfig.GEMFIRE_PREFIX + "disk.completeCompactionBeforeTermination";

  static final int MINIMUM_DIR_SIZE = 1024;

  /**
   * The static field delays the joining of the close/clear/destroy & forceFlush operation, with the
   * compactor thread. This joining occurs after the compactor thread is notified to exit. This was
   * added to reproduce deadlock caused by concurrent destroy & clear operation where clear
   * operation is restarting the compactor thread ( a new thread object different from the one for
   * which destroy operation issued notification for release). The delay occurs iff the flag used
   * for enabling callbacks to CacheObserver is enabled true
   */
  static volatile long DEBUG_DELAY_JOINING_WITH_COMPACTOR = 500;

  /**
   * Kept for backwards compat. Should use allowForceCompaction api/dtd instead.
   */
  private final static boolean ENABLE_NOTIFY_TO_ROLL =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "ENABLE_NOTIFY_TO_ROLL");

  public static final String RECOVER_VALUE_PROPERTY_NAME =
      DistributionConfig.GEMFIRE_PREFIX + "disk.recoverValues";
  public static final String RECOVER_VALUES_SYNC_PROPERTY_NAME =
      DistributionConfig.GEMFIRE_PREFIX + "disk.recoverValuesSync";
  boolean RECOVER_VALUES = getBoolean(DiskStoreImpl.RECOVER_VALUE_PROPERTY_NAME, true);
  boolean RECOVER_VALUES_SYNC = getBoolean(DiskStoreImpl.RECOVER_VALUES_SYNC_PROPERTY_NAME, false);
  boolean FORCE_KRF_RECOVERY =
      getBoolean(DistributionConfig.GEMFIRE_PREFIX + "disk.FORCE_KRF_RECOVERY", false);

  public static boolean getBoolean(String sysProp, boolean def) {
    return Boolean.valueOf(System.getProperty(sysProp, Boolean.valueOf(def).toString()));
  }

  public static final long MIN_RESERVED_DRID = 1;
  public static final long MAX_RESERVED_DRID = 8;
  static final long MIN_DRID = MAX_RESERVED_DRID + 1;

  /**
   * Estimated number of bytes written to disk for each new disk id.
   */
  static final int BYTES_PER_ID = 8;

  /**
   * Maximum number of oplogs to compact per compaction operations. Defaults to 1 to allows oplogs
   * to be deleted quickly, to reduce amount of memory used during a compaction and to be fair to
   * other regions waiting for a compactor thread from the pool. Ignored if set to <= 0. Made non
   * static so tests can set it.
   */
  private final int MAX_OPLOGS_PER_COMPACTION = Integer
      .getInteger(DistributionConfig.GEMFIRE_PREFIX + "MAX_OPLOGS_PER_COMPACTION", Integer
          .getInteger(DistributionConfig.GEMFIRE_PREFIX + "MAX_OPLOGS_PER_ROLL", 1).intValue())
      .intValue();
  /**
   *
   */
  public static final int MAX_CONCURRENT_COMPACTIONS = Integer
      .getInteger(DistributionConfig.GEMFIRE_PREFIX + "MAX_CONCURRENT_COMPACTIONS", Integer
          .getInteger(DistributionConfig.GEMFIRE_PREFIX + "MAX_CONCURRENT_ROLLS", 1).intValue())
      .intValue();

  /**
   * This system property indicates that maximum number of delayed write tasks that can be pending
   * before submitting the tasks start blocking. These tasks are things like unpreblow oplogs,
   * delete oplogs, etc.
   */
  public static final int MAX_PENDING_TASKS =
      Integer.getInteger(DistributionConfig.GEMFIRE_PREFIX + "disk.MAX_PENDING_TASKS", 6);
  /**
   * This system property indicates that IF should also be preallocated. This property will be used
   * in conjunction with the PREALLOCATE_OPLOGS property. If PREALLOCATE_OPLOGS is ON the below will
   * by default be ON but in order to switch it off you need to explicitly
   */
  static final boolean PREALLOCATE_IF =
      !System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "preAllocateIF", "true")
          .equalsIgnoreCase("false");
  /**
   * This system property indicates that Oplogs should be preallocated till the maxOplogSize as
   * specified for the disk store.
   */
  static final boolean PREALLOCATE_OPLOGS =
      !System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "preAllocateDisk", "true")
          .equalsIgnoreCase("false");

  /**
   * For some testing purposes we would not consider top property if this flag is set to true
   **/
  public static boolean SET_IGNORE_PREALLOCATE = false;

  /**
   * This system property turns on synchronous writes just the the init file.
   */
  static final boolean SYNC_IF_WRITES =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "syncMetaDataWrites");

  /**
   * For testing - to keep track of files for which fallocate happened
   */
  public static volatile HashSet<String> TEST_CHK_FALLOC_DIRS;
  public static volatile HashSet<String> TEST_NO_FALLOC_DIRS;

  // /** delay for slowing down recovery, for testing purposes only */
  // public static volatile int recoverDelay = 0;

  // //////////////////// Instance Fields ///////////////////////

  private final GemFireCacheImpl cache;

  /** The stats for this store */
  private final DiskStoreStats stats;

  /**
   * Asif:Added as stop gap arrangement to fix bug 39380. It is not a clean fix as keeping track of
   * the threads acquiring read lock, etc is not a good idea to solve the issue
   */
  private final AtomicInteger entryOpsCount = new AtomicInteger();
  /**
   * Do not want to take chance with any object like DiskRegion etc as lock
   */
  private final Object closeRegionGuard = new Object();

  /** Number of dirs* */
  final int dirLength;

  /** Disk directory holders* */
  DirectoryHolder[] directories;

  /** max of all the dir sizes given stored in bytes* */
  private final long maxDirSize;

  /** disk dir to be used by info file * */
  private int infoFileDirIndex;

  private final int compactionThreshold;

  /**
   * The limit of how many items can be in the async queue before async starts blocking and a flush
   * is forced. If this value is 0 then no limit.
   */
  private final int maxAsyncItems;
  private final AtomicInteger forceFlushCount;
  private final Object asyncMonitor;

  // complex vars
  /** Compactor task which does the compaction. Null if compaction not possible. */
  private final OplogCompactor oplogCompactor;

  private DiskInitFile initFile = null;

  private volatile DiskStoreBackup diskStoreBackup = null;

  private final ReentrantReadWriteLock compactorLock = new ReentrantReadWriteLock();
  private final WriteLock compactorWriteLock = compactorLock.writeLock();
  private final ReadLock compactorReadLock = compactorLock.readLock();

  /**
   * Set if we have encountered a disk exception causing us to shutdown this disk store. This is
   * currently used only to prevent trying to shutdown the disk store from multiple threads, but I
   * think at some point we should use this to prevent any other ops from completing during the
   * close operation.
   */
  private final AtomicReference<DiskAccessException> diskException =
      new AtomicReference<DiskAccessException>();

  PersistentOplogSet persistentOplogs = new PersistentOplogSet(this);
  OverflowOplogSet overflowOplogs = new OverflowOplogSet(this);

  // private boolean isThreadWaitingForSpace = false;

  /**
   * Get the next available dir
   */

  // /**
  // * Max timed wait for disk space to become available for an entry operation
  // ,
  // * in milliseconds. This will be the maximum time for which a
  // * create/modify/remove operation will wait so as to allow switch over & get
  // a
  // * new Oplog for writing. If no space is available in that time,
  // * DiskAccessException will be thrown. The default wait will be for 120
  // * seconds
  // */
  // private static final long MAX_WAIT_FOR_SPACE = Integer.getInteger(
  // "MAX_WAIT_FOR_SPACE", 20).intValue() * 1000;


  private final AtomicLong regionIdCtr = new AtomicLong(MIN_DRID);
  /**
   * Only contains backup DiskRegions. The Value could be a RecoveredDiskRegion or a DiskRegion
   */
  private final ConcurrentMap<Long, DiskRegion> drMap = new ConcurrentHashMap<Long, DiskRegion>();
  /**
   * A set of overflow only regions that are using this disk store.
   */
  private final Set<DiskRegion> overflowMap = new ConcurrentHashSet<DiskRegion>();
  /**
   * Contains all of the disk recovery stores for which we are recovering values asnynchronously.
   */
  private final Map<Long, DiskRecoveryStore> currentAsyncValueRecoveryMap =
      new HashMap<Long, DiskRecoveryStore>();

  private final Object asyncValueRecoveryLock = new Object();

  /**
   * The unique id for this disk store.
   * 
   * Either set during recovery of an existing disk store when the IFREC_DISKSTORE_ID record is read
   * or when a new init file is created.
   * 
   */
  private DiskStoreID diskStoreID;

  private final CountDownLatch _testHandleDiskAccessException = new CountDownLatch(1);

  private final ThreadPoolExecutor diskStoreTaskPool;

  private final ThreadPoolExecutor delayedWritePool;
  private volatile Future lastDelayedWrite;

  // ///////////////////// Constructors /////////////////////////

  private static int calcCompactionThreshold(int ct) {
    if (ct == DiskStoreFactory.DEFAULT_COMPACTION_THRESHOLD) {
      // allow the old sys prop for backwards compat.
      if (System
          .getProperty(DistributionConfig.GEMFIRE_PREFIX + "OVERFLOW_ROLL_PERCENTAGE") != null) {
        ct = (int) (Double.parseDouble(System
            .getProperty(DistributionConfig.GEMFIRE_PREFIX + "OVERFLOW_ROLL_PERCENTAGE", "0.50"))
            * 100.0);
      }
    }
    return ct;
  }

  /**
   * Creates a new <code>DiskRegion</code> that access disk on behalf of the given region.
   */
  DiskStoreImpl(Cache cache, DiskStoreAttributes props) {
    this(cache, props, false, null);
  }

  DiskStoreImpl(Cache cache, DiskStoreAttributes props, boolean ownedByRegion,
      InternalRegionArguments internalRegionArgs) {
    this(cache, props.getName(), props, ownedByRegion, internalRegionArgs, false,
        false/* upgradeVersionOnly */, false, false, true, false/* offlineModify */);
  }

  DiskStoreImpl(Cache cache, String name, DiskStoreAttributes props, boolean ownedByRegion,
      InternalRegionArguments internalRegionArgs, boolean offline, boolean upgradeVersionOnly,
      boolean offlineValidating, boolean offlineCompacting, boolean needsOplogs,
      boolean offlineModify) {
    this.offline = offline;
    this.upgradeVersionOnly = upgradeVersionOnly;
    this.validating = offlineValidating;
    this.offlineCompacting = offlineCompacting;
    this.offlineModify = offlineModify;

    assert internalRegionArgs == null || ownedByRegion : "internalRegionArgs "
        + "should be non-null only if the DiskStore is owned by region";
    this.ownedByRegion = ownedByRegion;
    this.internalRegionArgs = internalRegionArgs;

    this.name = name;
    this.autoCompact = props.getAutoCompact();
    this.allowForceCompaction = props.getAllowForceCompaction();
    this.compactionThreshold = calcCompactionThreshold(props.getCompactionThreshold());
    this.maxOplogSizeInBytes = props.getMaxOplogSizeInBytes();
    this.timeInterval = props.getTimeInterval();
    this.queueSize = props.getQueueSize();
    this.writeBufferSize = props.getWriteBufferSize();
    this.diskDirs = props.getDiskDirs();
    this.diskDirSizes = props.getDiskDirSizes();
    this.warningPercent = props.getDiskUsageWarningPercentage();
    this.criticalPercent = props.getDiskUsageCriticalPercentage();

    this.cache = (GemFireCacheImpl) cache;
    StatisticsFactory factory = cache.getDistributedSystem();
    this.stats = new DiskStoreStats(factory, getName());

    // start simple init

    this.isCompactionPossible = isOfflineCompacting() || (!isOffline()
        && (getAutoCompact() || getAllowForceCompaction() || ENABLE_NOTIFY_TO_ROLL));
    this.maxAsyncItems = getQueueSize();
    this.forceFlushCount = new AtomicInteger();
    this.asyncMonitor = new Object();
    // always use LinkedBlockingQueue to work around bug 41470
    // if (this.maxAsyncItems > 0 && this.maxAsyncItems < 1000000) {
    // // we compare to 1,000,000 so that very large maxItems will
    // // not cause us to consume too much memory in our queue.
    // // Removed the +13 since it made the queue bigger than was configured.
    // // The +13 is to give us a bit of headroom during the drain.
    // this.asyncQueue = new
    // ArrayBlockingQueue<Object>(this.maxAsyncItems/*+13*/);
    // } else {
    if (this.maxAsyncItems > 0) {
      this.asyncQueue = new ForceableLinkedBlockingQueue<Object>(this.maxAsyncItems); // fix for bug
                                                                                      // 41310
    } else {
      this.asyncQueue = new ForceableLinkedBlockingQueue<Object>();
    }
    if (!isValidating() && !isOfflineCompacting()) {
      startAsyncFlusher();
    }

    File[] dirs = getDiskDirs();
    int[] dirSizes = getDiskDirSizes();
    int length = dirs.length;
    this.directories = new DirectoryHolder[length];
    long tempMaxDirSize = 0;
    for (int i = 0; i < length; i++) {
      directories[i] =
          new DirectoryHolder(getName() + "_DIR#" + i, factory, dirs[i], dirSizes[i], i);

      if (tempMaxDirSize < dirSizes[i]) {
        tempMaxDirSize = dirSizes[i];
      }
    }
    // stored in bytes
    this.maxDirSize = tempMaxDirSize * 1024 * 1024;
    this.infoFileDirIndex = 0;
    // Now that we no longer have db files, use all directories for oplogs
    /**
     * The infoFileDir contains the lock file and the init file. It will be directories[0] on a
     * brand new disk store. On an existing disk store it will be the directory the init file is
     * found in.
     */
    this.dirLength = length;

    loadFiles(needsOplogs);

    // setFirstChild(getSortedOplogs());

    // complex init
    if (isCompactionPossible() && !isOfflineCompacting()) {
      this.oplogCompactor = new OplogCompactor();
      this.oplogCompactor.startCompactor();
    } else {
      this.oplogCompactor = null;
    }

    int MAXT = DiskStoreImpl.MAX_CONCURRENT_COMPACTIONS;
    final ThreadGroup compactThreadGroup =
        LoggingThreadGroup.createThreadGroup("Oplog Compactor Thread Group", this.logger);
    final ThreadFactory compactThreadFactory =
        GemfireCacheHelper.CreateThreadFactory(compactThreadGroup, "Idle OplogCompactor");
    this.diskStoreTaskPool = new ThreadPoolExecutor(MAXT, MAXT, 10, TimeUnit.SECONDS,
        new LinkedBlockingQueue(), compactThreadFactory);
    this.diskStoreTaskPool.allowCoreThreadTimeOut(true);


    final ThreadGroup deleteThreadGroup =
        LoggingThreadGroup.createThreadGroup("Oplog Delete Thread Group", this.logger);

    final ThreadFactory deleteThreadFactory =
        GemfireCacheHelper.CreateThreadFactory(deleteThreadGroup, "Oplog Delete Task");
    this.delayedWritePool = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
        new LinkedBlockingQueue(MAX_PENDING_TASKS), deleteThreadFactory,
        new ThreadPoolExecutor.CallerRunsPolicy());
    this.delayedWritePool.allowCoreThreadTimeOut(true);
  }

  // //////////////////// Instance Methods //////////////////////

  public boolean sameAs(DiskStoreAttributes props) {
    if (getAllowForceCompaction() != props.getAllowForceCompaction()) {
      if (logger.isDebugEnabled()) {
        logger.debug("allowForceCompaction {} != {}", getAllowForceCompaction(),
            props.getAllowForceCompaction());
      }
    }
    if (getAutoCompact() != props.getAutoCompact()) {
      if (logger.isDebugEnabled()) {
        logger.debug("AutoCompact {} != {}", getAutoCompact(), props.getAutoCompact());
      }
    }
    if (getCompactionThreshold() != props.getCompactionThreshold()) {
      if (logger.isDebugEnabled()) {
        logger.debug("CompactionThreshold {} != {}", getCompactionThreshold(),
            props.getCompactionThreshold());
      }
    }
    if (getMaxOplogSizeInBytes() != props.getMaxOplogSizeInBytes()) {
      if (logger.isDebugEnabled()) {
        logger.debug("MaxOplogSizeInBytes {} != {}", getMaxOplogSizeInBytes(),
            props.getMaxOplogSizeInBytes());
      }
    }
    if (!getName().equals(props.getName())) {
      if (logger.isDebugEnabled()) {
        logger.debug("Name {} != {}", getName(), props.getName());
      }
    }
    if (getQueueSize() != props.getQueueSize()) {
      if (logger.isDebugEnabled()) {
        logger.debug("QueueSize {} != {}", getQueueSize(), props.getQueueSize());
      }
    }
    if (getTimeInterval() != props.getTimeInterval()) {
      if (logger.isDebugEnabled()) {
        logger.debug("TimeInterval {} != {}", getTimeInterval(), props.getTimeInterval());
      }
    }
    if (getWriteBufferSize() != props.getWriteBufferSize()) {
      logger.debug("WriteBufferSize {} != {}", getWriteBufferSize(), props.getWriteBufferSize());
    }
    if (!Arrays.equals(getDiskDirs(), props.getDiskDirs())) {
      if (logger.isDebugEnabled()) {
        logger.debug("DiskDirs {} != {}", Arrays.toString(getDiskDirs()),
            Arrays.toString(props.getDiskDirs()));
      }
    }
    if (!Arrays.equals(getDiskDirSizes(), props.getDiskDirSizes())) {
      if (logger.isDebugEnabled()) {
        logger.debug("DiskDirSizes {} != {}", Arrays.toString(getDiskDirSizes()),
            Arrays.toString(props.getDiskDirSizes()));
      }
    }

    return getAllowForceCompaction() == props.getAllowForceCompaction()
        && getAutoCompact() == props.getAutoCompact()
        && getCompactionThreshold() == props.getCompactionThreshold()
        && getMaxOplogSizeInBytes() == props.getMaxOplogSizeInBytes()
        && getName().equals(props.getName()) && getQueueSize() == props.getQueueSize()
        && getTimeInterval() == props.getTimeInterval()
        && getWriteBufferSize() == props.getWriteBufferSize()
        && Arrays.equals(getDiskDirs(), props.getDiskDirs())
        && Arrays.equals(getDiskDirSizes(), props.getDiskDirSizes());
  }

  /**
   * Returns the <code>DiskStoreStats</code> for this store
   */
  public DiskStoreStats getStats() {
    return this.stats;
  }

  public Map<Long, AbstractDiskRegion> getAllDiskRegions() {
    Map<Long, AbstractDiskRegion> results = new HashMap<Long, AbstractDiskRegion>();
    results.putAll(drMap);
    results.putAll(initFile.getDRMap());
    return results;
  }

  void scheduleForRecovery(DiskRecoveryStore drs) {
    DiskRegionView dr = drs.getDiskRegionView();
    PersistentOplogSet oplogSet = getPersistentOplogSet(dr);
    oplogSet.scheduleForRecovery(drs);
  }

  /**
   * Initializes the contents of any regions on this DiskStore that have been registered but are not
   * yet initialized.
   */
  final void initializeOwner(LocalRegion lr) {
    DiskRegion dr = lr.getDiskRegion();
    // We don't need to do recovery for overflow regions.
    if (!lr.getDataPolicy().withPersistence() || !dr.isRecreated()) {
      return;
    }

    // prevent async recovery from recovering a value
    // while we are copying the entry map.
    synchronized (currentAsyncValueRecoveryMap) {
      DiskRegionView drv = lr.getDiskRegionView();
      if (drv.getRecoveredEntryMap() != null) {
        PersistentOplogSet oplogSet = getPersistentOplogSet(drv);

        // acquire CompactorWriteLock only if the region attributes for the
        // real region are different from the place holder region's
        boolean releaseCompactorWriteLock = false;
        if (drv.isEntriesMapIncompatible()) {
          acquireCompactorWriteLock(); // fix bug #51097 to prevent concurrent compaction
          releaseCompactorWriteLock = true;
        }
        try {
          drv.copyExistingRegionMap(lr);
          getStats().incUncreatedRecoveredRegions(-1);
          for (Oplog oplog : oplogSet.getAllOplogs()) {
            if (oplog != null) {
              oplog.updateDiskRegion(lr.getDiskRegionView());
            }
          }
        } finally {
          if (releaseCompactorWriteLock) {
            releaseCompactorWriteLock();
          }
        }
        if (currentAsyncValueRecoveryMap.containsKey(drv.getId())) {
          currentAsyncValueRecoveryMap.put(drv.getId(), lr);
        }
        return;
      }
    }

    scheduleForRecovery(lr);

    // boolean gotLock = false;

    try {
      // acquireReadLock(dr);
      // gotLock = true;
      recoverRegionsThatAreReady();
    } catch (DiskAccessException dae) {
      // Asif:Just rethrow t
      throw dae;
    } catch (RuntimeException re) {
      // @todo: if re is caused by a RegionDestroyedException
      // (or CacheClosed...) then don't we want to throw that instead
      // of a DiskAccessException?
      // Asif :wrap it in DiskAccessException
      // IOException is alerady wrappped by DiskRegion correctly.
      // Howvever EntryEventImpl .deserialize is converting IOException
      // into IllegalArgumentExcepption, so handle only run time exception
      // here
      throw new DiskAccessException("RuntimeException in initializing the disk store from the disk",
          re, this);
    }
    // finally {
    // if(gotLock) {
    // releaseReadLock(dr);
    // }
    // }
  }

  private OplogSet getOplogSet(DiskRegionView drv) {
    if (drv.isBackup()) {
      return persistentOplogs;
    } else {
      return overflowOplogs;
    }
  }

  PersistentOplogSet getPersistentOplogSet(DiskRegionView drv) {
    assert drv.isBackup();
    return persistentOplogs;
  }

  /**
   * Stores a key/value pair from a region entry on disk. Updates all of the necessary
   * {@linkplain DiskRegionStats statistics}and invokes {@link Oplog#create}or {@link Oplog#modify}.
   * 
   * @param entry The entry which is going to be written to disk
   * @throws RegionClearedException If a clear operation completed before the put operation
   *         completed successfully, resulting in the put operation to abort.
   * @throws IllegalArgumentException If <code>id</code> is less than zero
   */
  final void put(LocalRegion region, DiskEntry entry, ValueWrapper value, boolean async)
      throws RegionClearedException {
    DiskRegion dr = region.getDiskRegion();
    DiskId id = entry.getDiskId();
    if (dr.isBackup() && id.getKeyId() < 0) {
      throw new IllegalArgumentException(
          LocalizedStrings.DiskRegion_CANT_PUT_A_KEYVALUE_PAIR_WITH_ID_0.toLocalizedString(id));
    }
    long start = async ? this.stats.startFlush() : this.stats.startWrite();
    if (!async) {
      dr.getStats().startWrite();
    }
    try {
      if (!async) {
        acquireReadLock(dr);
      }
      try {
        if (dr.isRegionClosed()) {
          region.getCancelCriterion().checkCancelInProgress(null);
          throw new RegionDestroyedException(
              LocalizedStrings.DiskRegion_THE_DISKREGION_HAS_BEEN_CLOSED_OR_DESTROYED
                  .toLocalizedString(),
              dr.getName());
        }

        // Asif TODO: Should the htree reference in
        // DiskRegion/DiskRegion be made
        // volatile.Will theacquireReadLock ensure variable update?
        boolean doingCreate = false;
        if (dr.isBackup() && id.getKeyId() == INVALID_ID) {
          doingCreate = true;
          // the call to newOplogEntryId moved down into Oplog.basicCreate
        }
        boolean goahead = true;
        if (dr.didClearCountChange()) {
          // mbid: if the reference has changed (by a clear)
          // after a put has been made in the region
          // then we need to confirm if this key still exists in the region
          // before writing to disk
          goahead = region.basicGetEntry(entry.getKey()) == entry;
        }
        if (goahead) {
          // in overflow only mode, no need to write the key and the
          // extra data, hence if it is overflow only mode then use
          // modify and not create
          OplogSet oplogSet = getOplogSet(dr);
          if (doingCreate) {
            oplogSet.create(region, entry, value, async);
          } else {
            oplogSet.modify(region, entry, value, async);
          }
        } else {
          throw new RegionClearedException(
              LocalizedStrings.DiskRegion_CLEAR_OPERATION_ABORTING_THE_ONGOING_ENTRY_0_OPERATION_FOR_ENTRY_WITH_DISKID_1
                  .toLocalizedString(
                      new Object[] {((doingCreate) ? "creation" : "modification"), id}));
        }
      } finally {
        if (!async) {
          releaseReadLock(dr);
        }
      }
    } finally {
      if (async) {
        this.stats.endFlush(start);
      } else {
        dr.getStats().endWrite(start, this.stats.endWrite(start));
        dr.getStats().incWrittenBytes(id.getValueLength());
      }
    }
  }

  final void putVersionTagOnly(LocalRegion region, VersionTag tag, boolean async) {
    DiskRegion dr = region.getDiskRegion();
    // this method will only be called by backup oplog
    assert dr.isBackup();

    if (!async) {
      acquireReadLock(dr);
    }
    try {
      if (dr.isRegionClosed()) {
        region.getCancelCriterion().checkCancelInProgress(null);
        throw new RegionDestroyedException(
            LocalizedStrings.DiskRegion_THE_DISKREGION_HAS_BEEN_CLOSED_OR_DESTROYED
                .toLocalizedString(),
            dr.getName());
      }

      if (dr.getRegionVersionVector().contains(tag.getMemberID(), tag.getRegionVersion())) {
        // No need to write the conflicting tag to disk if the disk RVV already
        // contains this tag.
        return;
      }

      PersistentOplogSet oplogSet = getPersistentOplogSet(dr);

      oplogSet.getChild().saveConflictVersionTag(region, tag, async);
    } finally {
      if (!async) {
        releaseReadLock(dr);
      }
    }
  }

  /**
   * Returns the value of the key/value pair with the given diskId. Updates all of the necessary
   * {@linkplain DiskRegionStats statistics}
   * 
   */
  final Object get(DiskRegion dr, DiskId id) {
    acquireReadLock(dr);
    try {
      int count = 0;
      RuntimeException ex = null;
      while (count < 3) {
        // retry at most 3 times
        BytesAndBits bb = null;
        try {
          if (dr.isRegionClosed()) {
            throw new RegionDestroyedException(
                LocalizedStrings.DiskRegion_THE_DISKREGION_HAS_BEEN_CLOSED_OR_DESTROYED
                    .toLocalizedString(),
                dr.getName());
          }
          if (dr.didClearCountChange()) {
            return Token.REMOVED_PHASE1;
          }
          bb = getBytesAndBitsWithoutLock(dr, id, true/* fault -in */, false /*
                                                                              * Get only the userbit
                                                                              */);
          if (bb == CLEAR_BB) {
            return Token.REMOVED_PHASE1;
          }
          return convertBytesAndBitsIntoObject(bb);
        } catch (IllegalArgumentException e) {
          count++;
          if (logger.isDebugEnabled()) {
            logger.debug(
                "DiskRegion: Tried {}, getBytesAndBitsWithoutLock returns wrong byte array: {}",
                count, Arrays.toString(bb.getBytes()));
          }
          ex = e;
        }
      } // while
      if (logger.isDebugEnabled()) {
        logger.debug(
            "Retried 3 times, getting entry from DiskRegion still failed. It must be Oplog file corruption due to HA");
      }
      throw ex;
    } finally {
      releaseReadLock(dr);
    }
  }

  // private static String baToString(byte[] ba) {
  // StringBuffer sb = new StringBuffer();
  // for (int i=0; i < ba.length; i++) {
  // sb.append(ba[i]).append(", ");
  // }
  // return sb.toString();
  // }

  /**
   * This method was added to fix bug 40192. It is like getBytesAndBits except it will return
   * Token.REMOVE_PHASE1 if the htreeReference has changed (which means a clear was done).
   * 
   * @return an instance of BytesAndBits or Token.REMOVED_PHASE1
   */
  final Object getRaw(DiskRegionView dr, DiskId id) {
    if (dr.isRegionClosed()) {
      throw new RegionDestroyedException(
          LocalizedStrings.DiskRegion_THE_DISKREGION_HAS_BEEN_CLOSED_OR_DESTROYED
              .toLocalizedString(),
          dr.getName());
    }
    if (dr.didClearCountChange()) {
      return Token.REMOVED_PHASE1;
    }
    BytesAndBits bb = dr.getDiskStore().getBytesAndBitsWithoutLock(dr, id, true/* fault -in */,
        false /* Get only the userbit */);
    if (bb == CLEAR_BB) {
      return Token.REMOVED_PHASE1;
    }
    return bb;
  }

  /**
   * Given a BytesAndBits object convert it to the relevant Object (deserialize if necessary) and
   * return the object
   * 
   * @param bb
   * @return the converted object
   */
  static Object convertBytesAndBitsIntoObject(BytesAndBits bb) {
    byte[] bytes = bb.getBytes();
    Object value;
    if (EntryBits.isInvalid(bb.getBits())) {
      value = Token.INVALID;
    } else if (EntryBits.isSerialized(bb.getBits())) {
      value = DiskEntry.Helper.readSerializedValue(bytes, bb.getVersion(), null, true);
    } else if (EntryBits.isLocalInvalid(bb.getBits())) {
      value = Token.LOCAL_INVALID;
    } else if (EntryBits.isTombstone(bb.getBits())) {
      value = Token.TOMBSTONE;
    } else {
      value = DiskEntry.Helper.readRawValue(bytes, bb.getVersion(), null);
    }
    return value;
  }

  /**
   * Given a BytesAndBits object get the serialized blob
   * 
   * @param bb
   * @return the converted object
   */
  static Object convertBytesAndBitsToSerializedForm(BytesAndBits bb) {
    final byte[] bytes = bb.getBytes();
    Object value;
    if (EntryBits.isInvalid(bb.getBits())) {
      value = Token.INVALID;
    } else if (EntryBits.isSerialized(bb.getBits())) {
      value = DiskEntry.Helper.readSerializedValue(bytes, bb.getVersion(), null, false);
    } else if (EntryBits.isLocalInvalid(bb.getBits())) {
      value = Token.LOCAL_INVALID;
    } else if (EntryBits.isTombstone(bb.getBits())) {
      value = Token.TOMBSTONE;
    } else {
      value = DiskEntry.Helper.readRawValue(bytes, bb.getVersion(), null);
    }
    return value;
  }

  // CLEAR_BB was added in reaction to bug 41306
  private final BytesAndBits CLEAR_BB = new BytesAndBits(null, (byte) 0);

  /**
   * Gets the Object from the OpLog . It can be invoked from OpLog , if by the time a get operation
   * reaches the OpLog, the entry gets compacted or if we allow concurrent put & get operations. It
   * will also minimize the synch lock on DiskId
   * 
   * @param id DiskId object for the entry
   * @return value of the entry or CLEAR_BB if it is detected that the entry was removed by a
   *         concurrent region clear.
   */
  final BytesAndBits getBytesAndBitsWithoutLock(DiskRegionView dr, DiskId id, boolean faultIn,
      boolean bitOnly) {
    long oplogId = id.getOplogId();
    OplogSet oplogSet = getOplogSet(dr);
    CompactableOplog oplog = oplogSet.getChild(oplogId);
    if (oplog == null) {
      if (dr.didClearCountChange()) {
        return CLEAR_BB;
      }
      throw new DiskAccessException(
          LocalizedStrings.DiskRegion_DATA_FOR_DISKENTRY_HAVING_DISKID_AS_0_COULD_NOT_BE_OBTAINED_FROM_DISK_A_CLEAR_OPERATION_MAY_HAVE_DELETED_THE_OPLOGS
              .toLocalizedString(id),
          dr.getName());
    }
    return oplog.getBytesAndBits(dr, id, faultIn, bitOnly);
  }

  final BytesAndBits getBytesAndBits(DiskRegion dr, DiskId id, boolean faultingIn) {
    acquireReadLock(dr);
    try {
      if (dr.isRegionClosed()) {
        throw new RegionDestroyedException(
            LocalizedStrings.DiskRegion_THE_DISKREGION_HAS_BEEN_CLOSED_OR_DESTROYED
                .toLocalizedString(),
            dr.getName());
      }
      if (dr.didClearCountChange()) {
        throw new DiskAccessException(
            LocalizedStrings.DiskRegion_ENTRY_HAS_BEEN_CLEARED_AND_IS_NOT_PRESENT_ON_DISK
                .toLocalizedString(),
            dr.getName());
      }
      BytesAndBits bb = getBytesAndBitsWithoutLock(dr, id, faultingIn, false /*
                                                                              * Get only user bit
                                                                              */);
      if (bb == CLEAR_BB) {
        throw new DiskAccessException(
            LocalizedStrings.DiskRegion_ENTRY_HAS_BEEN_CLEARED_AND_IS_NOT_PRESENT_ON_DISK
                .toLocalizedString(),
            dr.getName());
      }
      return bb;
    } finally {
      releaseReadLock(dr);
    }

  }

  /**
   * @since GemFire 3.2.1
   */
  final byte getBits(DiskRegion dr, DiskId id) {
    acquireReadLock(dr);
    try {
      if (dr.isRegionClosed()) {
        throw new RegionDestroyedException(
            LocalizedStrings.DiskRegion_THE_DISKREGION_HAS_BEEN_CLOSED_OR_DESTROYED
                .toLocalizedString(),
            dr.getName());
      }
      if (dr.didClearCountChange()) {
        // value not present on disk as it has been cleared. Return invalid
        // userbit
        return EntryBits.setInvalid((byte) 0, true);
      }
      // TODO:Asif : Fault In?
      BytesAndBits bb = getBytesAndBitsWithoutLock(dr, id, true, true /*
                                                                       * Get only user bit
                                                                       */);
      if (bb == CLEAR_BB) {
        return EntryBits.setInvalid((byte) 0, true);
      }
      return bb.getBits();
    } finally {
      releaseReadLock(dr);
    }

  }

  /**
   * Asif: THIS SHOULD ONLY BE USED FOR TESTING PURPOSES AS IT IS NOT THREAD SAFE
   * 
   * Returns the object stored on disk with the given id. This method is used for testing purposes
   * only. As such, it bypasses the buffer and goes directly to the disk. This is not a thread safe
   * function , in the sense, it is possible that by the time the OpLog is queried , data might move
   * HTree with the oplog being destroyed
   * 
   * @return null if entry has nothing stored on disk (id == INVALID_ID)
   * @throws IllegalArgumentException If <code>id</code> is less than zero, no action is taken.
   */
  public final Object getNoBuffer(DiskRegion dr, DiskId id) {
    BytesAndBits bb = null;
    acquireReadLock(dr);
    try {
      long opId = id.getOplogId();
      if (opId != -1) {
        OplogSet oplogSet = getOplogSet(dr);
        bb = oplogSet.getChild(opId).getNoBuffer(dr, id);
        return convertBytesAndBitsIntoObject(bb);
      } else {
        return null;
      }
    } finally {
      releaseReadLock(dr);
    }
  }

  void testHookCloseAllOverflowChannels() {
    overflowOplogs.testHookCloseAllOverflowChannels();
  }

  ArrayList<OverflowOplog> testHookGetAllOverflowOplogs() {
    return overflowOplogs.testHookGetAllOverflowOplogs();
  }

  void testHookCloseAllOverflowOplogs() {
    overflowOplogs.testHookCloseAllOverflowOplogs();
  }

  /**
   * Removes the key/value pair with the given id on disk.
   * 
   * @param async true if called by the async flusher thread
   * 
   * @throws RegionClearedException If a clear operation completed before the put operation
   *         completed successfully, resulting in the put operation to abort.
   * @throws IllegalArgumentException If <code>id</code> is {@linkplain #INVALID_ID invalid}or is
   *         less than zero, no action is taken.
   */
  final void remove(LocalRegion region, DiskEntry entry, boolean async, boolean isClear)
      throws RegionClearedException {
    DiskRegion dr = region.getDiskRegion();
    if (!async) {
      acquireReadLock(dr);
    }
    try {
      if (dr.isRegionClosed()) {
        throw new RegionDestroyedException(
            LocalizedStrings.DiskRegion_THE_DISKREGION_HAS_BEEN_CLOSED_OR_DESTROYED
                .toLocalizedString(),
            dr.getName());
      }

      // mbid: if reference has changed (only clear
      // can change the reference) then we should not try to remove again.
      // Entry will not be found in diskRegion.
      // So if reference has changed, do nothing.
      if (!dr.didClearCountChange()) {
        long start = this.stats.startRemove();
        OplogSet oplogSet = getOplogSet(dr);
        oplogSet.remove(region, entry, async, isClear);
        dr.getStats().endRemove(start, this.stats.endRemove(start));
      } else {
        throw new RegionClearedException(
            LocalizedStrings.DiskRegion_CLEAR_OPERATION_ABORTING_THE_ONGOING_ENTRY_DESTRUCTION_OPERATION_FOR_ENTRY_WITH_DISKID_0
                .toLocalizedString(entry.getDiskId()));
      }
    } finally {
      if (!async) {
        releaseReadLock(dr);
      }
    }
  }

  private FlushPauser fp = null;

  /**
   * After tests call this method they must call flushForTesting.
   */
  public void pauseFlusherForTesting() {
    assert this.fp == null;
    this.fp = new FlushPauser();
    try {
      addAsyncItem(this.fp, true);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("unexpected interrupt in test code", ex);
    }
  }

  public void flushForTesting() {
    if (this.fp != null) {
      this.fp.unpause();
      this.fp = null;
    }
    forceFlush();
  }

  // //////////////////// Implementation Methods //////////////////////


  /**
   * This function is having a default visiblity as it is used in the OplogJUnitTest for a bug
   * verification of Bug # 35012
   * 
   * All callers must have {@link #releaseWriteLock(DiskRegion)} in a matching finally block.
   * 
   * Note that this is no longer implemented by getting a write lock but instead locks the same lock
   * that acquireReadLock does.
   * 
   * @since GemFire 5.1
   */
  private void acquireWriteLock(DiskRegion dr) {
    // @todo darrel: this is no longer a write lock need to change method name
    dr.acquireWriteLock();
  }

  /**
   * 
   * This function is having a default visiblity as it is used in the OplogJUnitTest for a bug
   * verification of Bug # 35012
   * 
   * @since GemFire 5.1
   */

  private void releaseWriteLock(DiskRegion dr) {
    // @todo darrel: this is no longer a write lock need to change method name
    dr.releaseWriteLock();
  }

  /**
   * All callers must have {@link #releaseReadLock(DiskRegion)} in a matching finally block. Note
   * that this is no longer implemented by getting a read lock but instead locks the same lock that
   * acquireWriteLock does.
   * 
   * @since GemFire 5.1
   */
  void acquireReadLock(DiskRegion dr) {
    dr.basicAcquireReadLock();
    synchronized (this.closeRegionGuard) {
      entryOpsCount.incrementAndGet();
      if (dr.isRegionClosed()) {
        dr.releaseReadLock();
        throw new RegionDestroyedException("The DiskRegion has been closed or destroyed",
            dr.getName());
      }
    }
  }

  /**
   * @since GemFire 5.1
   */

  void releaseReadLock(DiskRegion dr) {
    dr.basicReleaseReadLock();
    int currentOpsInProgress = entryOpsCount.decrementAndGet();
    // Potential candiate for notifying in case of disconnect
    if (currentOpsInProgress == 0) {
      synchronized (this.closeRegionGuard) {
        if (dr.isRegionClosed() && entryOpsCount.get() == 0) {
          this.closeRegionGuard.notify();
        }
      }
    }
  }

  public void forceRoll() {
    persistentOplogs.forceRoll(null);
  }

  /**
   * @since GemFire 5.1
   */
  public void forceRolling(DiskRegion dr) {
    if (!dr.isBackup())
      return;
    if (!dr.isSync() && this.maxAsyncItems == 0 && getTimeInterval() == 0) {
      forceFlush();
    }
    acquireReadLock(dr);
    try {
      PersistentOplogSet oplogSet = getPersistentOplogSet(dr);
      oplogSet.forceRoll(dr);
    } finally {
      releaseReadLock(dr);
    }
  }

  public boolean forceCompaction() {
    return basicForceCompaction(null);
  }

  public boolean forceCompaction(DiskRegion dr) {
    if (!dr.isBackup())
      return false;
    acquireReadLock(dr);
    try {
      return basicForceCompaction(dr);
    } finally {
      releaseReadLock(dr);
    }
  }

  /**
   * Get serialized form of data off the disk
   * 
   * @param id
   * @since GemFire 5.7
   */
  public Object getSerializedData(DiskRegion dr, DiskId id) {
    return convertBytesAndBitsToSerializedForm(getBytesAndBits(dr, id, true));
  }

  private void checkForFlusherThreadTermination() {
    if (this.flusherThreadTerminated) {
      String message =
          "Could not schedule asynchronous write because the flusher thread had been terminated.";
      if (this.isClosing()) {
        // for bug 41305
        throw this.cache.getCacheClosedException(message, null);
      } else {
        throw new DiskAccessException(message, this);
      }

    }
  }

  private void handleFullAsyncQueue(Object o) {
    AsyncDiskEntry ade = (AsyncDiskEntry) o;
    LocalRegion region = ade.region;
    try {
      VersionTag tag = ade.tag;
      if (ade.versionOnly) {
        DiskEntry.Helper.doAsyncFlush(tag, region);
      } else {
        DiskEntry entry = ade.de;
        DiskEntry.Helper.handleFullAsyncQueue(entry, region, tag);
      }
    } catch (RegionDestroyedException ex) {
      // Normally we flush before closing or destroying a region
      // but in some cases it is closed w/o flushing.
      // So just ignore it; see bug 41305.
    }
  }

  public void addDiskRegionToQueue(LocalRegion lr) {
    try {
      addAsyncItem(lr, true);
    } catch (InterruptedException ignore) {
      // If it fail, that means the RVVTrusted is not written. It will
      // automatically do full-GII
    }
  }

  private void addAsyncItem(Object item, boolean forceAsync) throws InterruptedException {
    synchronized (this.lock) { // fix for bug 41390
      // 43312: since this thread has gained dsi.lock, dsi.clear() should have
      // finished. We check if clear() has happened after ARM.putEntryIfAbsent()
      if (item instanceof AsyncDiskEntry) {
        AsyncDiskEntry ade = (AsyncDiskEntry) item;
        DiskRegion dr = ade.region.getDiskRegion();
        if (dr.didClearCountChange() && !ade.versionOnly) {
          return;
        }
        if (ade.region.isDestroyed) {
          throw new RegionDestroyedException(ade.region.toString(), ade.region.getFullPath());
        }
      }
      checkForFlusherThreadTermination();
      if (forceAsync) {
        this.asyncQueue.forcePut(item);
      } else {
        if (!this.asyncQueue.offer(item)) {
          // queue is full so do a sync write to prevent deadlock
          handleFullAsyncQueue(item);
          // return early since we didn't add it to the queue
          return;
        }
      }
      this.stats.incQueueSize(1);
    }
    if (this.maxAsyncItems > 0) {
      if (checkAsyncItemLimit()) {
        synchronized (this.asyncMonitor) {
          this.asyncMonitor.notifyAll();
        }
      }
    }
  }

  private void rmAsyncItem(Object item) {
    if (this.asyncQueue.remove(item)) {
      this.stats.incQueueSize(-1);
    }
  }

  private long startAsyncWrite(DiskRegion dr) {
    if (this.stoppingFlusher) {
      if (isClosed()) {
        throw (new Stopper()).generateCancelledException(null); // fix for bug
                                                                // 41141
      } else {
        throw new DiskAccessException(
            "The disk store is still open, but flusher is stopped, probably no space left on device",
            this);
      }
    } else {
      this.pendingAsyncEnqueue.incrementAndGet();
    }
    dr.getStats().startWrite();
    return this.stats.startWrite();
  }

  private void endAsyncWrite(AsyncDiskEntry ade, DiskRegion dr, long start) {
    this.pendingAsyncEnqueue.decrementAndGet();
    dr.getStats().endWrite(start, this.stats.endWrite(start));

    if (!ade.versionOnly) { // for versionOnly = true ade.de will be null
      long bytesWritten = ade.de.getDiskId().getValueLength();
      dr.getStats().incWrittenBytes(bytesWritten);
    }

  }

  /**
   * @since GemFire prPersistSprint1
   */
  public void scheduleAsyncWrite(AsyncDiskEntry ade) {
    DiskRegion dr = ade.region.getDiskRegion();
    long start = startAsyncWrite(dr);
    try {
      try {
        addAsyncItem(ade, false);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        ade.region.getCancelCriterion().checkCancelInProgress(ie);
        // @todo: I'm not sure we need an error here
        if (!ade.versionOnly)
          ade.de.getDiskId().setPendingAsync(false);
      }
    } finally {
      endAsyncWrite(ade, dr, start);
    }
  }

  /**
   * @since GemFire prPersistSprint1
   */
  public void unscheduleAsyncWrite(DiskId did) {
    if (did != null) {
      did.setPendingAsync(false);
      // we could remove it from the async buffer but currently
      // we just wait for the flusher to discover it and drop it.
    }
  }

  /**
   * This queue can continue DiskEntry of FlushNotifier.
   */
  private final ForceableLinkedBlockingQueue<Object> asyncQueue;
  private final Object drainSync = new Object();
  private ArrayList drainList = null;

  private int fillDrainList() {
    synchronized (this.drainSync) {
      this.drainList = new ArrayList(asyncQueue.size());
      int drainCount = asyncQueue.drainTo(this.drainList);
      return drainCount;
    }
  }

  private ArrayList getDrainList() {
    return this.drainList;
  }

  /**
   * To fix bug 41770 clear the list in a way that will not break a concurrent iterator that is not
   * synced on drainSync. Only clear from it entries on the given region. Currently we do this by
   * clearing the isPendingAsync bit on each entry in this list.
   * 
   * @param rvv
   */
  void clearDrainList(LocalRegion r, RegionVersionVector rvv) {
    synchronized (this.drainSync) {
      if (this.drainList == null)
        return;
      Iterator it = this.drainList.iterator();
      while (it.hasNext()) {
        Object o = it.next();
        if (o instanceof AsyncDiskEntry) {
          AsyncDiskEntry ade = (AsyncDiskEntry) o;
          if (shouldClear(r, rvv, ade) && ade.de != null) {
            unsetPendingAsync(ade);
          }
        }
      }
    }
  }

  private boolean shouldClear(LocalRegion r, RegionVersionVector rvv, AsyncDiskEntry ade) {
    if (ade.region != r) {
      return false;
    }

    // If no RVV, remove all of the async items for this region.
    if (rvv == null) {
      return true;
    }

    // If we are clearing based on an RVV, only remove
    // entries contained in the RVV
    if (ade.versionOnly) {
      return rvv.contains(ade.tag.getMemberID(), ade.tag.getRegionVersion());
    } else {
      VersionStamp stamp = ade.de.getVersionStamp();
      VersionSource member = stamp.getMemberID();
      if (member == null) {
        // For overflow only regions, the version member may be null
        // because that represents the local internal distributed member
        member = r.getVersionMember();
      }
      return rvv.contains(member, stamp.getRegionVersion());
    }

  }

  /**
   * Clear the pending async bit on a disk entry.
   */
  private void unsetPendingAsync(AsyncDiskEntry ade) {
    DiskId did = ade.de.getDiskId();
    if (did != null && did.isPendingAsync()) {
      synchronized (did) {
        did.setPendingAsync(false);
      }
    }
  }

  private Thread flusherThread;
  /**
   * How many threads are waiting to do a put on asyncQueue?
   */
  private final AtomicInteger pendingAsyncEnqueue = new AtomicInteger();
  private volatile boolean stoppingFlusher;
  private volatile boolean stopFlusher;
  private volatile boolean flusherThreadTerminated;

  private void startAsyncFlusher() {
    final String thName = LocalizedStrings.DiskRegion_ASYNCHRONOUS_DISK_WRITER_0
        .toLocalizedString(new Object[] {getName()});
    this.flusherThread = new Thread(
        LoggingThreadGroup.createThreadGroup(
            LocalizedStrings.DiskRegion_DISK_WRITERS.toLocalizedString(), logger),
        new FlusherThread(), thName);
    this.flusherThread.setDaemon(true);
    this.flusherThread.start();
  }

  private void stopAsyncFlusher() {
    this.stoppingFlusher = true;
    do {
      // Need to keep looping as long as we have more threads
      // that are already pending a put on the asyncQueue.
      // New threads will fail because stoppingFlusher has been set.
      // See bug 41141.
      forceFlush();
    } while (this.pendingAsyncEnqueue.get() > 0);
    synchronized (asyncMonitor) {
      this.stopFlusher = true;
      this.asyncMonitor.notifyAll();
    }
    while (!this.flusherThreadTerminated) {
      try {
        this.flusherThread.join(100);
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        getCache().getCancelCriterion().checkCancelInProgress(ie);
      }
    }
  }

  public boolean testWaitForAsyncFlusherThread(int waitMs) {
    try {
      this.flusherThread.join(waitMs);
      return true;
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
    return false;
  }

  /**
   * force a flush but do it async (don't wait for the flush to complete).
   */
  public void asynchForceFlush() {
    try {
      flushFlusher(true);
    } catch (InterruptedException ignore) {
    }
  }

  public GemFireCacheImpl getCache() {
    return this.cache;
  }

  public void flush() {
    forceFlush();
  }

  public void forceFlush() {
    try {
      flushFlusher(false);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      getCache().getCancelCriterion().checkCancelInProgress(ie);
    }
  }

  private boolean isFlusherTerminated() {
    return this.stopFlusher || this.flusherThreadTerminated || this.flusherThread == null
        || !this.flusherThread.isAlive();
  }

  private void flushFlusher(boolean async) throws InterruptedException {
    if (!isFlusherTerminated()) {
      FlushNotifier fn = new FlushNotifier();
      addAsyncItem(fn, true);
      if (isFlusherTerminated()) {
        rmAsyncItem(fn);
      } else {
        incForceFlush();
        if (!async) {
          fn.waitForFlush();
        }
      }
    }
  }

  private void incForceFlush() {
    synchronized (this.asyncMonitor) {
      this.forceFlushCount.incrementAndGet(); // moved inside sync to fix bug
                                              // 41654
      this.asyncMonitor.notifyAll();
    }
  }

  /**
   * Return true if a non-zero value is found and the decrement was done.
   */
  private boolean checkAndClearForceFlush() {
    if (stopFlusher) {
      return true;
    }
    boolean done = false;
    boolean result;
    do {
      int v = this.forceFlushCount.get();
      result = v > 0;
      if (result) {
        done = this.forceFlushCount.compareAndSet(v, 0);
      }
    } while (result && !done);
    return result;
  }

  private class FlushPauser extends FlushNotifier {
    @Override
    public synchronized void doFlush() {
      // this is called by flusher thread so have it wait
      try {
        super.waitForFlush();
      } catch (InterruptedException ignore) {
        Thread.currentThread().interrupt();
      }
    }

    public synchronized void unpause() {
      super.doFlush();
    }

    @Override
    protected boolean isStoppingFlusher() {
      return stoppingFlusher;
    }
  }

  private class FlushNotifier {
    private boolean flushed;

    protected boolean isStoppingFlusher() {
      return false;
    }

    public synchronized void waitForFlush() throws InterruptedException {
      while (!flushed && !isFlusherTerminated() && !isStoppingFlusher()) {
        wait(333);
      }
    }

    public synchronized void doFlush() {
      this.flushed = true;
      notifyAll();
    }
  }

  /**
   * Return true if we have enough async items to do a flush
   */
  private boolean checkAsyncItemLimit() {
    return this.asyncQueue.size() >= this.maxAsyncItems;
  }

  private class FlusherThread implements Runnable {
    private boolean waitUntilFlushIsReady() throws InterruptedException {
      if (maxAsyncItems > 0) {
        final long time = getTimeInterval();
        synchronized (asyncMonitor) {
          if (time > 0) {
            long nanosRemaining = TimeUnit.MILLISECONDS.toNanos(time);
            final long endTime = System.nanoTime() + nanosRemaining;
            boolean done = checkAndClearForceFlush() || checkAsyncItemLimit();
            while (!done && nanosRemaining > 0) {
              TimeUnit.NANOSECONDS.timedWait(asyncMonitor, nanosRemaining);
              done = checkAndClearForceFlush() || checkAsyncItemLimit();
              if (!done) {
                nanosRemaining = endTime - System.nanoTime();
              }
            }
          } else {
            boolean done = checkAndClearForceFlush() || checkAsyncItemLimit();
            while (!done) {
              asyncMonitor.wait();
              done = checkAndClearForceFlush() || checkAsyncItemLimit();
            }
          }
        }
      } else {
        long time = getTimeInterval();
        if (time > 0) {
          long nanosRemaining = TimeUnit.MILLISECONDS.toNanos(time);
          final long endTime = System.nanoTime() + nanosRemaining;
          synchronized (asyncMonitor) {
            boolean done = checkAndClearForceFlush();
            while (!done && nanosRemaining > 0) {
              TimeUnit.NANOSECONDS.timedWait(asyncMonitor, nanosRemaining);
              done = checkAndClearForceFlush();
              if (!done) {
                nanosRemaining = endTime - System.nanoTime();
              }
            }
          }
        } else {
          // wait for a forceFlush
          synchronized (asyncMonitor) {
            boolean done = checkAndClearForceFlush();
            while (!done) {
              asyncMonitor.wait();
              done = checkAndClearForceFlush();
            }
          }
        }
      }
      return !stopFlusher;
    }

    private void flushChild() {
      persistentOplogs.flushChild();
    }

    public void run() {
      DiskAccessException fatalDae = null;
      if (logger.isDebugEnabled()) {
        logger.debug("Async writer thread started");
      }
      boolean doingFlush = false;
      try {
        while (waitUntilFlushIsReady()) {
          int drainCount = fillDrainList();
          if (drainCount > 0) {
            stats.incQueueSize(-drainCount);
            Iterator it = getDrainList().iterator();
            while (it.hasNext()) {
              Object o = it.next();
              if (o instanceof FlushNotifier) {
                flushChild();
                if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
                  if (!it.hasNext()) {
                    doingFlush = false;
                    CacheObserverHolder.getInstance().afterWritingBytes();
                  }
                }
                ((FlushNotifier) o).doFlush();
              } else {
                try {
                  if (o != null && o instanceof LocalRegion) {
                    LocalRegion lr = (LocalRegion) o;
                    lr.getDiskRegion().writeRVV(null, true);
                    lr.getDiskRegion().writeRVVGC(lr);
                  } else {
                    AsyncDiskEntry ade = (AsyncDiskEntry) o;
                    LocalRegion region = ade.region;
                    VersionTag tag = ade.tag;
                    if (ade.versionOnly) {
                      DiskEntry.Helper.doAsyncFlush(tag, region);
                    } else {
                      DiskEntry entry = ade.de;
                      // We check isPendingAsync
                      if (entry.getDiskId().isPendingAsync()) {
                        if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
                          if (!doingFlush) {
                            doingFlush = true;
                            CacheObserverHolder.getInstance().goingToFlush();
                          }
                        }
                        DiskEntry.Helper.doAsyncFlush(entry, region, tag);
                      } else {
                        // If it is no longer pending someone called
                        // unscheduleAsyncWrite
                        // so we don't need to write the entry, but
                        // if we have a version tag we need to record the
                        // operation
                        // to update the RVV
                        if (tag != null) {
                          DiskEntry.Helper.doAsyncFlush(tag, region);
                        }
                      }
                    }
                  } // else
                } catch (RegionDestroyedException ex) {
                  // Normally we flush before closing or destroying a region
                  // but in some cases it is closed w/o flushing.
                  // So just ignore it; see bug 41305.
                }
              }
            }
            flushChild();
            if (doingFlush) {
              doingFlush = false;
              if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
                CacheObserverHolder.getInstance().afterWritingBytes();
              }
            }
          }
        }
      } catch (InterruptedException ie) {
        flushChild();
        Thread.currentThread().interrupt();
        getCache().getCancelCriterion().checkCancelInProgress(ie);
        throw new IllegalStateException("Async writer thread stopping due to unexpected interrupt");
      } catch (DiskAccessException dae) {
        boolean okToIgnore = dae.getCause() instanceof ClosedByInterruptException;
        if (!okToIgnore || !stopFlusher) {
          fatalDae = dae;
        }
      } catch (CancelException ignore) {
      } catch (Throwable t) {
        logger.fatal(LocalizedMessage.create(LocalizedStrings.DiskStoreImpl_FATAL_ERROR_ON_FLUSH),
            t);
        fatalDae = new DiskAccessException(
            LocalizedStrings.DiskStoreImpl_FATAL_ERROR_ON_FLUSH.toLocalizedString(), t,
            DiskStoreImpl.this);
      } finally {
        if (logger.isDebugEnabled()) {
          logger.debug("Async writer thread stopped. Pending opcount={}", asyncQueue.size());
        }
        flusherThreadTerminated = true;
        stopFlusher = true; // set this before calling handleDiskAccessException
        // or it will hang
        if (fatalDae != null) {
          handleDiskAccessException(fatalDae);
        }
      }
    }
  }

  // simple code
  /** Extension of the oplog lock file * */
  private static final String LOCK_FILE_EXT = ".lk";
  private FileLock fl;
  private File lockFile;

  private void createLockFile(String name) throws DiskAccessException {
    File f = new File(getInfoFileDir().getDir(), "DRLK_IF" + name + LOCK_FILE_EXT);
    if (logger.isDebugEnabled()) {
      logger.debug("Creating lock file {}", f);
    }
    FileOutputStream fs = null;
    // 41734: A known NFS issue on Redhat. The thread created the directory,
    // but when it try to lock, it will fail with permission denied or
    // input/output
    // error. To workarround it, introduce 5 times retries.
    int cnt = 0;
    DiskAccessException dae = null;
    do {
      try {
        fs = new FileOutputStream(f);
        this.lockFile = f;
        this.fl = fs.getChannel().tryLock();
        if (fl == null) {
          try {
            fs.close();
          } catch (IOException ignore) {
          }
          throw new IOException(LocalizedStrings.Oplog_THE_FILE_0_IS_BEING_USED_BY_ANOTHER_PROCESS
              .toLocalizedString(f));
        }
        f.deleteOnExit();
        dae = null;
        break;
      } catch (IOException ex) {
        if (fs != null) {
          try {
            fs.close();
          } catch (IOException ignore) {
          }
        }
        dae = new DiskAccessException(
            LocalizedStrings.Oplog_COULD_NOT_LOCK_0.toLocalizedString(f.getPath()), ex, this);
      } catch (IllegalStateException ex2) {
        // OverlappingFileLockExtension needs to be caught here see bug 41290
        if (fs != null) {
          try {
            fs.close();
          } catch (IOException ignore) {
          }
        }
        dae = new DiskAccessException(
            LocalizedStrings.Oplog_COULD_NOT_LOCK_0.toLocalizedString(f.getPath()), ex2, this);
      }
      cnt++;
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } while (cnt < 100);
    if (dae != null) {
      throw dae;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Locked disk store {} for exclusive access in directory: {}", name,
          getInfoFileDir().getDir());
    }
  }

  void closeLockFile() {
    FileLock myfl = this.fl;
    if (myfl != null) {
      try {
        FileChannel fc = myfl.channel();
        if (myfl.isValid()) {
          myfl.release();
        }
        fc.close();
      } catch (IOException ignore) {
      }
      this.fl = null;
    }
    File f = this.lockFile;
    if (f != null) {
      if (f.delete()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Deleted lock file {}", f);
        }
      } else if (f.exists()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Could not delete lock file {}", f);
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Unlocked disk store {}", name);
    }
  }

  private String getRecoveredGFVersionName() {
    String currentVersionStr = "GFE pre-7.0";
    Version version = getRecoveredGFVersion();
    if (version != null) {
      currentVersionStr = version.toString();
    }
    return currentVersionStr;
  }

  /**
   * Searches the given disk dirs for the files and creates the Oplog objects wrapping those files
   */
  private void loadFiles(boolean needsOplogs) {
    String partialFileName = getName();
    boolean foundIfFile = false;
    {
      // Figure out what directory the init file is in (if we even have one).
      // Also detect multiple if files and fail (see bug 41883).
      int ifDirIdx = 0;
      int idx = 0;
      String ifName = "BACKUP" + name + DiskInitFile.IF_FILE_EXT;
      for (DirectoryHolder dh : this.directories) {
        File f = new File(dh.getDir(), ifName);
        if (f.exists()) {
          if (foundIfFile) {
            throw new IllegalStateException(
                "Detected multiple disk store initialization files named \"" + ifName
                    + "\". This disk store directories must only contain one initialization file.");
          } else {
            foundIfFile = true;
            ifDirIdx = idx;
          }
        }
        idx++;
      }
      this.infoFileDirIndex = ifDirIdx;
    }
    // get a high level lock file first; if we can't get this then
    // this disk store is already open be someone else
    createLockFile(partialFileName);
    boolean finished = false;
    try {
      Map<File, DirectoryHolder> persistentBackupFiles =
          persistentOplogs.findFiles(partialFileName);
      {

        boolean backupFilesExist = !persistentBackupFiles.isEmpty();
        boolean ifRequired = backupFilesExist || isOffline();

        this.initFile =
            new DiskInitFile(partialFileName, this, ifRequired, persistentBackupFiles.keySet());
        if (this.upgradeVersionOnly) {
          if (Version.CURRENT.compareTo(getRecoveredGFVersion()) <= 0) {
            if (getCache() != null) {
              getCache().close();
            }
            throw new IllegalStateException("Recovered version = " + getRecoveredGFVersion() + ": "
                + LocalizedStrings.DiskStoreAlreadyInVersion_0
                    .toLocalizedString(getRecoveredGFVersionName()));
          }
        } else {
          if (Version.GFE_70.compareTo(getRecoveredGFVersion()) > 0) {
            // TODO: In each new version, need to modify the highest version
            // that needs converstion.
            if (getCache() != null) {
              getCache().close();
            }
            throw new IllegalStateException("Recovered version = " + getRecoveredGFVersion() + ": "
                + LocalizedStrings.DiskStoreStillAtVersion_0
                    .toLocalizedString(getRecoveredGFVersionName()));
          }
        }
      }

      {
        FilenameFilter overflowFileFilter =
            new DiskStoreFilter(OplogType.OVERFLOW, true, partialFileName);
        for (DirectoryHolder dh : this.directories) {
          File dir = dh.getDir();
          // delete all overflow files
          File[] files = FileUtil.listFiles(dir, overflowFileFilter);
          for (File file : files) {
            boolean deleted = file.delete();
            if (!deleted && file.exists() && logger.isDebugEnabled()) {
              logger.debug("Could not delete file {}", file);
            }
          }
        }
      }

      persistentOplogs.createOplogs(needsOplogs, persistentBackupFiles);
      finished = true;

      // Log a message with the disk store id, indicating whether we recovered
      // or created thi disk store.
      if (foundIfFile) {
        logger.info(
            LocalizedMessage.create(LocalizedStrings.DiskStoreImpl_RecoveredDiskStore_0_With_Id_1,
                new Object[] {getName(), getDiskStoreID()}));
      } else {
        logger.info(
            LocalizedMessage.create(LocalizedStrings.DiskStoreImpl_CreatedDiskStore_0_With_Id_1,
                new Object[] {getName(), getDiskStoreID()}));
      }

    } finally {
      if (!finished) {
        closeLockFile();
        if (getDiskInitFile() != null) {
          getDiskInitFile().close();
        }
      }
    }
  }

  /**
   * The diskStats are at PR level.Hence if the region is a bucket region, the stats should not be
   * closed, but the figures of entriesInVM and overflowToDisk contributed by that bucket need to be
   * removed from the stats .
   */
  private void statsClose() {
    this.stats.close();
    if (this.directories != null) {
      for (int i = 0; i < this.directories.length; i++) {
        this.directories[i].close();
      }
    }
  }

  void initializeIfNeeded() {
    if (!persistentOplogs.alreadyRecoveredOnce.get()) {
      recoverRegionsThatAreReady();
    }
  }

  void doInitialRecovery() {
    initializeIfNeeded();
  }

  /**
   * Reads the oplogs files and loads them into regions that are ready to be recovered.
   */
  public final void recoverRegionsThatAreReady() {
    persistentOplogs.recoverRegionsThatAreReady();
  }

  void scheduleValueRecovery(Set<Oplog> oplogsNeedingValueRecovery,
      Map<Long, DiskRecoveryStore> recoveredStores) {
    ValueRecoveryTask task = new ValueRecoveryTask(oplogsNeedingValueRecovery, recoveredStores);
    synchronized (currentAsyncValueRecoveryMap) {
      DiskStoreImpl.this.currentAsyncValueRecoveryMap.putAll(recoveredStores);
    }
    executeDiskStoreTask(task);
  }

  /**
   * get the directory which has the info file
   * 
   * @return directory holder which has the info file
   */
  DirectoryHolder getInfoFileDir() {
    return this.directories[this.infoFileDirIndex];
  }

  /** For Testing * */
  // void addToOplogSet(int oplogID, File opFile, DirectoryHolder dirHolder) {
  // Oplog oplog = new Oplog(oplogID, this);
  // oplog.addRecoveredFile(opFile, dirHolder);
  // // @todo check callers to see if they need drf support
  // this.oplogSet.add(oplog);
  // }

  /** For Testing * */
  /**
   * returns the size of the biggest directory available to the region
   * 
   */
  public long getMaxDirSize() {
    return maxDirSize;
  }

  /**
   * 
   * @return boolean indicating whether the disk region compaction is on or not
   */
  boolean isCompactionEnabled() {
    return getAutoCompact();
  }

  public int getCompactionThreshold() {
    return this.compactionThreshold;
  }

  private final boolean isCompactionPossible;

  final boolean isCompactionPossible() {
    return this.isCompactionPossible;
  }

  void scheduleCompaction() {
    if (isCompactionEnabled() && !isOfflineCompacting()) {
      this.oplogCompactor.scheduleIfNeeded(getOplogToBeCompacted());
    }
  }

  /**
   * All the oplogs except the current one are destroyed.
   * 
   * @param rvv if not null, clear the region using a version vector Clearing with a version vector
   *        only removes entries less than the version vector, which allows for a consistent clear
   *        across members.
   */
  private void basicClear(LocalRegion region, DiskRegion dr, RegionVersionVector rvv) {
    if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
      CacheObserverHolder.getInstance().beforeDiskClear();
    }
    if (region != null) {
      clearAsyncQueue(region, false, rvv);
      // to fix bug 41770 need to wait for async flusher thread to finish
      // any work it is currently doing since it might be doing an operation on
      // this region.
      // If I call forceFlush here I might wait forever since I hold the
      // writelock
      // this preventing the async flush from finishing.
      // Can I set some state that will cause the flusher to ignore records
      // it currently has in it's hand for region?
      // Bug 41770 is caused by us doing a regionMap.clear at the end of this
      // method.
      // That causes any entry mod for this region that the async flusher has a
      // ref to
      // to end up being written as a create. We then end up writing another
      // create
      // since the first create is not in the actual region map.
      clearDrainList(region, rvv);
    }

    if (rvv == null) {
      // if we have an RVV, the stats are updated by AbstractRegionMap.clear
      // removing each entry.
      dr.statsClear(region);
    }

    if (dr.isBackup()) {
      PersistentOplogSet oplogSet = getPersistentOplogSet(dr);
      oplogSet.clear(dr, rvv);
    } else if (rvv == null) {
      // For an RVV based clear on an overflow region, freeing entries is
      // handled in
      // AbstractRegionMap.clear
      dr.freeAllEntriesOnDisk(region);
    }
  }



  /**
   * Removes anything found in the async queue for the given region
   * 
   * @param rvv
   */
  private void clearAsyncQueue(LocalRegion region, boolean needsWriteLock,
      RegionVersionVector rvv) {
    DiskRegion dr = region.getDiskRegion();
    if (needsWriteLock) {
      acquireWriteLock(dr);
    }
    try {
      // Now while holding the write lock remove any elements from the queue
      // for this region.
      Iterator<Object> it = this.asyncQueue.iterator();
      while (it.hasNext()) {
        Object o = it.next();
        if (o instanceof AsyncDiskEntry) {
          AsyncDiskEntry ade = (AsyncDiskEntry) o;
          if (shouldClear(region, rvv, ade)) {
            rmAsyncItem(o);
          }
        }
      }
    } finally {
      if (needsWriteLock) {
        releaseWriteLock(dr);
      }
    }
  }

  /**
   * Obtained and held by clear/destroyRegion/close. Also obtained when adding to async queue.
   */
  private final Object lock = new Object();

  /**
   * It invokes appropriate methods of super & current class to clear the Oplogs.
   * 
   * @param rvv if not null, clear the region using the version vector
   */
  void clear(LocalRegion region, DiskRegion dr, RegionVersionVector rvv) {
    acquireCompactorWriteLock();
    try {
      // get lock on sizeGuard first to avoid deadlock that occurred in bug
      // #46133
      Object regionLock = region == null ? new Object() : region.getSizeGuard();
      synchronized (regionLock) {
        synchronized (this.lock) {
          // if (this.oplogCompactor != null) {
          // this.oplogCompactor.stopCompactor();
          // }
          acquireWriteLock(dr);
          try {
            if (dr.isRegionClosed()) {
              throw new RegionDestroyedException(
                  LocalizedStrings.DiskRegion_THE_DISKREGION_HAS_BEEN_CLOSED_OR_DESTROYED
                      .toLocalizedString(),
                  dr.getName());
            }
            basicClear(region, dr, rvv);
            if (rvv == null && region != null) {
              // If we have no RVV, clear the region under lock
              region.txClearRegion();
              region.clearEntries(null);
              dr.incClearCount();
            }
          } finally {
            releaseWriteLock(dr);
          }
          // if (this.oplogCompactor != null) {
          // this.oplogCompactor.startCompactor();
          // scheduleCompaction();
          // }
        }
      }
    } finally {
      releaseCompactorWriteLock();
    }

    if (rvv != null && region != null) {
      // If we have an RVV, we need to clear the region
      // without holding a lock.
      region.txClearRegion();
      region.clearEntries(rvv);
      // Note, do not increment the clear count in this case.
    }
  }

  private void releaseCompactorWriteLock() {
    compactorWriteLock.unlock();
  }

  private void acquireCompactorWriteLock() {
    compactorWriteLock.lock();
  }

  public void releaseCompactorReadLock() {
    compactorReadLock.unlock();
  }

  public void acquireCompactorReadLock() {
    compactorReadLock.lock();
  }

  private volatile boolean closing = false;
  private volatile boolean closed = false;

  boolean isClosing() {
    return this.closing;
  }

  boolean isClosed() {
    return this.closed;
  }

  public void close() {
    close(false);
  }

  protected void waitForClose() {
    if (diskException.get() != null) {
      try {
        _testHandleDiskAccessException.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  void close(boolean destroy) {
    this.closing = true;
    getCache().getDiskStoreMonitor().removeDiskStore(this);

    RuntimeException rte = null;
    try {
      try {
        closeCompactor(false);
      } catch (RuntimeException e) {
        rte = e;
      }
      if (!isOffline()) {
        try {
          // do this before write lock
          stopAsyncFlusher();
        } catch (RuntimeException e) {
          if (rte != null) {
            rte = e;
          }
        }
      }

      // Wakeup any threads waiting for the asnyc disk store recovery.
      synchronized (currentAsyncValueRecoveryMap) {
        currentAsyncValueRecoveryMap.notifyAll();
      }

      // don't block the shutdown hook
      if (Thread.currentThread() != InternalDistributedSystem.shutdownHook) {
        waitForBackgroundTasks();
      }
      try {
        overflowOplogs.closeOverflow();
      } catch (RuntimeException e) {
        if (rte != null) {
          rte = e;
        }
      }

      if ((!destroy && getDiskInitFile().hasLiveRegions()) || isValidating()) {
        RuntimeException exception = persistentOplogs.close();
        if (exception != null && rte != null) {
          rte = exception;
        }
        getDiskInitFile().close();
      } else {
        try {
          destroyAllOplogs();
        } catch (RuntimeException e) {
          if (rte != null) {
            rte = e;
          }
        }

        getDiskInitFile().close();
      }
      try {
        statsClose();
      } catch (RuntimeException e) {
        if (rte != null) {
          rte = e;
        }
      }

      closeLockFile();
      if (rte != null) {
        throw rte;
      }
    } finally {
      this.closed = true;
    }
  }

  final DiskAccessException getDiskAccessException() {
    return diskException.get();
  }

  boolean allowKrfCreation() {
    // Compactor might be stopped by cache-close. In that case, we should not create krf
    return diskException.get() == null
        && (this.oplogCompactor == null || this.oplogCompactor.keepCompactorRunning());
  }

  void closeCompactor(boolean isPrepare) {
    if (this.oplogCompactor == null) {
      return;
    }
    if (isPrepare) {
      acquireCompactorWriteLock();
    }
    try {
      synchronized (this.lock) {
        // final boolean orig =
        // this.oplogCompactor.compactionCompletionRequired;
        try {
          // to fix bug 40473 don't wait for the compactor to complete.
          // this.oplogCompactor.compactionCompletionRequired = true;
          this.oplogCompactor.stopCompactor();
        } catch (CancelException ignore) {
          // Asif:To fix Bug 39380 , ignore the cache closed exception here.
          // allow it to call super .close so that it would be able to close
          // the
          // oplogs
          // Though I do not think this exception will be thrown by
          // the stopCompactor. Still not taking chance and ignoring it

        } catch (RuntimeException e) {
          logger.warn(
              LocalizedMessage.create(
                  LocalizedStrings.DiskRegion_COMPLEXDISKREGION_CLOSE_EXCEPTION_IN_STOPPING_COMPACTOR),
              e);
          throw e;
          // } finally {
          // this.oplogCompactor.compactionCompletionRequired = orig;
        }
      }
    } finally {
      if (isPrepare) {
        releaseCompactorWriteLock();
      }
    }
  }

  private void basicClose(LocalRegion region, DiskRegion dr, boolean closeDataOnly) {
    if (dr.isBackup()) {
      if (region != null) {
        region.closeEntries();
      }
      if (!closeDataOnly) {
        getDiskInitFile().closeRegion(dr);
      }
      // call close(dr) on each oplog
      PersistentOplogSet oplogSet = getPersistentOplogSet(dr);
      oplogSet.basicClose(dr);
    } else {
      if (region != null) {
        // OVERFLOW ONLY
        clearAsyncQueue(region, true, null); // no need to try to write these to
                                             // disk any longer
        dr.freeAllEntriesOnDisk(region);
        region.closeEntries();
        this.overflowMap.remove(dr);
      }
    }
  }

  /**
   * Called before LocalRegion clears the contents of its entries map
   */
  void prepareForClose(LocalRegion region, DiskRegion dr) {
    if (dr.isBackup()) {
      // Need to flush any async ops done on dr.
      // The easiest way to do this is to flush the entire async queue.
      forceFlush();
    }
  }

  public void prepareForClose() {
    forceFlush();
    persistentOplogs.prepareForClose();
    closeCompactor(true);
  }



  void close(LocalRegion region, DiskRegion dr, boolean closeDataOnly) {
    // CancelCriterion stopper = dr.getOwner().getCancelCriterion();

    if (logger.isDebugEnabled()) {
      logger.debug("DiskRegion::close:Attempting to close DiskRegion. Region name ={}",
          dr.getName());
    }

    boolean closeDiskStore = false;
    acquireCompactorWriteLock();
    try {
      // Fix for 46284 - we must obtain the size guard lock before getting the
      // disk
      // store lock
      Object regionLock = region == null ? new Object() : region.getSizeGuard();
      synchronized (regionLock) {
        synchronized (this.lock) {
          // Fix 45104, wait here for addAsyncItem to finish adding into queue
          // prepareForClose() should be out of synchronized (this.lock) to avoid deadlock
          if (dr.isRegionClosed()) {
            return;
          }
        }
        prepareForClose(region, dr);
        synchronized (this.lock) {
          boolean gotLock = false;
          try {
            acquireWriteLock(dr);
            if (!closeDataOnly) {
              dr.setRegionClosed(true);
            }
            gotLock = true;
          } catch (CancelException e) {
            synchronized (this.closeRegionGuard) {
              if (!dr.isRegionClosed()) {
                if (!closeDataOnly) {
                  dr.setRegionClosed(true);
                }
                // Asif: I am quite sure that it should also be Ok if instead
                // while it is a If Check below. Because if acquireReadLock
                // thread
                // has acquired thelock, it is bound to see the isRegionClose as
                // true
                // and so will realse teh lock causing decrement to zeo , before
                // releasing the closeRegionGuard. But still...not to take any
                // chance

                while (this.entryOpsCount.get() > 0) {
                  try {
                    this.closeRegionGuard.wait(20000);
                  } catch (InterruptedException ie) {
                    // Exit without closing the region, do not know what else
                    // can be done
                    Thread.currentThread().interrupt();
                    dr.setRegionClosed(false);
                    return;
                  }
                }

              } else {
                return;
              }
            }

          }

          try {
            if (logger.isDebugEnabled()) {
              logger.debug("DiskRegion::close:Before invoking basic Close. Region name ={}",
                  dr.getName());
            }
            basicClose(region, dr, closeDataOnly);
          } finally {
            if (gotLock) {
              releaseWriteLock(dr);
            }
          }
        }
      }

      if (getOwnedByRegion() && !closeDataOnly) {
        if (this.ownCount.decrementAndGet() <= 0) {
          closeDiskStore = true;
        }
      }
    } finally {
      releaseCompactorWriteLock();
    }

    // Fix for 44538 - close the disk store without holding
    // the compactor write lock.
    if (closeDiskStore) {
      cache.removeDiskStore(this);
      close();
    }
  }

  /**
   * stops the compactor outside the write lock. Once stopped then it proceeds to destroy the
   * current & old oplogs
   * 
   * @param dr
   */
  void beginDestroyRegion(LocalRegion region, DiskRegion dr) {
    if (dr.isBackup()) {
      getDiskInitFile().beginDestroyRegion(dr);
    }
  }

  private final AtomicInteger backgroundTasks = new AtomicInteger();

  int incBackgroundTasks() {
    getCache().getCachePerfStats().incDiskTasksWaiting();
    int v = this.backgroundTasks.incrementAndGet();
    return v;
  }

  void decBackgroundTasks() {
    int v = this.backgroundTasks.decrementAndGet();
    if (v == 0) {
      synchronized (this.backgroundTasks) {
        this.backgroundTasks.notifyAll();
      }
    }
    getCache().getCachePerfStats().decDiskTasksWaiting();
  }

  private void waitForBackgroundTasks() {
    if (isBackgroundTaskThread()) {
      return; // fixes bug 42775
    }
    if (this.backgroundTasks.get() > 0) {
      boolean interrupted = Thread.interrupted();
      try {
        synchronized (this.backgroundTasks) {
          while (this.backgroundTasks.get() > 0) {
            try {
              this.backgroundTasks.wait(500L);
            } catch (InterruptedException ex) {
              interrupted = true;
            }
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  boolean basicForceCompaction(DiskRegion dr) {
    PersistentOplogSet oplogSet = persistentOplogs;
    // see if the current active oplog is compactable; if so
    {
      Oplog active = oplogSet.getChild();
      if (active != null) {
        if (active.hadLiveEntries() && active.needsCompaction()) {
          active.forceRolling(dr);
        }
      }
    }

    // Compact the oplogs
    CompactableOplog[] oplogs = getOplogsToBeCompacted(true/* fixes 41143 */);
    // schedule a compaction if at this point there are oplogs to be compacted
    if (oplogs != null) {
      if (this.oplogCompactor != null) {
        if (this.oplogCompactor.scheduleIfNeeded(oplogs)) {
          this.oplogCompactor.waitForRunToComplete();
        } else {
          oplogs = null;
          // @todo darrel: still need to schedule oplogs and wait for them to
          // compact.
        }
      }
    }
    return oplogs != null;
  }

  /**
   * Destroy the given region
   */
  private void basicDestroy(LocalRegion region, DiskRegion dr) {
    if (dr.isBackup()) {
      if (region != null) {
        region.closeEntries();
      }
      PersistentOplogSet oplogSet = getPersistentOplogSet(dr);
      oplogSet.basicDestroy(dr);
    } else {
      dr.freeAllEntriesOnDisk(region);
      if (region != null) {
        region.closeEntries();
      }
    }
  }

  /**
   * Destroy all the oplogs
   * 
   */
  private void destroyAllOplogs() {
    persistentOplogs.destroyAllOplogs();

    // Need to also remove all oplogs that logically belong to this DiskStore
    // even if we were not using them.
    { // delete all overflow oplog files
      FilenameFilter overflowFileFilter = new DiskStoreFilter(OplogType.OVERFLOW, true, getName());
      deleteFiles(overflowFileFilter);
    }
    { // delete all backup oplog files
      FilenameFilter backupFileFilter = new DiskStoreFilter(OplogType.BACKUP, true, getName());
      deleteFiles(backupFileFilter);
    }
  }

  private void deleteFiles(FilenameFilter overflowFileFilter) {
    for (int i = 0; i < this.directories.length; i++) {
      File dir = this.directories[i].getDir();
      File[] files = FileUtil.listFiles(dir, overflowFileFilter);
      for (File file : files) {
        boolean deleted = file.delete();
        if (!deleted && file.exists() && logger.isDebugEnabled()) {
          logger.debug("Could not delete file {}", file);
        }
      }
    }
  }

  public void destroy() {
    Set<String> liveRegions = new TreeSet<String>();
    for (AbstractDiskRegion dr : getDiskRegions()) {
      liveRegions.add(dr.getName());
    }
    for (AbstractDiskRegion dr : overflowMap) {
      liveRegions.add(dr.getName());
    }
    if (!liveRegions.isEmpty()) {
      throw new IllegalStateException(
          "Disk store is currently in use by these regions " + liveRegions);
    }
    close(true);
    getDiskInitFile().destroy();
    cache.removeDiskStore(this);
  }

  /**
   * gets the available oplogs to be compacted from the LinkedHashMap
   * 
   * @return Oplog[] returns the array of oplogs to be compacted if present else returns null
   */
  CompactableOplog[] getOplogToBeCompacted() {
    return getOplogsToBeCompacted(false);
  }

  /**
   * Test hook to see how many oplogs are available for compaction
   */
  public int numCompactableOplogs() {
    CompactableOplog[] oplogs = getOplogsToBeCompacted(true);
    if (oplogs == null) {
      return 0;
    } else {
      return oplogs.length;
    }

  }

  private CompactableOplog[] getOplogsToBeCompacted(boolean all) {
    ArrayList<CompactableOplog> l = new ArrayList<CompactableOplog>();

    int max = Integer.MAX_VALUE;
    if (!all && max > MAX_OPLOGS_PER_COMPACTION && MAX_OPLOGS_PER_COMPACTION > 0) {
      max = MAX_OPLOGS_PER_COMPACTION;
    }
    persistentOplogs.getCompactableOplogs(l, max);

    // Note this always puts overflow oplogs on the end of the list.
    // They may get starved.
    overflowOplogs.getCompactableOplogs(l, max);

    if (l.isEmpty()) {
      return null;
    }

    return l.toArray(new CompactableOplog[0]);
  }

  /**
   * Returns the dir name used to back up this DiskStore's directories under. The name is a
   * concatenation of the disk store name and id.
   */
  public String getBackupDirName() {
    String name = getName();

    if (name == null) {
      name = GemFireCacheImpl.DEFAULT_DS_NAME;
    }

    return (name + "_" + getDiskStoreID().toString());
  }

  /**
   * Filters and returns the current set of oplogs that aren't already in the baseline for
   * incremental backup
   * 
   * @param baselineInspector the inspector for the previous backup.
   * @param baselineCopyMap this will be populated with baseline oplogs Files that will be used in
   *        the restore script.
   * @return an array of Oplogs to be copied for an incremental backup.
   * @throws IOException
   */
  private Oplog[] filterBaselineOplogs(BackupInspector baselineInspector,
      Map<File, File> baselineCopyMap) throws IOException {
    File baselineDir = new File(baselineInspector.getBackupDir(), BackupManager.DATA_STORES);
    baselineDir = new File(baselineDir, getBackupDirName());

    // Find all of the member's diskstore oplogs in the member's baseline
    // diskstore directory structure (*.crf,*.krf,*.drf)
    List<File> baselineOplogFiles = FileUtil.findAll(baselineDir, ".*\\.[kdc]rf$");

    // Our list of oplogs to copy (those not already in the baseline)
    List<Oplog> oplogList = new LinkedList<Oplog>();

    // Total list of member oplogs
    Oplog[] allOplogs = getAllOplogsForBackup();

    /*
     * Loop through operation logs and see if they are already part of the baseline backup.
     */
    for (Oplog log : allOplogs) {
      // See if they are backed up in the current baseline
      Map<File, File> oplogMap = log.mapBaseline(baselineOplogFiles);

      // No? Then see if they were backed up in previous baselines
      if (oplogMap.isEmpty() && baselineInspector.isIncremental()) {
        Set<String> matchingOplogs =
            log.gatherMatchingOplogFiles(baselineInspector.getIncrementalOplogFileNames());
        if (!matchingOplogs.isEmpty()) {
          for (String matchingOplog : matchingOplogs) {
            oplogMap.put(new File(baselineInspector.getCopyFromForOplogFile(matchingOplog)),
                new File(baselineInspector.getCopyToForOplogFile(matchingOplog)));
          }
        }
      }

      if (oplogMap.isEmpty()) {
        /*
         * These are fresh operation log files so lets back them up.
         */
        oplogList.add(log);
      } else {
        /*
         * These have been backed up before so lets just add their entries from the previous backup
         * or restore script into the current one.
         */
        baselineCopyMap.putAll(oplogMap);
      }
    }

    // Convert the filtered oplog list to an array
    return oplogList.toArray(new Oplog[] {});
  }



  /**
   * Get all of the oplogs
   */
  private Oplog[] getAllOplogsForBackup() {
    return persistentOplogs.getAllOplogs();
  }

  // @todo perhaps a better thing for the tests would be to give them a listener
  // hook that notifies them every time an oplog is created.
  /**
   * Used by tests to confirm stat size.
   * 
   */
  final AtomicLong undeletedOplogSize = new AtomicLong();

  /**
   * Compacts oplogs
   * 
   * @since GemFire 5.1
   * 
   */
  class OplogCompactor implements Runnable {
    /** boolean for the thread to continue compaction* */
    private volatile boolean compactorEnabled;
    private volatile boolean scheduled;
    private CompactableOplog[] scheduledOplogs;
    /**
     * used to keep track of the Thread currently invoking run on this compactor
     */
    private volatile Thread me;

    // Boolean which decides if the compactor can terminate early i.e midway
    // between compaction.
    // If this boolean is true ,( default is false), then the compactor thread
    // if entered the
    // compaction phase will exit only after it has compacted the oplogs & also
    // deleted the compacted
    // oplogs

    private final boolean compactionCompletionRequired;

    OplogCompactor() {
      this.compactionCompletionRequired =
          Boolean.getBoolean(COMPLETE_COMPACTION_BEFORE_TERMINATION_PROPERTY_NAME);
    }

    /** Creates a new thread and starts the thread* */
    private void startCompactor() {
      this.compactorEnabled = true;
    }

    /**
     * Stops the thread from compaction and the compactor thread joins with the calling thread
     */
    private void stopCompactor() {
      synchronized (this) {
        if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
          CacheObserverHolder.getInstance().beforeStoppingCompactor();
        }
        this.compactorEnabled = false;
        if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
          CacheObserverHolder.getInstance().afterSignallingCompactor();
        }
      }
      if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
        CacheObserverHolder.getInstance().afterStoppingCompactor();
      }
    }

    /**
     * @return true if compaction done; false if it was not
     */
    private synchronized boolean scheduleIfNeeded(CompactableOplog[] opLogs) {
      if (!this.scheduled) {
        return schedule(opLogs);
      } else {
        return false;
      }
    }

    /**
     * @return true if compaction done; false if it was not
     */
    private synchronized boolean schedule(CompactableOplog[] opLogs) {
      assert !this.scheduled;
      if (!this.compactorEnabled)
        return false;
      if (opLogs != null) {
        for (int i = 0; i < opLogs.length; i++) {
          opLogs[i].prepareForCompact();
        }
        this.scheduled = true;
        this.scheduledOplogs = opLogs;
        boolean result = executeDiskStoreTask(this);
        if (!result) {
          reschedule(false);
          return false;
        } else {
          return true;
        }
      } else {
        return false;
      }
    }

    /**
     * A non-backup just needs values that are written to one of the oplogs being compacted that are
     * still alive (have not been deleted or modified in a future oplog) to be copied forward to the
     * current active oplog
     */
    private boolean compact() {
      CompactableOplog[] oplogs = this.scheduledOplogs;
      int totalCount = 0;
      long compactionStart = getStats().startCompaction();
      long start = System.nanoTime();
      try {
        for (int i = 0; i < oplogs.length && keepCompactorRunning() /*
                                                                     * @todo && !owner. isDestroyed
                                                                     */; i++) {
          totalCount += oplogs[i].compact(this);
        }

        // TODO:Asif : DiskRegion: How do we tackle
      } finally {
        getStats().endCompaction(compactionStart);
      }
      long endTime = System.nanoTime();
      logger.info(LocalizedMessage.create(LocalizedStrings.DiskRegion_COMPACTION_SUMMARY,
          new Object[] {totalCount, ((endTime - start) / 1000000)}));
      return true /* @todo !owner.isDestroyed */;
    }

    private boolean isClosing() {
      if (getCache().isClosed()) {
        return true;
      }
      CancelCriterion stopper = getCache().getCancelCriterion();
      if (stopper.isCancelInProgress()) {
        return true;
      }
      return false;
    }

    /**
     * Just do compaction and then check to see if another needs to be done and if so schedule it.
     * Asif:The compactor thread checks for an oplog in the LinkedHasMap in a synchronization on the
     * oplogIdToOplog object. This will ensure that an addition of an Oplog to the Map does not get
     * missed. Notifications need not be sent if the thread is already compaction
     */
    public void run() {
      getCache().getCachePerfStats().decDiskTasksWaiting();
      if (!this.scheduled)
        return;
      boolean compactedSuccessfully = false;
      try {
        SystemFailure.checkFailure();
        if (isClosing()) {
          return;
        }
        if (!this.compactorEnabled)
          return;
        final CompactableOplog[] oplogs = this.scheduledOplogs;
        this.me = Thread.currentThread();
        try {
          // set our thread's name
          String tName = "OplogCompactor " + getName() + " for oplog " + oplogs[0].toString();
          Thread.currentThread().setName(tName);

          StringBuffer buffer = new StringBuffer();
          for (int j = 0; j < oplogs.length; ++j) {
            buffer.append(oplogs[j].toString());
            if (j + 1 < oplogs.length) {
              buffer.append(", ");
            }
          }
          String ids = buffer.toString();
          logger.info(LocalizedMessage.create(LocalizedStrings.DiskRegion_COMPACTION_OPLOGIDS,
              new Object[] {getName(), ids}));
          if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
            CacheObserverHolder.getInstance().beforeGoingToCompact();
          }
          compactedSuccessfully = compact();
          if (compactedSuccessfully) {
            if (LocalRegion.ISSUE_CALLBACKS_TO_CACHE_OBSERVER) {
              CacheObserverHolder.getInstance().afterHavingCompacted();
            }
          } else {
            logger.warn(LocalizedMessage.create(LocalizedStrings.DiskRegion_COMPACTION_FAILURE,
                new Object[] {getName(), ids}));
          }
        } catch (DiskAccessException dae) {
          handleDiskAccessException(dae);
          throw dae;
        } catch (KillCompactorException ex) {
          if (logger.isDebugEnabled()) {
            logger.debug("compactor thread terminated by test");
          }
          throw ex;
        } finally {
          if (compactedSuccessfully) {
            this.me.setName("Idle OplogCompactor");
          }
          this.me = null;
        }
      } catch (CancelException ignore) {
        // if cache is closed, just about the compaction
      } finally {
        reschedule(compactedSuccessfully);
      }
    }

    synchronized void waitForRunToComplete() {
      if (this.me == Thread.currentThread()) {
        // no need to wait since we are the compactor to fix bug 40630
        return;
      }
      while (this.scheduled) {
        try {
          wait();
        } catch (InterruptedException ex) {
          Thread.currentThread().interrupt();
        }
      }
    }

    private synchronized void reschedule(boolean success) {
      this.scheduled = false;
      this.scheduledOplogs = null;
      notifyAll();
      if (!success)
        return;
      if (!this.compactorEnabled)
        return;
      if (isClosing())
        return;
      SystemFailure.checkFailure();
      // synchronized (DiskStoreImpl.this.oplogIdToOplog) {
      if (this.compactorEnabled) {
        if (isCompactionEnabled()) {
          schedule(getOplogToBeCompacted());
        }
      }
      // }
    }

    boolean keepCompactorRunning() {
      return this.compactorEnabled || this.compactionCompletionRequired;
    }
  }

  /**
   * Used by unit tests to kill the compactor operation.
   */
  public static class KillCompactorException extends RuntimeException {
  }

  DiskInitFile getDiskInitFile() {
    return this.initFile;
  }

  public void memberOffline(DiskRegionView dr, PersistentMemberID persistentID) {
    if (this.initFile != null) {
      this.initFile.addOfflinePMID(dr, persistentID);
    }
  }

  public void memberOfflineAndEqual(DiskRegionView dr, PersistentMemberID persistentID) {
    if (this.initFile != null) {
      this.initFile.addOfflineAndEqualPMID(dr, persistentID);
    }
  }

  public void memberOnline(DiskRegionView dr, PersistentMemberID persistentID) {
    if (this.initFile != null) {
      this.initFile.addOnlinePMID(dr, persistentID);
    }
  }

  public void memberRemoved(DiskRegionView dr, PersistentMemberID persistentID) {
    if (this.initFile != null) {
      this.initFile.rmPMID(dr, persistentID);
    }
  }

  public void memberRevoked(PersistentMemberPattern revokedPattern) {
    if (this.initFile != null) {
      this.initFile.revokeMember(revokedPattern);
    }
  }

  public void setInitializing(DiskRegionView dr, PersistentMemberID newId) {
    if (this.initFile != null) {
      this.initFile.addMyInitializingPMID(dr, newId);
    }
  }

  public void setInitialized(DiskRegionView dr) {
    if (this.initFile != null) {
      this.initFile.markInitialized(dr);
    }
  }

  public Set<PersistentMemberPattern> getRevokedMembers() {
    if (this.initFile != null) {
      return this.initFile.getRevokedIDs();
    }
    return Collections.emptySet();
  }

  public void endDestroyRegion(LocalRegion region, DiskRegion dr) {
    // CancelCriterion stopper = dr.getOwner().getCancelCriterion();
    // Fix for 46284 - we must obtain the size guard lock before getting the
    // disk
    // store lock
    Object regionLock = region == null ? new Object() : region.getSizeGuard();
    synchronized (regionLock) {
      synchronized (this.lock) {
        if (dr.isRegionClosed()) {
          return;
        }
        // // Stop the compactor if running, without taking lock.
        // if (this.oplogCompactor != null) {
        // try {
        // this.oplogCompactor.stopCompactor();
        // }
        // catch (CancelException ignore) {
        // // Asif:To fix Bug 39380 , ignore the cache closed exception here.
        // // allow it to call super .close so that it would be able to close
        // the
        // // oplogs
        // // Though I do not think this exception will be thrown by
        // // the stopCompactor. Still not taking chance and ignoring it

        // }
        // }
        // // if (!isSync()) {
        // stopAsyncFlusher(true); // do this before writeLock
        // // }
        boolean gotLock = false;
        try {
          try {
            acquireWriteLock(dr);
            gotLock = true;
          } catch (CancelException e) {
            // see workaround below.
          }

          if (!gotLock) { // workaround for bug39380
            // Allow only one thread to proceed
            synchronized (this.closeRegionGuard) {
              if (dr.isRegionClosed()) {
                return;
              }

              dr.setRegionClosed(true);
              // Asif: I am quite sure that it should also be Ok if instead
              // while it is a If Check below. Because if acquireReadLock thread
              // has acquired the lock, it is bound to see the isRegionClose as
              // true
              // and so will release the lock causing decrement to zeo , before
              // releasing the closeRegionGuard. But still...not to take any
              // chance
              final int loopCount = 10;
              for (int i = 0; i < loopCount; i++) {
                if (this.entryOpsCount.get() == 0) {
                  break;
                }
                boolean interrupted = Thread.interrupted();
                try {
                  this.closeRegionGuard.wait(1000);
                } catch (InterruptedException ie) {
                  interrupted = true;
                } finally {
                  if (interrupted) {
                    Thread.currentThread().interrupt();
                  }
                }
              } // for
              if (this.entryOpsCount.get() > 0) {
                logger.warn(LocalizedMessage.create(
                    LocalizedStrings.DisKRegion_OUTSTANDING_OPS_REMAIN_AFTER_0_SECONDS_FOR_DISK_REGION_1,
                    new Object[] {Integer.valueOf(loopCount), dr.getName()}));

                for (;;) {
                  if (this.entryOpsCount.get() == 0) {
                    break;
                  }
                  boolean interrupted = Thread.interrupted();
                  try {
                    this.closeRegionGuard.wait(1000);
                  } catch (InterruptedException ie) {
                    interrupted = true;
                  } finally {
                    if (interrupted) {
                      Thread.currentThread().interrupt();
                    }
                  }
                } // for
                logger.info(LocalizedMessage.create(
                    LocalizedStrings.DisKRegion_OUTSTANDING_OPS_CLEARED_FOR_DISK_REGION_0,
                    dr.getName()));
              }
            } // synchronized
          }

          dr.setRegionClosed(true);
          basicDestroy(region, dr);
        } finally {
          if (gotLock) {
            releaseWriteLock(dr);
          }
        }
      }
    }
    if (this.initFile != null && dr.isBackup()) {
      this.initFile.endDestroyRegion(dr);
    } else {
      rmById(dr.getId());
      this.overflowMap.remove(dr);
    }
    if (getOwnedByRegion()) {
      if (this.ownCount.decrementAndGet() <= 0) {
        destroy();
      }
    }
  }

  public void beginDestroyDataStorage(DiskRegion dr) {
    if (this.initFile != null && dr.isBackup()/* fixes bug 41389 */) {
      this.initFile.beginDestroyDataStorage(dr);
    }
  }

  public void endDestroyDataStorage(LocalRegion region, DiskRegion dr) {
    try {
      clear(region, dr, null);
      dr.resetRVV();
      dr.setRVVTrusted(false);
      dr.writeRVV(null, null); // just persist the empty rvv with trust=false
    } catch (RegionDestroyedException rde) {
      // ignore a RegionDestroyedException at this stage
    }
    if (this.initFile != null && dr.isBackup()) {
      this.initFile.endDestroyDataStorage(dr);
    }
  }

  public PersistentMemberID generatePersistentID(DiskRegionView dr) {
    File firstDir = getInfoFileDir().getDir();
    InternalDistributedSystem ids = getCache().getDistributedSystem();
    InternalDistributedMember memberId = ids.getDistributionManager().getDistributionManagerId();

    // NOTE - do NOT use DM.cacheTimeMillis here. See bug #49920
    long timestamp = System.currentTimeMillis();
    PersistentMemberID id = new PersistentMemberID(getDiskStoreID(), memberId.getInetAddress(),
        firstDir.getAbsolutePath(), memberId.getName(), timestamp, (short) 0);
    return id;
  }

  public PersistentID getPersistentID() {
    InetAddress host = cache.getDistributedSystem().getDistributedMember().getInetAddress();
    String dir = getDiskDirs()[0].getAbsolutePath();
    return new PersistentMemberPattern(host, dir, this.diskStoreID.toUUID(), 0);
  }

  // test hook
  public void forceIFCompaction() {
    if (this.initFile != null) {
      this.initFile.forceCompaction();
    }
  }

  // @todo DiskStore it
  /**
   * Need a stopper that only triggers if this DiskRegion has been closed. If we use the
   * LocalRegion's Stopper then our async writer will not be able to finish flushing on a cache
   * close.
   */
  private class Stopper extends CancelCriterion {
    @Override
    public String cancelInProgress() {
      if (isClosed()) {
        return "The disk store is closed.";
      } else {
        return null;
      }
    }

    @Override
    public RuntimeException generateCancelledException(Throwable e) {
      if (isClosed()) {
        return new CacheClosedException("The disk store is closed", e);
      } else {
        return null;
      }
    }

  }

  private final CancelCriterion stopper = new Stopper();

  public CancelCriterion getCancelCriterion() {
    return this.stopper;
  }

  /**
   * Called when we are doing recovery and we find a new id.
   */
  void recoverRegionId(long drId) {
    long newVal = drId + 1;
    if (this.regionIdCtr.get() < newVal) { // fixes bug 41421
      this.regionIdCtr.set(newVal);
    }
  }

  /**
   * Called when creating a new disk region (not a recovered one).
   */
  long generateRegionId() {
    long result;
    do {
      result = this.regionIdCtr.getAndIncrement();
    } while (result <= MAX_RESERVED_DRID && result >= MIN_RESERVED_DRID);
    return result;
  }

  /**
   * Returns a set of the disk regions that are using this disk store. Note that this set is read
   * only and live (its contents may change if the regions using this disk store changes).
   */
  Collection<DiskRegion> getDiskRegions() {
    return Collections.unmodifiableCollection(this.drMap.values());
  }

  /**
   * This method is slow and should be optimized if used for anything important. At this time it was
   * added to do some internal assertions that have since been removed.
   */
  DiskRegion getByName(String name) {
    for (DiskRegion dr : getDiskRegions()) {
      if (dr.getName().equals(name)) {
        return dr;
      }
    }
    return null;
  }

  void addDiskRegion(DiskRegion dr) {
    if (dr.isBackup()) {
      PersistentOplogSet oplogSet = getPersistentOplogSet(dr);
      if (!isOffline()) {
        oplogSet.initChild();
      }

      DiskRegion old = this.drMap.putIfAbsent(dr.getId(), dr);
      if (old != null) {
        throw new IllegalStateException(
            "DiskRegion already exists with id " + dr.getId() + " and name " + old.getName());
      }
      getDiskInitFile().createRegion(dr);
    } else {
      this.overflowMap.add(dr);
    }
    if (getOwnedByRegion()) {
      this.ownCount.incrementAndGet();
    }
  }

  void addPersistentPR(String name, PRPersistentConfig config) {
    getDiskInitFile().createPersistentPR(name, config);
  }

  void removePersistentPR(String name) {
    if (isClosed() && getOwnedByRegion()) {
      // A region owned disk store will destroy
      // itself when all buckets are removed, resulting
      // in an exception when this method is called.
      // Do nothing if the disk store is already
      // closed
      return;
    }
    getDiskInitFile().destroyPersistentPR(name);
  }

  PRPersistentConfig getPersistentPRConfig(String name) {
    return getDiskInitFile().getPersistentPR(name);
  }

  Map<String, PRPersistentConfig> getAllPRs() {
    return getDiskInitFile().getAllPRs();
  }

  DiskRegion getById(long regionId) {
    return this.drMap.get(regionId);
  }

  void rmById(long regionId) {
    this.drMap.remove(regionId);
  }

  void handleDiskAccessException(final DiskAccessException dae) {
    boolean causedByRDE = LocalRegion.causedByRDE(dae);

    // @todo is it ok for flusher and compactor to call this method if RDE?
    // I think they need to keep working (for other regions) in this case.
    if (causedByRDE) {
      return;
    }

    // If another thread has already hit a DAE and is cleaning up, do nothing
    if (!diskException.compareAndSet(null, dae)) {
      return;
    }

    // log the error
    final StringId sid =
        LocalizedStrings.LocalRegion_A_DISKACCESSEXCEPTION_HAS_OCCURED_WHILE_WRITING_TO_THE_DISK_FOR_DISKSTORE_0_THE_CACHE_WILL_BE_CLOSED;
    logger.error(LocalizedMessage.create(sid, DiskStoreImpl.this.getName()), dae);

    final ThreadGroup exceptionHandlingGroup =
        LoggingThreadGroup.createThreadGroup("Disk Store Exception Handling Group", logger);

    Thread thread = new Thread(exceptionHandlingGroup, "Disk store exception handler") {
      public void run() {
        try {
          // now close the cache
          getCache().close(sid.toLocalizedString(DiskStoreImpl.this.getName(), dae), dae);
          _testHandleDiskAccessException.countDown();

        } catch (Exception e) {
          logger.error(LocalizedMessage.create(
              LocalizedStrings.LocalRegion_AN_EXCEPTION_OCCURED_WHILE_CLOSING_THE_CACHE), e);
        }
      }
    };
    thread.start();
  }

  private final String name;
  private final boolean autoCompact;
  private final boolean allowForceCompaction;
  private final long maxOplogSizeInBytes;
  private final long timeInterval;
  private final int queueSize;
  private final int writeBufferSize;
  private final File[] diskDirs;
  private final int[] diskDirSizes;
  private volatile float warningPercent;
  private volatile float criticalPercent;

  // DiskStore interface methods
  public String getName() {
    return this.name;
  }

  public boolean getAutoCompact() {
    return this.autoCompact;
  }

  public boolean getAllowForceCompaction() {
    return this.allowForceCompaction;
  }

  public long getMaxOplogSize() {
    return this.maxOplogSizeInBytes / (1024 * 1024);
  }

  public long getMaxOplogSizeInBytes() {
    return this.maxOplogSizeInBytes;
  }

  public long getTimeInterval() {
    return this.timeInterval;
  }

  public int getQueueSize() {
    return this.queueSize;
  }

  public int getWriteBufferSize() {
    return this.writeBufferSize;
  }

  public File[] getDiskDirs() {
    return this.diskDirs;
  }

  public int[] getDiskDirSizes() {
    return this.diskDirSizes;
  }

  @Override
  public float getDiskUsageWarningPercentage() {
    return warningPercent;
  }

  @Override
  public float getDiskUsageCriticalPercentage() {
    return criticalPercent;
  }

  @Override
  public void setDiskUsageWarningPercentage(float warningPercent) {
    DiskStoreMonitor.checkWarning(warningPercent);
    this.warningPercent = warningPercent;
  }

  @Override
  public void setDiskUsageCriticalPercentage(float criticalPercent) {
    DiskStoreMonitor.checkCritical(criticalPercent);
    this.criticalPercent = criticalPercent;
  }

  // public String toString() {
  // StringBuffer sb = new StringBuffer();
  // sb.append("<");
  // sb.append(getName());
  // if (getOwnedByRegion()) {
  // sb.append(" OWNED_BY_REGION");
  // }
  // sb.append(">");
  // return sb.toString();
  // }

  public static class AsyncDiskEntry {
    public final LocalRegion region;
    public final DiskEntry de;
    public final boolean versionOnly;
    public final VersionTag tag;

    public AsyncDiskEntry(LocalRegion region, DiskEntry de, VersionTag tag) {
      this.region = region;
      this.de = de;
      this.tag = tag;
      this.versionOnly = false;
    }

    public AsyncDiskEntry(LocalRegion region, VersionTag tag) {
      this.region = region;
      this.de = null;
      this.tag = tag;
      this.versionOnly = true;
      // if versionOnly, only de.getDiskId() is used for synchronize
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("dr=").append(region.getDiskRegion().getId());
      sb.append(" versionOnly=" + this.versionOnly);
      if (this.versionOnly) {
        sb.append(" versionTag=" + this.tag);
      }
      if (de != null) {
        sb.append(" key=" + de.getKey());
      } else {
        sb.append(" <END CLEAR>");
      }
      return sb.toString();
    }
  }

  /**
   * Set of OplogEntryIds (longs). Memory is optimized by using an int[] for ids in the unsigned int
   * range.
   */
  static class OplogEntryIdSet {
    private final IntOpenHashSet ints = new IntOpenHashSet((int) INVALID_ID);
    private final LongOpenHashSet longs = new LongOpenHashSet((int) INVALID_ID);

    public void add(long id) {
      if (id == 0) {
        throw new IllegalArgumentException();
      } else if (id > 0 && id <= 0x00000000FFFFFFFFL) {
        this.ints.add((int) id);
      } else {
        this.longs.add(id);
      }
    }

    public boolean contains(long id) {
      if (id >= 0 && id <= 0x00000000FFFFFFFFL) {
        return this.ints.contains((int) id);
      } else {
        return this.longs.contains(id);
      }
    }

    public int size() {
      return this.ints.size() + this.longs.size();
    }
  }

  /**
   * Set to true if this diskStore is owned by a single region. This only happens in backwardsCompat
   * mode.
   */
  private final boolean ownedByRegion;

  /**
   * Set to the region's {@link InternalRegionArguments} when the diskStore is owned by a single
   * region in backwardsCompat mode ({@link #ownedByRegion} must be true).
   */
  private final InternalRegionArguments internalRegionArgs;

  /**
   * Number of current owners. Only valid if ownedByRegion is true.
   */
  private final AtomicInteger ownCount = new AtomicInteger();

  public boolean getOwnedByRegion() {
    return this.ownedByRegion;
  }

  public InternalRegionArguments getInternalRegionArguments() {
    return this.internalRegionArgs;
  }

  public int getOwnCount() {
    return this.ownCount.get();
  }

  private final boolean validating;

  boolean isValidating() {
    return this.validating;
  }

  private final boolean offline;

  boolean isOffline() {
    return this.offline;
  }

  public final boolean upgradeVersionOnly;

  boolean isUpgradeVersionOnly() {
    return this.upgradeVersionOnly && Version.GFE_70.compareTo(this.getRecoveredGFVersion()) > 0;
  }

  private final boolean offlineCompacting;

  boolean isOfflineCompacting() {
    return this.offlineCompacting;
  }

  // Set to true if diskStore will be used by an offline tool that modifies the disk store.
  private final boolean offlineModify;

  boolean isOfflineModify() {
    return this.offlineModify;
  }

  /**
   * Destroy a region which has not been created.
   * 
   * @param regName the name of the region to destroy
   */
  public void destroyRegion(String regName) {
    DiskRegionView drv = getDiskInitFile().getDiskRegionByName(regName);
    if (drv == null) {
      drv = getDiskInitFile().getDiskRegionByPrName(regName);
      PRPersistentConfig prConfig = getDiskInitFile().getPersistentPR(regName);
      if (drv == null && prConfig == null) {
        throw new IllegalArgumentException(
            "The disk store does not contain a region named: " + regName);
      } else {
        getDiskInitFile().destroyPRRegion(regName);
      }
    } else {
      getDiskInitFile().endDestroyRegion(drv);
    }
  }

  public String modifyRegion(String regName, String lruOption, String lruActionOption,
      String lruLimitOption, String concurrencyLevelOption, String initialCapacityOption,
      String loadFactorOption, String compressorClassNameOption, String statisticsEnabledOption,
      String offHeapOption, boolean printToConsole) {
    assert isOffline();
    DiskRegionView drv = getDiskInitFile().getDiskRegionByName(regName);
    if (drv == null) {
      drv = getDiskInitFile().getDiskRegionByPrName(regName);
      if (drv == null) {
        throw new IllegalArgumentException(
            "The disk store does not contain a region named: " + regName);
      } else {
        return getDiskInitFile().modifyPRRegion(regName, lruOption, lruActionOption, lruLimitOption,
            concurrencyLevelOption, initialCapacityOption, loadFactorOption,
            compressorClassNameOption, statisticsEnabledOption, offHeapOption, printToConsole);
      }
    } else {
      return getDiskInitFile().modifyRegion(drv, lruOption, lruActionOption, lruLimitOption,
          concurrencyLevelOption, initialCapacityOption, loadFactorOption,
          compressorClassNameOption, statisticsEnabledOption, offHeapOption, printToConsole);
    }
  }

  private void dumpInfo(PrintStream printStream, String regName) {
    assert isOffline();
    getDiskInitFile().dumpRegionInfo(printStream, regName);
  }

  private void dumpPdxTypes(PrintStream printStream) {
    try {
      ArrayList<PdxType> types = new ArrayList<>();
      ArrayList<EnumInfo> enums = new ArrayList<>();
      for (Object i : getPdxTypesAndEnums()) {
        if (i instanceof PdxType) {
          types.add((PdxType) i);
        } else {
          enums.add((EnumInfo) i);
        }
      }
      Collections.sort(types, new Comparator<PdxType>() {
        @Override
        public int compare(PdxType o1, PdxType o2) {
          return o1.getClassName().compareTo(o2.getClassName());
        }
      });
      Collections.sort(enums, new Comparator<EnumInfo>() {
        @Override
        public int compare(EnumInfo o1, EnumInfo o2) {
          return o1.compareTo(o2);
        }
      });

      printStream.println("PDX Types:");
      for (PdxType type : types) {
        type.toStream(printStream, true);
      }
      printStream.println("PDX Enums:");
      for (EnumInfo e : enums) {
        e.toStream(printStream);
      }
    } catch (IOException ignore) {
    }
  }

  private void dumpMetadata(boolean showBuckets) {
    assert isOffline();
    getDiskInitFile().dumpRegionMetadata(showBuckets);
  }

  private Collection<Object/* PdxType or EnumInfo */> pdxRename(String oldBase, String newBase)
      throws IOException {
    // Since we are recovering a disk store, the cast from DiskRegionView -->
    // PlaceHolderDiskRegion
    // and from RegionEntry --> DiskEntry should be ok.

    // In offline mode, we need to schedule the regions to be recovered
    // explicitly.
    DiskRegionView foundPdx = null;
    for (DiskRegionView drv : getKnown()) {
      if (drv.getName().equals(PeerTypeRegistration.REGION_FULL_PATH)) {
        foundPdx = drv;
        scheduleForRecovery((PlaceHolderDiskRegion) drv);
      }
    }
    if (foundPdx == null) {
      throw new IllegalStateException("The disk store does not contain any PDX types.");
    }
    recoverRegionsThatAreReady();
    PersistentOplogSet oplogSet = (PersistentOplogSet) getOplogSet(foundPdx);
    ArrayList<Object> result = new ArrayList<>();
    Pattern pattern = createPdxRenamePattern(oldBase);
    for (RegionEntry re : foundPdx.getRecoveredEntryMap().regionEntries()) {
      Object value = re._getValueRetain(foundPdx, true);
      if (Token.isRemoved(value)) {
        continue;
      }
      if (value instanceof CachedDeserializable) {
        value = ((CachedDeserializable) value).getDeserializedForReading();
      }
      if (value instanceof EnumInfo) {
        EnumInfo ei = (EnumInfo) value;
        String newName = replacePdxRenamePattern(pattern, ei.getClassName(), newBase);
        if (newName != null) {
          ei.setClassName(newName);
          result.add(ei);
          oplogSet.offlineModify(foundPdx, (DiskEntry) re, BlobHelper.serializeToBlob(ei), true);
        }
      } else {
        PdxType type = (PdxType) value;
        String newName = replacePdxRenamePattern(pattern, type.getClassName(), newBase);
        if (newName != null) {
          type.setClassName(newName);
          result.add(type);
          oplogSet.offlineModify(foundPdx, (DiskEntry) re, BlobHelper.serializeToBlob(type), true);
        }
      }
    }
    return result;
  }

  public static Pattern createPdxRenamePattern(String patBase) {
    return Pattern.compile(".*(?:^|\\.|\\$)(\\Q" + patBase + "\\E)(?:\\.|\\$|$).*");
  }

  /*
   * If existing matches pattern then return the string with the portion of it that matched the
   * pattern changed to replacement. If it did not match return null.
   */
  public static String replacePdxRenamePattern(Pattern pattern, String existing,
      String replacement) {
    Matcher matcher = pattern.matcher(existing);
    if (matcher.matches()) {
      int start = matcher.start(1);
      int end = matcher.end(1);
      StringBuilder sb = new StringBuilder();
      if (start > 0) {
        sb.append(existing.substring(0, start));
      }
      sb.append(replacement);
      if (end < existing.length()) {
        sb.append(existing.substring(end));
      }
      return sb.toString();
    }
    return null;
  }

  private Collection<PdxType> pdxDeleteField(String className, String fieldName)
      throws IOException {
    // Since we are recovering a disk store, the cast from DiskRegionView -->
    // PlaceHolderDiskRegion
    // and from RegionEntry --> DiskEntry should be ok.

    // In offline mode, we need to schedule the regions to be recovered
    // explicitly.
    DiskRegionView foundPdx = null;
    for (DiskRegionView drv : getKnown()) {
      if (drv.getName().equals(PeerTypeRegistration.REGION_FULL_PATH)) {
        foundPdx = drv;
        scheduleForRecovery((PlaceHolderDiskRegion) drv);
      }
    }
    if (foundPdx == null) {
      throw new IllegalStateException("The disk store does not contain any PDX types.");
    }
    recoverRegionsThatAreReady();
    PersistentOplogSet oplogSet = (PersistentOplogSet) getOplogSet(foundPdx);
    ArrayList<PdxType> result = new ArrayList<PdxType>();
    for (RegionEntry re : foundPdx.getRecoveredEntryMap().regionEntries()) {
      Object value = re._getValueRetain(foundPdx, true);
      if (Token.isRemoved(value)) {
        continue;
      }
      if (value instanceof CachedDeserializable) {
        value = ((CachedDeserializable) value).getDeserializedForReading();
      }
      if (value instanceof EnumInfo) {
        // nothing to delete in an enum
        continue;
      }
      PdxType type = (PdxType) value;
      if (type.getClassName().equals(className)) {
        PdxField field = type.getPdxField(fieldName);
        if (field != null) {
          field.setDeleted(true);
          type.setHasDeletedField(true);
          result.add(type);
          oplogSet.offlineModify(foundPdx, (DiskEntry) re, BlobHelper.serializeToBlob(type), true);
        }
      }
    }
    return result;
  }


  private Collection<PdxType> getPdxTypes() throws IOException {
    // Since we are recovering a disk store, the cast from DiskRegionView -->
    // PlaceHolderDiskRegion
    // and from RegionEntry --> DiskEntry should be ok.

    // In offline mode, we need to schedule the regions to be recovered
    // explicitly.
    DiskRegionView foundPdx = null;
    for (DiskRegionView drv : getKnown()) {
      if (drv.getName().equals(PeerTypeRegistration.REGION_FULL_PATH)) {
        foundPdx = drv;
        scheduleForRecovery((PlaceHolderDiskRegion) drv);
      }
    }
    if (foundPdx == null) {
      return Collections.emptyList();
      // throw new IllegalStateException("The disk store does not contain any PDX types.");
    }
    recoverRegionsThatAreReady();
    ArrayList<PdxType> result = new ArrayList<PdxType>();
    for (RegionEntry re : foundPdx.getRecoveredEntryMap().regionEntries()) {
      Object value = re._getValueRetain(foundPdx, true);
      if (Token.isRemoved(value)) {
        continue;
      }
      if (value instanceof CachedDeserializable) {
        value = ((CachedDeserializable) value).getDeserializedForReading();
      }
      if (value instanceof PdxType) {
        PdxType type = (PdxType) value;
        result.add(type);
      }
    }
    Collections.sort(result, new Comparator<PdxType>() {
      @Override
      public int compare(PdxType o1, PdxType o2) {
        return o1.getClassName().compareTo(o2.getClassName());
      }
    });
    return result;
  }

  private Collection<Object /* PdxType or EnumInfo */> getPdxTypesAndEnums() throws IOException {
    // Since we are recovering a disk store, the cast from DiskRegionView -->
    // PlaceHolderDiskRegion
    // and from RegionEntry --> DiskEntry should be ok.

    // In offline mode, we need to schedule the regions to be recovered
    // explicitly.
    DiskRegionView foundPdx = null;
    for (DiskRegionView drv : getKnown()) {
      if (drv.getName().equals(PeerTypeRegistration.REGION_FULL_PATH)) {
        foundPdx = drv;
        scheduleForRecovery((PlaceHolderDiskRegion) drv);
      }
    }
    if (foundPdx == null) {
      return Collections.emptyList();
      // throw new IllegalStateException("The disk store does not contain any PDX types.");
    }
    recoverRegionsThatAreReady();
    ArrayList<Object> result = new ArrayList<Object>();
    for (RegionEntry re : foundPdx.getRecoveredEntryMap().regionEntries()) {
      Object value = re._getValueRetain(foundPdx, true);
      if (Token.isRemoved(value)) {
        continue;
      }
      if (value instanceof CachedDeserializable) {
        value = ((CachedDeserializable) value).getDeserializedForReading();
      }
      result.add(value);
    }
    return result;
  }

  private void exportSnapshot(String name, File out) throws IOException {
    // Since we are recovering a disk store, the cast from DiskRegionView -->
    // PlaceHolderDiskRegion
    // and from RegionEntry --> DiskEntry should be ok.

    // coelesce disk regions so that partitioned buckets from a member end up in
    // the same file
    Map<String, SnapshotWriter> regions = new HashMap<String, SnapshotWriter>();

    try {
      for (DiskRegionView drv : getKnown()) {
        PlaceHolderDiskRegion ph = (PlaceHolderDiskRegion) drv;
        String regionName = (drv.isBucket() ? ph.getPrName() : drv.getName());
        SnapshotWriter writer = regions.get(regionName);
        if (writer == null) {
          String fname = regionName.substring(1).replace('/', '-');
          File f = new File(out, "snapshot-" + name + "-" + fname + ".gfd");
          writer = GFSnapshot.create(f, regionName);
          regions.put(regionName, writer);
        }
        // Add a mapping from the bucket name to the writer for the PR
        // if this is a bucket.
        regions.put(drv.getName(), writer);
      }

      // In offline mode, we need to schedule the regions to be recovered
      // explicitly.
      for (DiskRegionView drv : getKnown()) {
        final SnapshotWriter writer = regions.get(drv.getName());

        scheduleForRecovery(new ExportDiskRegion(this, drv, new ExportWriter() {

          @Override
          public void writeBatch(Map<Object, RecoveredEntry> entries) throws IOException {
            for (Map.Entry<Object, RecoveredEntry> re : entries.entrySet()) {
              Object key = re.getKey();
              // TODO:KIRK:OK Rusty's code was value = de.getValueWithContext(drv);
              Object value = re.getValue().getValue();
              writer.snapshotEntry(new SnapshotRecord(key, value));
            }
          }

        }));
      }
      recoverRegionsThatAreReady();
    } finally {
      // Some writers are in the map multiple times because of multiple buckets
      // get a the unique set of writers and close each writer once.
      Set<SnapshotWriter> uniqueWriters = new HashSet(regions.values());
      for (SnapshotWriter writer : uniqueWriters) {
        writer.snapshotComplete();
      }

    }
  }

  private void validate() {
    assert isValidating();
    this.RECOVER_VALUES = false; // save memory @todo should Oplog make sure
                                 // value is deserializable?
    this.liveEntryCount = 0;
    this.deadRecordCount = 0;
    for (DiskRegionView drv : getKnown()) {
      scheduleForRecovery(ValidatingDiskRegion.create(this, drv));
    }
    recoverRegionsThatAreReady();
    if (getDeadRecordCount() > 0) {
      System.out.println("Disk store contains " + getDeadRecordCount() + " compactable records.");
    }
    System.out
        .println("Total number of region entries in this disk store is: " + getLiveEntryCount());
  }

  private int liveEntryCount;

  void incLiveEntryCount(int count) {
    this.liveEntryCount += count;
  }

  public int getLiveEntryCount() {
    return this.liveEntryCount;
  }

  private int deadRecordCount;

  void incDeadRecordCount(int count) {
    this.deadRecordCount += count;
  }

  public int getDeadRecordCount() {
    return this.deadRecordCount;
  }

  private void offlineCompact() {
    assert isOfflineCompacting();
    this.RECOVER_VALUES = false;
    this.deadRecordCount = 0;
    for (DiskRegionView drv : getKnown()) {
      scheduleForRecovery(OfflineCompactionDiskRegion.create(this, drv));
    }

    persistentOplogs.recoverRegionsThatAreReady();
    persistentOplogs.offlineCompact();

    // TODO soplogs - we need to do offline compaction for
    // the soplog regions, but that is not currently implemented

    getDiskInitFile().forceCompaction();
    if (this.upgradeVersionOnly) {
      System.out.println("Upgrade disk store " + this.name + " to version "
          + getRecoveredGFVersionName() + " finished.");
    } else {
      if (getDeadRecordCount() == 0) {
        System.out.println("Offline compaction did not find anything to compact.");
      } else {
        System.out.println("Offline compaction removed " + getDeadRecordCount() + " records.");
      }
      // If we have more than one oplog then the liveEntryCount may not be the
      // total
      // number of live entries in the disk store. So do not log the live entry
      // count
    }
  }

  private final HashMap<String, LRUStatistics> prlruStatMap = new HashMap<String, LRUStatistics>();

  LRUStatistics getOrCreatePRLRUStats(PlaceHolderDiskRegion dr) {
    String prName = dr.getPrName();
    LRUStatistics result = null;
    synchronized (this.prlruStatMap) {
      result = this.prlruStatMap.get(prName);
      if (result == null) {
        EvictionAttributesImpl ea = dr.getEvictionAttributes();
        LRUAlgorithm ec = ea.createEvictionController(null, dr.getOffHeap());
        StatisticsFactory sf = cache.getDistributedSystem();
        result = ec.getLRUHelper().initStats(dr, sf);
        this.prlruStatMap.put(prName, result);
      }
    }
    return result;
  }

  /**
   * If we have recovered a bucket earlier for the given pr then we will have an LRUStatistics to
   * return for it. Otherwise return null.
   */
  LRUStatistics getPRLRUStats(PartitionedRegion pr) {
    String prName = pr.getFullPath();
    LRUStatistics result = null;
    synchronized (this.prlruStatMap) {
      result = this.prlruStatMap.get(prName);
    }
    return result;
  }

  /**
   * Lock the disk store to prevent updates. This is the first step of the backup process. Once all
   * disk stores on all members are locked, we still move on to startBackup.
   */
  public void lockStoreBeforeBackup() {
    // This will prevent any region level operations like
    // create/destroy region, and region view changes.
    // We might want to consider preventing any entry level
    // operations as well. We should at least prevent transactions
    // when we support persistent transactions.
    //
    // When we do start caring about blocking entry
    // level operations, we will need to be careful
    // to block them *before* they are put in the async
    // queue
    getDiskInitFile().lockForBackup();
  }

  /**
   * Release the lock that is preventing operations on this disk store during the backup process.
   */
  public void releaseBackupLock() {
    getDiskInitFile().unlockForBackup();
  }

  /**
   * Start the backup process. This is the second step of the backup process. In this method, we
   * define the data we're backing up by copying the init file and rolling to the next file. After
   * this method returns operations can proceed as normal, except that we don't remove oplogs.
   * 
   * @param targetDir
   * @param baselineInspector
   * @param restoreScript
   * @throws IOException
   */
  public void startBackup(File targetDir, BackupInspector baselineInspector,
      RestoreScript restoreScript) throws IOException {
    getDiskInitFile().setBackupThread(Thread.currentThread());
    boolean done = false;
    try {
      for (;;) {
        Oplog childOplog = persistentOplogs.getChild();
        if (childOplog == null) {
          this.diskStoreBackup = new DiskStoreBackup(new Oplog[0], targetDir);
          break;
        }

        // Get an appropriate lock object for each set of oplogs.
        Object childLock = childOplog == null ? new Object() : childOplog.lock;;

        // TODO - We really should move this lock into the disk store, but
        // until then we need to do this magic to make sure we're actually
        // locking the latest child for both types of oplogs

        // This ensures that all writing to disk is blocked while we are
        // creating the snapshot
        synchronized (childLock) {
          if (persistentOplogs.getChild() != childOplog) {
            continue;
          }

          if (logger.isDebugEnabled()) {
            logger.debug("snapshotting oplogs for disk store {}", getName());
          }

          // Create the directories for this disk store
          for (int i = 0; i < directories.length; i++) {
            File dir = getBackupDir(targetDir, i);
            if (!FileUtil.mkdirs(dir)) {
              throw new IOException("Could not create directory " + dir);
            }
            restoreScript.addFile(directories[i].getDir(), dir);
          }

          restoreScript.addExistenceTest(this.initFile.getIFFile());

          // Contains all oplogs that will backed up
          Oplog[] allOplogs = null;

          // Incremental backup so filter out oplogs that have already been
          // backed up
          if (null != baselineInspector) {
            Map<File, File> baselineCopyMap = new HashMap<File, File>();
            allOplogs = filterBaselineOplogs(baselineInspector, baselineCopyMap);
            restoreScript.addBaselineFiles(baselineCopyMap);
          } else {
            allOplogs = getAllOplogsForBackup();
          }

          // mark all oplogs as being backed up. This will
          // prevent the oplogs from being deleted
          this.diskStoreBackup = new DiskStoreBackup(allOplogs, targetDir);

          // copy the init file
          File firstDir = getBackupDir(targetDir, infoFileDirIndex);
          initFile.copyTo(firstDir);
          persistentOplogs.forceRoll(null);

          if (logger.isDebugEnabled()) {
            logger.debug("done snaphotting for disk store {}", getName());
          }
          break;
        }
      }
      done = true;
    } finally {
      if (!done) {
        clearBackup();
      }
    }
  }

  private File getBackupDir(File targetDir, int index) {
    return new File(targetDir, BACKUP_DIR_PREFIX + index);
  }

  /**
   * Copy the oplogs to the backup directory. This is the final step of the backup process. The
   * oplogs we copy are defined in the startBackup method.
   * 
   * @param backupManager
   * @throws IOException
   */
  public void finishBackup(BackupManager backupManager) throws IOException {
    if (diskStoreBackup == null) {
      return;
    }
    try {
      // Wait for oplogs to be unpreblown before backing them up.
      waitForDelayedWrites();

      // Backup all of the oplogs
      for (Oplog oplog : this.diskStoreBackup.getPendingBackup()) {
        if (backupManager.isCancelled()) {
          break;
        }
        // Copy theoplog to the destination directory
        int index = oplog.getDirectoryHolder().getArrayIndex();
        File backupDir = getBackupDir(this.diskStoreBackup.getTargetDir(), index);
        // TODO prpersist - We could probably optimize this to *move* the files
        // that we know are supposed to be deleted.
        oplog.copyTo(backupDir);

        // Allow the oplog to be deleted, and process any pending delete
        this.diskStoreBackup.backupFinished(oplog);
      }
    } finally {
      clearBackup();
    }
  }

  private int getArrayIndexOfDirectory(File searchDir) {
    for (DirectoryHolder holder : directories) {
      if (holder.getDir().equals(searchDir)) {
        return holder.getArrayIndex();
      }
    }
    return 0;
  }

  public DirectoryHolder[] getDirectoryHolders() {
    return this.directories;
  }

  private void clearBackup() {
    DiskStoreBackup backup = this.diskStoreBackup;
    if (backup != null) {
      this.diskStoreBackup = null;
      backup.cleanup();
    }
  }

  public DiskStoreBackup getInProgressBackup() {
    return diskStoreBackup;
  }

  public Collection<DiskRegionView> getKnown() {
    return this.initFile.getKnown();
  }

  private static DiskStoreImpl createForOffline(String dsName, File[] dsDirs) throws Exception {
    return createForOffline(dsName, dsDirs, false, false, false/* upgradeVersionOnly */, 0, true,
        false);
  }

  private static DiskStoreImpl createForOfflineModify(String dsName, File[] dsDirs)
      throws Exception {
    return createForOffline(dsName, dsDirs, false, false, false, 0, true/* needsOplogs */,
        true/* offlineModify */);
  }

  private static DiskStoreImpl createForOffline(String dsName, File[] dsDirs, boolean needsOplogs)
      throws Exception {
    return createForOffline(dsName, dsDirs, false, false, false/* upgradeVersionOnly */, 0,
        needsOplogs, false);
  }

  private static DiskStoreImpl createForOfflineValidate(String dsName, File[] dsDirs)
      throws Exception {
    return createForOffline(dsName, dsDirs, false, true, false/* upgradeVersionOnly */, 0, true,
        false);
  }

  private static Cache offlineCache = null;
  private static DistributedSystem offlineDS = null;

  private static void cleanupOffline() {
    if (offlineCache != null) {
      offlineCache.close();
      offlineCache = null;
    }
    if (offlineDS != null) {
      offlineDS.disconnect();
      offlineDS = null;
    }
  }

  private static DiskStoreImpl createForOffline(String dsName, File[] dsDirs,
      boolean offlineCompacting, boolean offlineValidate, boolean upgradeVersionOnly,
      long maxOplogSize, boolean needsOplogs, boolean offlineModify) throws Exception {
    if (dsDirs == null) {
      dsDirs = new File[] {new File("")};
    }
    // need a cache so create a loner ds
    Properties props = new Properties();
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(CACHE_XML_FILE, "");
    DistributedSystem ds = DistributedSystem.connect(props);
    offlineDS = ds;
    Cache c = org.apache.geode.cache.CacheFactory.create(ds);
    offlineCache = c;
    org.apache.geode.cache.DiskStoreFactory dsf = c.createDiskStoreFactory();
    dsf.setDiskDirs(dsDirs);
    if (offlineCompacting && maxOplogSize != -1L) {
      dsf.setMaxOplogSize(maxOplogSize);
    }
    DiskStoreImpl dsi = new DiskStoreImpl(c, dsName,
        ((DiskStoreFactoryImpl) dsf).getDiskStoreAttributes(), false, null, true,
        upgradeVersionOnly, offlineValidate, offlineCompacting, needsOplogs, offlineModify);
    ((GemFireCacheImpl) c).addDiskStore(dsi);
    return dsi;
  }

  /**
   * Use this method to destroy a region in an offline disk store.
   * 
   * @param dsName the name of the disk store
   * @param dsDirs the directories that that the disk store wrote files to
   * @param regName the name of the region to destroy
   */
  public static void destroyRegion(String dsName, File[] dsDirs, String regName) throws Exception {
    try {
      DiskStoreImpl dsi = createForOffline(dsName, dsDirs);
      dsi.destroyRegion(regName);
    } finally {
      cleanupOffline();
    }
  }

  public static String modifyRegion(String dsName, File[] dsDirs, String regName, String lruOption,
      String lruActionOption, String lruLimitOption, String concurrencyLevelOption,
      String initialCapacityOption, String loadFactorOption, String compressorClassNameOption,
      String statisticsEnabledOption, String offHeapOption, boolean printToConsole)
      throws Exception {
    try {
      DiskStoreImpl dsi = createForOffline(dsName, dsDirs);
      return dsi.modifyRegion(regName, lruOption, lruActionOption, lruLimitOption,
          concurrencyLevelOption, initialCapacityOption, loadFactorOption,
          compressorClassNameOption, statisticsEnabledOption, offHeapOption, printToConsole);
    } finally {
      cleanupOffline();
    }
  }

  public static void dumpInfo(PrintStream printStream, String dsName, File[] dsDirs, String regName,
      Boolean listPdxTypes) throws Exception {
    try {
      DiskStoreImpl dsi = createForOffline(dsName, dsDirs, false);
      dsi.dumpInfo(printStream, regName);
      if (listPdxTypes != null && listPdxTypes.booleanValue()) {
        dsi.dumpPdxTypes(printStream);
      }
    } finally {
      cleanupOffline();
    }
  }

  public static void dumpMetadata(String dsName, File[] dsDirs, boolean showBuckets)
      throws Exception {
    try {
      DiskStoreImpl dsi = createForOffline(dsName, dsDirs, false);
      dsi.dumpMetadata(showBuckets);
    } finally {
      cleanupOffline();
    }
  }

  public static void exportOfflineSnapshot(String dsName, File[] dsDirs, File out)
      throws Exception {
    try {
      DiskStoreImpl dsi = createForOffline(dsName, dsDirs);
      dsi.exportSnapshot(dsName, out);
    } finally {
      cleanupOffline();
    }
  }

  public static Collection<PdxType> getPdxTypes(String dsName, File[] dsDirs) throws Exception {
    try {
      DiskStoreImpl dsi = createForOffline(dsName, dsDirs);
      return dsi.getPdxTypes();
    } finally {
      cleanupOffline();
    }
  }

  /**
   * Returns a collection of the types renamed
   */
  public static Collection<Object/* PdxType or EnumInfo */> pdxRename(String dsName, File[] dsDirs,
      String oldRegEx, String newName) throws Exception {
    try {
      DiskStoreImpl dsi = createForOfflineModify(dsName, dsDirs);
      return dsi.pdxRename(oldRegEx, newName);
    } finally {
      cleanupOffline();
    }
  }

  /**
   * Returns a collection of the types with a deleted field
   */
  public static Collection<PdxType> pdxDeleteField(String dsName, File[] dsDirs, String className,
      String fieldName) throws Exception {
    try {
      DiskStoreImpl dsi = createForOfflineModify(dsName, dsDirs);
      return dsi.pdxDeleteField(className, fieldName);
    } finally {
      cleanupOffline();
    }
  }

  public static void validate(String name, File[] dirs) throws Exception {
    try {
      DiskStoreImpl dsi = createForOfflineValidate(name, dirs);
      dsi.validate();
    } finally {
      cleanupOffline();
    }
  }

  public static DiskStoreImpl offlineCompact(String name, File[] dirs, boolean upgradeVersionOnly,
      long maxOplogSize) throws Exception {
    try {
      DiskStoreImpl dsi =
          createForOffline(name, dirs, true, false, upgradeVersionOnly, maxOplogSize, true, false);
      dsi.offlineCompact();
      dsi.close();
      return dsi;
    } finally {
      cleanupOffline();
    }
  }

  public static void main(String args[]) throws Exception {
    if (args.length == 0) {
      System.out.println("Usage: diskStoreName [dirs]");
    } else {
      String dsName = args[0];
      File[] dirs = null;
      if (args.length > 1) {
        dirs = new File[args.length - 1];
        for (int i = 1; i < args.length; i++) {
          dirs[i - 1] = new File(args[i]);
        }
      }
      offlineCompact(dsName, dirs, false, 1024);
    }
  }

  public boolean hasPersistedData() {
    return persistentOplogs.getChild() != null;
  }

  public UUID getDiskStoreUUID() {
    return this.diskStoreID.toUUID();
  }

  public DiskStoreID getDiskStoreID() {
    return this.diskStoreID;
  }

  void setDiskStoreID(DiskStoreID diskStoreID) {
    this.diskStoreID = diskStoreID;
  }

  File getInitFile() {
    return getDiskInitFile().getIFFile();
  }

  public boolean needsLinkedList() {
    return isCompactionPossible() || couldHaveKrf();
  }

  /**
   * 
   * @return true if KRF files are used on this disk store's oplogs
   */
  boolean couldHaveKrf() {
    return !isOffline();
  }

  @Override
  public String toString() {
    return "DiskStore[" + name + "]";
  }

  private class ValueRecoveryTask implements Runnable {
    private final Set<Oplog> oplogSet;
    private final Map<Long, DiskRecoveryStore> recoveredStores;

    public ValueRecoveryTask(Set<Oplog> oplogSet, Map<Long, DiskRecoveryStore> recoveredStores) {
      this.oplogSet = oplogSet;
      this.recoveredStores = new HashMap<Long, DiskRecoveryStore>(recoveredStores);
    }

    public void run() {
      synchronized (asyncValueRecoveryLock) {
        DiskStoreObserver.startAsyncValueRecovery(DiskStoreImpl.this);
        try {
          for (Oplog oplog : oplogSet) {
            oplog.recoverValuesIfNeeded(currentAsyncValueRecoveryMap);
          }
        } catch (CancelException ignore) {
          // do nothing
        } finally {
          synchronized (currentAsyncValueRecoveryMap) {
            DiskStoreImpl.this.currentAsyncValueRecoveryMap.keySet()
                .removeAll(recoveredStores.keySet());
            currentAsyncValueRecoveryMap.notifyAll();
          }
          DiskStoreObserver.endAsyncValueRecovery(DiskStoreImpl.this);
        }
      }
    }
  }

  public void waitForAsyncRecovery(DiskRegion diskRegion) {
    synchronized (currentAsyncValueRecoveryMap) {
      boolean interrupted = false;
      while (!isClosing() && currentAsyncValueRecoveryMap.containsKey(diskRegion.getId())) {
        try {
          currentAsyncValueRecoveryMap.wait();
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static final ThreadLocal<Boolean> backgroundTaskThread = new ThreadLocal<Boolean>();

  private static boolean isBackgroundTaskThread() {
    boolean result = false;
    Boolean tmp = backgroundTaskThread.get();
    if (tmp != null) {
      result = tmp.booleanValue();
    }
    return result;
  }

  private static void markBackgroundTaskThread() {
    backgroundTaskThread.set(Boolean.TRUE);
  }

  /**
   * Execute a task which must be performed asnychronously, but has no requirement for timely
   * execution. This task pool is used for compactions, creating KRFS, etc. So some of the queued
   * tasks may take a while.
   */
  public boolean executeDiskStoreTask(final Runnable runnable) {
    return executeDiskStoreTask(runnable, this.diskStoreTaskPool) != null;
  }

  /**
   * Execute a task asynchronously, or in the calling thread if the bound is reached. This pool is
   * used for write operations which can be delayed, but we have a limit on how many write
   * operations we delay so that we don't run out of disk space. Used for deletes, unpreblow, RAF
   * close, etc.
   */
  public boolean executeDelayedExpensiveWrite(Runnable task) {
    Future<?> f = executeDiskStoreTask(task, this.delayedWritePool);
    lastDelayedWrite = f;
    return f != null;
  }

  /**
   * Wait for any current operations in the delayed write pool. Completion of this method ensures
   * that the writes have completed or the pool was shutdown
   */
  protected void waitForDelayedWrites() {
    Future<?> lastWriteTask = lastDelayedWrite;
    if (lastWriteTask != null) {
      try {
        lastWriteTask.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        // do nothing, an exception from the write task was already logged.
      }
    }
  }

  private Future<?> executeDiskStoreTask(final Runnable runnable, ThreadPoolExecutor executor) {
    // schedule another thread to do it
    incBackgroundTasks();
    Future<?> result = executeDiskStoreTask(new DiskStoreTask() {
      public void run() {
        try {
          markBackgroundTaskThread(); // for bug 42775
          // getCache().getCachePerfStats().decDiskTasksWaiting();
          runnable.run();
        } finally {
          decBackgroundTasks();
        }
      }

      public void taskCancelled() {
        decBackgroundTasks();
      }
    }, executor);

    if (result == null) {
      decBackgroundTasks();
    }

    return result;
  }

  private Future<?> executeDiskStoreTask(DiskStoreTask r, ThreadPoolExecutor executor) {
    try {
      return executor.submit(r);
    } catch (RejectedExecutionException ex) {
      if (logger.isDebugEnabled()) {
        logger.debug("Ignored compact schedule during shutdown", ex);
      }
    }
    return null;
  }

  private void stopDiskStoreTaskPool() {
    if (logger.isDebugEnabled()) {
      logger.debug("Stopping DiskStoreTaskPool");
    }
    shutdownPool(diskStoreTaskPool);

    // Allow the delayed writes to complete
    delayedWritePool.shutdown();
    try {
      delayedWritePool.awaitTermination(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void shutdownPool(ThreadPoolExecutor pool) {
    // All the regions have already been closed
    // so this pool shouldn't be doing anything.
    List<Runnable> l = pool.shutdownNow();
    for (Runnable runnable : l) {
      if (l instanceof DiskStoreTask) {
        ((DiskStoreTask) l).taskCancelled();
      }
    }
  }

  public void writeRVVGC(DiskRegion dr, LocalRegion region) {
    acquireReadLock(dr);
    try {
      if (dr.isRegionClosed()) {
        region.getCancelCriterion().checkCancelInProgress(null);
        throw new RegionDestroyedException(
            LocalizedStrings.DiskRegion_THE_DISKREGION_HAS_BEEN_CLOSED_OR_DESTROYED
                .toLocalizedString(),
            dr.getName());
      }

      // Update on the on disk region version vector.
      // TODO - RVV - For async regions, it's possible that
      // the on disk RVV is actually less than the GC RVV we're trying record
      // it might make sense to push the RVV through the async queue?
      // What we're doing here is only recording the GC RVV if it is dominated
      // by the RVV of what we have persisted.
      RegionVersionVector inMemoryRVV = region.getVersionVector();
      RegionVersionVector diskRVV = dr.getRegionVersionVector();

      // Update the GC version for each member in our on disk version map
      updateDiskGCRVV(diskRVV, inMemoryRVV, diskRVV.getOwnerId());
      for (VersionSource member : (Collection<VersionSource>) inMemoryRVV.getMemberToGCVersion()
          .keySet()) {
        updateDiskGCRVV(diskRVV, inMemoryRVV, member);
      }

      // Remove any exceptions from the disk RVV that are are dominated
      // by the GC RVV.
      diskRVV.pruneOldExceptions();

      PersistentOplogSet oplogSet = getPersistentOplogSet(dr);
      // persist the new GC RVV information for this region to the DRF
      oplogSet.getChild().writeGCRVV(dr);
    } finally {
      releaseReadLock(dr);
    }
  }

  public void writeRVV(DiskRegion dr, LocalRegion region, Boolean isRVVTrusted) {
    acquireReadLock(dr);
    try {
      if (dr.isRegionClosed()) {
        dr.getCancelCriterion().checkCancelInProgress(null);
        throw new RegionDestroyedException(
            LocalizedStrings.DiskRegion_THE_DISKREGION_HAS_BEEN_CLOSED_OR_DESTROYED
                .toLocalizedString(),
            dr.getName());
      }

      RegionVersionVector inMemoryRVV = (region == null) ? null : region.getVersionVector();
      // persist the new GC RVV information for this region to the CRF
      PersistentOplogSet oplogSet = getPersistentOplogSet(dr);
      // use current dr.rvvTrust
      oplogSet.getChild().writeRVV(dr, inMemoryRVV, isRVVTrusted);
    } finally {
      releaseReadLock(dr);
    }
  }

  /**
   * Update the on disk GC version for the given member, only if the disk has actually recorded all
   * of the updates including that member.
   * 
   * @param diskRVV the RVV for what has been persisted
   * @param inMemoryRVV the RVV of what is in memory
   * @param member The member we're trying to update
   */
  private void updateDiskGCRVV(RegionVersionVector diskRVV, RegionVersionVector inMemoryRVV,
      VersionSource member) {
    long diskVersion = diskRVV.getVersionForMember(member);
    long memoryGCVersion = inMemoryRVV.getGCVersion(member);

    // If the GC version is less than what we have on disk, go ahead
    // and record it.
    if (memoryGCVersion <= diskVersion) {
      diskRVV.recordGCVersion(member, memoryGCVersion);
    }

  }

  public void updateDiskRegion(AbstractDiskRegion dr) {
    PersistentOplogSet oplogSet = getPersistentOplogSet(dr);
    oplogSet.updateDiskRegion(dr);
  }

  public final Version getRecoveredGFVersion() {
    return getRecoveredGFVersion(this.initFile);
  }

  final Version getRecoveredGFVersion(DiskInitFile initFile) {
    return initFile.currentRecoveredGFVersion();
  }

  public boolean isDirectoryUsageNormal(DirectoryHolder dir) {
    return getCache().getDiskStoreMonitor().isNormal(this, dir);
  }
}
