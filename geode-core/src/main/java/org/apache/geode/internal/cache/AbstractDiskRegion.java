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

import static java.lang.System.lineSeparator;

import java.io.PrintStream;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import joptsimple.internal.Strings;
import org.apache.logging.log4j.Logger;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.compression.Compressor;
import org.apache.geode.internal.CopyOnWriteHashSet;
import org.apache.geode.internal.cache.DiskInitFile.DiskRegionFlag;
import org.apache.geode.internal.cache.entries.OffHeapRegionEntry;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;
import org.apache.geode.internal.cache.versions.DiskRegionVersionVector;
import org.apache.geode.internal.cache.versions.RegionVersionHolder;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.classloader.ClassPathLoader;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.util.concurrent.ConcurrentMapWithReusableEntries;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Code shared by both DiskRegion and RecoveredDiskRegion.
 *
 * @since GemFire prPersistSprint2
 */
public abstract class AbstractDiskRegion implements DiskRegionView {
  private static final Logger logger = LogService.getLogger();

  private final DiskStoreImpl ds;
  private final long id;
  private long clearOplogEntryId = DiskStoreImpl.INVALID_ID;
  private RegionVersionVector clearRVV;
  private byte lruAlgorithm;
  private byte lruAction;
  private int lruLimit;
  private int concurrencyLevel = 16;
  private int initialCapacity = 16;
  private float loadFactor = 0.75f;
  private boolean statisticsEnabled;
  private boolean isBucket;
  /** True if a persistent backup is needed */
  private boolean backup;

  /** Additional flags that are persisted to the meta-data. */
  private final EnumSet<DiskRegionFlag> flags;

  /**
   * A flag used to indicate that this disk region is being recreated using already existing data on
   * the disk.
   */
  private boolean isRecreated;
  private boolean configChanged;
  private boolean aboutToDestroy;
  private boolean aboutToDestroyDataStorage;
  private String partitionName;
  private int startingBucketId;
  private String compressorClassName;
  private Compressor compressor;

  private boolean offHeap;

  /**
   * Records the version vector of what has been persisted to disk. This may lag behind the version
   * vector of what is in memory, because updates may be written asynchronously to disk. We need to
   * keep track of exactly what has been written to disk so that we can record a version vector at
   * the beginning of each oplog.
   *
   * The version vector of what is in memory is held in is held in LocalRegion.versionVector.
   */
  private RegionVersionVector versionVector;

  /**
   * A flag whether the current version vector accurately represents what has been written to this
   * members disk.
   */
  private volatile boolean rvvTrusted = true;

  protected AbstractDiskRegion(DiskStoreImpl ds, String name) {
    DiskRegionView drv = ds.getDiskInitFile().takeDiskRegionByName(name);
    if (drv != null) {
      // if we found one in the initFile then we take it out of it and this
      // one we are constructing will replace it in the diskStore drMap.
      this.ds = drv.getDiskStore();
      id = drv.getId();
      backup = drv.isBackup();
      clearOplogEntryId = drv.getClearOplogEntryId();
      clearRVV = drv.getClearRVV();
      lruAlgorithm = drv.getLruAlgorithm();
      lruAction = drv.getLruAction();
      lruLimit = drv.getLruLimit();
      concurrencyLevel = drv.getConcurrencyLevel();
      initialCapacity = drv.getInitialCapacity();
      loadFactor = drv.getLoadFactor();
      statisticsEnabled = drv.getStatisticsEnabled();
      isBucket = drv.isBucket();
      flags = drv.getFlags();
      partitionName = drv.getPartitionName();
      startingBucketId = drv.getStartingBucketId();
      myInitializingId = drv.getMyInitializingID();
      myInitializedId = drv.getMyPersistentID();
      aboutToDestroy = drv.wasAboutToDestroy();
      aboutToDestroyDataStorage = drv.wasAboutToDestroyDataStorage();
      onlineMembers = new CopyOnWriteHashSet<>(drv.getOnlineMembers());
      offlineMembers = new CopyOnWriteHashSet<>(drv.getOfflineMembers());
      equalMembers =
          new CopyOnWriteHashSet<>(drv.getOfflineAndEqualMembers());
      isRecreated = true;
      // Use the same atomic counters as the previous disk region. This ensures that
      // updates from threads with a reference to the old region update this disk region
      // See 49943
      numOverflowOnDisk = ((AbstractDiskRegion) drv).numOverflowOnDisk;
      numEntriesInVM = ((AbstractDiskRegion) drv).numEntriesInVM;
      numOverflowBytesOnDisk = ((AbstractDiskRegion) drv).numOverflowBytesOnDisk;
      entries = drv.getRecoveredEntryMap();
      readyForRecovery = drv.isReadyForRecovery();
      recoveredEntryCount = drv.getRecoveredEntryCount();
      recoveryCompleted = ((AbstractDiskRegion) drv).recoveryCompleted;
      versionVector = drv.getRegionVersionVector();
      compressorClassName = drv.getCompressorClassName();
      compressor = drv.getCompressor();
      offHeap = drv.getOffHeap();
      if (drv instanceof PlaceHolderDiskRegion) {
        setRVVTrusted(((PlaceHolderDiskRegion) drv).getRVVTrusted());
      }
    } else {
      // This is a brand new disk region.
      this.ds = ds;
      // {
      // DiskRegion existingDr = ds.getByName(name);
      // if (existingDr != null) {
      // throw new IllegalStateException("DiskRegion named " + name + " already exists with id=" +
      // existingDr.getId());
      // }
      // }
      id = ds.generateRegionId();
      flags = EnumSet.noneOf(DiskRegionFlag.class);
      onlineMembers = new CopyOnWriteHashSet<>();
      offlineMembers = new CopyOnWriteHashSet<>();
      equalMembers = new CopyOnWriteHashSet<>();
      isRecreated = false;
      versionVector = new DiskRegionVersionVector(ds.getDiskStoreID());
      numOverflowOnDisk = new AtomicLong();
      numEntriesInVM = new AtomicLong();
      numOverflowBytesOnDisk = new AtomicLong();
    }
  }

  protected AbstractDiskRegion(DiskStoreImpl ds, long id) {
    this.ds = ds;
    this.id = id;
    flags = EnumSet.noneOf(DiskRegionFlag.class);
    onlineMembers = new CopyOnWriteHashSet<>();
    offlineMembers = new CopyOnWriteHashSet<>();
    equalMembers = new CopyOnWriteHashSet<>();
    isRecreated = true;
    backup = true;
    versionVector = new DiskRegionVersionVector(ds.getDiskStoreID());
    numOverflowOnDisk = new AtomicLong();
    numEntriesInVM = new AtomicLong();
    numOverflowBytesOnDisk = new AtomicLong();
    // We do not initialize the soplog set here. The soplog set needs
    // to be handled the complete set of recovered soplogs, which is not available
    // at the time a recovered disk region is first created.
  }

  /**
   * Used to initialize a PlaceHolderDiskRegion for a region that is being closed
   *
   * @param drv the region that is being closed
   */
  protected AbstractDiskRegion(DiskRegionView drv) {
    ds = drv.getDiskStore();
    id = drv.getId();
    backup = drv.isBackup();
    clearOplogEntryId = drv.getClearOplogEntryId();
    clearRVV = drv.getClearRVV();
    lruAlgorithm = drv.getLruAlgorithm();
    lruAction = drv.getLruAction();
    lruLimit = drv.getLruLimit();
    concurrencyLevel = drv.getConcurrencyLevel();
    initialCapacity = drv.getInitialCapacity();
    loadFactor = drv.getLoadFactor();
    statisticsEnabled = drv.getStatisticsEnabled();
    isBucket = drv.isBucket();
    flags = drv.getFlags();
    partitionName = drv.getPartitionName();
    startingBucketId = drv.getStartingBucketId();
    myInitializingId = null; // fixes 43650
    myInitializedId = drv.getMyPersistentID();
    aboutToDestroy = false;
    aboutToDestroyDataStorage = false;
    onlineMembers = new CopyOnWriteHashSet<>(drv.getOnlineMembers());
    offlineMembers = new CopyOnWriteHashSet<>(drv.getOfflineMembers());
    equalMembers = new CopyOnWriteHashSet<>(drv.getOfflineAndEqualMembers());
    isRecreated = true;
    numOverflowOnDisk = new AtomicLong();
    numEntriesInVM = new AtomicLong();
    numOverflowBytesOnDisk = new AtomicLong();
    entries = drv.getRecoveredEntryMap();
    readyForRecovery = drv.isReadyForRecovery();
    recoveredEntryCount = 0; // fix for bug 41570
    recoveryCompleted = ((AbstractDiskRegion) drv).recoveryCompleted;
    versionVector = drv.getRegionVersionVector();
    compressorClassName = drv.getCompressorClassName();
    compressor = drv.getCompressor();
    offHeap = drv.getOffHeap();
  }

  @Override
  public abstract String getName();

  @Override
  public DiskStoreImpl getDiskStore() {
    return ds;
  }

  abstract void beginDestroyRegion(LocalRegion region);

  public void resetRVV() {
    versionVector = new DiskRegionVersionVector(ds.getDiskStoreID());
  }

  @Override
  public long getId() {
    return id;
  }

  @Override
  public long getClearOplogEntryId() {
    return clearOplogEntryId;
  }

  @Override
  public void setClearOplogEntryId(long v) {
    clearOplogEntryId = v;
  }

  @Override
  public RegionVersionVector getClearRVV() {
    return clearRVV;
  }

  @Override
  public void setClearRVV(RegionVersionVector rvv) {
    clearRVV = rvv;
  }

  @Override
  public void setConfig(byte lruAlgorithm, byte lruAction, int lruLimit, int concurrencyLevel,
      int initialCapacity, float loadFactor, boolean statisticsEnabled, boolean isBucket,
      EnumSet<DiskRegionFlag> flags, String partitionName, int startingBucketId,
      String compressorClassName, boolean offHeap) {
    this.lruAlgorithm = lruAlgorithm;
    this.lruAction = lruAction;
    this.lruLimit = lruLimit;
    this.concurrencyLevel = concurrencyLevel;
    this.initialCapacity = initialCapacity;
    this.loadFactor = loadFactor;
    this.statisticsEnabled = statisticsEnabled;
    this.isBucket = isBucket;
    if (flags != null && flags != this.flags) {
      this.flags.clear();
      this.flags.addAll(flags);
    }
    this.partitionName = partitionName;
    this.startingBucketId = startingBucketId;
    this.compressorClassName = compressorClassName;
    this.offHeap = offHeap;
    if (!ds.isOffline()) {
      createCompressorFromClassName();
    }
  }

  public void createCompressorFromClassName() {
    if (Strings.isNullOrEmpty(compressorClassName)) {
      compressor = null;
    } else {
      try {
        @SuppressWarnings("unchecked")
        Class<Compressor> compressorClass =
            (Class<Compressor>) ClassPathLoader.getLatest().forName(compressorClassName);
        compressor = compressorClass.newInstance();
      } catch (ClassNotFoundException e) {
        throw new IllegalArgumentException(
            String.format("Unknown Compressor %s found in disk initialization file.",
                compressorClassName),
            e);
      } catch (InstantiationException e) {
        throw new IllegalArgumentException(
            String.format("Unknown Compressor %s found in disk initialization file.",
                compressorClassName),
            e);
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException(
            String.format("Unknown Compressor %s found in disk initialization file.",
                compressorClassName),
            e);
      }
    }
  }

  @Override
  public EvictionAttributes getEvictionAttributes() {
    return new EvictionAttributesImpl().setAlgorithm(getActualLruAlgorithm())
        .setAction(getActualLruAction()).setMaximum(getLruLimit());
  }

  @Override
  public byte getLruAlgorithm() {
    return lruAlgorithm;
  }

  public EvictionAlgorithm getActualLruAlgorithm() {
    return EvictionAlgorithm.parseValue(getLruAlgorithm());
  }

  @Override
  public byte getLruAction() {
    return lruAction;
  }

  public EvictionAction getActualLruAction() {
    return EvictionAction.parseValue(getLruAction());
  }

  @Override
  public int getLruLimit() {
    return lruLimit;
  }

  @Override
  public int getConcurrencyLevel() {
    return concurrencyLevel;
  }

  @Override
  public int getInitialCapacity() {
    return initialCapacity;
  }

  @Override
  public float getLoadFactor() {
    return loadFactor;
  }

  @Override
  public boolean getStatisticsEnabled() {
    return statisticsEnabled;
  }

  @Override
  public boolean isBucket() {
    return isBucket;
  }

  @Override
  public EnumSet<DiskRegionFlag> getFlags() {
    return flags;
  }

  @Override
  public String getPartitionName() {
    return partitionName;
  }

  @Override
  public int getStartingBucketId() {
    return startingBucketId;
  }

  public String getPrName() {
    assert isBucket();
    String bn = PartitionedRegionHelper.getBucketName(getName());
    return PartitionedRegionHelper.getPRPath(bn);
  }

  private PersistentMemberID myInitializingId = null;
  private PersistentMemberID myInitializedId = null;
  private final CopyOnWriteHashSet<PersistentMemberID> onlineMembers;
  private final CopyOnWriteHashSet<PersistentMemberID> offlineMembers;
  private final CopyOnWriteHashSet<PersistentMemberID> equalMembers;

  @Override
  public PersistentMemberID addMyInitializingPMID(PersistentMemberID pmid) {
    PersistentMemberID result = myInitializingId;
    myInitializingId = pmid;
    if (result != null) {
      myInitializedId = result;
    }
    return result;
  }

  @Override
  public void markInitialized() {
    assert myInitializingId != null;
    myInitializedId = myInitializingId;
    myInitializingId = null;
  }

  @Override
  public boolean addOnlineMember(PersistentMemberID pmid) {
    return onlineMembers.add(pmid);
  }

  @Override
  public boolean addOfflineMember(PersistentMemberID pmid) {
    return offlineMembers.add(pmid);
  }

  @Override
  public boolean addOfflineAndEqualMember(PersistentMemberID pmid) {
    return equalMembers.add(pmid);
  }

  @Override
  public boolean rmOnlineMember(PersistentMemberID pmid) {
    return onlineMembers.remove(pmid);
  }

  @Override
  public boolean rmOfflineMember(PersistentMemberID pmid) {
    return offlineMembers.remove(pmid);
  }

  @Override
  public boolean rmEqualMember(PersistentMemberID pmid) {
    return equalMembers.remove(pmid);
  }

  @Override
  public void markBeginDestroyRegion() {
    aboutToDestroy = true;
  }

  @Override
  public void markBeginDestroyDataStorage() {
    aboutToDestroyDataStorage = true;
  }

  @Override
  public void markEndDestroyRegion() {
    onlineMembers.clear();
    offlineMembers.clear();
    equalMembers.clear();
    myInitializedId = null;
    myInitializingId = null;
    aboutToDestroy = false;
    isRecreated = false;
  }

  @Override
  public void markEndDestroyDataStorage() {
    myInitializedId = null;
    myInitializingId = null;
    aboutToDestroyDataStorage = false;
  }

  // PersistentMemberView methods
  @Override
  public PersistentMemberID getMyInitializingID() {
    DiskInitFile dif = ds.getDiskInitFile();
    if (dif == null) {
      return myInitializingId;
    }
    synchronized (dif) {
      return myInitializingId;
    }
  }

  @Override
  public PersistentMemberID getMyPersistentID() {
    DiskInitFile dif = ds.getDiskInitFile();
    if (dif == null) {
      return myInitializedId;
    }
    synchronized (dif) {
      return myInitializedId;
    }
  }

  @Override
  public Set<PersistentMemberID> getOnlineMembers() {
    DiskInitFile dif = ds.getDiskInitFile();
    if (dif == null) {
      return onlineMembers.getSnapshot();
    }
    synchronized (dif) {
      return onlineMembers.getSnapshot();
    }
  }

  @Override
  public Set<PersistentMemberID> getOfflineMembers() {
    DiskInitFile dif = ds.getDiskInitFile();
    if (dif == null) {
      return offlineMembers.getSnapshot();
    }
    synchronized (dif) {
      return offlineMembers.getSnapshot();
    }
  }

  @Override
  public Set<PersistentMemberID> getOfflineAndEqualMembers() {
    DiskInitFile dif = ds.getDiskInitFile();
    if (dif == null) {
      return equalMembers.getSnapshot();
    }
    synchronized (dif) {
      return equalMembers.getSnapshot();
    }
  }

  @Override
  public Set<PersistentMemberPattern> getRevokedMembers() {
    DiskInitFile dif = ds.getDiskInitFile();
    return ds.getRevokedMembers();
  }

  @Override
  public void memberOffline(PersistentMemberID persistentID) {
    ds.memberOffline(this, persistentID);
    if (logger.isTraceEnabled(LogMarker.PERSIST_VERBOSE)) {
      logger.trace(LogMarker.PERSIST_VERBOSE, "PersistentView {} - {} - member offline {}",
          getDiskStoreID().abbrev(), getName(), persistentID);
    }
  }

  @Override
  public void memberOfflineAndEqual(PersistentMemberID persistentID) {
    ds.memberOfflineAndEqual(this, persistentID);
    if (logger.isTraceEnabled(LogMarker.PERSIST_VERBOSE)) {
      logger.trace(LogMarker.PERSIST_VERBOSE,
          "PersistentView {} - {} - member offline and equal {}", getDiskStoreID().abbrev(),
          getName(), persistentID);
    }
  }

  @Override
  public void memberOnline(PersistentMemberID persistentID) {
    ds.memberOnline(this, persistentID);
    if (logger.isTraceEnabled(LogMarker.PERSIST_VERBOSE)) {
      logger.trace(LogMarker.PERSIST_VERBOSE, "PersistentView {} - {} - member online {}",
          getDiskStoreID().abbrev(), getName(), persistentID);
    }
  }

  @Override
  public void memberRemoved(PersistentMemberID persistentID) {
    ds.memberRemoved(this, persistentID);
    if (logger.isTraceEnabled(LogMarker.PERSIST_VERBOSE)) {
      logger.trace(LogMarker.PERSIST_VERBOSE, "PersistentView {} - {} - member removed {}",
          getDiskStoreID().abbrev(), getName(), persistentID);
    }
  }

  @Override
  public void memberRevoked(PersistentMemberPattern revokedPattern) {
    ds.memberRevoked(revokedPattern);
    if (logger.isTraceEnabled(LogMarker.PERSIST_VERBOSE)) {
      logger.trace(LogMarker.PERSIST_VERBOSE, "PersistentView {} - {} - member revoked {}",
          getDiskStoreID().abbrev(), getName(), revokedPattern);
    }
  }

  @Override
  public void setInitializing(PersistentMemberID newId) {
    ds.setInitializing(this, newId);
    if (logger.isTraceEnabled(LogMarker.PERSIST_VERBOSE)) {
      logger.trace(LogMarker.PERSIST_VERBOSE, "PersistentView {} - {} - initializing local id: {}",
          getDiskStoreID().abbrev(), getName(), getMyInitializingID());
    }
  }

  @Override
  public void setInitialized() {
    ds.setInitialized(this);
    if (logger.isTraceEnabled(LogMarker.PERSIST_VERBOSE)) {
      logger.trace(LogMarker.PERSIST_VERBOSE, "PersistentView {} - {} - initialized local id: {}",
          getDiskStoreID().abbrev(), getName(), getMyPersistentID());
    }
  }

  @Override
  public PersistentMemberID generatePersistentID() {
    return ds.generatePersistentID();
  }

  @Override
  public boolean isRecreated() {
    return isRecreated;
  }

  @Override
  public boolean hasConfigChanged() {
    return configChanged;
  }

  @Override
  public void setConfigChanged(boolean v) {
    configChanged = v;
  }

  @Override
  public void endDestroy(LocalRegion region) {
    // Clean up the state if we were ready to recover this region
    if (isReadyForRecovery()) {
      ds.updateDiskRegion(this);
      entriesMapIncompatible = false;
      if (entries != null) {
        ConcurrentMapWithReusableEntries<Object, Object> other =
            entries.getCustomEntryConcurrentHashMap();
        for (Map.Entry<Object, Object> me : other.entrySetWithReusableEntries()) {
          RegionEntry oldRe = (RegionEntry) me.getValue();
          if (oldRe instanceof OffHeapRegionEntry) {
            ((OffHeapRegionEntry) oldRe).release();
          } else {
            // no need to keep iterating; they are all either off heap or on heap.
            break;
          }
        }
      }
      entries = null;
      readyForRecovery = false;
    }

    if (aboutToDestroyDataStorage) {
      ds.endDestroyDataStorage(region, (DiskRegion) this);
      if (logger.isTraceEnabled(LogMarker.PERSIST_VERBOSE)) {
        logger.trace(LogMarker.PERSIST_VERBOSE,
            "PersistentView {} - {} - endDestroyDataStorage: {}", getDiskStoreID().abbrev(),
            getName(), getMyPersistentID());
      }
    } else {
      ds.endDestroyRegion(region, (DiskRegion) this);
      if (logger.isTraceEnabled(LogMarker.PERSIST_VERBOSE)) {
        logger.trace(LogMarker.PERSIST_VERBOSE, "PersistentView {} - {} - endDestroy: {}",
            getDiskStoreID().abbrev(), getName(), getMyPersistentID());
      }
    }
  }

  /**
   * Begin the destroy of everything related to this disk region.
   */
  @Override
  public void beginDestroy(LocalRegion region) {
    beginDestroyRegion(region);
    if (logger.isTraceEnabled(LogMarker.PERSIST_VERBOSE)) {
      logger.trace(LogMarker.PERSIST_VERBOSE, "PersistentView {} - {} - beginDestroy: {}",
          getDiskStoreID().abbrev(), getName(), getMyPersistentID());
    }
    if (myInitializedId == null) {
      endDestroy(region);
    }
  }

  /**
   * Destroy the data storage this this disk region. Destroying the data storage leaves the
   * persistent view, but removes the data.
   */
  @Override
  public void beginDestroyDataStorage() {
    ds.beginDestroyDataStorage((DiskRegion) this);
    if (logger.isTraceEnabled(LogMarker.PERSIST_VERBOSE)) {
      logger.trace(LogMarker.PERSIST_VERBOSE,
          "PersistentView {} - {} - beginDestroyDataStorage: {}", getDiskStoreID().abbrev(),
          getName(), getMyPersistentID());
    }
  }

  public void createDataStorage() {}

  @Override
  public boolean wasAboutToDestroy() {
    return aboutToDestroy;
  }

  @Override
  public boolean wasAboutToDestroyDataStorage() {
    return aboutToDestroyDataStorage;
  }

  /**
   * Set to true once this DiskRegion is ready to be recovered.
   */
  private boolean readyForRecovery;
  /**
   * Total number of entries recovered by restoring from backup. Its initialized right after a
   * recovery but may be updated later as recovered entries go away due to updates and destroys.
   */
  protected int recoveredEntryCount;

  private boolean entriesMapIncompatible;
  private RegionMap entries;
  private AtomicBoolean recoveryCompleted;

  public void setEntriesMapIncompatible(boolean v) {
    entriesMapIncompatible = v;
  }

  @Override
  public boolean isEntriesMapIncompatible() {
    return entriesMapIncompatible;
  }

  public RegionMap useExistingRegionMap(LocalRegion lr) {
    RegionMap result = null;
    if (!entriesMapIncompatible) {
      result = entries;
      // if (result != null) {
      // result.changeOwner(lr);
      // }
    }
    return result;
  }

  private void waitForRecoveryCompletion() {
    boolean interrupted = Thread.interrupted();
    synchronized (recoveryCompleted) {
      try {
        // @todo also check for shutdown of diskstore?
        while (!recoveryCompleted.get()) {
          try {
            recoveryCompleted.wait();
          } catch (InterruptedException ex) {
            interrupted = true;
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  @Override
  public void copyExistingRegionMap(LocalRegion lr) {
    waitForRecoveryCompletion();
    if (entriesMapIncompatible) {
      // Reset the numEntriesInVM. It will be incremented when the copy to the new map is done,
      // down in DiskEntry.Help.initialize. However, the other stats can't be updated
      // there because we don't have the value length at that point. So leave
      // those stats alone.
      numEntriesInVM.set(0);

      lr.initializeStats(getNumEntriesInVM(), getNumOverflowOnDisk(),
          getNumOverflowBytesOnDisk());
      lr.copyRecoveredEntries(entries);
    } else {
      entries.changeOwner(lr);
      lr.initializeStats(getNumEntriesInVM(), getNumOverflowOnDisk(),
          getNumOverflowBytesOnDisk());
      lr.copyRecoveredEntries(null);
    }
    entries = null;
  }

  public void setRecoveredEntryMap(RegionMap rm) {
    recoveryCompleted = new AtomicBoolean();
    entries = rm;
  }

  @Override
  public RegionMap getRecoveredEntryMap() {
    return entries;
  }

  public void releaseRecoveryData() {
    readyForRecovery = false;
  }

  @Override
  public boolean isReadyForRecovery() {
    // better name for this method would be isRecovering
    return readyForRecovery;
  }

  @Override
  public void prepareForRecovery() {
    readyForRecovery = true;
  }

  /**
   * gets the number of entries recovered
   *
   * @since GemFire 3.2.1
   */
  @Override
  public int getRecoveredEntryCount() {
    return recoveredEntryCount;
  }

  @Override
  public void incRecoveredEntryCount() {
    recoveredEntryCount++;
  }

  /**
   * initializes the number of entries recovered
   */
  @Override
  public void initRecoveredEntryCount() {
    if (recoveryCompleted != null) {
      synchronized (recoveryCompleted) {
        recoveryCompleted.set(true);
        recoveryCompleted.notifyAll();
      }
    }
  }

  protected final AtomicLong numOverflowOnDisk;

  @Override
  public long getNumOverflowOnDisk() {
    return numOverflowOnDisk.get();
  }

  @Override
  public void incNumOverflowOnDisk(long delta) {
    numOverflowOnDisk.addAndGet(delta);
  }

  protected final AtomicLong numOverflowBytesOnDisk;

  @Override
  public long getNumOverflowBytesOnDisk() {
    return numOverflowBytesOnDisk.get();
  }

  @Override
  public void incNumOverflowBytesOnDisk(long delta) {
    numOverflowBytesOnDisk.addAndGet(delta);

  }

  protected final AtomicLong numEntriesInVM;

  @Override
  public long getNumEntriesInVM() {
    return numEntriesInVM.get();
  }

  @Override
  public void incNumEntriesInVM(long delta) {
    numEntriesInVM.addAndGet(delta);
  }

  /**
   * Returns true if this region maintains a backup of all its keys and values on disk. Returns
   * false if only values that will not fit in memory are written to disk.
   */
  @Override
  public boolean isBackup() {
    return backup;
  }

  protected void setBackup(boolean v) {
    backup = v;
  }

  public void dump(PrintStream printStream) {
    String name = getName();
    if (isBucket() && !logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
      name = getPrName();
    }
    String msg = name + ":" + " -lru=" + getEvictionAttributes().getAlgorithm();
    if (!getEvictionAttributes().getAlgorithm().isNone()) {
      msg += " -lruAction=" + getEvictionAttributes().getAction();
      if (!getEvictionAttributes().getAlgorithm().isLRUHeap()) {
        msg += " -lruLimit=" + getEvictionAttributes().getMaximum();
      }
    }
    msg += " -concurrencyLevel=" + getConcurrencyLevel() + " -initialCapacity="
        + getInitialCapacity() + " -loadFactor=" + getLoadFactor() + " -offHeap=" + getOffHeap()
        + " -compressor=" + (getCompressorClassName() == null ? "none" : getCompressorClassName())
        + " -statisticsEnabled=" + getStatisticsEnabled();
    if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
      msg += " drId=" + getId() + " isBucket=" + isBucket() + " clearEntryId="
          + getClearOplogEntryId() + " MyInitializingID=<" + getMyInitializingID() + ">"
          + " MyPersistentID=<" + getMyPersistentID() + ">" + " onlineMembers=" + getOnlineMembers()
          + " offlineMembers=" + getOfflineMembers() + " equalsMembers="
          + getOfflineAndEqualMembers();
    }
    printStream.println(msg);
  }

  public String dump2() {
    final String lineSeparator = lineSeparator();
    StringBuilder sb = new StringBuilder();
    String name = getName();
    if (isBucket() && logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
      name = getPrName();
    }
    String msg = name + ":" + " -lru=" + getEvictionAttributes().getAlgorithm();

    sb.append(name);
    sb.append(lineSeparator);
    sb.append("lru=" + getEvictionAttributes().getAlgorithm());
    sb.append(lineSeparator);

    if (!getEvictionAttributes().getAlgorithm().isNone()) {
      sb.append("lruAction=" + getEvictionAttributes().getAction());
      sb.append(lineSeparator);

      if (!getEvictionAttributes().getAlgorithm().isLRUHeap()) {
        sb.append("lruAction=" + getEvictionAttributes().getAction());
        sb.append(lineSeparator);
      }
    }

    sb.append("-concurrencyLevel=" + getConcurrencyLevel());
    sb.append(lineSeparator);
    sb.append("-initialCapacity=" + getInitialCapacity());
    sb.append(lineSeparator);
    sb.append("-loadFactor=" + getLoadFactor());
    sb.append(lineSeparator);
    sb.append("-offHeap=" + getOffHeap());
    sb.append(lineSeparator);
    sb.append(
        "-compressor=" + (getCompressorClassName() == null ? "none" : getCompressorClassName()));
    sb.append(lineSeparator);
    sb.append("-statisticsEnabled=" + getStatisticsEnabled());
    sb.append(lineSeparator);

    if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY_VERBOSE)) {
      sb.append("drId=" + getId());
      sb.append(lineSeparator);
      sb.append("isBucket=" + isBucket());
      sb.append(lineSeparator);
      sb.append("clearEntryId=" + getClearOplogEntryId());
      sb.append(lineSeparator);
      sb.append("MyInitializingID=<" + getMyInitializingID() + ">");
      sb.append(lineSeparator);
      sb.append("MyPersistentID=<" + getMyPersistentID() + ">");
      sb.append(lineSeparator);
      sb.append("onlineMembers=" + getOnlineMembers());
      sb.append(lineSeparator);
      sb.append("offlineMembers=" + getOfflineMembers());
      sb.append(lineSeparator);
      sb.append("equalsMembers=" + getOfflineAndEqualMembers());
      sb.append(lineSeparator);
      sb.append("flags=").append(getFlags());
      sb.append(lineSeparator);
    }
    return sb.toString();
  }

  public void dumpMetadata() {
    String name = getName();

    StringBuilder msg = new StringBuilder(name);

    dumpCommonAttributes(msg);

    dumpPersistentView(msg);

    System.out.println(msg);
  }

  /**
   * Dump the (bucket specific) persistent view to the string builder
   */
  public void dumpPersistentView(StringBuilder msg) {
    msg.append("\n\tMyInitializingID=<").append(getMyInitializingID()).append(">");
    msg.append("\n\tMyPersistentID=<").append(getMyPersistentID()).append(">");

    msg.append("\n\tonlineMembers:");
    for (PersistentMemberID id : getOnlineMembers()) {
      msg.append("\n\t\t").append(id);
    }

    msg.append("\n\tofflineMembers:");
    for (PersistentMemberID id : getOfflineMembers()) {
      msg.append("\n\t\t").append(id);
    }

    msg.append("\n\tequalsMembers:");
    for (PersistentMemberID id : getOfflineAndEqualMembers()) {
      msg.append("\n\t\t").append(id);
    }
  }

  /**
   * Dump the attributes which are common across the PR to the string builder.
   */
  public void dumpCommonAttributes(StringBuilder msg) {
    msg.append("\n\tlru=").append(getEvictionAttributes().getAlgorithm());
    if (!getEvictionAttributes().getAlgorithm().isNone()) {
      msg.append("\n\tlruAction=").append(getEvictionAttributes().getAction());
      if (!getEvictionAttributes().getAlgorithm().isLRUHeap()) {
        msg.append("\n\tlruLimit=").append(getEvictionAttributes().getMaximum());
      }
    }

    msg.append("\n\tconcurrencyLevel=").append(getConcurrencyLevel());
    msg.append("\n\tinitialCapacity=").append(getInitialCapacity());
    msg.append("\n\tloadFactor=").append(getLoadFactor());
    msg.append("\n\toffHeap=").append(getOffHeap());
    msg.append("\n\tstatisticsEnabled=").append(getStatisticsEnabled());

    msg.append("\n\tdrId=").append(getId());
    msg.append("\n\tisBucket=").append(isBucket());
    msg.append("\n\tclearEntryId=").append(getClearOplogEntryId());
    msg.append("\n\tflags=").append(getFlags());
  }

  /**
   * This method was added to fix bug 40192. It is like getBytesAndBits except it will return
   * Token.REMOVE_PHASE1 if the htreeReference has changed (which means a clear was done).
   *
   * @return an instance of BytesAndBits or Token.REMOVED_PHASE1
   */
  @Override
  public Object getRaw(DiskId id) {
    acquireReadLock();
    try {
      return getDiskStore().getRaw(this, id);
    } finally {
      releaseReadLock();
    }
  }

  @Override
  public RegionVersionVector getRegionVersionVector() {
    return versionVector;
  }

  public long getVersionForMember(VersionSource member) {
    return versionVector.getVersionForMember(member);
  }

  public void recordRecoveredGCVersion(VersionSource member, long gcVersion) {
    versionVector.recordGCVersion(member, gcVersion);

  }

  public void recordRecoveredVersionHolder(VersionSource member, RegionVersionHolder versionHolder,
      boolean latestOplog) {
    versionVector.initRecoveredVersion(member, versionHolder, latestOplog);
  }

  public void recordRecoveredVersionTag(VersionTag tag) {
    versionVector.recordVersion(tag.getMemberID(), tag.getRegionVersion());
  }

  /**
   * Indicate that the current RVV for this disk region does not accurately reflect what has been
   * recorded on disk. This is true while we are in the middle of a GII, because we record the new
   * RVV at the beginning of the GII. If we recover in this state, we need to know that the
   * recovered RVV is not something we can use to do a delta GII.
   */
  public void setRVVTrusted(boolean trusted) {
    rvvTrusted = trusted;
  }

  public boolean getRVVTrusted() {
    return rvvTrusted;
  }

  public PersistentOplogSet getOplogSet() {
    return getDiskStore().getPersistentOplogSet(this);
  }

  @Override
  public String getCompressorClassName() {
    return compressorClassName;
  }

  @Override
  public Compressor getCompressor() {
    return compressor;
  }

  @Override
  public boolean getOffHeap() {
    return offHeap;
  }

  @Override
  public CachePerfStats getCachePerfStats() {
    return ds.getCache().getCachePerfStats();
  }

  @Override
  public void oplogRecovered(long oplogId) {
    // do nothing. Overridden in ExportDiskRegion
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + ":" + getName();
  }

  @Override
  public void incRecentlyUsed() {
    entries.incRecentlyUsed();
  }

  @Override
  public StatisticsFactory getStatisticsFactory() {
    return ds.getStatisticsFactory();
  }

  @Override
  public String getNameForStats() {
    if (isBucket()) {
      return getPrName();
    } else {
      return getName();
    }
  }

  @Override
  public InternalCache getCache() {
    return getDiskStore().getCache();
  }
}
