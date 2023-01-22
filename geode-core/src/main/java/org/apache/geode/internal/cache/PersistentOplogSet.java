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

import java.io.File;
import java.io.FilenameFilter;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.internal.cache.DiskStoreImpl.OplogEntryIdSet;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.entries.DiskEntry.Helper.ValueWrapper;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.persistence.DiskStoreFilter;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.internal.cache.persistence.OplogType;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.sequencelog.EntryLogger;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class PersistentOplogSet implements OplogSet {
  private static final Logger logger = LogService.getLogger();

  /** variable to generate sequential unique oplogEntryId's* */
  private final AtomicLong oplogEntryId = new AtomicLong(DiskStoreImpl.INVALID_ID);

  /**
   * Contains all the oplogs that only have a drf (i.e. the crf has been deleted).
   */
  private final Map<Long, Oplog> drfOnlyOplogs = new LinkedHashMap<>();

  private final Map<Long, Oplog> oplogIdToOplog = new LinkedHashMap<>();

  /** oplogs that are done being written to but not yet ready to compact */
  private final Map<Long, Oplog> inactiveOplogs = new LinkedHashMap<>(16, 0.75f, true);

  private final DiskStoreImpl parent;

  private final AtomicInteger inactiveOpenCount = new AtomicInteger();

  private final Map<Long, DiskRecoveryStore> pendingRecoveryMap = new HashMap<>();
  private final Map<Long, DiskRecoveryStore> currentRecoveryMap = new HashMap<>();

  private final AtomicBoolean alreadyRecoveredOnce = new AtomicBoolean(false);

  private final PrintStream out;

  /** The active oplog * */
  private volatile Oplog child;

  /**
   * The maximum oplog id we saw while recovering
   */
  private volatile long maxRecoveredOplogId;

  /** counter used for round-robin logic * */
  private int dirCounter = -1;

  public PersistentOplogSet(DiskStoreImpl parent, PrintStream out) {
    this.parent = parent;
    this.out = out;
  }

  /**
   * returns the active child
   */
  public Oplog getChild() {
    return child;
  }

  /**
   * set the child to a new oplog
   */
  void setChild(Oplog oplog) {
    child = oplog;
  }

  Oplog[] getAllOplogs() {
    synchronized (getOplogIdToOplog()) {
      int rollNum = getOplogIdToOplog().size();
      int inactiveNum = inactiveOplogs.size();
      int drfOnlyNum = drfOnlyOplogs.size();
      int num = rollNum + inactiveNum + drfOnlyNum + 1;
      Oplog[] oplogs = new Oplog[num];
      oplogs[0] = getChild();

      Iterator<Oplog> oplogIterator = getOplogIdToOplog().values().iterator();
      for (int i = 1; i <= rollNum; i++) {
        oplogs[i] = oplogIterator.next();
      }

      oplogIterator = inactiveOplogs.values().iterator();
      for (int i = 1; i <= inactiveNum; i++) {
        oplogs[i + rollNum] = oplogIterator.next();
      }

      oplogIterator = drfOnlyOplogs.values().iterator();
      for (int i = 1; i <= drfOnlyNum; i++) {
        oplogs[i + rollNum + inactiveNum] = oplogIterator.next();
      }

      // Special case - no oplogs found
      if (oplogs.length == 1 && oplogs[0] == null) {
        return new Oplog[0];
      }

      return oplogs;
    }

  }

  private TreeSet<Oplog> getSortedOplogs() {
    TreeSet<Oplog> result = new TreeSet<>(
        (Comparator) (arg0, arg1) -> Long
            .signum(((Oplog) arg1).getOplogId() - ((Oplog) arg0).getOplogId()));
    for (Oplog oplog : getAllOplogs()) {
      if (oplog != null) {
        result.add(oplog);
      }
    }
    return result;
  }

  /**
   * Get the oplog specified
   *
   * @param id int oplogId to be got
   * @return Oplogs the oplog corresponding to the oplodId, id
   */
  @Override
  public Oplog getChild(long id) {
    Oplog localOplog = child;
    if (localOplog != null && id == localOplog.getOplogId()) {
      return localOplog;
    }

    synchronized (getOplogIdToOplog()) {
      Oplog result = getOplogIdToOplog().get(id);
      if (result == null) {
        result = inactiveOplogs.get(id);
      }
      return result;
    }
  }

  @Override
  public void create(InternalRegion region, DiskEntry entry, ValueWrapper value, boolean async) {
    getChild().create(region, entry, value, async);
  }

  @Override
  public void modify(InternalRegion region, DiskEntry entry, ValueWrapper value, boolean async) {
    getChild().modify(region, entry, value, async);
  }

  void offlineModify(DiskRegionView drv, DiskEntry entry, byte[] value,
      boolean isSerializedObject) {
    getChild().offlineModify(drv, entry, value, isSerializedObject);
  }

  @Override
  public void remove(InternalRegion region, DiskEntry entry, boolean async, boolean isClear) {
    getChild().remove(region, entry, async, isClear);
  }

  public void forceRoll(DiskRegion dr) {
    Oplog child = getChild();
    if (child != null) {
      child.forceRolling(dr);
    }
  }

  Map<File, DirectoryHolder> findFiles(String partialFileName) {
    dirCounter = 0;
    Map<File, DirectoryHolder> backupFiles = new HashMap<>();
    FilenameFilter backupFileFilter = getFileNameFilter(partialFileName);

    for (DirectoryHolder dh : parent.directories) {
      File[] backupList = dh.getDir().listFiles(backupFileFilter);
      if (backupList != null) {
        for (File f : backupList) {
          backupFiles.put(f, dh);
        }
      }
    }

    return backupFiles;
  }

  private FilenameFilter getFileNameFilter(String partialFileName) {
    return new DiskStoreFilter(OplogType.BACKUP, false, partialFileName);
  }

  void createOplogs(boolean needsOplogs, Map<File, DirectoryHolder> backupFiles) {
    LongOpenHashSet foundCrfs = new LongOpenHashSet();
    LongOpenHashSet foundDrfs = new LongOpenHashSet();

    for (Map.Entry<File, DirectoryHolder> entry : backupFiles.entrySet()) {
      File file = entry.getKey();
      String absolutePath = file.getAbsolutePath();

      int underscorePosition = absolutePath.lastIndexOf('_');
      int pointPosition = absolutePath.lastIndexOf('.');
      String oplogIdString = absolutePath.substring(underscorePosition + 1, pointPosition);
      long oplogId = Long.parseLong(oplogIdString);

      maxRecoveredOplogId = Math.max(maxRecoveredOplogId, oplogId);

      // here look diskinit file and check if this opid already deleted or not
      // if deleted then don't process it.

      if (Oplog.isCRFFile(file.getName())) {
        if (!isCrfOplogIdPresent(oplogId)) {
          deleteFileOnRecovery(file);
          try {
            String krfFileName = Oplog.getKRFFilenameFromCRFFilename(file.getAbsolutePath());
            File krfFile = new File(krfFileName);
            deleteFileOnRecovery(krfFile);
          } catch (Exception ignore) {
            // ignore
          }
          // this file we unable to delete earlier
          continue;
        }

      } else if (Oplog.isDRFFile(file.getName())) {
        if (!isDrfOplogIdPresent(oplogId)) {
          deleteFileOnRecovery(file);
          // this file we unable to delete earlier
          continue;
        }
      }

      Oplog oplog = getChild(oplogId);
      if (oplog == null) {
        oplog = new Oplog(oplogId, this);
        addRecoveredOplog(oplog);
      }

      if (oplog.addRecoveredFile(file, entry.getValue())) {
        foundCrfs.add(oplogId);
      } else {
        foundDrfs.add(oplogId);
      }
    }
    if (needsOplogs) {
      verifyOplogs(foundCrfs, foundDrfs);
    }
  }

  private boolean isDrfOplogIdPresent(long oplogId) {
    return parent.getDiskInitFile().isDRFOplogIdPresent(oplogId);
  }

  private boolean isCrfOplogIdPresent(long oplogId) {
    return parent.getDiskInitFile().isCRFOplogIdPresent(oplogId);
  }

  private void verifyOplogs(LongOpenHashSet foundCrfs, LongOpenHashSet foundDrfs) {
    parent.getDiskInitFile().verifyOplogs(foundCrfs, foundDrfs);
  }


  private void deleteFileOnRecovery(File f) {
    try {
      f.delete();
    } catch (Exception ignore) {
      // ignore, one more attempt to delete the file failed
    }
  }

  private void addRecoveredOplog(Oplog oplog) {
    basicAddToBeCompacted(oplog);
    // don't schedule a compaction here. Wait for recovery to complete
  }

  /**
   * Taking a lock on the LinkedHashMap oplogIdToOplog as it the operation of adding an Oplog to the
   * Map & notifying the Compactor thread , if not already compaction has to be an atomic operation.
   * add the oplog to the to be compacted set. if compactor thread is active and recovery is not
   * going on then the compactor thread is notified of the addition
   */
  void addToBeCompacted(Oplog oplog) {
    basicAddToBeCompacted(oplog);
    parent.scheduleCompaction();
  }

  private void basicAddToBeCompacted(Oplog oplog) {
    if (!oplog.isRecovering() && oplog.hasNoLiveValues()) {
      oplog.cancelKrf();
      oplog.close();
      oplog.deleteFiles(oplog.getHasDeletes());

    } else {
      parent.getStats().incCompactableOplogs(1);
      Long key = oplog.getOplogId();
      int inactivePromotedCount = 0;

      synchronized (getOplogIdToOplog()) {
        if (inactiveOplogs.remove(key) != null) {
          if (oplog.isRAFOpen()) {
            inactiveOpenCount.decrementAndGet();
          }
          inactivePromotedCount++;
        }
        getOplogIdToOplog().put(key, oplog);
      }

      if (inactivePromotedCount > 0) {
        parent.getStats().incInactiveOplogs(-inactivePromotedCount);
      }
    }
  }

  void recoverRegionsThatAreReady() {
    // The following sync also prevents concurrent recoveries by multiple regions
    // which is needed currently.
    synchronized (getAlreadyRecoveredOnce()) {

      // need to take a snapshot of DiskRecoveryStores we will recover
      synchronized (pendingRecoveryMap) {
        currentRecoveryMap.clear();
        currentRecoveryMap.putAll(pendingRecoveryMap);
        pendingRecoveryMap.clear();
      }

      if (currentRecoveryMap.isEmpty() && getAlreadyRecoveredOnce().get()) {
        // no recovery needed
        return;
      }

      for (DiskRecoveryStore drs : currentRecoveryMap.values()) {
        // Call prepare early to fix bug 41119.
        drs.getDiskRegionView().prepareForRecovery();
      }

      if (!getAlreadyRecoveredOnce().get()) {
        initOplogEntryId();
        // Fix for #43026 - make sure we don't reuse an entry
        // id that has been marked as cleared.
        updateOplogEntryId(parent.getDiskInitFile().getMaxRecoveredClearEntryId());
      }

      long start = parent.getStats().startRecovery();
      EntryLogger.setSource(parent.getDiskStoreID(), "recovery");

      long byteCount = 0;
      try {
        byteCount = recoverOplogs(byteCount);

      } finally {
        Map<String, Integer> prSizes = null;
        Map<String, Integer> prBuckets = null;
        if (parent.isValidating()) {
          prSizes = new HashMap<>();
          prBuckets = new HashMap<>();
        }

        for (DiskRecoveryStore drs : currentRecoveryMap.values()) {
          for (Oplog oplog : getAllOplogs()) {
            if (oplog != null) {
              // Need to do this AFTER recovery to protect from concurrent compactions
              // trying to remove the oplogs.
              // We can't remove a dr from the oplog's unrecoveredRegionCount
              // until it is fully recovered.
              // This fixes bug 41119.
              oplog.checkForRecoverableRegion(drs.getDiskRegionView());
            }
          }

          if (parent.isValidating()) {
            if (drs instanceof ValidatingDiskRegion) {
              ValidatingDiskRegion vdr = (ValidatingDiskRegion) drs;
              if (vdr.isBucket()) {
                String prName = vdr.getPrName();
                if (prSizes.containsKey(prName)) {
                  int oldSize = prSizes.get(prName);
                  oldSize += vdr.size();
                  prSizes.put(prName, oldSize);
                  int oldBuckets = prBuckets.get(prName);
                  oldBuckets++;
                  prBuckets.put(prName, oldBuckets);
                } else {
                  prSizes.put(prName, vdr.size());
                  prBuckets.put(prName, 1);
                }
              } else {
                parent.incLiveEntryCount(vdr.size());
                out.println(vdr.getName() + ": entryCount=" + vdr.size());
              }
            }
          }
        }

        if (parent.isValidating()) {
          for (Map.Entry<String, Integer> me : prSizes.entrySet()) {
            parent.incLiveEntryCount(me.getValue());
            out.println(me.getKey() + " entryCount=" + me.getValue() + " bucketCount="
                + prBuckets.get(me.getKey()));
          }
        }

        parent.getStats().endRecovery(start, byteCount);
        getAlreadyRecoveredOnce().set(true);
        currentRecoveryMap.clear();
        EntryLogger.clearSource();
      }
    }
  }

  private long recoverOplogs(long byteCount) {
    OplogEntryIdSet deletedIds = new OplogEntryIdSet();
    TreeSet<Oplog> oplogSet = getSortedOplogs();

    if (!getAlreadyRecoveredOnce().get()) {
      if (getChild() != null && !getChild().hasBeenUsed()) {
        // Then remove the current child since it is empty
        // and does not need to be recovered from
        // and it is important to not call initAfterRecovery on it.
        oplogSet.remove(getChild());
      }
    }

    Set<Oplog> oplogsNeedingValueRecovery = new HashSet<>();

    if (!oplogSet.isEmpty()) {
      long startOpLogRecovery = System.currentTimeMillis();

      // first figure out all entries that have been destroyed
      boolean latestOplog = true;
      for (Oplog oplog : oplogSet) {
        byteCount += oplog.recoverDrf(deletedIds, getAlreadyRecoveredOnce().get(), latestOplog);
        latestOplog = false;
        if (!getAlreadyRecoveredOnce().get()) {
          updateOplogEntryId(oplog.getMaxRecoveredOplogEntryId());
        }
      }

      parent.incDeadRecordCount(deletedIds.size());

      // now figure out live entries
      latestOplog = true;
      for (Oplog oplog : oplogSet) {
        long startOpLogRead = parent.getStats().startOplogRead();
        long bytesRead = oplog.recoverCrf(deletedIds, recoverValues(), recoverValuesSync(),
            getAlreadyRecoveredOnce().get(), oplogsNeedingValueRecovery, latestOplog);
        latestOplog = false;
        if (!getAlreadyRecoveredOnce().get()) {
          updateOplogEntryId(oplog.getMaxRecoveredOplogEntryId());
        }
        byteCount += bytesRead;
        parent.getStats().endOplogRead(startOpLogRead, bytesRead);

        // Callback to the disk regions to indicate the oplog is recovered
        // Used for offline export
        for (DiskRecoveryStore drs : currentRecoveryMap.values()) {
          drs.getDiskRegionView().oplogRecovered(oplog.oplogId);
        }
      }

      long endOpLogRecovery = System.currentTimeMillis();
      long elapsed = endOpLogRecovery - startOpLogRecovery;
      logger.info("recovery oplog load took {} ms", elapsed);
    }

    if (!parent.isOfflineCompacting()) {
      long startRegionInit = System.currentTimeMillis();

      // create the oplogs now so that loadRegionData can have them available
      // Create an array of Oplogs so that we are able to add it in a single shot
      // to the map
      for (DiskRecoveryStore drs : currentRecoveryMap.values()) {
        drs.getDiskRegionView().initRecoveredEntryCount();
      }

      if (!getAlreadyRecoveredOnce().get()) {
        for (Oplog oplog : oplogSet) {
          if (oplog != getChild()) {
            oplog.initAfterRecovery(parent.isOffline());
          }
        }
        if (getChild() == null) {
          setFirstChild(getSortedOplogs(), false);
        }
      }

      if (!parent.isOffline()) {
        if (recoverValues() && !recoverValuesSync()) {
          // value recovery defers compaction because it is using the compactor thread
          parent.scheduleValueRecovery(oplogsNeedingValueRecovery, currentRecoveryMap);
        }

        if (!getAlreadyRecoveredOnce().get()) {
          // Create krfs for oplogs that are missing them
          for (Oplog oplog : oplogSet) {
            if (oplog.needsKrf()) {
              oplog.createKrfAsync();
            }
          }
          parent.scheduleCompaction();
        }

        long endRegionInit = System.currentTimeMillis();
        logger.info("recovery region initialization took {} ms", endRegionInit - startRegionInit);
      }
    }
    return byteCount;
  }

  private boolean recoverValuesSync() {
    return parent.RECOVER_VALUES_SYNC;
  }

  private boolean recoverValues() {
    return parent.RECOVER_VALUES;
  }

  private void setFirstChild(TreeSet<Oplog> oplogSet, boolean force) {
    if (parent.isOffline() && !parent.isOfflineCompacting() && !parent.isOfflineModify()) {
      return;
    }

    if (!oplogSet.isEmpty()) {
      Oplog first = oplogSet.first();
      DirectoryHolder dh = first.getDirectoryHolder();
      dirCounter = dh.getArrayIndex();
      dirCounter = ++dirCounter % parent.dirLength;
      // we want the first child to go in the directory after the directory
      // used by the existing oplog with the max id.
      // This fixes bug 41822.
    }

    if (force || maxRecoveredOplogId > 0) {
      setChild(new Oplog(maxRecoveredOplogId + 1, this, getNextDir()));
    }
  }

  private void initOplogEntryId() {
    oplogEntryId.set(DiskStoreImpl.INVALID_ID);
  }

  /**
   * Sets the last created oplogEntryId to the given value if and only if the given value is greater
   * than the current last created oplogEntryId
   */
  private void updateOplogEntryId(long v) {
    long curVal;
    do {
      curVal = oplogEntryId.get();
      if (curVal >= v) {
        // no need to set
        return;
      }
    } while (!oplogEntryId.compareAndSet(curVal, v));
  }

  /**
   * Returns the last created oplogEntryId. Returns INVALID_ID if no oplogEntryId has been created.
   */
  long getOplogEntryId() {
    parent.initializeIfNeeded();
    return oplogEntryId.get();
  }

  /**
   * Creates and returns a new oplogEntryId for the given key. An oplogEntryId is needed when
   * storing a key/value pair on disk. A new one is only needed if the key is new. Otherwise the
   * oplogEntryId already allocated for a key can be reused for the same key.
   *
   * @return A disk id that can be used to access this key/value pair on disk
   */
  long newOplogEntryId() {
    return oplogEntryId.incrementAndGet();
  }

  /**
   * Returns the next available DirectoryHolder which has space. If no dir has space then it will
   * return one anyway if compaction is enabled.
   *
   * @param minAvailableSpace the minimum amount of space we need in this directory.
   */
  DirectoryHolder getNextDir(long minAvailableSpace, boolean checkForWarning) {
    if (minAvailableSpace < parent.getMaxOplogSizeInBytes()
        && !DiskStoreImpl.SET_IGNORE_PREALLOCATE) {
      minAvailableSpace = parent.getMaxOplogSizeInBytes();
    }

    DirectoryHolder selectedHolder = null;
    synchronized (parent.getDirectoryHolders()) {
      for (int i = 0; i < parent.dirLength; ++i) {
        DirectoryHolder dirHolder = parent.directories[dirCounter];

        // Increment the directory counter to next position so that next time when this operation
        // is invoked, it checks for the Directory Space in a cyclical fashion.
        dirCounter = ++dirCounter % parent.dirLength;

        // if the current directory has some space, then quit and return the dir
        if (dirHolder.getAvailableSpace() >= minAvailableSpace) {
          if (checkForWarning && !parent.isDirectoryUsageNormal(dirHolder)) {
            if (logger.isDebugEnabled()) {
              logger.debug("Ignoring directory {} due to insufficient disk space", dirHolder);
            }
          } else {
            selectedHolder = dirHolder;
            break;
          }
        }
      }

      if (selectedHolder == null) {
        // we didn't find a warning-free directory, try again ignoring the check
        if (checkForWarning) {
          return getNextDir(minAvailableSpace, false);
        }

        if (parent.isCompactionEnabled()) {
          selectedHolder = parent.directories[dirCounter];

          // Increment the directory counter to next position
          dirCounter = ++dirCounter % parent.dirLength;
          if (selectedHolder.getAvailableSpace() < minAvailableSpace) {
            logger.warn(
                "Even though the configured directory size limit has been exceeded a new oplog will be created because compaction is enabled. The configured limit is {}. The current space used in the directory by this disk store is {}.",
                selectedHolder.getUsedSpace(), selectedHolder.getCapacity());
          }
        } else {
          throw new DiskAccessException(
              "Disk is full and compaction is disabled. No space can be created", parent);
        }
      }
    }

    return selectedHolder;
  }

  DirectoryHolder getNextDir() {
    return getNextDir(DiskStoreImpl.MINIMUM_DIR_SIZE, true);
  }

  private void addDrf(Oplog oplog) {
    synchronized (getOplogIdToOplog()) {
      drfOnlyOplogs.put(oplog.getOplogId(), oplog);
    }
  }

  void removeDrf(Oplog oplog) {
    synchronized (getOplogIdToOplog()) {
      drfOnlyOplogs.remove(oplog.getOplogId());
    }
  }

  /**
   * Return true if id is less than all the ids in the oplogIdToOplog map. Since the oldest one is
   * in the LINKED hash map is first we only need to compare ourselves to it.
   */
  boolean isOldestExistingOplog(long id) {
    synchronized (getOplogIdToOplog()) {
      for (long otherId : getOplogIdToOplog().keySet()) {
        if (id > otherId) {
          return false;
        }
      }

      // since the inactiveOplogs map is an LRU we need to check each one
      for (long otherId : inactiveOplogs.keySet()) {
        if (id > otherId) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Destroy all the oplogs that are:
   *
   * <pre>
   * 1. the oldest (based on smallest oplog id)
   * 2. empty (have no live values)
   * </pre>
   */
  void destroyOldestReadyToCompact() {
    synchronized (getOplogIdToOplog()) {
      if (drfOnlyOplogs.isEmpty()) {
        return;
      }
    }

    Oplog oldestLiveOplog = getOldestLiveOplog();
    List<Oplog> toDestroy = new ArrayList<>();

    if (oldestLiveOplog == null) {
      // remove all oplogs that are empty
      synchronized (getOplogIdToOplog()) {
        toDestroy.addAll(drfOnlyOplogs.values());
      }

    } else {
      // remove all empty oplogs that are older than the oldest live one
      synchronized (getOplogIdToOplog()) {
        for (Oplog oplog : drfOnlyOplogs.values()) {
          if (oplog.getOplogId() < oldestLiveOplog.getOplogId()) {
            toDestroy.add(oplog);
          }
        }
      }
    }

    for (Oplog oplog : toDestroy) {
      oplog.destroy();
    }
  }

  private Oplog getOldestLiveOplog() {
    Oplog result = null;
    synchronized (getOplogIdToOplog()) {
      for (Oplog oplog : getOplogIdToOplog().values()) {
        if (result == null || oplog.getOplogId() < result.getOplogId()) {
          result = oplog;
        }
      }

      // since the inactiveOplogs map is an LRU we need to check each one
      for (Oplog oplog : inactiveOplogs.values()) {
        if (result == null || oplog.getOplogId() < result.getOplogId()) {
          result = oplog;
        }
      }
    }

    return result;
  }

  void inactiveAccessed(Oplog oplog) {
    Long key = oplog.getOplogId();
    synchronized (getOplogIdToOplog()) {
      // update last access time
      inactiveOplogs.get(key);
    }
  }

  void inactiveReopened(Oplog oplog) {
    addInactive(oplog, true);
  }

  void addInactive(Oplog oplog) {
    addInactive(oplog, false);
  }

  private void addInactive(Oplog oplog, boolean reopen) {
    Long key = oplog.getOplogId();
    List<Oplog> openlist = null;
    synchronized (getOplogIdToOplog()) {

      boolean isInactive = true;
      if (reopen) {
        // It is possible that 'oplog' is compactible instead of inactive. So set isInactive.
        isInactive = inactiveOplogs.get(key) != null;
      } else {
        inactiveOplogs.put(key, oplog);
      }

      if (reopen && isInactive || oplog.isRAFOpen()) {
        if (inactiveOpenCount.incrementAndGet() > DiskStoreImpl.MAX_OPEN_INACTIVE_OPLOGS) {
          openlist = new ArrayList<>();
          for (Oplog inactiveOplog : inactiveOplogs.values()) {
            if (inactiveOplog.isRAFOpen()) {
              // add to my list
              openlist.add(inactiveOplog);
            }
          }
        }
      }
    }

    if (openlist != null) {
      for (Oplog openOplog : openlist) {
        if (openOplog.closeRAF()) {
          if (inactiveOpenCount.decrementAndGet() <= DiskStoreImpl.MAX_OPEN_INACTIVE_OPLOGS) {
            break;
          }
        }
      }
    }

    if (!reopen) {
      parent.getStats().incInactiveOplogs(1);
    }
  }

  public void clear(DiskRegion diskRegion, RegionVersionVector<DiskStoreID> regionVersionVector) {
    // call clear on each oplog
    Collection<Oplog> oplogsToClear = new ArrayList<>();
    synchronized (getOplogIdToOplog()) {
      oplogsToClear.addAll(getOplogIdToOplog().values());
      oplogsToClear.addAll(inactiveOplogs.values());

      Oplog child = getChild();
      if (child != null) {
        oplogsToClear.add(child);
      }
    }

    for (Oplog oplog : oplogsToClear) {
      oplog.clear(diskRegion, regionVersionVector);
    }

    if (regionVersionVector != null) {
      parent.getDiskInitFile().clearRegion(diskRegion, regionVersionVector);
    } else {
      long clearedOplogEntryId = getOplogEntryId();
      parent.getDiskInitFile().clearRegion(diskRegion, clearedOplogEntryId);
    }
  }

  public RuntimeException close() {
    RuntimeException firstRuntimeException = null;
    try {
      closeOtherOplogs();
    } catch (RuntimeException e) {
      firstRuntimeException = e;
    }

    if (child != null) {
      try {
        child.finishKrf();
      } catch (RuntimeException e) {
        if (firstRuntimeException != null) {
          firstRuntimeException = e;
        }
      }

      try {
        child.close();
      } catch (RuntimeException e) {
        if (firstRuntimeException != null) {
          firstRuntimeException = e;
        }
      }
    }

    return firstRuntimeException;
  }

  /** closes all the oplogs except the current one * */
  private void closeOtherOplogs() {
    // get a snapshot to prevent CME
    Oplog[] oplogs = getAllOplogs();

    // if there are oplogs which are to be compacted, destroy them do not do oplogs[0]
    for (int i = 1; i < oplogs.length; i++) {
      oplogs[i].finishKrf();
      oplogs[i].close();
      removeOplog(oplogs[i].getOplogId());
    }
  }

  /**
   * Removes the oplog from the map given the oplogId
   *
   * @param id id of the oplog to be removed from the list
   * @return oplog Oplog which has been removed
   */
  Oplog removeOplog(long id) {
    return removeOplog(id, false, null);
  }

  Oplog removeOplog(long id, boolean deleting, Oplog olgToAddToDrfOnly) {
    Oplog oplog;
    boolean drfOnly = false;
    boolean inactive = false;

    synchronized (getOplogIdToOplog()) {
      Long key = id;
      oplog = getOplogIdToOplog().remove(key);
      if (oplog == null) {
        oplog = inactiveOplogs.remove(key);
        if (oplog != null) {
          if (oplog.isRAFOpen()) {
            inactiveOpenCount.decrementAndGet();
          }
          inactive = true;
        } else {
          oplog = drfOnlyOplogs.remove(key);
          if (oplog != null) {
            drfOnly = true;
          }
        }
      }

      if (olgToAddToDrfOnly != null) {
        addDrf(olgToAddToDrfOnly);
      }
    }

    if (oplog != null) {
      if (!drfOnly) {
        if (inactive) {
          parent.getStats().incInactiveOplogs(-1);
        } else {
          parent.getStats().incCompactableOplogs(-1);
        }
      }

      if (!deleting && !oplog.isOplogEmpty()) {
        // we are removing an oplog whose files are not deleted
        parent.undeletedOplogSize.addAndGet(oplog.getOplogSize());
      }
    }
    return oplog;
  }

  public void basicClose(DiskRegion dr) {
    List<Oplog> oplogsToClose = new ArrayList<>();
    synchronized (getOplogIdToOplog()) {
      oplogsToClose.addAll(getOplogIdToOplog().values());
      oplogsToClose.addAll(inactiveOplogs.values());
      oplogsToClose.addAll(drfOnlyOplogs.values());

      Oplog child = getChild();
      if (child != null) {
        oplogsToClose.add(child);
      }
    }

    for (Oplog oplog : oplogsToClose) {
      oplog.close(dr);
    }
  }

  public void prepareForClose() {
    Collection<Oplog> oplogsToPrepare = new ArrayList<>();
    synchronized (getOplogIdToOplog()) {
      oplogsToPrepare.addAll(getOplogIdToOplog().values());
      oplogsToPrepare.addAll(inactiveOplogs.values());
    }

    boolean childPreparedForClose = false;
    long childOplogId = getChild() == null ? -1 : getChild().oplogId;

    for (Oplog oplog : oplogsToPrepare) {
      oplog.prepareForClose();
      if (childOplogId != -1 && oplog.oplogId == childOplogId) {
        childPreparedForClose = true;
      }
    }

    if (!childPreparedForClose && getChild() != null) {
      getChild().prepareForClose();
    }
  }

  public void basicDestroy(DiskRegion diskRegion) {
    Collection<Oplog> oplogsToDestroy = new ArrayList<>();
    synchronized (getOplogIdToOplog()) {
      oplogsToDestroy.addAll(getOplogIdToOplog().values());
      oplogsToDestroy.addAll(inactiveOplogs.values());
      oplogsToDestroy.addAll(drfOnlyOplogs.values());

      Oplog child = getChild();
      if (child != null) {
        oplogsToDestroy.add(child);
      }
    }

    for (Oplog oplog : oplogsToDestroy) {
      oplog.destroy(diskRegion);
    }
  }

  void destroyAllOplogs() {
    // get a snapshot to prevent ConcurrentModificationException
    for (Oplog oplog : getAllOplogs()) {
      if (oplog != null) {
        oplog.destroy();
        removeOplog(oplog.getOplogId());
      }
    }
  }

  /**
   * Add compactable oplogs to the list, up to the maximum size.
   */
  void getCompactableOplogs(List<CompactableOplog> compactableOplogs, int max) {
    synchronized (getOplogIdToOplog()) {
      for (Oplog oplog : getOplogIdToOplog().values()) {
        if (compactableOplogs.size() >= max) {
          return;
        }
        if (oplog.needsCompaction()) {
          compactableOplogs.add(oplog);
        }
      }
    }
  }

  void scheduleForRecovery(DiskRecoveryStore diskRecoveryStore) {
    DiskRegionView diskRegionView = diskRecoveryStore.getDiskRegionView();
    if (diskRegionView.isRecreated() &&
        (diskRegionView.getMyPersistentID() != null
            || diskRegionView.getMyInitializingID() != null)) {
      // If a region does not have either id then don't pay the cost
      // of scanning the oplogs for recovered data.
      synchronized (pendingRecoveryMap) {
        pendingRecoveryMap.put(diskRegionView.getId(), diskRecoveryStore);
      }
    }
  }

  /**
   * Returns null if we are not currently recovering the DiskRegion with the given drId.
   */
  DiskRecoveryStore getCurrentlyRecovering(long drId) {
    return currentRecoveryMap.get(drId);
  }

  void initChild() {
    if (getChild() == null) {
      setFirstChild(getSortedOplogs(), true);
    }
  }

  public void offlineCompact() {
    if (getChild() != null) {
      // check active oplog and if it is empty delete it
      getChild().krfClose();
      if (getChild().isOplogEmpty()) {
        getChild().destroy();
      }
    }

    // remove any oplogs that only have a drf
    Collection<Oplog> oplogsToDestroy = new ArrayList<>();
    synchronized (getOplogIdToOplog()) {
      for (Oplog oplog : getOplogIdToOplog().values()) {
        if (oplog.isDrfOnly()) {
          oplogsToDestroy.add(oplog);
        }
      }
    }

    for (Oplog oplog : oplogsToDestroy) {
      oplog.destroy();
    }

    destroyOldestReadyToCompact();
  }

  public DiskStoreImpl getParent() {
    return parent;
  }

  void updateDiskRegion(AbstractDiskRegion diskRegion) {
    for (Oplog oplog : getAllOplogs()) {
      if (oplog != null) {
        oplog.updateDiskRegion(diskRegion);
      }
    }
  }

  void flushChild() {
    Oplog oplog = getChild();
    if (oplog != null) {
      oplog.flushAll();
    }
  }

  public String getPrefix() {
    return OplogType.BACKUP.getPrefix();
  }

  void crfCreate(long oplogId) {
    getParent().getDiskInitFile().crfCreate(oplogId);
  }

  void drfCreate(long oplogId) {
    getParent().getDiskInitFile().drfCreate(oplogId);
  }

  void crfDelete(long oplogId) {
    getParent().getDiskInitFile().crfDelete(oplogId);
  }

  void drfDelete(long oplogId) {
    getParent().getDiskInitFile().drfDelete(oplogId);
  }

  boolean couldHaveKrf() {
    return getParent().couldHaveKrf();
  }

  public boolean isCompactionPossible() {
    return getParent().isCompactionPossible();
  }

  /** oplogs that are ready to compact */
  Map<Long, Oplog> getOplogIdToOplog() {
    return oplogIdToOplog;
  }

  AtomicBoolean getAlreadyRecoveredOnce() {
    return alreadyRecoveredOnce;
  }

  @VisibleForTesting
  Map<Long, DiskRecoveryStore> getPendingRecoveryMap() {
    return pendingRecoveryMap;
  }
}
