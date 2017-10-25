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

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.apache.geode.cache.DiskAccessException;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.entries.DiskEntry.Helper.ValueWrapper;
import org.apache.geode.internal.cache.DiskStoreImpl.OplogEntryIdSet;
import org.apache.geode.internal.cache.persistence.DiskRecoveryStore;
import org.apache.geode.internal.cache.persistence.DiskRegionView;
import org.apache.geode.internal.cache.persistence.DiskStoreFilter;
import org.apache.geode.internal.cache.persistence.OplogType;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.sequencelog.EntryLogger;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
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

public class PersistentOplogSet implements OplogSet {
  private static final Logger logger = LogService.getLogger();

  /** The active oplog * */
  protected volatile Oplog child;

  /** variable to generate sequential unique oplogEntryId's* */
  private final AtomicLong oplogEntryId = new AtomicLong(DiskStoreImpl.INVALID_ID);

  /** counter used for round-robin logic * */
  int dirCounter = -1;

  /**
   * Contains all the oplogs that only have a drf (i.e. the crf has been deleted).
   */
  final Map<Long, Oplog> drfOnlyOplogs = new LinkedHashMap<Long, Oplog>();

  /** oplogs that are ready to compact */
  final Map<Long, Oplog> oplogIdToOplog = new LinkedHashMap<Long, Oplog>();
  /** oplogs that are done being written to but not yet ready to compact */
  private final Map<Long, Oplog> inactiveOplogs = new LinkedHashMap<Long, Oplog>(16, 0.75f, true);

  private final DiskStoreImpl parent;

  final AtomicInteger inactiveOpenCount = new AtomicInteger();

  private final Map<Long, DiskRecoveryStore> pendingRecoveryMap =
      new HashMap<Long, DiskRecoveryStore>();
  private final Map<Long, DiskRecoveryStore> currentRecoveryMap =
      new HashMap<Long, DiskRecoveryStore>();

  final AtomicBoolean alreadyRecoveredOnce = new AtomicBoolean(false);

  /**
   * The maximum oplog id we saw while recovering
   */
  private volatile long maxRecoveredOplogId = 0;


  public PersistentOplogSet(DiskStoreImpl parent) {
    this.parent = parent;
  }

  /**
   * returns the active child
   */
  public Oplog getChild() {
    return this.child;
  }

  /**
   * set the child to a new oplog
   *
   */
  void setChild(Oplog oplog) {
    this.child = oplog;
    // oplogSetAdd(oplog);
  }

  public Oplog[] getAllOplogs() {
    synchronized (this.oplogIdToOplog) {
      int rollNum = this.oplogIdToOplog.size();
      int inactiveNum = this.inactiveOplogs.size();
      int drfOnlyNum = this.drfOnlyOplogs.size();
      int num = rollNum + inactiveNum + drfOnlyNum + 1;
      Oplog[] oplogs = new Oplog[num];
      oplogs[0] = getChild();
      {
        Iterator<Oplog> itr = this.oplogIdToOplog.values().iterator();
        for (int i = 1; i <= rollNum; i++) {
          oplogs[i] = itr.next();
        }
      }
      {
        Iterator<Oplog> itr = this.inactiveOplogs.values().iterator();
        for (int i = 1; i <= inactiveNum; i++) {
          oplogs[i + rollNum] = itr.next();
        }
      }
      {
        Iterator<Oplog> itr = this.drfOnlyOplogs.values().iterator();
        for (int i = 1; i <= drfOnlyNum; i++) {
          oplogs[i + rollNum + inactiveNum] = itr.next();
        }
      }

      // Special case - no oplogs found
      if (oplogs.length == 1 && oplogs[0] == null) {
        return new Oplog[0];
      }
      return oplogs;
    }

  }

  private TreeSet<Oplog> getSortedOplogs() {
    TreeSet<Oplog> result = new TreeSet<Oplog>(new Comparator() {
      public int compare(Object arg0, Object arg1) {
        return Long.signum(((Oplog) arg1).getOplogId() - ((Oplog) arg0).getOplogId());
      }
    });
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
  public Oplog getChild(long id) {
    Oplog localOplog = this.child;
    if (localOplog != null && id == localOplog.getOplogId()) {
      return localOplog;
    } else {
      Long key = Long.valueOf(id);
      synchronized (this.oplogIdToOplog) {
        Oplog result = oplogIdToOplog.get(key);
        if (result == null) {
          result = inactiveOplogs.get(key);
        }
        return result;
      }
    }
  }

  @Override
  public void create(LocalRegion region, DiskEntry entry, ValueWrapper value, boolean async) {
    getChild().create(region, entry, value, async);
  }

  @Override
  public void modify(LocalRegion region, DiskEntry entry, ValueWrapper value, boolean async) {
    getChild().modify(region, entry, value, async);
  }

  public void offlineModify(DiskRegionView drv, DiskEntry entry, byte[] value,
      boolean isSerializedObject) {
    getChild().offlineModify(drv, entry, value, isSerializedObject);
  }

  @Override
  public void remove(LocalRegion region, DiskEntry entry, boolean async, boolean isClear) {
    getChild().remove(region, entry, async, isClear);
  }

  public void forceRoll(DiskRegion dr) {
    Oplog child = getChild();
    if (child != null) {
      child.forceRolling(dr);
    }
  }

  public Map<File, DirectoryHolder> findFiles(String partialFileName) {
    this.dirCounter = 0;
    Map<File, DirectoryHolder> backupFiles = new HashMap<File, DirectoryHolder>();
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

  protected FilenameFilter getFileNameFilter(String partialFileName) {
    return new DiskStoreFilter(OplogType.BACKUP, false, partialFileName);
  }

  public void createOplogs(boolean needsOplogs, Map<File, DirectoryHolder> backupFiles) {
    LongOpenHashSet foundCrfs = new LongOpenHashSet();
    LongOpenHashSet foundDrfs = new LongOpenHashSet();


    for (Map.Entry<File, DirectoryHolder> entry : backupFiles.entrySet()) {
      File file = entry.getKey();
      String absolutePath = file.getAbsolutePath();
      int underscorePosition = absolutePath.lastIndexOf("_");
      int pointPosition = absolutePath.lastIndexOf(".");
      String opid = absolutePath.substring(underscorePosition + 1, pointPosition);
      long oplogId = Long.parseLong(opid);
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
          } catch (Exception ex) {// ignore
          }
          continue; // this file we unable to delete earlier
        }
      } else if (Oplog.isDRFFile(file.getName())) {
        if (!isDrfOplogIdPresent(oplogId)) {
          deleteFileOnRecovery(file);
          continue; // this file we unable to delete earlier
        }
      }

      Oplog oplog = getChild(oplogId);
      if (oplog == null) {
        oplog = new Oplog(oplogId, this);
        // oplogSet.add(oplog);
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

  protected boolean isDrfOplogIdPresent(long oplogId) {
    return parent.getDiskInitFile().isDRFOplogIdPresent(oplogId);
  }

  protected boolean isCrfOplogIdPresent(long oplogId) {
    return parent.getDiskInitFile().isCRFOplogIdPresent(oplogId);
  }

  protected void verifyOplogs(LongOpenHashSet foundCrfs, LongOpenHashSet foundDrfs) {
    parent.getDiskInitFile().verifyOplogs(foundCrfs, foundDrfs);
  }


  private void deleteFileOnRecovery(File f) {
    try {
      f.delete();
    } catch (Exception e) {
      // ignore, one more attempt to delete the file failed
    }
  }

  void addRecoveredOplog(Oplog oplog) {
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
      oplog.close(); // fix for bug 41687
      oplog.deleteFiles(oplog.getHasDeletes());
    } else {
      int inactivePromotedCount = 0;
      parent.getStats().incCompactableOplogs(1);
      Long key = Long.valueOf(oplog.getOplogId());
      synchronized (this.oplogIdToOplog) {
        if (this.inactiveOplogs.remove(key) != null) {
          if (oplog.isRAFOpen()) {
            inactiveOpenCount.decrementAndGet();
          }
          inactivePromotedCount++;
        }
        this.oplogIdToOplog.put(key, oplog);
      }
      if (inactivePromotedCount > 0) {
        parent.getStats().incInactiveOplogs(-inactivePromotedCount);
      }
    }
  }

  public void recoverRegionsThatAreReady() {
    // The following sync also prevents concurrent recoveries by multiple regions
    // which is needed currently.
    synchronized (this.alreadyRecoveredOnce) {
      // need to take a snapshot of DiskRecoveryStores we will recover
      synchronized (this.pendingRecoveryMap) {
        this.currentRecoveryMap.clear();
        this.currentRecoveryMap.putAll(this.pendingRecoveryMap);
        this.pendingRecoveryMap.clear();
      }
      if (this.currentRecoveryMap.isEmpty() && this.alreadyRecoveredOnce.get()) {
        // no recovery needed
        return;
      }

      for (DiskRecoveryStore drs : this.currentRecoveryMap.values()) {
        // Call prepare early to fix bug 41119.
        drs.getDiskRegionView().prepareForRecovery();
      }
      if (!this.alreadyRecoveredOnce.get()) {
        initOplogEntryId();
        // Fix for #43026 - make sure we don't reuse an entry
        // id that has been marked as cleared.
        updateOplogEntryId(parent.getDiskInitFile().getMaxRecoveredClearEntryId());
      }

      final long start = parent.getStats().startRecovery();
      long byteCount = 0;
      EntryLogger.setSource(parent.getDiskStoreID(), "recovery");
      try {
        byteCount = recoverOplogs(byteCount);

      } finally {
        Map<String, Integer> prSizes = null;
        Map<String, Integer> prBuckets = null;
        if (parent.isValidating()) {
          prSizes = new HashMap<String, Integer>();
          prBuckets = new HashMap<String, Integer>();
        }
        for (DiskRecoveryStore drs : this.currentRecoveryMap.values()) {
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
              ValidatingDiskRegion vdr = ((ValidatingDiskRegion) drs);
              if (logger.isTraceEnabled(LogMarker.PERSIST_RECOVERY)) {
                vdr.dump(System.out);
              }
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
                System.out.println(vdr.getName() + ": entryCount=" + vdr.size());
              }
            }
          }
        }
        if (parent.isValidating()) {
          for (Map.Entry<String, Integer> me : prSizes.entrySet()) {
            parent.incLiveEntryCount(me.getValue());
            System.out.println(me.getKey() + " entryCount=" + me.getValue() + " bucketCount="
                + prBuckets.get(me.getKey()));
          }
        }
        parent.getStats().endRecovery(start, byteCount);
        this.alreadyRecoveredOnce.set(true);
        this.currentRecoveryMap.clear();
        EntryLogger.clearSource();
      }
    }
  }

  private long recoverOplogs(long byteCount) {
    OplogEntryIdSet deletedIds = new OplogEntryIdSet();

    TreeSet<Oplog> oplogSet = getSortedOplogs();
    Set<Oplog> oplogsNeedingValueRecovery = new HashSet<Oplog>();
    if (!this.alreadyRecoveredOnce.get()) {
      if (getChild() != null && !getChild().hasBeenUsed()) {
        // Then remove the current child since it is empty
        // and does not need to be recovered from
        // and it is important to not call initAfterRecovery on it.
        oplogSet.remove(getChild());
      }
    }
    if (oplogSet.size() > 0) {
      long startOpLogRecovery = System.currentTimeMillis();
      // first figure out all entries that have been destroyed
      boolean latestOplog = true;
      for (Oplog oplog : oplogSet) {
        byteCount += oplog.recoverDrf(deletedIds, this.alreadyRecoveredOnce.get(), latestOplog);
        latestOplog = false;
        if (!this.alreadyRecoveredOnce.get()) {
          updateOplogEntryId(oplog.getMaxRecoveredOplogEntryId());
        }
      }
      parent.incDeadRecordCount(deletedIds.size());
      // now figure out live entries
      latestOplog = true;
      for (Oplog oplog : oplogSet) {
        long startOpLogRead = parent.getStats().startOplogRead();
        long bytesRead = oplog.recoverCrf(deletedIds,
            // @todo make recoverValues per region
            recoverValues(), recoverValuesSync(), this.alreadyRecoveredOnce.get(),
            oplogsNeedingValueRecovery, latestOplog);
        latestOplog = false;
        if (!this.alreadyRecoveredOnce.get()) {
          updateOplogEntryId(oplog.getMaxRecoveredOplogEntryId());
        }
        byteCount += bytesRead;
        parent.getStats().endOplogRead(startOpLogRead, bytesRead);

        // Callback to the disk regions to indicate the oplog is recovered
        // Used for offline export
        for (DiskRecoveryStore drs : this.currentRecoveryMap.values()) {
          drs.getDiskRegionView().oplogRecovered(oplog.oplogId);
        }
      }
      long endOpLogRecovery = System.currentTimeMillis();
      long elapsed = endOpLogRecovery - startOpLogRecovery;
      logger.info(LocalizedMessage.create(LocalizedStrings.DiskRegion_OPLOG_LOAD_TIME, elapsed));
    }
    if (!parent.isOfflineCompacting()) {
      long startRegionInit = System.currentTimeMillis();
      // create the oplogs now so that loadRegionData can have them available
      // Create an array of Oplogs so that we are able to add it in a single shot
      // to the map
      for (DiskRecoveryStore drs : this.currentRecoveryMap.values()) {
        drs.getDiskRegionView().initRecoveredEntryCount();
      }
      if (!this.alreadyRecoveredOnce.get()) {
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
          // TODO DAN - should we defer compaction until after
          // value recovery is complete? Or at least until after
          // value recovery for a given oplog is complete?
          // Right now, that's effectively what we're doing
          // because this uses up the compactor thread.
          parent.scheduleValueRecovery(oplogsNeedingValueRecovery, this.currentRecoveryMap);
        }
        if (!this.alreadyRecoveredOnce.get()) {
          // Create krfs for oplogs that are missing them
          for (Oplog oplog : oplogSet) {
            if (oplog.needsKrf()) {
              oplog.createKrfAsync();
            }
          }
          parent.scheduleCompaction();
        }

        long endRegionInit = System.currentTimeMillis();
        logger.info(LocalizedMessage.create(LocalizedStrings.DiskRegion_REGION_INIT_TIME,
            endRegionInit - startRegionInit));
      }
    }
    return byteCount;
  }

  protected boolean recoverValuesSync() {
    return parent.RECOVER_VALUES_SYNC;
  }

  protected boolean recoverValues() {
    return parent.RECOVER_VALUES;
  }

  private void setFirstChild(TreeSet<Oplog> oplogSet, boolean force) {
    if (parent.isOffline() && !parent.isOfflineCompacting() && !parent.isOfflineModify())
      return;
    if (!oplogSet.isEmpty()) {
      Oplog first = oplogSet.first();
      DirectoryHolder dh = first.getDirectoryHolder();
      dirCounter = dh.getArrayIndex();
      dirCounter = (++dirCounter) % parent.dirLength;
      // we want the first child to go in the directory after the directory
      // used by the existing oplog with the max id.
      // This fixes bug 41822.
    }
    if (force || maxRecoveredOplogId > 0) {
      setChild(new Oplog(maxRecoveredOplogId + 1, this, getNextDir()));
    }
  }

  private void initOplogEntryId() {
    this.oplogEntryId.set(DiskStoreImpl.INVALID_ID);
  }

  /**
   * Sets the last created oplogEntryId to the given value if and only if the given value is greater
   * than the current last created oplogEntryId
   */
  private void updateOplogEntryId(long v) {
    long curVal;
    do {
      curVal = this.oplogEntryId.get();
      if (curVal >= v) {
        // no need to set
        return;
      }
    } while (!this.oplogEntryId.compareAndSet(curVal, v));
  }

  /**
   * Returns the last created oplogEntryId. Returns INVALID_ID if no oplogEntryId has been created.
   */
  long getOplogEntryId() {
    parent.initializeIfNeeded();
    return this.oplogEntryId.get();
  }

  /**
   * Creates and returns a new oplogEntryId for the given key. An oplogEntryId is needed when
   * storing a key/value pair on disk. A new one is only needed if the key is new. Otherwise the
   * oplogEntryId already allocated for a key can be reused for the same key.
   * 
   * @return A disk id that can be used to access this key/value pair on disk
   */
  long newOplogEntryId() {
    long result = this.oplogEntryId.incrementAndGet();
    return result;
  }

  /**
   * Returns the next available DirectoryHolder which has space. If no dir has space then it will
   * return one anyway if compaction is enabled.
   * 
   * @param minAvailableSpace the minimum amount of space we need in this directory.
   */
  DirectoryHolder getNextDir(int minAvailableSpace, boolean checkForWarning) {
    DirectoryHolder dirHolder = null;
    DirectoryHolder selectedHolder = null;
    synchronized (parent.directories) {
      for (int i = 0; i < parent.dirLength; ++i) {
        dirHolder = parent.directories[this.dirCounter];
        // Asif :Increment the directory counter to next position so that next
        // time when this operation is invoked, it checks for the Directory
        // Space in a cyclical fashion.
        dirCounter = (++dirCounter) % parent.dirLength;
        // if the current directory has some space, then quit and
        // return the dir
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
          /*
           * try { this.isThreadWaitingForSpace = true; this.directories.wait(MAX_WAIT_FOR_SPACE); }
           * catch (InterruptedException ie) { throw new DiskAccessException(LocalizedStrings.
           * DiskRegion_UNABLE_TO_GET_FREE_SPACE_FOR_CREATING_AN_OPLOG_AS_THE_THREAD_ENCOUNETERD_EXCEPTION_WHILE_WAITING_FOR_COMPACTOR_THREAD_TO_FREE_SPACE
           * .toLocalizedString(), ie); }
           */

          selectedHolder = parent.directories[this.dirCounter];
          // Increment the directory counter to next position
          this.dirCounter = (++this.dirCounter) % parent.dirLength;
          if (selectedHolder.getAvailableSpace() < minAvailableSpace) {
            /*
             * throw new DiskAccessException(LocalizedStrings.
             * DiskRegion_UNABLE_TO_GET_FREE_SPACE_FOR_CREATING_AN_OPLOG_AFTER_WAITING_FOR_0_1_2_SECONDS
             * .toLocalizedString(new Object[] {MAX_WAIT_FOR_SPACE, /, (1000)}));
             */
            logger.warn(LocalizedMessage.create(
                LocalizedStrings.DiskRegion_COMPLEXDISKREGIONGETNEXTDIR_MAX_DIRECTORY_SIZE_WILL_GET_VIOLATED__GOING_AHEAD_WITH_THE_SWITCHING_OF_OPLOG_ANY_WAYS_CURRENTLY_AVAILABLE_SPACE_IN_THE_DIRECTORY_IS__0__THE_CAPACITY_OF_DIRECTORY_IS___1,
                new Object[] {Long.valueOf(selectedHolder.getUsedSpace()),
                    Long.valueOf(selectedHolder.getCapacity())}));
          }
        } else {
          throw new DiskAccessException(
              LocalizedStrings.DiskRegion_DISK_IS_FULL_COMPACTION_IS_DISABLED_NO_SPACE_CAN_BE_CREATED
                  .toLocalizedString(),
              parent);
        }
      }
    }
    return selectedHolder;

  }

  DirectoryHolder getNextDir() {
    return getNextDir(DiskStoreImpl.MINIMUM_DIR_SIZE, true);
  }

  void addDrf(Oplog oplog) {
    synchronized (this.oplogIdToOplog) {
      this.drfOnlyOplogs.put(Long.valueOf(oplog.getOplogId()), oplog);
    }
  }

  void removeDrf(Oplog oplog) {
    synchronized (this.oplogIdToOplog) {
      this.drfOnlyOplogs.remove(Long.valueOf(oplog.getOplogId()));
    }
  }

  /**
   * Return true if id is less than all the ids in the oplogIdToOplog map. Since the oldest one is
   * in the LINKED hash map is first we only need to compare ourselves to it.
   */
  boolean isOldestExistingOplog(long id) {
    synchronized (this.oplogIdToOplog) {
      Iterator<Long> it = this.oplogIdToOplog.keySet().iterator();
      while (it.hasNext()) {
        long otherId = it.next().longValue();
        if (id > otherId) {
          return false;
        }
      }
      // since the inactiveOplogs map is an LRU we need to check each one
      it = this.inactiveOplogs.keySet().iterator();
      while (it.hasNext()) {
        long otherId = it.next().longValue();
        if (id > otherId) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Destroy all the oplogs that are: 1. the oldest (based on smallest oplog id) 2. empty (have no
   * live values)
   */
  void destroyOldestReadyToCompact() {
    synchronized (this.oplogIdToOplog) {
      if (this.drfOnlyOplogs.isEmpty())
        return;
    }
    Oplog oldestLiveOplog = getOldestLiveOplog();
    ArrayList<Oplog> toDestroy = new ArrayList<Oplog>();
    if (oldestLiveOplog == null) {
      // remove all oplogs that are empty
      synchronized (this.oplogIdToOplog) {
        toDestroy.addAll(this.drfOnlyOplogs.values());
      }
    } else {
      // remove all empty oplogs that are older than the oldest live one
      synchronized (this.oplogIdToOplog) {
        for (Oplog oplog : this.drfOnlyOplogs.values()) {
          if (oplog.getOplogId() < oldestLiveOplog.getOplogId()) {
            toDestroy.add(oplog);
            // } else {
            // // since drfOnlyOplogs is sorted any other ones will be even
            // bigger
            // // so we can break out of this loop
            // break;
          }
        }
      }
    }
    for (Oplog oplog : toDestroy) {
      oplog.destroy();
    }
  }

  /**
   * Returns the oldest oplog that is ready to compact. Returns null if no oplogs are ready to
   * compact. Age is based on the oplog id.
   */
  private Oplog getOldestReadyToCompact() {
    Oplog oldest = null;
    synchronized (this.oplogIdToOplog) {
      Iterator<Oplog> it = this.oplogIdToOplog.values().iterator();
      while (it.hasNext()) {
        Oplog oldestCompactable = it.next();
        if (oldest == null || oldestCompactable.getOplogId() < oldest.getOplogId()) {
          oldest = oldestCompactable;
        }
      }
      it = this.drfOnlyOplogs.values().iterator();
      while (it.hasNext()) {
        Oplog oldestDrfOnly = it.next();
        if (oldest == null || oldestDrfOnly.getOplogId() < oldest.getOplogId()) {
          oldest = oldestDrfOnly;
        }
      }
    }
    return oldest;
  }

  private Oplog getOldestLiveOplog() {
    Oplog result = null;
    synchronized (this.oplogIdToOplog) {
      Iterator<Oplog> it = this.oplogIdToOplog.values().iterator();
      while (it.hasNext()) {
        Oplog n = it.next();
        if (result == null || n.getOplogId() < result.getOplogId()) {
          result = n;
        }
      }
      // since the inactiveOplogs map is an LRU we need to check each one
      it = this.inactiveOplogs.values().iterator();
      while (it.hasNext()) {
        Oplog n = it.next();
        if (result == null || n.getOplogId() < result.getOplogId()) {
          result = n;
        }
      }
    }
    return result;
  }

  void inactiveAccessed(Oplog oplog) {
    Long key = Long.valueOf(oplog.getOplogId());
    synchronized (this.oplogIdToOplog) {
      // update last access time
      this.inactiveOplogs.get(key);
    }
  }

  void inactiveReopened(Oplog oplog) {
    addInactive(oplog, true);
  }

  void addInactive(Oplog oplog) {
    addInactive(oplog, false);
  }

  private void addInactive(Oplog oplog, boolean reopen) {
    Long key = Long.valueOf(oplog.getOplogId());
    ArrayList<Oplog> openlist = null;
    synchronized (this.oplogIdToOplog) {
      boolean isInactive = true;
      if (reopen) {
        // It is possible that 'oplog' is compactable instead of inactive.
        // So set the isInactive.
        isInactive = this.inactiveOplogs.get(key) != null;
      } else {
        this.inactiveOplogs.put(key, oplog);
      }
      if ((reopen && isInactive) || oplog.isRAFOpen()) {
        if (inactiveOpenCount.incrementAndGet() > DiskStoreImpl.MAX_OPEN_INACTIVE_OPLOGS) {
          openlist = new ArrayList<Oplog>();
          for (Oplog o : this.inactiveOplogs.values()) {
            if (o.isRAFOpen()) {
              // add to my list
              openlist.add(o);
            }
          }
        }
      }
    }

    if (openlist != null) {
      for (Oplog o : openlist) {
        if (o.closeRAF()) {
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

  public void clear(DiskRegion dr, RegionVersionVector rvv) {
    // call clear on each oplog
    // to fix bug 44336 put them in another collection
    ArrayList<Oplog> oplogsToClear = new ArrayList<Oplog>();
    synchronized (this.oplogIdToOplog) {
      for (Oplog oplog : this.oplogIdToOplog.values()) {
        oplogsToClear.add(oplog);
      }
      for (Oplog oplog : this.inactiveOplogs.values()) {
        oplogsToClear.add(oplog);
      }
      {
        Oplog child = getChild();
        if (child != null) {
          oplogsToClear.add(child);
        }
      }
    }
    for (Oplog oplog : oplogsToClear) {
      oplog.clear(dr, rvv);
    }

    if (rvv != null) {
      parent.getDiskInitFile().clearRegion(dr, rvv);
    } else {
      long clearedOplogEntryId = getOplogEntryId();
      parent.getDiskInitFile().clearRegion(dr, clearedOplogEntryId);
    }
  }

  public RuntimeException close() {
    RuntimeException rte = null;
    try {
      closeOtherOplogs();
    } catch (RuntimeException e) {
      if (rte != null) {
        rte = e;
      }
    }

    if (this.child != null) {
      try {
        this.child.finishKrf();
      } catch (RuntimeException e) {
        if (rte != null) {
          rte = e;
        }
      }

      try {
        this.child.close();
      } catch (RuntimeException e) {
        if (rte != null) {
          rte = e;
        }
      }
    }

    return rte;
  }

  /** closes all the oplogs except the current one * */
  private void closeOtherOplogs() {
    // get a snapshot to prevent CME
    Oplog[] oplogs = getAllOplogs();
    // if there are oplogs which are to be compacted, destroy them
    // do not do oplogs[0]
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
    Oplog oplog = null;
    boolean drfOnly = false;
    boolean inactive = false;
    Long key = Long.valueOf(id);
    synchronized (this.oplogIdToOplog) {
      oplog = this.oplogIdToOplog.remove(key);
      if (oplog == null) {
        oplog = this.inactiveOplogs.remove(key);
        if (oplog != null) {
          if (oplog.isRAFOpen()) {
            inactiveOpenCount.decrementAndGet();
          }
          inactive = true;
        } else {
          oplog = this.drfOnlyOplogs.remove(key);
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
    ArrayList<Oplog> oplogsToClose = new ArrayList<Oplog>();
    synchronized (this.oplogIdToOplog) {
      oplogsToClose.addAll(this.oplogIdToOplog.values());
      oplogsToClose.addAll(this.inactiveOplogs.values());
      oplogsToClose.addAll(this.drfOnlyOplogs.values());
      {
        Oplog child = getChild();
        if (child != null) {
          oplogsToClose.add(child);
        }
      }
    }
    for (Oplog oplog : oplogsToClose) {
      oplog.close(dr);
    }
  }

  public void prepareForClose() {
    ArrayList<Oplog> oplogsToPrepare = new ArrayList<Oplog>();
    synchronized (this.oplogIdToOplog) {
      oplogsToPrepare.addAll(this.oplogIdToOplog.values());
      oplogsToPrepare.addAll(this.inactiveOplogs.values());
    }
    boolean childPreparedForClose = false;
    long child_oplogid = this.getChild() == null ? -1 : this.getChild().oplogId;
    for (Oplog oplog : oplogsToPrepare) {
      oplog.prepareForClose();
      if (child_oplogid != -1 && oplog.oplogId == child_oplogid) {
        childPreparedForClose = true;
      }
    }
    if (!childPreparedForClose && this.getChild() != null) {
      this.getChild().prepareForClose();
    }
  }

  public void basicDestroy(DiskRegion dr) {
    ArrayList<Oplog> oplogsToDestroy = new ArrayList<Oplog>();
    synchronized (this.oplogIdToOplog) {
      for (Oplog oplog : this.oplogIdToOplog.values()) {
        oplogsToDestroy.add(oplog);
      }
      for (Oplog oplog : this.inactiveOplogs.values()) {
        oplogsToDestroy.add(oplog);
      }
      for (Oplog oplog : this.drfOnlyOplogs.values()) {
        oplogsToDestroy.add(oplog);
      }
      {
        Oplog child = getChild();
        if (child != null) {
          oplogsToDestroy.add(child);
        }
      }
    }
    for (Oplog oplog : oplogsToDestroy) {
      oplog.destroy(dr);
    }
  }

  public void destroyAllOplogs() {
    // get a snapshot to prevent CME
    for (Oplog oplog : getAllOplogs()) {
      if (oplog != null) {
        oplog.destroy();
        removeOplog(oplog.getOplogId());
      }
    }
  }

  /**
   * Add compactable oplogs to the list, up to the maximum size.
   * 
   * @param l
   * @param max
   */
  public void getCompactableOplogs(List<CompactableOplog> l, int max) {
    synchronized (this.oplogIdToOplog) {
      // Sort this list so we compact the oldest first instead of the one
      // that was
      // compactable first.
      // ArrayList<CompactableOplog> l = new
      // ArrayList<CompactableOplog>(this.oplogIdToOplog.values());
      // Collections.sort(l);
      // Iterator<Oplog> itr = l.iterator();
      {
        Iterator<Oplog> itr = this.oplogIdToOplog.values().iterator();
        while (itr.hasNext() && l.size() < max) {
          Oplog oplog = itr.next();
          if (oplog.needsCompaction()) {
            l.add(oplog);
          }
        }
      }
    }
  }

  public void scheduleForRecovery(DiskRecoveryStore drs) {
    DiskRegionView dr = drs.getDiskRegionView();
    if (dr.isRecreated() && (dr.getMyPersistentID() != null || dr.getMyInitializingID() != null)) {
      // If a region does not have either id then don't pay the cost
      // of scanning the oplogs for recovered data.
      DiskRecoveryStore p_drs = drs;
      synchronized (this.pendingRecoveryMap) {
        this.pendingRecoveryMap.put(dr.getId(), p_drs);
      }
    }
  }

  /**
   * Returns null if we are not currently recovering the DiskRegion with the given drId.
   */
  public DiskRecoveryStore getCurrentlyRecovering(long drId) {
    return this.currentRecoveryMap.get(drId);
  }

  public void initChild() {
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

    { // remove any oplogs that only have a drf to fix bug 42036
      ArrayList<Oplog> toDestroy = new ArrayList<Oplog>();
      synchronized (this.oplogIdToOplog) {
        Iterator<Oplog> it = this.oplogIdToOplog.values().iterator();
        while (it.hasNext()) {
          Oplog n = it.next();
          if (n.isDrfOnly()) {
            toDestroy.add(n);
          }
        }
      }
      for (Oplog oplog : toDestroy) {
        oplog.destroy();
      }
      destroyOldestReadyToCompact();
    }
  }

  public DiskStoreImpl getParent() {
    return parent;
  }

  public void updateDiskRegion(AbstractDiskRegion dr) {
    for (Oplog oplog : getAllOplogs()) {
      if (oplog != null) {
        oplog.updateDiskRegion(dr);
      }
    }
  }

  public void flushChild() {
    Oplog oplog = getChild();
    if (oplog != null) {
      oplog.flushAll();
    }
  }

  public String getPrefix() {
    return OplogType.BACKUP.getPrefix();
  }

  public void crfCreate(long oplogId) {
    getParent().getDiskInitFile().crfCreate(oplogId);
  }

  public void drfCreate(long oplogId) {
    getParent().getDiskInitFile().drfCreate(oplogId);
  }

  public void crfDelete(long oplogId) {
    getParent().getDiskInitFile().crfDelete(oplogId);
  }

  public void drfDelete(long oplogId) {
    getParent().getDiskInitFile().drfDelete(oplogId);
  }

  public boolean couldHaveKrf() {
    return getParent().couldHaveKrf();
  }

  public boolean isCompactionPossible() {
    return getParent().isCompactionPossible();
  }
}


