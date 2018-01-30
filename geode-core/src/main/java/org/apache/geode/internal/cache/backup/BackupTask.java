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
package org.apache.geode.internal.cache.backup;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DirectoryHolder;
import org.apache.geode.internal.cache.DiskStoreBackup;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.Oplog;
import org.apache.geode.internal.logging.LogService;

/**
 * This class manages the state an logic to backup a single cache.
 */
public class BackupTask {
  private static final Logger logger = LogService.getLogger();

  static final String INCOMPLETE_BACKUP_FILE = "INCOMPLETE_BACKUP_FILE";

  private static final String BACKUP_DIR_PREFIX = "dir";
  private static final String DATA_STORES_DIRECTORY = "diskstores";
  private static final String USER_FILES = "user";

  private final Map<DiskStoreImpl, DiskStoreBackup> backupByDiskStore = new HashMap<>();
  private final RestoreScript restoreScript = new RestoreScript();
  private final InternalCache cache;
  private final CountDownLatch allowDestroys = new CountDownLatch(1);
  private final String memberId;
  private final CountDownLatch locksAcquired = new CountDownLatch(1);
  private final CountDownLatch otherMembersReady = new CountDownLatch(1);
  private final HashSet<PersistentID> diskStoresWithData = new HashSet<>();

  private volatile File targetDir;
  private volatile File baselineDir;
  private volatile boolean isCancelled = false;

  private TemporaryBackupFiles temporaryFiles;
  private BackupFileCopier fileCopier;

  BackupTask(InternalCache gemFireCache) {
    this.cache = gemFireCache;
    memberId = getCleanedMemberId();
  }

  HashSet<PersistentID> awaitLockAcquisition() throws InterruptedException {
    locksAcquired.await();
    return diskStoresWithData;
  }

  void notifyOtherMembersReady(File targetDir, File baselineDir, boolean abort) {
    this.targetDir = targetDir;
    this.baselineDir = baselineDir;
    this.isCancelled = abort;
    otherMembersReady.countDown();
  }

  HashSet<PersistentID> backup() throws InterruptedException, IOException {
    prepareForBackup();
    locksAcquired.countDown();
    try {
      otherMembersReady.await();
    } catch (InterruptedException e) {
      cleanup();
      throw e;
    }
    if (isCancelled) {
      cleanup();
      return new HashSet<>();
    }

    return doBackup(targetDir, baselineDir);
  }

  private void prepareForBackup() {
    for (DiskStore store : cache.listDiskStoresIncludingRegionOwned()) {
      DiskStoreImpl storeImpl = (DiskStoreImpl) store;

      storeImpl.lockStoreBeforeBackup();
      if (storeImpl.hasPersistedData()) {
        diskStoresWithData.add(storeImpl.getPersistentID());
        storeImpl.getStats().startBackup();
      }
    }
  }

  private HashSet<PersistentID> doBackup(File targetDir, File baselineDir) throws IOException {
    if (isCancelled) {
      cleanup();
      return new HashSet<>();
    }

    try {
      temporaryFiles = TemporaryBackupFiles.create();
      fileCopier = new BackupFileCopier(cache, temporaryFiles);
      File memberBackupDir = new File(targetDir, memberId);

      // Make sure our baseline is okay for this member, then create inspector for baseline backup
      baselineDir = checkBaseline(baselineDir);
      BackupInspector inspector =
          (baselineDir == null ? null : BackupInspector.createInspector(baselineDir));
      File storesDir = new File(memberBackupDir, DATA_STORES_DIRECTORY);
      Collection<DiskStore> diskStores = cache.listDiskStoresIncludingRegionOwned();

      Map<DiskStoreImpl, DiskStoreBackup> backupByDiskStores =
          startDiskStoreBackups(inspector, storesDir, diskStores);
      allowDestroys.countDown();
      HashSet<PersistentID> persistentIds = finishDiskStoreBackups(backupByDiskStores);

      if (!backupByDiskStores.isEmpty()) {
        // TODO: allow different strategies...
        BackupDefinition backupDefinition = fileCopier.getBackupDefinition();
        backupAdditionalFiles(memberBackupDir);
        backupDefinition.setRestoreScript(restoreScript);
        BackupDestination backupDestination =
            new FileSystemBackupDestination(memberBackupDir.toPath());
        backupDestination.backupFiles(backupDefinition);
      }

      return persistentIds;
    } finally {
      cleanup();
    }
  }

  private HashSet<PersistentID> finishDiskStoreBackups(
      Map<DiskStoreImpl, DiskStoreBackup> backupByDiskStores) throws IOException {
    HashSet<PersistentID> persistentIds = new HashSet<>();
    for (Map.Entry<DiskStoreImpl, DiskStoreBackup> entry : backupByDiskStores.entrySet()) {
      DiskStoreImpl diskStore = entry.getKey();
      completeBackup(diskStore, entry.getValue());
      diskStore.getStats().endBackup();
      persistentIds.add(diskStore.getPersistentID());
    }
    return persistentIds;
  }

  private Map<DiskStoreImpl, DiskStoreBackup> startDiskStoreBackups(BackupInspector inspector,
      File storesDir, Collection<DiskStore> diskStores) throws IOException {
    Map<DiskStoreImpl, DiskStoreBackup> backupByDiskStore = new HashMap<>();

    for (DiskStore store : diskStores) {
      DiskStoreImpl diskStore = (DiskStoreImpl) store;
      try {
        if (diskStore.hasPersistedData()) {
          File diskStoreDir = new File(storesDir, getBackupDirName(diskStore));
          DiskStoreBackup backup = startDiskStoreBackup(diskStore, diskStoreDir, inspector);
          backupByDiskStore.put(diskStore, backup);
        }
      } finally {
        diskStore.releaseBackupLock();
      }
    }
    return backupByDiskStore;
  }

  void abort() {
    cleanup();
  }

  boolean isCancelled() {
    return isCancelled;
  }

  void waitForBackup() {
    try {
      allowDestroys.await();
    } catch (InterruptedException e) {
      throw new InternalGemFireError(e);
    }
  }

  private void cleanup() {
    isCancelled = true;
    allowDestroys.countDown();
    if (temporaryFiles != null) {
      temporaryFiles.cleanupFiles();
    }
    releaseBackupLocks();
  }

  private void releaseBackupLocks() {
    for (DiskStore store : cache.listDiskStoresIncludingRegionOwned()) {
      ((DiskStoreImpl) store).releaseBackupLock();
    }
  }

  /**
   * Returns the memberId directory for this member in the baseline. The memberId may have changed
   * if this member has been restarted since the last backup.
   *
   * @param baselineParentDir parent directory of last backup.
   * @return null if the baseline for this member could not be located.
   */
  private File findBaselineForThisMember(File baselineParentDir) {
    File baselineDir = null;

    // Find the first matching DiskStoreId directory for this member.
    for (DiskStore diskStore : cache.listDiskStoresIncludingRegionOwned()) {
      File[] matchingFiles = baselineParentDir
          .listFiles((file, name) -> name.endsWith(getBackupDirName((DiskStoreImpl) diskStore)));
      // We found it? Good. Set this member's baseline to the backed up disk store's member dir (two
      // levels up).
      if (null != matchingFiles && matchingFiles.length > 0)
        baselineDir = matchingFiles[0].getParentFile().getParentFile();
    }
    return baselineDir;
  }

  /**
   * Performs a sanity check on the baseline directory for incremental backups. If a baseline
   * directory exists for the member and there is no INCOMPLETE_BACKUP_FILE file then return the
   * data stores directory for this member.
   *
   * @param baselineParentDir a previous backup directory. This is used with the incremental backup
   *        option. May be null if the user specified a full backup.
   * @return null if the backup is to be a full backup otherwise return the data store directory in
   *         the previous backup for this member (if incremental).
   */
  private File checkBaseline(File baselineParentDir) {
    File baselineDir = null;

    if (null != baselineParentDir) {
      // Start by looking for this memberId
      baselineDir = new File(baselineParentDir, memberId);

      if (!baselineDir.exists()) {
        // hmmm, did this member have a restart?
        // Determine which member dir might be a match for us
        baselineDir = findBaselineForThisMember(baselineParentDir);
      }

      if (null != baselineDir) {
        // check for existence of INCOMPLETE_BACKUP_FILE file
        File incompleteBackup = new File(baselineDir, INCOMPLETE_BACKUP_FILE);
        if (incompleteBackup.exists()) {
          baselineDir = null;
        }
      }
    }

    return baselineDir;
  }

  private void backupAdditionalFiles(File backupDir) throws IOException {
    fileCopier.copyConfigFiles();

    Set<File> userFiles = fileCopier.copyUserFiles();
    File userBackupDir = new File(backupDir, USER_FILES);
    for (File file : userFiles) {
      File restoreScriptDestination = new File(userBackupDir, file.getName());
      restoreScript.addUserFile(file, restoreScriptDestination);
    }

    Set<File> jars = fileCopier.copyDeployedJars();
    for (File file : jars) {
      File restoreScriptDestination = new File(userBackupDir, file.getName());
      restoreScript.addFile(file, restoreScriptDestination);
    }
  }

  /**
   * Copy the oplogs to the backup directory. This is the final step of the backup process. The
   * oplogs we copy are defined in the startDiskStoreBackup method.
   */
  private void completeBackup(DiskStoreImpl diskStore, DiskStoreBackup backup) throws IOException {
    if (backup == null) {
      return;
    }
    try {
      // Wait for oplogs to be unpreblown before backing them up.
      diskStore.waitForDelayedWrites();

      // Backup all of the oplogs
      for (Oplog oplog : backup.getPendingBackup()) {
        if (isCancelled()) {
          break;
        }
        oplog.finishKrf();
        fileCopier.copyOplog(diskStore, oplog);

        // Allow the oplog to be deleted, and process any pending delete
        backup.backupFinished(oplog);
      }
    } finally {
      backup.cleanup();
    }
  }

  /**
   * Returns the dir name used to back up this DiskStore's directories under. The name is a
   * concatenation of the disk store name and id.
   */
  private String getBackupDirName(DiskStoreImpl diskStore) {
    String name = diskStore.getName();

    if (name == null) {
      name = GemFireCacheImpl.getDefaultDiskStoreName();
    }

    return (name + "_" + diskStore.getDiskStoreID().toString());
  }

  /**
   * Start the backup process. This is the second step of the backup process. In this method, we
   * define the data we're backing up by copying the init file and rolling to the next file. After
   * this method returns operations can proceed as normal, except that we don't remove oplogs.
   */
  private DiskStoreBackup startDiskStoreBackup(DiskStoreImpl diskStore, File targetDir,
      BackupInspector baselineInspector) throws IOException {
    DiskStoreBackup backup = null;
    boolean done = false;
    try {
      for (;;) {
        Oplog childOplog = diskStore.getPersistentOplogSet().getChild();
        if (childOplog == null) {
          backup = new DiskStoreBackup(new Oplog[0], targetDir);
          backupByDiskStore.put(diskStore, backup);
          break;
        }

        // Get an appropriate lock object for each set of oplogs.
        Object childLock = childOplog.getLock();

        // TODO - We really should move this lock into the disk store, but
        // until then we need to do this magic to make sure we're actually
        // locking the latest child for both types of oplogs

        // This ensures that all writing to disk is blocked while we are
        // creating the snapshot
        synchronized (childLock) {
          if (diskStore.getPersistentOplogSet().getChild() != childOplog) {
            continue;
          }

          if (logger.isDebugEnabled()) {
            logger.debug("snapshotting oplogs for disk store {}", diskStore.getName());
          }

          addDiskStoreDirectoriesToRestoreScript(diskStore, targetDir);

          restoreScript.addExistenceTest(diskStore.getDiskInitFile().getIFFile());

          // Contains all oplogs that will backed up

          // Incremental backup so filter out oplogs that have already been
          // backed up
          Oplog[] allOplogs;
          if (null != baselineInspector) {
            allOplogs = filterBaselineOplogs(diskStore, baselineInspector);
          } else {
            allOplogs = diskStore.getAllOplogsForBackup();
          }

          // mark all oplogs as being backed up. This will
          // prevent the oplogs from being deleted
          backup = new DiskStoreBackup(allOplogs, targetDir);
          backupByDiskStore.put(diskStore, backup);

          fileCopier.copyDiskInitFile(diskStore);
          diskStore.getPersistentOplogSet().forceRoll(null);

          if (logger.isDebugEnabled()) {
            logger.debug("done backing up disk store {}", diskStore.getName());
          }
          break;
        }
      }
      done = true;
    } finally {
      if (!done && backup != null) {
        backupByDiskStore.remove(diskStore);
        backup.cleanup();
      }
    }
    return backup;
  }

  private void addDiskStoreDirectoriesToRestoreScript(DiskStoreImpl diskStore, File targetDir) {
    DirectoryHolder[] directories = diskStore.getDirectoryHolders();
    for (int i = 0; i < directories.length; i++) {
      File backupDir = getBackupDirForCurrentMember(targetDir, i);
      restoreScript.addFile(directories[i].getDir(), backupDir);
    }
  }

  /**
   * Filters and returns the current set of oplogs that aren't already in the baseline for
   * incremental backup
   *
   * @param baselineInspector the inspector for the previous backup.
   * @return an array of Oplogs to be copied for an incremental backup.
   */
  private Oplog[] filterBaselineOplogs(DiskStoreImpl diskStore, BackupInspector baselineInspector) {
    File baselineDir = new File(baselineInspector.getBackupDir(), DATA_STORES_DIRECTORY);
    baselineDir = new File(baselineDir, getBackupDirName(diskStore));

    // Find all of the member's diskstore oplogs in the member's baseline
    // diskstore directory structure (*.crf,*.krf,*.drf)
    Collection<File> baselineOplogFiles =
        FileUtils.listFiles(baselineDir, new String[] {"krf", "drf", "crf"}, true);
    // Our list of oplogs to copy (those not already in the baseline)
    List<Oplog> oplogList = new LinkedList<>();

    // Total list of member oplogs
    Oplog[] allOplogs = diskStore.getAllOplogsForBackup();

    // Loop through operation logs and see if they are already part of the baseline backup.
    for (Oplog log : allOplogs) {
      // See if they are backed up in the current baseline
      Map<File, File> oplogMap = log.mapBaseline(baselineOplogFiles);

      // No? Then see if they were backed up in previous baselines
      if (oplogMap.isEmpty() && baselineInspector.isIncremental()) {
        oplogMap = addBaselineOplogToRestoreScript(baselineInspector, log);
      }

      if (oplogMap.isEmpty()) {
        // These are fresh operation log files so lets back them up.
        oplogList.add(log);
      } else {
        /*
         * These have been backed up before so lets just add their entries from the previous backup
         * or restore script into the current one.
         */
        restoreScript.addBaselineFiles(oplogMap);
      }
    }

    // Convert the filtered oplog list to an array
    return oplogList.toArray(new Oplog[oplogList.size()]);
  }

  private Map<File, File> addBaselineOplogToRestoreScript(BackupInspector baselineInspector,
      Oplog log) {
    Map<File, File> oplogMap = new HashMap<>();
    Set<String> matchingOplogs =
        log.gatherMatchingOplogFiles(baselineInspector.getIncrementalOplogFileNames());
    for (String matchingOplog : matchingOplogs) {
      oplogMap.put(new File(baselineInspector.getCopyFromForOplogFile(matchingOplog)),
          new File(baselineInspector.getCopyToForOplogFile(matchingOplog)));
    }
    return oplogMap;
  }

  private File getBackupDirForCurrentMember(File targetDir, int index) {
    return new File(targetDir, BACKUP_DIR_PREFIX + index);
  }

  private String getCleanedMemberId() {
    InternalDistributedMember memberId =
        cache.getInternalDistributedSystem().getDistributedMember();
    String vmId = memberId.toString();
    return cleanSpecialCharacters(vmId);
  }

  private String cleanSpecialCharacters(String string) {
    return string.replaceAll("[^\\w]+", "_");
  }

  DiskStoreBackup getBackupForDiskStore(DiskStoreImpl diskStore) {
    return backupByDiskStore.get(diskStore);
  }
}
