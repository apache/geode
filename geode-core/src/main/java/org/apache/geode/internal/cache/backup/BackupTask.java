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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.classloader.internal.ClassPathLoader;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.Oplog;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This class manages the state an logic to backup a single cache.
 */
class BackupTask {
  private static final Logger logger = LogService.getLogger();

  private final Map<DiskStoreImpl, DiskStoreBackup> backupByDiskStore = new HashMap<>();
  private final RestoreScript restoreScript = new RestoreScript();
  private final InternalCache cache;
  private final CountDownLatch allowDestroys = new CountDownLatch(1);
  private final CountDownLatch locksAcquired = new CountDownLatch(1);
  private final CountDownLatch otherMembersReady = new CountDownLatch(1);
  private final HashSet<PersistentID> diskStoresWithData = new HashSet<>();
  private final BackupWriter backupWriter;

  private volatile boolean isCancelled;

  private TemporaryBackupFiles temporaryFiles;
  private BackupFileCopier fileCopier;

  BackupTask(InternalCache cache, BackupWriter backupWriter) {
    this.cache = cache;
    this.backupWriter = backupWriter;
  }

  HashSet<PersistentID> getPreparedDiskStores() throws InterruptedException {
    locksAcquired.await();
    return diskStoresWithData;
  }

  void notifyOtherMembersReady() {
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

    return doBackup();
  }

  private void prepareForBackup() {
    for (DiskStore store : cache.listDiskStoresIncludingRegionOwned()) {
      DiskStoreImpl storeImpl = (DiskStoreImpl) store;

      storeImpl.lockStoreBeforeBackup();
      if (logger.isDebugEnabled()) {
        logger.debug("Acquired lock for backup on disk store {}", store.getName());
      }
      if (storeImpl.hasPersistedData()) {
        diskStoresWithData.add(storeImpl.getPersistentID());
        storeImpl.getStats().startBackup();
      }
    }
  }

  private HashSet<PersistentID> doBackup() throws IOException {
    if (isCancelled) {
      cleanup();
      return new HashSet<>();
    }

    try {
      Collection<DiskStore> diskStores = cache.listDiskStoresIncludingRegionOwned();
      temporaryFiles = TemporaryBackupFiles.create();
      fileCopier = new BackupFileCopier(cache,
          ClassPathLoader.getLatest().getJarDeploymentService(), temporaryFiles);

      Map<DiskStoreImpl, DiskStoreBackup> backupByDiskStores = startDiskStoreBackups(diskStores);

      allowDestroys.countDown();

      HashSet<PersistentID> persistentIds = finishDiskStoreBackups(backupByDiskStores);

      if (!backupByDiskStores.isEmpty()) {
        backupAdditionalFiles();
        BackupDefinition backupDefinition = fileCopier.getBackupDefinition();
        backupDefinition.setRestoreScript(restoreScript);
        backupWriter.backupFiles(backupDefinition);
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

  private Map<DiskStoreImpl, DiskStoreBackup> startDiskStoreBackups(
      Collection<DiskStore> diskStores) throws IOException {
    Map<DiskStoreImpl, DiskStoreBackup> backupByDiskStore = new HashMap<>();

    for (DiskStore store : diskStores) {
      DiskStoreImpl diskStore = (DiskStoreImpl) store;
      try {
        if (diskStore.hasPersistedData()) {
          DiskStoreBackup backup = startDiskStoreBackup(diskStore);
          backupByDiskStore.put(diskStore, backup);
        }
      } finally {
        diskStore.releaseBackupLock();
        if (logger.isDebugEnabled()) {
          logger.debug("Released lock for backup on disk store {}", store.getName());
        }
      }
    }
    return backupByDiskStore;
  }

  void abort() {
    isCancelled = true;
    otherMembersReady.countDown();
  }

  boolean isCancelled() {
    return isCancelled;
  }

  void waitTillBackupFilesAreCopiedToTemporaryLocation() {
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

  private void backupAdditionalFiles() throws IOException {
    fileCopier.copyConfigFiles();
    fileCopier.copyUserFiles();
    fileCopier.copyDeployedJars();
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
   * Start the backup process. This is the second step of the backup process. In this method, we
   * define the data we're backing up by copying the init file and rolling to the next file. After
   * this method returns operations can proceed as normal, except that we don't remove oplogs.
   */
  private DiskStoreBackup startDiskStoreBackup(DiskStoreImpl diskStore) throws IOException {
    DiskStoreBackup backup = null;
    boolean done = false;
    try {
      for (;;) {
        Oplog childOplog = diskStore.getPersistentOplogSet().getChild();
        if (childOplog == null) {
          backup = new DiskStoreBackup(new Oplog[0]);
          backupByDiskStore.put(diskStore, backup);
          break;
        }

        // Get an appropriate lock object for each set of oplogs.
        Object childLock = childOplog.getLock();

        // TODO: We really should move this lock into the disk store, but until then we need to do
        // this magic to make sure we're actually locking the latest child for both types of oplogs

        // This ensures that all writing to disk is blocked while we are creating the snapshot
        synchronized (childLock) {
          if (logger.isDebugEnabled()) {
            logger.debug("Synchronized on lock for oplog {} on disk store {}",
                childOplog.getOplogId(), diskStore.getName());
          }

          if (diskStore.getPersistentOplogSet().getChild() != childOplog) {
            continue;
          }

          if (logger.isDebugEnabled()) {
            logger.debug("Creating snapshot of oplogs for disk store {}", diskStore.getName());
          }

          restoreScript.addExistenceTest(diskStore.getDiskInitFile().getIFFile());

          // Contains all oplogs that will backed up
          Oplog[] allOplogs = diskStore.getAllOplogsForBackup();
          backup = new DiskStoreBackup(allOplogs);
          backupByDiskStore.put(diskStore, backup);

          fileCopier.copyDiskInitFile(diskStore);
          diskStore.getPersistentOplogSet().forceRoll(null);

          if (logger.isDebugEnabled()) {
            logger.debug("Finished backup of disk store {}", diskStore.getName());
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

  DiskStoreBackup getBackupForDiskStore(DiskStoreImpl diskStore) {
    return backupByDiskStore.get(diskStore);
  }
}
