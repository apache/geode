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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.Oplog;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThreadGroup;

public class BackupService {
  Logger logger = LogService.getLogger();

  public static final String DATA_STORES_TEMPORARY_DIRECTORY = "backupTemp_";
  private final ExecutorService executor;
  private final MembershipListener membershipListener = new BackupMembershipListener();
  private final InternalCache cache;

  private transient Future<HashSet<PersistentID>> taskFuture;

  final AtomicReference<BackupTask> currentTask = new AtomicReference<>();

  public BackupService(InternalCache cache) {
    this.cache = cache;
    executor = createExecutor();
  }

  private ExecutorService createExecutor() {
    LoggingThreadGroup group = LoggingThreadGroup.createThreadGroup("BackupService Thread", logger);
    ThreadFactory threadFactory = new ThreadFactory() {
      private final AtomicInteger threadId = new AtomicInteger();

      public Thread newThread(final Runnable command) {
        Thread thread =
            new Thread(group, command, "BackupServiceThread" + this.threadId.incrementAndGet());
        thread.setDaemon(true);
        return thread;
      }
    };
    return Executors.newSingleThreadExecutor(threadFactory);
  }

  public HashSet<PersistentID> prepareBackup(InternalDistributedMember sender, File targetDir,
      File baselineDir) throws IOException, InterruptedException {
    validateRequestingAdmin(sender);
    BackupTask backupTask = new BackupTask(cache, targetDir, baselineDir);
    if (!currentTask.compareAndSet(null, backupTask)) {
      throw new IOException("Another backup already in progress");
    }
    taskFuture = executor.submit(() -> backupTask.backup());
    return backupTask.getPreparedDiskStores();
  }

  public HashSet<PersistentID> doBackup() throws IOException {
    BackupTask task = currentTask.get();
    if (task == null) {
      throw new IOException("No backup currently in progress");
    }
    task.notifyOtherMembersReady();

    HashSet<PersistentID> result;
    try {
      result = taskFuture.get();
    } catch (InterruptedException | ExecutionException e) {
      result = new HashSet<>();
    } finally {
      cleanup();
    }
    return result;
  }

  public void waitForBackup() {
    BackupTask task = currentTask.get();
    if (task != null) {
      task.waitTillBackupFilesAreCopiedToTemporaryLocation();
    }
  }

  public DiskStoreBackup getBackupForDiskStore(DiskStoreImpl diskStore) {
    BackupTask task = currentTask.get();
    return task == null ? null : task.getBackupForDiskStore(diskStore);
  }

  void validateRequestingAdmin(InternalDistributedMember sender) {
    // We need to watch for pure admin guys that depart. this allMembershipListener set
    // looks like it should receive those events.
    Set allIds =
        cache.getDistributionManager().addAllMembershipListenerAndGetAllIds(membershipListener);
    if (!allIds.contains(sender)) {
      cleanup();
      throw new IllegalStateException("The admin member requesting a backup has already departed");
    }
  }

  public boolean deferDrfDelete(DiskStoreImpl diskStore, Oplog oplog) {
    DiskStoreBackup diskStoreBackup = getBackupForDiskStore(diskStore);
    if (diskStoreBackup != null) {
      return diskStoreBackup.deferDrfDelete(oplog);
    }
    return false;
  }

  public boolean deferCrfDelete(DiskStoreImpl diskStore, Oplog oplog) {
    DiskStoreBackup diskStoreBackup = getBackupForDiskStore(diskStore);
    if (diskStoreBackup != null) {
      return diskStoreBackup.deferCrfDelete(oplog);
    }
    return false;
  }

  void cleanup() {
    cache.getDistributionManager().removeAllMembershipListener(membershipListener);
    currentTask.set(null);
  }

  public void abortBackup() {
    BackupTask task = currentTask.get();
    cleanup();
    if (task != null) {
      task.abort();
    }
  }

  private class BackupMembershipListener implements MembershipListener {
    @Override
    public void memberDeparted(DistributionManager distributionManager,
        InternalDistributedMember id, boolean crashed) {
      cleanup();
    }

    @Override
    public void memberJoined(DistributionManager distributionManager,
        InternalDistributedMember id) {
      // unused
    }

    @Override
    public void quorumLost(DistributionManager distributionManager,
        Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {
      // unused
    }

    @Override
    public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
        InternalDistributedMember whoSuspected, String reason) {
      // unused
    }
  }
}
