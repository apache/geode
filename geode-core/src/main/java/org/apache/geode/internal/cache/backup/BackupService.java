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
import org.apache.geode.internal.cache.DiskStoreBackup;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThreadGroup;

public class BackupService {
  Logger logger = LogService.getLogger();

  public static final String DATA_STORES_TEMPORARY_DIRECTORY = "backupTemp_";
  private final ExecutorService executor;
  private final MembershipListener membershipListener = new BackupMembershipListener();
  private final InternalCache cache;

  private final AtomicReference<BackupTask> currentTask = new AtomicReference<>();
  private transient Future<HashSet<PersistentID>> taskFuture;

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

  public HashSet<PersistentID> startBackup(InternalDistributedMember sender)
      throws IOException, InterruptedException {
    validateRequestingAdmin(sender);
      if (!currentTask.compareAndSet(null, new BackupTask(cache))) {
        throw new IOException("Another backup already in progress");
      }
      taskFuture = executor.submit(() -> currentTask.get().backup());
    return getDiskStoreIdsToBackup();
  }

  private HashSet<PersistentID> getDiskStoreIdsToBackup() throws InterruptedException {
    BackupTask task = currentTask.get();
    return task == null ? new HashSet<>() : task.awaitLockAcquisition();
  }

  public HashSet<PersistentID> doBackup(File targetDir, File baselineDir, boolean abort)
      throws IOException {
    BackupTask task = currentTask.get();
    if (task == null) {
      if (abort) {
        return new HashSet<>();
      }
      throw new IOException("No backup currently in progress");
    }
    task.notifyOtherMembersReady(targetDir, baselineDir, abort);

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
      task.waitForBackup();
    }
  }

  public DiskStoreBackup getBackupForDiskStore(DiskStoreImpl diskStore) {
    BackupTask task = currentTask.get();
    return task == null ? null : task.getBackupForDiskStore(diskStore);
  }

  private void validateRequestingAdmin(InternalDistributedMember sender) {
    // We need to watch for pure admin guys that depart. this allMembershipListener set
    // looks like it should receive those events.
    Set allIds = getDistributionManager().addAllMembershipListenerAndGetAllIds(membershipListener);
    if (!allIds.contains(sender)) {
      cleanup();
      throw new IllegalStateException("The admin member requesting a backup has already departed");
    }
  }

  private void cleanup() {
    getDistributionManager().removeAllMembershipListener(membershipListener);
    currentTask.set(null);
  }

  private DistributionManager getDistributionManager() {
    return cache.getInternalDistributedSystem().getDistributionManager();
  }

  private class BackupMembershipListener implements MembershipListener {
    @Override
    public void memberDeparted(InternalDistributedMember id, boolean crashed) {
      cleanup();
    }

    @Override
    public void memberJoined(InternalDistributedMember id) {
      // unused
    }

    @Override
    public void quorumLost(Set<InternalDistributedMember> failures,
        List<InternalDistributedMember> remaining) {
      // unused
    }

    @Override
    public void memberSuspect(InternalDistributedMember id, InternalDistributedMember whoSuspected,
        String reason) {
      // unused
    }
  }
}
