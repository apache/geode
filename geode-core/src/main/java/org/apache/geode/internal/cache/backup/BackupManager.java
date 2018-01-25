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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DiskStoreBackup;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.InternalCache;

public class BackupManager {

  public static final String DATA_STORES_TEMPORARY_DIRECTORY = "backupTemp_";
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final MembershipListener membershipListener = new BackupMembershipListener();
  private final InternalCache cache;
  private final InternalDistributedMember sender;

  private BackupTask task;
  private Future<HashSet<PersistentID>> taskFuture;


  public BackupManager(InternalDistributedMember sender, InternalCache cache) {
    this.cache = cache;
    this.sender = sender;
  }

  public void startBackup() {
    task = new BackupTask(cache);
    taskFuture = executor.submit(task::backup);
  }

  public HashSet<PersistentID> getDiskStoreIdsToBackup() throws InterruptedException {
    return task.awaitLockAcquisition();
  }

  public HashSet<PersistentID> doBackup(File targetDir, File baselineDir, boolean abort) {
    task.notifyOtherMembersReady(targetDir, baselineDir, abort);

    HashSet<PersistentID> result;
    try {
      result = taskFuture.get();
    } catch (InterruptedException| ExecutionException e) {
      result = new HashSet<>();
    }
    return result;
  }

  public void waitForBackup() {
    task.waitForBackup();
  }

  public DiskStoreBackup getBackupForDiskStore(DiskStoreImpl diskStore) {
    return task.getBackupForDiskStore(diskStore);
  }
  public void validateRequestingAdmin() {
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
    cache.clearBackupManager();
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
