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

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.admin.internal.AdminDistributedSystemImpl;
import org.apache.geode.annotations.TestingOnly;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.management.ManagementException;
import org.apache.geode.management.internal.BackupStatusImpl;

/**
 * Performs backup of all members.
 */
public class BackupOperation {

  private final FlushToDiskFactory flushToDiskFactory;
  private final PrepareBackupFactory prepareBackupFactory;
  private final AbortBackupFactory abortBackupFactory;
  private final FinishBackupFactory finishBackupFactory;
  private final DistributionManager dm;
  private final InternalCache cache;
  private final BackupLockService backupLockService;
  private final MissingPersistentMembersProvider missingPersistentMembersProvider;

  public BackupOperation(DistributionManager dm, InternalCache cache) {
    this(new FlushToDiskFactory(), new PrepareBackupFactory(), new AbortBackupFactory(),
        new FinishBackupFactory(), dm, cache, new BackupLockService(),
        new DefaultMissingPersistentMembersProvider());
  }

  @TestingOnly
  BackupOperation(FlushToDiskFactory flushToDiskFactory, PrepareBackupFactory prepareBackupFactory,
      AbortBackupFactory abortBackupFactory, FinishBackupFactory finishBackupFactory,
      DistributionManager dm, InternalCache cache, BackupLockService backupLockService,
      MissingPersistentMembersProvider missingPersistentMembersProvider) {
    this.flushToDiskFactory = flushToDiskFactory;
    this.prepareBackupFactory = prepareBackupFactory;
    this.abortBackupFactory = abortBackupFactory;
    this.finishBackupFactory = finishBackupFactory;
    this.dm = dm;
    this.cache = cache;
    this.backupLockService = backupLockService;
    this.missingPersistentMembersProvider = missingPersistentMembersProvider;
  }

  public BackupStatus backupAllMembers(String targetDirPath, String baselineDirPath) {
    Properties properties = new BackupConfigFactory().withTargetDirPath(targetDirPath)
        .withBaselineDirPath(baselineDirPath).createBackupProperties();
    return performBackup(properties);
  }

  private BackupStatus performBackup(Properties properties) throws ManagementException {
    if (backupLockService.obtainLock(dm)) {
      try {
        return performBackupUnderLock(properties);
      } finally {
        backupLockService.releaseLock(dm);
      }
    } else {
      throw new ManagementException(
          "A backup is already in progress.");
    }
  }

  private BackupStatus performBackupUnderLock(Properties properties) {
    Set<PersistentID> missingMembers =
        missingPersistentMembersProvider.getMissingPersistentMembers(dm);
    Set<InternalDistributedMember> recipients = dm.getOtherDistributionManagerIds();

    BackupDataStoreResult result = performBackupSteps(dm.getId(), recipients, properties);

    // It's possible that when calling getMissingPersistentMembers, some members are
    // still creating/recovering regions, and at FinishBackupRequest.send, the
    // regions at the members are ready. Logically, since the members in successfulMembers
    // should override the previous missingMembers
    for (Set<PersistentID> onlineMembersIds : result.getSuccessfulMembers().values()) {
      missingMembers.removeAll(onlineMembersIds);
    }

    result.getExistingDataStores().keySet().removeAll(result.getSuccessfulMembers().keySet());
    for (Set<PersistentID> lostMembersIds : result.getExistingDataStores().values()) {
      missingMembers.addAll(lostMembersIds);
    }
    return new BackupStatusImpl(result.getSuccessfulMembers(), missingMembers);
  }

  private BackupDataStoreResult performBackupSteps(InternalDistributedMember member,
      Set<InternalDistributedMember> recipients, Properties properties) {
    flushToDiskFactory.createFlushToDiskStep(dm, member, cache, recipients, flushToDiskFactory)
        .send();

    boolean abort = true;
    Map<DistributedMember, Set<PersistentID>> successfulMembers;
    Map<DistributedMember, Set<PersistentID>> existingDataStores;
    try {
      PrepareBackupStep prepareBackupStep =
          prepareBackupFactory.createPrepareBackupStep(dm, member, cache, recipients,
              prepareBackupFactory, properties);
      existingDataStores = prepareBackupStep.send();
      abort = false;
    } finally {
      if (abort) {
        abortBackupFactory.createAbortBackupStep(dm, member, cache, recipients, abortBackupFactory)
            .send();
        successfulMembers = Collections.emptyMap();
      } else {
        successfulMembers =
            finishBackupFactory.createFinishBackupStep(dm, member, cache, recipients,
                finishBackupFactory).send();
      }
    }
    return new BackupDataStoreResult(existingDataStores, successfulMembers);
  }

  interface MissingPersistentMembersProvider {

    Set<PersistentID> getMissingPersistentMembers(DistributionManager dm);
  }

  private static class DefaultMissingPersistentMembersProvider
      implements MissingPersistentMembersProvider {

    @Override
    public Set<PersistentID> getMissingPersistentMembers(DistributionManager dm) {
      return AdminDistributedSystemImpl.getMissingPersistentMembers(dm);
    }
  }
}
