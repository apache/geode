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
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.management.ManagementException;
import org.apache.geode.management.internal.BackupStatusImpl;

public class BackupOperation {

  private final FlushToDiskFactory flushToDiskFactory;
  private final PrepareBackupFactory prepareBackupFactory;
  private final AbortBackupFactory abortBackupFactory;
  private final FinishBackupFactory finishBackupFactory;

  public BackupOperation() {
    this(new FlushToDiskFactory(), new PrepareBackupFactory(), new AbortBackupFactory(),
        new FinishBackupFactory());
  }

  BackupOperation(FlushToDiskFactory flushToDiskFactory, PrepareBackupFactory prepareBackupFactory,
      AbortBackupFactory abortBackupFactory, FinishBackupFactory finishBackupFactory) {
    this.flushToDiskFactory = flushToDiskFactory;
    this.prepareBackupFactory = prepareBackupFactory;
    this.abortBackupFactory = abortBackupFactory;
    this.finishBackupFactory = finishBackupFactory;
  }

  public BackupStatus backupAllMembers(DistributionManager dm, String targetDirPath,
      String baselineDirPath) {
    Properties properties = new BackupConfigFactory().withTargetDirPath(targetDirPath)
        .withBaselineDirPath(baselineDirPath).createBackupProperties();
    return performBackup(dm, properties);
  }

  private BackupStatus performBackup(DistributionManager dm, Properties properties)
      throws ManagementException {
    BackupLockService backupLockService = new BackupLockService();
    BackupStatus status;
    if (backupLockService.obtainLock(dm)) {
      try {
        Set<PersistentID> missingMembers =
            AdminDistributedSystemImpl.getMissingPersistentMembers(dm);
        Set<InternalDistributedMember> recipients = dm.getOtherDistributionManagerIds();

        BackupDataStoreResult result = performBackupSteps(dm, recipients, properties);

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
        status = new BackupStatusImpl(result.getSuccessfulMembers(), missingMembers);
      } finally {
        backupLockService.releaseLock(dm);
      }

    } else {
      throw new ManagementException(
          LocalizedStrings.DistributedSystem_BACKUP_ALREADY_IN_PROGRESS.toLocalizedString());
    }
    return status;
  }

  private BackupDataStoreResult performBackupSteps(DistributionManager dm, Set recipients,
      Properties properties) {
    new FlushToDiskStep(dm, dm.getId(), dm.getCache(), recipients, flushToDiskFactory).send();

    boolean abort = true;
    Map<DistributedMember, Set<PersistentID>> successfulMembers;
    Map<DistributedMember, Set<PersistentID>> existingDataStores;
    try {
      existingDataStores = new PrepareBackupStep(dm, dm.getId(), dm.getCache(), recipients,
          prepareBackupFactory, properties).send();
      abort = false;
    } finally {
      if (abort) {
        new AbortBackupStep(dm, dm.getId(), dm.getCache(), recipients, abortBackupFactory)
            .send();
        successfulMembers = Collections.emptyMap();
      } else {
        successfulMembers = new FinishBackupStep(dm, dm.getId(), dm.getCache(), recipients,
            finishBackupFactory).send();
      }
    }
    return new BackupDataStoreResult(existingDataStores, successfulMembers);
  }
}
