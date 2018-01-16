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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

import org.apache.geode.admin.internal.AdminDistributedSystemImpl;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.management.ManagementException;
import org.apache.geode.management.internal.BackupStatusImpl;

public class BackupUtil {

  private BackupUtil() {
    // do not instantiate
  }

  public static BackupStatus backupAllMembers(DistributionManager dm, File targetDir,
      File baselineDir) throws ManagementException {
    BackupStatus status = null;
    if (BackupDataStoreHelper.obtainLock(dm)) {
      try {
        Set<PersistentID> missingMembers =
            AdminDistributedSystemImpl.getMissingPersistentMembers(dm);
        Set recipients = dm.getOtherDistributionManagerIds();

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
        targetDir = new File(targetDir, format.format(new Date()));
        BackupDataStoreResult result =
            BackupDataStoreHelper.backupAllMembers(dm, recipients, targetDir, baselineDir);

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
        BackupDataStoreHelper.releaseLock(dm);
      }

    } else {
      throw new ManagementException(
          LocalizedStrings.DistributedSystem_BACKUP_ALREADY_IN_PROGRESS.toLocalizedString());
    }
    return status;
  }
}
