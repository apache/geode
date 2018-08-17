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

package org.apache.geode.management.internal.cli.commands;

import java.util.Map;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.backup.BackupOperation;
import org.apache.geode.management.BackupStatus;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class BackupDiskStoreCommand extends InternalGfshCommand {
  public static final String BACKED_UP_DISKSTORES_SECTION = "backed-up-diskstores";
  public static final String OFFLINE_DISKSTORES_SECTION = "offline-diskstores";

  /**
   * Internally, we also verify the resource operation permissions CLUSTER:WRITE:DISK if the region
   * is persistent
   */
  @CliCommand(value = CliStrings.BACKUP_DISK_STORE, help = CliStrings.BACKUP_DISK_STORE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.READ)
  public ResultModel backupDiskStore(
      @CliOption(key = CliStrings.BACKUP_DISK_STORE__DISKDIRS,
          help = CliStrings.BACKUP_DISK_STORE__DISKDIRS__HELP, mandatory = true) String targetDir,
      @CliOption(key = CliStrings.BACKUP_DISK_STORE__BASELINEDIR,
          help = CliStrings.BACKUP_DISK_STORE__BASELINEDIR__HELP) String baselineDir) {

    authorize(ResourcePermission.Resource.CLUSTER, ResourcePermission.Operation.WRITE,
        ResourcePermission.Target.DISK);

    ResultModel result = new ResultModel();
    try {
      InternalCache cache = (InternalCache) getCache();
      DistributionManager dm = cache.getDistributionManager();
      BackupStatus backupStatus;

      if (baselineDir != null && !baselineDir.isEmpty()) {
        backupStatus =
            new BackupOperation(dm, dm.getCache()).backupAllMembers(targetDir, baselineDir);
      } else {
        backupStatus = new BackupOperation(dm, dm.getCache()).backupAllMembers(targetDir, null);
      }

      Map<DistributedMember, Set<PersistentID>> backedupMemberDiskstoreMap =
          backupStatus.getBackedUpDiskStores();

      Set<DistributedMember> backedupMembers = backedupMemberDiskstoreMap.keySet();

      if (!backedupMembers.isEmpty()) {
        TabularResultModel backedupDiskStoresTable = result.addTable(BACKED_UP_DISKSTORES_SECTION);
        backedupDiskStoresTable.setHeader(CliStrings.BACKUP_DISK_STORE_MSG_BACKED_UP_DISK_STORES);

        for (DistributedMember member : backedupMembers) {
          Set<PersistentID> backedupDiskStores = backedupMemberDiskstoreMap.get(member);
          boolean printMember = true;
          String memberName = member.getName();

          if (memberName == null || memberName.isEmpty()) {
            memberName = member.getId();
          }
          for (PersistentID persistentId : backedupDiskStores) {
            if (persistentId != null) {

              String UUID = persistentId.getUUID().toString();
              String hostName = persistentId.getHost().getHostName();
              String directory = persistentId.getDirectory();

              if (printMember) {
                writeToBackupDiskStoreTable(backedupDiskStoresTable, memberName, UUID, hostName,
                    directory);
                printMember = false;
              } else {
                writeToBackupDiskStoreTable(backedupDiskStoresTable, "", UUID, hostName, directory);
              }
            }
          }
        }
      } else {
        result.addInfo().addLine(CliStrings.BACKUP_DISK_STORE_MSG_NO_DISKSTORES_BACKED_UP);
      }

      Set<PersistentID> offlineDiskStores = backupStatus.getOfflineDiskStores();

      if (!offlineDiskStores.isEmpty()) {
        TabularResultModel offlineDiskStoresTable = result.addTable(OFFLINE_DISKSTORES_SECTION);
        offlineDiskStoresTable.setHeader(CliStrings.BACKUP_DISK_STORE_MSG_OFFLINE_DISK_STORES);

        for (PersistentID offlineDiskStore : offlineDiskStores) {
          offlineDiskStoresTable.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_UUID,
              offlineDiskStore.getUUID().toString());
          offlineDiskStoresTable.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_HOST,
              offlineDiskStore.getHost().getHostName());
          offlineDiskStoresTable.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_DIRECTORY,
              offlineDiskStore.getDirectory());
        }
      }

    } catch (Exception e) {
      return ResultModel.createError(e.getMessage());
    }
    return result;
  }

  private void writeToBackupDiskStoreTable(TabularResultModel table, String memberId, String UUID,
      String host, String directory) {
    table.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_MEMBER, memberId);
    table.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_UUID, UUID);
    table.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_DIRECTORY, directory);
    table.accumulate(CliStrings.BACKUP_DISK_STORE_MSG_HOST, host);
  }
}
