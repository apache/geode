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

import static org.apache.geode.internal.util.TransformUtils.getFileNameTransformer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.util.TransformUtils;

class FileSystemIncrementalBackupLocation implements IncrementalBackupLocation {

  private static final String INCOMPLETE_BACKUP_FILE = "INCOMPLETE_BACKUP_FILE";

  private final Path memberBackupLocationDir;

  FileSystemIncrementalBackupLocation(File backupLocationDir, String memberId) {
    memberBackupLocationDir = new File(backupLocationDir, memberId).toPath();
  }

  Path getMemberBackupLocationDir() {
    return memberBackupLocationDir;
  }

  @Override
  public Map<String, File> getBackedUpOplogs(DiskStore diskStore) throws IOException {
    File checkedBaselineDir = checkBaseline(diskStore);
    if (checkedBaselineDir == null) {
      return Collections.emptyMap();
    }
    Collection<File> baselineOplogFiles = getBackedUpOplogs(checkedBaselineDir, diskStore);
    baselineOplogFiles.addAll(getPreviouslyBackedUpOpLogs(checkedBaselineDir));

    // Map of baseline oplog file name to oplog file
    return TransformUtils.transformAndMap(baselineOplogFiles, getFileNameTransformer());
  }

  Collection<File> getBackedUpOplogs(File checkedBaselineDir, DiskStore diskStore) {
    File baselineDir = new File(checkedBaselineDir, BackupWriter.DATA_STORES_DIRECTORY);
    baselineDir = new File(baselineDir, getBackupDirName((DiskStoreImpl) diskStore));
    return FileUtils.listFiles(baselineDir, new String[] {"krf", "drf", "crf"}, true);
  }

  Collection<File> getPreviouslyBackedUpOpLogs(File checkedBaselineDir) throws IOException {
    BackupInspector inspector = createBackupInspector(checkedBaselineDir);
    HashSet<File> oplogs = new HashSet<>();
    if (inspector.isIncremental() && inspector.getIncrementalOplogFileNames() != null) {
      inspector.getIncrementalOplogFileNames().forEach(oplog -> {
        oplog = inspector.getCopyFromForOplogFile(oplog);
        oplogs.add(new File(oplog));
      });
    }
    return oplogs;
  }

  BackupInspector createBackupInspector(File checkedBaselineDir) throws IOException {
    return BackupInspector.createInspector(checkedBaselineDir);
  }

  /**
   * Performs a sanity check on the baseline directory for incremental backups. If a baseline
   * directory exists for the member and there is no INCOMPLETE_BACKUP_FILE file then return the
   * data stores directory for this member.
   */
  private File checkBaseline(DiskStore diskStore) {
    File baselineDir = memberBackupLocationDir.toFile();

    if (!baselineDir.exists()) {
      // hmmm, did this member have a restart? Determine which member dir might be a match for us
      baselineDir = findBaselineForThisMember(memberBackupLocationDir.getParent(), diskStore);
    }

    if (null != baselineDir) {
      // check for existence of INCOMPLETE_BACKUP_FILE file
      File incompleteBackup = new File(baselineDir, INCOMPLETE_BACKUP_FILE);
      if (incompleteBackup.exists()) {
        baselineDir = null;
      }
    }
    return baselineDir;
  }

  private File findBaselineForThisMember(Path baselineParentDir, DiskStore diskStore) {
    File baselineDir = null;

    // Find the first matching DiskStoreId directory for this member.
    File[] matchingFiles = baselineParentDir.toFile()
        .listFiles((file, name) -> name.endsWith(getBackupDirName((DiskStoreImpl) diskStore)));
    // We found it? Good. Set this member's baseline to the backed up disk store's member dir (two
    // levels up).
    if (null != matchingFiles && matchingFiles.length > 0) {
      baselineDir = matchingFiles[0].getParentFile().getParentFile();
    }

    return baselineDir;
  }

  /**
   * Returns the dir name used to back up this DiskStore's directories under. The name is a
   * concatenation of the disk store name and id.
   */
  String getBackupDirName(DiskStoreImpl diskStore) {
    String name = diskStore.getName();
    if (name == null) {
      name = GemFireCacheImpl.getDefaultDiskStoreName();
    }
    return name + "_" + diskStore.getDiskStoreID().toString();
  }
}
