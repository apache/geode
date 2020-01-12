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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.internal.cache.DirectoryHolder;
import org.apache.geode.internal.cache.Oplog;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Creates and keeps track of the temporary locations used during a backup. Most temporary files are
 * stored in a shared temporary directory, except for those for DiskStores. For each
 * {@link DiskStore}, a temporary directory is created per disk directory within that disk directory
 * (for enabling creation of hard-links for backing up the files of an {@link Oplog}).
 */
class TemporaryBackupFiles {
  private static final Logger logger = LogService.getLogger();

  private final String diskStoreDirectoryName;
  private final Path directory;
  private final Map<DiskStore, Map<DirectoryHolder, Path>> diskStoreDirDirsByDiskStore =
      new HashMap<>();

  /**
   * Creates a new instance with the default structure, where temporary directories are created
   * using the current timestamp in their name for the purpose of uniquification
   *
   * @return a new TemporaryBackupFiles
   * @throws IOException If unable to create a temporary directory
   */
  static TemporaryBackupFiles create() throws IOException {
    long currentTime = System.currentTimeMillis();
    String diskStoreDirectoryName = BackupService.TEMPORARY_DIRECTORY_FOR_BACKUPS + currentTime;
    Path temporaryDirectory = Files.createTempDirectory("backup_" + currentTime);
    return new TemporaryBackupFiles(temporaryDirectory, diskStoreDirectoryName);
  }

  /**
   * Constructs a new TemporaryBackupFiles that will use the specified locations for temporary files
   *
   * @param directory the location to create and store temporary files during backup
   * @param diskStoreDirectoryName name of directory to create within each disk store directory for
   *        its temporary files during backup
   */
  TemporaryBackupFiles(Path directory, String diskStoreDirectoryName) {
    if (directory == null) {
      throw new IllegalArgumentException("Must provide a temporary directory location");
    }
    if (diskStoreDirectoryName == null || diskStoreDirectoryName.isEmpty()) {
      throw new IllegalArgumentException("Must provide a name for temporary DiskStore directories");
    }

    this.directory = directory;
    this.diskStoreDirectoryName = diskStoreDirectoryName;
  }

  /**
   * Provides the temporary directory location used for all temporary backup files except those for
   * Oplogs
   *
   * @return The path of the shared temporary directory
   */
  Path getDirectory() {
    return directory;
  }

  /**
   * Provides, and creates if necessary, the temporary directory used during the backup for Oplog
   * files for the given DiskStore and DirectoryHolder
   *
   * @param diskStore The corresponding {@link DiskStore} to get a temporary directory for
   * @param dirHolder The disk directory of the {@link DiskStore} to get a temporary directory for
   * @return Path to the temporary directory
   * @throws IOException If the temporary directory did not exist and could not be created
   */
  Path getDiskStoreDirectory(DiskStore diskStore, DirectoryHolder dirHolder) throws IOException {
    Map<DirectoryHolder, Path> tempDirByDirectoryHolder =
        diskStoreDirDirsByDiskStore.computeIfAbsent(diskStore, k -> new HashMap<>());
    Path directory = tempDirByDirectoryHolder.get(dirHolder);
    if (directory != null) {
      return directory;
    }

    File diskStoreDir = dirHolder.getDir();
    directory = diskStoreDir.toPath().resolve(diskStoreDirectoryName);
    Files.createDirectories(directory);
    tempDirByDirectoryHolder.put(dirHolder, directory);
    return directory;
  }

  /**
   * Attempts to delete all temporary directories and their contents. An attempt will be made to
   * delete each directory, regardless of the failure to delete any particular one.
   */
  void cleanupFiles() {
    if (directory != null) {
      deleteDirectory(directory);
    }

    for (Map<DirectoryHolder, Path> diskStoreDirToTempDirMap : diskStoreDirDirsByDiskStore
        .values()) {
      for (Path tempDir : diskStoreDirToTempDirMap.values()) {
        deleteDirectory(tempDir);
      }
    }
  }

  private void deleteDirectory(Path directory) {
    try {
      FileUtils.deleteDirectory(directory.toFile());
    } catch (IOException | IllegalArgumentException e) {
      logger.warn("Unable to delete temporary directory created during backup, " + this.directory,
          e);
    }
  }
}
