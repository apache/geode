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
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.io.FileUtils;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.i18n.LocalizedStrings;

public class FileSystemBackupWriter implements BackupWriter {

  static final String INCOMPLETE_BACKUP_FILE = "INCOMPLETE_BACKUP_FILE";

  final FileSystemBackupLocation targetBackupDestination;

  final FileSystemBackupLocation incrementalBaselineLocation;

  FileSystemBackupWriter(FileSystemBackupLocation backupDestination) {
    this.targetBackupDestination = backupDestination;
    this.incrementalBaselineLocation = null;
  }

  FileSystemBackupWriter(FileSystemBackupLocation backupDestination,
      FileSystemBackupLocation incrementalBaselineLocation) {
    this.targetBackupDestination = backupDestination;
    this.incrementalBaselineLocation = incrementalBaselineLocation;
  }

  @Override
  public void backupFiles(BackupDefinition backupDefinition) throws IOException {
    Path backupDir = targetBackupDestination.getMemberBackupLocationDir();
    Files.createDirectories(backupDir);
    Files.createFile(backupDir.resolve(INCOMPLETE_BACKUP_FILE));
    backupAllFilesets(backupDefinition);
    Files.delete(backupDir.resolve(INCOMPLETE_BACKUP_FILE));
  }

  private void backupAllFilesets(BackupDefinition backupDefinition) throws IOException {
    RestoreScript restoreScript = backupDefinition.getRestoreScript();
    backupUserFiles(backupDefinition.getUserFiles());
    backupDeployedJars(backupDefinition.getDeployedJars());
    backupConfigFiles(backupDefinition.getConfigFiles());
    backupOplogs(backupDefinition.getOplogFilesByDiskStore(), restoreScript);
    backupDiskInitFiles(backupDefinition.getDiskInitFiles());
    File scriptFile =
        restoreScript.generate(targetBackupDestination.getMemberBackupLocationDir().toFile());
    backupRestoreScript(scriptFile.toPath());
    writeReadMe();
  }

  private void writeReadMe() throws IOException {
    String text = LocalizedStrings.BackupService_README.toLocalizedString();
    Files.write(targetBackupDestination.getMemberBackupLocationDir().resolve(README_FILE),
        text.getBytes());
  }

  private void backupRestoreScript(Path restoreScriptFile) throws IOException {
    Files.copy(restoreScriptFile, targetBackupDestination.getMemberBackupLocationDir()
        .resolve(restoreScriptFile.getFileName()));
  }

  private void backupDiskInitFiles(Map<DiskStore, Path> diskInitFiles) throws IOException {
    for (Map.Entry<DiskStore, Path> entry : diskInitFiles.entrySet()) {
      Path destinationDirectory = getOplogBackupDir(entry.getKey(),
          ((DiskStoreImpl) entry.getKey()).getInforFileDirIndex());
      Files.createDirectories(destinationDirectory);
      Files.copy(entry.getValue(), destinationDirectory.resolve(entry.getValue().getFileName()),
          StandardCopyOption.COPY_ATTRIBUTES);
    }
  }

  private void backupUserFiles(Collection<Path> userFiles) throws IOException {
    Path userDirectory =
        targetBackupDestination.getMemberBackupLocationDir().resolve(USER_FILES_DIRECTORY);
    Files.createDirectories(userDirectory);
    moveFilesOrDirectories(userFiles, userDirectory);
  }

  private void backupDeployedJars(Collection<Path> jarFiles) throws IOException {
    Path jarsDirectory =
        targetBackupDestination.getMemberBackupLocationDir().resolve(DEPLOYED_JARS_DIRECTORY);
    Files.createDirectories(jarsDirectory);
    moveFilesOrDirectories(jarFiles, jarsDirectory);
  }

  private void backupConfigFiles(Collection<Path> configFiles) throws IOException {
    Path configDirectory =
        targetBackupDestination.getMemberBackupLocationDir().resolve(CONFIG_DIRECTORY);
    Files.createDirectories(configDirectory);
    moveFilesOrDirectories(configFiles, configDirectory);
  }


  private void backupOplogs(Map<DiskStore, Collection<Path>> oplogFiles,
      RestoreScript restoreScript) throws IOException {
    if (this.incrementalBaselineLocation == null) {
      fullBackup(oplogFiles);
    } else {
      incrementalBackup(oplogFiles, restoreScript);
    }
  }

  private void fullBackup(Map<DiskStore, Collection<Path>> oplogFiles) throws IOException {
    for (Map.Entry<DiskStore, Collection<Path>> entry : oplogFiles.entrySet()) {
      for (Path path : entry.getValue()) {
        int index = ((DiskStoreImpl) entry.getKey()).getInforFileDirIndex();
        Path backupDir = createOplogBackupDir(entry.getKey(), index);
        backupOplog(backupDir, path);
      }
    }
  }

  private void incrementalBackup(Map<DiskStore, Collection<Path>> oplogFiles,
      RestoreScript restoreScript) throws IOException {
    for (Map.Entry<DiskStore, Collection<Path>> entry : oplogFiles.entrySet()) {
      Map<String, File> baselineOplogMap =
          incrementalBaselineLocation.getBackedUpOplogs(entry.getKey());
      for (Path path : entry.getValue()) {
        int index = ((DiskStoreImpl) entry.getKey()).getInforFileDirIndex();
        Path backupDir = createOplogBackupDir(entry.getKey(), index);
        if (!baselineOplogMap.containsKey(path.getFileName().toString())) {
          backupOplog(backupDir, path);
        } else {
          restoreScript.addBaselineFile(baselineOplogMap.get(path.getFileName().toString()),
              new File(path.toAbsolutePath().getParent().getParent().toFile(),
                  path.getFileName().toString()));
        }
      }
    }
  }

  private Path getOplogBackupDir(DiskStore diskStore, int index) {
    String name = diskStore.getName();
    if (name == null) {
      name = GemFireCacheImpl.getDefaultDiskStoreName();
    }
    name = name + "_" + ((DiskStoreImpl) diskStore).getDiskStoreID().toString();
    return this.targetBackupDestination.getMemberBackupLocationDir().resolve(DATA_STORES_DIRECTORY)
        .resolve(name).resolve(BACKUP_DIR_PREFIX + index);
  }

  private Path createOplogBackupDir(DiskStore diskStore, int index) throws IOException {
    Path oplogBackupDir = getOplogBackupDir(diskStore, index);
    Files.createDirectories(oplogBackupDir);
    return oplogBackupDir;
  }

  private void backupOplog(Path targetDir, Path path) throws IOException {
    backupFile(targetDir, path.toFile());
  }

  private void backupFile(Path targetDir, File file) throws IOException {
    Files.move(file.toPath(), targetDir.resolve(file.getName()));
  }

  private void moveFilesOrDirectories(Collection<Path> paths, Path targetDirectory)
      throws IOException {
    for (Path userFile : paths) {
      Path destination = targetDirectory.resolve(userFile.getFileName());
      if (Files.isDirectory(userFile)) {
        FileUtils.moveDirectory(userFile.toFile(), destination.toFile());
      } else {
        Files.move(userFile, destination);
      }
    }
  }

}
