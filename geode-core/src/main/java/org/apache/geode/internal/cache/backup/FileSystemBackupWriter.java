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
import org.apache.geode.internal.cache.DirectoryHolder;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;

class FileSystemBackupWriter implements BackupWriter {

  private final Path backupDirectory;
  private final FileSystemIncrementalBackupLocation incrementalBaselineLocation;
  private final BackupFilter filter;

  FileSystemBackupWriter(Path backupDirectory) {
    this(backupDirectory, null);
  }

  FileSystemBackupWriter(Path backupDirectory,
      FileSystemIncrementalBackupLocation incrementalBaselineLocation) {
    this.backupDirectory = backupDirectory;
    this.incrementalBaselineLocation = incrementalBaselineLocation;
    filter = createBackupFilter(incrementalBaselineLocation);
  }

  private BackupFilter createBackupFilter(
      FileSystemIncrementalBackupLocation incrementalBaselineLocation) {
    BackupFilter filter;
    if (incrementalBaselineLocation != null
        && Files.exists(incrementalBaselineLocation.getMemberBackupLocationDir())) {
      filter = new IncrementalBackupFilter(incrementalBaselineLocation);
    } else {
      filter = (store, path) -> true;
    }
    return filter;
  }

  @Override
  public void backupFiles(BackupDefinition backupDefinition) throws IOException {
    Files.createDirectories(backupDirectory);
    Files.createFile(backupDirectory.resolve(INCOMPLETE_BACKUP_FILE));
    backupAllFilesets(backupDefinition);
    Files.delete(backupDirectory.resolve(INCOMPLETE_BACKUP_FILE));
  }

  @Override
  public Path getBaselineDirectory() {
    return incrementalBaselineLocation.getMemberBackupLocationDir().getParent();
  }

  @Override
  public Path getBackupDirectory() {
    return backupDirectory;
  }

  private void backupAllFilesets(BackupDefinition backupDefinition) throws IOException {
    RestoreScript restoreScript = backupDefinition.getRestoreScript();
    backupDiskInitFiles(backupDefinition.getDiskInitFiles());
    backupOplogs(backupDefinition.getOplogFilesByDiskStore(), restoreScript);
    backupConfigFiles(backupDefinition.getConfigFiles());
    backupUserFiles(backupDefinition.getUserFiles(), restoreScript);
    backupDeployedJars(backupDefinition.getDeployedJars(), restoreScript);
    File scriptFile = restoreScript.generate(backupDirectory.toFile());
    backupRestoreScript(scriptFile.toPath());
    writeReadMe();
  }

  private void writeReadMe() throws IOException {
    String text =
        "This directory contains a backup of the persistent data for a single gemfire VM. The layout is:diskstoresA backup of the persistent disk stores in the VMuserAny files specified by the backup element in the cache.xml file.configThe cache.xml and gemfire.properties for the backed up member.restore.[sh|bat]A script to restore the backup.Please note that the config is not restored, only the diskstores and user files.";
    Files.write(backupDirectory.resolve(README_FILE), text.getBytes());
  }

  private void backupRestoreScript(Path restoreScriptFile) throws IOException {
    Files.copy(restoreScriptFile, backupDirectory.resolve(restoreScriptFile.getFileName()));
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

  private void backupUserFiles(Map<Path, Path> userFiles, RestoreScript restoreScript)
      throws IOException {
    Path userDirectory = backupDirectory.resolve(USER_FILES_DIRECTORY);
    Files.createDirectories(userDirectory);

    for (Map.Entry<Path, Path> userFileEntry : userFiles.entrySet()) {
      Path userFile = userFileEntry.getKey();
      Path originalFile = userFileEntry.getValue();

      Path destination = userDirectory.resolve(userFile.getFileName());
      moveFileOrDirectory(userFile, destination);
      restoreScript.addUserFile(originalFile.toFile(), destination.toFile());
    }
  }

  private void backupDeployedJars(Map<Path, Path> jarFiles, RestoreScript restoreScript)
      throws IOException {
    Path jarsDirectory = backupDirectory.resolve(DEPLOYED_JARS_DIRECTORY);
    Files.createDirectories(jarsDirectory);

    for (Map.Entry<Path, Path> jarFileEntry : jarFiles.entrySet()) {
      Path jarFile = jarFileEntry.getKey();
      Path originalFile = jarFileEntry.getValue();

      Path destination = jarsDirectory.resolve(jarFile.getFileName());
      moveFileOrDirectory(jarFile, destination);
      restoreScript.addFile(originalFile.toFile(), destination.toFile());
    }
  }

  private void backupConfigFiles(Collection<Path> configFiles) throws IOException {
    Path configDirectory = backupDirectory.resolve(CONFIG_DIRECTORY);
    Files.createDirectories(configDirectory);
    moveFilesOrDirectories(configFiles, configDirectory);
  }

  private void backupOplogs(Map<DiskStore, Collection<Path>> oplogFiles,
      RestoreScript restoreScript) throws IOException {
    File storesDir = new File(backupDirectory.toFile(), DATA_STORES_DIRECTORY);
    for (Map.Entry<DiskStore, Collection<Path>> entry : oplogFiles.entrySet()) {
      DiskStoreImpl diskStore = (DiskStoreImpl) entry.getKey();
      boolean diskstoreHasFilesInBackup = false;
      for (Path path : entry.getValue()) {
        if (filter.accept(diskStore, path)) {
          diskstoreHasFilesInBackup = true;
          int index = diskStore.getInforFileDirIndex();
          Path backupDir = createOplogBackupDir(diskStore, index);
          backupOplog(backupDir, path);
        } else {
          Map<String, File> baselineOplogMap =
              incrementalBaselineLocation.getBackedUpOplogs(diskStore);
          restoreScript.addBaselineFile(baselineOplogMap.get(path.getFileName().toString()),
              new File(path.toAbsolutePath().getParent().getParent().toFile(),
                  path.getFileName().toString()));
        }
      }
      if (diskstoreHasFilesInBackup) {
        addDiskStoreDirectoriesToRestoreScript((DiskStoreImpl) entry.getKey(),
            getBaseBackupDirectory().toFile(), restoreScript);
      }
      File targetStoresDir = new File(storesDir, getBackupDirName(diskStore));
      addDiskStoreDirectoriesToRestoreScript(diskStore, targetStoresDir, restoreScript);

    }
  }

  private Path getOplogBackupDir(DiskStore diskStore, int index) {
    String name = diskStore.getName();
    if (name == null) {
      name = GemFireCacheImpl.getDefaultDiskStoreName();
    }
    name = name + "_" + ((DiskStoreImpl) diskStore).getDiskStoreID().toString();
    return backupDirectory.resolve(DATA_STORES_DIRECTORY).resolve(name)
        .resolve(BACKUP_DIR_PREFIX + index);
  }

  private Path createOplogBackupDir(DiskStore diskStore, int index) throws IOException {
    Path oplogBackupDir = getOplogBackupDir(diskStore, index);
    Files.createDirectories(oplogBackupDir);
    return oplogBackupDir;
  }

  /**
   * Returns the dir name used to back up this DiskStore's directories under. The name is a
   * concatenation of the disk store name and id.
   */
  private String getBackupDirName(DiskStoreImpl diskStore) {
    String name = diskStore.getName();

    if (name == null) {
      name = GemFireCacheImpl.getDefaultDiskStoreName();
    }

    return name + "_" + diskStore.getDiskStoreID().toString();
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
      moveFileOrDirectory(userFile, destination);
    }
  }

  private void moveFileOrDirectory(Path userFile, Path destination) throws IOException {
    if (Files.isDirectory(userFile)) {
      FileUtils.moveDirectory(userFile.toFile(), destination.toFile());
    } else {
      Files.move(userFile, destination);
    }
  }

  private void addDiskStoreDirectoriesToRestoreScript(DiskStoreImpl diskStore, File targetDir,
      RestoreScript restoreScript) {
    DirectoryHolder[] directories = diskStore.getDirectoryHolders();
    for (int i = 0; i < directories.length; i++) {
      File backupDir = getBackupDirForCurrentMember(targetDir, i);
      restoreScript.addFile(directories[i].getDir(), backupDir);
    }
  }

  private File getBackupDirForCurrentMember(File targetDir, int index) {
    return new File(targetDir, BACKUP_DIR_PREFIX + index);
  }

  private Path getBaseBackupDirectory() {
    return backupDirectory.getParent();
  }
}
