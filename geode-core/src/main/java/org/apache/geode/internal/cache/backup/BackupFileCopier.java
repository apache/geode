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
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.DirectoryHolder;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.Oplog;
import org.apache.geode.internal.deployment.JarDeploymentService;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.configuration.Deployment;

class BackupFileCopier {
  private static final Logger logger = LogService.getLogger();

  private static final String CONFIG_DIRECTORY = "config";
  private static final String USER_FILES = "user";

  private final InternalCache cache;
  private final TemporaryBackupFiles temporaryFiles;
  private final JarDeploymentService jarDeploymentService;
  private final BackupDefinition backupDefinition = new BackupDefinition();
  private final Path userDirectory;
  private final Path configDirectory;

  BackupFileCopier(InternalCache cache, JarDeploymentService jarDeploymentService,
      TemporaryBackupFiles temporaryFiles) {
    this.cache = cache;
    this.jarDeploymentService = jarDeploymentService;
    this.temporaryFiles = temporaryFiles;
    userDirectory = temporaryFiles.getDirectory().resolve(USER_FILES);
    configDirectory = temporaryFiles.getDirectory().resolve(CONFIG_DIRECTORY);
  }

  BackupDefinition getBackupDefinition() {
    return backupDefinition;
  }

  void copyConfigFiles() throws IOException {
    ensureExistence(configDirectory);
    addConfigFileToBackup(cache.getCacheXmlURL());
    addConfigFileToBackup(DistributedSystem.getPropertiesFileURL());
    // TODO: should the gfsecurity.properties file be backed up?
  }

  private void ensureExistence(Path directory) throws IOException {
    if (!Files.exists(directory)) {
      Files.createDirectories(directory);
    }
  }

  private void addConfigFileToBackup(URL fileUrl) throws IOException {
    if (fileUrl == null) {
      return;
    }

    try {
      Path source = getSource(fileUrl);
      if (Files.notExists(source)) {
        return;
      }

      Path destination = configDirectory.resolve(source.getFileName());
      Files.copy(source, destination, StandardCopyOption.COPY_ATTRIBUTES);
      backupDefinition.addConfigFileToBackup(destination);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  Set<File> copyUserFiles() throws IOException {
    ensureExistence(userDirectory);
    List<File> backupFiles = cache.getBackupFiles();
    Set<File> userFilesBackedUp = new HashSet<>();
    for (File original : backupFiles) {
      if (original.exists()) {
        original = original.getAbsoluteFile();
        Path destination = userDirectory.resolve(original.getName());
        if (original.isDirectory()) {
          FileUtils.copyDirectory(original, destination.toFile());
        } else {
          Files.copy(original.toPath(), destination, StandardCopyOption.COPY_ATTRIBUTES);
        }
        backupDefinition.addUserFilesToBackup(destination, original.toPath());
        userFilesBackedUp.add(original);
      }
    }
    return userFilesBackedUp;
  }

  Set<File> copyDeployedJars() throws IOException {
    ensureExistence(userDirectory);
    Set<File> userJars = new HashSet<>();
    List<Deployment> deployments = jarDeploymentService.listDeployed();
    for (Deployment deployment : deployments) {
      File source = deployment.getFile();
      String sourceFileName = source.getName();
      Path destination = userDirectory.resolve(sourceFileName);
      Files.copy(source.toPath(), destination, StandardCopyOption.COPY_ATTRIBUTES);
      backupDefinition.addDeployedJarToBackup(destination, source.toPath());
      userJars.add(source);
    }
    return userJars;
  }

  void copyDiskInitFile(DiskStoreImpl diskStore) throws IOException {
    File diskInitFile = diskStore.getDiskInitFile().getIFFile();
    String subDirName = Integer.toString(diskStore.getInforFileDirIndex());
    Path subDir = temporaryFiles.getDirectory().resolve(subDirName);
    Files.createDirectories(subDir);
    Files.copy(diskInitFile.toPath(), subDir.resolve(diskInitFile.getName()),
        StandardCopyOption.COPY_ATTRIBUTES);
    backupDefinition.addDiskInitFile(diskStore, subDir.resolve(diskInitFile.getName()));
  }

  void copyOplog(DiskStore diskStore, Oplog oplog) throws IOException {
    DirectoryHolder dirHolder = oplog.getDirectoryHolder();
    copyOplogFile(diskStore, dirHolder, oplog.getCrfFile());
    copyOplogFile(diskStore, dirHolder, oplog.getDrfFile());
    copyOplogFile(diskStore, dirHolder, oplog.getKrfFile());
  }

  private void copyOplogFile(DiskStore diskStore, DirectoryHolder dirHolder, File file)
      throws IOException {
    if (file == null || !file.exists()) {
      return;
    }

    Path tempDiskDir = temporaryFiles.getDiskStoreDirectory(diskStore, dirHolder);
    if (!SystemUtils.isWindows()) {
      try {
        createLink(tempDiskDir.resolve(file.getName()), file.toPath());
      } catch (IOException e) {
        logger.warn("Unable to create hard link for {}. Reverting to file copy",
            tempDiskDir.toString());
        FileUtils.copyFileToDirectory(file, tempDiskDir.toFile());
      }
    } else {
      // Hard links cannot be deleted on Windows if the process is still running, so prefer to
      // actually copy the files.
      FileUtils.copyFileToDirectory(file, tempDiskDir.toFile());
    }

    backupDefinition.addOplogFileToBackup(diskStore, tempDiskDir.resolve(file.getName()));
  }

  void createLink(Path link, Path existing) throws IOException {
    Files.createLink(link, existing);
  }

  Path getSource(URL fileUrl) throws URISyntaxException {
    return Paths.get(fileUrl.toURI());
  }
}
