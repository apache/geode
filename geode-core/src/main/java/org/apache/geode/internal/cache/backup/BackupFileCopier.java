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
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.DeployedJar;
import org.apache.geode.internal.JarDeployer;
import org.apache.geode.internal.cache.DirectoryHolder;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.Oplog;
import org.apache.geode.internal.logging.LogService;

public class BackupFileCopier {
  Logger logger = LogService.getLogger();

  private static final String CONFIG_DIRECTORY = "config";
  private static final String USER_FILES = "user";

  private InternalCache cache;
  private TemporaryBackupFiles temporaryFiles;
  private BackupDefinition backupDefinition = new BackupDefinition();

  BackupFileCopier(InternalCache cache, TemporaryBackupFiles temporaryFiles) {
    this.cache = cache;
    this.temporaryFiles = temporaryFiles;
  }

  void copyConfigFiles() throws IOException {
    Files.createDirectories(
        temporaryFiles.getDirectory().resolve(CONFIG_DIRECTORY));
    addConfigFileToBackup(cache.getCacheXmlURL());
    addConfigFileToBackup(DistributedSystem.getPropertiesFileURL());
    // TODO: should the gfsecurity.properties file be backed up?
  }

  private void addConfigFileToBackup(URL fileUrl) throws IOException {
    if (fileUrl != null) {
      try {
        Path source = Paths.get(fileUrl.toURI());
        Path destination =
            temporaryFiles.getDirectory().resolve(CONFIG_DIRECTORY).resolve(source.getFileName());
        Files.copy(source, destination, StandardCopyOption.COPY_ATTRIBUTES);
        backupDefinition.addConfigFileToBackup(destination);
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    }
  }

  Set<File> copyUserFiles() throws IOException {
    Set<File> userFilesBackedUp = new HashSet<>();
    Files.createDirectories(
        temporaryFiles.getDirectory().resolve(USER_FILES));
    List<File> backupFiles = cache.getBackupFiles();
    for (File original : backupFiles) {
      if (original.exists()) {
        original = original.getAbsoluteFile();
        Path destination =
            temporaryFiles.getDirectory().resolve(USER_FILES).resolve(original.getName());
        if (original.isDirectory()) {
          FileUtils.copyDirectory(original, destination.toFile());
        } else {
          Files.copy(original.toPath(), destination, StandardCopyOption.COPY_ATTRIBUTES);
        }
        backupDefinition.addUserFilesToBackup(destination);
        userFilesBackedUp.add(original);
      }
    }
    return userFilesBackedUp;
  }

  Set<File> copyDeployedJars() throws IOException {
    Set<File> userJars = new HashSet<>();
    JarDeployer deployer = null;

    try {
      Files.createDirectories(temporaryFiles.getDirectory().resolve(USER_FILES));

      // Suspend any user deployed jar file updates during this backup.
      deployer = getJarDeployer();
      deployer.suspendAll();

      List<DeployedJar> jarList = deployer.findDeployedJars();
      for (DeployedJar jar : jarList) {
        File source = new File(jar.getFileCanonicalPath());
        String sourceFileName = source.getName();
        Path destination =
            temporaryFiles.getDirectory().resolve(USER_FILES).resolve(sourceFileName);
        Files.copy(source.toPath(), destination, StandardCopyOption.COPY_ATTRIBUTES);
        backupDefinition.addDeployedJarToBackup(destination);
        userJars.add(source);
      }
    } finally {
      // Re-enable user deployed jar file updates.
      if (deployer != null) {
        deployer.resumeAll();
      }
    }
    return userJars;
  }

  // package access for testing purposes only
  JarDeployer getJarDeployer() {
    return ClassPathLoader.getLatest().getJarDeployer();
  }

  void copyDiskInitFile(DiskStoreImpl diskStore) throws IOException {
    File diskInitFile = diskStore.getDiskInitFile().getIFFile();
    String subDir = Integer.toString(diskStore.getInforFileDirIndex());
    Files.createDirectories(temporaryFiles.getDirectory().resolve(subDir));
    Files.copy(diskInitFile.toPath(), temporaryFiles.getDirectory().resolve(subDir).resolve(diskInitFile.getName()),
        StandardCopyOption.COPY_ATTRIBUTES);
    backupDefinition.addDiskInitFile(diskStore,
        temporaryFiles.getDirectory().resolve(subDir).resolve(diskInitFile.getName()));
  }

  void copyOplog(DiskStore diskStore, Oplog oplog) throws IOException {
    DirectoryHolder dirHolder = oplog.getDirectoryHolder();
    copyOplogFile(diskStore, dirHolder, oplog.getCrfFile());
    copyOplogFile(diskStore, dirHolder, oplog.getDrfFile());
    copyOplogFile(diskStore, dirHolder, oplog.getKrfFile());
  }

  private void copyOplogFile(DiskStore diskStore, DirectoryHolder dirHolder, File file)
      throws IOException {
    if (file != null && file.exists()) {
      Path tempDiskDir = temporaryFiles.getDiskStoreDirectory(diskStore, dirHolder);
      try {
        Files.createLink(tempDiskDir.resolve(file.getName()), file.toPath());
      } catch (IOException e) {
        logger.warn("Unable to create hard link for {}. Reverting to file copy", tempDiskDir.toString());
        FileUtils.copyFileToDirectory(file, tempDiskDir.toFile());
      }
      backupDefinition.addOplogFileToBackup(diskStore, tempDiskDir.resolve(file.getName()));
    }
  }

  public BackupDefinition getBackupDefinition() {
    return backupDefinition;
  }
}
