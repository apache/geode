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

import static org.apache.geode.internal.cache.backup.FileSystemBackupWriter.CONFIG_DIRECTORY;
import static org.apache.geode.internal.cache.backup.FileSystemBackupWriter.DATA_STORES_DIRECTORY;
import static org.apache.geode.internal.cache.backup.FileSystemBackupWriter.DEPLOYED_JARS_DIRECTORY;
import static org.apache.geode.internal.cache.backup.FileSystemBackupWriter.INCOMPLETE_BACKUP_FILE;
import static org.apache.geode.internal.cache.backup.FileSystemBackupWriter.README_FILE;
import static org.apache.geode.internal.cache.backup.FileSystemBackupWriter.USER_FILES_DIRECTORY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import junitparams.Parameters;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.apache.geode.internal.cache.DirectoryHolder;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.Oplog;
import org.apache.geode.internal.cache.persistence.DiskStoreID;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
public class FileSystemBackupWriterTest {
  private static final Path RELATIVE_TARGET_DIR = Paths.get("backupTest");

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  private BackupDefinition backupDefinition;
  private Path targetDir;
  private Path sourceDir;
  private RestoreScript restoreScript;
  String memberId = "member1";

  @Before
  public void setup() throws IOException {
    backupDefinition = new BackupDefinition();
    Path backupDirectory = tempDir.newFolder("backups").toPath();
    targetDir = backupDirectory.resolve("backupTarget");
    sourceDir = backupDirectory.resolve("backupSource");
    restoreScript = mock(RestoreScript.class);
    doNothing().when(restoreScript).addUserFile(any(), any());
    doNothing().when(restoreScript).addFile(any(), any());
    when(restoreScript.generate(any())).thenReturn(tempDir.newFile());
  }

  @After
  public void tearDown() throws IOException {
    // Testing relative paths forces creation of files in working dir instead of temporary folder
    if (Files.exists(RELATIVE_TARGET_DIR)) {
      FileUtils.deleteDirectory(RELATIVE_TARGET_DIR.toFile());
    }
  }

  @Test
  @Parameters({"true", "false"})
  public void userFilesAreBackedUp(boolean useRelativePath) throws Exception {
    Path userFile = tempDir.newFile("userFile").toPath();
    Path userSubdir = tempDir.newFolder("userSubDir").toPath();
    Path userFileInDir = Files.write(userSubdir.resolve("fileInDir"), new byte[] {});
    backupDefinition.addUserFilesToBackup(userFile, sourceDir);
    backupDefinition.addUserFilesToBackup(userSubdir, sourceDir);
    backupDefinition.setRestoreScript(restoreScript);

    executeBackup(useRelativePath);

    Path userDir = getTargetMemberDir(useRelativePath).resolve(USER_FILES_DIRECTORY);
    assertThat(userDir.resolve(userFile.getFileName())).exists();
    assertThat(userDir.resolve(userSubdir.getFileName())).exists();
    assertThat(userDir.resolve(userSubdir.getFileName()).resolve(userFileInDir.getFileName()))
        .exists();
  }

  @Test
  @Parameters({"true", "false"})
  public void deployedJarsAreBackedUp(boolean useRelativePath) throws Exception {
    Path jarFile = tempDir.newFile("jarFile").toPath();
    Path jarSubdir = tempDir.newFolder("jarSubdir").toPath();
    Path jarInSubdir = Files.write(jarSubdir.resolve("jarInSubdir"), new byte[] {});
    backupDefinition.addDeployedJarToBackup(jarFile, sourceDir);
    backupDefinition.addDeployedJarToBackup(jarSubdir, sourceDir);
    backupDefinition.setRestoreScript(restoreScript);

    executeBackup(useRelativePath);

    Path userDir = getTargetMemberDir(useRelativePath).resolve(DEPLOYED_JARS_DIRECTORY);
    assertThat(userDir.resolve(jarFile.getFileName())).exists();
    assertThat(userDir.resolve(jarSubdir.getFileName())).exists();
    assertThat(userDir.resolve(jarSubdir.getFileName()).resolve(jarInSubdir.getFileName()))
        .exists();
  }

  @Test
  @Parameters({"true", "false"})
  public void configFilesAreBackedUp(boolean useRelativePath) throws Exception {
    Path cacheXml = tempDir.newFile("cache.xml").toPath();
    Path propertyFile = tempDir.newFile("properties").toPath();
    backupDefinition.addConfigFileToBackup(cacheXml);
    backupDefinition.addConfigFileToBackup(propertyFile);
    backupDefinition.setRestoreScript(restoreScript);

    executeBackup(useRelativePath);

    Path configDir = getTargetMemberDir(useRelativePath).resolve(CONFIG_DIRECTORY);
    assertThat(configDir.resolve(cacheXml.getFileName())).exists();
    assertThat(configDir.resolve(propertyFile.getFileName())).exists();
  }

  @Test
  @Parameters({"true", "false"})
  public void oplogFilesAreBackedUp(boolean useRelativePath) throws Exception {
    DiskStoreImpl diskStore = mock(DiskStoreImpl.class);
    when(diskStore.getDiskStoreID()).thenReturn(new DiskStoreID(1, 2));
    Oplog oplog = mock(Oplog.class);
    when(oplog.getCrfFile()).thenReturn(tempDir.newFile("crf"));
    when(oplog.getDrfFile()).thenReturn(tempDir.newFile("drf"));
    when(oplog.getKrfFile()).thenReturn(tempDir.newFile("krf"));
    when(diskStore.getInforFileDirIndex()).thenReturn(1);
    DirectoryHolder[] directoryHolders = new DirectoryHolder[0];
    when(diskStore.getDirectoryHolders()).thenReturn(directoryHolders);

    backupDefinition.addOplogFileToBackup(diskStore, oplog.getCrfFile().toPath());
    backupDefinition.addOplogFileToBackup(diskStore, oplog.getDrfFile().toPath());
    backupDefinition.addOplogFileToBackup(diskStore, oplog.getKrfFile().toPath());
    backupDefinition.setRestoreScript(restoreScript);

    executeBackup(useRelativePath);

    Path diskStoreDir = getTargetMemberDir(useRelativePath).resolve(DATA_STORES_DIRECTORY)
        .resolve(GemFireCacheImpl.getDefaultDiskStoreName() + "_1-2");
    assertThat(diskStoreDir.resolve("dir1").resolve("crf")).exists();
    assertThat(diskStoreDir.resolve("dir1").resolve("drf")).exists();
    assertThat(diskStoreDir.resolve("dir1").resolve("krf")).exists();
  }

  @Test
  @Parameters({"true", "false"})
  public void diskInitFilesAreBackedUp(boolean useRelativePath) throws Exception {
    DiskStoreImpl diskStore1 = mock(DiskStoreImpl.class);
    when(diskStore1.getDiskStoreID()).thenReturn(new DiskStoreID(1, 2));
    when(diskStore1.getInforFileDirIndex()).thenReturn(1);
    DiskStoreImpl diskStore2 = mock(DiskStoreImpl.class);
    when(diskStore2.getDiskStoreID()).thenReturn(new DiskStoreID(1, 2));
    when(diskStore2.getInforFileDirIndex()).thenReturn(2);
    Path initFile1 = tempDir.newFolder("dir1").toPath().resolve("initFile1");
    Path initFile2 = tempDir.newFolder("dir2").toPath().resolve("initFile2");
    Files.createFile(initFile1);
    Files.createFile(initFile2);
    backupDefinition.addDiskInitFile(diskStore1, initFile1);
    backupDefinition.addDiskInitFile(diskStore2, initFile2);
    backupDefinition.setRestoreScript(restoreScript);

    executeBackup(useRelativePath);

    Path diskStoreDir = getTargetMemberDir(useRelativePath).resolve(DATA_STORES_DIRECTORY)
        .resolve(GemFireCacheImpl.getDefaultDiskStoreName() + "_1-2");
    assertThat(diskStoreDir.resolve("dir1").resolve("initFile1")).exists();
    assertThat(diskStoreDir.resolve("dir2").resolve("initFile2")).exists();
  }

  @Test
  @Parameters({"true", "false"})
  public void restoreScriptIsBackedUp(boolean useRelativePath) throws Exception {
    Path restoreScriptPath = tempDir.newFile("restoreScript").toPath();
    when(restoreScript.generate(any())).thenReturn(restoreScriptPath.toFile());
    backupDefinition.setRestoreScript(restoreScript);

    executeBackup(useRelativePath);

    assertThat(getTargetMemberDir(useRelativePath).resolve("restoreScript")).exists();
  }

  @Test
  @Parameters({"true", "false"})
  public void backupContainsReadMe(boolean useRelativePath) throws IOException {
    executeBackup(useRelativePath);

    assertThat(getTargetMemberDir(useRelativePath).resolve(README_FILE)).exists();
  }

  @Test
  @Parameters({"true", "false"})
  public void leavesBehindIncompleteFileOnFailure(boolean useRelativePath) throws Exception {
    Path notCreatedFile = tempDir.newFolder("dir1").toPath().resolve("notCreated");
    backupDefinition.addDeployedJarToBackup(notCreatedFile, sourceDir);

    try {
      executeBackup(useRelativePath);
    } catch (IOException ignore) {
      // expected to occur on missing file
    }

    assertThat(getTargetMemberDir(useRelativePath).resolve(INCOMPLETE_BACKUP_FILE)).exists();
  }

  @Test
  @Parameters({"true", "false"})
  public void doesNotLeaveBehindIncompleteFileOnSuccess(boolean useRelativePath) throws Exception {
    executeBackup(useRelativePath);
    assertThat(getTargetMemberDir(useRelativePath).resolve(INCOMPLETE_BACKUP_FILE)).doesNotExist();
  }

  private void executeBackup(boolean useRelativePath) throws IOException {
    backupDefinition.setRestoreScript(restoreScript);

    BackupWriter backupWriter =
        new FileSystemBackupWriter(getTargetDir(useRelativePath).resolve(memberId));
    backupWriter.backupFiles(backupDefinition);
  }

  private Path getTargetDir(boolean useRelativePath) {
    return useRelativePath ? RELATIVE_TARGET_DIR : targetDir;
  }

  private Path getTargetMemberDir(boolean useRelativePath) {
    Path target = useRelativePath ? RELATIVE_TARGET_DIR : targetDir;
    return target.resolve(memberId);
  }
}
