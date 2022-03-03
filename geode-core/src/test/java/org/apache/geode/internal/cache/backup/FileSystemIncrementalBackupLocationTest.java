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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.internal.cache.persistence.DiskStoreID;

public class FileSystemIncrementalBackupLocationTest {

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void testNonExistentBackupLocation() throws IOException {
    DiskStore diskstore = mock(DiskStore.class);
    File nonExistingDir = Paths.get("nonexistent").toFile();
    FileSystemIncrementalBackupLocation backupLocation =
        new FileSystemIncrementalBackupLocation(nonExistingDir, "member1");
    assertThat(backupLocation.getBackedUpOplogs(diskstore)).isEmpty();
  }

  @Test
  public void testNonExistentMemberBackupLocation() throws IOException {
    File backupLocation = tempDir.newFolder("backup");
    DiskStore diskstore = mock(DiskStore.class);
    FileSystemIncrementalBackupLocation fileBackupLocation =
        new FileSystemIncrementalBackupLocation(backupLocation, "member1");
    assertThat(fileBackupLocation.getBackedUpOplogs(diskstore)).isEmpty();
  }

  @Test
  public void testWhenDiskstoresAreEmpty() throws IOException {
    String memberId = "member1";
    File backupLocation = tempDir.newFolder("backup");
    Path memberBackupLocation = Files.createDirectories(backupLocation.toPath().resolve(memberId));
    Path diskStoreMemberBackupLocation =
        Files.createDirectories(memberBackupLocation.resolve(BackupWriter.DATA_STORES_DIRECTORY));

    DiskStoreImpl diskStore = mock(DiskStoreImpl.class);
    when(diskStore.getDiskStoreID()).thenReturn(new DiskStoreID(1, 2));
    FileSystemIncrementalBackupLocation fileBackupLocation =
        new FileSystemIncrementalBackupLocation(backupLocation, "member1");

    Files.createDirectories(
        diskStoreMemberBackupLocation.resolve(fileBackupLocation.getBackupDirName(diskStore)));

    assertThat(fileBackupLocation
        .getBackedUpOplogs(fileBackupLocation.getMemberBackupLocationDir().toFile(), diskStore))
            .isEmpty();
  }

  @Test
  public void returnsFilesFromDiskstoreDirectory() throws IOException {
    String memberId = "member1";
    File backupLocation = tempDir.newFolder("backup");
    Path memberBackupLocation = Files.createDirectories(backupLocation.toPath().resolve(memberId));
    Path diskStoreMemberBackupLocation =
        Files.createDirectories(memberBackupLocation.resolve(BackupWriter.DATA_STORES_DIRECTORY));

    DiskStoreImpl diskStore = mock(DiskStoreImpl.class);
    when(diskStore.getDiskStoreID()).thenReturn(new DiskStoreID(1, 2));
    FileSystemIncrementalBackupLocation fileBackupLocation =
        new FileSystemIncrementalBackupLocation(backupLocation, "member1");

    Path diskStorePath = Files.createDirectories(
        diskStoreMemberBackupLocation.resolve(fileBackupLocation.getBackupDirName(diskStore)));

    Path crf = Files.createFile(diskStorePath.resolve("oplog1.crf"));
    Path krf = Files.createFile(diskStorePath.resolve("oplog1.krf"));
    Path drf = Files.createFile(diskStorePath.resolve("oplog1.drf"));

    Collection<File> logFiles = fileBackupLocation
        .getBackedUpOplogs(fileBackupLocation.getMemberBackupLocationDir().toFile(), diskStore);
    assertThat(logFiles).isNotEmpty();
    assertThat(logFiles).contains(crf.toFile());
    assertThat(logFiles).contains(krf.toFile());
    assertThat(logFiles).contains(drf.toFile());
  }

  @Test
  public void returnsPreviouslyBackedFilesFromBackupLocation() throws IOException {
    String memberId = "member1";
    File backupLocation = tempDir.newFolder("backup");
    Files.createDirectories(backupLocation.toPath().resolve(memberId));

    TestableFileSystemIncrementalBackupLocation fileBackupLocation =
        new TestableFileSystemIncrementalBackupLocation(backupLocation, "member1");

    initializeBackupInspector(fileBackupLocation);

    Collection<File> logFiles = fileBackupLocation
        .getPreviouslyBackedUpOpLogs(fileBackupLocation.getMemberBackupLocationDir().toFile());
    assertThat(logFiles).isNotEmpty();
  }

  @Test
  public void returnsCurrentAndPreviouslyBackedFiles() throws IOException {
    String memberId = "member1";
    File backupLocation = tempDir.newFolder("backup");
    Path memberBackupLocation = Files.createDirectories(backupLocation.toPath().resolve(memberId));
    Path diskStoreMemberBackupLocation =
        Files.createDirectories(memberBackupLocation.resolve(BackupWriter.DATA_STORES_DIRECTORY));

    DiskStoreImpl diskStore = mock(DiskStoreImpl.class);
    when(diskStore.getDiskStoreID()).thenReturn(new DiskStoreID(1, 2));
    TestableFileSystemIncrementalBackupLocation fileBackupLocation =
        new TestableFileSystemIncrementalBackupLocation(backupLocation, "member1");

    Path diskStorePath = Files.createDirectories(
        diskStoreMemberBackupLocation.resolve(fileBackupLocation.getBackupDirName(diskStore)));

    Files.createFile(diskStorePath.resolve("2.crf"));
    Files.createFile(diskStorePath.resolve("2.krf"));
    Files.createFile(diskStorePath.resolve("2.drf"));

    initializeBackupInspector(fileBackupLocation);

    Map<String, File> allBackedFiles = fileBackupLocation.getBackedUpOplogs(diskStore);
    assertThat(allBackedFiles.size()).isEqualTo(6);
    assertThat(allBackedFiles.keySet()).contains("1.crf", "1.drf", "1.krf", "2.crf", "2.drf",
        "2.krf");
  }

  private void initializeBackupInspector(
      TestableFileSystemIncrementalBackupLocation fileSystemBackupLocation) {
    BackupInspector backupInspector = mock(BackupInspector.class);
    when(backupInspector.isIncremental()).thenReturn(true);
    Set<String> previousBackupFiles =
        new HashSet<>(Arrays.asList("1.crf", "1.drf", "1.krf"));
    when(backupInspector.getIncrementalOplogFileNames()).thenReturn(previousBackupFiles);
    when(backupInspector.getCopyFromForOplogFile(anyString())).thenAnswer(i -> i.getArguments()[0]);
    fileSystemBackupLocation.setBackupInspector(backupInspector);
  }

  public class TestableFileSystemIncrementalBackupLocation
      extends FileSystemIncrementalBackupLocation {

    BackupInspector backupInspector;

    TestableFileSystemIncrementalBackupLocation(File backupLocationDir, String memberId) {
      super(backupLocationDir, memberId);
    }

    public void setBackupInspector(BackupInspector backupInspector) {
      this.backupInspector = backupInspector;
    }

    @Override
    BackupInspector createBackupInspector(File checkedBaselineDir) throws IOException {
      return backupInspector;
    }
  }
}
