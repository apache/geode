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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.internal.cache.DirectoryHolder;

public class TemporaryBackupFilesTest {

  private static final String DISK_STORE_DIR_NAME = "testDiskStores";

  @Rule
  public TemporaryFolder tempDir = new TemporaryFolder();

  private Path baseTempDirectory;
  private TemporaryBackupFiles backupFiles;
  private DiskStore diskStore;
  private DirectoryHolder directoryHolder;

  @Before
  public void setup() throws IOException {
    baseTempDirectory = tempDir.newFolder().toPath();
    backupFiles = new TemporaryBackupFiles(baseTempDirectory, DISK_STORE_DIR_NAME);
    diskStore = mock(DiskStore.class);
    directoryHolder = mock(DirectoryHolder.class);
    when(directoryHolder.getDir()).thenReturn(baseTempDirectory.resolve("dir1").toFile());
  }

  @Test
  public void factoryMethodCreatesValidInstance() throws IOException {
    TemporaryBackupFiles backupFiles = TemporaryBackupFiles.create();
    assertThat(backupFiles.getDirectory()).exists();
  }

  @Test
  public void cannotCreateInstanceWithoutDirectoryLocation() {
    assertThatThrownBy(() -> new TemporaryBackupFiles(null, "test"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void cannotCreateInstanceWithoutNameForDiskStoreDirectories() {
    assertThatThrownBy(() -> new TemporaryBackupFiles(baseTempDirectory, null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new TemporaryBackupFiles(baseTempDirectory, ""))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void returnsCorrectTemporaryDirectory() {
    assertThat(backupFiles.getDirectory()).isEqualTo(baseTempDirectory);
  }

  @Test
  public void returnsCreatedDirectoryForADiskStore() throws IOException {
    Path diskStoreDir = backupFiles.getDiskStoreDirectory(diskStore, directoryHolder);
    assertThat(diskStoreDir)
        .isEqualTo(directoryHolder.getDir().toPath().resolve(DISK_STORE_DIR_NAME));
    assertThat(diskStoreDir).exists();
  }

  @Test
  public void returnsTheSameFileEachTimeForADiskStoreAndDirHolder() throws IOException {
    Path diskStoreDir = backupFiles.getDiskStoreDirectory(diskStore, directoryHolder);
    assertThat(backupFiles.getDiskStoreDirectory(diskStore, directoryHolder))
        .isSameAs(diskStoreDir);
  }

  @Test
  public void cleansUpAllTemporaryDirectories() throws IOException {
    DirectoryHolder directoryHolder2 = mock(DirectoryHolder.class);
    when(directoryHolder2.getDir()).thenReturn(baseTempDirectory.resolve("dir2").toFile());
    Path diskStoreDir = backupFiles.getDiskStoreDirectory(diskStore, directoryHolder);
    Path diskStoreDir2 = backupFiles.getDiskStoreDirectory(diskStore, directoryHolder2);

    backupFiles.cleanupFiles();

    assertThat(baseTempDirectory).doesNotExist();
    assertThat(diskStoreDir).doesNotExist();
    assertThat(diskStoreDir2).doesNotExist();
  }
}
