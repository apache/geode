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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import org.junit.Test;

import org.apache.geode.cache.DiskStore;

public class BackupDefinitionTest {

  private final BackupDefinition backupDefinition = new BackupDefinition();

  @Test
  public void hasNoFilesWhenInitialized() {
    assertThat(backupDefinition.getConfigFiles()).isEmpty();
    assertThat(backupDefinition.getDeployedJars()).isEmpty();
    assertThat(backupDefinition.getUserFiles()).isEmpty();
    assertThat(backupDefinition.getOplogFilesByDiskStore()).isEmpty();
    assertThat(backupDefinition.getDiskInitFiles()).isEmpty();
    assertThat(backupDefinition.getRestoreScript()).isNull();
  }

  @Test
  public void returnsNonModifiableCollections() {
    Path cannotBeAdded = Paths.get("");
    assertThatThrownBy(() -> backupDefinition.getConfigFiles().add(cannotBeAdded))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> backupDefinition.getDeployedJars().put(cannotBeAdded, cannotBeAdded))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> backupDefinition.getUserFiles().put(cannotBeAdded, cannotBeAdded))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> backupDefinition.getOplogFilesByDiskStore().put(mock(DiskStore.class),
        Collections.emptySet())).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(
        () -> backupDefinition.getDiskInitFiles().put(mock(DiskStore.class), cannotBeAdded))
            .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void containsConfigFilesAdded() {
    Path config1 = Paths.get("config1");
    Path config2 = Paths.get("config2");
    backupDefinition.addConfigFileToBackup(config1);
    backupDefinition.addConfigFileToBackup(config2);
    assertThat(backupDefinition.getConfigFiles()).containsOnly(config1, config2);
  }


  @Test
  public void containsDeployedJarFilesAdded() {
    Path jar1 = Paths.get("jar1");
    Path jar2 = Paths.get("jar2");
    Path source = Paths.get("source");
    backupDefinition.addDeployedJarToBackup(jar1, source);
    backupDefinition.addDeployedJarToBackup(jar2, source);
    assertThat(backupDefinition.getDeployedJars().keySet()).containsOnly(jar1, jar2);
    assertThat(backupDefinition.getDeployedJars().values()).containsOnly(source, source);
  }

  @Test
  public void containsUserFilesAdded() {
    Path userFile1 = Paths.get("userFile1");
    Path userFile2 = Paths.get("userFile2");
    Path source = Paths.get("source");
    backupDefinition.addUserFilesToBackup(userFile1, source);
    backupDefinition.addUserFilesToBackup(userFile2, source);
    assertThat(backupDefinition.getUserFiles().keySet()).containsOnly(userFile1, userFile2);
    assertThat(backupDefinition.getUserFiles().values()).containsOnly(source, source);
  }

  @Test
  public void containsAllAddedOplogFilesAdded() {
    DiskStore diskStore = mock(DiskStore.class);
    Path file1 = mock(Path.class);
    Path file2 = mock(Path.class);
    backupDefinition.addOplogFileToBackup(diskStore, file1);
    assertThat(backupDefinition.getOplogFilesByDiskStore()).containsEntry(diskStore,
        Collections.singleton(file1));
    backupDefinition.addOplogFileToBackup(diskStore, file2);
    assertThat(backupDefinition.getOplogFilesByDiskStore().get(diskStore))
        .containsExactlyInAnyOrder(file1, file2);
  }

  @Test
  public void containsAllDiskInitFiles() {
    DiskStore diskStore1 = mock(DiskStore.class);
    DiskStore diskStore2 = mock(DiskStore.class);
    Path diskInit1 = Paths.get("diskInit1");
    Path diskInit2 = Paths.get("diskInit2");
    backupDefinition.addDiskInitFile(diskStore1, diskInit1);
    backupDefinition.addDiskInitFile(diskStore2, diskInit2);
    assertThat(backupDefinition.getDiskInitFiles()).hasSize(2).containsValues(diskInit1, diskInit2);
  }

  @Test
  public void hasSetRestoreScript() {
    RestoreScript restoreScript = new RestoreScript();
    backupDefinition.setRestoreScript(restoreScript);
    assertThat(backupDefinition.getRestoreScript()).isSameAs(restoreScript);
  }
}
