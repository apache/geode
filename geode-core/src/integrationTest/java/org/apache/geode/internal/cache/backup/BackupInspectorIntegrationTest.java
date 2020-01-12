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

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Tests for the BackupInspector.
 */
public class BackupInspectorIntegrationTest {

  private static final String IF_FILE_SUFFIX = ".if";
  private static final String CRF_FILE_SUFFIX = ".crf";
  private static final String DRF_FILE_SUFFIX = ".drf";

  private static final String DISK_STORE_BASE_FILE_NAME = "diskStore";
  private static final String DISK_STORE_INCREMENTAL_BASE_FILE_NAME = "diskStore_1";

  private static final String IF_FILE_NAME = DISK_STORE_BASE_FILE_NAME + IF_FILE_SUFFIX;
  private static final String CRF_FILE_NAME = DISK_STORE_BASE_FILE_NAME + CRF_FILE_SUFFIX;
  private static final String DRF_FILE_NAME = DISK_STORE_BASE_FILE_NAME + DRF_FILE_SUFFIX;

  private static final String DISK_STORE_DIR_NAME = "diskStore";
  private static final String BACKUP_DIR_NAME = "backup";
  private static final String INCREMENTAL_DIR_NAME = "incremental";

  // directories created during the test
  private File diskDir = null;
  private File fullBackupDir = null;
  private File incrementalBackupDir = null;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  /**
   * Set up data for all tests.
   */
  @Before
  public void setUp() throws Exception {
    diskDir = createFakeDirectory(DISK_STORE_DIR_NAME, DISK_STORE_BASE_FILE_NAME);
    fullBackupDir = createFakeDirectory(BACKUP_DIR_NAME, DISK_STORE_BASE_FILE_NAME);
    incrementalBackupDir =
        createFakeDirectory(INCREMENTAL_DIR_NAME, DISK_STORE_INCREMENTAL_BASE_FILE_NAME);
    createRestoreScript(fullBackupDir, null); // full restore script; no incremental
    createRestoreScript(incrementalBackupDir, fullBackupDir); // incremental restore script
  }

  /**
   * Tests that an IOException is thrown for a non-existent restore script.
   */
  @Test
  public void nonExistentScriptThrowsIOException() throws Exception {
    assertThatThrownBy(
        () -> BackupInspector.createInspector(temporaryFolder.newFolder("emptyFolder")))
            .isInstanceOf(IOException.class)
            .hasMessageMatching("Restore file restore\\.(bat|sh) does not exist.");
  }

  /**
   * Tests that the parser succeeds for an incremental backup restore script.s
   */
  @Test
  public void canParseRestoreScriptForIncrementalBackup() throws Exception {
    BackupInspector inspector = BackupInspector.createInspector(incrementalBackupDir);
    assertThat(inspector.isIncremental()).isTrue();
    Set<String> oplogFiles = inspector.getIncrementalOplogFileNames();
    assertThat(oplogFiles.isEmpty()).isFalse();
    assertThat(oplogFiles).hasSize(2);
    assertThat(oplogFiles.contains(CRF_FILE_NAME)).isTrue();
    assertThat(oplogFiles.contains(DRF_FILE_NAME)).isTrue();
    validateIncrementalBackupScript(inspector);
  }

  /**
   * Tests that the parser works with a full backup restore script.
   */
  @Test
  public void canParseRestoreScriptForFullBackup() throws Exception {
    BackupInspector inspector = BackupInspector.createInspector(fullBackupDir);
    assertThat(inspector.isIncremental()).isFalse();
    assertThat(inspector.getIncrementalOplogFileNames().isEmpty()).isTrue();
    assertThat(inspector.getScriptLineForOplogFile(DRF_FILE_NAME)).isNull();
  }

  /**
   * Create a directory containing empty oplog files.
   *
   * @param dirName The name of the directory to create.
   * @param diskFileBaseName The base name of the oplog files created in the new directory.
   */
  private File createFakeDirectory(final String dirName, final String diskFileBaseName)
      throws IOException {
    File aDir = temporaryFolder.newFolder(dirName);
    new File(aDir, diskFileBaseName + IF_FILE_SUFFIX).createNewFile();
    new File(aDir, diskFileBaseName + CRF_FILE_SUFFIX).createNewFile();
    new File(aDir, diskFileBaseName + DRF_FILE_SUFFIX).createNewFile();
    return aDir;
  }

  /**
   * Create a restore script. Place it in backupDirToRestoreFrom.
   *
   * @param backupDirToRestoreFrom The directory containing the backup files to restore from. This
   *        could be a full backup or an incremental backup.
   * @param incrementalBaseDir If backupdirToRestoreFrom is an incremental backup, this directory
   *        contains the full backup to apply the incremental to.
   */
  private void createRestoreScript(final File backupDirToRestoreFrom, final File incrementalBaseDir)
      throws IOException {
    RestoreScript restoreScript = new RestoreScript();
    restoreScript.addExistenceTest(new File(diskDir, IF_FILE_NAME));
    restoreScript.addFile(diskDir, backupDirToRestoreFrom);
    if (incrementalBaseDir != null) {
      Map<File, File> baselineFilesMap = new HashMap<>();
      baselineFilesMap.put(new File(incrementalBaseDir, CRF_FILE_NAME),
          new File(diskDir, CRF_FILE_NAME));
      baselineFilesMap.put(new File(incrementalBaseDir, DRF_FILE_NAME),
          new File(diskDir, DRF_FILE_NAME));
      restoreScript.addBaselineFiles(baselineFilesMap);
    }
    restoreScript.generate(backupDirToRestoreFrom);
  }

  /**
   * Tests copy lines for an incremental backup.
   *
   * @param inspector a BackupInspector.
   */
  private void validateIncrementalBackupScript(final BackupInspector inspector) {
    // verify copyFrom
    assertThat(inspector.getCopyFromForOplogFile(CRF_FILE_NAME))
        .isEqualTo(fullBackupDir.getAbsolutePath() + File.separator + CRF_FILE_NAME);
    assertThat(inspector.getCopyFromForOplogFile(DRF_FILE_NAME))
        .isEqualTo(fullBackupDir.getAbsolutePath() + File.separator + DRF_FILE_NAME);

    // verify copyTo
    assertThat(inspector.getCopyToForOplogFile(CRF_FILE_NAME))
        .isEqualTo(diskDir.getAbsolutePath() + File.separator + CRF_FILE_NAME);
    assertThat(inspector.getCopyToForOplogFile(DRF_FILE_NAME))
        .isEqualTo(diskDir.getAbsolutePath() + File.separator + DRF_FILE_NAME);
  }
}
