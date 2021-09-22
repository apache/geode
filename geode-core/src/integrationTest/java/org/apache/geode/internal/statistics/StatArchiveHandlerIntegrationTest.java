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
package org.apache.geode.internal.statistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;

import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import org.apache.geode.internal.io.MainWithChildrenRollingFileHandler;
import org.apache.geode.internal.io.RollingFileHandler;
import org.apache.geode.test.junit.categories.StatisticsTest;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

/**
 * Tests behavior that interacts with file system.
 */
@Category({StatisticsTest.class})
@RunWith(GeodeParamsRunner.class)
public class StatArchiveHandlerIntegrationTest {

  private File dir;
  private String ext;
  private String name;
  private File archive;

  private StatArchiveHandlerConfig mockConfig;
  private SampleCollector mockCollector;
  private RollingFileHandler rollingFileHandler;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    this.dir = this.temporaryFolder.getRoot();

    this.ext = ".gfs";
    this.name = this.testName.getMethodName();
    this.archive = new File(this.dir, this.name + this.ext);

    this.mockConfig = mock(StatArchiveHandlerConfig.class);
    this.mockCollector = mock(SampleCollector.class);
    this.rollingFileHandler = new MainWithChildrenRollingFileHandler();
  }

  @Test
  @Parameters({"false,false,false", "false,false,true", "false,true,false", "false,true,true",
      "true,false,false", "true,false,true", "true,true,false", "true,true,true"})
  public void getRollingArchiveName_withEmptyDir_createsFirstIds(final boolean archiveExists,
      final boolean archiveClosed, final boolean initMainId) throws Exception {
    if (archiveExists) {
      this.archive.createNewFile();
    }
    StatArchiveHandler handler =
        new StatArchiveHandler(this.mockConfig, this.mockCollector, this.rollingFileHandler);
    if (initMainId) {
      handler.initMainArchiveId(this.archive);
    }

    File file = handler.getRollingArchiveName(this.archive, archiveClosed);

    assertThat(file).hasParent(this.dir).hasName(this.name + formatIds(1, 1) + this.ext);
  }

  @Test
  @Parameters({"1,1,false,false", "1,1,false,true", "1,1,true,false", "1,1,true,true",
      "1,10,false,false", "1,10,false,true", "1,10,true,false", "1,10,true,true",
      "10,1,false,false", "10,1,false,true", "10,1,true,false", "10,1,true,true",
      "10,10,false,false", "10,10,false,true", "10,10,true,false", "10,10,true,true"})
  public void getRollingArchiveName_withOldArchives_rollsChildId(final int mainCount,
      final int childCount, final boolean archiveExists, final boolean archiveClosed)
      throws Exception {
    createEmptyArchiveFiles(this.dir, this.name, this.ext, mainCount, childCount);
    if (archiveExists) {
      this.archive.createNewFile();
    }
    StatArchiveHandler handler =
        new StatArchiveHandler(this.mockConfig, this.mockCollector, this.rollingFileHandler);

    File file = handler.getRollingArchiveName(this.archive, archiveClosed);

    assertThat(file).hasParent(this.dir)
        .hasName(this.name + formatIds(mainCount, childCount + 1) + this.ext);
  }

  @Test
  public void initMainArchiveId_withEmptyDir_createsMainId_createsFirstMarker() throws Exception {
    StatArchiveHandler handler =
        new StatArchiveHandler(this.mockConfig, this.mockCollector, this.rollingFileHandler);

    handler.initMainArchiveId(this.archive);

    assertThat(new File(this.dir, this.name + formatIds(1, 0) + ".marker")).exists();
  }

  @Test
  @Parameters({"1,1", "1,10", "10,1", "10,10"})
  public void initMainArchiveId_withOldArchives_rollsMainId_rollsMarker(final int mainCount,
      final int childCount) throws Exception {
    createEmptyArchiveFiles(this.dir, this.name, this.ext, mainCount, childCount);
    StatArchiveHandler handler =
        new StatArchiveHandler(this.mockConfig, this.mockCollector, this.rollingFileHandler);

    handler.initMainArchiveId(this.archive);

    assertThat(new File(this.dir, this.name + formatIds(mainCount + 1, 0) + ".marker")).exists();
  }

  @Test
  public void getRenameArchiveName_withEmptyDir_createsFirstIds() throws Exception {
    StatArchiveHandler handler =
        new StatArchiveHandler(this.mockConfig, this.mockCollector, this.rollingFileHandler);

    File renamed = handler.getRenameArchiveName(this.archive);

    assertThat(renamed).isNotNull().isEqualTo(new File(this.dir, this.name + "-01-01" + this.ext));
  }

  @Test
  public void getRenameArchiveName_withExtraneousIds_withEmptyDir_appendsIds() throws Exception {
    StatArchiveHandler handler =
        new StatArchiveHandler(this.mockConfig, this.mockCollector, this.rollingFileHandler);
    this.archive = new File(this.dir, this.name + "-01-01" + this.ext);

    File renamed = handler.getRenameArchiveName(this.archive);

    assertThat(renamed).hasParent(this.dir).hasName(this.name + "-01-01-01-01" + this.ext);
  }

  @Test
  public void getRenameArchiveName_withExtraneousDots_withEmptyDir_appendsIds() throws Exception {
    StatArchiveHandler handler =
        new StatArchiveHandler(this.mockConfig, this.mockCollector, this.rollingFileHandler);
    this.archive = new File(this.dir, this.name + ".test.test" + this.ext);

    File renamed = handler.getRenameArchiveName(this.archive);

    assertThat(renamed).hasParent(this.dir).hasName(this.name + ".test.test-01-01" + this.ext);
  }

  @Test
  public void getRenameArchiveName_withExtraneousUnderscores_withEptyDir_appendsIds()
      throws Exception {
    StatArchiveHandler handler =
        new StatArchiveHandler(this.mockConfig, this.mockCollector, this.rollingFileHandler);
    this.archive = new File(this.dir, this.name + "_test_test" + this.ext);

    File renamed = handler.getRenameArchiveName(this.archive);

    assertThat(renamed).hasParent(this.dir).hasName(this.name + "_test_test-01-01" + this.ext);
  }

  @Test
  public void getRenameArchiveName_withExtraneousHyphens_withEmptyDir_appendsIds()
      throws Exception {
    StatArchiveHandler handler =
        new StatArchiveHandler(this.mockConfig, this.mockCollector, this.rollingFileHandler);
    this.archive = new File(this.dir, this.name + "-test-test" + this.ext);

    File renamed = handler.getRenameArchiveName(this.archive);

    assertThat(renamed).hasParent(this.dir).hasName(this.name + "-test-test-01-01" + this.ext);
  }

  @Test
  @Parameters({"1,1", "1,10", "10,1", "10,10"})
  public void getRenameArchiveName_withOldArchives_rollsMainId(final int mainCount,
      final int childCount) throws Exception {
    StatArchiveHandler handler =
        new StatArchiveHandler(this.mockConfig, this.mockCollector, this.rollingFileHandler);
    createEmptyArchiveFiles(this.dir, this.name, this.ext, mainCount, childCount);

    File renamed = handler.getRenameArchiveName(this.archive);

    assertThat(renamed).doesNotExist().hasParent(this.dir)
        .hasName(this.name + formatIds(mainCount + 1, 1) + this.ext);
  }

  @Test
  public void getRenameArchiveName_withNullArchive_throwsNullPointerException() throws Exception {
    StatArchiveHandler handler =
        new StatArchiveHandler(this.mockConfig, this.mockCollector, this.rollingFileHandler);
    File archive = null;

    assertThatThrownBy(() -> handler.getRenameArchiveName(archive))
        .isInstanceOf(NullPointerException.class);
  }

  /**
   * Returns mainId and childId formatted like "-01-01"
   */
  private String formatIds(final int mainId, final int childId) {
    return "-" + formatId(mainId) + "-" + formatId(childId);
  }

  /**
   * Returns id formatted like "01"
   */
  private String formatId(final int id) {
    return String.format("%02d", id);
  }

  /**
   * Creates empty archive files like dir/name-mainId-childId.ext. Returns greatest mainId.
   */
  private int createEmptyArchiveFiles(final File dir, final String name, final String ext,
      final int mainCount, final int childCount) throws IOException {
    int mainId = 1;
    for (; mainId <= mainCount; mainId++) {
      for (int childId = 1; childId <= childCount; childId++) {
        File existing = new File(dir, name + formatIds(mainId, childId) + ext);
        existing.createNewFile();
      }
    }
    return mainId - 1;
  }
}
