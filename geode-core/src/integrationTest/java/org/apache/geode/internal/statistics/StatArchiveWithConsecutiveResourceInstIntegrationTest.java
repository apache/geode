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

import static org.apache.geode.internal.statistics.StatArchiveWithConsecutiveResourceInstGenerator.ARCHIVE_FILE_NAME;
import static org.apache.geode.internal.statistics.StatArchiveWithConsecutiveResourceInstGenerator.STATS_SPEC_STRING;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.internal.SystemAdmin.StatSpec;
import org.apache.geode.internal.statistics.StatArchiveReader.ResourceInst;
import org.apache.geode.internal.statistics.StatArchiveReader.StatValue;
import org.apache.geode.test.junit.categories.StatisticsTest;

/**
 * Confirms existence of GEODE-1782 and its fix.
 *
 * <p>
 * GEODE-1782: StatArchiveReader ignores later stats resource with same name as closed stats
 * resource
 *
 * @since Geode 1.0
 */
@Category({StatisticsTest.class})
public class StatArchiveWithConsecutiveResourceInstIntegrationTest {

  private static final Logger logger = LogManager.getLogger();

  private File archiveFile;
  private StatSpec statSpec;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    URL url = getClass().getResource(ARCHIVE_FILE_NAME);
    archiveFile = temporaryFolder.newFile(ARCHIVE_FILE_NAME);
    FileUtils.copyURLToFile(url, archiveFile);

    statSpec = new StatSpec(STATS_SPEC_STRING);

    // precondition
    assertThat(archiveFile).exists();
  }

  @Test
  public void readingFourActiveCacheClientUpdaterStatsWithReaderMatchSpec() throws Exception {
    StatArchiveReader reader =
        new StatArchiveReader(new File[] {archiveFile}, new StatSpec[] {statSpec}, true);

    Set<ResourceInst> resourceInstList = new HashSet<>();
    for (StatValue statValue : reader.matchSpec(statSpec)) {
      for (int i = 0; i < statValue.getResources().length; i++) {
        resourceInstList.add(statValue.getResources()[i]);
      }
    }

    assertThat(resourceInstList.size()).isEqualTo(2);
  }

  @Test
  public void readingFourActiveCacheClientUpdaterStatsWithReader() throws Exception {
    StatArchiveReader reader =
        new StatArchiveReader(new File[] {archiveFile}, new StatSpec[] {statSpec}, true);

    Set<ResourceInst> resourceInstList = new HashSet<>();
    for (Iterator<ResourceInst> it = reader.getResourceInstList().iterator(); it.hasNext();) {
      resourceInstList.add(it.next());
    }

    assertThat(resourceInstList.size()).isEqualTo(2);
  }

  private void printResourceInsts(Set<ResourceInst> resourceInstList) {
    for (ResourceInst resourceInst : resourceInstList) {
      logger.info(testName.getMethodName() + ":ResourceInst: {}", resourceInst);
    }
  }

}
