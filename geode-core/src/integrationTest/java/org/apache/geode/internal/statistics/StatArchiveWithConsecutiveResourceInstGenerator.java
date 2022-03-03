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

import static org.apache.geode.internal.statistics.StatArchiveFormat.NANOS_PER_MILLI;
import static org.apache.geode.internal.statistics.StatUtils.findResourceInsts;
import static org.apache.geode.internal.statistics.TestStatArchiveWriter.WRITER_INITIAL_DATE_MILLIS;
import static org.apache.geode.internal.statistics.TestStatArchiveWriter.WRITER_PREVIOUS_TIMESTAMP_NANOS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.statistics.StatArchiveReader.ResourceInst;
import org.apache.geode.internal.statistics.StatArchiveReader.StatValue;

/**
 * Generates the stat archive file that is committed under src/test/resources for
 * {@link StatArchiveWithConsecutiveResourceInstIntegrationTest} to load.
 *
 * <p>
 * The generated gfs file is used to confirm GEODE-1782 and its fix.
 *
 * @since Geode 1.0
 */
public class StatArchiveWithConsecutiveResourceInstGenerator {

  private static final Logger logger = LogManager.getLogger();

  protected static final String STATS_TYPE_NAME = "TestStats";
  protected static final String STATS_SPEC_STRING = ":" + STATS_TYPE_NAME;

  protected static final String TEST_NAME =
      StatArchiveWithConsecutiveResourceInstIntegrationTest.class.getSimpleName();
  protected static final String ARCHIVE_FILE_NAME = TEST_NAME + ".gfs";

  private File dir;
  private Map<String, String> statisticTypes;
  private Map<String, Map<String, Number>> allStatistics;
  protected String archiveFileName;

  private TestStatisticsManager manager;
  private TestStatisticsSampler sampler;
  private SampleCollector sampleCollector;
  private StatArchiveWriter writer;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUpGenerator() throws Exception {
    statisticTypes = new HashMap<>();
    allStatistics = new HashMap<>();
    dir = temporaryFolder.getRoot();
    archiveFileName =
        new File(ARCHIVE_FILE_NAME).getAbsolutePath();

    manager = new TestStatisticsManager(1, getUniqueName(), WRITER_INITIAL_DATE_MILLIS);
    StatArchiveDescriptor archiveDescriptor =
        new StatArchiveDescriptor.Builder().setArchiveName(archiveFileName).setSystemId(1)
            .setSystemStartTime(WRITER_INITIAL_DATE_MILLIS - 2000).setSystemDirectoryPath(TEST_NAME)
            .setProductDescription(TEST_NAME).build();
    writer = new TestStatArchiveWriter(archiveDescriptor);
    sampler = new TestStatisticsSampler(manager);
    sampleCollector = new SampleCollector(sampler);
    sampleCollector.addSampleHandler(writer);
  }

  @After
  public void tearDown() throws Exception {
    StatisticsTypeFactoryImpl.clear();
  }

  @Test
  public void generateStatArchiveFile() throws Exception {

    long sampleTimeNanos = WRITER_PREVIOUS_TIMESTAMP_NANOS + NANOS_PER_MILLI * 1000;

    // 1) create statistics

    StatisticsType type =
        createStatisticsType(STATS_TYPE_NAME, "description of " + STATS_TYPE_NAME);
    Statistics statistics1 = createStatistics(type, STATS_TYPE_NAME + "1", 1);

    // 2) sample changing stat

    for (int i = 0; i < 100; i++) {
      incInt(statistics1, "stat", 1);
      sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));
    }

    // 3) close statistics

    statistics1.close();

    // 4) recreate statistics

    Statistics statistics2 = createStatistics(type, STATS_TYPE_NAME + "1", 1);

    // 5) sample changing stat again

    for (int i = 0; i < 100; i++) {
      incInt(statistics2, "stat", 1);
      sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));
    }

    // close the writer

    writer.close();

    // validate that stat archive file exists

    File actual = new File(archiveFileName);
    assertTrue(actual.exists());

    // validate content of stat archive file using StatArchiveReader

    StatArchiveReader reader = new StatArchiveReader(new File[] {actual}, null, false);

    // compare all resourceInst values against what was printed above

    for (final Object o : reader.getResourceInstList()) {
      ResourceInst ri = (ResourceInst) o;
      String resourceName = ri.getName();
      assertNotNull(resourceName);

      String expectedStatsType = statisticTypes.get(resourceName);
      assertNotNull(expectedStatsType);
      assertEquals(expectedStatsType, ri.getType().getName());

      Map<String, Number> expectedStatValues = allStatistics.get(resourceName);
      assertNotNull(expectedStatValues);

      StatValue[] statValues = ri.getStatValues();
      for (int i = 0; i < statValues.length; i++) {
        final String statName = ri.getType().getStats()[i].getName();
        assertNotNull(statName);
        assertNotNull(expectedStatValues.get(statName));

        assertEquals(statName, statValues[i].getDescriptor().getName());

        statValues[i].setFilter(StatValue.FILTER_NONE);
        double[] rawSnapshots = statValues[i].getRawSnapshots();
        assertEquals("Value " + i + " for " + statName + " is wrong: " + expectedStatValues,
            expectedStatValues.get(statName).doubleValue(), statValues[i].getSnapshotsMostRecent(),
            0.01);
      }
    }

    validateArchiveFile();
  }

  protected void validateArchiveFile() throws IOException {
    final File archiveFile = new File(archiveFileName);
    assertTrue(archiveFile.exists());

    logger.info("ArchiveFile: {}", archiveFile.getAbsolutePath());
    logger.info("ArchiveFile length: {}", archiveFile.length());

    for (ResourceInst resourceInst : findResourceInsts(archiveFile, STATS_SPEC_STRING)) {
      logger.info("ResourceInst: {}", resourceInst);
    }
  }

  private String getUniqueName() {
    return StatArchiveWithConsecutiveResourceInstGenerator.class + "_"
        + testName.getMethodName();
  }

  private StatisticsType createStatisticsType(final String name, final String description) {
    StatisticDescriptor[] descriptors = new StatisticDescriptor[] {
        manager.createIntCounter("stat", "description of stat", "units"),};
    return manager.createType(name, description, descriptors);
  }

  private Statistics createStatistics(final StatisticsType type, final String textId,
      final long numericId) {
    return manager.createAtomicStatistics(type, textId, 1);
  }

  private void incInt(Statistics statistics, String stat, int value) {
    assertFalse(statistics.isClosed());
    Map<String, Number> statValues = allStatistics.get(statistics.getTextId());
    if (statValues == null) {
      statValues = new HashMap<>();
      allStatistics.put(statistics.getTextId(), statValues);
    }
    statistics.incInt(stat, value);
    statValues.put(stat, statistics.getInt(stat));
    if (statisticTypes.get(statistics.getTextId()) == null) {
      statisticTypes.put(statistics.getTextId(), statistics.getType().getName());
    }
  }
}
