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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.CancelCriterion;
import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.statistics.StatArchiveReader.StatValue;
import org.apache.geode.test.junit.categories.StatisticsTest;

/**
 * Integration tests for statistics sampling.
 *
 * @since GemFire 7.0
 */
@Category({StatisticsTest.class})
public class StatSamplerIntegrationTest {

  private static final Logger logger = LogService.getLogger();

  private Map<String, String> statisticTypes;
  private Map<String, Map<String, Number>> allStatistics;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() {
    this.statisticTypes = new HashMap<>();
    this.allStatistics = new HashMap<>();
  }

  @After
  public void tearDown() {
    this.statisticTypes = null;
    this.allStatistics = null;
    StatisticsTypeFactoryImpl.clear();
    StatArchiveWriter.clearTraceFilter();
  }

  private String getName() {
    return getClass().getSimpleName() + "_" + testName.getMethodName();
  }

  @Test
  public void testStatSampler() throws Exception {
    StatArchiveWriter.setTraceFilter("st1_1", "ST1");

    File folder = temporaryFolder.newFolder();
    String archiveFileName = folder.getAbsolutePath() + File.separator + getName() + ".gfs";

    System.setProperty("stats.log-level", "config");
    System.setProperty("stats.disable", "false");
    System.setProperty("stats.name", getName());
    System.setProperty("stats.archive-file", archiveFileName);
    System.setProperty("stats.file-size-limit", "0");
    System.setProperty("stats.disk-space-limit", "0");
    System.setProperty("stats.sample-rate", "100");

    final CancelCriterion stopper = new CancelCriterion() {
      public String cancelInProgress() {
        return null;
      }

      public RuntimeException generateCancelledException(Throwable e) {
        return null;
      }
    };
    final LocalStatisticsFactory factory = new LocalStatisticsFactory(stopper);
    final StatisticDescriptor[] statsST1 =
        new StatisticDescriptor[] {factory.createDoubleCounter("double_counter_1", "d1", "u1"),
            factory.createDoubleCounter("double_counter_2", "d2", "u2", true),
            factory.createDoubleGauge("double_gauge_3", "d3", "u3"),
            factory.createDoubleGauge("double_gauge_4", "d4", "u4", false),
            factory.createIntCounter("int_counter_5", "d5", "u5"),
            factory.createIntCounter("int_counter_6", "d6", "u6", true),
            factory.createIntGauge("int_gauge_7", "d7", "u7"),
            factory.createIntGauge("int_gauge_8", "d8", "u8", false),
            factory.createLongCounter("long_counter_9", "d9", "u9"),
            factory.createLongCounter("long_counter_10", "d10", "u10", true),
            factory.createLongGauge("long_gauge_11", "d11", "u11"),
            factory.createLongGauge("long_gauge_12", "d12", "u12", false),
            factory.createLongGauge("sampled_long", "d13", "u13", false),
            factory.createIntGauge("sampled_int", "d14", "u14", false),
            factory.createDoubleGauge("sampled_double", "d15", "u15", false)};
    final StatisticsType ST1 = factory.createType("ST1", "ST1", statsST1);
    final Statistics st1_1 = factory.createAtomicStatistics(ST1, "st1_1", 1);
    st1_1.setIntSupplier("sampled_int", () -> 5);
    getOrCreateExpectedValueMap(st1_1).put("sampled_int", 5);
    st1_1.setLongSupplier("sampled_long", () -> 6);
    getOrCreateExpectedValueMap(st1_1).put("sampled_long", 6);
    st1_1.setDoubleSupplier("sampled_double", () -> 7.0);
    getOrCreateExpectedValueMap(st1_1).put("sampled_double", 7.0);

    await("awaiting StatSampler readiness")
        .until(() -> hasSamplerStatsInstances(factory));

    Statistics[] samplerStatsInstances = factory.findStatisticsByTextId("statSampler");
    assertNotNull(samplerStatsInstances);
    assertEquals(1, samplerStatsInstances.length);
    final Statistics samplerStats = samplerStatsInstances[0];

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_gauge_3", 3);
    incInt(st1_1, "int_counter_5", 5);
    incInt(st1_1, "int_gauge_7", 7);
    incLong(st1_1, "long_counter_9", 9);
    incLong(st1_1, "long_gauge_11", 11);

    waitForStatSamplerToRun(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3", 1);
    incDouble(st1_1, "double_gauge_4", 1);
    incInt(st1_1, "int_counter_5", 1);
    incInt(st1_1, "int_counter_6", 1);

    waitForStatSamplerToRun(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3", 1);
    incDouble(st1_1, "double_gauge_4", 1);
    incInt(st1_1, "int_counter_5", 1);
    incInt(st1_1, "int_counter_6", 1);
    incInt(st1_1, "int_gauge_7", 1);
    incInt(st1_1, "int_gauge_8", 1);
    incLong(st1_1, "long_counter_9", 1);
    incLong(st1_1, "long_counter_10", 1);
    incLong(st1_1, "long_gauge_11", 1);
    incLong(st1_1, "long_gauge_12", 1);

    waitForStatSamplerToRun(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3", -1);
    incDouble(st1_1, "double_gauge_4", 1);
    incInt(st1_1, "int_counter_5", 1);
    incInt(st1_1, "int_counter_6", 1);
    incInt(st1_1, "int_gauge_7", -1);
    incInt(st1_1, "int_gauge_8", 1);
    incLong(st1_1, "long_counter_9", 1);
    incLong(st1_1, "long_counter_10", 1);
    incLong(st1_1, "long_gauge_11", -1);
    incLong(st1_1, "long_gauge_12", 1);

    waitForStatSamplerToRun(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3", 1);
    incDouble(st1_1, "double_gauge_4", 1);
    incInt(st1_1, "int_counter_5", 1);
    incInt(st1_1, "int_counter_6", 1);
    incInt(st1_1, "int_gauge_7", 1);
    incInt(st1_1, "int_gauge_8", 1);
    incLong(st1_1, "long_counter_9", 1);
    incLong(st1_1, "long_counter_10", 1);
    incLong(st1_1, "long_gauge_11", 1);
    incLong(st1_1, "long_gauge_12", 1);

    waitForStatSamplerToRun(samplerStats);
    waitForStatSamplerToRun(samplerStats);
    waitForStatSamplerToRun(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_gauge_3", 3);
    incInt(st1_1, "int_counter_5", 5);
    incInt(st1_1, "int_gauge_7", 7);
    incLong(st1_1, "long_counter_9", 9);
    incLong(st1_1, "long_gauge_11", 11);

    waitForStatSamplerToRun(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3", 1);
    incDouble(st1_1, "double_gauge_4", 1);
    incInt(st1_1, "int_counter_5", 1);
    incInt(st1_1, "int_counter_6", 1);

    waitForStatSamplerToRun(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3", 1);
    incDouble(st1_1, "double_gauge_4", 1);
    incInt(st1_1, "int_counter_5", 1);
    incInt(st1_1, "int_counter_6", 1);
    incInt(st1_1, "int_gauge_7", 1);
    incInt(st1_1, "int_gauge_8", 1);
    incLong(st1_1, "long_counter_9", 1);
    incLong(st1_1, "long_counter_10", 1);
    incLong(st1_1, "long_gauge_11", 1);
    incLong(st1_1, "long_gauge_12", 1);

    waitForStatSamplerToRun(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3", -1);
    incDouble(st1_1, "double_gauge_4", 1);
    incInt(st1_1, "int_counter_5", 1);
    incInt(st1_1, "int_counter_6", 1);
    incInt(st1_1, "int_gauge_7", -1);
    incInt(st1_1, "int_gauge_8", 1);
    incLong(st1_1, "long_counter_9", 1);
    incLong(st1_1, "long_counter_10", 1);
    incLong(st1_1, "long_gauge_11", -1);
    incLong(st1_1, "long_gauge_12", 1);

    waitForStatSamplerToRun(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3", 1);
    incDouble(st1_1, "double_gauge_4", 1);
    incInt(st1_1, "int_counter_5", 1);
    incInt(st1_1, "int_counter_6", 1);
    incInt(st1_1, "int_gauge_7", 1);
    incInt(st1_1, "int_gauge_8", 1);
    incLong(st1_1, "long_counter_9", 1);
    incLong(st1_1, "long_counter_10", 1);
    incLong(st1_1, "long_gauge_11", 1);
    incLong(st1_1, "long_gauge_12", 1);

    /*
     * After updating all stats we should wait for the stat sampler to run at least twice. The
     * first statSampler iteration may be running as the stats are being updated. The second
     * statSampler iteration
     * makes sure that all the stats we care about have been sampled and flush to the stat archive
     */
    waitForStatSamplerToRun(samplerStats, 2);

    factory.close();

    final File archiveFile = new File(System.getProperty("stats.archive-file"));
    assertTrue(archiveFile.exists());
    final StatArchiveReader reader = new StatArchiveReader(new File[] {archiveFile}, null, false);

    List resources = reader.getResourceInstList();
    for (Object resource : resources) {
      StatArchiveReader.ResourceInst ri = (StatArchiveReader.ResourceInst) resource;
      String resourceName = ri.getName();
      assertNotNull(resourceName);

      if (!resourceName.equals("st1_1")) {
        logger.info("testStatSampler skipping {}", resourceName);
        continue;
      }

      String expectedStatsType = this.statisticTypes.get(resourceName);
      assertNotNull(expectedStatsType);
      assertEquals(expectedStatsType, ri.getType().getName());

      Map<String, Number> expectedStatValues = this.allStatistics.get(resourceName);
      assertNotNull(expectedStatValues);

      StatValue[] statValues = ri.getStatValues();
      for (int i = 0; i < statValues.length; i++) {
        String statName = ri.getType().getStats()[i].getName();
        assertNotNull(statName);
        assertNotNull(expectedStatValues.get(statName));

        assertEquals(statName, statValues[i].getDescriptor().getName());

        statValues[i].setFilter(StatValue.FILTER_NONE);
        // double[] rawSnapshots = statValues[i].getRawSnapshots();
        assertEquals("Value " + i + " for " + statName + " is wrong: " + expectedStatValues,
            expectedStatValues.get(statName).doubleValue(), statValues[i].getSnapshotsMostRecent(),
            0);
      }
    }
  }

  private boolean hasSamplerStatsInstances(final LocalStatisticsFactory factory) {
    Statistics[] samplerStatsInstances = factory.findStatisticsByTextId("statSampler");
    return samplerStatsInstances != null && samplerStatsInstances.length > 0;
  }

  private void waitForStatSamplerToRun(final Statistics samplerStats, final int timesToRun) {
    final int startSampleCount = samplerStats.getInt("sampleCount");
    await("waiting for the StatSampler to run")
        .until(() -> samplerStats.getInt("sampleCount") >= startSampleCount + timesToRun);
  }

  private void waitForStatSamplerToRun(final Statistics samplerStats) {
    waitForStatSamplerToRun(samplerStats, 1);
  }

  private void incDouble(Statistics statistics, String stat, double value) {
    assertFalse(statistics.isClosed());
    Map<String, Number> statValues = getOrCreateExpectedValueMap(statistics);
    statistics.incDouble(stat, value);
    statValues.put(stat, statistics.getDouble(stat));
    if (this.statisticTypes.get(statistics.getTextId()) == null) {
      this.statisticTypes.put(statistics.getTextId(), statistics.getType().getName());
    }
  }

  private void incInt(Statistics statistics, String stat, int value) {
    assertFalse(statistics.isClosed());
    Map<String, Number> statValues = getOrCreateExpectedValueMap(statistics);
    statistics.incInt(stat, value);
    statValues.put(stat, statistics.getInt(stat));
    if (this.statisticTypes.get(statistics.getTextId()) == null) {
      this.statisticTypes.put(statistics.getTextId(), statistics.getType().getName());
    }
  }

  private Map<String, Number> getOrCreateExpectedValueMap(final Statistics statistics) {
    return this.allStatistics.computeIfAbsent(statistics.getTextId(), k -> new HashMap<>());
  }

  private void incLong(Statistics statistics, String stat, long value) {
    assertFalse(statistics.isClosed());
    Map<String, Number> statValues = getOrCreateExpectedValueMap(statistics);
    statistics.incLong(stat, value);
    statValues.put(stat, statistics.getLong(stat));
    if (this.statisticTypes.get(statistics.getTextId()) == null) {
      this.statisticTypes.put(statistics.getTextId(), statistics.getType().getName());
    }
  }
}
