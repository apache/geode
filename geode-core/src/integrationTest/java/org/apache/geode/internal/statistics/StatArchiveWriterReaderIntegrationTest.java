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
import static org.apache.geode.internal.statistics.TestStatArchiveWriter.WRITER_INITIAL_DATE_MILLIS;
import static org.apache.geode.internal.statistics.TestStatArchiveWriter.WRITER_PREVIOUS_TIMESTAMP_NANOS;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.statistics.StatArchiveReader.StatValue;
import org.apache.geode.test.junit.categories.StatisticsTest;

/**
 * Integration tests for {@link StatArchiveWriter} and {@link StatArchiveReader}.
 *
 * @since GemFire 7.0
 */
@Category({StatisticsTest.class})
@SuppressWarnings({"rawtypes", "unused"})
public class StatArchiveWriterReaderIntegrationTest {

  private static final Logger logger = LogManager.getLogger();

  private File dir;
  private Map<String, String> statisticTypes;
  private Map<String, Map<String, Number>> allStatistics;
  private String archiveFileName;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    statisticTypes = new HashMap<>();
    allStatistics = new HashMap<>();
    dir = temporaryFolder.getRoot();
    archiveFileName =
        dir.getAbsolutePath() + File.separator + testName.getMethodName() + ".gfs";
  }

  @After
  public void tearDown() throws Exception {
    statisticTypes = null;
    allStatistics = null;
    StatisticsTypeFactoryImpl.clear();
  }

  @Test
  public void testDoubleCounterOneSample() throws Exception {
    final TestStatisticsManager manager =
        new TestStatisticsManager(1, getUniqueName(), WRITER_INITIAL_DATE_MILLIS);

    final TestStatisticsSampler sampler = new TestStatisticsSampler(manager);
    final SampleCollector sampleCollector = new SampleCollector(sampler);

    final StatArchiveDescriptor archiveDescriptor =
        new StatArchiveDescriptor.Builder().setArchiveName(archiveFileName).setSystemId(1)
            .setSystemStartTime(WRITER_INITIAL_DATE_MILLIS)
            .setSystemDirectoryPath(testName.getMethodName())
            .setProductDescription(getClass().getSimpleName()).build();
    final StatArchiveWriter writer = new TestStatArchiveWriter(archiveDescriptor);
    sampleCollector.addSampleHandler(writer);

    final StatisticDescriptor[] statsST1 =
        new StatisticDescriptor[] {manager.createDoubleCounter("long_double_1", "d1", "u1")};

    final StatisticsType ST1 = manager.createType("ST1", "ST1", statsST1);
    final Statistics st1_1 = manager.createAtomicStatistics(ST1, "st1_1", 1);
    final double value = 32317.716467;
    incDouble(st1_1, "long_double_1", value);

    final long sampleIncNanos = NANOS_PER_MILLI * 1000;
    final long sampleTimeNanos = WRITER_PREVIOUS_TIMESTAMP_NANOS + sampleIncNanos;
    sampleCollector.sample(sampleTimeNanos);

    writer.close();

    final StatisticDescriptor[] sds = ST1.getStatistics();
    for (int i = 0; i < sds.length; i++) {
      assertEquals(value, st1_1.get(sds[i].getName()));
    }

    final StatArchiveReader reader =
        new StatArchiveReader(new File[] {new File(archiveFileName)}, null, false);

    // compare all resourceInst values against what was printed above

    final List resources = reader.getResourceInstList();
    assertNotNull(resources);
    assertEquals(1, resources.size());

    final StatArchiveReader.ResourceInst ri = (StatArchiveReader.ResourceInst) resources.get(0);
    assertNotNull(ri);
    final String statsName = ri.getName();
    assertNotNull(statsName);
    assertEquals("st1_1", statsName);
    assertEquals("ST1", ri.getType().getName());

    final StatValue[] statValues = ri.getStatValues();
    assertNotNull(statValues);
    assertEquals(1, statValues.length);

    final String statName = ri.getType().getStats()[0].getName();
    assertNotNull(statName);
    assertEquals("long_double_1", statName);
    assertEquals(statName, statValues[0].getDescriptor().getName());
    assertEquals(1, statValues[0].getSnapshotsSize());
    assertEquals(value, statValues[0].getSnapshotsMostRecent(), 0.01);

    final long[] timeStampsMillis = statValues[0].getRawAbsoluteTimeStamps();
    assertNotNull(timeStampsMillis);
    assertEquals(1, timeStampsMillis.length);

    final long initPreviousTimeStampMillis =
        NanoTimer.nanosToMillis(WRITER_PREVIOUS_TIMESTAMP_NANOS);
    final long timeStampMillis = NanoTimer.nanosToMillis(sampleTimeNanos);
    final long deltaMillis = timeStampMillis - initPreviousTimeStampMillis;
    assertEquals(NanoTimer.nanosToMillis(sampleIncNanos), deltaMillis);

    final long expectedTimeStampMillis = deltaMillis + WRITER_INITIAL_DATE_MILLIS;
    assertEquals(expectedTimeStampMillis, timeStampsMillis[0]);

    final double[] snapshots = statValues[0].getRawSnapshots();
    assertNotNull(snapshots);
    assertEquals(1, snapshots.length);
    assertEquals(value, snapshots[0], 0.01);
  }

  @Test
  public void testIntCounterOneSample() throws Exception {
    final TestStatisticsManager manager =
        new TestStatisticsManager(1, getUniqueName(), WRITER_INITIAL_DATE_MILLIS);

    final TestStatisticsSampler sampler = new TestStatisticsSampler(manager);
    final SampleCollector sampleCollector = new SampleCollector(sampler);

    final StatArchiveDescriptor archiveDescriptor =
        new StatArchiveDescriptor.Builder().setArchiveName(archiveFileName).setSystemId(1)
            .setSystemStartTime(WRITER_INITIAL_DATE_MILLIS)
            .setSystemDirectoryPath(testName.getMethodName())
            .setProductDescription(getClass().getSimpleName()).build();
    final StatArchiveWriter writer = new TestStatArchiveWriter(archiveDescriptor);
    sampleCollector.addSampleHandler(writer);

    final StatisticDescriptor[] statsST1 =
        new StatisticDescriptor[] {manager.createIntCounter("int_counter_1", "d1", "u1")};

    final StatisticsType ST1 = manager.createType("ST1", "ST1", statsST1);
    final Statistics st1_1 = manager.createAtomicStatistics(ST1, "st1_1", 1);
    final long value = 5;
    incInt(st1_1, "int_counter_1", (int) value);

    final long sampleIncNanos = NANOS_PER_MILLI * 1000;
    final long sampleTimeNanos = WRITER_PREVIOUS_TIMESTAMP_NANOS + sampleIncNanos;
    sampleCollector.sample(sampleTimeNanos);

    writer.close();

    final StatisticDescriptor[] sds = ST1.getStatistics();
    for (int i = 0; i < sds.length; i++) {
      assertEquals(value, st1_1.get(sds[i].getName()));
    }

    final StatArchiveReader reader =
        new StatArchiveReader(new File[] {new File(archiveFileName)}, null, false);

    // compare all resourceInst values against what was printed above

    final List resources = reader.getResourceInstList();
    assertNotNull(resources);
    assertEquals(1, resources.size());

    final StatArchiveReader.ResourceInst ri = (StatArchiveReader.ResourceInst) resources.get(0);
    assertNotNull(ri);
    final String statsName = ri.getName();
    assertNotNull(statsName);
    assertEquals("st1_1", statsName);
    assertEquals("ST1", ri.getType().getName());

    final StatValue[] statValues = ri.getStatValues();
    assertNotNull(statValues);
    assertEquals(1, statValues.length);

    final String statName = ri.getType().getStats()[0].getName();
    assertNotNull(statName);
    assertEquals("int_counter_1", statName);
    assertEquals(statName, statValues[0].getDescriptor().getName());
    assertEquals(1, statValues[0].getSnapshotsSize());
    assertEquals((double) value, statValues[0].getSnapshotsMostRecent(), 0.01);

    final long[] timeStampsMillis = statValues[0].getRawAbsoluteTimeStamps();
    assertNotNull(timeStampsMillis);
    assertEquals(1, timeStampsMillis.length);

    final long initPreviousTimeStampMillis =
        NanoTimer.nanosToMillis(WRITER_PREVIOUS_TIMESTAMP_NANOS);
    final long timeStampMillis = NanoTimer.nanosToMillis(sampleTimeNanos);
    final long deltaMillis = timeStampMillis - initPreviousTimeStampMillis;
    assertEquals(NanoTimer.nanosToMillis(sampleIncNanos), deltaMillis);

    final long expectedTimeStampMillis = deltaMillis + WRITER_INITIAL_DATE_MILLIS;
    assertEquals(expectedTimeStampMillis, timeStampsMillis[0]);

    final double[] snapshots = statValues[0].getRawSnapshots();
    assertNotNull(snapshots);
    assertEquals(1, snapshots.length);
    assertEquals((double) value, snapshots[0], 0.01);
  }

  @Test
  public void testIntGaugeOneSample() throws Exception {
    final TestStatisticsManager manager =
        new TestStatisticsManager(1, getUniqueName(), WRITER_INITIAL_DATE_MILLIS);

    final TestStatisticsSampler sampler = new TestStatisticsSampler(manager);
    final SampleCollector sampleCollector = new SampleCollector(sampler);

    final StatArchiveDescriptor archiveDescriptor =
        new StatArchiveDescriptor.Builder().setArchiveName(archiveFileName).setSystemId(1)
            .setSystemStartTime(WRITER_INITIAL_DATE_MILLIS)
            .setSystemDirectoryPath(testName.getMethodName())
            .setProductDescription(getClass().getSimpleName()).build();
    final StatArchiveWriter writer = new TestStatArchiveWriter(archiveDescriptor);
    sampleCollector.addSampleHandler(writer);

    final StatisticDescriptor[] statsST1 =
        new StatisticDescriptor[] {manager.createIntGauge("int_gauge_1", "d1", "u1")};

    final StatisticsType ST1 = manager.createType("ST1", "ST1", statsST1);
    final Statistics st1_1 = manager.createAtomicStatistics(ST1, "st1_1", 1);
    final long value = 5;
    incInt(st1_1, "int_gauge_1", (int) value);

    final long sampleIncNanos = NANOS_PER_MILLI * 1000;
    final long sampleTimeNanos = WRITER_PREVIOUS_TIMESTAMP_NANOS + sampleIncNanos;
    sampleCollector.sample(sampleTimeNanos);

    writer.close();

    final StatisticDescriptor[] sds = ST1.getStatistics();
    for (int i = 0; i < sds.length; i++) {
      assertEquals(value, st1_1.get(sds[i].getName()));
    }

    final StatArchiveReader reader =
        new StatArchiveReader(new File[] {new File(archiveFileName)}, null, false);

    // compare all resourceInst values against what was printed above

    final List resources = reader.getResourceInstList();
    assertNotNull(resources);
    assertEquals(1, resources.size());

    final StatArchiveReader.ResourceInst ri = (StatArchiveReader.ResourceInst) resources.get(0);
    assertNotNull(ri);
    final String statsName = ri.getName();
    assertNotNull(statsName);
    assertEquals("st1_1", statsName);
    assertEquals("ST1", ri.getType().getName());

    final StatValue[] statValues = ri.getStatValues();
    assertNotNull(statValues);
    assertEquals(1, statValues.length);

    final String statName = ri.getType().getStats()[0].getName();
    assertNotNull(statName);
    assertEquals("int_gauge_1", statName);
    assertEquals(statName, statValues[0].getDescriptor().getName());
    assertEquals(1, statValues[0].getSnapshotsSize());
    assertEquals((double) value, statValues[0].getSnapshotsMostRecent(), 0.01);

    final long[] timeStampsMillis = statValues[0].getRawAbsoluteTimeStamps();
    assertNotNull(timeStampsMillis);
    assertEquals(1, timeStampsMillis.length);

    final long initialPreviousTimeStampMillis =
        NanoTimer.nanosToMillis(WRITER_PREVIOUS_TIMESTAMP_NANOS);
    final long timeStampMillis = NanoTimer.nanosToMillis(sampleTimeNanos);
    final long deltaMillis = timeStampMillis - initialPreviousTimeStampMillis;
    assertEquals(NanoTimer.nanosToMillis(sampleIncNanos), deltaMillis);

    final long expectedTimeStampMillis = deltaMillis + WRITER_INITIAL_DATE_MILLIS;
    assertEquals(expectedTimeStampMillis, timeStampsMillis[0]);

    final double[] snapshots = statValues[0].getRawSnapshots();
    assertNotNull(snapshots);
    assertEquals(1, snapshots.length);
    assertEquals((double) value, snapshots[0], 0.01);
  }

  @Test
  public void testLongCounterOneSample() throws Exception {
    final TestStatisticsManager manager =
        new TestStatisticsManager(1, getUniqueName(), WRITER_INITIAL_DATE_MILLIS);

    final TestStatisticsSampler sampler = new TestStatisticsSampler(manager);
    final SampleCollector sampleCollector = new SampleCollector(sampler);

    final StatArchiveDescriptor archiveDescriptor =
        new StatArchiveDescriptor.Builder().setArchiveName(archiveFileName).setSystemId(1)
            .setSystemStartTime(WRITER_INITIAL_DATE_MILLIS)
            .setSystemDirectoryPath(testName.getMethodName())
            .setProductDescription(getClass().getSimpleName()).build();
    final StatArchiveWriter writer = new TestStatArchiveWriter(archiveDescriptor);
    sampleCollector.addSampleHandler(writer);

    final StatisticDescriptor[] statsST1 =
        new StatisticDescriptor[] {manager.createLongCounter("long_counter_1", "d1", "u1")};

    final StatisticsType ST1 = manager.createType("ST1", "ST1", statsST1);
    final Statistics st1_1 = manager.createAtomicStatistics(ST1, "st1_1", 1);
    final long value = 5;
    incLong(st1_1, "long_counter_1", value);

    final long sampleIncNanos = NANOS_PER_MILLI * 1000;
    final long sampleTimeNanos = WRITER_PREVIOUS_TIMESTAMP_NANOS + sampleIncNanos;
    sampleCollector.sample(sampleTimeNanos);

    writer.close();

    final StatisticDescriptor[] sds = ST1.getStatistics();
    for (int i = 0; i < sds.length; i++) {
      assertEquals(value, st1_1.get(sds[i].getName()));
    }

    final StatArchiveReader reader =
        new StatArchiveReader(new File[] {new File(archiveFileName)}, null, false);

    // compare all resourceInst values against what was printed above

    final List resources = reader.getResourceInstList();
    assertNotNull(resources);
    assertEquals(1, resources.size());

    final StatArchiveReader.ResourceInst ri = (StatArchiveReader.ResourceInst) resources.get(0);
    assertNotNull(ri);
    final String statsName = ri.getName();
    assertNotNull(statsName);
    assertEquals("st1_1", statsName);
    assertEquals("ST1", ri.getType().getName());

    final StatValue[] statValues = ri.getStatValues();
    assertNotNull(statValues);
    assertEquals(1, statValues.length);

    final String statName = ri.getType().getStats()[0].getName();
    assertNotNull(statName);
    assertEquals("long_counter_1", statName);
    assertEquals(statName, statValues[0].getDescriptor().getName());
    assertEquals(1, statValues[0].getSnapshotsSize());
    assertEquals((double) value, statValues[0].getSnapshotsMostRecent(), 0.01);

    final long[] timeStampsMillis = statValues[0].getRawAbsoluteTimeStamps();
    assertNotNull(timeStampsMillis);
    assertEquals(1, timeStampsMillis.length);

    final long initialPreviousTimeStampMillis =
        NanoTimer.nanosToMillis(WRITER_PREVIOUS_TIMESTAMP_NANOS);
    final long timeStampMillis = NanoTimer.nanosToMillis(sampleTimeNanos);
    final long deltaMillis = timeStampMillis - initialPreviousTimeStampMillis;
    assertEquals(NanoTimer.nanosToMillis(sampleIncNanos), deltaMillis);

    final long expectedTimeStampMillis = deltaMillis + WRITER_INITIAL_DATE_MILLIS;
    assertEquals(expectedTimeStampMillis, timeStampsMillis[0]);

    final double[] snapshots = statValues[0].getRawSnapshots();
    assertNotNull(snapshots);
    assertEquals(1, snapshots.length);
    assertEquals((double) value, snapshots[0], 0.01);
  }

  @Test
  public void testLongGaugeOneSample() throws Exception {
    final TestStatisticsManager manager =
        new TestStatisticsManager(1, getUniqueName(), WRITER_INITIAL_DATE_MILLIS);

    final TestStatisticsSampler sampler = new TestStatisticsSampler(manager);
    final SampleCollector sampleCollector = new SampleCollector(sampler);

    final StatArchiveDescriptor archiveDescriptor =
        new StatArchiveDescriptor.Builder().setArchiveName(archiveFileName).setSystemId(1)
            .setSystemStartTime(WRITER_INITIAL_DATE_MILLIS)
            .setSystemDirectoryPath(testName.getMethodName())
            .setProductDescription(getClass().getSimpleName()).build();
    final StatArchiveWriter writer = new TestStatArchiveWriter(archiveDescriptor);
    sampleCollector.addSampleHandler(writer);

    final StatisticDescriptor[] statsST1 =
        new StatisticDescriptor[] {manager.createLongGauge("long_gauge_1", "d1", "u1")};

    final StatisticsType ST1 = manager.createType("ST1", "ST1", statsST1);
    final Statistics st1_1 = manager.createAtomicStatistics(ST1, "st1_1", 1);
    incLong(st1_1, "long_gauge_1", 5);

    final long sampleTimeNanos = WRITER_PREVIOUS_TIMESTAMP_NANOS + NANOS_PER_MILLI * 2;
    sampleCollector.sample(sampleTimeNanos);

    writer.close();

    final StatisticDescriptor[] sds = ST1.getStatistics();
    for (int i = 0; i < sds.length; i++) {
      assertEquals(5L, st1_1.get(sds[i].getName()));
    }

    final StatArchiveReader reader =
        new StatArchiveReader(new File[] {new File(archiveFileName)}, null, false);

    // compare all resourceInst values against what was printed above

    final List resources = reader.getResourceInstList();
    assertNotNull(resources);
    assertEquals(1, resources.size());

    final StatArchiveReader.ResourceInst ri = (StatArchiveReader.ResourceInst) resources.get(0);
    assertNotNull(ri);
    final String statsName = ri.getName();
    assertNotNull(statsName);
    assertEquals("st1_1", statsName);
    assertEquals("ST1", ri.getType().getName());

    final StatValue[] statValues = ri.getStatValues();
    assertNotNull(statValues);
    assertEquals(1, statValues.length);

    final String statName = ri.getType().getStats()[0].getName();
    assertNotNull(statName);
    assertEquals("long_gauge_1", statName);
    assertEquals(statName, statValues[0].getDescriptor().getName());
    assertEquals(1, statValues[0].getSnapshotsSize());
    assertEquals(5.0, statValues[0].getSnapshotsMostRecent(), 0.01);

    final long[] timeStampsMillis = statValues[0].getRawAbsoluteTimeStamps();
    assertNotNull(timeStampsMillis);
    assertEquals(1, timeStampsMillis.length);

    final long initialPreviousTimeStampMillis =
        NanoTimer.nanosToMillis(WRITER_PREVIOUS_TIMESTAMP_NANOS);
    final long timeStampMillis = NanoTimer.nanosToMillis(sampleTimeNanos);
    final long deltaMillis = timeStampMillis - initialPreviousTimeStampMillis;
    assertEquals(2, deltaMillis);

    final long expectedTimeStampMillis = deltaMillis + WRITER_INITIAL_DATE_MILLIS;
    assertEquals(expectedTimeStampMillis, timeStampsMillis[0]);

    final double[] snapshots = statValues[0].getRawSnapshots();
    assertNotNull(snapshots);
    assertEquals(1, snapshots.length);
    assertEquals(5.0, snapshots[0], 0.01);
  }

  @Test
  public void testLongCounterTwoSamples() throws Exception {
    final TestStatisticsManager manager =
        new TestStatisticsManager(1, getUniqueName(), WRITER_INITIAL_DATE_MILLIS);

    final TestStatisticsSampler sampler = new TestStatisticsSampler(manager);
    final SampleCollector sampleCollector = new SampleCollector(sampler);

    final StatArchiveDescriptor archiveDescriptor =
        new StatArchiveDescriptor.Builder().setArchiveName(archiveFileName).setSystemId(1)
            .setSystemStartTime(WRITER_INITIAL_DATE_MILLIS)
            .setSystemDirectoryPath(testName.getMethodName())
            .setProductDescription(getClass().getSimpleName()).build();
    final StatArchiveWriter writer = new TestStatArchiveWriter(archiveDescriptor);
    sampleCollector.addSampleHandler(writer);

    final StatisticDescriptor[] statsST1 =
        new StatisticDescriptor[] {manager.createLongCounter("long_counter_1", "d1", "u1")};

    final StatisticsType ST1 = manager.createType("ST1", "ST1", statsST1);
    final Statistics st1_1 = manager.createAtomicStatistics(ST1, "st1_1", 1);
    final long value1 = 5;
    incLong(st1_1, "long_counter_1", value1);

    final long sampleIncNanos = NANOS_PER_MILLI * 1000;
    long sampleTimeNanos = WRITER_PREVIOUS_TIMESTAMP_NANOS + sampleIncNanos;
    sampleCollector.sample(sampleTimeNanos);

    final long value2 = 15;
    incLong(st1_1, "long_counter_1", value2);
    sampleTimeNanos += sampleIncNanos;
    sampleCollector.sample(sampleTimeNanos);

    writer.close();

    final StatisticDescriptor[] sds = ST1.getStatistics();
    for (int i = 0; i < sds.length; i++) {
      assertEquals(value1 + value2, st1_1.get(sds[i].getName()));
    }

    final StatArchiveReader reader =
        new StatArchiveReader(new File[] {new File(archiveFileName)}, null, false);

    // compare all resourceInst values against what was printed above

    final List resources = reader.getResourceInstList();
    assertNotNull(resources);
    assertEquals(1, resources.size());

    final StatArchiveReader.ResourceInst ri = (StatArchiveReader.ResourceInst) resources.get(0);
    assertNotNull(ri);
    final String statsName = ri.getName();
    assertNotNull(statsName);
    assertEquals("st1_1", statsName);
    assertEquals("ST1", ri.getType().getName());

    final StatValue[] statValues = ri.getStatValues();
    assertNotNull(statValues);
    assertEquals(1, statValues.length);

    statValues[0].setFilter(StatValue.FILTER_NONE);

    final String statName = ri.getType().getStats()[0].getName();
    assertNotNull(statName);
    assertEquals("long_counter_1", statName);
    assertEquals(statName, statValues[0].getDescriptor().getName());
    assertEquals(2, statValues[0].getSnapshotsSize());
    assertEquals((double) (value1 + value2), statValues[0].getSnapshotsMostRecent(), 0.01);

    final long[] timeStampsMillis = statValues[0].getRawAbsoluteTimeStamps();
    assertNotNull(timeStampsMillis);
    assertEquals(2, timeStampsMillis.length);

    final long initialPreviousTimeStampMillis =
        NanoTimer.nanosToMillis(WRITER_PREVIOUS_TIMESTAMP_NANOS);
    final long timeStampMillis = NanoTimer.nanosToMillis(sampleTimeNanos);
    final long deltaMillis = timeStampMillis - initialPreviousTimeStampMillis;
    assertEquals(NanoTimer.nanosToMillis(sampleIncNanos * 2), deltaMillis);

    long expectedTimeStampMillis = WRITER_INITIAL_DATE_MILLIS;
    for (int i = 0; i < timeStampsMillis.length; i++) {
      expectedTimeStampMillis += 1000;
      assertEquals("expectedTimeStampMillis for " + i + " is wrong", expectedTimeStampMillis,
          timeStampsMillis[i]);
    }

    final double[] snapshots = statValues[0].getRawSnapshots();
    assertNotNull(snapshots);
    assertEquals(2, snapshots.length);
    assertEquals((double) value1, snapshots[0], 0.01);
    assertEquals((double) (value1 + value2), snapshots[1], 0.01);
  }

  /**
   * Tests the stat archive file written by StatArchiveWriter.
   */
  @Test
  public void testWriteAfterSamplingBegins() throws Exception {
    final TestStatisticsManager manager =
        new TestStatisticsManager(1, getUniqueName(), WRITER_INITIAL_DATE_MILLIS);

    final TestStatisticsSampler sampler = new TestStatisticsSampler(manager);
    final SampleCollector sampleCollector = new SampleCollector(sampler);

    final StatArchiveDescriptor archiveDescriptor =
        new StatArchiveDescriptor.Builder().setArchiveName(archiveFileName).setSystemId(1)
            .setSystemStartTime(WRITER_INITIAL_DATE_MILLIS - 2000)
            .setSystemDirectoryPath(testName.getMethodName())
            .setProductDescription(getClass().getSimpleName()).build();
    final StatArchiveWriter writer = new TestStatArchiveWriter(archiveDescriptor);
    sampleCollector.addSampleHandler(writer);

    long sampleTimeNanos = WRITER_PREVIOUS_TIMESTAMP_NANOS + NANOS_PER_MILLI * 1000;
    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 1) create ST1 and st1_1

    final StatisticDescriptor[] statsST1 =
        new StatisticDescriptor[] {manager.createDoubleCounter("double_counter_1", "d1", "u1"),
            manager.createDoubleCounter("double_counter_2", "d2", "u2", true),
            manager.createDoubleGauge("double_gauge_3", "d3", "u3"),
            manager.createDoubleGauge("double_gauge_4", "d4", "u4", false),
            manager.createIntCounter("int_counter_5", "d5", "u5"),
            manager.createIntCounter("int_counter_6", "d6", "u6", true),
            manager.createIntGauge("int_gauge_7", "d7", "u7"),
            manager.createIntGauge("int_gauge_8", "d8", "u8", false),
            manager.createLongCounter("long_counter_9", "d9", "u9"),
            manager.createLongCounter("long_counter_10", "d10", "u10", true),
            manager.createLongGauge("long_gauge_11", "d11", "u11"),
            manager.createLongGauge("long_gauge_12", "d12", "u12", false)};
    final StatisticsType ST1 = manager.createType("ST1", "ST1", statsST1);
    final Statistics st1_1 = manager.createAtomicStatistics(ST1, "st1_1", 1);

    // 2) create st1_2

    final Statistics st1_2 = manager.createAtomicStatistics(ST1, "st1_2", 2);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 3) some new values

    incDouble(st1_1, "double_counter_1", 18347.94880);
    incDouble(st1_1, "double_gauge_4", 24885.02346);
    incInt(st1_1, "int_counter_5", 3);
    incInt(st1_1, "int_gauge_8", 4);
    incLong(st1_1, "long_counter_9", 1073741824);
    incLong(st1_1, "long_gauge_12", 154367897);

    incDouble(st1_2, "double_counter_2", 346.95);
    incDouble(st1_2, "double_gauge_3", 9865.23008);
    incInt(st1_2, "int_counter_6", 4);
    incInt(st1_2, "int_gauge_7", 3);
    incLong(st1_2, "long_counter_10", 3497536);
    incLong(st1_2, "long_gauge_11", 103909646);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 4) all new values

    incDouble(st1_1, "double_counter_1", 1.098367);
    incDouble(st1_1, "double_counter_2", 50247.0983254);
    incDouble(st1_1, "double_gauge_3", 987654.2344);
    incDouble(st1_1, "double_gauge_4", 23.097);
    incInt(st1_1, "int_counter_5", 3);
    incInt(st1_1, "int_counter_6", 4);
    incInt(st1_1, "int_gauge_7", 3);
    incInt(st1_1, "int_gauge_8", 4);
    incLong(st1_1, "long_counter_9", 5);
    incLong(st1_1, "long_counter_10", 465793);
    incLong(st1_1, "long_gauge_11", -203050);
    incLong(st1_1, "long_gauge_12", 6);

    incDouble(st1_2, "double_counter_1", 0.000846643);
    incDouble(st1_2, "double_counter_2", 4.0);
    incDouble(st1_2, "double_gauge_3", -4.0);
    incDouble(st1_2, "double_gauge_4", 19276.0346624);
    incInt(st1_2, "int_counter_5", 1);
    incInt(st1_2, "int_counter_6", 2);
    incInt(st1_2, "int_gauge_7", -1);
    incInt(st1_2, "int_gauge_8", -2);
    incLong(st1_2, "long_counter_9", 309876);
    incLong(st1_2, "long_counter_10", 4);
    incLong(st1_2, "long_gauge_11", -4);
    incLong(st1_2, "long_gauge_12", 1098764);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 5) no new values

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 6) some new values

    incDouble(st1_1, "double_counter_2", 10.255);
    incDouble(st1_1, "double_gauge_3", -4123.05);
    incInt(st1_1, "int_counter_6", 2);
    incInt(st1_1, "int_gauge_7", 3);
    incLong(st1_1, "long_counter_10", 4);
    incLong(st1_1, "long_gauge_11", -2);

    incDouble(st1_2, "double_counter_1", 5.00007634);
    incDouble(st1_2, "double_gauge_4", 16904.06524);
    incInt(st1_2, "int_counter_5", 4);
    incInt(st1_2, "int_gauge_8", 1);
    incLong(st1_2, "long_counter_9", 8);
    incLong(st1_2, "long_gauge_12", 10);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 7) all new values

    incDouble(st1_1, "double_counter_1", 4065.340);
    incDouble(st1_1, "double_counter_2", 2.01342568);
    incDouble(st1_1, "double_gauge_3", 1.367890);
    incDouble(st1_1, "double_gauge_4", 8.0549003);
    incInt(st1_1, "int_counter_5", 2);
    incInt(st1_1, "int_counter_6", 9);
    incInt(st1_1, "int_gauge_7", 1);
    incInt(st1_1, "int_gauge_8", 2);
    incLong(st1_1, "long_counter_9", 6);
    incLong(st1_1, "long_counter_10", 2);
    incLong(st1_1, "long_gauge_11", -10);
    incLong(st1_1, "long_gauge_12", 8);

    incDouble(st1_2, "double_counter_1", 128.2450);
    incDouble(st1_2, "double_counter_2", 113.550);
    incDouble(st1_2, "double_gauge_3", 21.0676);
    incDouble(st1_2, "double_gauge_4", 2.01346);
    incInt(st1_2, "int_counter_5", 3);
    incInt(st1_2, "int_counter_6", 4);
    incInt(st1_2, "int_gauge_7", 4);
    incInt(st1_2, "int_gauge_8", 2);
    incLong(st1_2, "long_counter_9", 1);
    incLong(st1_2, "long_counter_10", 2);
    incLong(st1_2, "long_gauge_11", 3);
    incLong(st1_2, "long_gauge_12", -2);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 8) create ST2 and ST3 and st2_1 and st3_1 and st3_2

    final StatisticDescriptor[] statsST2 =
        new StatisticDescriptor[] {manager.createIntGauge("int_gauge_7", "d7", "u7"),
            manager.createIntGauge("int_gauge_8", "d8", "u8", false),
            manager.createLongCounter("long_counter_9", "d9", "u9"),
            manager.createLongCounter("long_counter_10", "d10", "u10", true),
            manager.createLongGauge("long_gauge_11", "d11", "u11"),
            manager.createLongGauge("long_gauge_12", "d12", "u12", false)};
    final StatisticsType ST2 = manager.createType("ST2", "ST2", statsST2);
    final Statistics st2_1 = manager.createAtomicStatistics(ST2, "st2_1", 1);

    final StatisticDescriptor[] statsST3 =
        new StatisticDescriptor[] {manager.createDoubleCounter("double_counter_1", "d1", "u1"),
            manager.createDoubleCounter("double_counter_2", "d2", "u2", true),
            manager.createDoubleGauge("double_gauge_3", "d3", "u3"),
            manager.createDoubleGauge("double_gauge_4", "d4", "u4", false),
            manager.createIntCounter("int_counter_5", "d5", "u5"),
            manager.createIntCounter("int_counter_6", "d6", "u6", true),};
    final StatisticsType ST3 = manager.createType("ST3", "ST3", statsST3);
    final Statistics st3_1 = manager.createAtomicStatistics(ST3, "st3_1", 1);
    final Statistics st3_2 = manager.createAtomicStatistics(ST3, "st3_2", 2);

    // 9) all new values

    incDouble(st1_1, "double_counter_1", 9499.10);
    incDouble(st1_1, "double_counter_2", 83.0);
    incDouble(st1_1, "double_gauge_3", -7.05678);
    incDouble(st1_1, "double_gauge_4", 5111.031);
    incInt(st1_1, "int_counter_5", 1);
    incInt(st1_1, "int_counter_6", 3);
    incInt(st1_1, "int_gauge_7", 9);
    incInt(st1_1, "int_gauge_8", -3);
    incLong(st1_1, "long_counter_9", 3);
    incLong(st1_1, "long_counter_10", 8);
    incLong(st1_1, "long_gauge_11", 5);
    incLong(st1_1, "long_gauge_12", 4);

    incDouble(st1_2, "double_counter_1", 2509.0235);
    incDouble(st1_2, "double_counter_2", 409.10063);
    incDouble(st1_2, "double_gauge_3", -42.66904);
    incDouble(st1_2, "double_gauge_4", 21.0098);
    incInt(st1_2, "int_counter_5", 8);
    incInt(st1_2, "int_counter_6", 9);
    incInt(st1_2, "int_gauge_7", -2);
    incInt(st1_2, "int_gauge_8", 6);
    incLong(st1_2, "long_counter_9", 4);
    incLong(st1_2, "long_counter_10", 5);
    incLong(st1_2, "long_gauge_11", 5);
    incLong(st1_2, "long_gauge_12", -1);

    incInt(st2_1, "int_gauge_7", 2);
    incInt(st2_1, "int_gauge_8", -1);
    incLong(st2_1, "long_counter_9", 1002948);
    incLong(st2_1, "long_counter_10", 29038856);
    incLong(st2_1, "long_gauge_11", -2947465);
    incLong(st2_1, "long_gauge_12", 4934745);

    incDouble(st3_1, "double_counter_1", 562.0458);
    incDouble(st3_1, "double_counter_2", 14.0086);
    incDouble(st3_1, "double_gauge_3", -2.0);
    incDouble(st3_1, "double_gauge_4", 1.0);
    incInt(st3_1, "int_counter_5", 2);
    incInt(st3_1, "int_counter_6", 1);

    incDouble(st3_2, "double_counter_1", 33.087);
    incDouble(st3_2, "double_counter_2", 2.02);
    incDouble(st3_2, "double_gauge_3", 1.06);
    incDouble(st3_2, "double_gauge_4", 3.021);
    incInt(st3_2, "int_counter_5", 1);
    incInt(st3_2, "int_counter_6", 4);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 10) some new values

    incDouble(st1_1, "double_counter_1", 3.014);
    incDouble(st1_1, "double_gauge_3", 57.003);
    incInt(st1_1, "int_counter_5", 3);
    incInt(st1_1, "int_gauge_7", 5);
    incLong(st1_1, "long_counter_9", 1);
    incLong(st1_1, "long_gauge_11", 1);

    incDouble(st1_2, "double_counter_2", 20.107);
    incDouble(st1_2, "double_gauge_4", 1.5078);
    incInt(st1_2, "int_counter_6", 1);
    incInt(st1_2, "int_gauge_8", -1);
    incLong(st1_2, "long_counter_10", 1073741824);
    incLong(st1_2, "long_gauge_12", 5);

    incInt(st2_1, "int_gauge_7", 2);
    incLong(st2_1, "long_counter_9", 2);
    incLong(st2_1, "long_gauge_11", -2);

    incDouble(st3_1, "double_counter_1", 24.80097);
    incDouble(st3_1, "double_gauge_3", -22.09834);
    incInt(st3_1, "int_counter_5", 2);

    incDouble(st3_2, "double_counter_2", 21.0483);
    incDouble(st3_2, "double_gauge_4", 36310.012);
    incInt(st3_2, "int_counter_6", 4);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 11) remove ST2 and st2_1

    manager.destroyStatistics(st2_1);

    // 12) some new values

    incDouble(st1_1, "double_counter_1", 339.0803);
    incDouble(st1_1, "double_counter_2", 21.06);
    incDouble(st1_1, "double_gauge_3", 12.056);
    incDouble(st1_1, "double_gauge_4", 27.108);
    incInt(st1_1, "int_counter_5", 2);
    incInt(st1_1, "int_counter_6", 4);

    incInt(st1_2, "int_gauge_7", 4);
    incInt(st1_2, "int_gauge_8", 7);
    incLong(st1_2, "long_counter_9", 8);
    incLong(st1_2, "long_counter_10", 4);
    incLong(st1_2, "long_gauge_11", 2);
    incLong(st1_2, "long_gauge_12", 1);

    incDouble(st3_1, "double_counter_1", 41.103);
    incDouble(st3_1, "double_counter_2", 2.0333);
    incDouble(st3_1, "double_gauge_3", -14.0);

    incDouble(st3_2, "double_gauge_4", 26.01);
    incInt(st3_2, "int_counter_5", 3);
    incInt(st3_2, "int_counter_6", 1);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 13) no new values

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 14) remove st1_2

    manager.destroyStatistics(st1_2);

    // 15) all new values

    incDouble(st1_1, "double_counter_1", 62.1350);
    incDouble(st1_1, "double_counter_2", 33.306);
    incDouble(st1_1, "double_gauge_3", 41.1340);
    incDouble(st1_1, "double_gauge_4", -1.04321);
    incInt(st1_1, "int_counter_5", 2);
    incInt(st1_1, "int_counter_6", 2);
    incInt(st1_1, "int_gauge_7", 1);
    incInt(st1_1, "int_gauge_8", 9);
    incLong(st1_1, "long_counter_9", 2);
    incLong(st1_1, "long_counter_10", 5);
    incLong(st1_1, "long_gauge_11", 3);
    incLong(st1_1, "long_gauge_12", -2);

    incDouble(st3_1, "double_counter_1", 3461.0153);
    incDouble(st3_1, "double_counter_2", 5.03167);
    incDouble(st3_1, "double_gauge_3", -1.31051);
    incDouble(st3_1, "double_gauge_4", 71.031);
    incInt(st3_1, "int_counter_5", 4);
    incInt(st3_1, "int_counter_6", 2);

    incDouble(st3_2, "double_counter_1", 531.5608);
    incDouble(st3_2, "double_counter_2", 55.0532);
    incDouble(st3_2, "double_gauge_3", 8.40956);
    incDouble(st3_2, "double_gauge_4", 23230.0462);
    incInt(st3_2, "int_counter_5", 9);
    incInt(st3_2, "int_counter_6", 5);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // close the writer

    writer.close();

    // print out all the stat values

    if (false) {
      StatisticDescriptor[] sds = ST1.getStatistics();
      for (int i = 0; i < sds.length; i++) {
        logger.info("testWriteAfterSamplingBegins#st1_1#" + sds[i].getName() + "="
            + st1_1.get(sds[i].getName()));
      }
      for (int i = 0; i < sds.length; i++) {
        logger.info("testWriteAfterSamplingBegins#st1_2#" + sds[i].getName() + "="
            + st1_2.get(sds[i].getName()));
      }

      sds = ST2.getStatistics();
      for (int i = 0; i < sds.length; i++) {
        logger.info("testWriteAfterSamplingBegins#st2_1#" + sds[i].getName() + "="
            + st2_1.get(sds[i].getName()));
      }

      sds = ST3.getStatistics();
      for (int i = 0; i < sds.length; i++) {
        logger.info("testWriteAfterSamplingBegins#st3_1#" + sds[i].getName() + "="
            + st3_1.get(sds[i].getName()));
      }
      for (int i = 0; i < sds.length; i++) {
        logger.info("testWriteAfterSamplingBegins#st3_2#" + sds[i].getName() + "="
            + st3_2.get(sds[i].getName()));
      }
    }

    // validate that stat archive file exists

    final File actual = new File(archiveFileName);
    assertTrue(actual.exists());

    // validate content of stat archive file using StatArchiveReader

    final StatArchiveReader reader = new StatArchiveReader(new File[] {actual}, null, false);

    // compare all resourceInst values against what was printed above

    final List resources = reader.getResourceInstList();
    for (final Iterator iter = resources.iterator(); iter.hasNext();) {
      final StatArchiveReader.ResourceInst ri = (StatArchiveReader.ResourceInst) iter.next();
      final String resourceName = ri.getName();
      assertNotNull(resourceName);

      final String expectedStatsType = statisticTypes.get(resourceName);
      assertNotNull(expectedStatsType);
      assertEquals(expectedStatsType, ri.getType().getName());

      final Map<String, Number> expectedStatValues = allStatistics.get(resourceName);
      assertNotNull(expectedStatValues);

      final StatValue[] statValues = ri.getStatValues();
      for (int i = 0; i < statValues.length; i++) {
        final String statName = ri.getType().getStats()[i].getName();
        assertNotNull(statName);
        assertNotNull(expectedStatValues.get(statName));

        assertEquals(statName, statValues[i].getDescriptor().getName());

        statValues[i].setFilter(StatValue.FILTER_NONE);
        final double[] rawSnapshots = statValues[i].getRawSnapshots();
        // for (int j = 0; j < rawSnapshots.length; j++) {
        // logger.info("DEBUG " + ri.getName() + " " + statName + " rawSnapshots[" + j + "] = " +
        // rawSnapshots[j]);
        // }
        assertEquals("Value " + i + " for " + statName + " is wrong: " + expectedStatValues,
            expectedStatValues.get(statName).doubleValue(), statValues[i].getSnapshotsMostRecent(),
            0.01);
      }
    }

    // validate byte content of stat archive file against saved expected file

    final File expected = new File(createTempFileFromResource(getClass(),
        "StatArchiveWriterReaderJUnitTest_" + testName.getMethodName() + "_expected.gfs")
            .getAbsolutePath());
    assertTrue(expected + " does not exist!", expected.exists());
    assertEquals(expected.length(), actual.length());

    assertTrue("Actual stat archive file: " + actual.getAbsolutePath()
        + " bytes differ from expected stat archive file bytes!",
        Arrays.equals(readBytes(expected), readBytes(actual)));
  }

  /**
   * Tests the stat archive file written by StatArchiveWriter.
   */
  @Test
  public void testWriteWhenSamplingBegins() throws Exception {
    final TestStatisticsManager manager =
        new TestStatisticsManager(1, getUniqueName(), WRITER_INITIAL_DATE_MILLIS);

    final TestStatisticsSampler sampler = new TestStatisticsSampler(manager);
    final SampleCollector sampleCollector = new SampleCollector(sampler);

    final StatArchiveDescriptor archiveDescriptor =
        new StatArchiveDescriptor.Builder().setArchiveName(archiveFileName).setSystemId(1)
            .setSystemStartTime(WRITER_INITIAL_DATE_MILLIS - 2000)
            .setSystemDirectoryPath(testName.getMethodName())
            .setProductDescription(getClass().getSimpleName()).build();
    final StatArchiveWriter writer = new TestStatArchiveWriter(archiveDescriptor);
    sampleCollector.addSampleHandler(writer);

    long sampleTimeNanos = WRITER_PREVIOUS_TIMESTAMP_NANOS + NANOS_PER_MILLI * 1000;

    // 1) create ST1 and st1_1

    final StatisticDescriptor[] statsST1 =
        new StatisticDescriptor[] {manager.createDoubleCounter("double_counter_1", "d1", "u1"),
            manager.createDoubleCounter("double_counter_2", "d2", "u2", true),
            manager.createDoubleGauge("double_gauge_3", "d3", "u3"),
            manager.createDoubleGauge("double_gauge_4", "d4", "u4", false),
            manager.createIntCounter("int_counter_5", "d5", "u5"),
            manager.createIntCounter("int_counter_6", "d6", "u6", true),
            manager.createIntGauge("int_gauge_7", "d7", "u7"),
            manager.createIntGauge("int_gauge_8", "d8", "u8", false),
            manager.createLongCounter("long_counter_9", "d9", "u9"),
            manager.createLongCounter("long_counter_10", "d10", "u10", true),
            manager.createLongGauge("long_gauge_11", "d11", "u11"),
            manager.createLongGauge("long_gauge_12", "d12", "u12", false)};
    final StatisticsType ST1 = manager.createType("ST1", "ST1", statsST1);
    final Statistics st1_1 = manager.createAtomicStatistics(ST1, "st1_1", 1);

    // 2) create st1_2

    final Statistics st1_2 = manager.createAtomicStatistics(ST1, "st1_2", 2);

    // 3) some new values

    incDouble(st1_1, "double_counter_1", 18347.94880);
    incDouble(st1_1, "double_gauge_4", 24885.02346);
    incInt(st1_1, "int_counter_5", 3);
    incInt(st1_1, "int_gauge_8", 4);
    incLong(st1_1, "long_counter_9", 1073741824);
    incLong(st1_1, "long_gauge_12", 154367897);

    incDouble(st1_2, "double_counter_2", 346.95);
    incDouble(st1_2, "double_gauge_3", 9865.23008);
    incInt(st1_2, "int_counter_6", 4);
    incInt(st1_2, "int_gauge_7", 3);
    incLong(st1_2, "long_counter_10", 3497536);
    incLong(st1_2, "long_gauge_11", 103909646);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 4) all new values

    incDouble(st1_1, "double_counter_1", 1.098367);
    incDouble(st1_1, "double_counter_2", 50247.0983254);
    incDouble(st1_1, "double_gauge_3", 987654.2344);
    incDouble(st1_1, "double_gauge_4", 23.097);
    incInt(st1_1, "int_counter_5", 3);
    incInt(st1_1, "int_counter_6", 4);
    incInt(st1_1, "int_gauge_7", 3);
    incInt(st1_1, "int_gauge_8", 4);
    incLong(st1_1, "long_counter_9", 5);
    incLong(st1_1, "long_counter_10", 465793);
    incLong(st1_1, "long_gauge_11", -203050);
    incLong(st1_1, "long_gauge_12", 6);

    incDouble(st1_2, "double_counter_1", 0.000846643);
    incDouble(st1_2, "double_counter_2", 4.0);
    incDouble(st1_2, "double_gauge_3", -4.0);
    incDouble(st1_2, "double_gauge_4", 19276.0346624);
    incInt(st1_2, "int_counter_5", 1);
    incInt(st1_2, "int_counter_6", 2);
    incInt(st1_2, "int_gauge_7", -1);
    incInt(st1_2, "int_gauge_8", -2);
    incLong(st1_2, "long_counter_9", 309876);
    incLong(st1_2, "long_counter_10", 4);
    incLong(st1_2, "long_gauge_11", -4);
    incLong(st1_2, "long_gauge_12", 1098764);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 5) no new values

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 6) some new values

    incDouble(st1_1, "double_counter_2", 10.255);
    incDouble(st1_1, "double_gauge_3", -4123.05);
    incInt(st1_1, "int_counter_6", 2);
    incInt(st1_1, "int_gauge_7", 3);
    incLong(st1_1, "long_counter_10", 4);
    incLong(st1_1, "long_gauge_11", -2);

    incDouble(st1_2, "double_counter_1", 5.00007634);
    incDouble(st1_2, "double_gauge_4", 16904.06524);
    incInt(st1_2, "int_counter_5", 4);
    incInt(st1_2, "int_gauge_8", 1);
    incLong(st1_2, "long_counter_9", 8);
    incLong(st1_2, "long_gauge_12", 10);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 7) all new values

    incDouble(st1_1, "double_counter_1", 4065.340);
    incDouble(st1_1, "double_counter_2", 2.01342568);
    incDouble(st1_1, "double_gauge_3", 1.367890);
    incDouble(st1_1, "double_gauge_4", 8.0549003);
    incInt(st1_1, "int_counter_5", 2);
    incInt(st1_1, "int_counter_6", 9);
    incInt(st1_1, "int_gauge_7", 1);
    incInt(st1_1, "int_gauge_8", 2);
    incLong(st1_1, "long_counter_9", 6);
    incLong(st1_1, "long_counter_10", 2);
    incLong(st1_1, "long_gauge_11", -10);
    incLong(st1_1, "long_gauge_12", 8);

    incDouble(st1_2, "double_counter_1", 128.2450);
    incDouble(st1_2, "double_counter_2", 113.550);
    incDouble(st1_2, "double_gauge_3", 21.0676);
    incDouble(st1_2, "double_gauge_4", 2.01346);
    incInt(st1_2, "int_counter_5", 3);
    incInt(st1_2, "int_counter_6", 4);
    incInt(st1_2, "int_gauge_7", 4);
    incInt(st1_2, "int_gauge_8", 2);
    incLong(st1_2, "long_counter_9", 1);
    incLong(st1_2, "long_counter_10", 2);
    incLong(st1_2, "long_gauge_11", 3);
    incLong(st1_2, "long_gauge_12", -2);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 8) create ST2 and ST3 and st2_1 and st3_1 and st3_2

    final StatisticDescriptor[] statsST2 =
        new StatisticDescriptor[] {manager.createIntGauge("int_gauge_7", "d7", "u7"),
            manager.createIntGauge("int_gauge_8", "d8", "u8", false),
            manager.createLongCounter("long_counter_9", "d9", "u9"),
            manager.createLongCounter("long_counter_10", "d10", "u10", true),
            manager.createLongGauge("long_gauge_11", "d11", "u11"),
            manager.createLongGauge("long_gauge_12", "d12", "u12", false)};
    final StatisticsType ST2 = manager.createType("ST2", "ST2", statsST2);
    final Statistics st2_1 = manager.createAtomicStatistics(ST2, "st2_1", 1);

    final StatisticDescriptor[] statsST3 =
        new StatisticDescriptor[] {manager.createDoubleCounter("double_counter_1", "d1", "u1"),
            manager.createDoubleCounter("double_counter_2", "d2", "u2", true),
            manager.createDoubleGauge("double_gauge_3", "d3", "u3"),
            manager.createDoubleGauge("double_gauge_4", "d4", "u4", false),
            manager.createIntCounter("int_counter_5", "d5", "u5"),
            manager.createIntCounter("int_counter_6", "d6", "u6", true),};
    final StatisticsType ST3 = manager.createType("ST3", "ST3", statsST3);
    final Statistics st3_1 = manager.createAtomicStatistics(ST3, "st3_1", 1);
    final Statistics st3_2 = manager.createAtomicStatistics(ST3, "st3_2", 2);

    // 9) all new values

    incDouble(st1_1, "double_counter_1", 9499.10);
    incDouble(st1_1, "double_counter_2", 83.0);
    incDouble(st1_1, "double_gauge_3", -7.05678);
    incDouble(st1_1, "double_gauge_4", 5111.031);
    incInt(st1_1, "int_counter_5", 1);
    incInt(st1_1, "int_counter_6", 3);
    incInt(st1_1, "int_gauge_7", 9);
    incInt(st1_1, "int_gauge_8", -3);
    incLong(st1_1, "long_counter_9", 3);
    incLong(st1_1, "long_counter_10", 8);
    incLong(st1_1, "long_gauge_11", 5);
    incLong(st1_1, "long_gauge_12", 4);

    incDouble(st1_2, "double_counter_1", 2509.0235);
    incDouble(st1_2, "double_counter_2", 409.10063);
    incDouble(st1_2, "double_gauge_3", -42.66904);
    incDouble(st1_2, "double_gauge_4", 21.0098);
    incInt(st1_2, "int_counter_5", 8);
    incInt(st1_2, "int_counter_6", 9);
    incInt(st1_2, "int_gauge_7", -2);
    incInt(st1_2, "int_gauge_8", 6);
    incLong(st1_2, "long_counter_9", 4);
    incLong(st1_2, "long_counter_10", 5);
    incLong(st1_2, "long_gauge_11", 5);
    incLong(st1_2, "long_gauge_12", -1);

    incInt(st2_1, "int_gauge_7", 2);
    incInt(st2_1, "int_gauge_8", -1);
    incLong(st2_1, "long_counter_9", 1002948);
    incLong(st2_1, "long_counter_10", 29038856);
    incLong(st2_1, "long_gauge_11", -2947465);
    incLong(st2_1, "long_gauge_12", 4934745);

    incDouble(st3_1, "double_counter_1", 562.0458);
    incDouble(st3_1, "double_counter_2", 14.0086);
    incDouble(st3_1, "double_gauge_3", -2.0);
    incDouble(st3_1, "double_gauge_4", 1.0);
    incInt(st3_1, "int_counter_5", 2);
    incInt(st3_1, "int_counter_6", 1);

    incDouble(st3_2, "double_counter_1", 33.087);
    incDouble(st3_2, "double_counter_2", 2.02);
    incDouble(st3_2, "double_gauge_3", 1.06);
    incDouble(st3_2, "double_gauge_4", 3.021);
    incInt(st3_2, "int_counter_5", 1);
    incInt(st3_2, "int_counter_6", 4);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 10) some new values

    incDouble(st1_1, "double_counter_1", 3.014);
    incDouble(st1_1, "double_gauge_3", 57.003);
    incInt(st1_1, "int_counter_5", 3);
    incInt(st1_1, "int_gauge_7", 5);
    incLong(st1_1, "long_counter_9", 1);
    incLong(st1_1, "long_gauge_11", 1);

    incDouble(st1_2, "double_counter_2", 20.107);
    incDouble(st1_2, "double_gauge_4", 1.5078);
    incInt(st1_2, "int_counter_6", 1);
    incInt(st1_2, "int_gauge_8", -1);
    incLong(st1_2, "long_counter_10", 1073741824);
    incLong(st1_2, "long_gauge_12", 5);

    incInt(st2_1, "int_gauge_7", 2);
    incLong(st2_1, "long_counter_9", 2);
    incLong(st2_1, "long_gauge_11", -2);

    incDouble(st3_1, "double_counter_1", 24.80097);
    incDouble(st3_1, "double_gauge_3", -22.09834);
    incInt(st3_1, "int_counter_5", 2);

    incDouble(st3_2, "double_counter_2", 21.0483);
    incDouble(st3_2, "double_gauge_4", 36310.012);
    incInt(st3_2, "int_counter_6", 4);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 11) remove ST2 and st2_1

    manager.destroyStatistics(st2_1);

    // 12) some new values

    incDouble(st1_1, "double_counter_1", 339.0803);
    incDouble(st1_1, "double_counter_2", 21.06);
    incDouble(st1_1, "double_gauge_3", 12.056);
    incDouble(st1_1, "double_gauge_4", 27.108);
    incInt(st1_1, "int_counter_5", 2);
    incInt(st1_1, "int_counter_6", 4);

    incInt(st1_2, "int_gauge_7", 4);
    incInt(st1_2, "int_gauge_8", 7);
    incLong(st1_2, "long_counter_9", 8);
    incLong(st1_2, "long_counter_10", 4);
    incLong(st1_2, "long_gauge_11", 2);
    incLong(st1_2, "long_gauge_12", 1);

    incDouble(st3_1, "double_counter_1", 41.103);
    incDouble(st3_1, "double_counter_2", 2.0333);
    incDouble(st3_1, "double_gauge_3", -14.0);

    incDouble(st3_2, "double_gauge_4", 26.01);
    incInt(st3_2, "int_counter_5", 3);
    incInt(st3_2, "int_counter_6", 1);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 13) no new values

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // 14) remove st1_2

    manager.destroyStatistics(st1_2);

    // 15) all new values

    incDouble(st1_1, "double_counter_1", 62.1350);
    incDouble(st1_1, "double_counter_2", 33.306);
    incDouble(st1_1, "double_gauge_3", 41.1340);
    incDouble(st1_1, "double_gauge_4", -1.04321);
    incInt(st1_1, "int_counter_5", 2);
    incInt(st1_1, "int_counter_6", 2);
    incInt(st1_1, "int_gauge_7", 1);
    incInt(st1_1, "int_gauge_8", 9);
    incLong(st1_1, "long_counter_9", 2);
    incLong(st1_1, "long_counter_10", 5);
    incLong(st1_1, "long_gauge_11", 3);
    incLong(st1_1, "long_gauge_12", -2);

    incDouble(st3_1, "double_counter_1", 3461.0153);
    incDouble(st3_1, "double_counter_2", 5.03167);
    incDouble(st3_1, "double_gauge_3", -1.31051);
    incDouble(st3_1, "double_gauge_4", 71.031);
    incInt(st3_1, "int_counter_5", 4);
    incInt(st3_1, "int_counter_6", 2);

    incDouble(st3_2, "double_counter_1", 531.5608);
    incDouble(st3_2, "double_counter_2", 55.0532);
    incDouble(st3_2, "double_gauge_3", 8.40956);
    incDouble(st3_2, "double_gauge_4", 23230.0462);
    incInt(st3_2, "int_counter_5", 9);
    incInt(st3_2, "int_counter_6", 5);

    sampleCollector.sample(sampleTimeNanos += (1000 * NANOS_PER_MILLI));

    // close the writer

    writer.close();

    // print out all the stat values

    if (false) {
      StatisticDescriptor[] sds = ST1.getStatistics();
      for (int i = 0; i < sds.length; i++) {
        logger.info("testWriteAfterSamplingBegins#st1_1#" + sds[i].getName() + "="
            + st1_1.get(sds[i].getName()));
      }
      for (int i = 0; i < sds.length; i++) {
        logger.info("testWriteAfterSamplingBegins#st1_2#" + sds[i].getName() + "="
            + st1_2.get(sds[i].getName()));
      }

      sds = ST2.getStatistics();
      for (int i = 0; i < sds.length; i++) {
        logger.info("testWriteAfterSamplingBegins#st2_1#" + sds[i].getName() + "="
            + st2_1.get(sds[i].getName()));
      }

      sds = ST3.getStatistics();
      for (int i = 0; i < sds.length; i++) {
        logger.info("testWriteAfterSamplingBegins#st3_1#" + sds[i].getName() + "="
            + st3_1.get(sds[i].getName()));
      }
      for (int i = 0; i < sds.length; i++) {
        logger.info("testWriteAfterSamplingBegins#st3_2#" + sds[i].getName() + "="
            + st3_2.get(sds[i].getName()));
      }
    }

    // validate that stat archive file exists

    final File actual = new File(archiveFileName);
    assertTrue(actual.exists());

    // validate content of stat archive file using StatArchiveReader

    final StatArchiveReader reader = new StatArchiveReader(new File[] {actual}, null, false);

    // compare all resourceInst values against what was printed above

    final List resources = reader.getResourceInstList();
    for (final Iterator iter = resources.iterator(); iter.hasNext();) {
      final StatArchiveReader.ResourceInst ri = (StatArchiveReader.ResourceInst) iter.next();
      final String resourceName = ri.getName();
      assertNotNull(resourceName);

      final String expectedStatsType = statisticTypes.get(resourceName);
      assertNotNull(expectedStatsType);
      assertEquals(expectedStatsType, ri.getType().getName());

      final Map<String, Number> expectedStatValues = allStatistics.get(resourceName);
      assertNotNull(expectedStatValues);

      final StatValue[] statValues = ri.getStatValues();
      for (int i = 0; i < statValues.length; i++) {
        final String statName = ri.getType().getStats()[i].getName();
        assertNotNull(statName);
        assertNotNull(expectedStatValues.get(statName));

        assertEquals(statName, statValues[i].getDescriptor().getName());

        statValues[i].setFilter(StatValue.FILTER_NONE);
        final double[] rawSnapshots = statValues[i].getRawSnapshots();
        // for (int j = 0; j < rawSnapshots.length; j++) {
        // logger.info("DEBUG " + ri.getName() + " " + statName + " rawSnapshots[" + j + "] = " +
        // rawSnapshots[j]);
        // }
        assertEquals("Value " + i + " for " + statName + " is wrong: " + expectedStatValues,
            expectedStatValues.get(statName).doubleValue(), statValues[i].getSnapshotsMostRecent(),
            0.01);
      }
    }

    // validate byte content of stat archive file against saved expected file

    final File expected = new File(createTempFileFromResource(getClass(),
        "StatArchiveWriterReaderJUnitTest_" + testName.getMethodName() + "_expected.gfs")
            .getAbsolutePath());
    assertTrue(expected + " does not exist!", expected.exists());
    assertEquals(expected.length(), actual.length());

    assertTrue("Actual stat archive file: " + actual.getAbsolutePath()
        + " bytes differ from expected stat archive file bytes!",
        Arrays.equals(readBytes(expected), readBytes(actual)));
  }

  /**
   * Validates that IllegalArgumentException is thrown if sample time stamp is before the previous
   * time stamp. Tests the fix for cause of bug #45268.
   */
  @Test
  public void testNegativeSampleTimeStamp() throws Exception {
    final StatArchiveDescriptor archiveDescriptor =
        new StatArchiveDescriptor.Builder().setArchiveName(archiveFileName).setSystemId(1)
            .setSystemStartTime(WRITER_INITIAL_DATE_MILLIS)
            .setSystemDirectoryPath(testName.getMethodName())
            .setProductDescription(getClass().getSimpleName()).build();
    final StatArchiveWriter writer = new TestStatArchiveWriter(archiveDescriptor);

    final long sampleIncNanos = NANOS_PER_MILLI * 1000;

    try {
      writer.sampled(WRITER_PREVIOUS_TIMESTAMP_NANOS - sampleIncNanos, Collections.emptyList());
      fail(
          "Expected IllegalArgumentException to be thrown from StatArchiveWriter#writeTimeStamp(long)");
    } catch (IllegalArgumentException expected) {
      // test passed
    } finally {
      writer.close();
    }
  }

  /**
   * Validates that IllegalArgumentException is thrown if sample time stamp is same as the previous
   * time stamp. Tests the fix for cause of bug #45268.
   */
  @Test
  public void testPreviousSampleTimeStamp() throws Exception {
    final StatArchiveDescriptor archiveDescriptor =
        new StatArchiveDescriptor.Builder().setArchiveName(archiveFileName).setSystemId(1)
            .setSystemStartTime(WRITER_INITIAL_DATE_MILLIS)
            .setSystemDirectoryPath(testName.getMethodName())
            .setProductDescription(getClass().getSimpleName()).build();
    final StatArchiveWriter writer = new TestStatArchiveWriter(archiveDescriptor);

    try {
      writer.sampled(WRITER_PREVIOUS_TIMESTAMP_NANOS, Collections.emptyList());
      fail(
          "Expected IllegalArgumentException to be thrown from StatArchiveWriter#writeTimeStamp(long)");
    } catch (IllegalArgumentException expected) {
      // test passed
    } finally {
      writer.close();
    }

    writer.close();
  }

  /**
   * Verifies fix for bug #45377.
   */
  @Test
  public void testDestroyClosedStatistics() throws Exception {
    final StatArchiveDescriptor archiveDescriptor =
        new StatArchiveDescriptor.Builder().setArchiveName(archiveFileName).setSystemId(1)
            .setSystemStartTime(WRITER_INITIAL_DATE_MILLIS - 2000)
            .setSystemDirectoryPath(testName.getMethodName())
            .setProductDescription(getClass().getSimpleName()).build();
    final StatArchiveWriter writer = new TestStatArchiveWriter(archiveDescriptor);

    final StatisticsType statsType = createDummyStatisticsType();
    final ResourceType rt = new ResourceType(0, statsType);
    final Statistics statistics = mock(Statistics.class);
    when(statistics.isClosed()).thenReturn(true);
    final ResourceInstance ri = new ResourceInstance(0, statistics, rt);

    // if bug #45377 still existed, this call would throw IllegalStateException
    writer.destroyedResourceInstance(ri);
  }

  /**
   * Control which helps verify fix for bug #45377.
   */
  @Test
  public void testDestroyUnallocatedStatistics() throws Exception {
    final StatArchiveDescriptor archiveDescriptor =
        new StatArchiveDescriptor.Builder().setArchiveName(archiveFileName).setSystemId(1)
            .setSystemStartTime(WRITER_INITIAL_DATE_MILLIS - 2000)
            .setSystemDirectoryPath(testName.getMethodName())
            .setProductDescription(getClass().getSimpleName()).build();
    final StatArchiveWriter writer = new TestStatArchiveWriter(archiveDescriptor);

    final StatisticsType statsType = createDummyStatisticsType();
    final ResourceType rt = new ResourceType(0, statsType);
    final Statistics statistics = mock(Statistics.class);
    final ResourceInstance ri = new ResourceInstance(0, statistics, rt);

    writer.sampled(WRITER_INITIAL_DATE_MILLIS + 1000, Collections.singletonList(ri));

    writer.destroyedResourceInstance(ri);
    writer.close();

    // Verify StatArchiveReader.update returns cleanly, without throwing an exception
    final StatArchiveReader reader =
        new StatArchiveReader(new File[] {new File(archiveFileName)}, null, false);
    reader.update();
    reader.close();
  }

  private void incDouble(Statistics statistics, String stat, double value) {
    assertFalse(statistics.isClosed());
    Map<String, Number> statValues = allStatistics.get(statistics.getTextId());
    if (statValues == null) {
      statValues = new HashMap<String, Number>();
      allStatistics.put(statistics.getTextId(), statValues);
    }
    statistics.incDouble(stat, value);
    statValues.put(stat, statistics.getDouble(stat));
    if (statisticTypes.get(statistics.getTextId()) == null) {
      statisticTypes.put(statistics.getTextId(), statistics.getType().getName());
    }
  }

  private void incInt(Statistics statistics, String stat, int value) {
    assertFalse(statistics.isClosed());
    Map<String, Number> statValues = allStatistics.get(statistics.getTextId());
    if (statValues == null) {
      statValues = new HashMap<String, Number>();
      allStatistics.put(statistics.getTextId(), statValues);
    }
    statistics.incInt(stat, value);
    statValues.put(stat, statistics.getInt(stat));
    if (statisticTypes.get(statistics.getTextId()) == null) {
      statisticTypes.put(statistics.getTextId(), statistics.getType().getName());
    }
  }

  private void incLong(Statistics statistics, String stat, long value) {
    assertFalse(statistics.isClosed());
    Map<String, Number> statValues = allStatistics.get(statistics.getTextId());
    if (statValues == null) {
      statValues = new HashMap<String, Number>();
      allStatistics.put(statistics.getTextId(), statValues);
    }
    statistics.incLong(stat, value);
    statValues.put(stat, statistics.getLong(stat));
    if (statisticTypes.get(statistics.getTextId()) == null) {
      statisticTypes.put(statistics.getTextId(), statistics.getType().getName());
    }
  }

  private byte[] readBytes(File file) throws IOException {
    int byteCount = (int) file.length();

    byte[] input = new byte[byteCount];

    URL url = file.toURL();
    assertNotNull(url);

    InputStream is = url.openStream();
    assertNotNull(is);

    BufferedInputStream bis = new BufferedInputStream(is);
    int bytesRead = bis.read(input);
    bis.close();

    assertEquals(byteCount, bytesRead);
    return input;
  }

  private String getUniqueName() {
    return getClass().getSimpleName() + "_" + testName.getMethodName();
  }

  private static StatisticsType createDummyStatisticsType() {
    return new StatisticsType() {

      @Override
      public String getName() {
        return null;
      }

      @Override
      public String getDescription() {
        return null;
      }

      @Override
      public StatisticDescriptor[] getStatistics() {
        return new StatisticDescriptor[0];
      }

      @Override
      public int nameToId(String name) {
        return 0;
      }

      @Override
      public StatisticDescriptor nameToDescriptor(String name) {
        return null;
      }
    };
  }

  /*
   * [KEEP] alternative method for getting an expected golden file: Class clazz = getClass();
   * assertNotNull(clazz); URL url =
   * clazz.getResource("StatArchiveWriterReaderIntegrationTest.gfs.expected"); assertNotNull(url);
   *
   * File expected; try { expected = new File(url.toURI()); } catch(URISyntaxException e) { expected
   * = new File(url.getPath()); } assertTrue(expected.exists());
   */
}
