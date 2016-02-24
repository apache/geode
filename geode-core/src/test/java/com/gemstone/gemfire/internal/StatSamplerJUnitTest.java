/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal;

import static org.junit.Assert.*;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.StatArchiveReader.StatValue;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.util.StopWatch;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * @since 7.0
 */
@Category(UnitTest.class)
public class StatSamplerJUnitTest {
  private static final Logger logger = LogService.getLogger();
  
  @Rule
  public TestName testName = new TestName();

  private Map<String,String> statisticTypes;
  private Map<String,Map<String,Number>> allStatistics;

  @Before
  public void setUp() {
    this.statisticTypes = new HashMap<String,String>();
    this.allStatistics = new HashMap<String,Map<String,Number>>();
  }
  
  @After
  public void tearDown() {
    System.clearProperty("stats.log-level");
    System.clearProperty("stats.disable");
    System.clearProperty("stats.name");
    System.clearProperty("stats.archive-file");
    System.clearProperty("stats.file-size-limit");
    System.clearProperty("stats.disk-space-limit");
    System.clearProperty("stats.sample-rate");
    this.statisticTypes = null;
    this.allStatistics = null;
    StatisticsTypeFactoryImpl.clear();
    StatArchiveWriter.clearTraceFilter();
  }
  
  @Test
  public void testStatSampler() throws Exception {
    StatArchiveWriter.setTraceFilter("st1_1", "ST1");
    
    System.setProperty("stats.log-level", "config");
    System.setProperty("stats.disable", "false");
    System.setProperty("stats.name", getClass().getSimpleName() + "_" + testName.getMethodName());
    System.setProperty("stats.archive-file", getClass().getSimpleName() + "_" + testName.getMethodName() + ".gfs");
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
    final StatisticDescriptor[] statsST1 = new StatisticDescriptor[] {
        factory.createDoubleCounter("double_counter_1", "d1",  "u1"),
        factory.createDoubleCounter("double_counter_2", "d2",  "u2",  true),
        factory.createDoubleGauge(  "double_gauge_3",   "d3",  "u3"),
        factory.createDoubleGauge(  "double_gauge_4",   "d4",  "u4",  false),
        factory.createIntCounter(   "int_counter_5",    "d5",  "u5"),
        factory.createIntCounter(   "int_counter_6",    "d6",  "u6",  true),
        factory.createIntGauge(     "int_gauge_7",      "d7",  "u7"),
        factory.createIntGauge(     "int_gauge_8",      "d8",  "u8",  false),
        factory.createLongCounter(  "long_counter_9",   "d9",  "u9"),
        factory.createLongCounter(  "long_counter_10",  "d10", "u10", true),
        factory.createLongGauge(    "long_gauge_11",    "d11", "u11"),
        factory.createLongGauge(    "long_gauge_12",    "d12", "u12", false)
    };
    final StatisticsType ST1 = factory.createType("ST1", "ST1", statsST1);
    final Statistics st1_1 = factory.createAtomicStatistics(ST1, "st1_1", 1);
    
    boolean done = false;
    try {
      Statistics[] samplerStatsInstances = factory.findStatisticsByTextId("statSampler");
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < 4000; done = (samplerStatsInstances != null && samplerStatsInstances.length > 0)) {
        Thread.sleep(10);
        samplerStatsInstances = factory.findStatisticsByTextId("statSampler");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue("Waiting for statSampler stats", done);

    Statistics[] samplerStatsInstances = factory.findStatisticsByTextId("statSampler");
    assertNotNull(samplerStatsInstances);
    assertEquals(1, samplerStatsInstances.length);
    final Statistics samplerStats = samplerStatsInstances[0];
    
    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_gauge_3",   3);
    incInt(st1_1,    "int_counter_5",    5);
    incInt(st1_1,    "int_gauge_7",      7);
    incLong(st1_1,   "long_counter_9",   9);
    incLong(st1_1,   "long_gauge_11",    11);
    
    waitForStatSample(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3",   1);
    incDouble(st1_1, "double_gauge_4",   1);
    incInt(st1_1,    "int_counter_5",    1);
    incInt(st1_1,    "int_counter_6",    1);
    
    waitForStatSample(samplerStats);
    
    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3",   1);
    incDouble(st1_1, "double_gauge_4",   1);
    incInt(st1_1,    "int_counter_5",    1);
    incInt(st1_1,    "int_counter_6",    1);
    incInt(st1_1,    "int_gauge_7",      1);
    incInt(st1_1,    "int_gauge_8",      1);
    incLong(st1_1,   "long_counter_9",   1);
    incLong(st1_1,   "long_counter_10",  1);
    incLong(st1_1,   "long_gauge_11",    1);
    incLong(st1_1,   "long_gauge_12",    1);
    
    waitForStatSample(samplerStats);
    
    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3",   -1);
    incDouble(st1_1, "double_gauge_4",   1);
    incInt(st1_1,    "int_counter_5",    1);
    incInt(st1_1,    "int_counter_6",    1);
    incInt(st1_1,    "int_gauge_7",      -1);
    incInt(st1_1,    "int_gauge_8",      1);
    incLong(st1_1,   "long_counter_9",   1);
    incLong(st1_1,   "long_counter_10",  1);
    incLong(st1_1,   "long_gauge_11",    -1);
    incLong(st1_1,   "long_gauge_12",    1);

    waitForStatSample(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3",   1);
    incDouble(st1_1, "double_gauge_4",   1);
    incInt(st1_1,    "int_counter_5",    1);
    incInt(st1_1,    "int_counter_6",    1);
    incInt(st1_1,    "int_gauge_7",      1);
    incInt(st1_1,    "int_gauge_8",      1);
    incLong(st1_1,   "long_counter_9",   1);
    incLong(st1_1,   "long_counter_10",  1);
    incLong(st1_1,   "long_gauge_11",    1);
    incLong(st1_1,   "long_gauge_12",    1);
    
    waitForStatSample(samplerStats);
    waitForStatSample(samplerStats);
    waitForStatSample(samplerStats);
    
    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_gauge_3",   3);
    incInt(st1_1,    "int_counter_5",    5);
    incInt(st1_1,    "int_gauge_7",      7);
    incLong(st1_1,   "long_counter_9",   9);
    incLong(st1_1,   "long_gauge_11",    11);
    
    waitForStatSample(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3",   1);
    incDouble(st1_1, "double_gauge_4",   1);
    incInt(st1_1,    "int_counter_5",    1);
    incInt(st1_1,    "int_counter_6",    1);
    
    waitForStatSample(samplerStats);
    
    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3",   1);
    incDouble(st1_1, "double_gauge_4",   1);
    incInt(st1_1,    "int_counter_5",    1);
    incInt(st1_1,    "int_counter_6",    1);
    incInt(st1_1,    "int_gauge_7",      1);
    incInt(st1_1,    "int_gauge_8",      1);
    incLong(st1_1,   "long_counter_9",   1);
    incLong(st1_1,   "long_counter_10",  1);
    incLong(st1_1,   "long_gauge_11",    1);
    incLong(st1_1,   "long_gauge_12",    1);
    
    waitForStatSample(samplerStats);
    
    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3",   -1);
    incDouble(st1_1, "double_gauge_4",   1);
    incInt(st1_1,    "int_counter_5",    1);
    incInt(st1_1,    "int_counter_6",    1);
    incInt(st1_1,    "int_gauge_7",      -1);
    incInt(st1_1,    "int_gauge_8",      1);
    incLong(st1_1,   "long_counter_9",   1);
    incLong(st1_1,   "long_counter_10",  1);
    incLong(st1_1,   "long_gauge_11",    -1);
    incLong(st1_1,   "long_gauge_12",    1);

    waitForStatSample(samplerStats);

    incDouble(st1_1, "double_counter_1", 1);
    incDouble(st1_1, "double_counter_2", 1);
    incDouble(st1_1, "double_gauge_3",   1);
    incDouble(st1_1, "double_gauge_4",   1);
    incInt(st1_1,    "int_counter_5",    1);
    incInt(st1_1,    "int_counter_6",    1);
    incInt(st1_1,    "int_gauge_7",      1);
    incInt(st1_1,    "int_gauge_8",      1);
    incLong(st1_1,   "long_counter_9",   1);
    incLong(st1_1,   "long_counter_10",  1);
    incLong(st1_1,   "long_gauge_11",    1);
    incLong(st1_1,   "long_gauge_12",    1);
    
    waitForStatSample(samplerStats);
    
    factory.close();
    
    final File archiveFile = new File(System.getProperty("stats.archive-file"));
    assertTrue(archiveFile.exists());
    final StatArchiveReader reader = new StatArchiveReader(
        new File[]{archiveFile}, null, false);

    @SuppressWarnings("rawtypes")
    List resources = reader.getResourceInstList();
    for (@SuppressWarnings("rawtypes")
    Iterator iter = resources.iterator(); iter.hasNext();) {
      StatArchiveReader.ResourceInst ri = (StatArchiveReader.ResourceInst) iter.next();
      String resourceName = ri.getName();
      assertNotNull(resourceName);
      
      if (!resourceName.equals("st1_1")) {
        logger.info("testStatSampler skipping {}", resourceName);
        continue;
      }
      
      String expectedStatsType = this.statisticTypes.get(resourceName);
      assertNotNull(expectedStatsType);
      assertEquals(expectedStatsType, ri.getType().getName());
      
      Map<String,Number> expectedStatValues = this.allStatistics.get(resourceName);
      assertNotNull(expectedStatValues);
      
      StatValue[] statValues = ri.getStatValues();
      for (int i = 0; i < statValues.length; i++) {
        String statName = ri.getType().getStats()[i].getName();
        assertNotNull(statName);
        assertNotNull(expectedStatValues.get(statName));
        
        assertEquals(statName, statValues[i].getDescriptor().getName());
        
        statValues[i].setFilter(StatValue.FILTER_NONE);
        //double[] rawSnapshots = statValues[i].getRawSnapshots();
        assertEquals("Value " + i + " for " + statName + " is wrong: " + expectedStatValues,
            expectedStatValues.get(statName).doubleValue(), 
            statValues[i].getSnapshotsMostRecent(), 
            0);
      }
    }
  }
  
  private void waitForStatSample(final Statistics samplerStats) {
    int startSampleCount = samplerStats.getInt("sampleCount");
    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < 3000; done = (samplerStats.getInt("sampleCount") > startSampleCount)) {
        Thread.sleep(10);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue("Waiting for statSampler sampleCount to increment", done);
  }
  
  private void incDouble(Statistics statistics, String stat, double value) {
    assertFalse(statistics.isClosed());
    Map<String,Number> statValues = this.allStatistics.get(statistics.getTextId());
    if (statValues == null) {
      statValues = new HashMap<String,Number>();
      this.allStatistics.put(statistics.getTextId(), statValues);
    }
    statistics.incDouble(stat, value);
    statValues.put(stat, statistics.getDouble(stat));
    if (this.statisticTypes.get(statistics.getTextId()) == null) {
      this.statisticTypes.put(statistics.getTextId(), statistics.getType().getName());
    }
  }
  
  private void incInt(Statistics statistics, String stat, int value) {
    assertFalse(statistics.isClosed());
    Map<String,Number> statValues = this.allStatistics.get(statistics.getTextId());
    if (statValues == null) {
      statValues = new HashMap<String,Number>();
      this.allStatistics.put(statistics.getTextId(), statValues);
    }
    statistics.incInt(stat, value);
    statValues.put(stat, statistics.getInt(stat));
    if (this.statisticTypes.get(statistics.getTextId()) == null) {
      this.statisticTypes.put(statistics.getTextId(), statistics.getType().getName());
    }
  }

  private void incLong(Statistics statistics, String stat, long value) {
    assertFalse(statistics.isClosed());
    Map<String,Number> statValues = this.allStatistics.get(statistics.getTextId());
    if (statValues == null) {
      statValues = new HashMap<String,Number>();
      this.allStatistics.put(statistics.getTextId(), statValues);
    }
    statistics.incLong(stat, value);
    statValues.put(stat, statistics.getLong(stat));
    if (this.statisticTypes.get(statistics.getTextId()) == null) {
      this.statisticTypes.put(statistics.getTextId(), statistics.getType().getName());
    }
  }
}
