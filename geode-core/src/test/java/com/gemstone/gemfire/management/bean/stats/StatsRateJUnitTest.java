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
package com.gemstone.gemfire.management.bean.stats;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.management.internal.beans.stats.MBeanStatsMonitor;
import com.gemstone.gemfire.management.internal.beans.stats.StatType;
import com.gemstone.gemfire.management.internal.beans.stats.StatsRate;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class StatsRateJUnitTest  {

  private Long SINGLE_STATS_LONG_COUNTER = null;
  private Integer SINGLE_STATS_INT_COUNTER = null;
  private Long MULTI_STATS_LONG_COUNTER_1 = null;
  private Long MULTI_STATS_LONG_COUNTER_2 = null;
  private Integer MULTI_STATS_INT_COUNTER_1 = null;
  private Integer MULTI_STATS_INT_COUNTER_2 = null;
  private TestMBeanStatsMonitor statsMonitor;

  @Before
  public void setUp() throws Exception {
    SINGLE_STATS_LONG_COUNTER = 0L;
    SINGLE_STATS_INT_COUNTER = 0;
    MULTI_STATS_LONG_COUNTER_1 = 0L;
    MULTI_STATS_LONG_COUNTER_2 = 0L;
    MULTI_STATS_INT_COUNTER_1 = 0;
    MULTI_STATS_INT_COUNTER_2 = 0;
    statsMonitor = new TestMBeanStatsMonitor("TestStatsMonitor");
  }

  @Test
  public void testSingleStatLongRate() throws Exception {
    StatsRate singleStatsRate = new StatsRate("SINGLE_STATS_LONG_COUNTER", StatType.LONG_TYPE, statsMonitor);

    SINGLE_STATS_LONG_COUNTER = 5000L;
    float actualRate = singleStatsRate.getRate();

    SINGLE_STATS_LONG_COUNTER = 10000L;

    actualRate = singleStatsRate.getRate();

    float expectedRate = 5000;

    assertEquals(expectedRate, actualRate, 0);
  }

  @Test
  public void testSingleStatIntRate() throws Exception {
    StatsRate singleStatsRate = new StatsRate("SINGLE_STATS_INT_COUNTER", StatType.INT_TYPE, statsMonitor);

    SINGLE_STATS_INT_COUNTER = 5000;
    float actualRate = singleStatsRate.getRate();

    SINGLE_STATS_INT_COUNTER = 10000;
    long poll2 = System.currentTimeMillis();

    actualRate = singleStatsRate.getRate();

    float expectedRate = 5000;

    assertEquals(expectedRate, actualRate, 0);
  }

  @Test
  public void testMultiStatLongRate() throws Exception {
    String[] counters = new String[] { "MULTI_STATS_LONG_COUNTER_1", "MULTI_STATS_LONG_COUNTER_2" };
    StatsRate multiStatsRate = new StatsRate(counters, StatType.LONG_TYPE, statsMonitor);

    MULTI_STATS_LONG_COUNTER_1 = 5000L;
    MULTI_STATS_LONG_COUNTER_2 = 4000L;
    float actualRate = multiStatsRate.getRate();

    MULTI_STATS_LONG_COUNTER_1 = 10000L;
    MULTI_STATS_LONG_COUNTER_2 = 8000L;

    actualRate = multiStatsRate.getRate();

    float expectedRate = 9000;

    assertEquals(expectedRate, actualRate, 0);
  }

  @Test
  public void testMultiStatIntRate() throws Exception {
    String[] counters = new String[] { "MULTI_STATS_INT_COUNTER_1", "MULTI_STATS_INT_COUNTER_2" };
    StatsRate multiStatsRate = new StatsRate(counters, StatType.INT_TYPE, statsMonitor);

    MULTI_STATS_INT_COUNTER_1 = 5000;
    MULTI_STATS_INT_COUNTER_2 = 4000;
    float actualRate = multiStatsRate.getRate();

    MULTI_STATS_INT_COUNTER_1 = 10000;
    MULTI_STATS_INT_COUNTER_2 = 8000;

    actualRate = multiStatsRate.getRate();

    float expectedRate = 9000;

    assertEquals(expectedRate, actualRate, 0);
  }
  
  private class TestMBeanStatsMonitor extends MBeanStatsMonitor {

    public TestMBeanStatsMonitor(String name) {
      super(name);
    }

    @Override
    public void addStatisticsToMonitor(Statistics stats) {
    }

    @Override
    public Number getStatistic(String statName) {
      if (statName.equals("SINGLE_STATS_LONG_COUNTER")) {
        return SINGLE_STATS_LONG_COUNTER;
      }
      if (statName.equals("SINGLE_STATS_INT_COUNTER")) {
        return SINGLE_STATS_INT_COUNTER;
      }

      if (statName.equals("MULTI_STATS_LONG_COUNTER_1")) {
        return MULTI_STATS_LONG_COUNTER_1;
      }
      if (statName.equals("MULTI_STATS_LONG_COUNTER_2")) {
        return MULTI_STATS_LONG_COUNTER_2;
      }
      if (statName.equals("MULTI_STATS_INT_COUNTER_1")) {
        return MULTI_STATS_INT_COUNTER_1;
      }
      if (statName.equals("MULTI_STATS_INT_COUNTER_2")) {
        return MULTI_STATS_INT_COUNTER_2;
      }
      return null;
    }

    @Override
    public void removeStatisticsFromMonitor(Statistics stats) {
    }

    @Override
    public void stopListener() {
    }
  }

}
