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
package org.apache.geode.management.internal.beans.stats;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.test.junit.categories.StatisticsTest;

@Category(StatisticsTest.class)
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("*.UnitTest")
@PrepareForTest(MBeanJMXAdapter.class)
public class VMStatsMonitorTest {
  @Rule
  public TestName testName = new TestName();

  private VMStatsMonitor vmStatsMonitor;

  @Before
  public void setUp() {
    PowerMockito.mockStatic(MBeanJMXAdapter.class);
  }

  @Test
  public void statisticInitialValueShouldBeZeroWhenTheProcessCpuTimeJmxAttributeIsAvailable() {
    when(MBeanJMXAdapter.isAttributeAvailable(anyString(), anyString())).thenReturn(true);
    vmStatsMonitor = new VMStatsMonitor(testName.getMethodName());
    assertThat(vmStatsMonitor).isNotNull();
    assertThat(vmStatsMonitor.getCpuUsage()).isEqualTo(0);
  }

  @Test
  public void statisticInitialValueShouldBeUndefinedWhenTheProcessCpuTimeJmxAttributeIsNotAvailable() {
    when(MBeanJMXAdapter.isAttributeAvailable(anyString(), anyString())).thenReturn(false);
    vmStatsMonitor = new VMStatsMonitor(testName.getMethodName());
    assertThat(vmStatsMonitor).isNotNull();
    assertThat(vmStatsMonitor.getCpuUsage()).isEqualTo(VMStatsMonitor.VALUE_NOT_AVAILABLE);
  }

  @Test
  public void calculateCpuUsageShouldCorrectlyCalculateTheCpuUsed() {
    Instant now = Instant.now();
    long halfSecondAsNanoseconds = 500000000L;
    long quarterSecondAsNanoseconds = 250000000L;
    long threeQuarterSecondAsNanoseconds = 750000000L;
    vmStatsMonitor = spy(new VMStatsMonitor(testName.getMethodName()));
    when(vmStatsMonitor.getLastSystemTime()).thenReturn(now.toEpochMilli());

    // 50% used
    when(vmStatsMonitor.getLastProcessCpuTime()).thenReturn(0L);
    float initialCpuUsage = vmStatsMonitor
        .calculateCpuUsage(now.plus(1, ChronoUnit.SECONDS).toEpochMilli(), halfSecondAsNanoseconds);
    assertThat(initialCpuUsage).isNotEqualTo(Float.NaN);
    assertThat(initialCpuUsage).isCloseTo(50F, within(1F));

    // 25% decrease
    when(vmStatsMonitor.getLastProcessCpuTime()).thenReturn(50L);
    float decreasedCpuUsage = vmStatsMonitor.calculateCpuUsage(
        now.plus(1, ChronoUnit.SECONDS).toEpochMilli(), quarterSecondAsNanoseconds);
    assertThat(decreasedCpuUsage).isNotEqualTo(Float.NaN);
    assertThat(decreasedCpuUsage).isLessThan(initialCpuUsage);
    assertThat(decreasedCpuUsage).isCloseTo(25F, within(1F));

    // 50% increase
    when(vmStatsMonitor.getLastProcessCpuTime()).thenReturn(25L);
    float increasedCpuUsage = vmStatsMonitor.calculateCpuUsage(
        now.plus(1, ChronoUnit.SECONDS).toEpochMilli(), threeQuarterSecondAsNanoseconds);
    assertThat(increasedCpuUsage).isNotEqualTo(Float.NaN);
    assertThat(increasedCpuUsage).isGreaterThan(decreasedCpuUsage);
    assertThat(increasedCpuUsage).isCloseTo(75F, within(1F));
  }

  @Test
  public void refreshStatsShouldUpdateCpuUsage() {
    ZonedDateTime now = ZonedDateTime.now();
    when(MBeanJMXAdapter.isAttributeAvailable(anyString(), anyString())).thenReturn(true);
    vmStatsMonitor = spy(new VMStatsMonitor(testName.getMethodName()));
    assertThat(vmStatsMonitor).isNotNull();
    assertThat(vmStatsMonitor.getCpuUsage()).isEqualTo(0);
    Number processCpuTime = spy(Number.class);
    vmStatsMonitor.statsMap.put(StatsKey.VM_PROCESS_CPU_TIME, processCpuTime);

    // First Run: updates lastSystemTime
    when(vmStatsMonitor.currentTimeMillis()).thenReturn(now.toInstant().toEpochMilli());
    vmStatsMonitor.refreshStats();
    assertThat(vmStatsMonitor.getCpuUsage()).isEqualTo(0);
    verify(processCpuTime, times(0)).longValue();

    // Second Run: updates lastProcessCpuTime
    when(processCpuTime.longValue()).thenReturn(500L);
    vmStatsMonitor.refreshStats();
    assertThat(vmStatsMonitor.getCpuUsage()).isEqualTo(0);
    verify(processCpuTime, times(1)).longValue();

    // Next runs will update the actual cpuUsage
    for (int i = 2; i < 6; i++) {
      long mockProcessCpuTime = i * 500;
      long mockSystemTime = now.plus(i, ChronoUnit.SECONDS).toInstant().toEpochMilli();
      when(processCpuTime.longValue()).thenReturn(mockProcessCpuTime);
      when(vmStatsMonitor.currentTimeMillis()).thenReturn(mockSystemTime);

      vmStatsMonitor.refreshStats();
      verify(vmStatsMonitor, times(1)).calculateCpuUsage(mockSystemTime, mockProcessCpuTime);
      assertThat(vmStatsMonitor.getCpuUsage()).isNotEqualTo(Float.NaN);
      assertThat(vmStatsMonitor.getCpuUsage()).isLessThan(1F);
    }
  }
}
