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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.CancelCriterion;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.process.PidUnavailableException;
import org.apache.geode.internal.process.UncheckedPidUnavailableException;
import org.apache.geode.logging.spi.LogFile;

/**
 * Unit tests for {@link HostStatSampler}.
 */
public class HostStatSamplerTest {

  private CancelCriterion cancelCriterion;
  private StatSamplerStats statSamplerStats;
  private NanoTimer timer;
  private LogFile logFile;
  private StatisticsManager statisticsManager;

  private HostStatSampler hostStatSampler;

  @Before
  public void setUp() {
    cancelCriterion = mock(CancelCriterion.class);
    statSamplerStats = mock(StatSamplerStats.class);
    timer = new NanoTimer();
    logFile = null;
    statisticsManager = mock(StatisticsManager.class);
  }

  @Test
  public void getSpecialStatsId_returnsPidFromPidSupplier_ifValueIsGreaterThanZero() {
    int thePid = 42;
    when(statisticsManager.getPid()).thenReturn(thePid);
    long anySystemId = 2;
    hostStatSampler = new TestableHostStatSampler(cancelCriterion, statSamplerStats, timer, logFile,
        statisticsManager, anySystemId);

    assertThat(hostStatSampler.getSpecialStatsId()).isEqualTo(thePid);
  }

  @Test
  public void getSpecialStatsId_returnsSystemId_ifValueFromPidSupplierIsZero() {
    when(statisticsManager.getPid()).thenReturn(0);
    long theSystemId = 21;
    hostStatSampler = new TestableHostStatSampler(cancelCriterion, statSamplerStats, timer, logFile,
        statisticsManager, theSystemId);

    assertThat(hostStatSampler.getSpecialStatsId()).isEqualTo(theSystemId);
  }

  @Test
  public void getSpecialStatsId_returnsSystemId_ifValueFromPidSupplierIsLessThanZero() {
    when(statisticsManager.getPid()).thenReturn(-1);
    long theSystemId = 21;
    hostStatSampler = new TestableHostStatSampler(cancelCriterion, statSamplerStats, timer, logFile,
        statisticsManager, theSystemId);

    assertThat(hostStatSampler.getSpecialStatsId()).isEqualTo(theSystemId);
  }

  @Test
  public void getSpecialStatsId_returnsSystemId_ifPidSupplierThrows() {
    when(statisticsManager.getPid()).thenThrow(
        new UncheckedPidUnavailableException(new PidUnavailableException("Pid not found")));
    long theSystemId = 21;
    hostStatSampler = new TestableHostStatSampler(cancelCriterion, statSamplerStats, timer, logFile,
        statisticsManager, theSystemId);

    assertThat(hostStatSampler.getSpecialStatsId()).isEqualTo(theSystemId);
  }

  private static class TestableHostStatSampler extends HostStatSampler {

    private final StatisticsManager statisticsManager;
    private final long systemId;

    TestableHostStatSampler(CancelCriterion stopper, StatSamplerStats samplerStats, NanoTimer timer,
        LogFile logFile, StatisticsManager statisticsManager, long systemId) {
      super(stopper, samplerStats, timer, logFile);
      this.statisticsManager = statisticsManager;
      this.systemId = systemId;
    }

    @Override
    protected void checkListeners() {

    }

    @Override
    protected int getSampleRate() {
      return 0;
    }

    @Override
    public boolean isSamplingEnabled() {
      return false;
    }

    @Override
    protected StatisticsManager getStatisticsManager() {
      return statisticsManager;
    }

    @Override
    public File getArchiveFileName() {
      return null;
    }

    @Override
    public long getArchiveFileSizeLimit() {
      return 0;
    }

    @Override
    public long getArchiveDiskSpaceLimit() {
      return 0;
    }

    @Override
    public long getSystemId() {
      return systemId;
    }

    @Override
    public String getProductDescription() {
      return null;
    }
  }
}
