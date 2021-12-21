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

import static org.apache.geode.internal.statistics.platform.LinuxSystemStats.TCP_EXT_LISTEN_DROPS;
import static org.apache.geode.internal.statistics.platform.LinuxSystemStats.TCP_EXT_LISTEN_OVERFLOWS;
import static org.apache.geode.internal.statistics.platform.LinuxSystemStats.TCP_EXT_SYN_COOKIES_RECV;
import static org.apache.geode.internal.statistics.platform.LinuxSystemStats.TCP_EXT_SYN_COOKIES_SENT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.internal.statistics.platform.LinuxProcFsStatistics;
import org.apache.geode.internal.statistics.platform.LinuxSystemStats;
import org.apache.geode.test.junit.categories.StatisticsTest;


/**
 * Technically a linux only test - the file handling is all mocked up so the test can run on any
 * host os.
 */
@Category(StatisticsTest.class)
public class LinuxSystemStatsTest extends StatSamplerTestCase {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private LocalStatisticsFactory statisticsFactory;
  private LocalStatisticsImpl localStats;

  @Before
  public void setUp() throws Exception {
    File testDir = temporaryFolder.getRoot();
    assertThat(testDir.exists()).isTrue();
    System.setProperty(SimpleStatSampler.ARCHIVE_FILE_NAME_PROPERTY, testDir.getAbsolutePath()
        + File.separator + SimpleStatSampler.DEFAULT_ARCHIVE_FILE_NAME);
    LinuxProcFsStatistics.init();
    initStats();
    StatisticsTypeImpl statisticsType = (StatisticsTypeImpl) LinuxSystemStats.getType();
    localStats = (LocalStatisticsImpl) getStatisticsManager()
        .createStatistics(statisticsType, statisticsType.getName());
  }

  @After
  public void tearDown() throws Exception {
    StatisticsTypeFactoryImpl.clear();
    if (statisticsFactory != null) {
      statisticsFactory.close();
    }
  }

  @Test
  public void testFlatStealTime() throws Exception {
    // add on 4 clicks 4 idle 0 steal
    String mockStats = LinuxSystemStatsTest.class.getResource("FlatStealTime.txt").getFile();

    doStealTimeTest(mockStats, 0);
  }

  @Test
  public void test25PercentStealTime() throws Exception {
    // add on 4 clicks 3 idle 1 steal
    String mockStats = LinuxSystemStatsTest.class.getResource("25PercentStealTime.txt").getFile();

    doStealTimeTest(mockStats, 25);
  }

  @Test
  public void test66PercentStealTime() throws Exception {
    // add on 3 clicks 1 idle 2 steal
    String mockStats = LinuxSystemStatsTest.class.getResource("66PercentStealTime.txt").getFile();

    doStealTimeTest(mockStats, 66);
  }

  @Test
  public void test100PercentStealTime() throws Exception {
    // add on 1 clicks 0 idle 1 steal
    String mockStats = LinuxSystemStatsTest.class.getResource("100PercentStealTime.txt").getFile();

    doStealTimeTest(mockStats, 100);
  }

  @Test
  public void netstatStatsTest() throws Exception {
    long expectedSyncookiesSent = 1L;
    long expectedSyncookiesRecv = 2L;
    long expectedListenOverflows = 3L;
    long expectedListenDrops = 4L;

    // This file simulates the contents of the /proc/net/netstat file, omitting all stats that
    // aren't parsed in the LinuxProcFsStatistics.getNetStatStats() method and including a dummy
    // stat that should not be parsed
    String mockNetstatStats =
        LinuxSystemStatsTest.class.getResource("mockNetstatStats.txt").getFile();
    LinuxProcFsStatistics.refreshSystem(localStats,
        LinuxSystemStatsTest.class.getResource("100PercentStealTime.txt").getFile(),
        mockNetstatStats);

    Statistics statistics = getStatisticsManager().findStatisticsByTextId("LinuxSystemStats")[0];
    assertThat(statistics.getLong(TCP_EXT_SYN_COOKIES_SENT)).isEqualTo(expectedSyncookiesSent);
    assertThat(statistics.getLong(TCP_EXT_SYN_COOKIES_RECV)).isEqualTo(expectedSyncookiesRecv);
    assertThat(statistics.getLong(TCP_EXT_LISTEN_OVERFLOWS)).isEqualTo(expectedListenOverflows);
    assertThat(statistics.getLong(TCP_EXT_LISTEN_DROPS)).isEqualTo(expectedListenDrops);
  }

  private void doStealTimeTest(String fileInputPath, int expectedStatValue) {
    LinuxProcFsStatistics.refreshSystem(localStats, fileInputPath,
        LinuxSystemStatsTest.class.getResource("mockNetstatStats.txt").getFile());

    Statistics[] statistics = getStatisticsManager().findStatisticsByTextId("LinuxSystemStats");
    waitForExpectedStatValue(statistics[0], "cpuSteal", expectedStatValue, 20000, 10);
  }

  private void initStats() {
    statisticsFactory = new LocalStatisticsFactory(new CancelCriterion() {
      @Override
      public String cancelInProgress() {
        return null;
      }

      @Override
      public RuntimeException generateCancelledException(Throwable e) {
        return null;
      }
    });
  }

  @Override
  protected StatisticsManager getStatisticsManager() {
    return statisticsFactory;
  }
}
