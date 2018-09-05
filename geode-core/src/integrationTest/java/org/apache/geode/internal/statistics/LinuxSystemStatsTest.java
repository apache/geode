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
import static org.mockito.ArgumentMatchers.anyString;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

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
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("*.IntegrationTest")
@PrepareForTest(LinuxProcFsStatistics.class)
public class LinuxSystemStatsTest extends StatSamplerTestCase {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private int[] ints;
  private long[] longs;
  private double[] doubles;
  private LocalStatisticsFactory statisticsFactory;

  @Before
  public void setUp() throws Exception {
    File testDir = this.temporaryFolder.getRoot();
    assertThat(testDir.exists()).isTrue();
    System.setProperty(SimpleStatSampler.ARCHIVE_FILE_NAME_PROPERTY, testDir.getAbsolutePath()
        + File.separator + SimpleStatSampler.DEFAULT_ARCHIVE_FILE_NAME);
    LinuxProcFsStatistics.init();
    initStats();
    StatisticsTypeImpl statisticsType = (StatisticsTypeImpl) LinuxSystemStats.getType();
    LocalStatisticsImpl statistics = (LocalStatisticsImpl) getStatisticsManager()
        .createStatistics(statisticsType, statisticsType.getName());

    ints = statistics._getIntStorage();
    longs = statistics._getLongStorage();
    doubles = statistics._getDoubleStorage();
  }

  @After
  public void tearDown() throws Exception {
    StatisticsTypeFactoryImpl.clear();
    if (this.statisticsFactory != null) {
      this.statisticsFactory.close();
    }
  }

  @Test
  public void testFlatStealTime() throws Exception {
    String[] results = {"cpu  0 0 0 0 0 0 0 0 0 0",
        // add on 4 clicks 4 idle 0 steal
        "cpu  0 0 0 4 0 0 0 0 0 0"};

    doTest(results, 0);
  }

  @Test
  public void test25PercentStealTime() throws Exception {
    String[] results = {"cpu  0 0 0 0 0 0 0 0 0 0",
        // add on 4 clicks 3 idle 1 steal
        "cpu  0 0 0 3 0 0 0 1 0 0"};

    doTest(results, 25);
  }

  @Test
  public void test66PercentStealTime() throws Exception {
    String[] results = {"cpu  0 0 0 0 0 0 0 0 0 0",
        // add on 3 clicks 1 idle 2 steal
        "cpu  0 0 0 1 0 0 0 2 0 0"};

    doTest(results, 66);
  }

  @Test
  public void test100PercentStealTime() throws Exception {
    String[] results = {"cpu  0 0 0 0 0 0 0 0 0 0",
        // add on 1 clicks 0 idle 1 steal
        "cpu  0 0 0 0 0 0 0 1 0 0"};

    doTest(results, 100);
  }

  private void doTest(String[] results, int expectedStatValue) throws Exception {
    Answer<FileInputStream> answer = new MyStealTimeAnswer(results);
    PowerMockito.whenNew(FileInputStream.class).withArguments(anyString()).thenAnswer(answer);

    LinuxProcFsStatistics.refreshSystem(ints, longs, doubles);
    LinuxProcFsStatistics.refreshSystem(ints, longs, doubles);

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

  /**
   * This method will allow junit to mock up how Linux reports the CPU information though a file
   * called "/proc/stat". We need to create a new file for each call since each file could represent
   * another phase in the mock test.
   */
  private File writeStringToFile(String mockProcStatFileContents) throws IOException {
    File mockFile = temporaryFolder.newFile();
    FileUtils.writeStringToFile(mockFile, mockProcStatFileContents, (Charset) null);

    return mockFile;
  }

  @Override
  protected StatisticsManager getStatisticsManager() {
    return this.statisticsFactory;
  }

  private class MyStealTimeAnswer implements Answer<FileInputStream> {

    private final List<FileInputStream> results = new ArrayList<>();
    private final FileInputStream bogus;

    MyStealTimeAnswer(String[] samples) throws IOException {
      for (String item : samples) {
        results.add(new FileInputStream(writeStringToFile(item)));
      }
      bogus = new FileInputStream(writeStringToFile(""));
    }

    @Override
    public FileInputStream answer(InvocationOnMock invocation) throws Throwable {
      // Since we are mocking the test we can run this test on any OS.
      if ("/proc/stat".equals(invocation.getArgument(0))) {
        return results.remove(0);
      }
      return bogus;
    }
  }
}
