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

import org.apache.commons.io.IOUtils;
import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.internal.statistics.platform.LinuxProcFsStatistics;
import org.apache.geode.internal.statistics.platform.LinuxSystemStats;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.tools.ant.filters.StringInputStream;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Technically a linux only test - the file handling is all mocked up so the test can run on any
 * host os.
 */
@Category(IntegrationTest.class)
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("*.IntegrationTest")
@PrepareForTest(LinuxProcFsStatistics.class)
@Ignore
public class LinuxSystemStatsTest extends StatSamplerTestCase {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  private int[] ints;
  private long[] longs;
  private double[] doubles;
  private LocalStatisticsFactory statisticsFactory;
  private File testDir;

  @Before
  public void setUp() throws Exception {
    this.testDir = this.temporaryFolder.getRoot();
    assertTrue(this.testDir.exists());
    System.setProperty(SimpleStatSampler.ARCHIVE_FILE_NAME_PROPERTY, this.testDir.getAbsolutePath()
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
      this.statisticsFactory = null;
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

  protected void doTest(String[] results, int expectedStatValue) throws Exception {

    Answer<FileInputStream> answer = new MyStealTimeAnswer(results);
    PowerMockito.whenNew(FileInputStream.class).withArguments(anyString()).thenAnswer(answer);


    LinuxProcFsStatistics.refreshSystem(ints, longs, doubles);
    LinuxProcFsStatistics.refreshSystem(ints, longs, doubles);

    Statistics[] statistics = getStatisticsManager().findStatisticsByTextId("LinuxSystemStats");
    waitForExpectedStatValue(statistics[0], "cpuSteal", expectedStatValue, 20000, 10);
  }

  protected void initStats() {
    statisticsFactory = new LocalStatisticsFactory(new CancelCriterion() {
      public String cancelInProgress() {
        return null;
      }

      public RuntimeException generateCancelledException(Throwable e) {
        return null;
      }
    });
  }

  private File writeStringToFile(String string) throws IOException {
    File file = File.createTempFile("LinuxSystemStatsTest", ".test");
    file.deleteOnExit();
    StringInputStream sis = new StringInputStream(string);
    FileOutputStream fos = new FileOutputStream(file);
    IOUtils.copy(sis, fos);
    IOUtils.closeQuietly(fos);
    return file;
  }

  @Override
  protected StatisticsManager getStatisticsManager() {
    return this.statisticsFactory;
  }


  private class MyStealTimeAnswer implements Answer<FileInputStream> {

    private List<FileInputStream> results = new ArrayList<>();
    private FileInputStream bogus;

    public MyStealTimeAnswer(String[] samples) throws IOException {
      for (String item : samples) {
        results.add(new FileInputStream(writeStringToFile(item)));
      }
      bogus = new FileInputStream(writeStringToFile(""));
    }

    @Override
    public FileInputStream answer(InvocationOnMock invocation) throws Throwable {
      if ("/proc/stat".equals(invocation.getArgument(0))) {
        return results.remove(0);
      }
      return bogus;
    }
  }
}
