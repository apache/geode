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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.io.MainWithChildrenRollingFileHandler;
import org.apache.geode.test.junit.categories.StatisticsTest;

@Category({StatisticsTest.class})
public class FileSizeLimitIntegrationTest {

  private static final long FILE_SIZE_LIMIT = 1;

  private File dir;
  private String archiveFileName;

  private LocalStatisticsFactory factory;
  private StatisticDescriptor[] statisticDescriptors;
  private StatisticsType statisticsType;
  private Statistics statistics;

  private SampleCollector sampleCollector;

  private NanoTimer timer = new NanoTimer();
  private long nanosTimeStamp;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    this.dir = this.temporaryFolder.getRoot();
    this.archiveFileName =
        new File(this.dir, this.testName.getMethodName() + ".gfs").getAbsolutePath();

    this.factory = new LocalStatisticsFactory(null);
    this.statisticDescriptors = new StatisticDescriptor[] {
        this.factory.createIntCounter("stat1", "description of stat1", "units", true)};
    this.statisticsType =
        factory.createType("statisticsType1", "statisticsType1", this.statisticDescriptors);
    this.statistics = factory.createAtomicStatistics(this.statisticsType, "statistics1", 1);

    Answer<Statistics[]> statisticsAnswer = new Answer<Statistics[]>() {
      @Override
      public Statistics[] answer(InvocationOnMock invocation) throws Throwable {
        return factory.getStatistics();
      }
    };

    Answer<Integer> modCountAnswer = new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocation) throws Throwable {
        return factory.getStatListModCount();
      }
    };

    StatisticsSampler sampler = mock(StatisticsSampler.class);
    when(sampler.getStatistics()).thenAnswer(statisticsAnswer);
    when(sampler.getStatisticsModCount()).thenAnswer(modCountAnswer);

    StatArchiveHandlerConfig config = mock(StatArchiveHandlerConfig.class);
    when(config.getArchiveFileName()).thenReturn(new File(this.archiveFileName));
    when(config.getArchiveFileSizeLimit()).thenReturn(FILE_SIZE_LIMIT);
    when(config.getSystemId()).thenReturn(1L);
    when(config.getSystemStartTime()).thenReturn(System.currentTimeMillis());
    when(config.getSystemDirectoryPath())
        .thenReturn(this.temporaryFolder.getRoot().getAbsolutePath());
    when(config.getProductDescription()).thenReturn(this.testName.getMethodName());
    when(config.getArchiveDiskSpaceLimit()).thenReturn(0L);

    this.sampleCollector = new SampleCollector(sampler);
    this.sampleCollector.initialize(config, this.timer.getTime(),
        new MainWithChildrenRollingFileHandler());

    this.timer.reset();
    this.nanosTimeStamp = this.timer.getLastResetTime() - getNanoRate();
  }

  @After
  public void tearDown() throws Exception {
    StatisticsTypeFactoryImpl.clear();
  }

  @Test
  public void rollsWhenLimitIsReached() throws Exception { // TODO: add test to assert size is
                                                           // correct
    sampleUntilFileExists(archiveFile(1));
    sampleUntilFileExists(archiveFile(2));
    assertThat(archiveFile(1)).exists();
    assertThat(archiveFile(2)).exists();
  }

  private void sampleUntilFileExists(final File file)
      throws InterruptedException, TimeoutException {
    long timeout = System.nanoTime() + MINUTES.toNanos(1);
    int count = 0;
    do {
      sample(advanceNanosTimeStamp());
      count++;
      Thread.sleep(10);
    } while (!file.exists() && System.nanoTime() < timeout);
    if (!file.exists()) {
      throw new TimeoutException("File " + file + " does not exist after " + count
          + " samples within " + 1 + " " + MINUTES);
    }
    System.out.println("Sampled " + count + " times to create " + file);
  }

  private void sample(final long time) {
    getSampleCollector().sample(time);
  }

  private SampleCollector getSampleCollector() {
    return this.sampleCollector;
  }

  private long advanceNanosTimeStamp() {
    this.nanosTimeStamp += getNanoRate();
    return this.nanosTimeStamp;
  }

  private long getNanoRate() {
    return NanoTimer.millisToNanos(getSampleRate());
  }

  private long getSampleRate() {
    return 1000; // 1 second
  }

  private File archiveFile(final int child) {
    return new File(this.dir,
        this.testName.getMethodName() + "-01-" + String.format("%02d", child) + ".gfs");
  }

  private File archiveFile() {
    return new File(this.archiveFileName);
  }
}
