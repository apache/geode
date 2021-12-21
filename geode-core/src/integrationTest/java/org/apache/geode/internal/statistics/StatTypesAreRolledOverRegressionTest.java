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
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.stubbing.Answer;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.io.MainWithChildrenRollingFileHandler;
import org.apache.geode.internal.statistics.StatArchiveReader.ResourceInst;
import org.apache.geode.test.junit.categories.StatisticsTest;

@Category({StatisticsTest.class})
public class StatTypesAreRolledOverRegressionTest {

  private static final long FILE_SIZE_LIMIT = 512;

  private File dir;
  private String archiveFileName;

  private LocalStatisticsFactory factory;
  private StatisticDescriptor[] statisticDescriptors;
  private StatisticsType statisticsType;
  private Statistics statistics;

  private SampleCollector sampleCollector;

  private final NanoTimer timer = new NanoTimer();
  private long nanosTimeStamp;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    dir = temporaryFolder.getRoot();
    archiveFileName =
        new File(dir, testName.getMethodName() + ".gfs").getAbsolutePath();

    factory = new LocalStatisticsFactory(null);
    statisticDescriptors = new StatisticDescriptor[] {
        factory.createIntCounter("stat1", "description of stat1", "units", true)};
    statisticsType =
        factory.createType("statisticsType1", "statisticsType1", statisticDescriptors);
    statistics = factory.createAtomicStatistics(statisticsType, "statistics1", 1);

    Answer<Statistics[]> statisticsAnswer = invocation -> factory.getStatistics();

    Answer<Integer> modCountAnswer = invocation -> factory.getStatListModCount();

    StatisticsSampler sampler = mock(StatisticsSampler.class);
    when(sampler.getStatistics()).thenAnswer(statisticsAnswer);
    when(sampler.getStatisticsModCount()).thenAnswer(modCountAnswer);

    StatArchiveHandlerConfig config = mock(StatArchiveHandlerConfig.class);
    when(config.getArchiveFileName()).thenReturn(new File(archiveFileName));
    when(config.getArchiveFileSizeLimit()).thenReturn(FILE_SIZE_LIMIT);
    when(config.getSystemId()).thenReturn(1L);
    when(config.getSystemStartTime()).thenReturn(System.currentTimeMillis());
    when(config.getSystemDirectoryPath())
        .thenReturn(temporaryFolder.getRoot().getAbsolutePath());
    when(config.getProductDescription()).thenReturn(testName.getMethodName());
    when(config.getArchiveDiskSpaceLimit()).thenReturn(0L);

    sampleCollector = new SampleCollector(sampler);
    sampleCollector.initialize(config, NanoTimer.getTime(),
        new MainWithChildrenRollingFileHandler());

    timer.reset();
    nanosTimeStamp = timer.getLastResetTime() - getNanoRate();
  }

  @After
  public void tearDown() throws Exception {
    StatisticsTypeFactoryImpl.clear();
  }

  @Test
  public void closedInstanceShouldHaveTypeInRolledArchives() throws Exception {
    // initial state
    verifyStatisticsTypeIsInArchiveFile(archiveFile(), 0);

    // one sample
    sample(advanceNanosTimeStamp());
    verifyStatisticsTypeIsInArchiveFile(archiveFile(), 1);

    // close stats
    statistics.close();

    assertThat(archiveFile(1)).doesNotExist();

    // cause roll
    sampleUntilFileExists(archiveFile(1));

    sample(advanceNanosTimeStamp());
    verifyStatisticsTypeIsInArchiveFile(archiveFile(), 0);

    statistics = factory.createAtomicStatistics(statisticsType, "statistics1", 2);

    sample(advanceNanosTimeStamp());
    verifyStatisticsTypeIsInArchiveFile(archiveFile(), 1); // should be corrupt?

  }

  private void verifyStatisticsTypeIsInArchiveFile(final File archiveFile,
      final int expectedResources) throws IOException {
    try (StatArchiveReader reader = new StatArchiveReader(new File[] {archiveFile}, null, false)) {

      // compare all resourceInst values against what was printed above

      List<ResourceInst> resources = reader.getResourceInstList();
      if (expectedResources > 0) {
        assertThat(resources).hasAtLeastOneElementOfType(ResourceInst.class);
      }

      for (ResourceInst resourceInstance : resources) {
        if (resourceInstance == null) {
          continue;
        }
        assertThat(resourceInstance.getName()).isNotNull();
        assertThat(resourceInstance.getType()).isNotNull();
        assertThat(resourceInstance.getType().getName()).isEqualTo(statisticsType.getName());
      }
    }
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
    return sampleCollector;
  }

  private long advanceNanosTimeStamp() {
    nanosTimeStamp += getNanoRate();
    return nanosTimeStamp;
  }

  private long getNanoRate() {
    return NanoTimer.millisToNanos(getSampleRate());
  }

  private long getSampleRate() {
    return 1000; // 1 second
  }

  private File archiveFile(final int child) {
    return new File(dir,
        testName.getMethodName() + "-01-" + String.format("%02d", child) + ".gfs");
  }

  private File archiveFile() {
    return new File(archiveFileName);
  }
}
