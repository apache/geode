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
import static org.apache.commons.io.FileUtils.moveFileToDirectory;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.io.MainWithChildrenRollingFileHandler;
import org.apache.geode.internal.io.RollingFileHandler;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Category(IntegrationTest.class)
public class DiskSpaceLimitIntegrationTest {

  private static final long FILE_SIZE_LIMIT = 256;
  private static final long DISK_SPACE_LIMIT = FILE_SIZE_LIMIT * 2;

  private File dir;
  private File dirOfDeletedFiles;

  private String archiveFileName;

  private LocalStatisticsFactory factory;
  private StatisticDescriptor[] statisticDescriptors;
  private StatisticsType statisticsType;
  private Statistics statistics;

  private SampleCollector sampleCollector;
  private StatArchiveHandlerConfig config;

  private NanoTimer timer = new NanoTimer();
  private long nanosTimeStamp;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    this.dir = this.temporaryFolder.getRoot();
    this.dirOfDeletedFiles = this.temporaryFolder.newFolder("deleted");

    this.archiveFileName =
        new File(this.dir, this.testName.getMethodName() + ".gfs").getAbsolutePath();

    this.factory = new LocalStatisticsFactory(null);
    this.statisticDescriptors = new StatisticDescriptor[] {
        this.factory.createIntCounter("stat1", "description of stat1", "units", true)};
    this.statisticsType =
        factory.createType("statisticsType1", "statisticsType1", this.statisticDescriptors);
    this.statistics = factory.createAtomicStatistics(this.statisticsType, "statistics1", 1);

    StatisticsSampler sampler = mock(StatisticsSampler.class);
    when(sampler.getStatistics()).thenReturn(this.factory.getStatistics());

    this.config = mock(StatArchiveHandlerConfig.class);
    when(this.config.getArchiveFileName()).thenReturn(new File(this.archiveFileName));
    when(this.config.getArchiveFileSizeLimit()).thenReturn(FILE_SIZE_LIMIT);
    when(this.config.getSystemId()).thenReturn(1L);
    when(this.config.getSystemStartTime()).thenReturn(System.currentTimeMillis());
    when(this.config.getSystemDirectoryPath())
        .thenReturn(this.temporaryFolder.getRoot().getAbsolutePath());
    when(this.config.getProductDescription()).thenReturn(this.testName.getMethodName());

    RollingFileHandler rollingFileHandler = new TestableRollingFileHandler();

    this.sampleCollector = new SampleCollector(sampler);
    this.sampleCollector.initialize(this.config, NanoTimer.getTime(), rollingFileHandler);

    this.timer.reset();
    this.nanosTimeStamp = this.timer.getLastResetTime() - getNanoRate();
  }

  @After
  public void tearDown() throws Exception {
    StatisticsTypeFactoryImpl.clear();
  }

  @Test
  public void zeroKeepsAllFiles() throws Exception {
    when(this.config.getArchiveDiskSpaceLimit()).thenReturn(0L);
    sampleUntilFileExists(archiveFile(1));
    sampleUntilFileExists(archiveFile(2));
    assertThat(archiveFile(1)).exists();
    assertThat(archiveFile(2)).exists();
  }

  @Test
  public void aboveZeroDeletesOldestFile() throws Exception {
    when(this.config.getArchiveDiskSpaceLimit()).thenReturn(DISK_SPACE_LIMIT);
    sampleUntilFileExists(archiveFile(1));
    sampleUntilFileExists(archiveFile(2));
    sampleUntilFileDeleted(archiveFile(1));

    assertThat(archiveFile(1)).doesNotExist();

    // different file systems may have different children created/deleted
    int childFile = 2;
    for (; childFile < 10; childFile++) {
      if (archiveFile(childFile).exists()) {
        break;
      }
    }
    assertThat(childFile).isLessThan(10);

    assertThat(archiveFile(childFile)).exists();
    assertThat(everExisted(archiveFile(1))).isTrue();
  }

  private void sampleUntilFileExists(final File file)
      throws InterruptedException, TimeoutException {
    long minutes = 1;
    long timeout = System.nanoTime() + MINUTES.toNanos(minutes);
    int count = 0;
    do {
      sample(advanceNanosTimeStamp());
      count++;
      Thread.sleep(10);
    } while (!everExisted(file) && System.nanoTime() < timeout);
    if (!everExisted(file)) {
      throw new TimeoutException("File " + file + " does not exist after " + count
          + " samples within " + minutes + " " + MINUTES);
    }
    System.out.println("Sampled " + count + " times to create " + file);
  }

  private void sampleUntilFileDeleted(final File file)
      throws InterruptedException, TimeoutException {
    long minutes = 1;
    long timeout = System.nanoTime() + MINUTES.toNanos(minutes);
    int count = 0;
    do {
      sample(advanceNanosTimeStamp());
      count++;
      Thread.sleep(10);
    } while (file.exists() && System.nanoTime() < timeout);
    if (file.exists()) {
      throw new TimeoutException("File " + file + " does not exist after " + count
          + " samples within " + minutes + " " + MINUTES);
    }
    System.out.println("Sampled " + count + " times to delete " + file);
  }

  private boolean everExisted(final File file) {
    if (file.exists()) {
      return true;
    } else { // check dirOfDeletedFiles
      String name = file.getName();
      File deleted = new File(this.dirOfDeletedFiles, name);
      return deleted.exists();
    }
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

  /**
   * Override protected method to move file instead of deleting it.
   */
  private class TestableRollingFileHandler extends MainWithChildrenRollingFileHandler {
    @Override
    protected boolean delete(final File file) {
      try {
        moveFileToDirectory(file, dirOfDeletedFiles, false);
        return true;
      } catch (IOException e) {
        throw new Error(e);
      }
    }
  }

}
