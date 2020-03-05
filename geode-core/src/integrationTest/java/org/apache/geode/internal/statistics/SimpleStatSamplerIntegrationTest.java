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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.lang.reflect.Method;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.stats50.VMStats50;
import org.apache.geode.test.junit.categories.StatisticsTest;

/**
 * Integration tests for {@link SimpleStatSampler}.
 *
 * @since GemFire 7.0
 */
@Category({StatisticsTest.class})
public class SimpleStatSamplerIntegrationTest extends StatSamplerTestCase {

  private LocalStatisticsFactory statisticsFactory;
  private File testDir;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    this.testDir = this.temporaryFolder.getRoot();
    assertTrue(this.testDir.exists());
    System.setProperty(SimpleStatSampler.ARCHIVE_FILE_NAME_PROPERTY, this.testDir.getAbsolutePath()
        + File.separator + SimpleStatSampler.DEFAULT_ARCHIVE_FILE_NAME);
  }

  @After
  public void tearDown() throws Exception {
    System.clearProperty(SimpleStatSampler.ARCHIVE_FILE_NAME_PROPERTY);
    System.clearProperty(SimpleStatSampler.FILE_SIZE_LIMIT_PROPERTY);
    System.clearProperty(SimpleStatSampler.DISK_SPACE_LIMIT_PROPERTY);
    System.clearProperty(SimpleStatSampler.SAMPLE_RATE_PROPERTY);
    closeStatisticsFactory();
  }

  /**
   * Tests the majority of getters and the basic functionality of the sampler.
   */
  @Test
  public void testBasics() throws Exception {
    initStatisticsFactory();

    SimpleStatSampler statSampler = getSimpleStatSampler();
    assertTrue(statSampler.waitForInitialization(5000));

    assertEquals(new File(System.getProperty(SimpleStatSampler.ARCHIVE_FILE_NAME_PROPERTY)),
        statSampler.getArchiveFileName());
    assertEquals(0, statSampler.getArchiveFileSizeLimit());
    assertEquals(0, statSampler.getArchiveDiskSpaceLimit());
    assertEquals(SimpleStatSampler.DEFAULT_SAMPLE_RATE, statSampler.getSampleRate());
    assertEquals(true, statSampler.isSamplingEnabled());

    int statsCount = statSampler.getStatisticsManager().getStatisticsCount();

    assertEquals(statsCount, statSampler.getStatisticsModCount());
    assertEquals(statsCount, statSampler.getStatisticsManager().getStatisticsCount());
    assertEquals(statsCount, statSampler.getStatistics().length);

    assertTrue(statsCount > 0);

    assertTrue(statSampler.getSystemStartTime() <= System.currentTimeMillis());
    assertEquals(LocalHostUtil.getLocalHostName(),
        statSampler.getSystemDirectoryPath());

    VMStatsContract vmStats = statSampler.getVMStats();
    assertNotNull(vmStats);
    assertTrue(vmStats instanceof VMStats50);
    /*
     * NOTE: VMStats50 is not an instance of Statistics but instead its instance contains 3
     * instances of Statistics: 1) vmStats 2) heapMemStats 3) nonHeapMemStats
     */

    Method getProcessStats = null;
    try {
      getProcessStats = SimpleStatSampler.class.getMethod("getProcessStats");
      fail("SimpleStatSampler should not have the method getProcessStats()");
    } catch (NoSuchMethodException exected) {
      // passed
    }
    assertNull(getProcessStats);

    assertEquals("Unknown product", statSampler.getProductDescription());
  }

  /**
   * Tests that the configured archive file is created and exists.
   */
  @Test
  public void testArchiveFileExists() throws Exception {
    final String dir = this.testDir.getAbsolutePath();
    final String archiveFileName = dir + File.separator + this.testName + ".gfs";
    final File archiveFile1 = new File(dir + File.separator + this.testName + ".gfs");
    System.setProperty(SimpleStatSampler.ARCHIVE_FILE_NAME_PROPERTY, archiveFileName);
    initStatisticsFactory();

    SimpleStatSampler statSampler = getSimpleStatSampler();
    assertTrue(statSampler.waitForInitialization(5000));

    final File archiveFile = statSampler.getArchiveFileName();
    assertNotNull(archiveFile);
    assertEquals(archiveFile1, archiveFile);

    waitForFileToExist(archiveFile, 5000, 10);

    assertTrue(
        "File name incorrect: archiveFile.getName()=" + archiveFile.getName()
            + " archiveFile.getAbsolutePath()=" + archiveFile.getAbsolutePath()
            + " getCanonicalPath()" + archiveFile.getCanonicalPath(),
        archiveFileName.contains(archiveFile.getName()));
  }

  /**
   * Tests the statistics sample rate within an acceptable margin of error.
   */
  @Test
  public void testSampleRate() throws Exception {
    initStatisticsFactory();

    SimpleStatSampler statSampler = getSimpleStatSampler();
    assertTrue(statSampler.waitForInitialization(5000));

    assertEquals(SimpleStatSampler.DEFAULT_SAMPLE_RATE, statSampler.getSampleRate());

    assertTrue(getStatisticsManager().getStatListModCount() > 0);

    List<Statistics> statistics = getStatisticsManager().getStatsList();
    assertNotNull(statistics);
    assertTrue(statistics.size() > 0);

    StatisticsType statSamplerType = getStatisticsManager().findType("StatSampler");
    Statistics[] statsArray = getStatisticsManager().findStatisticsByType(statSamplerType);
    assertEquals(1, statsArray.length);

    final Statistics statSamplerStats = statsArray[0];
    final int initialSampleCount = statSamplerStats.getInt("sampleCount");
    final int expectedSampleCount = initialSampleCount + 2;

    waitForStatSample(statSamplerStats, expectedSampleCount, 20000, 10);
  }

  /**
   * Tests lack of methods for supporting LocalStatListener.
   */
  @Test
  public void testLocalStatListener() throws Exception {
    initStatisticsFactory();

    SimpleStatSampler statSampler = getSimpleStatSampler();
    assertTrue(statSampler.waitForInitialization(5000));

    Method getLocalListeners = null;
    try {
      getLocalListeners = getSimpleStatSampler().getClass().getMethod("getLocalListeners");
      fail("SimpleStatSampler should not have the method getLocalListeners()");
    } catch (NoSuchMethodException exected) {
      // passed
    }
    assertNull(getLocalListeners);

    Method addLocalStatListener = null;
    try {
      addLocalStatListener = getSimpleStatSampler().getClass().getMethod("addLocalStatListener",
          LocalStatListener.class, Statistics.class, String.class);
      fail("SimpleStatSampler should not have the method addLocalStatListener()");
    } catch (NoSuchMethodException exected) {
      // passed
    }
    assertNull(addLocalStatListener);

    Method removeLocalStatListener = null;
    try {
      removeLocalStatListener = getSimpleStatSampler().getClass()
          .getMethod("removeLocalStatListener", LocalStatListener.class);
      fail("SimpleStatSampler should not have the method addLocalStatListener()");
    } catch (NoSuchMethodException exected) {
      // passed
    }
    assertNull(removeLocalStatListener);
  }

  /**
   * Invokes stop() and then validates that the sampler did in fact stop.
   */
  @Test
  public void testStop() throws Exception {
    initStatisticsFactory();

    SimpleStatSampler statSampler = getSimpleStatSampler();
    assertTrue(statSampler.waitForInitialization(5000));

    // validate the stat sampler is running
    StatisticsType statSamplerType = getStatisticsManager().findType("StatSampler");
    Statistics[] statsArray = getStatisticsManager().findStatisticsByType(statSamplerType);
    assertEquals(1, statsArray.length);

    final Statistics statSamplerStats = statsArray[0];
    final int initialSampleCount = statSamplerStats.getInt("sampleCount");
    final int expectedSampleCount = initialSampleCount + 2;

    waitForStatSample(statSamplerStats, expectedSampleCount, 20000, 10);

    // stop the stat sampler
    statSampler.stop();

    // validate the stat sampler has stopped
    final int stoppedSampleCount = statSamplerStats.getInt("sampleCount");

    // the following should timeout without completing
    assertStatValueDoesNotChange(statSamplerStats, "sampleCount", stoppedSampleCount, 5000, 10);

    assertEquals(stoppedSampleCount, statSamplerStats.getInt("sampleCount"));
  }

  /**
   * Verifies that archive rolling works correctly when archive-file-size-limit is specified. This
   * feature is broken in SimpleStatSampler.
   */
  @Test
  public void testArchiveRolling() throws Exception {
    // set the system property to use KB instead of MB for file size
    System.setProperty(HostStatSampler.TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY, "true");

    final String dir = this.testDir.getAbsolutePath() + File.separator + this.testName;
    new File(dir).mkdir();

    final String archiveFileName = dir + File.separator + this.testName + ".gfs";
    System.setProperty(SimpleStatSampler.ARCHIVE_FILE_NAME_PROPERTY, archiveFileName);
    System.setProperty(SimpleStatSampler.FILE_SIZE_LIMIT_PROPERTY, "1");
    System.setProperty(SimpleStatSampler.DISK_SPACE_LIMIT_PROPERTY, "0");
    System.setProperty(SimpleStatSampler.SAMPLE_RATE_PROPERTY, "1000");
    initStatisticsFactory();

    final File archiveFile1 = new File(dir + File.separator + this.testName + "-01-01.gfs");
    final File archiveFile2 = new File(dir + File.separator + this.testName + "-01-02.gfs");
    final File archiveFile3 = new File(dir + File.separator + this.testName + "-01-03.gfs");
    final File archiveFile4 = new File(dir + File.separator + this.testName + "-01-04.gfs");

    assertTrue(getSimpleStatSampler().waitForInitialization(5000));

    assertTrue(getSimpleStatSampler().fileSizeLimitInKB());
    assertEquals(1024, getSimpleStatSampler().getArchiveFileSizeLimit());

    waitForFileToExist(archiveFile1, 4000, 10);
    waitForFileToExist(archiveFile2, 4000, 10);
    waitForFileToExist(archiveFile3, 4000, 10);
    waitForFileToExist(archiveFile4, 4000, 10);
  }

  /**
   * Verifies that archive removal works correctly when archive-disk-space-limit is specified. This
   * feature is broken in SimpleStatSampler.
   */
  @Test
  public void testArchiveRemoval() throws Exception {
    // set the system property to use KB instead of MB for file size
    System.setProperty(HostStatSampler.TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY, "true");

    final String dir = this.testDir.getAbsolutePath() + File.separator + this.testName;
    new File(dir).mkdir();

    final String archiveFileName = dir + File.separator + this.testName + ".gfs";
    final int sampleRate = 1000;
    System.setProperty(SimpleStatSampler.ARCHIVE_FILE_NAME_PROPERTY, archiveFileName);
    System.setProperty(SimpleStatSampler.FILE_SIZE_LIMIT_PROPERTY, "1");
    System.setProperty(SimpleStatSampler.DISK_SPACE_LIMIT_PROPERTY, "12");
    System.setProperty(SimpleStatSampler.SAMPLE_RATE_PROPERTY, String.valueOf(sampleRate));
    initStatisticsFactory();

    final File archiveFile1 = new File(dir + File.separator + this.testName + "-01-01.gfs");
    final File archiveFile2 = new File(dir + File.separator + this.testName + "-01-02.gfs");
    final File archiveFile3 = new File(dir + File.separator + this.testName + "-01-03.gfs");
    final File archiveFile4 = new File(dir + File.separator + this.testName + "-01-04.gfs");
    final File archiveFile5 = new File(dir + File.separator + this.testName + "-01-05.gfs");

    assertTrue(getSimpleStatSampler().waitForInitialization(5000));

    waitForFileToExist(archiveFile1, 4 * sampleRate, 10);
    waitForFileToExist(archiveFile2, 4 * sampleRate, 10);
    waitForFileToExist(archiveFile3, 4 * sampleRate, 10);
    waitForFileToExist(archiveFile4, 4 * sampleRate, 10);
    waitForFileToExist(archiveFile5, 4 * sampleRate, 10);
    waitForFileToDelete(archiveFile1, 10 * sampleRate, 10);
  }

  @Override
  protected StatisticsManager getStatisticsManager() {
    return this.statisticsFactory;
  }

  private SimpleStatSampler getSimpleStatSampler() {
    return this.statisticsFactory.getStatSampler();
  }

  private void initStatisticsFactory() {
    CancelCriterion stopper = new CancelCriterion() {
      @Override
      public String cancelInProgress() {
        return null;
      }

      @Override
      public RuntimeException generateCancelledException(Throwable e) {
        return null;
      }
    };
    this.statisticsFactory = new LocalStatisticsFactory(stopper);
  }

  private void closeStatisticsFactory() {
    if (this.statisticsFactory != null) {
      this.statisticsFactory.close();
      this.statisticsFactory = null;
    }
  }
}
