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

import static org.apache.geode.distributed.ConfigurationProperties.ARCHIVE_DISK_SPACE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.ARCHIVE_FILE_SIZE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLE_RATE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.PureJavaMode;
import org.apache.geode.internal.cache.control.HeapMemoryMonitor;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.statistics.GemFireStatSampler.LocalStatListenerImpl;
import org.apache.geode.internal.statistics.platform.OsStatisticsFactory;
import org.apache.geode.internal.statistics.platform.ProcessStats;
import org.apache.geode.internal.stats50.VMStats50;
import org.apache.geode.internal.util.StopWatch;
import org.apache.geode.test.junit.categories.StatisticsTest;

/**
 * Integration tests for {@link GemFireStatSampler}.
 *
 * @since GemFire 7.0
 */
@Category({StatisticsTest.class})
public class GemFireStatSamplerIntegrationTest extends StatSamplerTestCase {

  private static final Logger logger = LogService.getLogger();

  private static final int STAT_SAMPLE_RATE = 1000;

  private DistributedSystem system;
  private File testDir;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    this.testDir = this.temporaryFolder.getRoot();
    assertTrue(this.testDir.exists());
  }

  /**
   * Removes the loner DistributedSystem at the end of each test.
   */
  @After
  public void tearDown() throws Exception {
    System.clearProperty(GemFireStatSampler.TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY);
    disconnect();
  }

  /**
   * Tests the majority of getters and the basic functionality of the sampler.
   *
   * This test is skipped when running on Windows 7 because ProcessStats is not created for this OS.
   * See #45395.
   */
  @Test
  public void testBasics() throws Exception {
    connect(createGemFireProperties());

    GemFireStatSampler statSampler = getGemFireStatSampler();
    assertTrue(statSampler.waitForInitialization(5000));

    assertEquals(0, statSampler.getArchiveFileSizeLimit());
    assertEquals(0, statSampler.getArchiveDiskSpaceLimit());
    assertEquals(STAT_SAMPLE_RATE, statSampler.getSampleRate());
    assertEquals(true, statSampler.isSamplingEnabled());

    int statsCount = statSampler.getStatisticsManager().getStatisticsCount();

    assertEquals(statsCount, statSampler.getStatisticsModCount());
    assertEquals(statsCount, statSampler.getStatisticsManager().getStatisticsCount());
    assertEquals(statsCount, statSampler.getStatistics().length);

    Assert.assertEquals(getStatisticsManager().getId(), statSampler.getSystemId());
    assertTrue(statSampler.getSystemStartTime() < System.currentTimeMillis());
    Assert.assertEquals(SocketCreator.getHostName(SocketCreator.getLocalHost()),
        statSampler.getSystemDirectoryPath());

    VMStatsContract vmStats = statSampler.getVMStats();
    assertNotNull(vmStats);
    assertTrue(vmStats instanceof VMStats50);
    /*
     * NOTE: VMStats50 is not an instance of Statistics but instead its instance contains 3
     * instances of Statistics: 1) vmStats 2) heapMemStats 3) nonHeapMemStats
     */

    Method getProcessStats = getGemFireStatSampler().getClass().getMethod("getProcessStats");
    assertNotNull(getProcessStats);
  }

  @Test
  public void testBasicProcessStats() throws Exception {
    final String osName = System.getProperty("os.name", "unknown");
    assumeFalse(osName.contains("Windows"));

    connect(createGemFireProperties());
    GemFireStatSampler statSampler = getGemFireStatSampler();
    assertTrue(statSampler.waitForInitialization(5000));

    ProcessStats processStats = statSampler.getProcessStats();
    AllStatistics allStats = new AllStatistics(statSampler);

    if (osName.equals("SunOS")) {
      assertNotNull(processStats);
      assertTrue(PureJavaMode.osStatsAreAvailable());
      assertTrue(allStats.containsStatisticsType("SolarisProcessStats"));
      assertTrue(allStats.containsStatisticsType("SolarisSystemStats"));
    } else if (osName.startsWith("Windows")) {
      // fails on Windows 7: 45395 "ProcessStats are not created on Windows 7"
      assertNotNull("ProcessStats were not created on " + osName, processStats);
      assertTrue(PureJavaMode.osStatsAreAvailable());
      assertTrue(allStats.containsStatisticsType("WindowsProcessStats"));
      assertTrue(allStats.containsStatisticsType("WindowsSystemStats"));
    } else if (osName.startsWith("Linux")) {
      assertNotNull(processStats);
      assertTrue(PureJavaMode.osStatsAreAvailable());
      assertTrue(allStats.containsStatisticsType("LinuxProcessStats"));
      assertTrue(allStats.containsStatisticsType("LinuxSystemStats"));
    } else if (osName.equals("Mac OS X")) {
      assertNull(processStats);
      assertFalse(PureJavaMode.osStatsAreAvailable());
      assertFalse(allStats.containsStatisticsType("OSXProcessStats"));
      assertFalse(allStats.containsStatisticsType("OSXSystemStats"));
    } else {
      assertNull(processStats);
    }

    String productDesc = statSampler.getProductDescription();
    assertTrue(productDesc.contains(GemFireVersion.getGemFireVersion()));
    assertTrue(productDesc.contains(GemFireVersion.getBuildId()));
    assertTrue(productDesc.contains(GemFireVersion.getSourceDate()));
  }

  /**
   * Tests that the configured archive file is created and exists.
   */
  @Test
  public void testArchiveFileExists() throws Exception {
    final String dir = this.testDir.getAbsolutePath();
    final String archiveFileName = dir + File.separator + this.testName.getMethodName() + ".gfs";

    final File archiveFile1 =
        new File(dir + File.separator + this.testName.getMethodName() + ".gfs");

    Properties props = createGemFireProperties();
    props.setProperty(STATISTIC_ARCHIVE_FILE, archiveFileName);
    connect(props);

    GemFireStatSampler statSampler = getGemFireStatSampler();
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
    connect(createGemFireProperties());

    GemFireStatSampler statSampler = getGemFireStatSampler();
    assertTrue(statSampler.waitForInitialization(5000));

    assertEquals(STAT_SAMPLE_RATE, statSampler.getSampleRate());

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

    waitForExpectedStatValue(statSamplerStats, "sampleCount", expectedSampleCount, 5000, 10);
  }

  /**
   * Adds a LocalStatListener for an individual stat. Validates that it receives notifications.
   * Removes the listener and validates that it was in fact removed and no longer receives
   * notifications.
   */
  @Test
  public void testLocalStatListener() throws Exception {
    connect(createGemFireProperties());

    GemFireStatSampler statSampler = getGemFireStatSampler();
    assertTrue(statSampler.waitForInitialization(5000));

    Method getLocalListeners = getGemFireStatSampler().getClass().getMethod("getLocalListeners");
    assertNotNull(getLocalListeners);

    Method addLocalStatListener = getGemFireStatSampler().getClass()
        .getMethod("addLocalStatListener", LocalStatListener.class, Statistics.class, String.class);
    assertNotNull(addLocalStatListener);

    Method removeLocalStatListener = getGemFireStatSampler().getClass()
        .getMethod("removeLocalStatListener", LocalStatListener.class);
    assertNotNull(removeLocalStatListener);

    // validate that there are no listeners
    assertTrue(statSampler.getLocalListeners().isEmpty());

    // add a listener for sampleCount stat in StatSampler statistics
    StatisticsType statSamplerType = getStatisticsManager().findType("StatSampler");
    Statistics[] statsArray = getStatisticsManager().findStatisticsByType(statSamplerType);
    assertEquals(1, statsArray.length);

    final Statistics statSamplerStats = statsArray[0];
    final String statName = "sampleCount";
    final AtomicInteger sampleCountValue = new AtomicInteger(0);
    final AtomicInteger sampleCountChanged = new AtomicInteger(0);

    LocalStatListener listener = new LocalStatListener() {
      public void statValueChanged(double value) {
        sampleCountValue.set((int) value);
        sampleCountChanged.incrementAndGet();
      }
    };

    statSampler.addLocalStatListener(listener, statSamplerStats, statName);
    assertTrue(statSampler.getLocalListeners().size() == 1);

    // there's a level of indirection here and some protected member fields
    LocalStatListenerImpl lsli =
        (LocalStatListenerImpl) statSampler.getLocalListeners().iterator().next();
    assertEquals("sampleCount", lsli.stat.getName());

    // wait for the listener to update 4 times
    final int expectedChanges = 4;
    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < 5000; done =
          (sampleCountChanged.get() >= expectedChanges)) {
        Thread.sleep(10);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue("Waiting for sampleCountChanged >= " + expectedChanges, done);

    // validate that the listener fired and updated the value
    assertTrue(sampleCountValue.get() > 0);
    assertTrue(sampleCountChanged.get() >= expectedChanges);

    // remove the listener
    statSampler.removeLocalStatListener(listener);
    final int expectedSampleCountValue = sampleCountValue.get();
    final int expectedSampleCountChanged = sampleCountChanged.get();

    // validate that there are no listeners now
    assertTrue(statSampler.getLocalListeners().isEmpty());

    // wait for 2 stat samples to occur
    waitForStatSample(statSamplerStats, expectedSampleCountValue, 5000, 10);

    // validate that the listener did not fire
    assertEquals(expectedSampleCountValue, sampleCountValue.get());
    assertEquals(expectedSampleCountChanged, sampleCountChanged.get());
  }

  /**
   * Invokes stop() and then validates that the sampler did in fact stop.
   */
  @Test
  public void testStop() throws Exception {
    connect(createGemFireProperties());

    GemFireStatSampler statSampler = getGemFireStatSampler();
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

    // the following should timeout rather than complete
    assertStatValueDoesNotChange(statSamplerStats, "sampleCount", stoppedSampleCount, 5000, 10);

    assertEquals(stoppedSampleCount, statSamplerStats.getInt("sampleCount"));
  }

  /**
   * Verifies that archive rolling works correctly when archive-file-size-limit is specified.
   */
  @Test
  public void testArchiveRolling() throws Exception {
    final String dirName = this.testDir.getAbsolutePath() + File.separator + this.testName;
    new File(dirName).mkdirs();
    final String archiveFileName = dirName + File.separator + this.testName + ".gfs";

    final File archiveFile = new File(archiveFileName);
    final File archiveFile1 = new File(dirName + File.separator + this.testName + "-01-01.gfs");
    final File archiveFile2 = new File(dirName + File.separator + this.testName + "-01-02.gfs");
    final File archiveFile3 = new File(dirName + File.separator + this.testName + "-01-03.gfs");

    // set the system property to use KB instead of MB for file size
    System.setProperty(HostStatSampler.TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY, "true");
    Properties props = createGemFireProperties();
    props.setProperty(ARCHIVE_FILE_SIZE_LIMIT, "1");
    props.setProperty(ARCHIVE_DISK_SPACE_LIMIT, "0");
    props.setProperty(STATISTIC_ARCHIVE_FILE, archiveFileName);
    connect(props);

    assertTrue(getGemFireStatSampler().waitForInitialization(5000));

    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < 4000; done =
          (getSampleCollector() != null && getSampleCollector().getStatArchiveHandler() != null)) {
        Thread.sleep(10);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue("Waiting for getSampleCollector().getStatArchiveHandler() to not be null", done);

    StatArchiveHandler statArchiveHandler = getSampleCollector().getStatArchiveHandler();
    StatArchiveHandlerConfig config = statArchiveHandler.getStatArchiveHandlerConfig();
    assertEquals(1 * 1024, config.getArchiveFileSizeLimit());

    waitForFileToExist(archiveFile, 4000, 10);
    waitForFileToExist(archiveFile1, 4000, 10);
    waitForFileToExist(archiveFile2, 4000, 10);
    waitForFileToExist(archiveFile3, 4000, 10);
  }

  /**
   * Verifies that archive removal works correctly when archive-disk-space-limit is specified.
   */
  @Test
  public void testArchiveRemoval() throws Exception {
    final String dirName = this.testDir.getAbsolutePath();// + File.separator + this.testName;
    new File(dirName).mkdirs();
    final String archiveFileName = dirName + File.separator + this.testName + ".gfs";

    final File archiveFile = new File(archiveFileName);
    final File archiveFile1 = new File(dirName + File.separator + this.testName + "-01-01.gfs");
    final File archiveFile2 = new File(dirName + File.separator + this.testName + "-01-02.gfs");
    final File archiveFile3 = new File(dirName + File.separator + this.testName + "-01-03.gfs");
    final File archiveFile4 = new File(dirName + File.separator + this.testName + "-01-04.gfs");

    final int sampleRate = 1000;

    // set the system property to use KB instead of MB for file size
    System.setProperty(HostStatSampler.TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY, "true");
    Properties props = createGemFireProperties();
    props.setProperty(STATISTIC_ARCHIVE_FILE, archiveFileName);
    props.setProperty(ARCHIVE_FILE_SIZE_LIMIT, "1");
    props.setProperty(ARCHIVE_DISK_SPACE_LIMIT, "12");
    props.setProperty(STATISTIC_SAMPLE_RATE, String.valueOf(sampleRate));
    connect(props);

    assertTrue(getGemFireStatSampler().waitForInitialization(5000));

    boolean exists1 = false;
    boolean exists2 = false;
    boolean exists3 = false;
    boolean exists4 = false;
    boolean exists = false;
    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done
          && time.elapsedTimeMillis() < 10 * sampleRate;) {
        exists1 = exists1 || archiveFile1.exists();
        exists2 = exists2 || archiveFile2.exists();
        exists3 = exists3 || archiveFile3.exists();
        exists4 = exists4 || archiveFile4.exists();
        exists = exists || archiveFile.exists();
        done = exists1 && exists2 && exists3 && exists4 && exists;
        if (!done) {
          Thread.sleep(10);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue("Waiting for archive files to exist:" + " exists1=" + exists1 + " exists2=" + exists2
        + " exists3=" + exists3 + " exists4=" + exists4 + " exists=" + exists, done);

    waitForFileToDelete(archiveFile1, 10 * sampleRate, 10);
  }

  @Test
  public void testLocalStatListenerRegistration() throws Exception {
    connect(createGemFireProperties());

    final GemFireStatSampler statSampler = getGemFireStatSampler();
    statSampler.waitForInitialization(5000);

    final AtomicBoolean flag = new AtomicBoolean(false);
    final LocalStatListener listener = new LocalStatListener() {
      public void statValueChanged(double value) {
        flag.set(true);
      }
    };

    final String tenuredPoolName = HeapMemoryMonitor.getTenuredMemoryPoolMXBean().getName();
    logger.info("TenuredPoolName: {}", tenuredPoolName);

    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < 5000;) {
        Thread.sleep(10);
        Statistics si =
            HeapMemoryMonitor.getTenuredPoolStatistics((StatisticsManager) this.system);
        if (si != null) {
          statSampler.addLocalStatListener(listener, si, "currentUsedMemory");
          done = true;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue("Waiting for " + tenuredPoolName + " statistics to be added to create listener for",
        done);

    assertTrue(
        "expected at least one stat listener, found " + statSampler.getLocalListeners().size(),
        statSampler.getLocalListeners().size() > 0);

    long maxTenuredMemory = HeapMemoryMonitor.getTenuredMemoryPoolMXBean().getUsage().getMax();

    // byte[] bytes = new byte[1024 * 1024 * 10];
    byte[] bytes = new byte[(int) (maxTenuredMemory * 0.01)];
    Arrays.fill(bytes, Byte.MAX_VALUE);

    done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < 5000; done =
          (flag.get())) {
        Thread.sleep(10);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue("Waiting for listener to set flag to true", done);
  }

  @Override
  protected StatisticsManager getStatisticsManager() {
    return (InternalDistributedSystem) this.system;
  }

  protected OsStatisticsFactory getOsStatisticsFactory() {
    return (InternalDistributedSystem) this.system;
  }

  private GemFireStatSampler getGemFireStatSampler() {
    return ((InternalDistributedSystem) this.system).getStatSampler();
  }

  private SampleCollector getSampleCollector() {
    return getGemFireStatSampler().getSampleCollector();
  }

  private Properties createGemFireProperties() {
    Properties props = new Properties();
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true"); // TODO: test true/false
    props.setProperty(ENABLE_TIME_STATISTICS, "true"); // TODO: test true/false
    props.setProperty(STATISTIC_SAMPLE_RATE, String.valueOf(STAT_SAMPLE_RATE));
    props.setProperty(ARCHIVE_FILE_SIZE_LIMIT, "0");
    props.setProperty(ARCHIVE_DISK_SPACE_LIMIT, "0");
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    return props;
  }

  /**
   * Creates a fresh loner DistributedSystem for each test. Note that the DistributedSystem is the
   * StatisticsManager/Factory/etc.
   */
  @SuppressWarnings("deprecation")
  private void connect(Properties props) {
    this.system = DistributedSystem.connect(props);
  }

  @SuppressWarnings("deprecation")
  private void disconnect() {
    if (this.system != null) {
      this.system.disconnect();
      this.system = null;
    }
  }

  // public static class AsyncInvoker {
  // public static AsyncInvocation invokeAsync(Runnable r) {
  // return invokeAsync(r, "run", new Object[0]);
  // }
  // public static AsyncInvocation invokeAsync(Callable c) {
  // return invokeAsync(c, "call", new Object[0]);
  // }
  // public static AsyncInvocation invokeAsync(
  // final Object o, final String methodName, final Object[] args) {
  // AsyncInvocation ai =
  // new AsyncInvocation(o, methodName, new Runnable() {
  // public void run() {
  // MethExecutorResult result =
  // MethExecutor.executeObject(o, methodName, args);
  // if (result.exceptionOccurred()) {
  // throw new AsyncInvocationException(result.getException());
  // }
  // AsyncInvocation.setReturnValue(result.getResult());
  // }
  // });
  // ai.start();
  // return ai;
  // }
  //
  // public static class AsyncInvocationException extends RuntimeException {
  // private static final long serialVersionUID = -5522299018650622945L;
  // /**
  // * Creates a new <code>AsyncInvocationException</code>.
  // */
  // public AsyncInvocationException(String message) {
  // super(message);
  // }
  //
  // /**
  // * Creates a new <code>AsyncInvocationException</code> that was
  // * caused by a given exception
  // */
  // public AsyncInvocationException(String message, Throwable thr) {
  // super(message, thr);
  // }
  //
  // /**
  // * Creates a new <code>AsyncInvocationException</code> that was
  // * caused by a given exception
  // */
  // public AsyncInvocationException(Throwable thr) {
  // super(thr.getMessage(), thr);
  // }
  // }
  // }
}
