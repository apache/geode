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

import static java.io.File.separator;
import static java.lang.Byte.MAX_VALUE;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.getProperty;
import static java.lang.System.setProperty;
import static java.util.Arrays.fill;
import static org.apache.geode.distributed.ConfigurationProperties.ARCHIVE_DISK_SPACE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.ARCHIVE_FILE_SIZE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLE_RATE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.apache.geode.internal.GemFireVersion.getBuildId;
import static org.apache.geode.internal.GemFireVersion.getGemFireVersion;
import static org.apache.geode.internal.GemFireVersion.getSourceDate;
import static org.apache.geode.internal.cache.control.HeapMemoryMonitor.getTenuredMemoryPoolMXBean;
import static org.apache.geode.internal.cache.control.HeapMemoryMonitor.getTenuredPoolStatistics;
import static org.apache.geode.internal.inet.LocalHostUtil.getLocalHost;
import static org.apache.geode.internal.statistics.HostStatSampler.TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;
import org.junit.After;
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
import org.apache.geode.internal.statistics.GemFireStatSampler.LocalStatListenerImpl;
import org.apache.geode.internal.statistics.platform.ProcessStats;
import org.apache.geode.internal.stats50.VMStats50;
import org.apache.geode.logging.internal.log4j.api.LogService;
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

  private InternalDistributedSystem system;
  private File testDir;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    testDir = temporaryFolder.getRoot();
    assertThat(testDir).exists();
  }

  /**
   * Removes the loner DistributedSystem at the end of each test.
   */
  @After
  public void tearDown() throws Exception {
    System.clearProperty(GemFireStatSampler.TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY);
    disconnect();
  }

  @Test
  public void testInitialization() throws Exception {
    connect(createGemFireProperties());

    GemFireStatSampler statSampler = getGemFireStatSampler();
    assertThat(statSampler.waitForInitialization(5000))
        .as("initialized within 5 seconds")
        .isTrue();

    assertThat(statSampler.getArchiveFileSizeLimit())
        .as("archive file size limit")
        .isZero();
    assertThat(statSampler.getArchiveDiskSpaceLimit())
        .as("archive disk space limit")
        .isZero();
    assertThat(statSampler.getSampleRate())
        .as("sample rate")
        .isEqualTo(STAT_SAMPLE_RATE);
    assertThat(statSampler.isSamplingEnabled())
        .as("sampling is enabled")
        .isTrue();

    int statsCount = statSampler.getStatisticsManager().getStatisticsCount();

    assertThat(statSampler.getStatistics().length)
        .as("statistics length")
        .isEqualTo(statsCount);

    assertThat(statSampler.getSystemStartTime())
        .as("system start time")
        .isLessThanOrEqualTo(currentTimeMillis());
    assertThat(statSampler.getSystemDirectoryPath())
        .as("system directory path")
        .isEqualTo(getLocalHost().getHostName());

    assertThat(statSampler.getVMStats())
        .as("vm stats")
        .isInstanceOf(VMStats50.class);
    /*
     * NOTE: VMStats50 is not an instance of Statistics but instead its instance contains 3
     * instances of Statistics: 1) vmStats 2) heapMemStats 3) nonHeapMemStats
     */

    Method getProcessStats = getGemFireStatSampler().getClass().getMethod("getProcessStats");
    assertThat(getProcessStats)
        .withFailMessage("gemfire stat sampler has no getProcessStats method")
        .isNotNull();
  }

  @Test
  public void testBasicProcessStats() throws Exception {
    final String osName = getProperty("os.name", "unknown");
    assumeThat(osName)
        .as("os name")
        .doesNotContain("Windows");

    connect(createGemFireProperties());
    GemFireStatSampler statSampler = getGemFireStatSampler();
    assertThat(statSampler.waitForInitialization(5000))
        .as("initialized within 5 seconds")
        .isTrue();

    ProcessStats processStats = statSampler.getProcessStats();
    AllStatistics allStats = new AllStatistics(statSampler);

    if (osName.startsWith("Linux")) {
      assertThat(processStats)
          .withFailMessage("ProcessStats were not created on" + osName)
          .isNotNull();
      assertThat(OsStatisticsProvider.build().osStatsSupported())
          .as("os stats are available on Linux")
          .isTrue();
      assertThat(allStats.containsStatisticsType("LinuxProcessStats"))
          .as("Linux stats include statistics type named LinuxProcessStats")
          .isTrue();
      assertThat(allStats.containsStatisticsType("LinuxSystemStats"))
          .as("Linux stats include statistics type named LinuxSystemStats")
          .isTrue();
    } else {
      assertThat(processStats)
          .withFailMessage("ProcessStats were created on" + osName)
          .isNull();
    }

    String productDesc = statSampler.getProductDescription();
    assertThat(productDesc)
        .as("product description")
        .contains(getGemFireVersion())
        .contains(getBuildId())
        .contains(getSourceDate());
  }

  /**
   * Tests that the configured archive file is created and exists.
   */
  @Test
  public void testArchiveFileExists() throws Exception {
    final String dir = testDir.getAbsolutePath();
    final String archiveFileName = dir + separator + testName.getMethodName() + ".gfs";

    final File archiveFile1 =
        new File(dir + separator + testName.getMethodName() + ".gfs");

    Properties props = createGemFireProperties();
    props.setProperty(STATISTIC_ARCHIVE_FILE, archiveFileName);
    connect(props);

    GemFireStatSampler statSampler = getGemFireStatSampler();
    assertThat(statSampler.waitForInitialization(5000))
        .as("initialized within 5 seconds")
        .isTrue();

    final File archiveFile = statSampler.getArchiveFileName();
    assertThat(archiveFile).isNotNull();
    assertThat(archiveFile)
        .as("archive file")
        .isEqualTo(archiveFile1);

    waitForFileToExist(archiveFile, 5000, 10);

    assertThat(archiveFile.getName())
        .as("archive file name")
        .isSubstringOf(archiveFileName);
  }

  /**
   * Tests the statistics sample rate within an acceptable margin of error.
   */
  @Test
  public void testSampleRate() throws Exception {
    connect(createGemFireProperties());

    GemFireStatSampler statSampler = getGemFireStatSampler();
    assertThat(statSampler.waitForInitialization(5000))
        .as("initialized within 5 seconds")
        .isTrue();

    assertThat(statSampler.getSampleRate())
        .as("sample rate")
        .isEqualTo(STAT_SAMPLE_RATE);

    assertThat(getStatisticsManager().getStatListModCount())
        .as("stat list mod count")
        .isNotZero();

    List<Statistics> statistics = getStatisticsManager().getStatsList();
    assertThat(statistics).isNotNull();
    assertThat(statistics.size())
        .as("statistics size")
        .isNotZero();

    StatisticsType statSamplerType = getStatisticsManager().findType("StatSampler");
    Statistics[] statsArray = getStatisticsManager().findStatisticsByType(statSamplerType);
    assertThat(statsArray.length)
        .as("stats array length")
        .isEqualTo(1);

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
    assertThat(statSampler.waitForInitialization(5000))
        .as("initialized within 5 seconds")
        .isTrue();

    Method getLocalListeners = getGemFireStatSampler().getClass().getMethod("getLocalListeners");
    assertThat(getLocalListeners).isNotNull();

    Method addLocalStatListener = getGemFireStatSampler().getClass()
        .getMethod("addLocalStatListener", LocalStatListener.class, Statistics.class, String.class);
    assertThat(addLocalStatListener).isNotNull();

    Method removeLocalStatListener = getGemFireStatSampler().getClass()
        .getMethod("removeLocalStatListener", LocalStatListener.class);
    assertThat(removeLocalStatListener).isNotNull();

    assertThat(statSampler.getLocalListeners())
        .as("local listeners before adding first listener")
        .isEmpty();

    // add a listener for sampleCount stat in StatSampler statistics
    StatisticsType statSamplerType = getStatisticsManager().findType("StatSampler");
    Statistics[] statsArray = getStatisticsManager().findStatisticsByType(statSamplerType);
    assertThat(statsArray.length)
        .as("stats array length")
        .isEqualTo(1);

    final Statistics statSamplerStats = statsArray[0];
    final String statName = "sampleCount";
    final AtomicInteger sampleCountValue = new AtomicInteger(0);
    final AtomicInteger sampleCountChanged = new AtomicInteger(0);

    LocalStatListener listener = value -> {
      sampleCountValue.set((int) value);
      sampleCountChanged.incrementAndGet();
    };

    statSampler.addLocalStatListener(listener, statSamplerStats, statName);
    assertThat(statSampler.getLocalListeners())
        .as("local listeners after adding 1 listener")
        .hasSize(1);

    // there's a level of indirection here and some protected member fields
    LocalStatListenerImpl lsli = statSampler.getLocalListeners().iterator().next();
    assertThat(lsli.stat.getName())
        .as("listener's first stat's name")
        .isEqualTo("sampleCount");

    await("listener to update several times").untilAsserted(
        () -> assertThat(sampleCountChanged).hasValueGreaterThanOrEqualTo(4));

    // validate that the listener fired and updated the value
    assertThat(sampleCountValue.get())
        .as("sample count value after the listener has fired")
        .isGreaterThan(0);

    // remove the listener
    statSampler.removeLocalStatListener(listener);
    final int expectedSampleCountValue = sampleCountValue.get();
    final int expectedSampleCountChanged = sampleCountChanged.get();

    assertThat(statSampler.getLocalListeners())
        .as("local listeners after removing the listener")
        .isEmpty();

    // wait for 2 stat samples to occur
    waitForStatSample(statSamplerStats, expectedSampleCountValue, 5000, 10);

    // validate that the listener did not fire
    assertThat(sampleCountValue.get())
        .as("sample count value after the listener was removed")
        .isEqualTo(expectedSampleCountValue);
    assertThat(sampleCountChanged.get())
        .as("sample count changed after the listener was removed")
        .isEqualTo(expectedSampleCountChanged);
  }

  /**
   * Invokes stop() and then validates that the sampler did in fact stop.
   */
  @Test
  public void testStop() throws Exception {
    connect(createGemFireProperties());

    GemFireStatSampler statSampler = getGemFireStatSampler();
    assertThat(statSampler.waitForInitialization(5000))
        .as("initialized within 5 seconds")
        .isTrue();

    // validate the stat sampler is running
    StatisticsType statSamplerType = getStatisticsManager().findType("StatSampler");
    Statistics[] statsArray = getStatisticsManager().findStatisticsByType(statSamplerType);
    assertThat(statsArray.length)
        .as("stats array length")
        .isEqualTo(1);

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

    assertThat(statSamplerStats.getInt("sampleCount"))
        .as("value of sample count stat after timing out")
        .isEqualTo(stoppedSampleCount);
  }

  /**
   * Verifies that archive rolling works correctly when archive-file-size-limit is specified.
   */
  @Test
  public void testArchiveRolling() throws Exception {
    final String dirName = testDir.getAbsolutePath() + separator + testName;
    new File(dirName).mkdirs();
    final String archiveFileName = dirName + separator + testName + ".gfs";

    final File archiveFile = new File(archiveFileName);
    final File archiveFile1 = new File(dirName + separator + testName + "-01-01.gfs");
    final File archiveFile2 = new File(dirName + separator + testName + "-01-02.gfs");
    final File archiveFile3 = new File(dirName + separator + testName + "-01-03.gfs");

    // set the system property to use KB instead of MB for file size
    setProperty(TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY, "true");
    Properties props = createGemFireProperties();
    props.setProperty(ARCHIVE_FILE_SIZE_LIMIT, "1");
    props.setProperty(ARCHIVE_DISK_SPACE_LIMIT, "0");
    props.setProperty(STATISTIC_ARCHIVE_FILE, archiveFileName);
    connect(props);

    assertThat(getGemFireStatSampler().waitForInitialization(5000))
        .as("initialized within 5 seconds")
        .isTrue();

    await().untilAsserted(
        () -> {
          SampleCollector sampleCollector = getSampleCollector();
          assertThat(sampleCollector)
              .as("sample collector")
              .isNotNull();
          assertThat(sampleCollector.getStatArchiveHandler())
              .as("stat archive handler")
              .isNotNull();
        });
    StatArchiveHandler statArchiveHandler = getSampleCollector().getStatArchiveHandler();
    StatArchiveHandlerConfig config = statArchiveHandler.getStatArchiveHandlerConfig();
    assertThat(config.getArchiveFileSizeLimit())
        .as("archive file size limit")
        .isEqualTo(1024);

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
    final String dirName = testDir.getAbsolutePath();// + File.separator + this.testName;
    new File(dirName).mkdirs();
    final String archiveFileName = dirName + separator + testName + ".gfs";

    final File archiveFile = new File(archiveFileName);
    final File archiveFile1 = new File(dirName + separator + testName + "-01-01.gfs");
    final File archiveFile2 = new File(dirName + separator + testName + "-01-02.gfs");
    final File archiveFile3 = new File(dirName + separator + testName + "-01-03.gfs");
    final File archiveFile4 = new File(dirName + separator + testName + "-01-04.gfs");

    final int sampleRate = 1000;

    setProperty(TEST_FILE_SIZE_LIMIT_IN_KB_PROPERTY, "true");
    Properties props = createGemFireProperties();
    props.setProperty(STATISTIC_ARCHIVE_FILE, archiveFileName);
    props.setProperty(ARCHIVE_FILE_SIZE_LIMIT, "2");
    props.setProperty(ARCHIVE_DISK_SPACE_LIMIT, "14");
    props.setProperty(STATISTIC_SAMPLE_RATE, String.valueOf(sampleRate));

    connect(props);
    assertThat(getGemFireStatSampler().waitForInitialization(5000))
        .as("initialized within 5 seconds")
        .isTrue();

    final AtomicBoolean rolloverArchiveFile1 = new AtomicBoolean(false);
    final AtomicBoolean rolloverArchiveFile2 = new AtomicBoolean(false);
    final AtomicBoolean rolloverArchiveFile3 = new AtomicBoolean(false);
    final AtomicBoolean rolloverArchiveFile4 = new AtomicBoolean(false);
    final AtomicBoolean currentArchiveFile = new AtomicBoolean(false);

    await("current archive file and four rollover archive files")
        .untilAsserted(() -> {
          currentArchiveFile.lazySet(currentArchiveFile.get() || archiveFile.exists());
          rolloverArchiveFile1.lazySet(rolloverArchiveFile1.get() || archiveFile1.exists());
          rolloverArchiveFile2.lazySet(rolloverArchiveFile2.get() || archiveFile2.exists());
          rolloverArchiveFile3.lazySet(rolloverArchiveFile3.get() || archiveFile3.exists());
          rolloverArchiveFile4.lazySet(rolloverArchiveFile4.get() || archiveFile4.exists());
          assertThat(rolloverArchiveFile1.get()
              && rolloverArchiveFile2.get()
              && rolloverArchiveFile3.get()
              && rolloverArchiveFile4.get()
              && currentArchiveFile.get())
                  .as("Waiting for archive files to exist:"
                      + " currentArchiveFile=" + currentArchiveFile
                      + " rolloverArchiveFile1=" + rolloverArchiveFile1
                      + " rolloverArchiveFile2=" + rolloverArchiveFile2
                      + " rolloverArchiveFile3=" + rolloverArchiveFile3
                      + " rolloverArchiveFile4=" + rolloverArchiveFile4)
                  .isTrue();
        });
    waitForFileToDelete(archiveFile1, 10 * sampleRate, 10);
  }

  @Test
  public void testLocalStatListenerRegistration() throws Exception {
    connect(createGemFireProperties());

    final GemFireStatSampler statSampler = getGemFireStatSampler();
    statSampler.waitForInitialization(5000);

    final AtomicBoolean flag = new AtomicBoolean(false);
    final LocalStatListener listener = value -> flag.set(true);

    final String tenuredPoolName = getTenuredMemoryPoolMXBean().getName();
    logger.info("TenuredPoolName: {}", tenuredPoolName);

    Statistics tenuredPoolStatistics =
        await("tenured pool statistics " + tenuredPoolName + " is not null")
            .until(() -> getTenuredPoolStatistics(system.getStatisticsManager()), Objects::nonNull);

    statSampler.addLocalStatListener(listener, tenuredPoolStatistics, "currentUsedMemory");

    assertThat(statSampler.getLocalListeners().size() > 0)
        .as("expected at least one stat listener, found " + statSampler.getLocalListeners().size())
        .isTrue();

    long maxTenuredMemory = getTenuredMemoryPoolMXBean().getUsage().getMax();

    byte[] bytes = new byte[(int) (maxTenuredMemory * 0.01)];
    fill(bytes, MAX_VALUE);

    await("listener to be triggered").untilTrue(flag);
  }

  @Override
  protected StatisticsManager getStatisticsManager() {
    return system.getStatisticsManager();
  }

  private GemFireStatSampler getGemFireStatSampler() {
    return system.getStatSampler();
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
    system = (InternalDistributedSystem) DistributedSystem.connect(props);
  }

  private void disconnect() {
    if (system != null) {
      system.disconnect();
      system = null;
    }
  }
}
