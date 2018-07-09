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

import static java.lang.String.valueOf;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.io.FileUtils.moveFileToDirectory;
import static org.apache.commons.io.FileUtils.sizeOfDirectory;
import static org.apache.commons.lang.StringUtils.leftPad;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.io.MainWithChildrenRollingFileHandler;
import org.apache.geode.internal.io.RollingFileHandler;
import org.apache.geode.internal.util.ArrayUtils;
import org.apache.geode.test.junit.categories.StatisticsTest;

/**
 * Flaky: GEODE-2790, GEODE-3205
 */
@Category({StatisticsTest.class})
@SuppressWarnings("unused")
public class DiskSpaceLimitIntegrationTest {

  private static final long FILE_SIZE_LIMIT = 256;
  private static final long DISK_SPACE_LIMIT = FILE_SIZE_LIMIT * 2;

  private File dir;
  private File dirOfDeletedFiles;

  private String name;

  private String archiveFileName;

  private LocalStatisticsFactory factory;
  private StatisticDescriptor[] statisticDescriptors;
  private StatisticsType statisticsType;
  private Statistics statistics;

  private RollingFileHandler testRollingFileHandler;

  private SampleCollector sampleCollector;
  private StatArchiveHandlerConfig config;

  private long initTimeStamp;

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

    this.name = this.testName.getMethodName();

    this.archiveFileName = new File(this.dir, this.name + ".gfs").getAbsolutePath();

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

    this.testRollingFileHandler = new TestableRollingFileHandler();

    this.sampleCollector = new SampleCollector(sampler);

    this.initTimeStamp = NanoTimer.getTime();
    this.timer.reset();
    this.nanosTimeStamp = this.timer.getLastResetTime() - getNanoRate();

    preConditions();
  }

  private void preConditions() throws Exception {
    validateNumberFiles(0);
  }

  @After
  public void tearDown() throws Exception {
    StatisticsTypeFactoryImpl.clear();
  }

  @Test
  public void zeroKeepsAllFiles() throws Exception {
    this.sampleCollector.initialize(this.config, this.initTimeStamp, this.testRollingFileHandler);

    when(this.config.getArchiveDiskSpaceLimit()).thenReturn(0L);
    sampleUntilFileExists(archiveFile(1));
    sampleUntilFileExists(archiveFile(2));
    assertThat(archiveFile(1)).exists();
    assertThat(archiveFile(2)).exists();
  }

  @Test
  public void aboveZeroDeletesOldestFile() throws Exception {
    this.sampleCollector.initialize(this.config, this.initTimeStamp, this.testRollingFileHandler);

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

  @Test
  public void aboveZeroDeletesPreviousFiles() throws Exception {
    int oldMainId = 1;
    int newMainId = 2;

    int numberOfPreviousFiles = 100;
    int numberOfLines = 100;
    createPreviousFiles(oldMainId, numberOfPreviousFiles, numberOfLines);

    validateNumberFiles(numberOfPreviousFiles);

    for (int childId = 1; childId <= numberOfPreviousFiles; childId++) {
      assertThat(archiveFile(oldMainId, childId)).exists();
    }

    // current archive file does not exist yet
    assertThat(archiveFile()).doesNotExist();

    // rolling files for mainId 2 do not exist yet
    assertThat(markerFile(newMainId)).doesNotExist();
    assertThat(archiveFile(newMainId, 1)).doesNotExist();

    when(this.config.getArchiveDiskSpaceLimit())
        .thenReturn(sizeOfDirectory(this.dir) / numberOfPreviousFiles);

    this.sampleCollector.initialize(this.config, this.initTimeStamp, this.testRollingFileHandler);

    assertThat(archiveFile()).exists().hasParent(this.dir);
    assertThat(markerFile(newMainId)).exists().hasParent(this.dir).hasBinaryContent(new byte[0]);

    assertThat(archiveFile(newMainId, 1)).doesNotExist();

    sampleNumberOfTimes(1);

    sampleUntilFileExists(archiveFile(newMainId, 1));
    assertThat(archiveFile(newMainId, 1)).exists();

    validateNumberFilesIsAtLeast(2);

    for (int childId = 1; childId <= numberOfPreviousFiles; childId++) {
      assertThat(archiveFile(oldMainId, childId)).doesNotExist();
    }
  }

  @Test
  public void aboveZeroDeletesPreviousFiles_nameWithHyphen() throws Exception {
    this.name = "psin8p724_cache1-statistics";
    this.archiveFileName = new File(this.dir, this.name + ".gfs").getAbsolutePath();
    when(this.config.getArchiveFileName()).thenReturn(new File(this.archiveFileName));

    int oldMainId = 1;
    int newMainId = 2;

    int numberOfPreviousFiles = 100;
    int numberOfLines = 100;
    createPreviousFiles(oldMainId, numberOfPreviousFiles, numberOfLines);

    validateNumberFiles(numberOfPreviousFiles);

    for (int childId = 1; childId <= numberOfPreviousFiles; childId++) {
      assertThat(archiveFile(oldMainId, childId)).exists();
    }

    // current archive file does not exist yet
    assertThat(archiveFile()).doesNotExist();

    // rolling files for mainId 2 do not exist yet
    assertThat(markerFile(newMainId)).doesNotExist();
    assertThat(archiveFile(newMainId, 1)).doesNotExist();

    when(this.config.getArchiveDiskSpaceLimit())
        .thenReturn(sizeOfDirectory(this.dir) / numberOfPreviousFiles);

    this.sampleCollector.initialize(this.config, this.initTimeStamp, this.testRollingFileHandler);

    assertThat(archiveFile()).exists().hasParent(this.dir);
    assertThat(markerFile(newMainId)).exists().hasParent(this.dir).hasBinaryContent(new byte[0]);

    assertThat(archiveFile(newMainId, 1)).doesNotExist();

    sampleNumberOfTimes(1);

    sampleUntilFileExists(archiveFile(newMainId, 1));
    assertThat(archiveFile(newMainId, 1)).exists();

    validateNumberFilesIsAtLeast(2);

    for (int childId = 1; childId <= numberOfPreviousFiles; childId++) {
      assertThat(archiveFile(oldMainId, childId)).doesNotExist();
    }
  }

  /**
   * Validates number of files under this.dir while ignoring this.dirOfDeletedFiles.
   */
  private void validateNumberFiles(final int expected) {
    assertThat(numberOfFiles(this.dir)).as("Unexpected files: " + listFiles(this.dir))
        .isEqualTo(expected);
  }

  /**
   * Validates number of files under this.dir while ignoring this.dirOfDeletedFiles.
   */
  private void validateNumberFilesIsAtLeast(final int expected) {
    assertThat(numberOfFiles(this.dir)).as("Unexpected files: " + listFiles(this.dir))
        .isGreaterThanOrEqualTo(expected);
  }

  private void sampleNumberOfTimes(final int value) throws InterruptedException {
    long minutes = 1;
    long timeout = System.nanoTime() + MINUTES.toNanos(minutes);
    int count = 0;
    do {
      sample(advanceNanosTimeStamp());
      count++;
      Thread.sleep(10);
    } while (count < value && System.nanoTime() < timeout);
    System.out.println("Sampled " + count + " times.");
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

  private File archiveFile() {
    return archiveFile(archiveName());
  }

  private String archiveName() {
    return this.name;
  }

  private File archiveFile(final String name) {
    return new File(this.dir, name + ".gfs");
  }

  private File archiveFile(final int childId) {
    return archiveFile(1, childId);
  }

  private File archiveFile(final int mainId, final int childId) {
    return archiveFile(archiveName(), mainId, childId);
  }

  private File archiveFile(final String name, final int mainId, final int childId) {
    return new File(this.dir, archiveName(name, mainId, childId) + ".gfs");
  }

  private String archiveName(final int mainId, final int childId) {
    return archiveName(archiveName(), mainId, childId);
  }

  private String archiveName(final String name, final int mainId, final int childId) {
    return name + "-" + formatId(mainId) + "-" + formatId(childId);
  }

  private File markerFile(final int mainId) {
    return markerFile(archiveName(), mainId);
  }

  private File markerFile(final String name, final int mainId) {
    return new File(this.dir, markerName(name, mainId));
  }

  private String markerName(final int mainId) {
    return markerName(archiveName(), mainId);
  }

  private String markerName(final String name, final int mainId) {
    return archiveName(name, mainId, 0) + ".marker";
  }

  private String formatId(final int id) {
    return String.format("%02d", id);
  }

  private List<File> listFiles(final File dir) {
    List<File> files = ArrayUtils.asList(dir.listFiles());
    files.remove(this.dirOfDeletedFiles);
    return files;
  }

  private int numberOfFiles(final File dir) {
    return listFiles(dir).size();
  }

  private void createPreviousFiles(final int mainId, final int fileCount, final int lineCount)
      throws IOException {
    createPreviousFiles(this.name, mainId, fileCount, lineCount);
  }

  private void createPreviousFiles(final String name, final int mainId, final int fileCount,
      final int lineCount) throws IOException {
    int childId = 1;
    List<String> lines = lines(lineCount);
    for (int i = 0; i < fileCount; i++) {
      File file = createFile(name, mainId, childId);
      writeFile(file, lines);
      childId++;
    }
  }

  private File createFile(final String name, final int mainId, final int childId) {
    File file = new File(this.dir, name + "-" + leftPad(valueOf(mainId), 2, "0") + "-"
        + leftPad(valueOf(childId), 2, "0") + ".gfs");
    return file;
  }

  private void writeFile(final File file, final List<String> lines) throws IOException {
    PrintWriter writer = new PrintWriter(file, "UTF-8");
    for (String line : lines) {
      writer.println(line);
    }
    writer.close();
  }

  private List<String> lines(final int lineCount) {
    List<String> lines = new ArrayList<>();
    for (int i = 0; i < lineCount; i++) {
      lines.add(this.testName.getMethodName());
    }
    return lines;
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
