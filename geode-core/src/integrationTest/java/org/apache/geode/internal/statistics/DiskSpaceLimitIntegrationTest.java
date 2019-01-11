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
import static java.lang.System.lineSeparator;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.commons.io.FileUtils.moveFileToDirectory;
import static org.apache.commons.io.FileUtils.sizeOfDirectory;
import static org.apache.commons.lang3.StringUtils.leftPad;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.commons.io.FileUtils;
import org.assertj.core.api.AbstractFileAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.StatisticsType;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.io.MainWithChildrenRollingFileHandler;
import org.apache.geode.internal.io.RollingFileHandler;
import org.apache.geode.internal.util.ArrayUtils;
import org.apache.geode.test.junit.categories.StatisticsTest;

/**
 * Integration tests for rolling and deleting behavior of
 * {@link ConfigurationProperties#ARCHIVE_FILE_SIZE_LIMIT} and
 * {@link ConfigurationProperties#ARCHIVE_DISK_SPACE_LIMIT}.
 */
@Category(StatisticsTest.class)
public class DiskSpaceLimitIntegrationTest {

  private static final long FILE_SIZE_LIMIT_BYTES = 1024 * 2;
  private static final long DISK_SPACE_LIMIT_BYTES = FILE_SIZE_LIMIT_BYTES * 2;

  private File dir;
  private File dirOfDeletedFiles;

  private String name;
  private String archiveFileName;

  private RollingFileHandler movingRollingFileHandler;
  private SampleCollector sampleCollector;
  private StatArchiveHandlerConfig config;

  private long initTimeStamp;
  private long nanosTimeStamp;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    dir = temporaryFolder.getRoot();
    dirOfDeletedFiles = temporaryFolder.newFolder("deleted");

    name = testName.getMethodName();

    archiveFileName = new File(dir, name + ".gfs").getAbsolutePath();

    LocalStatisticsFactory factory = new LocalStatisticsFactory(null);
    StatisticDescriptor[] statisticDescriptors = new StatisticDescriptor[] {
        factory.createIntCounter("stat1", "description of stat1", "units", true)};
    StatisticsType statisticsType =
        factory.createType("statisticsType1", "statisticsType1", statisticDescriptors);
    factory.createAtomicStatistics(statisticsType, "statistics1", 1);

    StatisticsSampler sampler = mock(StatisticsSampler.class);
    when(sampler.getStatistics()).thenReturn(factory.getStatistics());

    config = mock(StatArchiveHandlerConfig.class);
    when(config.getArchiveFileName()).thenReturn(new File(archiveFileName));
    when(config.getArchiveFileSizeLimit()).thenReturn(FILE_SIZE_LIMIT_BYTES);
    when(config.getSystemId()).thenReturn(1L);
    when(config.getSystemStartTime()).thenReturn(System.currentTimeMillis());
    when(config.getSystemDirectoryPath()).thenReturn(dir.getAbsolutePath());
    when(config.getProductDescription()).thenReturn(testName.getMethodName());

    movingRollingFileHandler = new MovingRollingFileHandler();

    sampleCollector = new SampleCollector(sampler);

    NanoTimer timer = new NanoTimer();
    initTimeStamp = timer.getConstructionTime() - getNanoRate();
    nanosTimeStamp = timer.getConstructionTime();

    validateNumberFiles(0);
  }

  @After
  public void tearDown() throws Exception {
    StatisticsTypeFactoryImpl.clear();
  }

  @Test
  public void zeroKeepsAllFiles() throws Exception {
    sampleCollector.initialize(config, initTimeStamp, movingRollingFileHandler);
    when(config.getArchiveDiskSpaceLimit()).thenReturn(0L);

    File fileWithChildId1 = archiveFile(1);
    File fileWithChildId2 = archiveFile(2);

    // sample until file with childId 1 exists
    sampleUntilFileExists(fileWithChildId1);

    // sample until filed with childId 2 exists
    sampleUntilFileExists(fileWithChildId2);


    // both files should still exist
    assertThat(fileWithChildId1).withFailMessage(fileShouldExist(fileWithChildId1)).exists();
    assertThat(fileWithChildId2).withFailMessage(fileShouldExist(fileWithChildId2)).exists();
  }

  @Test
  public void aboveZeroDeletesOldestFile() throws Exception {
    sampleCollector.initialize(config, initTimeStamp, movingRollingFileHandler);
    when(config.getArchiveDiskSpaceLimit()).thenReturn(DISK_SPACE_LIMIT_BYTES);

    File fileWithChildId1 = archiveFile(1);
    File fileWithChildId2 = archiveFile(2);

    // sample until file with childId 1 exists
    sampleUntilFileExists(fileWithChildId1);

    // sample until file with childId 2 exists
    sampleUntilFileExists(fileWithChildId2);

    // sample until file with childId 1 is deleted (moved to deleted dir)
    sampleUntilFileDeleted(fileWithChildId1);
    assertThat(fileWithChildId1).withFailMessage(fileShouldNotExist(fileWithChildId1))
        .doesNotExist();

    // different file systems may have different children created/deleted
    int latestChildId = 2;
    for (; latestChildId < 10; latestChildId++) {
      if (archiveFile(latestChildId).exists()) {
        break;
      }
    }
    assertThat(latestChildId).isLessThan(10);

    File fileWithLatestChildId = archiveFile(latestChildId);

    assertThat(fileWithLatestChildId).withFailMessage(fileShouldExist(fileWithLatestChildId))
        .exists();
    assertThat(fileExisted(fileWithChildId1))
        .withFailMessage(fileShouldHaveExisted(fileWithChildId1)).isTrue();
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
      File fileWithOldMainId = archiveFile(oldMainId, childId);
      assertThatFileExists(fileWithOldMainId);
    }

    // current archive file does not exist yet
    File archiveFile = archiveFile();
    assertThatFileDoesNotExist(archiveFile);

    // rolling files for mainId 2 do not exist yet
    File markerFileWithNewMainId = markerFile(newMainId);
    assertThatFileDoesNotExist(markerFileWithNewMainId);

    File fileWithNewMainId = archiveFile(newMainId, 1);
    assertThatFileDoesNotExist(fileWithNewMainId);

    when(config.getArchiveDiskSpaceLimit())
        .thenReturn(sizeOfDirectory(dir) / numberOfPreviousFiles);

    sampleCollector.initialize(config, initTimeStamp, movingRollingFileHandler);

    assertThatFileExists(archiveFile).hasParent(dir);
    assertThatFileExists(markerFileWithNewMainId).hasParent(dir).hasBinaryContent(new byte[0]);

    assertThatFileDoesNotExist(fileWithNewMainId);

    sampleNumberOfTimes(1);

    sampleUntilFileExists(fileWithNewMainId);
    assertThatFileExists(fileWithNewMainId);

    validateNumberFilesIsAtLeast(2);

    for (int childId = 1; childId <= numberOfPreviousFiles; childId++) {
      File fileWithOldMainId = archiveFile(oldMainId, childId);
      assertThatFileDoesNotExist(fileWithOldMainId);
    }
  }

  @Test
  public void aboveZeroDeletesPreviousFiles_nameWithHyphen() throws Exception {
    name = "psin8p724_cache1-statistics";
    archiveFileName = new File(dir, name + ".gfs").getAbsolutePath();
    when(config.getArchiveFileName()).thenReturn(new File(archiveFileName));

    int oldMainId = 1;
    int newMainId = 2;

    int numberOfPreviousFiles = 100;
    int numberOfLines = 100;
    createPreviousFiles(oldMainId, numberOfPreviousFiles, numberOfLines);
    validateNumberFiles(numberOfPreviousFiles);

    for (int childId = 1; childId <= numberOfPreviousFiles; childId++) {
      File fileWithOldMainId = archiveFile(oldMainId, childId);
      assertThatFileExists(fileWithOldMainId);
    }

    // current archive file does not exist yet
    File archiveFile = archiveFile();
    assertThatFileDoesNotExist(archiveFile);

    // rolling files for mainId 2 do not exist yet
    File markerFileWithNewMainId = markerFile(newMainId);
    assertThatFileDoesNotExist(markerFileWithNewMainId);

    File fileWithNewMainId = archiveFile(newMainId, 1);
    assertThatFileDoesNotExist(fileWithNewMainId);

    when(config.getArchiveDiskSpaceLimit())
        .thenReturn(sizeOfDirectory(dir) / numberOfPreviousFiles);

    sampleCollector.initialize(config, initTimeStamp, movingRollingFileHandler);

    assertThatFileExists(archiveFile).hasParent(dir);
    assertThatFileExists(markerFileWithNewMainId).hasParent(dir).hasBinaryContent(new byte[0]);

    assertThatFileDoesNotExist(fileWithNewMainId);

    sampleNumberOfTimes(1);

    sampleUntilFileExists(fileWithNewMainId);
    assertThatFileExists(fileWithNewMainId);

    validateNumberFilesIsAtLeast(2);

    for (int childId = 1; childId <= numberOfPreviousFiles; childId++) {
      File fileWithOldMainId = archiveFile(oldMainId, childId);
      assertThatFileDoesNotExist(fileWithOldMainId);
    }
  }

  private AbstractFileAssert<?> assertThatFileExists(File file) {
    return assertThat(file).withFailMessage(fileShouldExist(file)).exists();
  }

  private void assertThatFileDoesNotExist(File file) {
    assertThat(file).withFailMessage(fileShouldNotExist(file)).doesNotExist();
  }

  private String fileShouldExist(File file) {
    return "Expecting:" + lineSeparator() + " " + file.getAbsolutePath() + lineSeparator() +
        " to exist in:" + lineSeparator() + " " + dir.getAbsolutePath() + lineSeparator() +
        " but only found:" + lineSeparator() + " " + filesAndSizes();
  }

  private String fileShouldNotExist(File file) {
    return " Expecting:" + lineSeparator() + " " + file.getAbsolutePath() + lineSeparator() +
        " to not exist in:" + lineSeparator() + " " + dir.getAbsolutePath() + lineSeparator() +
        " but found:" + lineSeparator() + " " + filesAndSizes();
  }

  private String fileShouldHaveExisted(File file) {
    return "Expecting:" + lineSeparator() + " " + file.getAbsolutePath() + lineSeparator() +
        " to exist in:" + lineSeparator() + " " + dir.getAbsolutePath() + lineSeparator() +
        " or moved to:" + lineSeparator() + " " + dirOfDeletedFiles.getAbsolutePath()
        + lineSeparator() +
        " but only found:" + lineSeparator() + " " + filesAndSizes();
  }

  private Map<File, String> filesAndSizes() {
    Collection<File> existingFiles = FileUtils.listFiles(dir, null, true);
    Map<File, String> filesAndSizes = new HashMap<>();
    for (File existingFile : existingFiles) {
      filesAndSizes.put(existingFile, FileUtils.sizeOf(existingFile) + " bytes");
    }
    return filesAndSizes;
  }

  /**
   * Validates number of files under this.dir while ignoring this.dirOfDeletedFiles.
   */
  private void validateNumberFiles(final int expected) {
    assertThat(numberOfFiles(dir)).as("Unexpected files: " + listFiles(dir))
        .isEqualTo(expected);
  }

  /**
   * Validates number of files under this.dir while ignoring this.dirOfDeletedFiles.
   */
  private void validateNumberFilesIsAtLeast(final int expected) {
    assertThat(numberOfFiles(dir)).as("Unexpected files: " + listFiles(dir))
        .isGreaterThanOrEqualTo(expected);
  }

  private void sampleNumberOfTimes(final int value) throws InterruptedException {
    long minutes = 1;
    long timeout = System.nanoTime() + MINUTES.toNanos(minutes);
    int count = 0;
    do {
      sample(advanceNanosTimeStamp());
      count++;
      Thread.sleep(10); // avoid hot thread loop
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
      Thread.sleep(10); // avoid hot thread loop
    } while (!fileExisted(file) && System.nanoTime() < timeout);
    if (!fileExisted(file)) {
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
      Thread.sleep(10); // avoid hot thread loop
    } while (file.exists() && System.nanoTime() < timeout);
    if (file.exists()) {
      throw new TimeoutException("File " + file + " does not exist after " + count
          + " samples within " + minutes + " " + MINUTES);
    }
    System.out.println("Sampled " + count + " times to delete " + file);
  }

  private boolean fileExisted(final File file) {
    if (file.exists()) {
      return true;
    } else { // check dirOfDeletedFiles
      File deleted = new File(dirOfDeletedFiles, file.getName());
      return deleted.exists();
    }
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

  private File archiveFile() {
    return archiveFile(archiveName());
  }

  private String archiveName() {
    return name;
  }

  private File archiveFile(final String name) {
    return new File(dir, name + ".gfs");
  }

  private File archiveFile(final int childId) {
    return archiveFile(1, childId);
  }

  private File archiveFile(final int mainId, final int childId) {
    return archiveFile(archiveName(), mainId, childId);
  }

  private File archiveFile(final String name, final int mainId, final int childId) {
    return new File(dir, archiveName(name, mainId, childId) + ".gfs");
  }

  private String archiveName(final String name, final int mainId, final int childId) {
    return name + "-" + formatId(mainId) + "-" + formatId(childId);
  }

  private File markerFile(final int mainId) {
    return markerFile(archiveName(), mainId);
  }

  private File markerFile(final String name, final int mainId) {
    return new File(dir, markerName(name, mainId));
  }

  private String markerName(final String name, final int mainId) {
    return archiveName(name, mainId, 0) + ".marker";
  }

  private String formatId(final int id) {
    return String.format("%02d", id);
  }

  private List<File> listFiles(final File dir) {
    List<File> files = ArrayUtils.asList(dir.listFiles());
    files.remove(dirOfDeletedFiles);
    return files;
  }

  private int numberOfFiles(final File dir) {
    return listFiles(dir).size();
  }

  private void createPreviousFiles(final int mainId, final int fileCount, final int lineCount)
      throws IOException {
    createPreviousFiles(name, mainId, fileCount, lineCount);
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
    File file = new File(dir, name + "-" + leftPad(valueOf(mainId), 2, "0") + "-"
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
      lines.add(testName.getMethodName());
    }
    return lines;
  }

  /**
   * Override protected method to move file instead of deleting it.
   */
  private class MovingRollingFileHandler extends MainWithChildrenRollingFileHandler {
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
