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
 *
 */
package org.apache.geode.management.internal.cli.commands;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.io.FileUtils.listFiles;
import static org.apache.geode.management.internal.cli.commands.ExportLogsCommand.FORMAT;
import static org.apache.geode.management.internal.cli.commands.ExportLogsCommand.ONLY_DATE_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.cli.functions.ExportLogsFunction;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.Member;

public class ExportLogsDistributedTestBase {

  private static final String ERROR_LOG_PREFIX = "[IGNORE]";

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule().withLogFile();

  @Rule
  public GfshCommandRule gfshConnector = new GfshCommandRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  protected MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;

  private Map<MemberVM, List<LogLine>> expectedMessages;

  public File getWorkingDirectory() throws Exception {
    return locator.getWorkingDir();
  }

  @Before
  public void setup() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(ConfigurationProperties.LOG_LEVEL, "debug");

    locator = lsRule.startLocatorVM(0, l -> l.withProperties(properties).withHttpService());
    server1 = lsRule.startServerVM(1, properties, locator.getPort());
    server2 = lsRule.startServerVM(2, properties, locator.getPort());

    IgnoredException.addIgnoredException(ERROR_LOG_PREFIX);

    expectedMessages = new HashMap<>();
    expectedMessages.put(locator, listOfLogLines(locator, "info", "error", "debug"));
    expectedMessages.put(server1, listOfLogLines(server1, "info", "error", "debug"));
    expectedMessages.put(server2, listOfLogLines(server2, "info", "error", "debug"));

    // log the messages in each of the members
    for (MemberVM member : expectedMessages.keySet()) {
      List<LogLine> logLines = expectedMessages.get(member);

      member.invoke(() -> {
        Logger logger = LogService.getLogger();
        logLines.forEach((LogLine logLine) -> logLine.writeLog(logger));
      });
    }

    connect();
  }

  public void connect() throws Exception {
    gfshConnector.connectAndVerify(locator);
  }

  @After
  public void after() throws Exception {
    Stream.of(getWorkingDirectory().listFiles())
        .filter(f -> f.getName().endsWith(".zip")).forEach(file -> file.delete());
  }

  @Test
  public void withFiles_savedToLocatorWorkingDir() throws Exception {
    String[] extensions = {"zip"};
    // Expects locator to produce file in own working directory when connected via JMX
    gfshConnector.executeCommand("export logs");
    assertThat(listFiles(getWorkingDirectory(), extensions, false)).isNotEmpty();
  }

  @Test
  public void withFiles_savedToLocatorSpecifiedRelativeDir() throws Exception {
    String[] extensions = {"zip"};
    Path workingDirPath = getWorkingDirectory().toPath();
    Path subdirPath = workingDirPath.resolve("some").resolve("test").resolve("directory");
    Path relativeDir = workingDirPath.relativize(subdirPath);
    // Expects locator to produce file in own working directory when connected via JMX
    gfshConnector.executeCommand("export logs --dir=" + relativeDir.toString());
    assertThat(listFiles(getWorkingDirectory(), extensions, false)).isEmpty();
    assertThat(listFiles(getWorkingDirectory(), extensions, true)).isNotEmpty();
    assertThat(listFiles(subdirPath.toFile(), extensions, false)).isNotEmpty();
  }

  @Test
  public void withFiles_savedToLocatorSpecifiedAbsoluteDir() throws Exception {
    String[] extensions = {"zip"};
    Path workingDirPath = getWorkingDirectory().toPath();
    Path absoluteDirPath =
        workingDirPath.resolve("some").resolve("test").resolve("directory").toAbsolutePath();
    // Expects locator to produce file in own working directory when connected via JMX
    gfshConnector.executeCommand("export logs --dir=" + absoluteDirPath.toString());
    assertThat(listFiles(getWorkingDirectory(), extensions, false)).isEmpty();
    assertThat(listFiles(getWorkingDirectory(), extensions, true)).isNotEmpty();
    assertThat(listFiles(absoluteDirPath.toFile(), extensions, false)).isNotEmpty();
  }

  @Test
  public void startAndEndDateCanIncludeLogs() throws Exception {
    ZonedDateTime now = LocalDateTime.now().atZone(ZoneId.systemDefault());
    ZonedDateTime yesterday = now.minusDays(1);
    ZonedDateTime tomorrow = now.plusDays(1);

    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(ONLY_DATE_FORMAT);

    CommandStringBuilder commandStringBuilder = new CommandStringBuilder("export logs");
    commandStringBuilder.addOption("start-time", dateTimeFormatter.format(yesterday));
    commandStringBuilder.addOption("end-time", dateTimeFormatter.format(tomorrow));
    commandStringBuilder.addOption("log-level", "debug");

    gfshConnector.executeAndAssertThat(commandStringBuilder.toString()).statusIsSuccess();

    Set<String> acceptedLogLevels = Stream.of("info", "error", "debug").collect(toSet());
    verifyZipFileContents(acceptedLogLevels);
  }

  @Test
  public void testExportWithStartAndEndDateTimeFiltering() throws Exception {
    ZonedDateTime cutoffTime = LocalDateTime.now().atZone(ZoneId.systemDefault());

    // wait for atleast 1 second to reduce flakiness on windows
    // on windows the flakiness is caused due to the cutoffTime
    // being same as the log message logged on server1.
    Thread.sleep(1);

    String messageAfterCutoffTime =
        "[this message should not show up since it is after cutoffTime]";
    LogLine logLineAfterCutoffTime = new LogLine(messageAfterCutoffTime, "info", true);
    server1.invoke(() -> {
      Logger logger = LogService.getLogger();
      logLineAfterCutoffTime.writeLog(logger);
    });

    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(FORMAT);
    String cutoffTimeString = dateTimeFormatter.format(cutoffTime);

    CommandStringBuilder commandStringBuilder = new CommandStringBuilder("export logs");
    commandStringBuilder.addOption("start-time", dateTimeFormatter.format(cutoffTime.minusDays(1)));
    commandStringBuilder.addOption("end-time", cutoffTimeString);
    commandStringBuilder.addOption("log-level", "debug");

    gfshConnector.executeAndAssertThat(commandStringBuilder.toString()).statusIsSuccess();

    expectedMessages.get(server1).add(logLineAfterCutoffTime);
    Set<String> acceptedLogLevels = Stream.of("info", "error", "debug").collect(toSet());
    verifyZipFileContents(acceptedLogLevels);
  }

  @Test
  public void testExportWithThresholdLogLevelFilter() throws Exception {

    gfshConnector.executeAndAssertThat("export logs --log-level=info --only-log-level=false")
        .statusIsSuccess();

    Set<String> acceptedLogLevels = Stream.of("info", "error").collect(toSet());
    verifyZipFileContents(acceptedLogLevels);

  }

  @Test
  public void testExportWithExactLogLevelFilter() throws Exception {
    gfshConnector.executeAndAssertThat("export logs --log-level=info --only-log-level=true")
        .statusIsSuccess();
    Set<String> acceptedLogLevels = Stream.of("info").collect(toSet());
    verifyZipFileContents(acceptedLogLevels);
  }

  @Test
  public void testExportWithNoOptionsGiven() throws Exception {
    gfshConnector.executeAndAssertThat("export logs").statusIsSuccess();
    Set<String> acceptedLogLevels = Stream.of("info", "error", "debug").collect(toSet());
    verifyZipFileContents(acceptedLogLevels);
  }

  @Test
  public void testExportWithNoFilters() throws Exception {
    gfshConnector.executeAndAssertThat("export logs --log-level=all").statusIsSuccess();

    Set<String> acceptedLogLevels = Stream.of("info", "error", "debug").collect(toSet());
    verifyZipFileContents(acceptedLogLevels);

    // Ensure export logs region gets cleaned up
    server1.invoke(ExportLogsDistributedTestBase::verifyExportLogsRegionWasDestroyed);
    server2.invoke(ExportLogsDistributedTestBase::verifyExportLogsRegionWasDestroyed);
    locator.invoke(ExportLogsDistributedTestBase::verifyExportLogsRegionWasDestroyed);
  }

  @Test
  public void exportLogsRegionIsCleanedUpProperly() {
    locator.invoke(() -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      ExportLogsFunction.createOrGetExistingExportLogsRegion(true, cache);
      assertThat(cache.getRegion(ExportLogsFunction.EXPORT_LOGS_REGION)).isNotNull();
    });

    server1.invoke(() -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      ExportLogsFunction.createOrGetExistingExportLogsRegion(false, cache);
      assertThat(cache.getRegion(ExportLogsFunction.EXPORT_LOGS_REGION)).isNotNull();
    });

    locator.invoke(() -> {
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      ExportLogsFunction.destroyExportLogsRegion(cache);

      assertThat(cache.getRegion(ExportLogsFunction.EXPORT_LOGS_REGION)).isNull();
    });

    server1.invoke(() -> {
      Cache cache = GemFireCacheImpl.getInstance();
      assertThat(cache.getRegion(ExportLogsFunction.EXPORT_LOGS_REGION)).isNull();
    });
  }


  private void verifyZipFileContents(Set<String> acceptedLogLevels) throws Exception {
    File unzippedLogFileDir = unzipExportedLogs();

    Set<File> dirsFromZipFile =
        Stream.of(unzippedLogFileDir.listFiles()).filter(File::isDirectory).collect(toSet());
    assertThat(dirsFromZipFile).hasSize(expectedMessages.keySet().size());

    Set<String> expectedDirNames =
        expectedMessages.keySet().stream().map(Member::getName).collect(toSet());
    Set<String> actualDirNames = dirsFromZipFile.stream().map(File::getName).collect(toSet());
    assertThat(actualDirNames).isEqualTo(expectedDirNames);

    System.out.println("Unzipped artifacts:");
    for (File dir : dirsFromZipFile) {
      verifyLogFileContents(acceptedLogLevels, dir);
    }
  }

  private void verifyLogFileContents(Set<String> acceptedLogLevels, File dirForMember)
      throws IOException {

    String memberName = dirForMember.getName();
    MemberVM member = expectedMessages.keySet().stream()
        .filter((Member aMember) -> aMember.getName().equals(memberName)).findFirst().get();

    assertThat(member).isNotNull();

    Set<String> fileNamesInDir =
        Stream.of(dirForMember.listFiles()).map(File::getName).collect(toSet());

    System.out.println(dirForMember.getCanonicalPath() + " : " + fileNamesInDir);

    File logFileForMember = new File(dirForMember, memberName + ".log");
    assertThat(logFileForMember).exists();

    String logFileContents = FileUtils.readLines(logFileForMember, Charset.defaultCharset())
        .stream().collect(joining("\n"));

    for (LogLine logLine : expectedMessages.get(member)) {
      boolean shouldExpectLogLine =
          acceptedLogLevels.contains(logLine.level) && !logLine.shouldBeIgnoredDueToTimestamp;

      if (shouldExpectLogLine) {
        assertThat(logFileContents).contains(logLine.getMessage());
      } else {
        assertThat(logFileContents).doesNotContain(logLine.getMessage());
      }
    }

  }

  private File unzipExportedLogs() throws Exception {
    File locatorWorkingDir = getWorkingDirectory();
    List<File> filesInDir = Stream.of(getWorkingDirectory().listFiles()).collect(toList());
    assertThat(filesInDir).isNotEmpty();


    List<File> zipFilesInDir = Stream.of(getWorkingDirectory().listFiles())
        .filter(f -> f.getName().endsWith(".zip")).collect(toList());
    assertThat(zipFilesInDir)
        .describedAs(filesInDir.stream().map(File::getAbsolutePath).collect(joining(",")))
        .hasSize(1);

    File unzippedLogFileDir = temporaryFolder.newFolder("unzippedLogs");
    ZipUtils.unzip(zipFilesInDir.get(0).getCanonicalPath(), unzippedLogFileDir.getCanonicalPath());
    return unzippedLogFileDir;
  }

  private List<LogLine> listOfLogLines(Member member, String... levels) {
    return Stream.of(levels).map(level -> new LogLine(member, level)).collect(toList());
  }

  private static void verifyExportLogsRegionWasDestroyed() {
    Cache cache = GemFireCacheImpl.getInstance();
    assertThat(cache.getRegion(ExportLogsFunction.EXPORT_LOGS_REGION)).isNull();
  }

  public static class LogLine implements Serializable {
    String level;
    String message;
    boolean shouldBeIgnoredDueToTimestamp;

    LogLine(String message, String level, boolean shouldBeIgnoredDueToTimestamp) {
      this.message = message;
      this.level = level;
      this.shouldBeIgnoredDueToTimestamp = shouldBeIgnoredDueToTimestamp;
    }

    LogLine(Member member, String level) {
      this.level = level;
      this.message = buildMessage(member.getName());
    }

    public String getMessage() {
      return message;
    }

    private String buildMessage(String memberName) {
      StringBuilder stringBuilder = new StringBuilder();
      if (Objects.equals(level, "error")) {
        stringBuilder.append(ERROR_LOG_PREFIX + "-");
      }
      stringBuilder.append(level).append("-");

      return stringBuilder.append(memberName).toString();
    }


    void writeLog(Logger logger) {
      switch (this.level) {
        case "info":
          logger.info(getMessage());
          break;
        case "error":
          logger.error(getMessage());
          break;
        case "debug":
          logger.debug(getMessage());
      }
    }
  }
}
