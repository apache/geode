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
package org.apache.geode.management.internal.cli.commands;

import static java.util.stream.Collectors.toSet;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.apache.geode.management.internal.cli.commands.ExportLogsCommand.ONLY_DATE_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class ExportLogsStatsDistributedTestBase {

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule().withLogFile();

  @ClassRule
  public static GfshCommandRule connector = new GfshCommandRule();

  protected static Set<String> expectedZipEntries = new HashSet<>();
  protected static MemberVM locator;

  @BeforeClass
  public static void beforeClass() {
    // start the locator in vm0 and then connect to it over http
    locator = lsRule.startLocatorVM(0, l -> l.withHttpService());

    Properties serverProperties = new Properties();
    serverProperties.setProperty(ConfigurationProperties.STATISTIC_SAMPLING_ENABLED, "true");
    serverProperties.setProperty(STATISTIC_ARCHIVE_FILE, "statistics.gfs");
    lsRule.startServerVM(1, serverProperties, locator.getPort());

    expectedZipEntries = Sets.newHashSet(
        "locator-0" + File.separator + "locator-0.log",
        "server-1" + File.separator + "server-1.log",
        "server-1" + File.separator + "statistics.gfs");
  }

  protected void connectIfNeeded() throws Exception {
    if (!connector.isConnected()) {
      connector.connect(locator);
    }
  }

  @Test
  public void testExportLogsAndStats() throws Exception {
    connectIfNeeded();
    connector.executeAndAssertThat("export logs").statusIsSuccess();
    String zipPath = getZipPathFromCommandResult(connector.getGfshOutput());
    Set<String> actualZipEnries = getZipEntries(zipPath);

    Set<String> expectedFiles = Sets.newHashSet(
        "locator-0" + File.separator + "locator-0.log",
        "server-1" + File.separator + "server-1.log",
        "server-1" + File.separator + "statistics.gfs");
    assertThat(actualZipEnries).containsAll(expectedFiles);
    // remove pulse.log if present
    actualZipEnries =
        actualZipEnries.stream().filter(x -> !x.endsWith("pulse.log")).collect(toSet());
    assertThat(actualZipEnries).hasSize(3);
  }

  @Test
  public void testExportLogsOnly() throws Exception {
    connectIfNeeded();
    connector.executeAndAssertThat("export logs --logs-only").statusIsSuccess();
    String zipPath = getZipPathFromCommandResult(connector.getGfshOutput());
    Set<String> actualZipEnries = getZipEntries(zipPath);

    Set<String> expectedFiles = Sets.newHashSet(
        "locator-0" + File.separator + "locator-0.log",
        "server-1" + File.separator + "server-1.log");
    assertThat(actualZipEnries).containsAll(expectedFiles);
    // remove pulse.log if present
    actualZipEnries =
        actualZipEnries.stream().filter(x -> !x.endsWith("pulse.log")).collect(toSet());
    assertThat(actualZipEnries).hasSize(2);
  }

  @Test
  public void testExportStatsOnly() throws Exception {
    connectIfNeeded();
    connector.executeAndAssertThat("export logs --stats-only").statusIsSuccess();
    String zipPath = getZipPathFromCommandResult(connector.getGfshOutput());
    Set<String> actualZipEnries = getZipEntries(zipPath);

    Set<String> expectedFiles = Sets.newHashSet("server-1" + File.separator + "statistics.gfs");
    assertThat(actualZipEnries).isEqualTo(expectedFiles);
  }

  @Test
  public void startAndEndDateCanExcludeLogs() throws Exception {
    connectIfNeeded();
    ZonedDateTime now = LocalDateTime.now().atZone(ZoneId.systemDefault());
    ZonedDateTime tomorrow = now.plusDays(1);

    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(ONLY_DATE_FORMAT);

    CommandStringBuilder commandStringBuilder = new CommandStringBuilder("export logs");
    commandStringBuilder.addOption("start-time", dateTimeFormatter.format(tomorrow));
    commandStringBuilder.addOption("log-level", "debug");

    connector.executeAndAssertThat(commandStringBuilder.toString()).statusIsError()
        .containsOutput("No files to be exported");
  }

  @Test
  public void testExportedZipFileTooBig() throws Exception {
    connectIfNeeded();
    CommandResult result = connector.executeCommand("export logs --file-size-limit=10k");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
  }

  protected String getZipPathFromCommandResult(String message) {
    return message.replaceAll("Logs exported to the connected member's file system: ", "").trim();
  }

  private static Set<String> getZipEntries(String zipFilePath) throws IOException {
    return new ZipFile(zipFilePath).stream().map(ZipEntry::getName)
        .filter(x -> !x.endsWith("views.log")).collect(Collectors.toSet());
  }
}
