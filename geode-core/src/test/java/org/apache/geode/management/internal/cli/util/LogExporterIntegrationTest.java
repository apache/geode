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

package org.apache.geode.management.internal.cli.util;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_ARCHIVE_FILE;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.Sets;

import org.apache.geode.management.internal.cli.functions.ExportLogsFunctionIntegrationTest;
import org.apache.geode.test.dunit.rules.LocalServerStarterRule;
import org.apache.geode.test.dunit.rules.Server;
import org.apache.geode.test.dunit.rules.ServerStarterBuilder;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.Set;

@Category(IntegrationTest.class)
public class LogExporterIntegrationTest {
  private LogExporter logExporter;

  private Properties properties;

  private LogFilter filter = new LogFilter(Level.INFO, null, null);

  LocalServerStarterRule serverStarterRule;

  @Before
  public void before() throws Exception {
    properties = new Properties();
  }

  @After
  public void after() {
    if (serverStarterRule != null) {
      serverStarterRule.after();
    }
  }

  @Test
  public void serverStartedWithWrongSuffix() throws Throwable {
    serverStarterRule =
        new ServerStarterBuilder().withProperty(LOG_FILE, new File("test.txt").getAbsolutePath())
            .withProperty(STATISTIC_ARCHIVE_FILE, "archive.archive").buildInThisVM();
    serverStarterRule.before();

    File serverWorkingDir = serverStarterRule.getWorkingDir();

    logExporter = new LogExporter(filter, new File(serverWorkingDir, "test.log"),
        new File(serverWorkingDir, "stats.gfs"));
    List<Path> logFiles = logExporter.findLogFiles(serverWorkingDir.toPath());
    assertThat(logFiles).isEmpty();

    List<Path> statsFiles = logExporter.findStatFiles(serverWorkingDir.toPath());
    assertThat(statsFiles).isEmpty();
  }

  @Test
  @Ignore // This test assume that new File() ends up in the workingDir, which is not true
  public void serverStartedWithCorrectSuffix() throws Throwable {

    serverStarterRule =
        new ServerStarterBuilder().withProperty(LOG_FILE, new File("test.log").getAbsolutePath())
            .withProperty(STATISTIC_ARCHIVE_FILE, "archive.gfs").buildInThisVM();
    serverStarterRule.before();

    // ("relative log file is problematic in the test environment")
    File serverWorkingDir = serverStarterRule.getWorkingDir();

    logExporter = new LogExporter(filter, new File("test.log"), new File("archive.gfs"));
    List<Path> logFiles = logExporter.findLogFiles(serverWorkingDir.toPath());
    assertThat(logFiles).hasSize(1);
    assertThat(logFiles.get(0)).hasFileName("test.log");

    List<Path> statsFiles = logExporter.findStatFiles(serverWorkingDir.toPath());
    assertThat(statsFiles).hasSize(1);
    assertThat(statsFiles.get(0)).hasFileName("archive.gfs");
  }

  @Test
  @Ignore("fix .gz suffix")
  public void serverStartedWithGZSuffix() throws Throwable {
    serverStarterRule = new ServerStarterBuilder().withProperty(LOG_FILE, "test.log.gz")
        .withProperty(STATISTIC_ARCHIVE_FILE, "archive.gfs.gz").buildInThisVM();
    serverStarterRule.before();

    File serverWorkingDir = serverStarterRule.getWorkingDir();

    logExporter = new LogExporter(filter, new File(serverWorkingDir, "test.log"),
        new File(serverWorkingDir, "stats.gfs"));
    List<Path> logFiles = logExporter.findLogFiles(serverWorkingDir.toPath());
    assertThat(logFiles).hasSize(1);

    List<Path> statsFiles = logExporter.findStatFiles(serverWorkingDir.toPath());
    assertThat(statsFiles).hasSize(1);
  }

  @Test
  public void testNoStatsFile() throws Throwable {
    Path logsFile = Files.createTempFile("server", ".log");

    serverStarterRule =
        new ServerStarterBuilder().withProperty(LOG_FILE, logsFile.toString()).buildInThisVM();
    serverStarterRule.before();

    ExportLogsFunctionIntegrationTest.verifyExportLogsFunctionDoesNotBlowUp();
  }

  @Test
  public void testWithRelativeStatsFile() throws Throwable {
    Path logsFile = Files.createTempFile("server", ".log");

    serverStarterRule = new ServerStarterBuilder().withProperty(LOG_FILE, logsFile.toString())
        .withProperty(STATISTIC_ARCHIVE_FILE, "stats.gfs").buildInThisVM();
    serverStarterRule.before();

    ExportLogsFunctionIntegrationTest.verifyExportLogsFunctionDoesNotBlowUp();
  }

  @Test
  public void testWithRelativeLogsFile() throws Throwable {
    Path statsFile = Files.createTempFile("stats", ".gfs");

    serverStarterRule = new ServerStarterBuilder().withProperty(LOG_FILE, "sever.log")
        .withProperty(STATISTIC_ARCHIVE_FILE, statsFile.toString()).buildInThisVM();
    serverStarterRule.before();

    ExportLogsFunctionIntegrationTest.verifyExportLogsFunctionDoesNotBlowUp();
  }

  @Test
  public void testWithAbsoluteLogsStatsFile() throws Throwable {
    File logsDir = Files.createTempDirectory("logs").toFile();
    File statsDir = Files.createTempDirectory("stats").toFile();

    File logFile = new File(logsDir, "server.log");
    File statsFile = new File(statsDir, "stats.gfs");

    serverStarterRule = new ServerStarterBuilder().withProperty(LOG_FILE, logFile.getAbsolutePath())
        .withProperty(STATISTIC_ARCHIVE_FILE, statsFile.getAbsolutePath()).buildInThisVM();
    serverStarterRule.before();

    logExporter = new LogExporter(filter, logFile, statsFile);
    Path exportedZip = logExporter.export();
    Set<String> actualFiles = LogExporterTest.getZipEntries(exportedZip.toString());
    Set<String> expectedFiles = Sets.newHashSet("server.log", "stats.gfs");

    assertThat(actualFiles).isEqualTo(expectedFiles);

  }


}
