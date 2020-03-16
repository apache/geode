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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.google.common.collect.Sets;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.management.internal.cli.functions.ExportLogsFunction;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({GfshTest.class, LoggingTest.class})
public class LogExporterIntegrationTest {

  private final LogFilter filter = new LogFilter(Level.INFO, null, null);

  private LogExporter logExporter;
  private Properties properties;

  @Rule
  public ServerStarterRule server = new ServerStarterRule();

  @Before
  public void before() {
    properties = new Properties();
    // make sure the server's working dir has no log files or stats file to begin with, since in
    // some tests we are asserting on the # of log files and stats files created by the server
    File workingDir = server.getWorkingDir();
    Arrays.stream(workingDir.listFiles())
        .filter(f -> (f.getName().endsWith(".log") || f.getName().endsWith(".gfs")))
        .forEach(FileUtils::deleteQuietly);
  }

  @Test
  public void serverStartedWithWrongSuffix() throws Exception {
    properties.setProperty(LOG_FILE, new File("test.txt").getAbsolutePath());
    properties.setProperty(STATISTIC_ARCHIVE_FILE, "archive.archive");
    server.withProperties(properties).startServer();
    File serverWorkingDir = server.getWorkingDir();

    logExporter = new LogExporter(filter, new File(serverWorkingDir, "test.log"),
        new File(serverWorkingDir, "stats.gfs"));
    List<Path> logFiles = logExporter.findLogFiles(serverWorkingDir.toPath());
    assertThat(logFiles).isEmpty();

    List<Path> statsFiles = logExporter.findStatFiles(serverWorkingDir.toPath());
    assertThat(statsFiles).isEmpty();
  }

  @Test
  public void serverStartedWithCorrectSuffix() throws Exception {
    // ("relative log file is problematic in the test environment")
    properties.setProperty(LOG_FILE, new File("test.log").getAbsolutePath());
    properties.setProperty(STATISTIC_ARCHIVE_FILE, "archive.gfs");
    server.withProperties(properties).startServer();
    File serverWorkingDir = server.getWorkingDir();

    logExporter = new LogExporter(filter, new File(serverWorkingDir, "test.log"),
        new File(serverWorkingDir, "archive.gfs"));
    List<Path> logFiles = logExporter.findLogFiles(serverWorkingDir.toPath());
    assertThat(logFiles).hasSize(1);
    assertThat(logFiles.get(0)).hasFileName("test.log");

    List<Path> statsFiles = logExporter.findStatFiles(serverWorkingDir.toPath());
    assertThat(statsFiles).hasSize(1);
    assertThat(statsFiles.get(0)).hasFileName("archive.gfs");
  }

  @Test
  @Ignore("GEODE-2574: fix .gz suffix")
  public void serverStartedWithGZSuffix() throws Exception {
    properties.setProperty(LOG_FILE, "test.log.gz");
    properties.setProperty(STATISTIC_ARCHIVE_FILE, "archive.gfs.gz");
    server.withProperties(properties).startServer();
    File serverWorkingDir = server.getWorkingDir();

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
    properties.setProperty(LOG_FILE, logsFile.toString());
    server.withProperties(properties).startServer();

    verifyExportLogsFunctionDoesNotBlowUp(server.getCache());
  }

  @Test
  public void testWithRelativeStatsFile() throws Throwable {
    Path logsFile = Files.createTempFile("server", ".log");
    // Path statsFile = Files.createTempFile("stats", ".gfs");
    properties.setProperty(LOG_FILE, logsFile.toString());
    properties.setProperty(STATISTIC_ARCHIVE_FILE, "stats.gfs");
    server.withProperties(properties).startServer();

    verifyExportLogsFunctionDoesNotBlowUp(server.getCache());
  }

  @Test
  public void testWithRelativeLogsFile() throws Throwable {
    Path statsFile = Files.createTempFile("stats", ".gfs");
    properties.setProperty(LOG_FILE, "sever.log");
    properties.setProperty(STATISTIC_ARCHIVE_FILE, statsFile.toString());
    server.withProperties(properties).startServer();

    verifyExportLogsFunctionDoesNotBlowUp(server.getCache());
  }

  @Test
  public void testWithAbsoluteLogsStatsFile() throws Exception {
    File logsDir = Files.createTempDirectory("logs").toFile();
    File statsDir = Files.createTempDirectory("stats").toFile();

    File logFile = new File(logsDir, "server.log");
    File statsFile = new File(statsDir, "stats.gfs");

    properties.setProperty(LOG_FILE, logFile.getAbsolutePath());
    properties.setProperty(STATISTIC_ARCHIVE_FILE, statsFile.getAbsolutePath());

    server.withProperties(properties).startServer();

    logExporter = new LogExporter(filter, logFile, statsFile);
    Path exportedZip = logExporter.export();
    Set<String> actualFiles = getZipEntries(exportedZip.toString());
    Set<String> expectedFiles = Sets.newHashSet("server.log", "stats.gfs");

    assertThat(actualFiles).isEqualTo(expectedFiles);
  }

  private static void verifyExportLogsFunctionDoesNotBlowUp(Cache cache) throws Throwable {
    ExportLogsFunction.Args args =
        new ExportLogsFunction.Args(null, null, "info", false, false, false);
    CapturingResultSender resultSender = new CapturingResultSender();
    @SuppressWarnings("unchecked")
    FunctionContext<ExportLogsFunction.Args> context =
        new FunctionContextImpl(cache, "functionId", args, resultSender);
    new ExportLogsFunction().execute(context);
    if (resultSender.getThrowable() != null) {
      throw resultSender.getThrowable();
    }
  }

  private static Set<String> getZipEntries(String zipFilePath) throws IOException {
    return new ZipFile(zipFilePath).stream().map(ZipEntry::getName).collect(Collectors.toSet());
  }

  private static class CapturingResultSender implements ResultSender<Object> {

    private Throwable throwable;

    public Throwable getThrowable() {
      return throwable;
    }

    @Override
    public void sendResult(Object oneResult) {
      // nothing
    }

    @Override
    public void lastResult(Object lastResult) {
      // nothing
    }

    @Override
    public void sendException(Throwable t) {
      throwable = t;
    }
  }
}
