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
package org.apache.geode.management.internal.cli.util;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;

@Category({GfshTest.class, LoggingTest.class})
public class LogExporterFileIntegrationTest {

  private LogExporter logExporter;
  private LogFilter logFilter;
  private File workingDir;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    logFilter = mock(LogFilter.class);

    when(logFilter.acceptsFile(any())).thenReturn(true);
    when(logFilter.acceptsLine(any())).thenReturn(LogFilter.LineFilterResult.LINE_ACCEPTED);

    workingDir = temporaryFolder.newFolder("workingDir");
    logExporter = new LogExporter(logFilter, new File(workingDir, "server.log"),
        new File(workingDir, "stats.gfs"));
  }

  @Test
  public void exporterShouldReturnNullIfNoFile() throws Exception {
    assertThat(logExporter.export()).isNull();
  }

  @Test
  public void exporterShouldStillReturnFileIfNoAcceptableLogs() throws Exception {
    File logFile1 = new File(workingDir, "server1.log");
    FileUtils.writeStringToFile(logFile1, "some log for server1 \n some other log line",
        Charset.defaultCharset());
    when(logFilter.acceptsLine(any())).thenReturn(LogFilter.LineFilterResult.LINE_REJECTED);
    Path exportedZip = logExporter.export();
    assertThat(exportedZip).isNotNull();

    File unzippedExportDir = temporaryFolder.newFolder("unzippedExport");
    ZipUtils.unzip(exportedZip.toString(), unzippedExportDir.getCanonicalPath());
    assertThat(unzippedExportDir.listFiles()).hasSize(1);

    // check the exported file has no content
    BufferedReader br =
        new BufferedReader(new FileReader(new File(unzippedExportDir, "server1.log")));
    assertThat(br.readLine()).isNull();
  }

  @Test
  public void exportBuildsZipCorrectlyWithTwoLogFiles() throws Exception {
    File logFile1 = new File(workingDir, "server1.log");
    FileUtils.writeStringToFile(logFile1, "some log for server1 \n some other log line",
        Charset.defaultCharset());
    File logFile2 = new File(workingDir, "server2.log");
    FileUtils.writeStringToFile(logFile2, "some log for server2 \n some other log line",
        Charset.defaultCharset());

    File notALogFile = new File(workingDir, "foo.txt");
    FileUtils.writeStringToFile(notALogFile, "some text", Charset.defaultCharset());


    Path zippedExport = logExporter.export();

    File unzippedExportDir = temporaryFolder.newFolder("unzippedExport");
    ZipUtils.unzip(zippedExport.toString(), unzippedExportDir.getCanonicalPath());

    assertThat(unzippedExportDir.listFiles()).hasSize(2);
    List<File> exportedFiles = Stream.of(unzippedExportDir.listFiles())
        .sorted(Comparator.comparing(File::getName)).collect(toList());

    assertThat(exportedFiles.get(0)).hasSameContentAs(logFile1);
    assertThat(exportedFiles.get(1)).hasSameContentAs(logFile2);
  }

  @Test
  public void findLogFilesExcludesFilesWithIncorrectExtension() throws Exception {
    File logFile = new File(workingDir, "server.log");

    FileUtils.writeStringToFile(logFile, "some log line", Charset.defaultCharset());

    File notALogFile = new File(workingDir, "foo.txt");
    FileUtils.writeStringToFile(notALogFile, "some text", Charset.defaultCharset());

    assertThat(logExporter.findLogFiles(workingDir.toPath())).contains(logFile.toPath());
    assertThat(logExporter.findLogFiles(workingDir.toPath())).doesNotContain(notALogFile.toPath());
  }

  @Test
  public void findStatFiles() throws Exception {
    File statFile = new File(workingDir, "server.gfs");

    FileUtils.writeStringToFile(statFile, "some stat line", Charset.defaultCharset());

    File notALogFile = new File(workingDir, "foo.txt");
    FileUtils.writeStringToFile(notALogFile, "some text", Charset.defaultCharset());

    assertThat(logExporter.findStatFiles(workingDir.toPath())).contains(statFile.toPath());
    assertThat(logExporter.findStatFiles(workingDir.toPath())).doesNotContain(notALogFile.toPath());
  }

  @Test
  // GEODE-6707 - Rolled over GC logs end with names like ".log.1"
  public void findLogsWhichContainsTheWordLog() throws Exception {
    File gcLogFile = new File(workingDir, "gc.log");
    FileUtils.writeStringToFile(gcLogFile, "some gc log line", Charset.defaultCharset());

    File gcRolledOverLogFile = new File(workingDir, "gc.log.1");
    FileUtils.writeStringToFile(gcRolledOverLogFile, "some gc log line", Charset.defaultCharset());

    assertThat(logExporter.findLogFiles(workingDir.toPath())).contains(gcLogFile.toPath());
    assertThat(logExporter.findLogFiles(workingDir.toPath()))
        .contains(gcRolledOverLogFile.toPath());
  }
}
