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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.io.FileUtils;
import org.apache.geode.management.internal.configuration.utils.ZipUtils;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Path;
import java.text.ParseException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

@Category(IntegrationTest.class)
public class LogExporterTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private LogExporter logExporter;

  @Before
  public void setup() throws ParseException {
    LogFilter logFilter = mock(LogFilter.class);

    when(logFilter.acceptsFile(any())).thenReturn(true);
    when(logFilter.acceptsLine(any())).thenReturn(LogFilter.LineFilterResult.LINE_ACCEPTED);

    logExporter = new LogExporter(logFilter);
  }


  @Test
  public void exportBuildsZipCorrectlyWithTwoLogFiles() throws Exception {
    File serverWorkingDir = temporaryFolder.newFolder("serverWorkingDir");
    File logFile1 = new File(serverWorkingDir, "server1.log");
    FileUtils.writeStringToFile(logFile1, "some log for server1 \n some other log line");
    File logFile2 = new File(serverWorkingDir, "server2.log");
    FileUtils.writeStringToFile(logFile2, "some log for server2 \n some other log line");

    File notALogFile = new File(serverWorkingDir, "foo.txt");
    FileUtils.writeStringToFile(notALogFile, "some text");


    Path zippedExport = logExporter.export(serverWorkingDir.toPath());

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
    File workingDir = temporaryFolder.newFolder("workingDir");
    File logFile = new File(workingDir, "server.log");

    FileUtils.writeStringToFile(logFile, "some log line");

    File notALogFile = new File(workingDir, "foo.txt");
    FileUtils.writeStringToFile(notALogFile, "some text");

    assertThat(logExporter.findLogFiles(workingDir.toPath())).contains(logFile.toPath());
    assertThat(logExporter.findLogFiles(workingDir.toPath())).doesNotContain(notALogFile.toPath());
  }

}
