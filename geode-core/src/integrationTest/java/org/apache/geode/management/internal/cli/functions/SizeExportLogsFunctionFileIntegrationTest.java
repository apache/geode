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
package org.apache.geode.management.internal.cli.functions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.internal.cli.functions.ExportLogsFunction.Args;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;

@Category({GfshTest.class, LoggingTest.class})
public class SizeExportLogsFunctionFileIntegrationTest {

  private File dir;
  private DistributedMember member;
  private SizeExportLogsFunction.Args nonFilteringArgs;
  private long expectedSize;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void before() throws Exception {
    dir = temporaryFolder.getRoot();
    member = mock(DistributedMember.class);
    nonFilteringArgs = new Args(null, null, null, false, false, false);
  }

  @After
  public void after() throws Exception {
    FileUtils.deleteDirectory(dir);
  }

  @Test
  public void bothFiles_returnsCombinedSize() throws Exception {
    List<File> logFiles =
        createLogFiles(new File(dir.getName(), testName.getMethodName()), 1, 1, FileUtils.ONE_KB);
    File logFile = logFiles.get(0);
    long logFileSize = FileUtils.sizeOf(logFiles.get(0));

    List<File> statFiles =
        createStatFiles(new File(dir.getName(), testName.getMethodName()), 1, 1, FileUtils.ONE_KB);
    File statArchive = statFiles.get(0);
    long statFileSize = FileUtils.sizeOf(statArchive);

    SizeExportLogsFunction function = new SizeExportLogsFunction();
    assertThat(function.estimateLogFileSize(member, logFile, statArchive, nonFilteringArgs))
        .isEqualTo(logFileSize + statFileSize);
  }

  @Test
  public void manyFiles_returnsCombinedSize() throws Exception {
    expectedSize = 0;
    List<File> logFiles =
        createLogFiles(new File(dir.getName(), testName.getMethodName()), 1, 3, FileUtils.ONE_KB);
    logFiles.forEach((file) -> {
      expectedSize += FileUtils.sizeOf(file);
    });

    List<File> statFiles = createStatFiles(new File(dir.getName(), testName.getMethodName()), 1, 2,
        FileUtils.ONE_KB * 2);
    statFiles.forEach((file) -> {
      expectedSize += FileUtils.sizeOf(file);
    });

    SizeExportLogsFunction function = new SizeExportLogsFunction();
    assertThat(function.estimateLogFileSize(member, logFiles.get(0), statFiles.get(0),
        nonFilteringArgs)).isEqualTo(expectedSize);
  }

  @Test
  public void emptyFiles_returnsZeroSize() throws Exception {
    List<File> logFiles =
        createLogFiles(new File(dir.getName(), testName.getMethodName()), 1, 3, 0);

    List<File> statFiles =
        createStatFiles(new File(dir.getName(), testName.getMethodName()), 1, 2, 0);
    SizeExportLogsFunction function = new SizeExportLogsFunction();
    assertThat(function.estimateLogFileSize(member, logFiles.get(0), statFiles.get(0),
        nonFilteringArgs)).isEqualTo(0);
  }

  @Test
  public void nullFiles_returnsZeroSize() throws Exception {
    File nullLogFile = new File(dir.getPath(), "nullLogFile");
    File nullStatFile = new File(dir.getPath(), "nullStatFile");
    SizeExportLogsFunction function = new SizeExportLogsFunction();
    assertThat(
        function.estimateLogFileSize(member, nullLogFile, nullStatFile, nonFilteringArgs))
            .isEqualTo(0);
  }

  private List<File> createLogFiles(File logFile, int mainId, int numberOfFiles, long sizeOfFile)
      throws IOException {
    List<File> files = new ArrayList<>();
    for (int i = 0; i < numberOfFiles; i++) {
      String name =
          baseName(logFile.getName()) + "-" + formatId(mainId) + "-" + formatId(i + 1) + ".log";
      File file = createFile(name, sizeOfFile, true);
      files.add(file);
    }
    return files;
  }

  private List<File> createStatFiles(File logFile, int mainId, int numberOfFiles, long sizeOfFile)
      throws IOException {
    List<File> files = new ArrayList<>();
    for (int i = 0; i < numberOfFiles; i++) {
      String name =
          baseName(logFile.getName()) + "-" + formatId(mainId) + "-" + formatId(i + 1) + ".gfs";
      File file = createFile(name, sizeOfFile, false);
      files.add(file);
    }
    return files;
  }

  private String baseName(String logFileName) {
    // base log file: myfile.log
    // mainId childId for rolling
    // myfile-01-01.log
    // myfile-01-02.log
    // pass in myfile.log
    // return myfile
    return null;
  }

  private String formatId(final int id) {
    return String.format("%02d", id);
  }

  private File createFile(String name, long sizeInBytes, boolean lineFeed) throws IOException {
    File file = new File(dir, name);
    fillUpFile(file, sizeInBytes, lineFeed);
    return file;
  }

  private void fillUpFile(File file, long sizeInBytes, boolean lineFeed) throws IOException {
    PrintWriter writer = new PrintWriter(file, "UTF-8");
    while (FileUtils.sizeOf(file) < sizeInBytes) {
      writer.print("this is a line of data in the file");
      if (lineFeed) {
        writer.println();
      }
    }
    writer.close();
  }
}
