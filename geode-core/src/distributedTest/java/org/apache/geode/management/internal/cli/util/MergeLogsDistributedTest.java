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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;

@Category({GfshTest.class, LoggingTest.class})
public class MergeLogsDistributedTest {

  private static final String MESSAGE_1 = "MergeLogsMessage1";
  private static final String MESSAGE_2 = "MergeLogsMessage2";
  private static final String MESSAGE_3 = "MergeLogsMessage3";
  private static final String MESSAGE_4 = "MergeLogsMessage4";
  private static final String MESSAGE_5 = "MergeLogsMessage5";
  private static final String MESSAGE_6 = "MergeLogsMessage6";

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule().withLogFile();

  @Before
  public void setup() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
    MemberVM locator = lsRule.startLocatorVM(0, properties);

    MemberVM server = lsRule.startServerVM(1, properties, locator.getPort());
    MemberVM server2 = lsRule.startServerVM(2, properties, locator.getPort());

    // Especially on Windows, wait for time to pass otherwise all log messages may appear in the
    // same millisecond.
    locator.invoke(() -> LogService.getLogger().info(MESSAGE_1));
    Thread.sleep(1);
    server.invoke(() -> LogService.getLogger().info(MESSAGE_2));
    Thread.sleep(1);
    server2.invoke(() -> LogService.getLogger().info(MESSAGE_3));
    Thread.sleep(1);

    locator.invoke(() -> LogService.getLogger().info(MESSAGE_4));
    Thread.sleep(1);
    server.invoke(() -> LogService.getLogger().info(MESSAGE_5));
    Thread.sleep(1);
    server2.invoke(() -> LogService.getLogger().info(MESSAGE_6));
  }

  @Test
  public void testExportInProcess() throws Exception {
    List<File> actualFiles = MergeLogs.findLogFilesToMerge(lsRule.getWorkingDirRoot());
    // remove pulse.log if present
    actualFiles =
        actualFiles.stream().filter(x -> !x.getName().endsWith("pulse.log")).collect(toList());
    assertThat(actualFiles).hasSize(5);

    File result = MergeLogs.mergeLogFile(lsRule.getWorkingDirRoot().getCanonicalPath());
    assertOnLogContents(result);
  }

  @Test
  public void testExportInNewProcess() throws Throwable {
    List<File> actualFiles = MergeLogs.findLogFilesToMerge(lsRule.getWorkingDirRoot());
    // remove pulse.log if present
    actualFiles =
        actualFiles.stream().filter(x -> !x.getName().endsWith("pulse.log")).collect(toList());
    assertThat(actualFiles).hasSize(5);

    MergeLogs.mergeLogsInNewProcess(lsRule.getWorkingDirRoot().toPath());
    File result = Arrays.stream(lsRule.getWorkingDirRoot().listFiles())
        .filter((File f) -> f.getName().startsWith("merge")).findFirst().orElseThrow(() -> {
          throw new AssertionError("No merged log file found");
        });
    assertOnLogContents(result);
  }

  private void assertOnLogContents(File mergedLogFile) throws IOException {
    String mergedLines = FileUtils.readLines(mergedLogFile, Charset.defaultCharset()).stream()
        .collect(joining("\n"));

    assertThat(mergedLines).contains(MESSAGE_1);
    assertThat(mergedLines).contains(MESSAGE_2);
    assertThat(mergedLines).contains(MESSAGE_3);
    assertThat(mergedLines).contains(MESSAGE_4);
    assertThat(mergedLines).contains(MESSAGE_5);
    assertThat(mergedLines).contains(MESSAGE_6);

    // Make sure that our merged log file contains the proper ordering
    assertThat(mergedLines.indexOf(MESSAGE_1)).isLessThan(mergedLines.indexOf(MESSAGE_2));
    assertThat(mergedLines.indexOf(MESSAGE_2)).isLessThan(mergedLines.indexOf(MESSAGE_3));
    assertThat(mergedLines.indexOf(MESSAGE_3)).isLessThan(mergedLines.indexOf(MESSAGE_4));
    assertThat(mergedLines.indexOf(MESSAGE_4)).isLessThan(mergedLines.indexOf(MESSAGE_5));
    assertThat(mergedLines.indexOf(MESSAGE_5)).isLessThan(mergedLines.indexOf(MESSAGE_6));
  }
}
