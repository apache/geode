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
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.commons.io.FileUtils;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.rules.Locator;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.Server;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;

@Category(DistributedTest.class)
public class MergeLogsDUnitTest {
  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();
  private Locator locator;

  private static final String MESSAGE_1 = "MergeLogsMessage1";
  private static final String MESSAGE_2 = "MergeLogsMessage2";
  private static final String MESSAGE_3 = "MergeLogsMessage3";
  private static final String MESSAGE_4 = "MergeLogsMessage4";
  private static final String MESSAGE_5 = "MergeLogsMessage5";
  private static final String MESSAGE_6 = "MergeLogsMessage6";

  @Before
  public void setup() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
    locator = lsRule.startLocatorVM(0, properties);

    properties.setProperty(DistributionConfig.LOCATORS_NAME,
        "localhost[" + locator.getPort() + "]");

    Server server = lsRule.startServerVM(1, properties);
    Server server2 = lsRule.startServerVM(2, properties);

    locator.invoke(() -> LogService.getLogger().info(MESSAGE_1));
    server.invoke(() -> LogService.getLogger().info(MESSAGE_2));
    server2.invoke(() -> LogService.getLogger().info(MESSAGE_3));

    locator.invoke(() -> LogService.getLogger().info(MESSAGE_4));
    server.invoke(() -> LogService.getLogger().info(MESSAGE_5));
    server2.invoke(() -> LogService.getLogger().info(MESSAGE_6));
  }

  @Test
  public void testExportInProcess() throws Exception {
    assertThat(MergeLogs.findLogFilesToMerge(lsRule.getTempFolder().getRoot())).hasSize(3);

    File result = MergeLogs.mergeLogFile(lsRule.getTempFolder().getRoot().getCanonicalPath());
    assertOnLogContents(result);
  }

  @Test
  public void testExportInNewProcess() throws Throwable {
    assertThat(MergeLogs.findLogFilesToMerge(lsRule.getTempFolder().getRoot())).hasSize(3);

    MergeLogs.mergeLogsInNewProcess(lsRule.getTempFolder().getRoot().toPath());
    File result = Arrays.stream(lsRule.getTempFolder().getRoot().listFiles())
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
