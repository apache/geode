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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({GfshTest.class, LoggingTest.class})
public class ExportLogsStatsOverHttpDistributedTest extends ExportLogsStatsDistributedTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Override
  public void connectIfNeeded() throws Exception {
    if (!connector.isConnected())
      connector.connect(locator.getHttpPort(), GfshCommandRule.PortType.http);
  }

  @Test
  public void testExportWithDir() throws Exception {
    connectIfNeeded();
    File dir = temporaryFolder.newFolder();
    connector.executeAndAssertThat("export logs --dir=" + dir.getAbsolutePath())
        .containsOutput("Logs exported to: ", dir.getAbsolutePath());
    String message = connector.getGfshOutput();
    String zipPath = getZipPathFromCommandResult(message);
    Set<String> actualZipEntries =
        new ZipFile(zipPath).stream().map(ZipEntry::getName).collect(Collectors.toSet());

    assertThat(actualZipEntries).containsAll(expectedZipEntries);

    // also verify that the zip file on locator is deleted
    assertThat(Arrays.stream(locator.getWorkingDir().listFiles())
        .filter(file -> file.getName().endsWith(".zip")).collect(Collectors.toSet())).isEmpty();
  }

  @Override
  protected String getZipPathFromCommandResult(String message) {
    return message.replaceAll("Logs exported to: ", "").trim();
  }
}
