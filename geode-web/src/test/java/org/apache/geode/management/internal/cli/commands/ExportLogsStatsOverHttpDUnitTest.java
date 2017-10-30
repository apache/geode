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

import org.apache.geode.test.junit.rules.GfshShellConnectionRule;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

@Category(DistributedTest.class)
public class ExportLogsStatsOverHttpDUnitTest extends ExportLogsStatsDUnitTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Override
  public void connectIfNeeded() throws Exception {
    if (!connector.isConnected())
      connector.connect(httpPort, GfshShellConnectionRule.PortType.http);
  }

  @Test
  public void testExportWithDir() throws Exception {
    connectIfNeeded();
    File dir = temporaryFolder.newFolder();
    // export the logs
    connector.executeCommand("export logs --dir=" + dir.getAbsolutePath());
    // verify that the message contains a path to the user.dir
    String message = connector.getGfshOutput();
    assertThat(message).contains("Logs exported to: ");
    assertThat(message).contains(dir.getAbsolutePath());

    String zipPath = getZipPathFromCommandResult(message);
    Set<String> actualZipEntries =
        new ZipFile(zipPath).stream().map(ZipEntry::getName).collect(Collectors.toSet());

    assertThat(actualZipEntries).isEqualTo(expectedZipEntries);

    // also verify that the zip file on locator is deleted
    assertThat(Arrays.stream(locator.getWorkingDir().listFiles())
        .filter(file -> file.getName().endsWith(".zip")).collect(Collectors.toSet())).isEmpty();
  }

  protected String getZipPathFromCommandResult(String message) {
    return message.replaceAll("Logs exported to: ", "").trim();
  }
}
