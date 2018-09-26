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

import java.nio.file.Paths;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import com.google.common.collect.Sets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.categories.LoggingTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({GfshTest.class, LoggingTest.class})
public class ExportLogsOnServerManagerDistributedTest {

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule().withLogFile();

  @Rule
  public GfshCommandRule gfshConnector = new GfshCommandRule();

  @Test
  public void testExportWithOneServer() throws Exception {
    MemberVM server0 = lsRule.startServerVM(0, x -> x.withJMXManager());
    gfshConnector.connect(server0.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    gfshConnector.executeAndAssertThat("export logs").statusIsSuccess();

    String message = gfshConnector.getGfshOutput();
    assertThat(message).contains(server0.getWorkingDir().getAbsolutePath());

    String zipPath = getZipPathFromCommandResult(message);

    Set<String> expectedZipEntries =
        Sets.newHashSet(Paths.get("server-0", "server-0.log").toString());
    Set<String> actualZipEnries =
        new ZipFile(zipPath).stream().map(ZipEntry::getName).collect(Collectors.toSet());
    assertThat(actualZipEnries).containsAll(expectedZipEntries);
  }

  @Test
  public void testExportWithPeerLocator() throws Exception {
    MemberVM server0 = lsRule.startServerVM(0, x -> x.withEmbeddedLocator().withJMXManager());
    lsRule.startServerVM(1, server0.getEmbeddedLocatorPort());
    gfshConnector.connect(server0.getEmbeddedLocatorPort(), GfshCommandRule.PortType.locator);
    gfshConnector.executeAndAssertThat("export logs").statusIsSuccess();

    String message = gfshConnector.getGfshOutput();
    assertThat(message).contains(server0.getWorkingDir().getAbsolutePath());

    String zipPath = getZipPathFromCommandResult(message);

    Set<String> expectedZipEntries =
        Sets.newHashSet(Paths.get("server-0", "server-0.log").toString(),
            Paths.get("server-1", "server-1.log").toString());
    Set<String> actualZipEnries =
        new ZipFile(zipPath).stream().map(ZipEntry::getName).collect(Collectors.toSet());
    assertThat(actualZipEnries).containsAll(expectedZipEntries);
  }

  private String getZipPathFromCommandResult(String message) {
    return message.replaceAll("Logs exported to the connected member's file system: ", "").trim();
  }

}
