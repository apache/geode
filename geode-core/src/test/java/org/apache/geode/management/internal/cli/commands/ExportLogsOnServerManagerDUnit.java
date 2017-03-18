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

import com.google.common.collect.Sets;

import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.Server;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;


@Category(DistributedTest.class)
public class ExportLogsOnServerManagerDUnit {

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfshConnector = new GfshShellConnectionRule();

  @Test
  public void testExportWithOneServer() throws Exception {
    MemberVM server0 = lsRule.startServerAsJmxManager(0);
    gfshConnector.connect(server0.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger);
    gfshConnector.executeAndVerifyCommand("export logs");

    String message = gfshConnector.getGfshOutput();
    assertThat(message).contains(server0.getWorkingDir().getAbsolutePath());

    String zipPath = getZipPathFromCommandResult(message);

    Set<String> expectedZipEntries = Sets.newHashSet("server-0/server-0.log");
    Set<String> actualZipEnries =
        new ZipFile(zipPath).stream().map(ZipEntry::getName).collect(Collectors.toSet());
    assertThat(actualZipEnries).isEqualTo(expectedZipEntries);
  }

  @Test
  public void testExportWithPeerLocator() throws Exception {
    MemberVM<Server> server0 = lsRule.startServerAsEmbededLocator(0);
    lsRule.startServerVM(1, server0.getMember().getEmbeddedLocatorPort());
    gfshConnector.connect(server0.getMember().getEmbeddedLocatorPort(),
        GfshShellConnectionRule.PortType.locator);
    gfshConnector.executeAndVerifyCommand("export logs");

    String message = gfshConnector.getGfshOutput();
    assertThat(message).contains(server0.getWorkingDir().getAbsolutePath());

    String zipPath = getZipPathFromCommandResult(message);

    Set<String> expectedZipEntries =
        Sets.newHashSet("server-0/server-0.log", "server-1/server-1.log");
    Set<String> actualZipEnries =
        new ZipFile(zipPath).stream().map(ZipEntry::getName).collect(Collectors.toSet());
    assertThat(actualZipEnries).isEqualTo(expectedZipEntries);

  }

  private String getZipPathFromCommandResult(String message) {
    return message.replaceAll("Logs exported to the connected member's file system: ", "").trim();
  }

}
