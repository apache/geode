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

import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({GfshTest.class})
@RunWith(JUnitParamsRunner.class)
public class ExportConfigCommandDUnitTest {

  @Rule
  public ClusterStartupRule startupRule = new ClusterStartupRule().withLogFile();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  @Parameters({"true", "false"})
  public void testExportConfig(final boolean connectOverHttp) throws Exception {
    MemberVM server0 = startupRule.startServerVM(0,
        x -> x.withProperty(GROUPS, "Group1").withJMXManager().withHttpService()
            .withEmbeddedLocator());

    if (connectOverHttp) {
      gfsh.connectAndVerify(server0.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(server0.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    }

    // start server1 and server2 in group2
    startupRule.startServerVM(1, "Group2", server0.getEmbeddedLocatorPort());
    startupRule.startServerVM(2, "Group2", server0.getEmbeddedLocatorPort());

    // start server3 that has no group info
    startupRule.startServerVM(3, server0.getEmbeddedLocatorPort());

    // export all members' config into a folder
    File tempDir = temporaryFolder.newFolder("all-members");
    gfsh.executeAndAssertThat("export config --dir=" + tempDir.getAbsolutePath()).statusIsSuccess();

    List<String> expectedFiles = Arrays.asList("server-0-cache.xml", "server-1-cache.xml",
        "server-2-cache.xml", "server-3-cache.xml", "server-0-gf.properties",
        "server-1-gf.properties", "server-2-gf.properties", "server-3-gf.properties");

    List<String> actualFiles =
        Arrays.stream(tempDir.listFiles()).map(File::getName).collect(Collectors.toList());
    assertThat(actualFiles).hasSameElementsAs(expectedFiles);
    tempDir.delete();

    // export just one member's config
    tempDir = temporaryFolder.newFolder("member0");
    gfsh.executeAndAssertThat("export config --member=server-0 --dir=" + tempDir.getAbsolutePath())
        .statusIsSuccess().containsOutput(tempDir.getAbsolutePath());

    expectedFiles = Arrays.asList("server-0-cache.xml", "server-0-gf.properties");

    actualFiles =
        Arrays.stream(tempDir.listFiles()).map(File::getName).collect(Collectors.toList());
    assertThat(actualFiles).hasSameElementsAs(expectedFiles);
    tempDir.delete();

    // export group2 config into a folder
    tempDir = temporaryFolder.newFolder("group2");
    gfsh.executeAndAssertThat("export config --group=Group2 --dir=" + tempDir.getAbsolutePath())
        .statusIsSuccess().containsOutput(tempDir.getAbsolutePath());

    expectedFiles = Arrays.asList("server-1-cache.xml", "server-2-cache.xml",
        "server-1-gf.properties", "server-2-gf.properties");

    actualFiles =
        Arrays.stream(tempDir.listFiles()).map(File::getName).collect(Collectors.toList());
    assertThat(actualFiles).hasSameElementsAs(expectedFiles);
    tempDir.delete();
  }
}
