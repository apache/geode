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
import java.nio.file.Paths;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({GfshTest.class})
public class CommandOverHttpTest {

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withLogFile().withJMXManager()
          .withHttpService()
          .withAutoStart();

  @Rule
  public GfshCommandRule gfshRule = new GfshCommandRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    gfshRule.connectAndVerify(server.getHttpPort(), GfshCommandRule.PortType.http);
  }

  @Test
  public void testListClient() throws Exception {
    gfshRule.executeAndAssertThat("list clients")
        .statusIsSuccess()
        .hasInfoSection().hasOutput()
        .isEqualTo("No clients were retrieved for cache-servers.");
  }

  @Test
  public void testDescribeClient() throws Exception {
    gfshRule.executeAndAssertThat("describe client --clientID=xyz")
        .statusIsError()
        .hasInfoSection().hasOutput()
        .contains("Specified Client ID xyz not present");
  }

  @Test
  public void exportLogs() throws Exception {
    gfshRule.executeAndAssertThat("export logs").statusIsSuccess()
        .containsOutput("Logs exported to:");
  }

  @Test
  public void deployJar() throws Exception {
    String className = "DeployCommandFunction";
    String jarName = "deployCommand.jar";
    File jar = temporaryFolder.newFile(jarName);
    new ClassBuilder().writeJarFromName(className, jar);
    gfshRule.executeAndAssertThat("deploy --jar=" + jar).statusIsSuccess();
  }

  @Test
  public void exportConfig() throws Exception {
    String dir = temporaryFolder.getRoot().getAbsolutePath();
    gfshRule.executeAndAssertThat("export config --dir=" + dir).statusIsSuccess()
        .containsOutput("File saved to " + Paths.get(dir, "server-cache.xml"))
        .containsOutput("File saved to " + Paths.get(dir, "server-gf.properties"));
  }

  @Test
  public void commandEncodeDecodeProperly() throws Exception {
    Region<Object, Object> testRegion =
        server.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("test");
    // this command would not fail because of parameter url encoding/decoding
    gfshRule.executeAndAssertThat("put --region=test --key='k 1' --value=#abdf%dgadf")
        .statusIsSuccess();

    assertThat(testRegion.get("k 1")).isEqualTo("#abdf%dgadf");
  }
}
