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

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.ClassBuilder;
import org.apache.geode.test.dunit.rules.gfsh.GfshRule;
import org.apache.geode.test.dunit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class OptionAliasesIntegrationTest {
  private String jarPath;
  private String dirPath;

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    dirPath = temporaryFolder.getRoot().getAbsolutePath();

    String className = "SomeJavaClass";
    File jarFile = new File(temporaryFolder.getRoot(), "myJar.jar");
    new ClassBuilder().writeJarFromName(className, jarFile);
    jarPath = jarFile.getAbsolutePath();
  }

  @Test
  public void startLocator_withGroupOption_oneGroup() throws Exception {
    gfshRule.execute(GfshScript.of("start locator --name=locator1 --group=g1")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void startLocator_withGroupOption_twoGroups() throws Exception {
    gfshRule.execute(GfshScript.of("start locator --name=locator1 --group=g1,g2")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void startLocator_withGroupsOption_oneGroup() throws Exception {
    gfshRule.execute(GfshScript.of("start locator --name=locator1 --groups=g1")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void startLocator_withGroupsOption_twoGroups() throws Exception {
    gfshRule.execute(GfshScript.of("start locator --name=locator1 --groups=g1,g2")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void startServer_withGroupOption_oneGroup() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --name=server1 --group=g1 --disable-default-server")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void startServer_withGroupOption_twoGroups() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --name=server1 --group=g1,g2 --disable-default-server")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void startServer_withGroupsOption_oneGroup() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --name=server1 --groups=g1 --disable-default-server")
        // "describe member --name=server1")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void startServer_withGroupsOption_twoGroups() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --name=server1 --groups=g1,g2 --disable-default-server")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }



  @Test
  public void exportConfig_withMemberOption_oneMember() throws Exception {
    gfshRule
        .execute(
            GfshScript
                .of("start locator", "start server --name=m1 --disable-default-server",
                    "export config --member=m1")
                .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void exportConfig_withMemberOption_twoMembers() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --name=m1 --disable-default-server",
            "start server --name=m2 --disable-default-server", "export config --member=m1,m2")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void exportConfig_withMembersOption_oneMember() throws Exception {
    gfshRule
        .execute(
            GfshScript
                .of("start locator", "start server --name=m1 --disable-default-server",
                    "export config --members=m1")
                .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void exportConfig_withMembersOption_twoMembers() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --name=m1 --disable-default-server",
            "start server --name=m2 --disable-default-server", "export config --members=m1,m2")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void exportConfig_withGroupOption_oneGroup() throws Exception {
    gfshRule
        .execute(GfshScript.of("start locator", "start server --group=g1 --disable-default-server",
            "export config --group=g1").awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void exportConfig_withGroupOption_twoGroups() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --group=g1 --disable-default-server",
            "start server --group=g2 --disable-default-server", "export config --group=g1,g2")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void exportConfig_withGroupsOption_oneGroup() throws Exception {
    gfshRule
        .execute(
            GfshScript
                .of("start locator", "start server --groups=g1 --disable-default-server",
                    "export config --groups=g1")
                .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void exportConfig_withGroupsOption_twoGroups() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --groups=g1 --disable-default-server",
            "start server --groups=g2 --disable-default-server", "export config --groups=g1,g2")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void createAlterRegion_withGroupOption_oneGroup() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --group=g1 --disable-default-server",
            "create region --name=region1 --type=REPLICATE_PERSISTENT --group=g1",
            "alter region --name=region1 --enable-cloning --group=g1")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void createAlterRegion_withGroupOption_twoGroups() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --group=g1 --disable-default-server",
            "start server --group=g2 --disable-default-server",
            "create region --name=region1 --type=REPLICATE_PERSISTENT --group=g1,g2",
            "alter region --name=region1 --enable-cloning --group=g1,g2")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void createAlterRegion_withGroupsOption_oneGroup() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --group=g1 --disable-default-server",
            "create region --name=region1 --type=REPLICATE_PERSISTENT --groups=g1",
            "alter region --name=region1 --enable-cloning --groups=g1")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void createAlterRegion_withGroupsOption_twoGroups() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --group=g1 --disable-default-server",
            "start server --group=g2 --disable-default-server",
            "create region --name=region1 --type=REPLICATE_PERSISTENT --groups=g1,g2",
            "alter region --name=region1 --enable-cloning --groups=g1,g2")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void deployUndeploy_withGroupOption_oneGroup() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --group=g1 --disable-default-server",
            "deploy --group=g1 --jar=" + jarPath, "undeploy --group=g1 --jar=" + jarPath)
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void deployUndeploy_withGroupOption_twoGroups() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --group=g1 --disable-default-server",
            "start server --group=g2 --disable-default-server",
            "deploy --group=g1,g2 --jar=" + jarPath, "undeploy --group=g1,g2 --jar=" + jarPath)
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void deployUndeploy_withGroupsOption_oneGroup() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --groups=g1 --disable-default-server",
            "deploy --groups=g1 --jar=" + jarPath, "undeploy --groups=g1 --jar=" + jarPath)
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  public void deployUndeploy_withGroupsOption_twoGroups() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --groups=g1 --disable-default-server",
            "start server --groups=g2 --disable-default-server",
            "deploy --groups=g1,g2 --jar=" + jarPath, "undeploy --groups=g1,g2 --jar=" + jarPath)
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }

  @Test
  @Ignore
  public void createCompactDestroyDiskStore_withGroupOption_oneGroup() throws Exception {
    gfshRule.execute(GfshScript
        .of("start locator", "start server --group=g1 --disable-default-server",
            "create disk-store --name=ds1 --dir=" + dirPath
                + " --group=g1 --allow-force-compaction",
            "compact disk-store --name=ds1 --group=g1", "destroy disk-store --name=ds1 --group=g1")
        .awaitAtMost(1, TimeUnit.MINUTES).expectExitCode(0));
  }
}
