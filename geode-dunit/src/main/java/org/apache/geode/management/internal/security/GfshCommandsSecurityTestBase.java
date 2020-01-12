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
package org.apache.geode.management.internal.security;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.apache.shiro.authz.permission.WildcardPermission;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ConnectionConfiguration;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class GfshCommandsSecurityTestBase {
  @ClassRule
  public static ServerStarterRule serverStarter =
      new ServerStarterRule().withJMXManager().withHttpService()
          .withSecurityManager(SimpleSecurityManager.class)
          .withRegion(RegionShortcut.REPLICATE_PERSISTENT, "persistentRegion")
          .withEmbeddedLocator();

  @Rule
  public GfshCommandRule gfshConnection =
      new GfshCommandRule(serverStarter::getJmxPort, GfshCommandRule.PortType.jmxManager);

  @BeforeClass
  public static void beforeClass() throws Exception {
    serverStarter.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("region1");
  }

  @Test
  @ConnectionConfiguration(user = "data", password = "wrongPwd")
  public void testInvalidCredentials() throws Exception {
    assertThat(gfshConnection.isConnected()).isFalse();
  }

  @Test
  @ConnectionConfiguration(user = "data", password = "data")
  public void testValidCredentials() throws Exception {
    assertThat(gfshConnection.isConnected()).isTrue();
  }

  @Test
  @ConnectionConfiguration(user = "clusterRead", password = "clusterRead")
  public void testClusterReader() throws Exception {
    runCommandsPermittedAndForbiddenBy("CLUSTER:READ");
  }

  @Test
  @ConnectionConfiguration(user = "clusterWrite", password = "clusterWrite")
  public void testClusterWriter() throws Exception {
    runCommandsPermittedAndForbiddenBy("CLUSTER:WRITE");
  }

  @Test
  @ConnectionConfiguration(user = "clusterManage", password = "clusterManage")
  public void testClusterManager() throws Exception {
    runCommandsPermittedAndForbiddenBy("CLUSTER:MANAGE");
  }

  @Test
  @ConnectionConfiguration(user = "dataRead", password = "dataRead")
  public void testDataReader() throws Exception {
    runCommandsPermittedAndForbiddenBy("DATA:READ");
  }

  @Test
  @ConnectionConfiguration(user = "dataWrite", password = "dataWrite")
  public void testDataWriter() throws Exception {
    runCommandsPermittedAndForbiddenBy("DATA:WRITE");
  }

  @Test
  @ConnectionConfiguration(user = "dataManage", password = "dataManage")
  public void testDataManager() throws Exception {
    runCommandsPermittedAndForbiddenBy("DATA:MANAGE");
  }

  @Test
  @ConnectionConfiguration(user = "dataReadRegionA", password = "dataReadRegionA")
  public void testRegionAReader() throws Exception {
    runCommandsPermittedAndForbiddenBy("DATA:READ:RegionA");
  }

  @Test
  @ConnectionConfiguration(user = "dataWriteRegionA", password = "dataWriteRegionA")
  public void testRegionAWriter() throws Exception {
    runCommandsPermittedAndForbiddenBy("DATA:WRITE:RegionA");
  }

  @Test
  @ConnectionConfiguration(user = "dataManageRegionA", password = "dataManageRegionA")
  public void testRegionAManager() throws Exception {
    runCommandsPermittedAndForbiddenBy("DATA:MANAGE:RegionA");
  }

  @Test
  @ConnectionConfiguration(user = "data,cluster", password = "data,cluster")
  public void testRegionSuperUser() throws Exception {
    runCommandsPermittedAndForbiddenBy("*");
  }

  private void runCommandsPermittedAndForbiddenBy(String permission) throws Exception {
    List<TestCommand> allPermitted =
        TestCommand.getPermittedCommands(new WildcardPermission(permission, true));

    for (TestCommand permitted : allPermitted) {
      System.out.println("Processing authorized command: " + permitted.getCommand());
      CommandResult result = gfshConnection.executeCommand(permitted.getCommand());
      assertThat(result).isNotNull();

      // for permitted commands, if any error happens, it's not an Unauthorized error
      if (result.getStatus() == Result.Status.ERROR) {
        assertThat(result.asString()).doesNotContain("not authorized");
      }
    }

    // skip no permission commands
    List<TestCommand> others = TestCommand.getOnlineCommands();
    others.removeAll(allPermitted);
    for (TestCommand other : others) {
      System.out.println("Processing unauthorized command: " + other.getCommand());
      gfshConnection.executeAndAssertThat(other.getCommand()).statusIsError()
          .containsOutput("Unauthorized");
    }
  }

  @Test
  @ConnectionConfiguration(user = "data,cluster", password = "data,cluster")
  public void modifyInternalRegionSuperUser() {
    CommandResult result =
        gfshConnection.executeCommand("put --key=key1 --value=value1 --region=PdxTypes");
    assertThat(result.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(result.asString()).contains("Unauthorized");
  }

  @Test
  @ConnectionConfiguration(user = "data", password = "data")
  public void testGetPostProcess() throws Exception {
    gfshConnection.executeCommand("put --region=region1 --key=key2 --value=value2");
    gfshConnection.executeCommand("put --region=region1 --key=key2 --value=value2");
    gfshConnection.executeCommand("put --region=region1 --key=key3 --value=value3");

    // gfsh.executeCommand("get --region=region1 --key=key1");
    gfshConnection.executeCommand("query --query=\"select * from /region1\"");
  }

  @Test
  @ConnectionConfiguration(user = "data", password = "data")
  public void createDiskStore() throws Exception {
    gfshConnection.executeAndAssertThat("create disk-store --name=disk1 --dir=disk1")
        .statusIsError()
        .containsOutput("not authorized for CLUSTER:MANAGE:DISK");
  }

  @Test
  @ConnectionConfiguration(user = "dataManage,clusterWriteDisk",
      password = "dataManage,clusterWriteDisk")
  public void createPartitionedPersistentRegionWithCorrectPermissions() throws Exception {
    gfshConnection.executeAndAssertThat("create region --name=region2 --type=PARTITION_PERSISTENT")
        .statusIsSuccess();
  }

  @Test
  @ConnectionConfiguration(user = "dataManage", password = "dataManage")
  public void createPartitionedPersistentRegionWithoutClusterWriteDisk() throws Exception {
    gfshConnection.executeAndAssertThat("create region --name=region2 --type=PARTITION_PERSISTENT")
        .statusIsError()
        .containsOutput("not authorized for CLUSTER:WRITE:DISK");
  }

  @Test
  @ConnectionConfiguration(user = "clusterWriteDisk", password = "clusterWriteDisk")
  public void createPartitionedPersistentRegionWithoutDataManage() throws Exception {
    gfshConnection.executeAndAssertThat("create region --name=region2 --type=PARTITION_PERSISTENT")
        .statusIsError()
        .containsOutput("not authorized for DATA:MANAGE");
  }

}
