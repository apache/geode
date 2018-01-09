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

package org.apache.geode.modules.util;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category(IntegrationTest.class)
public class ModuleFunctionsSecurityTest {

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withJMXManager().withSecurityManager(SimpleSecurityManager.class)
          .withRegion(RegionShortcut.REPLICATE, "REPLICATE_1")
          .withRegion(RegionShortcut.PARTITION, "PARTITION_1").withAutoStart();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void setupClass() {
    FunctionService.registerFunction(new BootstrappingFunction());
    FunctionService.registerFunction(new CreateRegionFunction());
    FunctionService.registerFunction(new RegionSizeFunction());
    FunctionService.registerFunction(new TouchPartitionedRegionEntriesFunction());
    FunctionService.registerFunction(new TouchReplicatedRegionEntriesFunction());
  }

  @After
  public void teardown() throws Exception {
    gfsh.disconnect();
  }

  @Test
  public void testInvalidPermissionsForBootstrappingFunction() throws Exception {
    gfsh.secureConnectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager,
        "dataWrite", "dataWrite");
    gfsh.executeAndAssertThat("execute function --id=" + BootstrappingFunction.ID)
        .containsOutput("not authorized for CLUSTER:MANAGE").statusIsSuccess();
  }

  @Test
  public void testValidPermissionsForBootstrappingFunction() throws Exception {
    gfsh.secureConnectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager,
        "clusterManage", "clusterManage");
    gfsh.executeAndAssertThat("execute function --id=" + BootstrappingFunction.ID)
        .containsOutput("true").statusIsSuccess();
  }

  @Test
  public void testInvalidPermissionsForCreateRegionFunction() throws Exception {
    gfsh.secureConnectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager,
        "dataWrite", "dataWrite");
    gfsh.executeAndAssertThat("execute function --id=" + CreateRegionFunction.ID)
        .containsOutput("not authorized for DATA:MANAGE").statusIsSuccess();
  }

  @Test
  public void testValidPermissionsForCreateRegionFunction() throws Exception {
    gfsh.secureConnectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager,
        "dataManage", "dataManage");
    gfsh.executeAndAssertThat("execute function --id=" + CreateRegionFunction.ID)
        .containsOutput("java.lang.NullPointerException").statusIsSuccess();
  }

  @Test
  public void testInvalidPermissionsForRegionSizeFunction() throws Exception {
    gfsh.secureConnectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager,
        "dataWrite", "dataWrite");
    gfsh.executeAndAssertThat("execute function --region=REPLICATE_1 --id=" + RegionSizeFunction.ID)
        .containsOutput("not authorized for DATA:READ:REPLICATE_1").statusIsSuccess();
  }

  @Test
  public void testValidPermissionsForRegionSizeFunction() throws Exception {
    gfsh.secureConnectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager,
        "dataRead", "dataRead");
    gfsh.executeAndAssertThat("execute function --arguments=REPLICATE_1 --region=REPLICATE_1 --id="
        + RegionSizeFunction.ID).containsOutput(" 0\n").statusIsSuccess();
  }

  @Test
  public void testInvalidPermissionsForTouchPartitionedRegionEntriesFunction() throws Exception {
    gfsh.secureConnectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager,
        "dataWrite", "dataWrite");
    gfsh.executeAndAssertThat(
        "execute function --region=PARTITION_1 --id=" + TouchPartitionedRegionEntriesFunction.ID)
        .containsOutput("not authorized for DATA:READ:PARTITION_1").statusIsSuccess();
  }

  @Test
  public void testValidPermissionsForTouchPartitionedRegionEntriesFunction() throws Exception {
    gfsh.secureConnectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager,
        "dataRead", "dataRead");
    gfsh.executeAndAssertThat("execute function --arguments=PARTITION_1 --region=PARTITION_1 --id="
        + TouchPartitionedRegionEntriesFunction.ID).containsOutput("java.lang.NullPointerException")
        .statusIsSuccess();
  }

  @Test
  public void testInvalidPermissionsForTouchReplicatedRegionEntriesFunction() throws Exception {
    gfsh.secureConnectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager,
        "dataWrite", "dataWrite");
    gfsh.executeAndAssertThat(
        "execute function --region=REPLICATE_1 --id=" + TouchReplicatedRegionEntriesFunction.ID)
        .containsOutput("not authorized for DATA:READ:REPLICATE_1").statusIsSuccess();
  }

  @Test
  public void testValidPermissionsForTouchReplicatedRegionEntriesFunction() throws Exception {
    gfsh.secureConnectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager,
        "dataRead", "dataRead");
    gfsh.executeAndAssertThat(
        "execute function --arguments=REPLICATE_1 --id=" + TouchReplicatedRegionEntriesFunction.ID)
        .containsOutput("java.lang.ArrayIndexOutOfBoundsException").statusIsSuccess();
  }
}
