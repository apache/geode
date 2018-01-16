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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ConnectionConfiguration;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({IntegrationTest.class, SecurityTest.class})
public class ModuleFunctionsSecurityTest {

  private static final String RESULT_HEADER = "Function Execution Result";

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withJMXManager().withSecurityManager(SimpleSecurityManager.class)
          .withRegion(RegionShortcut.REPLICATE, "REPLICATE_1")
          .withRegion(RegionShortcut.PARTITION, "PARTITION_1").withAutoStart();

  @Rule
  public GfshCommandRule gfsh =
      new GfshCommandRule(server::getJmxPort, GfshCommandRule.PortType.jmxManager);

  @BeforeClass
  public static void setupClass() {
    FunctionService.registerFunction(new BootstrappingFunction());
    FunctionService.registerFunction(new CreateRegionFunction());
    FunctionService.registerFunction(new RegionSizeFunction());
    FunctionService.registerFunction(new TouchPartitionedRegionEntriesFunction());
    FunctionService.registerFunction(new TouchReplicatedRegionEntriesFunction());
  }

  @Test
  @ConnectionConfiguration(user = "dataWrite", password = "dataWrite")
  public void testInvalidPermissionsForBootstrappingFunction() throws Exception {
    gfsh.executeAndAssertThat("execute function --id=" + BootstrappingFunction.ID)
        .tableHasColumnWithExactValuesInAnyOrder(RESULT_HEADER,
            "Exception: dataWrite not authorized for CLUSTER:MANAGE")
        .statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "dataWrite", password = "dataWrite")
  public void testInvalidPermissionsForCreateRegionFunction() throws Exception {
    gfsh.executeAndAssertThat("execute function --id=" + CreateRegionFunction.ID)
        .tableHasColumnWithExactValuesInAnyOrder(RESULT_HEADER,
            "Exception: dataWrite not authorized for DATA:MANAGE")
        .statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "dataWrite", password = "dataWrite")
  public void testInvalidPermissionsForRegionSizeFunction() throws Exception {
    gfsh.executeAndAssertThat("execute function --region=REPLICATE_1 --id=" + RegionSizeFunction.ID)
        .tableHasColumnWithExactValuesInAnyOrder(RESULT_HEADER,
            "Exception: dataWrite not authorized for DATA:READ:REPLICATE_1")
        .statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "dataWrite", password = "dataWrite")
  public void testInvalidPermissionsForTouchPartitionedRegionEntriesFunction() throws Exception {
    gfsh.executeAndAssertThat(
        "execute function --region=PARTITION_1 --id=" + TouchPartitionedRegionEntriesFunction.ID)
        .tableHasColumnWithExactValuesInAnyOrder(RESULT_HEADER,
            "Exception: dataWrite not authorized for DATA:READ:PARTITION_1")
        .statusIsError();
  }

  @Test
  @ConnectionConfiguration(user = "dataWrite", password = "dataWrite")
  public void testInvalidPermissionsForTouchReplicatedRegionEntriesFunction() throws Exception {
    gfsh.executeAndAssertThat(
        "execute function --region=REPLICATE_1 --id=" + TouchReplicatedRegionEntriesFunction.ID)
        .tableHasColumnWithExactValuesInAnyOrder(RESULT_HEADER,
            "Exception: dataWrite not authorized for DATA:READ:REPLICATE_1")
        .statusIsError();
  }
}
