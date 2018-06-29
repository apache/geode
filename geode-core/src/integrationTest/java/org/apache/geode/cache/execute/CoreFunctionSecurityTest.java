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

package org.apache.geode.cache.execute;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.internal.cli.functions.AlterRuntimeConfigFunction;
import org.apache.geode.management.internal.cli.functions.ChangeLogLevelFunction;
import org.apache.geode.management.internal.cli.functions.CloseDurableClientFunction;
import org.apache.geode.management.internal.cli.functions.CloseDurableCqFunction;
import org.apache.geode.management.internal.cli.functions.ContinuousQueryFunction;
import org.apache.geode.management.internal.cli.functions.CreateAsyncEventQueueFunction;
import org.apache.geode.management.internal.cli.functions.CreateDefinedIndexesFunction;
import org.apache.geode.management.internal.cli.functions.CreateDiskStoreFunction;
import org.apache.geode.management.internal.cli.functions.CreateIndexFunction;
import org.apache.geode.management.internal.cli.functions.DataCommandFunction;
import org.apache.geode.management.internal.cli.functions.DeployFunction;
import org.apache.geode.management.internal.cli.functions.DescribeDiskStoreFunction;
import org.apache.geode.management.internal.cli.functions.DestroyAsyncEventQueueFunction;
import org.apache.geode.management.internal.cli.functions.DestroyDiskStoreFunction;
import org.apache.geode.management.internal.cli.functions.DestroyIndexFunction;
import org.apache.geode.management.internal.cli.functions.ExportConfigFunction;
import org.apache.geode.management.internal.cli.functions.ExportDataFunction;
import org.apache.geode.management.internal.cli.functions.ExportLogsFunction;
import org.apache.geode.management.internal.cli.functions.FetchRegionAttributesFunction;
import org.apache.geode.management.internal.cli.functions.FetchSharedConfigurationStatusFunction;
import org.apache.geode.management.internal.cli.functions.GarbageCollectionFunction;
import org.apache.geode.management.internal.cli.functions.GatewayReceiverCreateFunction;
import org.apache.geode.management.internal.cli.functions.GatewaySenderCreateFunction;
import org.apache.geode.management.internal.cli.functions.GatewaySenderDestroyFunction;
import org.apache.geode.management.internal.cli.functions.GetMemberConfigInformationFunction;
import org.apache.geode.management.internal.cli.functions.GetMemberInformationFunction;
import org.apache.geode.management.internal.cli.functions.GetRegionDescriptionFunction;
import org.apache.geode.management.internal.cli.functions.GetRegionsFunction;
import org.apache.geode.management.internal.cli.functions.GetStackTracesFunction;
import org.apache.geode.management.internal.cli.functions.GetSubscriptionQueueSizeFunction;
import org.apache.geode.management.internal.cli.functions.ImportDataFunction;
import org.apache.geode.management.internal.cli.functions.ListAsyncEventQueuesFunction;
import org.apache.geode.management.internal.cli.functions.ListDeployedFunction;
import org.apache.geode.management.internal.cli.functions.ListDiskStoresFunction;
import org.apache.geode.management.internal.cli.functions.ListDurableCqNamesFunction;
import org.apache.geode.management.internal.cli.functions.ListFunctionFunction;
import org.apache.geode.management.internal.cli.functions.ListIndexFunction;
import org.apache.geode.management.internal.cli.functions.NetstatFunction;
import org.apache.geode.management.internal.cli.functions.RebalanceFunction;
import org.apache.geode.management.internal.cli.functions.RegionAlterFunction;
import org.apache.geode.management.internal.cli.functions.RegionCreateFunction;
import org.apache.geode.management.internal.cli.functions.RegionDestroyFunction;
import org.apache.geode.management.internal.cli.functions.ShowMissingDiskStoresFunction;
import org.apache.geode.management.internal.cli.functions.ShutDownFunction;
import org.apache.geode.management.internal.cli.functions.SizeExportLogsFunction;
import org.apache.geode.management.internal.cli.functions.UndeployFunction;
import org.apache.geode.management.internal.cli.functions.UnregisterFunction;
import org.apache.geode.management.internal.cli.functions.UserFunctionExecution;
import org.apache.geode.management.internal.configuration.functions.DownloadJarFunction;
import org.apache.geode.management.internal.configuration.functions.GetClusterConfigurationFunction;
import org.apache.geode.management.internal.configuration.functions.GetRegionNamesFunction;
import org.apache.geode.management.internal.configuration.functions.RecreateCacheFunction;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.ConnectionConfiguration;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;


@Category(IntegrationTest.class)
public class CoreFunctionSecurityTest {
  private static final String RESULT_HEADER = "Message";

  @ClassRule
  public static ServerStarterRule server =
      new ServerStarterRule().withJMXManager().withSecurityManager(SimpleSecurityManager.class)
          .withRegion(RegionShortcut.PARTITION, "testRegion").withAutoStart();

  @Rule
  public GfshCommandRule gfsh =
      new GfshCommandRule(server::getJmxPort, GfshCommandRule.PortType.jmxManager);

  private static Map<Function, String> functionStringMap = new HashMap<>();

  @BeforeClass
  public static void setupClass() {
    functionStringMap.put(new AlterRuntimeConfigFunction(), "*");
    functionStringMap.put(new ChangeLogLevelFunction(), "*");
    functionStringMap.put(new CloseDurableClientFunction(), "*");
    functionStringMap.put(new CloseDurableCqFunction(), "*");
    functionStringMap.put(new ContinuousQueryFunction(), "*");
    functionStringMap.put(new CreateAsyncEventQueueFunction(), "*");
    functionStringMap.put(new CreateDefinedIndexesFunction(), "*");
    functionStringMap.put(new CreateDiskStoreFunction(), "*");
    functionStringMap.put(new CreateIndexFunction(), "*");
    functionStringMap.put(new DataCommandFunction(), "*");
    functionStringMap.put(new DeployFunction(), "*");
    functionStringMap.put(new DescribeDiskStoreFunction(), "*");
    functionStringMap.put(new DestroyAsyncEventQueueFunction(), "*");
    functionStringMap.put(new DestroyDiskStoreFunction(), "*");
    functionStringMap.put(new DestroyIndexFunction(), "*");
    functionStringMap.put(new ExportConfigFunction(), "*");
    functionStringMap.put(new ExportDataFunction(), "*");
    functionStringMap.put(new ExportLogsFunction(), "*");
    functionStringMap.put(new FetchRegionAttributesFunction(), "*");
    functionStringMap.put(new FetchSharedConfigurationStatusFunction(), "*");
    functionStringMap.put(new GarbageCollectionFunction(), "*");
    functionStringMap.put(new GatewayReceiverCreateFunction(), "*");
    functionStringMap.put(new GatewaySenderCreateFunction(), "*");
    functionStringMap.put(new GatewaySenderDestroyFunction(), "*");
    functionStringMap.put(new GetClusterConfigurationFunction(), "*");
    functionStringMap.put(new GetMemberConfigInformationFunction(), "*");
    functionStringMap.put(new GetMemberInformationFunction(), "*");
    functionStringMap.put(new GetRegionDescriptionFunction(), "*");
    functionStringMap.put(new GetRegionsFunction(), "*");
    functionStringMap.put(new GetStackTracesFunction(), "*");
    functionStringMap.put(new GetSubscriptionQueueSizeFunction(), "*");
    functionStringMap.put(new ImportDataFunction(), "*");
    functionStringMap.put(new ListAsyncEventQueuesFunction(), "*");
    functionStringMap.put(new ListDeployedFunction(), "*");
    functionStringMap.put(new ListDiskStoresFunction(), "*");
    functionStringMap.put(new ListDurableCqNamesFunction(), "*");
    functionStringMap.put(new ListFunctionFunction(), "*");
    functionStringMap.put(new ListIndexFunction(), "*");
    functionStringMap.put(new NetstatFunction(), "*");
    functionStringMap.put(new RebalanceFunction(), "*");
    functionStringMap.put(new RegionAlterFunction(), "*");
    functionStringMap.put(new RegionCreateFunction(), "*");
    functionStringMap.put(new RegionDestroyFunction(), "*");
    functionStringMap.put(new ShowMissingDiskStoresFunction(), "*");
    functionStringMap.put(new ShutDownFunction(), "*");
    functionStringMap.put(new SizeExportLogsFunction(), "*");
    functionStringMap.put(new UndeployFunction(), "*");
    functionStringMap.put(new UnregisterFunction(), "*");
    functionStringMap.put(new GetRegionNamesFunction(), "*");
    functionStringMap.put(new RecreateCacheFunction(), "*");
    functionStringMap.put(new DownloadJarFunction(), "*");

    functionStringMap.keySet().forEach(FunctionService::registerFunction);
  }

  @Test
  @ConnectionConfiguration(user = "user", password = "user")
  public void functionRequireExpectedPermission() throws Exception {
    functionStringMap.entrySet().stream().forEach(entry -> {
      Function function = entry.getKey();
      String permission = entry.getValue();
      System.out.println("function: " + function.getId() + ", permission: " + permission);
      gfsh.executeAndAssertThat("execute function --id=" + function.getId())
          .tableHasRowCount(RESULT_HEADER, 1)
          .tableHasRowWithValues(RESULT_HEADER, "Exception: user not authorized for " + permission)
          .statusIsError();
    });
  }

  @Test
  public void userFunctionExecutionRequiresNoSecurity() {
    Function function = new UserFunctionExecution();
    assertThat(function.getRequiredPermissions("testRegion")).isEmpty();
  }
}
