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
  private static final String RESULT_HEADER = "Function Execution Result";

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
    functionStringMap.put(new AlterRuntimeConfigFunction(), "CLUSTER:WRITE");
    functionStringMap.put(new ChangeLogLevelFunction(), "CLUSTER:WRITE");
    functionStringMap.put(new CloseDurableClientFunction(), "CLUSTER:MANAGE:QUERY");
    functionStringMap.put(new CloseDurableCqFunction(), "CLUSTER:MANAGE:QUERY");
    functionStringMap.put(new ContinuousQueryFunction(), "CLUSTER:READ");
    functionStringMap.put(new CreateAsyncEventQueueFunction(), "CLUSTER:MANAGE:DEPLOY");
    functionStringMap.put(new CreateDefinedIndexesFunction(), "CLUSTER:MANAGE:QUERY");
    functionStringMap.put(new CreateDiskStoreFunction(), "CLUSTER:MANAGE:DISK");
    functionStringMap.put(new CreateIndexFunction(), "CLUSTER:MANAGE:QUERY");
    functionStringMap.put(new DataCommandFunction(), "DATA");
    functionStringMap.put(new DeployFunction(), "CLUSTER:MANAGE:DEPLOY");
    functionStringMap.put(new DescribeDiskStoreFunction(), "CLUSTER:READ");
    functionStringMap.put(new DestroyAsyncEventQueueFunction(), "CLUSTER:MANAGE");
    functionStringMap.put(new DestroyDiskStoreFunction(), "CLUSTER:MANAGE:DISK");
    functionStringMap.put(new DestroyIndexFunction(), "CLUSTER:MANAGE:QUERY");
    functionStringMap.put(new ExportConfigFunction(), "CLUSTER:READ");
    functionStringMap.put(new ExportDataFunction(), "DATA:READ");
    functionStringMap.put(new ExportLogsFunction(), "CLUSTER:READ");
    functionStringMap.put(new FetchRegionAttributesFunction(), "CLUSTER:READ");
    functionStringMap.put(new FetchSharedConfigurationStatusFunction(), "CLUSTER:READ");
    functionStringMap.put(new GarbageCollectionFunction(), "CLUSTER:MANAGE");
    functionStringMap.put(new GatewayReceiverCreateFunction(), "CLUSTER:MANAGE:GATEWAY");
    functionStringMap.put(new GatewaySenderCreateFunction(), "CLUSTER:MANAGE:GATEWAY");
    functionStringMap.put(new GatewaySenderDestroyFunction(), "CLUSTER:MANAGE:GATEWAY");
    functionStringMap.put(new GetClusterConfigurationFunction(), "*");
    functionStringMap.put(new GetMemberConfigInformationFunction(), "CLUSTER:READ");
    functionStringMap.put(new GetMemberInformationFunction(), "CLUSTER:READ");
    functionStringMap.put(new GetRegionDescriptionFunction(), "CLUSTER:READ");
    functionStringMap.put(new GetRegionsFunction(), "CLUSTER:READ");
    functionStringMap.put(new GetStackTracesFunction(), "CLUSTER:READ");
    functionStringMap.put(new GetSubscriptionQueueSizeFunction(), "CLUSTER:READ");
    functionStringMap.put(new ImportDataFunction(), "DATA:WRITE");
    functionStringMap.put(new ListAsyncEventQueuesFunction(), "CLUSTER:READ");
    functionStringMap.put(new ListDeployedFunction(), "CLUSTER:READ");
    functionStringMap.put(new ListDiskStoresFunction(), "CLUSTER:READ");
    functionStringMap.put(new ListDurableCqNamesFunction(), "CLUSTER:READ");
    functionStringMap.put(new ListFunctionFunction(), "CLUSTER:READ");
    functionStringMap.put(new ListIndexFunction(), "CLUSTER:READ:QUERY");
    functionStringMap.put(new NetstatFunction(), "CLUSTER:READ");
    functionStringMap.put(new RebalanceFunction(), "DATA:MANAGE");
    functionStringMap.put(new RegionAlterFunction(), "DATA:MANAGE");
    functionStringMap.put(new RegionCreateFunction(), "DATA:MANAGE");
    functionStringMap.put(new RegionDestroyFunction(), "DATA:MANAGE");
    functionStringMap.put(new ShowMissingDiskStoresFunction(), "CLUSTER:READ");
    functionStringMap.put(new ShutDownFunction(), "CLUSTER:MANAGE");
    functionStringMap.put(new SizeExportLogsFunction(), "CLUSTER:READ");
    functionStringMap.put(new UndeployFunction(), "CLUSTER:MANAGE:DEPLOY");
    functionStringMap.put(new UnregisterFunction(), "CLUSTER:MANAGE:DEPLOY");
    functionStringMap.put(new GetRegionNamesFunction(), "CLUSTER:READ");
    functionStringMap.put(new RecreateCacheFunction(), "CLUSTER:MANAGE");
    functionStringMap.put(new DownloadJarFunction(), "CLUSTER:READ");

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
