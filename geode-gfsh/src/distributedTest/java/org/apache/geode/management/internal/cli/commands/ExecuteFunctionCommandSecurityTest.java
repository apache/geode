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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.security.ResourceConstants;
import org.apache.geode.management.internal.security.TestFunctions.ReadFunction;
import org.apache.geode.management.internal.security.TestFunctions.WriteFunction;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(SecurityTest.class)
public class ExecuteFunctionCommandSecurityTest implements Serializable {

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator;

  private static String REPLICATED_REGION = "replicatedRegion";
  private static String PARTITIONED_REGION = "partitionedRegion";

  @BeforeClass
  public static void beforeClass() {
    Properties locatorProps = new Properties();
    locatorProps.setProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName());
    locator = lsRule.startLocatorVM(0, locatorProps);

    Properties serverProps = new Properties();
    serverProps.setProperty(ResourceConstants.USER_NAME, "clusterManage");
    serverProps.setProperty(ResourceConstants.PASSWORD, "clusterManage");
    MemberVM server1 = lsRule.startServerVM(1, serverProps, locator.getPort());
    MemberVM server2 = lsRule.startServerVM(2, serverProps, locator.getPort());

    Stream.of(server1, server2).forEach(server -> server.invoke(() -> {
      FunctionService.registerFunction(new ReadFunction());
      FunctionService.registerFunction(new WriteFunction());

      InternalCache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      cache.createRegionFactory(RegionShortcut.REPLICATE).create(REPLICATED_REGION);
      cache.createRegionFactory(RegionShortcut.PARTITION).create(PARTITIONED_REGION);
    }));

    locator.invoke(ExecuteFunctionCommandSecurityTest::waitUntilRegionMBeansAreRegistered);
  }


  @Test
  public void dataReaderCanExecuteReadFunction() throws Exception {
    gfsh.secureConnectAndVerify(locator.getPort(), GfshCommandRule.PortType.locator, "dataRead",
        "dataRead");
    gfsh.executeAndAssertThat("execute function --id=" + new ReadFunction().getId())
        .statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains(ReadFunction.SUCCESS_OUTPUT);
  }

  @Test
  public void dataReaderCanNotExecuteWriteFunction() throws Exception {
    gfsh.secureConnectAndVerify(locator.getPort(), GfshCommandRule.PortType.locator, "dataRead",
        "dataRead");
    gfsh.executeAndAssertThat("execute function --id=" + new WriteFunction().getId())
        .containsOutput("dataRead not authorized for DATA:WRITE")
        .doesNotContainOutput(WriteFunction.SUCCESS_OUTPUT);
  }

  @Test
  public void dataWriterCanExecuteWriteFunction() throws Exception {
    gfsh.secureConnectAndVerify(locator.getPort(), GfshCommandRule.PortType.locator, "dataWrite",
        "dataWrite");
    gfsh.executeAndAssertThat("execute function --id=" + new WriteFunction().getId())
        .statusIsSuccess();
    assertThat(gfsh.getGfshOutput()).contains(WriteFunction.SUCCESS_OUTPUT);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void dataWriterCanNotExecuteReadFunction() throws Exception {
    gfsh.secureConnectAndVerify(locator.getPort(), GfshCommandRule.PortType.locator, "dataWrite",
        "dataWrite");
    gfsh.executeCommand("execute function --id=" + new ReadFunction().getId());
    assertThat(gfsh.getGfshOutput()).contains("dataWrite not authorized for DATA:READ");
    assertThat(gfsh.getGfshOutput()).doesNotContain(ReadFunction.SUCCESS_OUTPUT);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void readOnlyUserOnReplicatedRegion() throws Exception {
    gfsh.secureConnectAndVerify(locator.getPort(), GfshCommandRule.PortType.locator, "dataRead",
        "dataRead");
    gfsh.executeAndAssertThat(
        "execute function --id=" + new ReadFunction().getId() + " --region=" + REPLICATED_REGION)
        .statusIsSuccess().containsOutput(ReadFunction.SUCCESS_OUTPUT);

    gfsh.executeAndAssertThat(
        "execute function --id=" + new WriteFunction().getId() + " --region=" + REPLICATED_REGION)
        .statusIsError().tableHasColumnWithExactValuesInAnyOrder("Message",
            "Exception: dataRead not authorized for DATA:WRITE");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void readOnlyUserOnPartitionedRegion() throws Exception {
    gfsh.secureConnectAndVerify(locator.getPort(), GfshCommandRule.PortType.locator, "dataRead",
        "dataRead");
    gfsh.executeAndAssertThat(
        "execute function --id=" + new ReadFunction().getId() + " --region=" + PARTITIONED_REGION)
        .statusIsSuccess().containsOutput(ReadFunction.SUCCESS_OUTPUT);

    gfsh.executeAndAssertThat(
        "execute function --id=" + new WriteFunction().getId() + " --region=" + PARTITIONED_REGION)
        .statusIsError().tableHasColumnWithExactValuesInAnyOrder("Message",
            "Exception: dataRead not authorized for DATA:WRITE");
  }

  private static void waitUntilRegionMBeansAreRegistered() {
    await().untilAsserted(() -> {
      Set<DistributedMember> regionMembers =
          ManagementUtils.getRegionAssociatedMembers(REPLICATED_REGION,
              (InternalCache) CacheFactory.getAnyInstance(), true);
      assertThat(regionMembers).hasSize(2);
    });
  }
}
