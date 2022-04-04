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
package org.apache.geode.internal.cache.wan.parallel;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.getMember;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.validateGatewayReceiverMXBeanProxy;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.validateGatewaySenderMXBeanProxy;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifyReceiverState;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifySenderState;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({WanTest.class})
public class ParallelGatewaySenderReplicationDUnitTest implements Serializable {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(11);

  @Rule
  public transient GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM locator1Site2;
  private MemberVM locator1Site1;
  private MemberVM locator2Site1;
  private MemberVM server1Site1;
  private MemberVM server2Site1;
  private MemberVM server3Site1;
  private MemberVM server1Site2;
  private MemberVM server2Site2;
  private MemberVM server3Site2;
  private MemberVM server4Site2;

  private ClientVM clientSite2;
  private ClientVM clientSite1;

  private static final String DISTRIBUTED_SYSTEM_ID_SITE1 = "1";
  private static final String DISTRIBUTED_SYSTEM_ID_SITE2 = "2";
  private static final String REGION_NAME = "test1";

  @Before
  public void setupMultiSiteWithClients() throws Exception {
    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, DISTRIBUTED_SYSTEM_ID_SITE1);
    locator1Site1 = clusterStartupRule.startLocatorVM(0, props);
    locator2Site1 = clusterStartupRule.startLocatorVM(1, props, locator1Site1.getPort());

    Properties serverProps = new Properties();
    // start servers for site #1
    server1Site1 =
        clusterStartupRule.startServerVM(2, serverProps, locator1Site1.getPort(),
            locator2Site1.getPort());
    server2Site1 =
        clusterStartupRule.startServerVM(3, serverProps, locator1Site1.getPort(),
            locator2Site1.getPort());
    server3Site1 =
        clusterStartupRule.startServerVM(4, serverProps, locator1Site1.getPort(),
            locator2Site1.getPort());

    connectGfshToSite(locator1Site1);

    // create partition region on site #1
    CommandStringBuilder regionCmd = new CommandStringBuilder(CliStrings.CREATE_REGION);
    regionCmd.addOption(CliStrings.CREATE_REGION__REGION, REGION_NAME);
    regionCmd.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION");
    regionCmd.addOption(CliStrings.CREATE_REGION__REDUNDANTCOPIES, "1");

    gfsh.executeAndAssertThat(regionCmd.toString()).statusIsSuccess();

    String csb = new CommandStringBuilder(CliStrings.CREATE_GATEWAYRECEIVER)
        .addOption(CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS, "localhost")
        .getCommandString();

    gfsh.executeAndAssertThat(csb).statusIsSuccess();

    server1Site1.invoke(() -> verifyReceiverState(true));
    locator2Site1.invoke(
        () -> validateGatewayReceiverMXBeanProxy(getMember(server1Site1.getVM()), true));
    locator1Site1.invoke(
        () -> validateGatewayReceiverMXBeanProxy(getMember(server1Site1.getVM()), true));

    server2Site1.invoke(() -> verifyReceiverState(true));
    locator2Site1.invoke(
        () -> validateGatewayReceiverMXBeanProxy(getMember(server2Site1.getVM()), true));
    locator1Site1.invoke(
        () -> validateGatewayReceiverMXBeanProxy(getMember(server2Site1.getVM()), true));

    server3Site1.invoke(() -> verifyReceiverState(true));
    locator2Site1.invoke(
        () -> validateGatewayReceiverMXBeanProxy(getMember(server3Site1.getVM()), true));
    locator1Site1.invoke(
        () -> validateGatewayReceiverMXBeanProxy(getMember(server3Site1.getVM()), true));

    props.setProperty(DISTRIBUTED_SYSTEM_ID, DISTRIBUTED_SYSTEM_ID_SITE2);
    props.setProperty(REMOTE_LOCATORS,
        "localhost[" + locator1Site1.getPort() + "],localhost[" + locator2Site1.getPort() + "]");
    locator1Site2 = clusterStartupRule.startLocatorVM(5, props);

    // start servers for site #2
    server1Site2 = clusterStartupRule.startServerVM(6, serverProps, locator1Site2.getPort());
    server2Site2 = clusterStartupRule.startServerVM(7, serverProps, locator1Site2.getPort());
    server3Site2 = clusterStartupRule.startServerVM(10, serverProps, locator1Site2.getPort());
    server4Site2 = clusterStartupRule.startServerVM(11, serverProps, locator1Site2.getPort());

    connectGfshToSite(locator1Site2);
    // crate disk store
    regionCmd = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE)
        .addOption(CliStrings.CREATE_DISK_STORE__NAME, "gwsdisk")
        .addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, "gwsdisk");
    gfsh.executeAndAssertThat(regionCmd.getCommandString()).statusIsSuccess();

    // create parallel gateway-sender on site #2
    String command = new CommandStringBuilder(CliStrings.CREATE_GATEWAYSENDER)
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ID, "ln")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID, "1")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__PARALLEL, "true")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY, "key")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__ENABLEPERSISTENCE, "true")
        .addOption(CliStrings.CREATE_GATEWAYSENDER__DISKSTORENAME, "gwsdisk")
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();

    server1Site2.invoke(() -> verifySenderState("ln", true, false));
    server2Site2.invoke(() -> verifySenderState("ln", true, false));
    server3Site2.invoke(() -> verifySenderState("ln", true, false));
    server4Site2.invoke(() -> verifySenderState("ln", true, false));
    locator1Site2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server1Site2.getVM()), "ln", true, false));
    locator1Site2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server2Site2.getVM()), "ln", true, false));
    locator1Site2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server3Site2.getVM()), "ln", true, false));
    locator1Site2.invoke(
        () -> validateGatewaySenderMXBeanProxy(getMember(server4Site2.getVM()), "ln", true, false));

    // crate disk store
    regionCmd = new CommandStringBuilder(CliStrings.CREATE_DISK_STORE)
        .addOption(CliStrings.CREATE_DISK_STORE__NAME, "diskStore")
        .addOption(CliStrings.CREATE_DISK_STORE__DIRECTORY_AND_SIZE, "diskStoreDir");
    gfsh.executeAndAssertThat(regionCmd.getCommandString()).statusIsSuccess();

    // create partition region on site #2
    regionCmd = new CommandStringBuilder(CliStrings.CREATE_REGION);
    regionCmd.addOption(CliStrings.CREATE_REGION__REGION, REGION_NAME);
    regionCmd.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_PERSISTENT");
    regionCmd.addOption(CliStrings.CREATE_REGION__GATEWAYSENDERID, "ln");
    regionCmd.addOption(CliStrings.CREATE_REGION__REDUNDANTCOPIES, "1");
    regionCmd.addOption(CliStrings.CREATE_REGION__DISKSTORE, "diskStore");
    regionCmd.addOption(CliStrings.CREATE_REGION__TOTALNUMBUCKETS, "113");
    regionCmd.addOption(CliStrings.CREATE_REGION__DISKSYNCHRONOUS, "false");
    gfsh.executeAndAssertThat(regionCmd.toString()).statusIsSuccess();

    startClientSite2(locator1Site2.getPort());
    startClientSite1(locator1Site1.getPort());
  }

  @Test
  public void testGatewaySenderReplicatesEventsWhenRecreatingPartitionedRegion() throws Exception {
    // Do put operations to initialize some buckets
    doPutsClientSite2(0, 500);

    executeAlterRegionCommand();
    executedDestroyRegion();
    executedCreateRegion();

    doPutsClientSite2(500, 2000);
    checkDataReplicatedOnSite1(500, 2000);
  }

  void executeAlterRegionCommand()
      throws Exception {
    connectGfshToSite(locator1Site2);
    String command;
    command = new CommandStringBuilder(CliStrings.ALTER_REGION)
        .addOption(CliStrings.ALTER_REGION__REGION, REGION_NAME)
        .addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, "")
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }

  void executedDestroyRegion()
      throws Exception {
    connectGfshToSite(locator1Site2);
    String command;
    command = new CommandStringBuilder(CliStrings.DESTROY_REGION)
        .addOption(CliStrings.DESTROY_REGION__REGION, REGION_NAME)
        .getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess();
  }

  void executedCreateRegion()
      throws Exception {
    connectGfshToSite(locator1Site2);
    CommandStringBuilder regionCmd = new CommandStringBuilder(CliStrings.CREATE_REGION);
    regionCmd.addOption(CliStrings.CREATE_REGION__REGION, REGION_NAME);
    regionCmd.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION_PERSISTENT");
    regionCmd.addOption(CliStrings.CREATE_REGION__GATEWAYSENDERID, "ln");
    regionCmd.addOption(CliStrings.CREATE_REGION__REDUNDANTCOPIES, "1");
    regionCmd.addOption(CliStrings.CREATE_REGION__DISKSTORE, "diskStore");
    regionCmd.addOption(CliStrings.CREATE_REGION__TOTALNUMBUCKETS, "113");
    regionCmd.addOption(CliStrings.CREATE_REGION__DISKSYNCHRONOUS, "false");
    gfsh.executeAndAssertThat(regionCmd.toString()).statusIsSuccess();
  }

  void startClientSite2(int locatorPort) throws Exception {
    clientSite2 =
        clusterStartupRule.startClientVM(8, c -> c.withLocatorConnection(locatorPort));
    clientSite2.invoke(() -> {
      ClusterStartupRule.clientCacheRule.createProxyRegion(REGION_NAME);
    });
  }

  void startClientSite1(int locatorPort) throws Exception {
    clientSite1 =
        clusterStartupRule.startClientVM(9, c -> c.withLocatorConnection(locatorPort));
    clientSite1.invoke(() -> {
      ClusterStartupRule.clientCacheRule.createProxyRegion(REGION_NAME);
    });
  }

  void doPutsClientSite2(int startRange, int stopRange) {
    clientSite2.invoke(() -> {
      Region<Integer, Integer> region =
          ClusterStartupRule.clientCacheRule.getCache().getRegion(REGION_NAME);
      for (int i = startRange; i < stopRange; i++) {
        region.put(i, i);
      }
    });
  }

  void checkDataReplicatedOnSite1(int startRange, int stopRange) {
    clientSite1.invoke(() -> {
      Region<Integer, Integer> region =
          ClusterStartupRule.clientCacheRule.getCache().getRegion(REGION_NAME);
      List<Integer> missingIntegers = new ArrayList<>();
      for (int i = startRange; i < stopRange; i++) {
        Integer result = region.get(i, i);
        if (result == null) {
          missingIntegers.add(i);
        }
      }
      assertThat(missingIntegers).isEmpty();
    });
  }

  void connectGfshToSite(MemberVM locator) throws Exception {
    if (gfsh.isConnected()) {
      gfsh.disconnect();
    }
    gfsh.connectAndVerify(locator);
  }
}
