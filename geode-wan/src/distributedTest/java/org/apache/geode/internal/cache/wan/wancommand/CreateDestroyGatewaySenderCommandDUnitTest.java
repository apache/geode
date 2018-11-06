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
package org.apache.geode.internal.cache.wan.wancommand;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifySenderAttributes;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifySenderDoesNotExist;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifySenderState;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;

@Category({WanTest.class})
@SuppressWarnings("serial")
public class CreateDestroyGatewaySenderCommandDUnitTest implements Serializable {

  private static final String SERVER_3 = "server-3";
  private static final String SERVER_4 = "server-4";
  private static final String SERVER_5 = "server-5";

  private static final String CREATE =
      "create gateway-sender --id=ln " + "--remote-distributed-system-id=2";
  private static final String DESTROY = "destroy gateway-sender --id=ln ";

  @ClassRule
  public static ClusterStartupRule clusterStartupRule = new ClusterStartupRule(8);

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locatorSite1;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;

  @BeforeClass
  public static void beforeClass() throws Exception {
    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    locatorSite1 = clusterStartupRule.startLocatorVM(1, props);

    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 2);
    props.setProperty(REMOTE_LOCATORS, "localhost[" + locatorSite1.getPort() + "]");
    clusterStartupRule.startLocatorVM(2, props);

    server1 = clusterStartupRule.startServerVM(3, "senderGroup1", locatorSite1.getPort());
    server2 = clusterStartupRule.startServerVM(4, locatorSite1.getPort());
    server3 = clusterStartupRule.startServerVM(5, locatorSite1.getPort());
  }

  @Before
  public void before() throws Exception {
    gfsh.connectAndVerify(locatorSite1);
  }

  @After
  public void after() {
    gfsh.executeAndAssertThat(DESTROY + " --if-exists").statusIsSuccess();
  }

  /**
   * GatewaySender with all default attributes
   */
  @Test
  public void testCreateDestroyGatewaySenderWithDefault() throws Exception {
    gfsh.executeAndAssertThat(CREATE).statusIsSuccess().tableHasColumnWithExactValuesInAnyOrder(
        "Message", "GatewaySender \"ln\" created on \"" + SERVER_3 + "\"",
        "GatewaySender \"ln\" created on \"" + SERVER_4 + "\"",
        "GatewaySender \"ln\" created on \"" + SERVER_5 + "\"");

    VMProvider.invokeInEveryMember(() -> verifySenderState("ln", true, false), server1, server2,
        server3);

    locatorSite1.invoke(() -> {
      String xml = ClusterStartupRule.getLocator().getConfigurationPersistenceService()
          .getConfiguration("cluster").getCacheXmlContent();
      assertThat(xml).contains("<gateway-sender id=\"ln\" remote-distributed-system-id=\"2\""
          + " parallel=\"false\"/>");
    });

    // destroy gateway sender and verify AEQs cleaned up
    gfsh.executeAndAssertThat(DESTROY).statusIsSuccess().tableHasColumnWithExactValuesInAnyOrder(
        "Message", "GatewaySender \"ln\" destroyed on \"" + SERVER_3 + "\"",
        "GatewaySender \"ln\" destroyed on \"" + SERVER_4 + "\"",
        "GatewaySender \"ln\" destroyed on \"" + SERVER_5 + "\"");

    VMProvider.invokeInEveryMember(() -> verifySenderDoesNotExist("ln", false), server1, server2,
        server3);

    locatorSite1.invoke(() -> {
      String xml = ClusterStartupRule.getLocator().getConfigurationPersistenceService()
          .getConfiguration("cluster").getCacheXmlContent();
      assertThat(xml).doesNotContain("gateway-sender id=\"ln\"");
    });
  }

  /**
   * GatewaySender with given attribute values +
   */
  @Test
  public void testCreateDestroyGatewaySender() throws Exception {
    int socketReadTimeout = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 1000;
    String command = CliStrings.CREATE_GATEWAYSENDER + " --" + CliStrings.CREATE_GATEWAYSENDER__ID
        + "=ln" + " --" + CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__PARALLEL + "=false" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__MANUALSTART + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT + "=" + socketReadTimeout + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__BATCHSIZE + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL + "=5000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ENABLEPERSISTENCE + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__DISKSYNCHRONOUS + "=false" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__MAXQUEUEMEMORY + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ALERTTHRESHOLD + "=100" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY + "=THREAD";
    gfsh.executeAndAssertThat(command).statusIsSuccess().tableHasColumnWithExactValuesInAnyOrder(
        "Message", "GatewaySender \"ln\" created on \"" + SERVER_3 + "\"",
        "GatewaySender \"ln\" created on \"" + SERVER_4 + "\"",
        "GatewaySender \"ln\" created on \"" + SERVER_5 + "\"");

    VMProvider.invokeInEveryMember(() -> {
      verifySenderState("ln", false, false);
      verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true, 1000, 5000, true,
          false, 1000, 100, 2, GatewaySender.OrderPolicy.THREAD, null, null);
    }, server1, server2, server3);

    // destroy gateway sender and verify AEQs cleaned up
    gfsh.executeAndAssertThat(DESTROY).statusIsSuccess().tableHasColumnWithExactValuesInAnyOrder(
        "Message", "GatewaySender \"ln\" destroyed on \"" + SERVER_3 + "\"",
        "GatewaySender \"ln\" destroyed on \"" + SERVER_4 + "\"",
        "GatewaySender \"ln\" destroyed on \"" + SERVER_5 + "\"");

    VMProvider.invokeInEveryMember(() -> verifySenderDoesNotExist("ln", false), server1, server2,
        server3);
  }

  /**
   * GatewaySender with given attribute values and event filters.
   */
  @Test
  public void testCreateDestroyGatewaySenderWithGatewayEventFilters() throws Exception {
    int socketReadTimeout = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 1000;
    String command = CliStrings.CREATE_GATEWAYSENDER + " --" + CliStrings.CREATE_GATEWAYSENDER__ID
        + "=ln" + " --" + CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__PARALLEL + "=false" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__MANUALSTART + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT + "=" + socketReadTimeout + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__BATCHSIZE + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL + "=5000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ENABLEPERSISTENCE + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__DISKSYNCHRONOUS + "=false" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__MAXQUEUEMEMORY + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ALERTTHRESHOLD + "=100" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY + "=THREAD" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__GATEWAYEVENTFILTER
        + "=org.apache.geode.cache30.MyGatewayEventFilter1,org.apache.geode.cache30.MyGatewayEventFilter2";

    gfsh.executeAndAssertThat(command).statusIsSuccess().tableHasColumnWithExactValuesInAnyOrder(
        "Message", "GatewaySender \"ln\" created on \"" + SERVER_3 + "\"",
        "GatewaySender \"ln\" created on \"" + SERVER_4 + "\"",
        "GatewaySender \"ln\" created on \"" + SERVER_5 + "\"");

    List<String> eventFilters = new ArrayList<>();
    eventFilters.add("org.apache.geode.cache30.MyGatewayEventFilter1");
    eventFilters.add("org.apache.geode.cache30.MyGatewayEventFilter2");

    VMProvider.invokeInEveryMember(() -> {
      verifySenderState("ln", false, false);
      verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true, 1000, 5000, true,
          false, 1000, 100, 2, GatewaySender.OrderPolicy.THREAD, eventFilters, null);
    }, server1, server2, server3);

    // destroy gateway sender and verify AEQs cleaned up
    gfsh.executeAndAssertThat(DESTROY).statusIsSuccess().tableHasColumnWithExactValuesInAnyOrder(
        "Message", "GatewaySender \"ln\" destroyed on \"" + SERVER_3 + "\"",
        "GatewaySender \"ln\" destroyed on \"" + SERVER_4 + "\"",
        "GatewaySender \"ln\" destroyed on \"" + SERVER_5 + "\"");

    VMProvider.invokeInEveryMember(() -> verifySenderDoesNotExist("ln", false), server1, server2,
        server3);
  }

  /**
   * GatewaySender with given attribute values and transport filters.
   */
  @Test
  public void testCreateDestroyGatewaySenderWithGatewayTransportFilters() throws Exception {
    int socketReadTimeout = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 1000;
    String command = CliStrings.CREATE_GATEWAYSENDER + " --" + CliStrings.CREATE_GATEWAYSENDER__ID
        + "=ln" + " --" + CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__PARALLEL + "=false" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__MANUALSTART + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT + "=" + socketReadTimeout + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__BATCHSIZE + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL + "=5000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ENABLEPERSISTENCE + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__DISKSYNCHRONOUS + "=false" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__MAXQUEUEMEMORY + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ALERTTHRESHOLD + "=100" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__DISPATCHERTHREADS + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ORDERPOLICY + "=THREAD" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__GATEWAYTRANSPORTFILTER
        + "=org.apache.geode.cache30.MyGatewayTransportFilter1";
    gfsh.executeAndAssertThat(command).statusIsSuccess().tableHasColumnWithExactValuesInAnyOrder(
        "Message", "GatewaySender \"ln\" created on \"" + SERVER_3 + "\"",
        "GatewaySender \"ln\" created on \"" + SERVER_4 + "\"",
        "GatewaySender \"ln\" created on \"" + SERVER_5 + "\"");

    List<String> transportFilters = new ArrayList<>();
    transportFilters.add("org.apache.geode.cache30.MyGatewayTransportFilter1");

    VMProvider.invokeInEveryMember(() -> {
      verifySenderState("ln", false, false);
      verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true, 1000, 5000, true,
          false, 1000, 100, 2, GatewaySender.OrderPolicy.THREAD, null, transportFilters);
    }, server1, server2, server3);

    // destroy gateway sender and verify AEQs cleaned up
    gfsh.executeAndAssertThat(DESTROY).statusIsSuccess().tableHasColumnWithExactValuesInAnyOrder(
        "Message", "GatewaySender \"ln\" destroyed on \"" + SERVER_3 + "\"",
        "GatewaySender \"ln\" destroyed on \"" + SERVER_4 + "\"",
        "GatewaySender \"ln\" destroyed on \"" + SERVER_5 + "\"");

    VMProvider.invokeInEveryMember(() -> verifySenderDoesNotExist("ln", false), server1, server2,
        server3);
  }

  /**
   * GatewaySender with given attribute values on given member.
   */
  @Test
  public void testCreateDestroyGatewaySender_OnMember() throws Exception {
    gfsh.executeAndAssertThat(CREATE + " --member=" + server1.getName()).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Message",
            "GatewaySender \"ln\" created on \"" + SERVER_3 + "\"");

    server1.invoke(() -> {
      verifySenderState("ln", true, false);
    });

    VMProvider.invokeInEveryMember(() -> verifySenderDoesNotExist("ln", false), server2, server3);

    gfsh.executeAndAssertThat(DESTROY + " --member=" + server1.getName()).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Message",
            "GatewaySender \"ln\" destroyed on \"" + SERVER_3 + "\"");

    VMProvider.invokeInEveryMember(() -> verifySenderDoesNotExist("ln", false), server1);
  }

  /**
   * GatewaySender with given attribute values on given group
   */
  @Test
  public void testCreateDestroyGatewaySender_Group() throws Exception {
    gfsh.executeAndAssertThat(CREATE + " --group=senderGroup1").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Message",
            "GatewaySender \"ln\" created on \"" + SERVER_3 + "\"");

    server1.invoke(() -> {
      verifySenderState("ln", true, false);
    });

    VMProvider.invokeInEveryMember(() -> verifySenderDoesNotExist("ln", false), server2, server3);

    gfsh.executeAndAssertThat(DESTROY + " --group=senderGroup1").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Message",
            "GatewaySender \"ln\" destroyed on \"" + SERVER_3 + "\"");

    VMProvider.invokeInEveryMember(() -> verifySenderDoesNotExist("ln", false), server1);
  }

  /**
   * Parallel GatewaySender with given attribute values +
   */
  @Test
  public void testCreateDestroyParallelGatewaySender() throws Exception {
    gfsh.executeAndAssertThat(CREATE + " --parallel").statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Message",
            "GatewaySender \"ln\" created on \"" + SERVER_3 + "\"",
            "GatewaySender \"ln\" created on \"" + SERVER_4 + "\"",
            "GatewaySender \"ln\" created on \"" + SERVER_5 + "\"");

    // destroy gateway sender and verify AEQs cleaned up
    gfsh.executeAndAssertThat(DESTROY).statusIsSuccess().tableHasColumnWithExactValuesInAnyOrder(
        "Message", "GatewaySender \"ln\" destroyed on \"" + SERVER_3 + "\"",
        "GatewaySender \"ln\" destroyed on \"" + SERVER_4 + "\"",
        "GatewaySender \"ln\" destroyed on \"" + SERVER_5 + "\"");

    VMProvider.invokeInEveryMember(() -> verifySenderDoesNotExist("ln", false), server1, server2,
        server3);
  }
}
