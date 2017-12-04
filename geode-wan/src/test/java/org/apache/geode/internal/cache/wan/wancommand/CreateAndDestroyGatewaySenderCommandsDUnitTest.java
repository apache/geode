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
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.getMemberIdCallable;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifySenderAttributes;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifySenderDestroyed;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifySenderState;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class CreateAndDestroyGatewaySenderCommandsDUnitTest {

  @Rule
  public LocatorServerStartupRule locatorServerStartupRule = new LocatorServerStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM locatorSite1;
  private MemberVM locatorSite2;
  private MemberVM server1;
  private MemberVM server2;
  private MemberVM server3;

  @Before
  public void before() throws Exception {
    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    locatorSite1 = locatorServerStartupRule.startLocatorVM(1, props);

    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 2);
    props.setProperty(REMOTE_LOCATORS, "localhost[" + locatorSite1.getPort() + "]");
    locatorSite2 = locatorServerStartupRule.startLocatorVM(2, props);

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locatorSite1);

  }

  /**
   * GatewaySender with all default attributes
   */
  @Test
  public void testCreateDestroyGatewaySenderWithDefault() throws Exception {

    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = locatorServerStartupRule.startServerVM(3, locator1Port);
    server2 = locatorServerStartupRule.startServerVM(4, locator1Port);
    server3 = locatorServerStartupRule.startServerVM(5, locator1Port);

    // create gateway senders to Site #2
    String command = CliStrings.CREATE_GATEWAYSENDER + " --" + CliStrings.CREATE_GATEWAYSENDER__ID
        + "=ln" + " --" + CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID + "=2";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info(
          "testCreateDestroyGatewaySenderWithDefault stringResult : " + strCmdResult + ">>>>");

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(3, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateDestroyGatewaySenderWithDefault failed as did not get CommandResult");
    }

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));

    // destroy gateway sender and verify AEQs cleaned up
    doDestroyAndVerifyGatewaySender("ln", null, null, "testCreateDestroyGatewaySenderWithDefault",
        Arrays.asList(server1, server2, server3), 3, false);
  }

  /**
   * + * GatewaySender with given attribute values +
   */
  @Test
  public void testCreateDestroyGatewaySender() throws Exception {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = locatorServerStartupRule.startServerVM(3, locator1Port);
    server2 = locatorServerStartupRule.startServerVM(4, locator1Port);
    server3 = locatorServerStartupRule.startServerVM(5, locator1Port);

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
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testCreateDestroyGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(3, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateDestroyGatewaySender failed as did not get CommandResult");
    }
    server1.invoke(() -> verifySenderState("ln", false, false));
    server2.invoke(() -> verifySenderState("ln", false, false));
    server3.invoke(() -> verifySenderState("ln", false, false));

    server1.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, GatewaySender.OrderPolicy.THREAD, null, null));
    server2.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, GatewaySender.OrderPolicy.THREAD, null, null));
    server3.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, GatewaySender.OrderPolicy.THREAD, null, null));

    doDestroyAndVerifyGatewaySender("ln", null, null, "testCreateDestroyGatewaySender",
        Arrays.asList(server1, server2, server3), 3, false);
  }

  /**
   * GatewaySender with given attribute values and event filters.
   */
  @Test
  public void testCreateDestroyGatewaySenderWithGatewayEventFilters() throws Exception {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = locatorServerStartupRule.startServerVM(3, locator1Port);
    server2 = locatorServerStartupRule.startServerVM(4, locator1Port);
    server3 = locatorServerStartupRule.startServerVM(5, locator1Port);

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
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testCreateDestroyGatewaySenderWithGatewayEventFilters stringResult : "
          + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(3, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail(
          "testCreateDestroyGatewaySenderWithGatewayEventFilters failed as did not get CommandResult");
    }

    server1.invoke(() -> verifySenderState("ln", false, false));
    server2.invoke(() -> verifySenderState("ln", false, false));
    server3.invoke(() -> verifySenderState("ln", false, false));

    List<String> eventFilters = new ArrayList<String>();
    eventFilters.add("org.apache.geode.cache30.MyGatewayEventFilter1");
    eventFilters.add("org.apache.geode.cache30.MyGatewayEventFilter2");

    server1.invoke(
        () -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true, 1000,
            5000, true, false, 1000, 100, 2, GatewaySender.OrderPolicy.THREAD, eventFilters, null));
    server2.invoke(
        () -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true, 1000,
            5000, true, false, 1000, 100, 2, GatewaySender.OrderPolicy.THREAD, eventFilters, null));
    server3.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,

        1000, 5000, true, false, 1000, 100, 2, GatewaySender.OrderPolicy.THREAD, eventFilters,
        null));

    doDestroyAndVerifyGatewaySender("ln", null, null,
        "testCreateDestroyGatewaySenderWithGatewayEventFilters",
        Arrays.asList(server1, server2, server3), 3, false);
  }

  /**
   * GatewaySender with given attribute values and transport filters.
   */
  @Test
  public void testCreateDestroyGatewaySenderWithGatewayTransportFilters() throws Exception {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = locatorServerStartupRule.startServerVM(3, locator1Port);
    server2 = locatorServerStartupRule.startServerVM(4, locator1Port);
    server3 = locatorServerStartupRule.startServerVM(5, locator1Port);

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
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter()
          .info("testCreateDestroyGatewaySenderWithGatewayTransportFilters stringResult : "
              + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(3, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail(
          "testCreateDestroyGatewaySenderWithGatewayTransportFilters failed as did not get CommandResult");
    }

    server1.invoke(() -> verifySenderState("ln", false, false));
    server2.invoke(() -> verifySenderState("ln", false, false));
    server2.invoke(() -> verifySenderState("ln", false, false));

    List<String> transportFilters = new ArrayList<String>();
    transportFilters.add("org.apache.geode.cache30.MyGatewayTransportFilter1");
    server1.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, GatewaySender.OrderPolicy.THREAD, null,
        transportFilters));
    server2.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, GatewaySender.OrderPolicy.THREAD, null,
        transportFilters));
    server3.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, GatewaySender.OrderPolicy.THREAD, null,
        transportFilters));

    doDestroyAndVerifyGatewaySender("ln", null, null,
        "testCreateDestroyGatewaySenderWithGatewayTransportFilters",
        Arrays.asList(server1, server2, server3), 3, false);
  }

  /**
   * GatewaySender with given attribute values on given member.
   */
  @Test
  public void testCreateDestroyGatewaySender_OnMember() throws Exception {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = locatorServerStartupRule.startServerVM(3, locator1Port);
    server2 = locatorServerStartupRule.startServerVM(4, locator1Port);
    server3 = locatorServerStartupRule.startServerVM(5, locator1Port);

    final DistributedMember server1DM = (DistributedMember) server1.invoke(getMemberIdCallable());
    int socketReadTimeout = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 1000;
    String command = CliStrings.CREATE_GATEWAYSENDER + " --" + CliStrings.CREATE_GATEWAYSENDER__ID
        + "=ln" + " --" + CliStrings.MEMBER + "=" + server1DM.getId() + " --"
        + CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID + "=2" + " --"
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
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter()
          .info("testCreateDestroyGatewaySender_OnMember stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(1, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateDestroyGatewaySender_OnMember failed as did not get CommandResult");
    }

    server1.invoke(() -> verifySenderState("ln", false, false));
    server1.invoke(() -> verifySenderAttributes("ln", 2, false, true, 1000, socketReadTimeout, true,
        1000, 5000, true, false, 1000, 100, 2, GatewaySender.OrderPolicy.THREAD, null, null));

    doDestroyAndVerifyGatewaySender("ln", null, server1DM.getId(),
        "testCreateDestroyGatewaySender_OnMember", Arrays.asList(server1), 1, false);
  }

  /**
   * GatewaySender with given attribute values on given group
   */
  @Test
  public void testCreateDestroyGatewaySender_Group() throws Exception {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = startServerWithGroups(3, "SenderGroup1", locator1Port);
    server2 = startServerWithGroups(4, "SenderGroup1", locator1Port);
    server3 = startServerWithGroups(5, "SenderGroup1", locator1Port);

    int socketReadTimeout = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 1000;
    String command = CliStrings.CREATE_GATEWAYSENDER + " --" + CliStrings.CREATE_GATEWAYSENDER__ID
        + "=ln" + " --" + CliStrings.GROUP + "=SenderGroup1" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__PARALLEL + "=false" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__MANUALSTART + "=false" + " --"
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
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter()
          .info("testCreateDestroyGatewaySender_Group stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(3, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateDestroyGatewaySender_Group failed as did not get CommandResult");
    }

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));
    server3.invoke(() -> verifySenderState("ln", true, false));

    doDestroyAndVerifyGatewaySender("ln", "SenderGroup1", null,
        "testCreateDestroyGatewaySender_Group", Arrays.asList(server1, server2, server3), 3, false);
  }

  /**
   * GatewaySender with given attribute values on given group. Only 2 of 3 members are part of the
   * group.
   */
  @Test
  public void testCreateDestroyGatewaySender_Group_Scenario2() throws Exception {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = startServerWithGroups(3, "SenderGroup1", locator1Port);
    server2 = startServerWithGroups(4, "SenderGroup1", locator1Port);
    server3 = startServerWithGroups(5, "SenderGroup2", locator1Port);

    int socketReadTimeout = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 1000;
    String command = CliStrings.CREATE_GATEWAYSENDER + " --" + CliStrings.CREATE_GATEWAYSENDER__ID
        + "=ln" + " --" + CliStrings.GROUP + "=SenderGroup1" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__PARALLEL + "=false" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__MANUALSTART + "=false" + " --"
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
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info(
          "testCreateDestroyGatewaySender_Group_Scenario2 stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(2, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateDestroyGatewaySender_Group_Scenario2 failed as did not get CommandResult");
    }

    server1.invoke(() -> verifySenderState("ln", true, false));
    server2.invoke(() -> verifySenderState("ln", true, false));

    doDestroyAndVerifyGatewaySender("ln", "SenderGroup1", null,
        "testCreateDestroyGatewaySender_Group_Scenario2", Arrays.asList(server1, server2), 2,
        false);
  }

  /**
   * + * Parallel GatewaySender with given attribute values +
   */
  @Test
  public void testCreateDestroyParallelGatewaySender() throws Exception {
    Integer locator1Port = locatorSite1.getPort();

    // setup servers in Site #1
    server1 = locatorServerStartupRule.startServerVM(3, locator1Port);
    server2 = locatorServerStartupRule.startServerVM(4, locator1Port);
    server3 = locatorServerStartupRule.startServerVM(5, locator1Port);

    int socketReadTimeout = GatewaySender.MINIMUM_SOCKET_READ_TIMEOUT + 1000;
    String command = CliStrings.CREATE_GATEWAYSENDER + " --" + CliStrings.CREATE_GATEWAYSENDER__ID
        + "=ln" + " --" + CliStrings.CREATE_GATEWAYSENDER__REMOTEDISTRIBUTEDSYSTEMID + "=2" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__PARALLEL + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__MANUALSTART + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__SOCKETBUFFERSIZE + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__SOCKETREADTIMEOUT + "=" + socketReadTimeout + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ENABLEBATCHCONFLATION + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__BATCHSIZE + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__BATCHTIMEINTERVAL + "=5000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ENABLEPERSISTENCE + "=true" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__DISKSYNCHRONOUS + "=false" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__MAXQUEUEMEMORY + "=1000" + " --"
        + CliStrings.CREATE_GATEWAYSENDER__ALERTTHRESHOLD + "=100";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter()
          .info("testCreateDestroyParallelGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(3, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateDestroyParallelGatewaySender failed as did not get CommandResult");
    }

    server1.invoke(() -> verifySenderState("ln", false, false));
    server2.invoke(() -> verifySenderState("ln", false, false));
    server3.invoke(() -> verifySenderState("ln", false, false));

    server1.invoke(
        () -> verifySenderAttributes("ln", 2, true, true, 1000, socketReadTimeout, true, 1000, 5000,
            true, false, 1000, 100, GatewaySender.DEFAULT_DISPATCHER_THREADS, null, null, null));
    server2.invoke(
        () -> verifySenderAttributes("ln", 2, true, true, 1000, socketReadTimeout, true, 1000, 5000,
            true, false, 1000, 100, GatewaySender.DEFAULT_DISPATCHER_THREADS, null, null, null));
    server3.invoke(
        () -> verifySenderAttributes("ln", 2, true, true, 1000, socketReadTimeout, true, 1000, 5000,
            true, false, 1000, 100, GatewaySender.DEFAULT_DISPATCHER_THREADS, null, null, null));

    doDestroyAndVerifyGatewaySender("ln", null, null, "testCreateDestroyParallelGatewaySender",
        Arrays.asList(server1, server2), 3, true);
  }

  /**
   * doDestroyAndVerifyGatewaySender helper command.
   *
   * @param id id of the Gateway Sender
   * @param group Group for the GatewaySender
   * @param DMId String representing DistributedMember id
   * @param testName testName for the logging
   * @param vms list of vms where to verify the destroyed gateway sender
   * @param size command result.
   * @param isParallel true if parallel , false otherwise.
   */
  private void doDestroyAndVerifyGatewaySender(final String id, final String group,
      final String DMId, final String testName, final List<MemberVM> vms, final int size,
      final boolean isParallel) throws Exception {
    String command =
        CliStrings.DESTROY_GATEWAYSENDER + " --" + CliStrings.DESTROY_GATEWAYSENDER__ID + "=" + id;
    if (group != null) {
      command += " --" + CliStrings.GROUP + "=" + group;
    }
    if (DMId != null) {
      command += " --" + CliStrings.MEMBER + "=" + DMId;
    }
    final CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info(testName + " stringResult : " + strCmdResult + ">>>>");

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(size, status.size());
      for (String stat : status) {
        assertTrue("GatewaySender destroy failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail(testName + " failed as did not get CommandResult");
    }
    for (MemberVM vm : vms) {
      vm.invoke(() -> verifySenderDestroyed(id, isParallel));
    }
  }

  private CommandResult executeCommandWithIgnoredExceptions(String command) throws Exception {
    final IgnoredException ignored = IgnoredException.addIgnoredException("Could not connect");
    try {
      return gfsh.executeCommand(command);
    } finally {
      ignored.remove();
    }
  }

  private MemberVM startServerWithGroups(int index, String groups, int locPort) throws Exception {
    Properties props = new Properties();
    props.setProperty(GROUPS, groups);
    return locatorServerStartupRule.startServerVM(index, props, locPort);
  }
}
