/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.wan.wancommand;

import hydra.Log;

import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;

public class WanCommandGatewayReceiverStopDUnitTest extends WANCommandTestBase {

  private static final long serialVersionUID = 1L;

  public WanCommandGatewayReceiverStopDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  /**
   * Test wan commands for error in input 1> start gateway-sender command needs
   * only one of member or group.
   */
  public void testStopGatewayReceiver_ErrorConditions() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(WANCommandTestBase.class,
        "getLocatorPort");

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + punePort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, punePort });

    vm3.invoke(WANCommandTestBase.class, "createReceiver",
        new Object[] { punePort });

    final DistributedMember vm1Member = (DistributedMember) vm3.invoke(
        WANCommandTestBase.class, "getMember");

    String command = CliStrings.STOP_GATEWAYRECEIVER + " --"
        + CliStrings.STOP_GATEWAYRECEIVER__MEMBER + "=" + vm1Member.getId()
        + " --" + CliStrings.STOP_GATEWAYRECEIVER__GROUP + "=RG1";

    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testStopGatewayReceiver_ErrorConditions stringResult : "
              + strCmdResult + ">>>>");
      assertEquals(Result.Status.ERROR, cmdResult.getStatus());
      assertTrue(strCmdResult.contains(CliStrings.PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE));
    } else {
      fail("testStopGatewayReceiver_ErrorConditions failed as did not get CommandResult");
    }
  }

  public void testStopGatewayReceiver() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(WANCommandTestBase.class,
        "getLocatorPort");

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + punePort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, punePort });

    vm3.invoke(WANCommandTestBase.class, "createAndStartReceiver",
        new Object[] { punePort });
    vm4.invoke(WANCommandTestBase.class, "createAndStartReceiver",
        new Object[] { punePort });
    vm5.invoke(WANCommandTestBase.class, "createAndStartReceiver",
        new Object[] { punePort });

    vm3.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });
    vm4.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });
    vm5.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });

    Wait.pause(10000);
    String command = CliStrings.STOP_GATEWAYRECEIVER;
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testStopGatewayReceiver stringResult : " + strCmdResult + ">>>>");

      TabularResultData resultData = (TabularResultData) cmdResult
          .getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(4, status.size());
      assertTrue(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testStopGatewayReceiver failed as did not get CommandResult");
    }

    vm3.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { false });
    vm4.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { false });
    vm5.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { false });
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on
   * a member
   */
  public void testStopGatewayReceiver_onMember() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(WANCommandTestBase.class,
        "getLocatorPort");

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + punePort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, punePort });

    vm3.invoke(WANCommandTestBase.class, "createAndStartReceiver",
        new Object[] { punePort });
    vm4.invoke(WANCommandTestBase.class, "createAndStartReceiver",
        new Object[] { punePort });
    vm5.invoke(WANCommandTestBase.class, "createAndStartReceiver",
        new Object[] { punePort });

    vm3.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });
    vm4.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });
    vm5.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });

    final DistributedMember vm1Member = (DistributedMember) vm3.invoke(
        WANCommandTestBase.class, "getMember");
    Wait.pause(10000);
    String command = CliStrings.STOP_GATEWAYRECEIVER + " --"
        + CliStrings.STOP_GATEWAYRECEIVER__MEMBER + "=" + vm1Member.getId();

    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testStopGatewayReceiver_onMember stringResult : " + strCmdResult
              + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      assertTrue(strCmdResult.contains("stopped on member"));
    } else {
      fail("testStopGatewayReceiver failed as did not get CommandResult");
    }

    vm3.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { false });
    vm4.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });
    vm5.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on
   * a group of members
   */
  public void testStopGatewayReceiver_Group() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(WANCommandTestBase.class,
        "getLocatorPort");

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + punePort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, punePort });

    vm3.invoke(WANCommandTestBase.class, "createAndStartReceiverWithGroup",
        new Object[] { punePort, "RG1" });
    vm4.invoke(WANCommandTestBase.class, "createAndStartReceiverWithGroup",
        new Object[] { punePort, "RG1" });
    vm5.invoke(WANCommandTestBase.class, "createAndStartReceiverWithGroup",
        new Object[] { punePort, "RG1" });

    vm3.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });
    vm4.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });
    vm5.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });

    Wait.pause(10000);
    String command = CliStrings.STOP_GATEWAYRECEIVER + " --"
        + CliStrings.STOP_GATEWAYRECEIVER__GROUP + "=RG1";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testStopGatewayReceiver_Group stringResult : " + strCmdResult
              + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult
          .getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(3, status.size());
      assertFalse(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testStopGatewayReceiver_Group failed as did not get CommandResult");
    }

    vm3.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { false });
    vm4.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { false });
    vm5.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { false });
  }

  /**
   * Test to validate the scenario gateway sender is started when one or more
   * sender members belongs to multiple groups
   * 
   */
  public void testStopGatewayReceiver_MultipleGroup() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(WANCommandTestBase.class,
        "getLocatorPort");

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + punePort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, punePort });

    vm3.invoke(WANCommandTestBase.class, "createAndStartReceiverWithGroup",
        new Object[] { punePort, "RG1" });
    vm4.invoke(WANCommandTestBase.class, "createAndStartReceiverWithGroup",
        new Object[] { punePort, "RG1" });
    vm5.invoke(WANCommandTestBase.class, "createAndStartReceiverWithGroup",
        new Object[] { punePort, "RG1" });
    vm6.invoke(WANCommandTestBase.class, "createAndStartReceiverWithGroup",
        new Object[] { punePort, "RG1, RG2" });
    vm7.invoke(WANCommandTestBase.class, "createAndStartReceiverWithGroup",
        new Object[] { punePort, "RG3" });

    vm3.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });
    vm4.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });
    vm5.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });
    vm6.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });
    vm7.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });

    Wait.pause(10000);
    String command = CliStrings.STOP_GATEWAYRECEIVER + " --"
        + CliStrings.STOP_GATEWAYRECEIVER__GROUP + "=RG1,RG2";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testStopGatewayReceiver_Group stringResult : " + strCmdResult
              + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult
          .getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(4, status.size());
      assertFalse(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testStopGatewayReceiver failed as did not get CommandResult");
    }

    vm3.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { false });
    vm4.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { false });
    vm5.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { false });
    vm6.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { false });
    vm7.invoke(WANCommandTestBase.class, "verifyReceiverState",
        new Object[] { true });
  }
}
