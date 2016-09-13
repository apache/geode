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

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.LOCATORS;
import static com.gemstone.gemfire.distributed.ConfigurationProperties.MCAST_PORT;
import static com.gemstone.gemfire.test.dunit.Assert.*;
import static com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter;
import static com.gemstone.gemfire.test.dunit.Wait.pause;

@Category(DistributedTest.class)
public class WanCommandGatewayReceiverStopDUnitTest extends WANCommandTestBase {

  private static final long serialVersionUID = 1L;

  /**
   * Test wan commands for error in input 1> start gateway-sender command needs
   * only one of member or group.
   */
  @Test
  public void testStopGatewayReceiver_ErrorConditions() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(() -> getLocatorPort());

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator( 2, punePort ));

    vm3.invoke(() -> createReceiver( punePort ));

    final DistributedMember vm1Member = (DistributedMember) vm3.invoke(() -> getMember());

    String command = CliStrings.STOP_GATEWAYRECEIVER + " --"
        + CliStrings.STOP_GATEWAYRECEIVER__MEMBER + "=" + vm1Member.getId()
        + " --" + CliStrings.STOP_GATEWAYRECEIVER__GROUP + "=RG1";

    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testStopGatewayReceiver_ErrorConditions stringResult : "
              + strCmdResult + ">>>>");
      assertEquals(Result.Status.ERROR, cmdResult.getStatus());
      assertTrue(strCmdResult.contains(CliStrings.PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE));
    } else {
      fail("testStopGatewayReceiver_ErrorConditions failed as did not get CommandResult");
    }
  }

  @Test
  public void testStopGatewayReceiver() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(() -> getLocatorPort());

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator( 2, punePort ));

    vm3.invoke(() -> createAndStartReceiver( punePort ));
    vm4.invoke(() -> createAndStartReceiver( punePort ));
    vm5.invoke(() -> createAndStartReceiver( punePort ));

    vm3.invoke(() -> verifyReceiverState( true ));
    vm4.invoke(() -> verifyReceiverState( true ));
    vm5.invoke(() -> verifyReceiverState( true ));

    pause(10000);
    String command = CliStrings.STOP_GATEWAYRECEIVER;
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
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

    vm3.invoke(() -> verifyReceiverState( false ));
    vm4.invoke(() -> verifyReceiverState( false ));
    vm5.invoke(() -> verifyReceiverState( false ));
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on
   * a member
   */
  @Test
  public void testStopGatewayReceiver_onMember() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(() -> getLocatorPort());

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator( 2, punePort ));

    vm3.invoke(() -> createAndStartReceiver( punePort ));
    vm4.invoke(() -> createAndStartReceiver( punePort ));
    vm5.invoke(() -> createAndStartReceiver( punePort ));

    vm3.invoke(() -> verifyReceiverState( true ));
    vm4.invoke(() -> verifyReceiverState( true ));
    vm5.invoke(() -> verifyReceiverState( true ));

    final DistributedMember vm1Member = (DistributedMember) vm3.invoke(() -> getMember());
    pause(10000);
    String command = CliStrings.STOP_GATEWAYRECEIVER + " --"
        + CliStrings.STOP_GATEWAYRECEIVER__MEMBER + "=" + vm1Member.getId();

    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testStopGatewayReceiver_onMember stringResult : " + strCmdResult
              + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      assertTrue(strCmdResult.contains("stopped on member"));
    } else {
      fail("testStopGatewayReceiver failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifyReceiverState( false ));
    vm4.invoke(() -> verifyReceiverState( true ));
    vm5.invoke(() -> verifyReceiverState( true ));
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on
   * a group of members
   */
  @Test
  public void testStopGatewayReceiver_Group() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(() -> getLocatorPort());

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator( 2, punePort ));

    vm3.invoke(() -> createAndStartReceiverWithGroup( punePort, "RG1" ));
    vm4.invoke(() -> createAndStartReceiverWithGroup( punePort, "RG1" ));
    vm5.invoke(() -> createAndStartReceiverWithGroup( punePort, "RG1" ));

    vm3.invoke(() -> verifyReceiverState( true ));
    vm4.invoke(() -> verifyReceiverState( true ));
    vm5.invoke(() -> verifyReceiverState( true ));

    pause(10000);
    String command = CliStrings.STOP_GATEWAYRECEIVER + " --"
        + CliStrings.STOP_GATEWAYRECEIVER__GROUP + "=RG1";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
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

    vm3.invoke(() -> verifyReceiverState( false ));
    vm4.invoke(() -> verifyReceiverState( false ));
    vm5.invoke(() -> verifyReceiverState( false ));
  }

  /**
   * Test to validate the scenario gateway sender is started when one or more
   * sender members belongs to multiple groups
   * 
   */
  @Test
  public void testStopGatewayReceiver_MultipleGroup() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(() -> getLocatorPort());

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator( 2, punePort ));

    vm3.invoke(() -> createAndStartReceiverWithGroup( punePort, "RG1" ));
    vm4.invoke(() -> createAndStartReceiverWithGroup( punePort, "RG1" ));
    vm5.invoke(() -> createAndStartReceiverWithGroup( punePort, "RG1" ));
    vm6.invoke(() -> createAndStartReceiverWithGroup( punePort, "RG1, RG2" ));
    vm7.invoke(() -> createAndStartReceiverWithGroup( punePort, "RG3" ));

    vm3.invoke(() -> verifyReceiverState( true ));
    vm4.invoke(() -> verifyReceiverState( true ));
    vm5.invoke(() -> verifyReceiverState( true ));
    vm6.invoke(() -> verifyReceiverState( true ));
    vm7.invoke(() -> verifyReceiverState( true ));

    pause(10000);
    String command = CliStrings.STOP_GATEWAYRECEIVER + " --"
        + CliStrings.STOP_GATEWAYRECEIVER__GROUP + "=RG1,RG2";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
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

    vm3.invoke(() -> verifyReceiverState( false ));
    vm4.invoke(() -> verifyReceiverState( false ));
    vm5.invoke(() -> verifyReceiverState( false ));
    vm6.invoke(() -> verifyReceiverState( false ));
    vm7.invoke(() -> verifyReceiverState( true ));
  }
}
