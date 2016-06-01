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
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Properties;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static com.gemstone.gemfire.test.dunit.Assert.*;
import static com.gemstone.gemfire.test.dunit.LogWriterUtils.getLogWriter;
import static com.gemstone.gemfire.test.dunit.Wait.pause;

@Category(DistributedTest.class)
public class WanCommandGatewaySenderStartDUnitTest extends WANCommandTestBase {

  private static final long serialVersionUID = 1L;

  /**
   * Test wan commands for error in input 1> start gateway-sender command needs
   * only one of member or group.
   */
  @Test
  public void testStartGatewaySender_ErrorConditions() {

    Integer punePort = (Integer) vm1.invoke(() -> createFirstLocatorWithDSId( 1 ));

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "1");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);
    
    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator( 2, punePort ));

    vm3.invoke(() -> createCache( punePort ));
    vm3.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));

    final DistributedMember vm1Member = (DistributedMember) vm3.invoke(() -> getMember());

    String command = CliStrings.START_GATEWAYSENDER + " --"
        + CliStrings.START_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.START_GATEWAYSENDER__MEMBER + "=" + vm1Member.getId() + " --"
        + CliStrings.START_GATEWAYSENDER__GROUP + "=SenserGroup1";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testStartGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.ERROR, cmdResult.getStatus());
      assertTrue(strCmdResult.contains(CliStrings.PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE));
    } else {
      fail("testStartGatewaySender failed as did not get CommandResult");
    }
  }

  private CommandResult executeCommandWithIgnoredExceptions(String command) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
    try {
      CommandResult commandResult = executeCommand(command);
      return commandResult;
    } finally {
      exln.remove();
    }
  }

  @Test
  public void testStartGatewaySender() {

    Integer punePort = (Integer) vm1.invoke(() -> createFirstLocatorWithDSId( 1 ));

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "1");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator( 2, punePort ));

    vm3.invoke(() -> createCache( punePort ));
    vm3.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));
    vm4.invoke(() -> createCache( punePort ));
    vm4.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));
    vm5.invoke(() -> createCache( punePort ));
    vm5.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));

    vm3.invoke(() -> verifySenderState(
        "ln", false, false ));
    vm4.invoke(() -> verifySenderState(
        "ln", false, false ));
    vm5.invoke(() -> verifySenderState(
        "ln", false, false ));

    pause(10000);
    String command = CliStrings.START_GATEWAYSENDER + " --"
        + CliStrings.START_GATEWAYSENDER__ID + "=ln";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testStartGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(5, status.size());
      assertTrue(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testStartGatewaySender failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm4.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm5.invoke(() -> verifySenderState(
        "ln", true, false ));
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on
   * a member
   */
  @Test
  public void testStartGatewaySender_onMember() {

    Integer punePort = (Integer) vm1.invoke(() -> createFirstLocatorWithDSId( 1 ));

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "1");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator( 2, punePort ));

    vm3.invoke(() -> createCache( punePort ));
    vm3.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));

    vm3.invoke(() -> verifySenderState(
        "ln", false, false ));

    final DistributedMember vm1Member = (DistributedMember) vm3.invoke(() -> getMember());
    pause(10000);
    String command = CliStrings.START_GATEWAYSENDER + " --"
        + CliStrings.START_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.START_GATEWAYSENDER__MEMBER + "=" + vm1Member.getId();
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testStartGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      assertTrue(strCmdResult.contains("is started on member"));
    } else {
      fail("testStartGatewaySender failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifySenderState(
        "ln", true, false ));
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on
   * a group of members
   */
  @Test
  public void testStartGatewaySender_Group() {

    Integer punePort = (Integer) vm1.invoke(() -> createFirstLocatorWithDSId( 1 ));

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "1");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);
    
    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator( 2, punePort ));

    vm3.invoke(() -> createCacheWithGroups(
        punePort, "SenderGroup1" ));
    vm3.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));
    vm4.invoke(() -> createCacheWithGroups(
        punePort, "SenderGroup1" ));
    vm4.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));
    vm5.invoke(() -> createCacheWithGroups(
        punePort, "SenderGroup1" ));
    vm5.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));

    vm3.invoke(() -> verifySenderState(
        "ln", false, false ));
    vm4.invoke(() -> verifySenderState(
        "ln", false, false ));
    vm5.invoke(() -> verifySenderState(
        "ln", false, false ));

    pause(10000);
    String command = CliStrings.START_GATEWAYSENDER + " --"
        + CliStrings.START_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.START_GATEWAYSENDER__GROUP + "=SenderGroup1";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testStartGatewaySender_Group stringResult : " + strCmdResult
              + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(3, status.size());
      assertFalse(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testStartGatewaySender failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm4.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm5.invoke(() -> verifySenderState(
        "ln", true, false ));
  }

  /**
   * Test to validate the scenario gateway sender is started when one or more
   * sender members belongs to multiple groups
   */
  @Test
  public void testStartGatewaySender_MultipleGroup() {

    Integer punePort = (Integer) vm1.invoke(() -> createFirstLocatorWithDSId( 1 ));

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "1");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator( 2, punePort ));

    vm3.invoke(() -> createCacheWithGroups(
        punePort, "SenderGroup1" ));
    vm3.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));
    vm4.invoke(() -> createCacheWithGroups(
        punePort, "SenderGroup1" ));
    vm4.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));
    vm5.invoke(() -> createCacheWithGroups(
        punePort, "SenderGroup1, SenderGroup2" ));
    vm5.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));
    vm6.invoke(() -> createCacheWithGroups(
        punePort, "SenderGroup1, SenderGroup2" ));
    vm6.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));
    vm7.invoke(() -> createCacheWithGroups(
        punePort, "SenderGroup3" ));
    vm7.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));

    vm3.invoke(() -> verifySenderState(
        "ln", false, false ));
    vm4.invoke(() -> verifySenderState(
        "ln", false, false ));
    vm5.invoke(() -> verifySenderState(
        "ln", false, false ));
    vm6.invoke(() -> verifySenderState(
        "ln", false, false ));
    vm7.invoke(() -> verifySenderState(
        "ln", false, false ));

    pause(10000);
    String command = CliStrings.START_GATEWAYSENDER + " --"
        + CliStrings.START_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.START_GATEWAYSENDER__GROUP + "=SenderGroup1,SenderGroup2";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testStartGatewaySender_Group stringResult : " + strCmdResult
              + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(4, status.size());
      assertFalse(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testStartGatewaySender failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm4.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm5.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm6.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm7.invoke(() -> verifySenderState(
        "ln", false, false ));
  }

  /**
   * Test to validate the test scenario when one of the member ion group does
   * not have the sender.
   */
  @Test
  public void testStartGatewaySender_Group_MissingSenderFromGroup() {

    Integer punePort = (Integer) vm1.invoke(() -> createFirstLocatorWithDSId( 1 ));

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "1");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator( 2, punePort ));

    vm3.invoke(() -> createCacheWithGroups(
        punePort, "SenderGroup1" ));
    vm3.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));
    vm4.invoke(() -> createCacheWithGroups(
        punePort, "SenderGroup1" ));
    vm5.invoke(() -> createCacheWithGroups(
        punePort, "SenderGroup1" ));
    vm5.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));

    vm3.invoke(() -> verifySenderState(
        "ln", false, false ));
    vm5.invoke(() -> verifySenderState(
        "ln", false, false ));

    pause(10000);
    String command = CliStrings.START_GATEWAYSENDER + " --"
        + CliStrings.START_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.START_GATEWAYSENDER__GROUP + "=SenderGroup1";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      assertTrue(strCmdResult.contains("Error"));
      assertTrue(strCmdResult.contains("is not available"));
      getLogWriter().info(
          "testStartGatewaySender_Group stringResult : " + strCmdResult
              + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(3, status.size());
      assertTrue(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testStartGatewaySender failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm5.invoke(() -> verifySenderState(
        "ln", true, false ));
  }

}
