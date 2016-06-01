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
public class WanCommandPauseResumeDUnitTest extends WANCommandTestBase {

  private static final long serialVersionUID = 1L;

  @Test
  public void testPauseGatewaySender_ErrorConditions() {

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

    String command = CliStrings.PAUSE_GATEWAYSENDER + " --"
        + CliStrings.PAUSE_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.PAUSE_GATEWAYSENDER__MEMBER + "=" + vm1Member.getId()
        + " --" + CliStrings.PAUSE_GATEWAYSENDER__GROUP + "=SenderGroup1";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testPauseGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.ERROR, cmdResult.getStatus());
      assertTrue(strCmdResult.contains(CliStrings.PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE));
    } else {
      fail("testPauseGatewaySender failed as did not get CommandResult");
    }
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on
   * a member
   */
  @Test
  public void testPauseGatewaySender_onMember() {

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

    vm3.invoke(() -> startSender( "ln" ));

    vm3.invoke(() -> verifySenderState(
        "ln", true, false ));

    final DistributedMember vm1Member = (DistributedMember) vm3.invoke(() -> getMember());
    pause(10000);
    String command = CliStrings.PAUSE_GATEWAYSENDER + " --"
        + CliStrings.PAUSE_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.PAUSE_GATEWAYSENDER__MEMBER + "=" + vm1Member.getId();
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testPauseGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      assertTrue(strCmdResult.contains("is paused on member"));
    } else {
      fail("testPauseGatewaySender failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifySenderState(
        "ln", true, true ));
  }

  @Test
  public void testPauseGatewaySender() {

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

    vm3.invoke(() -> startSender( "ln" ));
    vm4.invoke(() -> startSender( "ln" ));
    vm5.invoke(() -> startSender( "ln" ));

    vm3.invoke(() -> verifySenderState(
            "ln", true, false ));
    vm4.invoke(() -> verifySenderState(
            "ln", true, false ));
    vm5.invoke(() -> verifySenderState(
            "ln", true, false ));

    pause(10000);
    String command = CliStrings.PAUSE_GATEWAYSENDER + " --"
            + CliStrings.PAUSE_GATEWAYSENDER__ID + "=ln";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
              "testPauseGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult
              .getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(5, status.size());
      assertTrue(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testPauseGatewaySender failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifySenderState(
            "ln", true, true ));
    vm4.invoke(() -> verifySenderState(
            "ln", true, true ));
    vm5.invoke(() -> verifySenderState(
            "ln", true, true ));
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on
   * a group of members
   */
  @Test
  public void testPauseGatewaySender_Group() {

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

    vm3.invoke(() -> startSender( "ln" ));
    vm4.invoke(() -> startSender( "ln" ));
    vm5.invoke(() -> startSender( "ln" ));

    vm3.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm4.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm5.invoke(() -> verifySenderState(
        "ln", true, false ));

    pause(10000);
    String command = CliStrings.PAUSE_GATEWAYSENDER + " --"
        + CliStrings.PAUSE_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.PAUSE_GATEWAYSENDER__GROUP + "=SenderGroup1";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testPauseGatewaySender_Group stringResult : " + strCmdResult
              + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult
          .getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(3, status.size());
      assertFalse(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testPauseGatewaySender failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifySenderState(
        "ln", true, true ));
    vm4.invoke(() -> verifySenderState(
        "ln", true, true ));
    vm5.invoke(() -> verifySenderState(
        "ln", true, true ));
  }

  /**
   * Test to validate the scenario gateway sender is started when one or more
   * sender members belongs to multiple groups
   */
  @Test
  public void testPauseGatewaySender_MultipleGroup() {

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
        punePort, "SenderGroup2" ));
    vm6.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));
    vm7.invoke(() -> createCacheWithGroups(
        punePort, "SenderGroup3" ));
    vm7.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));

    vm3.invoke(() -> startSender( "ln" ));
    vm4.invoke(() -> startSender( "ln" ));
    vm5.invoke(() -> startSender( "ln" ));
    vm6.invoke(() -> startSender( "ln" ));
    vm7.invoke(() -> startSender( "ln" ));

    vm3.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm4.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm5.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm6.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm7.invoke(() -> verifySenderState(
        "ln", true, false ));

    pause(10000);
    String command = CliStrings.PAUSE_GATEWAYSENDER + " --"
        + CliStrings.PAUSE_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.PAUSE_GATEWAYSENDER__GROUP + "=SenderGroup1,SenderGroup2";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testPauseGatewaySender_Group stringResult : " + strCmdResult
              + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult
          .getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(4, status.size());
      assertFalse(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testPauseGatewaySender failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifySenderState(
        "ln", true, true ));
    vm4.invoke(() -> verifySenderState(
        "ln", true, true ));
    vm5.invoke(() -> verifySenderState(
        "ln", true, true ));
    vm6.invoke(() -> verifySenderState(
        "ln", true, true ));
    vm7.invoke(() -> verifySenderState(
        "ln", true, false ));
  }

  @Test
  public void testResumeGatewaySender_ErrorConditions() {

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

    String command = CliStrings.RESUME_GATEWAYSENDER + " --"
        + CliStrings.RESUME_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.RESUME_GATEWAYSENDER__MEMBER + "=" + vm1Member.getId()
        + " --" + CliStrings.RESUME_GATEWAYSENDER__GROUP + "=SenderGroup1";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testResumeGatewaySender_ErrorConditions stringResult : "
              + strCmdResult + ">>>>");
      assertEquals(Result.Status.ERROR, cmdResult.getStatus());
      assertTrue(strCmdResult.contains(CliStrings.PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE));
    } else {
      fail("testPauseGatewaySender failed as did not get CommandResult");
    }
  }

  @Test
  public void testResumeGatewaySender() {

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

    vm3.invoke(() -> startSender( "ln" ));
    vm4.invoke(() -> startSender( "ln" ));
    vm5.invoke(() -> startSender( "ln" ));

    vm3.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm4.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm5.invoke(() -> verifySenderState(
        "ln", true, false ));

    vm3.invoke(() -> pauseSender( "ln" ));
    vm4.invoke(() -> pauseSender( "ln" ));
    vm5.invoke(() -> pauseSender( "ln" ));

    vm3.invoke(() -> verifySenderState(
        "ln", true, true ));
    vm4.invoke(() -> verifySenderState(
        "ln", true, true ));
    vm5.invoke(() -> verifySenderState(
        "ln", true, true ));

    pause(10000);
    String command = CliStrings.RESUME_GATEWAYSENDER + " --"
        + CliStrings.RESUME_GATEWAYSENDER__ID + "=ln";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testResumeGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult
          .getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(5, status.size());
      assertTrue(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testResumeGatewaySender failed as did not get CommandResult");
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
  public void testResumeGatewaySender_onMember() {

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

    vm3.invoke(() -> startSender( "ln" ));

    vm3.invoke(() -> verifySenderState(
        "ln", true, false ));

    vm3.invoke(() -> pauseSender( "ln" ));

    vm3.invoke(() -> verifySenderState(
        "ln", true, true ));

    final DistributedMember vm1Member = (DistributedMember) vm3.invoke(() -> getMember());
    pause(10000);
    String command = CliStrings.RESUME_GATEWAYSENDER + " --"
        + CliStrings.RESUME_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.RESUME_GATEWAYSENDER__MEMBER + "=" + vm1Member.getId();
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testResumeGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      assertTrue(strCmdResult.contains("is resumed on member"));
    } else {
      fail("testResumeGatewaySender failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifySenderState(
        "ln", true, false ));
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on
   * a group of members
   */
  @Test
  public void testResumeGatewaySender_Group() {

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

    vm3.invoke(() -> startSender( "ln" ));
    vm4.invoke(() -> startSender( "ln" ));
    vm5.invoke(() -> startSender( "ln" ));

    vm3.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm4.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm5.invoke(() -> verifySenderState(
        "ln", true, false ));

    vm3.invoke(() -> pauseSender( "ln" ));
    vm4.invoke(() -> pauseSender( "ln" ));
    vm5.invoke(() -> pauseSender( "ln" ));

    vm3.invoke(() -> verifySenderState(
        "ln", true, true ));
    vm4.invoke(() -> verifySenderState(
        "ln", true, true ));
    vm5.invoke(() -> verifySenderState(
        "ln", true, true ));

    pause(10000);
    String command = CliStrings.RESUME_GATEWAYSENDER + " --"
        + CliStrings.RESUME_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.RESUME_GATEWAYSENDER__GROUP + "=SenderGroup1";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testResumeGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult
          .getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(3, status.size());
      assertFalse(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testResumeGatewaySender failed as did not get CommandResult");
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
  public void testResumeGatewaySender_MultipleGroup() {

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
        punePort, "SenderGroup2" ));
    vm6.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));
    vm7.invoke(() -> createCacheWithGroups(
        punePort, "SenderGroup3" ));
    vm7.invoke(() -> createSender( "ln",
        2, false, 100, 400, false, false, null, true ));

    vm3.invoke(() -> startSender( "ln" ));
    vm4.invoke(() -> startSender( "ln" ));
    vm5.invoke(() -> startSender( "ln" ));
    vm6.invoke(() -> startSender( "ln" ));
    vm7.invoke(() -> startSender( "ln" ));

    vm3.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm4.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm5.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm6.invoke(() -> verifySenderState(
        "ln", true, false ));
    vm7.invoke(() -> verifySenderState(
        "ln", true, false ));

    vm3.invoke(() -> pauseSender( "ln" ));
    vm4.invoke(() -> pauseSender( "ln" ));
    vm5.invoke(() -> pauseSender( "ln" ));
    vm6.invoke(() -> pauseSender( "ln" ));
    vm7.invoke(() -> pauseSender( "ln" ));

    vm3.invoke(() -> verifySenderState(
        "ln", true, true ));
    vm4.invoke(() -> verifySenderState(
        "ln", true, true ));
    vm5.invoke(() -> verifySenderState(
        "ln", true, true ));
    vm6.invoke(() -> verifySenderState(
        "ln", true, true ));
    vm7.invoke(() -> verifySenderState(
        "ln", true, true ));

    pause(10000);
    String command = CliStrings.RESUME_GATEWAYSENDER + " --"
        + CliStrings.RESUME_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.RESUME_GATEWAYSENDER__GROUP
        + "=SenderGroup1,SenderGroup2";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info(
          "testResumeGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult
          .getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(4, status.size());
      assertFalse(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testResumeGatewaySender failed as did not get CommandResult");
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
        "ln", true, true ));
  }
}
