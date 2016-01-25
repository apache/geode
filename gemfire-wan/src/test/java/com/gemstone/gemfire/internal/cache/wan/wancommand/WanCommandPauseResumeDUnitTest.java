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

public class WanCommandPauseResumeDUnitTest extends WANCommandTestBase {

  private static final long serialVersionUID = 1L;

  public WanCommandPauseResumeDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testPauseGatewaySender_ErrorConditions() {

    Integer punePort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + punePort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, punePort });

    vm3.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { punePort });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });

    final DistributedMember vm1Member = (DistributedMember) vm3.invoke(
        WANCommandTestBase.class, "getMember");

    String command = CliStrings.PAUSE_GATEWAYSENDER + " --"
        + CliStrings.PAUSE_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.PAUSE_GATEWAYSENDER__MEMBER + "=" + vm1Member.getId()
        + " --" + CliStrings.PAUSE_GATEWAYSENDER__GROUP + "=SenderGroup1";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testPauseGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.ERROR, cmdResult.getStatus());
      assertTrue(strCmdResult.contains(CliStrings.PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE));
    } else {
      fail("testPauseGatewaySender failed as did not get CommandResult");
    }
  }

  public void testPauseGatewaySender() {

    Integer punePort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + punePort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, punePort });

    vm3.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { punePort });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });
    vm4.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { punePort });
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });
    vm5.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { punePort });
    vm5.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });

    vm3.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });
    vm4.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm4.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm5.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });

    pause(10000);
    String command = CliStrings.PAUSE_GATEWAYSENDER + " --"
        + CliStrings.PAUSE_GATEWAYSENDER__ID + "=ln";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
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

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
    vm4.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
    vm5.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on
   * a member
   */
  public void testPauseGatewaySender_onMember() {

    Integer punePort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + punePort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, punePort });

    vm3.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { punePort });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });

    vm3.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });

    final DistributedMember vm1Member = (DistributedMember) vm3.invoke(
        WANCommandTestBase.class, "getMember");
    pause(10000);
    String command = CliStrings.PAUSE_GATEWAYSENDER + " --"
        + CliStrings.PAUSE_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.PAUSE_GATEWAYSENDER__MEMBER + "=" + vm1Member.getId();
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testPauseGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      assertTrue(strCmdResult.contains("is paused on member"));
    } else {
      fail("testPauseGatewaySender failed as did not get CommandResult");
    }

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on
   * a group of members
   */
  public void testPauseGatewaySender_Group() {

    Integer punePort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + punePort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, punePort });

    vm3.invoke(WANCommandTestBase.class, "createCacheWithGroups", new Object[] {
        punePort, "SenderGroup1" });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });
    vm4.invoke(WANCommandTestBase.class, "createCacheWithGroups", new Object[] {
        punePort, "SenderGroup1" });
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });
    vm5.invoke(WANCommandTestBase.class, "createCacheWithGroups", new Object[] {
        punePort, "SenderGroup1" });
    vm5.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });

    vm3.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });
    vm4.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm4.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm5.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });

    pause(10000);
    String command = CliStrings.PAUSE_GATEWAYSENDER + " --"
        + CliStrings.PAUSE_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.PAUSE_GATEWAYSENDER__GROUP + "=SenderGroup1";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
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

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
    vm4.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
    vm5.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
  }

  /**
   * Test to validate the scenario gateway sender is started when one or more
   * sender members belongs to multiple groups
   * 
   */
  public void testPauseGatewaySender_MultipleGroup() {

    Integer punePort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + punePort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, punePort });

    vm3.invoke(WANCommandTestBase.class, "createCacheWithGroups", new Object[] {
        punePort, "SenderGroup1" });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });
    vm4.invoke(WANCommandTestBase.class, "createCacheWithGroups", new Object[] {
        punePort, "SenderGroup1" });
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });
    vm5.invoke(WANCommandTestBase.class, "createCacheWithGroups", new Object[] {
        punePort, "SenderGroup1, SenderGroup2" });
    vm5.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });
    vm6.invoke(WANCommandTestBase.class, "createCacheWithGroups", new Object[] {
        punePort, "SenderGroup2" });
    vm6.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });
    vm7.invoke(WANCommandTestBase.class, "createCacheWithGroups", new Object[] {
        punePort, "SenderGroup3" });
    vm7.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });

    vm3.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });
    vm4.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm4.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm5.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm6.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm7.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });

    pause(10000);
    String command = CliStrings.PAUSE_GATEWAYSENDER + " --"
        + CliStrings.PAUSE_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.PAUSE_GATEWAYSENDER__GROUP + "=SenderGroup1,SenderGroup2";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
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

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
    vm4.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
    vm5.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
    vm6.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
    vm7.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
  }

  public void testResumeGatewaySender_ErrorConditions() {

    Integer punePort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + punePort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, punePort });

    vm3.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { punePort });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });

    final DistributedMember vm1Member = (DistributedMember) vm3.invoke(
        WANCommandTestBase.class, "getMember");

    String command = CliStrings.RESUME_GATEWAYSENDER + " --"
        + CliStrings.RESUME_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.RESUME_GATEWAYSENDER__MEMBER + "=" + vm1Member.getId()
        + " --" + CliStrings.RESUME_GATEWAYSENDER__GROUP + "=SenderGroup1";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testResumeGatewaySender_ErrorConditions stringResult : "
              + strCmdResult + ">>>>");
      assertEquals(Result.Status.ERROR, cmdResult.getStatus());
      assertTrue(strCmdResult.contains(CliStrings.PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE));
    } else {
      fail("testPauseGatewaySender failed as did not get CommandResult");
    }
  }

  public void testResumeGatewaySender() {

    Integer punePort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + punePort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, punePort });

    vm3.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { punePort });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });
    vm4.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { punePort });
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });
    vm5.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { punePort });
    vm5.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });

    vm3.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });
    vm4.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm4.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm5.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });

    vm3.invoke(WANCommandTestBase.class, "pauseSender", new Object[] { "ln" });
    vm4.invoke(WANCommandTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANCommandTestBase.class, "pauseSender", new Object[] { "ln" });

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
    vm4.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
    vm5.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });

    pause(10000);
    String command = CliStrings.RESUME_GATEWAYSENDER + " --"
        + CliStrings.RESUME_GATEWAYSENDER__ID + "=ln";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
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

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm4.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm5.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on
   * a member
   */
  public void testResumeGatewaySender_onMember() {

    Integer punePort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + punePort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, punePort });

    vm3.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { punePort });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });

    vm3.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });

    vm3.invoke(WANCommandTestBase.class, "pauseSender", new Object[] { "ln" });

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });

    final DistributedMember vm1Member = (DistributedMember) vm3.invoke(
        WANCommandTestBase.class, "getMember");
    pause(10000);
    String command = CliStrings.RESUME_GATEWAYSENDER + " --"
        + CliStrings.RESUME_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.RESUME_GATEWAYSENDER__MEMBER + "=" + vm1Member.getId();
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testResumeGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      assertTrue(strCmdResult.contains("is resumed on member"));
    } else {
      fail("testResumeGatewaySender failed as did not get CommandResult");
    }

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on
   * a group of members
   */
  public void testResumeGatewaySender_Group() {

    Integer punePort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + punePort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, punePort });

    vm3.invoke(WANCommandTestBase.class, "createCacheWithGroups", new Object[] {
        punePort, "SenderGroup1" });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });
    vm4.invoke(WANCommandTestBase.class, "createCacheWithGroups", new Object[] {
        punePort, "SenderGroup1" });
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });
    vm5.invoke(WANCommandTestBase.class, "createCacheWithGroups", new Object[] {
        punePort, "SenderGroup1" });
    vm5.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });

    vm3.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });
    vm4.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm4.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm5.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });

    vm3.invoke(WANCommandTestBase.class, "pauseSender", new Object[] { "ln" });
    vm4.invoke(WANCommandTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANCommandTestBase.class, "pauseSender", new Object[] { "ln" });

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
    vm4.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
    vm5.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });

    pause(10000);
    String command = CliStrings.RESUME_GATEWAYSENDER + " --"
        + CliStrings.RESUME_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.RESUME_GATEWAYSENDER__GROUP + "=SenderGroup1";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
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

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm4.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm5.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
  }

  /**
   * Test to validate the scenario gateway sender is started when one or more
   * sender members belongs to multiple groups
   * 
   */
  public void testResumeGatewaySender_MultipleGroup() {

    Integer punePort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + punePort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, punePort });

    vm3.invoke(WANCommandTestBase.class, "createCacheWithGroups", new Object[] {
        punePort, "SenderGroup1" });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });
    vm4.invoke(WANCommandTestBase.class, "createCacheWithGroups", new Object[] {
        punePort, "SenderGroup1" });
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });
    vm5.invoke(WANCommandTestBase.class, "createCacheWithGroups", new Object[] {
        punePort, "SenderGroup1, SenderGroup2" });
    vm5.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });
    vm6.invoke(WANCommandTestBase.class, "createCacheWithGroups", new Object[] {
        punePort, "SenderGroup2" });
    vm6.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });
    vm7.invoke(WANCommandTestBase.class, "createCacheWithGroups", new Object[] {
        punePort, "SenderGroup3" });
    vm7.invoke(WANCommandTestBase.class, "createSender", new Object[] { "ln",
        2, false, 100, 400, false, false, null, true });

    vm3.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });
    vm4.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });
    vm5.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });
    vm6.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });
    vm7.invoke(WANCommandTestBase.class, "startSender", new Object[] { "ln" });

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm4.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm5.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm6.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm7.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });

    vm3.invoke(WANCommandTestBase.class, "pauseSender", new Object[] { "ln" });
    vm4.invoke(WANCommandTestBase.class, "pauseSender", new Object[] { "ln" });
    vm5.invoke(WANCommandTestBase.class, "pauseSender", new Object[] { "ln" });
    vm6.invoke(WANCommandTestBase.class, "pauseSender", new Object[] { "ln" });
    vm7.invoke(WANCommandTestBase.class, "pauseSender", new Object[] { "ln" });

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
    vm4.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
    vm5.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
    vm6.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
    vm7.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });

    pause(10000);
    String command = CliStrings.RESUME_GATEWAYSENDER + " --"
        + CliStrings.RESUME_GATEWAYSENDER__ID + "=ln --"
        + CliStrings.RESUME_GATEWAYSENDER__GROUP
        + "=SenderGroup1,SenderGroup2";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
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

    vm3.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm4.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm5.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm6.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, false });
    vm7.invoke(WANCommandTestBase.class, "verifySenderState", new Object[] {
        "ln", true, true });
  }
}
