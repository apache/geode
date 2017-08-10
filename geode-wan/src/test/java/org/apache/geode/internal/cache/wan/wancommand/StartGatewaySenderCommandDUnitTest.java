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

import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.Wait.pause;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class StartGatewaySenderCommandDUnitTest extends WANCommandTestBase {
  /**
   * Test wan commands for error in input 1> start gateway-sender command needs only one of member
   * or group.
   */
  @Test
  public void testStartGatewaySender_ErrorConditions() {
    Integer dsIdPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createCache(dsIdPort));
    vm3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    final DistributedMember vm1Member = vm3.invoke(this::getMember);
    String command = CliStrings.START_GATEWAYSENDER + " --" + CliStrings.START_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.MEMBER + "=" + vm1Member.getId() + " --" + CliStrings.GROUP
        + "=SenserGroup1";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testStartGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.ERROR, cmdResult.getStatus());
      assertTrue(strCmdResult.contains(CliStrings.PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE));
    } else {
      fail("testStartGatewaySender failed as did not get CommandResult");
    }
  }

  @Test
  public void testStartGatewaySender() {
    Integer dsIdPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createCache(dsIdPort));
    vm3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm4.invoke(() -> createCache(dsIdPort));
    vm4.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm5.invoke(() -> createCache(dsIdPort));
    vm5.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm3.invoke(() -> verifySenderState("ln", false, false));
    vm4.invoke(() -> verifySenderState("ln", false, false));
    vm5.invoke(() -> verifySenderState("ln", false, false));

    pause(10000);
    String command =
        CliStrings.START_GATEWAYSENDER + " --" + CliStrings.START_GATEWAYSENDER__ID + "=ln";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testStartGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(5, status.size());
      assertTrue(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testStartGatewaySender failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifySenderState("ln", true, false));
    vm4.invoke(() -> verifySenderState("ln", true, false));
    vm5.invoke(() -> verifySenderState("ln", true, false));
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on a member
   */
  @Test
  public void testStartGatewaySender_onMember() {
    Integer dsIdPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createCache(dsIdPort));
    vm3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm3.invoke(() -> verifySenderState("ln", false, false));

    final DistributedMember vm1Member = vm3.invoke(this::getMember);
    pause(10000);
    String command = CliStrings.START_GATEWAYSENDER + " --" + CliStrings.START_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.MEMBER + "=" + vm1Member.getId();
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testStartGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      assertTrue(strCmdResult.contains("is started on member"));
    } else {
      fail("testStartGatewaySender failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifySenderState("ln", true, false));
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on a group of members
   */
  @Test
  public void testStartGatewaySender_Group() {
    Integer dsIdPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup1"));
    vm3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm4.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup1"));
    vm4.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm5.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup1"));
    vm5.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm3.invoke(() -> verifySenderState("ln", false, false));
    vm4.invoke(() -> verifySenderState("ln", false, false));
    vm5.invoke(() -> verifySenderState("ln", false, false));

    pause(10000);
    String command = CliStrings.START_GATEWAYSENDER + " --" + CliStrings.START_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.GROUP + "=SenderGroup1";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testStartGatewaySender_Group stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(3, status.size());
      assertFalse(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testStartGatewaySender failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifySenderState("ln", true, false));
    vm4.invoke(() -> verifySenderState("ln", true, false));
    vm5.invoke(() -> verifySenderState("ln", true, false));
  }

  /**
   * Test to validate the scenario gateway sender is started when one or more sender members belongs
   * to multiple groups
   */
  @Test
  public void testStartGatewaySender_MultipleGroup() {
    Integer dsIdPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup1"));
    vm3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm4.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup1"));
    vm4.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm5.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup1, SenderGroup2"));
    vm5.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm6.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup1, SenderGroup2"));
    vm6.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm7.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup3"));
    vm7.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm3.invoke(() -> verifySenderState("ln", false, false));
    vm4.invoke(() -> verifySenderState("ln", false, false));
    vm5.invoke(() -> verifySenderState("ln", false, false));
    vm6.invoke(() -> verifySenderState("ln", false, false));
    vm7.invoke(() -> verifySenderState("ln", false, false));

    pause(10000);
    String command = CliStrings.START_GATEWAYSENDER + " --" + CliStrings.START_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.GROUP + "=SenderGroup1,SenderGroup2";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testStartGatewaySender_Group stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(4, status.size());
      assertFalse(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testStartGatewaySender failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifySenderState("ln", true, false));
    vm4.invoke(() -> verifySenderState("ln", true, false));
    vm5.invoke(() -> verifySenderState("ln", true, false));
    vm6.invoke(() -> verifySenderState("ln", true, false));
    vm7.invoke(() -> verifySenderState("ln", false, false));
  }

  /**
   * Test to validate the test scenario when one of the member ion group does not have the sender.
   */
  @Test
  public void testStartGatewaySender_Group_MissingSenderFromGroup() {
    Integer dsIdPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup1"));
    vm3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm4.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup1"));
    vm5.invoke(() -> createCacheWithGroups(dsIdPort, "SenderGroup1"));
    vm5.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm3.invoke(() -> verifySenderState("ln", false, false));
    vm5.invoke(() -> verifySenderState("ln", false, false));

    pause(10000);
    String command = CliStrings.START_GATEWAYSENDER + " --" + CliStrings.START_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.GROUP + "=SenderGroup1";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      assertTrue(strCmdResult.contains("Error"));
      assertTrue(strCmdResult.contains("is not available"));
      getLogWriter().info("testStartGatewaySender_Group stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(3, status.size());
      assertTrue(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testStartGatewaySender failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifySenderState("ln", true, false));
    vm5.invoke(() -> verifySenderState("ln", true, false));
  }

  private CommandResult executeCommandWithIgnoredExceptions(String command) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
    try {
      return executeCommand(command);
    } finally {
      exln.remove();
    }
  }
}
