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

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.test.dunit.Assert.*;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.Wait.pause;

@Category(DistributedTest.class)
public class WanCommandGatewaySenderStopDUnitTest extends WANCommandTestBase {

  private static final long serialVersionUID = 1L;

  private CommandResult executeCommandWithIgnoredExceptions(String command) {
    final IgnoredException exln = IgnoredException.addIgnoredException("Could not connect");
    CommandResult commandResult = executeCommand(command);
    exln.remove();
    return commandResult;
  }

  @Test
  public void testStopGatewaySender_ErrorConditions() {

    Integer punePort = (Integer) vm1.invoke(() -> createFirstLocatorWithDSId(1));

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "1");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(2, punePort));

    vm3.invoke(() -> createCache(punePort));
    vm3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    final DistributedMember vm1Member = (DistributedMember) vm3.invoke(() -> getMember());

    String command = CliStrings.STOP_GATEWAYSENDER + " --" + CliStrings.STOP_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.STOP_GATEWAYSENDER__MEMBER + "=" + vm1Member.getId() + " --"
        + CliStrings.STOP_GATEWAYSENDER__GROUP + "=SenderGroup1";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testStopGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.ERROR, cmdResult.getStatus());
      assertTrue(strCmdResult.contains(CliStrings.PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE));
    } else {
      fail("testStopGatewaySender failed as did not get CommandResult");
    }
  }

  @Test
  public void testStopGatewaySender() {

    Integer punePort = (Integer) vm1.invoke(() -> createFirstLocatorWithDSId(1));

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "1");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(2, punePort));

    vm3.invoke(() -> createCache(punePort));
    vm3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm4.invoke(() -> createCache(punePort));
    vm4.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm5.invoke(() -> createCache(punePort));
    vm5.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    vm3.invoke(() -> startSender("ln"));
    vm4.invoke(() -> startSender("ln"));
    vm5.invoke(() -> startSender("ln"));

    vm3.invoke(() -> verifySenderState("ln", true, false));
    vm4.invoke(() -> verifySenderState("ln", true, false));
    vm5.invoke(() -> verifySenderState("ln", true, false));

    pause(10000);
    String command =
        CliStrings.STOP_GATEWAYSENDER + " --" + CliStrings.STOP_GATEWAYSENDER__ID + "=ln";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testStopGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(5, status.size());
      assertTrue(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testStopGatewaySender failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifySenderState("ln", false, false));
    vm4.invoke(() -> verifySenderState("ln", false, false));
    vm5.invoke(() -> verifySenderState("ln", false, false));
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on a member
   */
  @Test
  public void testStopGatewaySender_onMember() {

    Integer punePort = (Integer) vm1.invoke(() -> createFirstLocatorWithDSId(1));

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "1");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(2, punePort));

    vm3.invoke(() -> createCache(punePort));
    vm3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    vm3.invoke(() -> startSender("ln"));

    vm3.invoke(() -> verifySenderState("ln", true, false));

    final DistributedMember vm1Member = (DistributedMember) vm3.invoke(() -> getMember());
    pause(10000);
    String command = CliStrings.STOP_GATEWAYSENDER + " --" + CliStrings.STOP_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.STOP_GATEWAYSENDER__MEMBER + "=" + vm1Member.getId();
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testStopGatewaySender stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      assertTrue(strCmdResult.contains("is stopped on member"));
    } else {
      fail("testStopGatewaySender failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifySenderState("ln", false, false));
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on a group of members
   */
  @Test
  public void testStopGatewaySender_Group() {

    Integer punePort = (Integer) vm1.invoke(() -> createFirstLocatorWithDSId(1));

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "1");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(2, punePort));

    vm3.invoke(() -> createCacheWithGroups(punePort, "SenderGroup1"));
    vm3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm4.invoke(() -> createCacheWithGroups(punePort, "SenderGroup1"));
    vm4.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm5.invoke(() -> createCacheWithGroups(punePort, "SenderGroup1"));
    vm5.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    vm3.invoke(() -> startSender("ln"));
    vm4.invoke(() -> startSender("ln"));
    vm5.invoke(() -> startSender("ln"));

    vm3.invoke(() -> verifySenderState("ln", true, false));
    vm4.invoke(() -> verifySenderState("ln", true, false));
    vm5.invoke(() -> verifySenderState("ln", true, false));

    pause(10000);
    String command = CliStrings.STOP_GATEWAYSENDER + " --" + CliStrings.STOP_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.STOP_GATEWAYSENDER__GROUP + "=SenderGroup1";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testStopGatewaySender_Group stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(3, status.size());
      assertFalse(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testStopGatewaySender failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifySenderState("ln", false, false));
    vm4.invoke(() -> verifySenderState("ln", false, false));
    vm5.invoke(() -> verifySenderState("ln", false, false));
  }

  /**
   * Test to validate the scenario gateway sender is started when one or more sender members belongs
   * to multiple groups
   */
  @Test
  public void testStopGatewaySender_MultipleGroup() {

    Integer punePort = (Integer) vm1.invoke(() -> createFirstLocatorWithDSId(1));

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "1");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(2, punePort));

    vm3.invoke(() -> createCacheWithGroups(punePort, "SenderGroup1"));
    vm3.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm4.invoke(() -> createCacheWithGroups(punePort, "SenderGroup1"));
    vm4.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm5.invoke(() -> createCacheWithGroups(punePort, "SenderGroup1, SenderGroup2"));
    vm5.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm6.invoke(() -> createCacheWithGroups(punePort, "SenderGroup2"));
    vm6.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));
    vm7.invoke(() -> createCacheWithGroups(punePort, "SenderGroup3"));
    vm7.invoke(() -> createSender("ln", 2, false, 100, 400, false, false, null, true));

    vm3.invoke(() -> startSender("ln"));
    vm4.invoke(() -> startSender("ln"));
    vm5.invoke(() -> startSender("ln"));
    vm6.invoke(() -> startSender("ln"));
    vm7.invoke(() -> startSender("ln"));

    vm3.invoke(() -> verifySenderState("ln", true, false));
    vm4.invoke(() -> verifySenderState("ln", true, false));
    vm5.invoke(() -> verifySenderState("ln", true, false));
    vm6.invoke(() -> verifySenderState("ln", true, false));
    vm7.invoke(() -> verifySenderState("ln", true, false));

    pause(10000);
    String command = CliStrings.STOP_GATEWAYSENDER + " --" + CliStrings.STOP_GATEWAYSENDER__ID
        + "=ln --" + CliStrings.STOP_GATEWAYSENDER__GROUP + "=SenderGroup1,SenderGroup2";
    CommandResult cmdResult = executeCommandWithIgnoredExceptions(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testStopGatewaySender_Group stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(4, status.size());
      assertFalse(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testStopGatewaySender failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifySenderState("ln", false, false));
    vm4.invoke(() -> verifySenderState("ln", false, false));
    vm5.invoke(() -> verifySenderState("ln", false, false));
    vm6.invoke(() -> verifySenderState("ln", false, false));
    vm7.invoke(() -> verifySenderState("ln", true, false));
  }
}
