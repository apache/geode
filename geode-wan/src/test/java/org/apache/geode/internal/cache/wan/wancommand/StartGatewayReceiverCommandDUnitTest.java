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
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

@Category(DistributedTest.class)
public class StartGatewayReceiverCommandDUnitTest extends WANCommandTestBase {
  /**
   * Test wan commands for error in input 1> start gateway-sender command needs only one of member
   * or group.
   */
  @Test
  public void testStartGatewayReceiver_ErrorConditions() {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createReceiver(dsIdPort));

    final DistributedMember vm1Member = vm3.invoke(this::getMember);
    String command = CliStrings.START_GATEWAYRECEIVER + " --" + CliStrings.MEMBER + "="
        + vm1Member.getId() + " --" + CliStrings.GROUP + "=RG1";
    CommandResult cmdResult = executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter()
          .info("testStartGatewayReceiver_ErrorConditions stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.ERROR, cmdResult.getStatus());
      assertTrue(strCmdResult.contains(CliStrings.PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE));
    } else {
      fail("testStartGatewayReceiver_ErrorConditions failed as did not get CommandResult");
    }
  }

  @Category(FlakyTest.class) // GEODE-1448
  @Test
  public void testStartGatewayReceiver() {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createReceiver(dsIdPort));
    vm4.invoke(() -> createReceiver(dsIdPort));
    vm5.invoke(() -> createReceiver(dsIdPort));
    vm3.invoke(() -> verifyReceiverState(false));
    vm4.invoke(() -> verifyReceiverState(false));
    vm5.invoke(() -> verifyReceiverState(false));

    pause(10000);
    String command = CliStrings.START_GATEWAYRECEIVER;
    CommandResult cmdResult = executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testStartGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(4, status.size());
      assertTrue(status.contains("Error"));
    } else {
      fail("testStartGatewayReceiver failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifyReceiverState(true));
    vm4.invoke(() -> verifyReceiverState(true));
    vm5.invoke(() -> verifyReceiverState(true));
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on a member
   */
  @Test
  public void testStartGatewayReceiver_onMember() {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createReceiver(dsIdPort));
    vm4.invoke(() -> createReceiver(dsIdPort));
    vm5.invoke(() -> createReceiver(dsIdPort));
    vm3.invoke(() -> verifyReceiverState(false));
    vm4.invoke(() -> verifyReceiverState(false));
    vm5.invoke(() -> verifyReceiverState(false));

    final DistributedMember vm1Member = vm3.invoke(() -> getMember());
    pause(10000);
    String command =
        CliStrings.START_GATEWAYRECEIVER + " --" + CliStrings.MEMBER + "=" + vm1Member.getId();
    CommandResult cmdResult = executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter()
          .info("testStartGatewayReceiver_onMember stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      assertTrue(strCmdResult.contains("is started on member"));
    } else {
      fail("testStartGatewayReceiver failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifyReceiverState(true));
    vm4.invoke(() -> verifyReceiverState(false));
    vm5.invoke(() -> verifyReceiverState(false));
  }

  /**
   * test to validate that the start gateway sender starts the gateway sender on a group of members
   */
  @Test
  public void testStartGatewayReceiver_Group() {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createReceiverWithGroup(dsIdPort, "RG1"));
    vm4.invoke(() -> createReceiverWithGroup(dsIdPort, "RG1"));
    vm5.invoke(() -> createReceiverWithGroup(dsIdPort, "RG1"));
    vm3.invoke(() -> verifyReceiverState(false));
    vm4.invoke(() -> verifyReceiverState(false));
    vm5.invoke(() -> verifyReceiverState(false));

    pause(10000);
    String command = CliStrings.START_GATEWAYRECEIVER + " --" + CliStrings.GROUP + "=RG1";
    CommandResult cmdResult = executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testStartGatewayReceiver_Group stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(3, status.size());
      assertFalse(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testStartGatewayReceiver_Group failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifyReceiverState(true));
    vm4.invoke(() -> verifyReceiverState(true));
    vm5.invoke(() -> verifyReceiverState(true));
  }

  /**
   * Test to validate the scenario gateway sender is started when one or more sender members belongs
   * to multiple groups
   * 
   */
  @Test
  public void testStartGatewayReceiver_MultipleGroup() {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);

    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));
    vm3.invoke(() -> createReceiverWithGroup(dsIdPort, "RG1"));
    vm4.invoke(() -> createReceiverWithGroup(dsIdPort, "RG1"));
    vm5.invoke(() -> createReceiverWithGroup(dsIdPort, "RG1, RG2"));
    vm6.invoke(() -> createReceiverWithGroup(dsIdPort, "RG1, RG2"));
    vm7.invoke(() -> createReceiverWithGroup(dsIdPort, "RG3"));
    vm3.invoke(() -> verifyReceiverState(false));
    vm4.invoke(() -> verifyReceiverState(false));
    vm5.invoke(() -> verifyReceiverState(false));
    vm6.invoke(() -> verifyReceiverState(false));
    vm7.invoke(() -> verifyReceiverState(false));

    pause(10000);
    String command = CliStrings.START_GATEWAYRECEIVER + " --" + CliStrings.GROUP + "=RG1,RG2";
    CommandResult cmdResult = executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testStartGatewayReceiver_Group stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Result");
      assertEquals(4, status.size());
      assertFalse(status.contains("Error"));
      assertTrue(status.contains("OK"));
    } else {
      fail("testStartGatewayReceiver failed as did not get CommandResult");
    }
    vm3.invoke(() -> verifyReceiverState(true));
    vm4.invoke(() -> verifyReceiverState(true));
    vm5.invoke(() -> verifyReceiverState(true));
    vm6.invoke(() -> verifyReceiverState(true));
    vm7.invoke(() -> verifyReceiverState(false));
  }
}
