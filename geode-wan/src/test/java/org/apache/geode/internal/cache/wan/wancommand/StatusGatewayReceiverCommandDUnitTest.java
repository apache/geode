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

import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.Wait.pause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

@Category(DistributedTest.class)
public class StatusGatewayReceiverCommandDUnitTest extends WANCommandTestBase {
  @Test
  public void testGatewayReceiverStatus() {
    Integer lnPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(lnPort);
    Integer nyPort = vm2.invoke(() -> createFirstRemoteLocator(2, lnPort));

    vm6.invoke(() -> createAndStartReceiver(nyPort));
    vm3.invoke(() -> createAndStartReceiver(lnPort));
    vm4.invoke(() -> createAndStartReceiver(lnPort));
    vm5.invoke(() -> createAndStartReceiver(lnPort));

    pause(10000);
    String command = CliStrings.STATUS_GATEWAYRECEIVER;
    CommandResult cmdResult = executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testGatewayReceiverStatus : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(3, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_NOT_RUNNING));
      tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER_NOT_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> result_hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(2, result_hosts.size());
    } else {
      fail("testGatewayReceiverStatus failed as did not get CommandResult");
    }
    vm3.invoke(this::stopReceiver);
    vm4.invoke(this::stopReceiver);
    vm5.invoke(this::stopReceiver);
    pause(10000);
    command = CliStrings.STATUS_GATEWAYRECEIVER;
    cmdResult = executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testGatewayReceiverStatus : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(3, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_RUNNING));
      tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER_NOT_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> result_hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(2, result_hosts.size());
    } else {
      fail("testGatewayReceiverStatus failed as did not get CommandResult");
    }
  }

  @Category(FlakyTest.class) // GEODE-1395
  @Test
  public void testGatewayReceiverStatus_OnMember() {
    Integer lnPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(lnPort);
    Integer nyPort = vm2.invoke(() -> createFirstRemoteLocator(2, lnPort));

    vm6.invoke(() -> createAndStartReceiver(nyPort));
    vm3.invoke(() -> createAndStartReceiver(lnPort));
    vm4.invoke(() -> createAndStartReceiver(lnPort));
    vm5.invoke(() -> createAndStartReceiver(lnPort));

    final DistributedMember vm3Member = vm3.invoke(this::getMember);
    pause(10000);
    String command =
        CliStrings.STATUS_GATEWAYRECEIVER + " --" + CliStrings.MEMBER + "=" + vm3Member.getId();
    CommandResult cmdResult = executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testGatewayReceiverStatus : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(1, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_NOT_RUNNING));
    } else {
      fail("testGatewayReceiverStatus failed as did not get CommandResult");
    }
    vm3.invoke(this::stopReceiver);
    vm4.invoke(this::stopReceiver);
    vm5.invoke(this::stopReceiver);

    pause(10000);
    command =
        CliStrings.STATUS_GATEWAYRECEIVER + " --" + CliStrings.MEMBER + "=" + vm3Member.getId();
    cmdResult = executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testGatewayReceiverStatus : " + strCmdResult + ">>>>> ");
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(1, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_RUNNING));
    } else {
      fail("testGatewayReceiverStatus failed as did not get CommandResult");
    }
  }

  @Test
  public void testGatewayReceiverStatus_OnGroups() {
    Integer lnPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(lnPort);
    Integer nyPort = vm2.invoke(() -> createFirstRemoteLocator(2, lnPort));

    vm7.invoke(() -> createAndStartReceiver(nyPort));
    vm3.invoke(() -> createAndStartReceiverWithGroup(lnPort, "RG1, RG2"));
    vm4.invoke(() -> createAndStartReceiverWithGroup(lnPort, "RG1, RG2"));
    vm5.invoke(() -> createAndStartReceiverWithGroup(lnPort, "RG1"));
    vm6.invoke(() -> createAndStartReceiverWithGroup(lnPort, "RG2"));

    pause(10000);
    String command = CliStrings.STATUS_GATEWAYRECEIVER + " --" + CliStrings.GROUP + "=RG1";
    CommandResult cmdResult = executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testGatewayReceiverStatus : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(3, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_NOT_RUNNING));
    } else {
      fail("testGatewayReceiverStatus failed as did not get CommandResult");
    }
    vm3.invoke(this::stopReceiver);
    vm4.invoke(this::stopReceiver);
    vm5.invoke(this::stopReceiver);

    pause(10000);
    command = CliStrings.STATUS_GATEWAYRECEIVER + " --" + CliStrings.GROUP + "=RG1";
    cmdResult = executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testGatewayReceiverStatus_OnGroups : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(3, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_RUNNING));
    } else {
      fail("testGatewayReceiverStatus failed as did not get CommandResult");
    }
  }
}
