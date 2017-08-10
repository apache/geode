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
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class StatusGatewaySenderCommandDUnitTest extends WANCommandTestBase {
  @Test
  public void testGatewaySenderStatus() {
    Integer lnPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(lnPort);
    Integer nyPort = vm2.invoke(() -> createFirstRemoteLocator(2, lnPort));

    vm6.invoke(() -> createAndStartReceiver(nyPort));
    vm3.invoke(() -> createCache(lnPort));
    vm3.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    vm3.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));
    vm4.invoke(() -> createCache(lnPort));
    vm4.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    vm4.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));
    vm5.invoke(() -> createCache(lnPort));
    vm5.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    vm5.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));

    pause(10000);
    String command = CliStrings.STATUS_GATEWAYSENDER + " --" + CliStrings.STATUS_GATEWAYSENDER__ID
        + "=ln_Serial";
    CommandResult cmdResult = executeCommand(command);

    if (cmdResult != null) {
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(3, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_RUNNING));
      tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_NOT_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(2, result_hosts.size());
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testGatewaySenderStatus : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
    vm3.invoke(() -> startSender("ln_Serial"));
    vm3.invoke(() -> startSender("ln_Parallel"));
    vm4.invoke(() -> startSender("ln_Serial"));
    vm4.invoke(() -> startSender("ln_Parallel"));
    vm5.invoke(() -> startSender("ln_Serial"));
    vm5.invoke(() -> startSender("ln_Parallel"));

    pause(10000);
    command = CliStrings.STATUS_GATEWAYSENDER + " --" + CliStrings.STATUS_GATEWAYSENDER__ID
        + "=ln_Serial";
    cmdResult = executeCommand(command);

    if (cmdResult != null) {
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(3, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_NOT_RUNNING));
      tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_NOT_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(2, result_hosts.size());
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testGatewaySenderStatus : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
  }

  @Test
  public void testGatewaySenderStatus_OnMember() {
    Integer lnPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(lnPort);
    Integer nyPort = vm2.invoke(() -> createFirstRemoteLocator(2, lnPort));

    vm6.invoke(() -> createAndStartReceiver(nyPort));
    vm3.invoke(() -> createCache(lnPort));
    vm3.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    vm3.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));
    vm4.invoke(() -> createCache(lnPort));
    vm4.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    vm4.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));
    vm5.invoke(() -> createCache(lnPort));

    final DistributedMember vm1Member = vm3.invoke(this::getMember);
    pause(10000);
    String command = CliStrings.STATUS_GATEWAYSENDER + " --" + CliStrings.STATUS_GATEWAYSENDER__ID
        + "=ln_Serial --" + CliStrings.MEMBER + "=" + vm1Member.getId();
    CommandResult cmdResult = executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testGatewaySenderStatus_OnMember : " + strCmdResult + ">>>>> ");
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(1, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_RUNNING));
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
    vm3.invoke(() -> startSender("ln_Serial"));
    vm3.invoke(() -> startSender("ln_Parallel"));
    vm4.invoke(() -> startSender("ln_Serial"));
    vm4.invoke(() -> startSender("ln_Parallel"));

    pause(10000);
    command = CliStrings.STATUS_GATEWAYSENDER + " --" + CliStrings.STATUS_GATEWAYSENDER__ID
        + "=ln_Serial --" + CliStrings.MEMBER + "=" + vm1Member.getId();
    cmdResult = executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testGatewaySenderStatus_OnMember : " + strCmdResult + ">>>>> ");
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(1, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_NOT_RUNNING));
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
    final DistributedMember vm5Member = vm5.invoke(this::getMember);
    command = CliStrings.STATUS_GATEWAYSENDER + " --" + CliStrings.STATUS_GATEWAYSENDER__ID
        + "=ln_Serial --" + CliStrings.MEMBER + "=" + vm5Member.getId();
    cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testGatewaySenderStatus_OnMember : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
  }

  @Test
  public void testGatewaySenderStatus_OnGroups() {
    Integer lnPort = vm1.invoke(() -> createFirstLocatorWithDSId(1));
    propsSetUp(lnPort);
    Integer nyPort = vm2.invoke(() -> createFirstRemoteLocator(2, lnPort));

    vm7.invoke(() -> createAndStartReceiver(nyPort));
    vm3.invoke(() -> createCacheWithGroups(lnPort, "Serial_Sender, Parallel_Sender"));
    vm3.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    vm3.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));
    vm4.invoke(() -> createCacheWithGroups(lnPort, "Serial_Sender, Parallel_Sender"));
    vm4.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    vm4.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));
    vm5.invoke(() -> createCacheWithGroups(lnPort, "Parallel_Sender"));
    vm5.invoke(() -> createSender("ln_Serial", 2, false, 100, 400, false, false, null, true));
    vm5.invoke(() -> createSender("ln_Parallel", 2, true, 100, 400, false, false, null, true));
    vm6.invoke(() -> createCacheWithGroups(lnPort, "Serial_Sender"));

    final DistributedMember vm1Member = vm3.invoke(this::getMember);
    pause(10000);
    String command = CliStrings.STATUS_GATEWAYSENDER + " --" + CliStrings.STATUS_GATEWAYSENDER__ID
        + "=ln_Serial --" + CliStrings.GROUP + "=Serial_Sender";
    CommandResult cmdResult = executeCommand(command);

    if (cmdResult != null) {
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(2, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_RUNNING));
      tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_NOT_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(1, result_hosts.size());
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testGatewaySenderStatus_OnGroups : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
    vm3.invoke(() -> startSender("ln_Serial"));
    vm3.invoke(() -> startSender("ln_Parallel"));
    vm4.invoke(() -> startSender("ln_Serial"));
    vm4.invoke(() -> startSender("ln_Parallel"));

    pause(10000);
    command = CliStrings.STATUS_GATEWAYSENDER + " --" + CliStrings.STATUS_GATEWAYSENDER__ID
        + "=ln_Serial --" + CliStrings.GROUP + "=Serial_Sender";
    cmdResult = executeCommand(command);

    if (cmdResult != null) {
      TabularResultData tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(2, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_NOT_RUNNING));
      tableResultData = ((CompositeResultData) cmdResult.getResultData())
          .retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_NOT_AVAILABLE)
          .retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(1, result_hosts.size());
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testGatewaySenderStatus_OnGroups : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
  }
}
