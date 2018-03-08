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

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.createAndStartReceiver;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.getMemberIdCallable;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.stopReceiver;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.Wait.pause;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({DistributedTest.class, WanTest.class})
public class StatusGatewayReceiverCommandDUnitTest {

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private MemberVM locatorSite1;
  private MemberVM locatorSite2;
  private MemberVM server1;
  private MemberVM server2;
  private MemberVM server3;
  private MemberVM server4;
  private MemberVM server5;

  @Before
  public void before() throws Exception {
    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    locatorSite1 = clusterStartupRule.startLocatorVM(1, props);

    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 2);
    props.setProperty(REMOTE_LOCATORS, "localhost[" + locatorSite1.getPort() + "]");
    locatorSite2 = clusterStartupRule.startLocatorVM(2, props);

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locatorSite1);
  }

  @Test
  public void testGatewayReceiverStatus() throws Exception {

    Integer lnPort = locatorSite1.getPort();
    Integer nyPort = locatorSite2.getPort();

    // setup servers in Site #1 (London)
    server1 = clusterStartupRule.startServerVM(3, lnPort);
    server2 = clusterStartupRule.startServerVM(4, lnPort);
    server3 = clusterStartupRule.startServerVM(5, lnPort);

    // server in Site 2 (New York)
    server4 = clusterStartupRule.startServerVM(6, nyPort);

    server1.invoke(() -> createAndStartReceiver(lnPort));
    server2.invoke(() -> createAndStartReceiver(lnPort));
    server3.invoke(() -> createAndStartReceiver(lnPort));

    server4.invoke(() -> createAndStartReceiver(nyPort));

    pause(10000);
    String command = CliStrings.STATUS_GATEWAYRECEIVER;
    CommandResult cmdResult = gfsh.executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
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

    server1.invoke(() -> stopReceiver());
    server2.invoke(() -> stopReceiver());
    server3.invoke(() -> stopReceiver());

    pause(10000);
    command = CliStrings.STATUS_GATEWAYRECEIVER;
    cmdResult = gfsh.executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
      getLogWriter().info("testGatewayReceiverStatus : " + strCmdResult + ">>>>> ");
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

  @Test
  public void testGatewayReceiverStatus_OnMember() throws Exception {

    Integer lnPort = locatorSite1.getPort();
    Integer nyPort = locatorSite2.getPort();

    // setup servers in Site #1 (London)
    server1 = clusterStartupRule.startServerVM(3, lnPort);
    server2 = clusterStartupRule.startServerVM(4, lnPort);
    server3 = clusterStartupRule.startServerVM(5, lnPort);

    // server in Site 2 (New York)
    server4 = clusterStartupRule.startServerVM(6, nyPort);

    server1.invoke(() -> createAndStartReceiver(lnPort));
    server2.invoke(() -> createAndStartReceiver(lnPort));
    server3.invoke(() -> createAndStartReceiver(lnPort));

    server4.invoke(() -> createAndStartReceiver(nyPort));

    final DistributedMember vm3Member = (DistributedMember) server1.invoke(getMemberIdCallable());
    pause(10000);
    String command =
        CliStrings.STATUS_GATEWAYRECEIVER + " --" + CliStrings.MEMBER + "=" + vm3Member.getId();
    CommandResult cmdResult = gfsh.executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
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
    server1.invoke(() -> stopReceiver());
    server2.invoke(() -> stopReceiver());
    server3.invoke(() -> stopReceiver());

    pause(10000);
    command =
        CliStrings.STATUS_GATEWAYRECEIVER + " --" + CliStrings.MEMBER + "=" + vm3Member.getId();
    cmdResult = gfsh.executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
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
  public void testGatewayReceiverStatus_OnGroups() throws Exception {

    Integer lnPort = locatorSite1.getPort();
    Integer nyPort = locatorSite2.getPort();

    // setup servers in Site #1 (London)
    server1 = startServerWithGroups(3, "RG1, RG2", lnPort);
    server2 = startServerWithGroups(4, "RG1, RG2", lnPort);
    server3 = startServerWithGroups(5, "RG1", lnPort);
    server4 = startServerWithGroups(6, "RG2", lnPort);

    // server in Site 2 (New York) - no group
    server5 = clusterStartupRule.startServerVM(7, nyPort);

    server1.invoke(() -> createAndStartReceiver(lnPort));
    server2.invoke(() -> createAndStartReceiver(lnPort));
    server3.invoke(() -> createAndStartReceiver(lnPort));
    server4.invoke(() -> createAndStartReceiver(lnPort));

    server5.invoke(() -> createAndStartReceiver(nyPort));

    pause(10000);
    String command = CliStrings.STATUS_GATEWAYRECEIVER + " --" + CliStrings.GROUP + "=RG1";
    CommandResult cmdResult = gfsh.executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
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
    server1.invoke(() -> stopReceiver());
    server2.invoke(() -> stopReceiver());
    server3.invoke(() -> stopReceiver());

    pause(10000);
    command = CliStrings.STATUS_GATEWAYRECEIVER + " --" + CliStrings.GROUP + "=RG1";
    cmdResult = gfsh.executeCommand(command);

    if (cmdResult != null) {
      String strCmdResult = cmdResult.toString();
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

  private MemberVM startServerWithGroups(int index, String groups, int locPort) throws Exception {
    Properties props = new Properties();
    props.setProperty(GROUPS, groups);
    return clusterStartupRule.startServerVM(index, props, locPort);
  }
}
