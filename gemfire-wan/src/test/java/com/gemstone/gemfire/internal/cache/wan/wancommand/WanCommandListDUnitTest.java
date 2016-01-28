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

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.json.GfJsonException;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;

public class WanCommandListDUnitTest extends WANCommandTestBase {

  private static final long serialVersionUID = 1L;

  public WanCommandListDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void testListGatewayWithNoSenderReceiver() {

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
    vm4.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { punePort });
    vm5.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { punePort });
    
    pause(10000);
    String command = CliStrings.LIST_GATEWAY;
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info("testListGatewaySender : : " + strCmdResult);
      assertEquals(Result.Status.ERROR, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
  }

  
  public void testListGatewaySender() {

    Integer punePort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + punePort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, punePort });

    vm6.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { nyPort });
    vm7.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { nyPort });

    vm3.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { punePort });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Serial", 2, false, 100, 400, false, false, null, false });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 2, true, 100, 400, false, false, null, false });

    vm4.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { punePort });
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 2, true, 100, 400, false, false, null, false });
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Serial", 2, false, 100, 400, false, false, null, false });

    vm5.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { punePort });
    vm5.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Serial", 2, false, 100, 400, false, false, null, false });

    pause(10000);
    String command = CliStrings.LIST_GATEWAY;
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info("testListGatewaySender" + strCmdResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      
      TabularResultData tableResultData =
      ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_senderIds = tableResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
      assertTrue(result_senderIds.contains("ln_Serial"));
      assertTrue(result_senderIds.contains("ln_Parallel"));
      assertEquals(5, result_senderIds.size());
      
      assertEquals(null, ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER));
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
  }

  public void testListGatewayReceiver() {

    Integer lnPort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + lnPort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm3.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { lnPort });
    vm4.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { lnPort });

    vm5
        .invoke(WANCommandTestBase.class, "createCache",
            new Object[] { nyPort });
    vm5.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Serial", 1, false, 100, 400, false, false, null, false });
    vm6
        .invoke(WANCommandTestBase.class, "createCache",
            new Object[] { nyPort });
    vm6.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Serial", 1, false, 100, 400, false, false, null, false });
    vm6.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 1, true, 100, 400, false, false, null, false });

    pause(10000);
    String command = CliStrings.LIST_GATEWAY;
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info("testListGatewayReceiver" + strCmdResult);
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      
      TabularResultData tableResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER).retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> ports = tableResultData.retrieveAllValues(CliStrings.RESULT_PORT);
      assertEquals(2, ports.size());
      List<String> hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(2, hosts.size());
      
      assertEquals(null, ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER));
      
      
    } else {
      fail("testListGatewayReceiver failed as did not get CommandResult");
    }
  }
  
  public void testListGatewaySenderGatewayReceiver() throws GfJsonException {

    Integer lnPort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + lnPort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm6.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { nyPort });

    vm3.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { lnPort });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Serial", 2, false, 100, 400, false, false, null, false });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 2, true, 100, 400, false, false, null, false });

    vm4.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { lnPort });
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 2, true, 100, 400, false, false, null, false });
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Serial", 2, false, 100, 400, false, false, null, false });

    vm5.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { lnPort });

    vm7.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { nyPort });
    vm7.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Serial", 1, false, 100, 400, false, false, null, false });
    vm7.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 1, true, 100, 400, false, false, null, false });

    pause(10000);
    String command = CliStrings.LIST_GATEWAY;
    CommandResult cmdResult = executeCommand(command);
    
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testListGatewaySenderGatewayReceiver : " + strCmdResult );
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      
      TabularResultData tableSenderResultData = ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> senders = tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
      assertEquals(4, senders.size());
      List<String> hosts = tableSenderResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(4, hosts.size());
      
      
      TabularResultData tableReceiverResultData = ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER).retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> ports = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_PORT);
      assertEquals(1, ports.size());
      hosts = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(1, hosts.size());
    } else {
      fail("testListGatewaySenderGatewayReceiver failed as did not get CommandResult");
    }
  }
  
  public void testListGatewaySenderGatewayReceiver_group() {

    Integer lnPort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + lnPort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm6.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { nyPort });

    vm3.invoke(WANCommandTestBase.class, "createCacheWithGroups",
        new Object[] { lnPort, "Serial_Sender, Paralle_Sender"});
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Serial", 2, false, 100, 400, false, false, null, false });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 2, true, 100, 400, false, false, null, false });

    vm4.invoke(WANCommandTestBase.class, "createCacheWithGroups",
        new Object[] { lnPort,"Serial_Sender, Paralle_Sender"});
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 2, true, 100, 400, false, false, null, false });
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Serial", 2, false, 100, 400, false, false, null, false });

    vm5.invoke(WANCommandTestBase.class, "createAndStartReceiverWithGroup", new Object[] { lnPort, "Paralle_Sender,Receiver_Group" });
    vm5.invoke(WANCommandTestBase.class, "createSender", new Object[] {
      "ln_Parallel", 2, true, 100, 400, false, false, null, false });
    

    vm7.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { nyPort });
    vm7.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Serial", 1, false, 100, 400, false, false, null, false });
    vm7.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 1, true, 100, 400, false, false, null, false });

    pause(10000);
    String command = CliStrings.LIST_GATEWAY + " --" + CliStrings.LIST_GATEWAY__GROUP + "=Serial_Sender";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testListGatewaySenderGatewayReceiver_group : " + strCmdResult );
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      
      TabularResultData tableSenderResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> senders = tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
      assertEquals(4, senders.size());
      List<String> hosts = tableSenderResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(4, hosts.size());
      
    } else {
      fail("testListGatewaySenderGatewayReceiver_group failed as did not get CommandResult");
    }
    
    command = CliStrings.LIST_GATEWAY + " --" + CliStrings.LIST_GATEWAY__GROUP + "=Paralle_Sender";
    cmdResult = executeCommand(command);
    if (cmdResult != null) {
      TabularResultData tableSenderResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> senders = tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
      assertEquals(5, senders.size());
      
      TabularResultData tableReceiverResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER).retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> ports = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_PORT);
      assertEquals(1, ports.size());
      
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testListGatewaySenderGatewayReceiver_group : " + strCmdResult );
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySenderGatewayReceiver_group failed as did not get CommandResult");
    }
    
    command = CliStrings.LIST_GATEWAY + " --" + CliStrings.LIST_GATEWAY__GROUP + "=Receiver_Group";
    cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testListGatewaySenderGatewayReceiver_group : " + strCmdResult );
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      
      TabularResultData tableSenderResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> senders = tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
      assertEquals(1, senders.size());
      
      TabularResultData tableReceiverResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER).retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> ports = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_PORT);
      assertEquals(1, ports.size());
      
    } else {
      fail("testListGatewaySenderGatewayReceiver_group failed as did not get CommandResult");
    }

    command = CliStrings.LIST_GATEWAY + " --" + CliStrings.LIST_GATEWAY__GROUP + "=Serial_Sender,Paralle_Sender";
    cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testListGatewaySenderGatewayReceiver_group : " + strCmdResult );
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      
      TabularResultData tableSenderResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> senders = tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
      assertEquals(5, senders.size());
      
      TabularResultData tableReceiverResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER).retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> ports = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_PORT);
      assertEquals(1, ports.size());
    } else {
      fail("testListGatewaySenderGatewayReceiver_group failed as did not get CommandResult");
    }

    command = CliStrings.LIST_GATEWAY + " --" + CliStrings.LIST_GATEWAY__GROUP + "=Serial_Sender,Paralle_Sender,Receiver_Group";
    cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testListGatewaySenderGatewayReceiver_group : " + strCmdResult );
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      
      TabularResultData tableSenderResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> senders = tableSenderResultData.retrieveAllValues(CliStrings.RESULT_GATEWAY_SENDER_ID);
      assertEquals(5, senders.size());
      
      TabularResultData tableReceiverResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER).retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> ports = tableReceiverResultData.retrieveAllValues(CliStrings.RESULT_PORT);
      assertEquals(1, ports.size());
      
    } else {
      fail("testListGatewaySenderGatewayReceiver_group failed as did not get CommandResult");
    }
    
  }
}
