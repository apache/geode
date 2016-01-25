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
import com.gemstone.gemfire.management.internal.cli.result.CompositeResultData;
import com.gemstone.gemfire.management.internal.cli.result.TabularResultData;

public class WanCommandStatusDUnitTest extends WANCommandTestBase{
  
  private static final long serialVersionUID = 1L;

  public WanCommandStatusDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }
  
  public void testGatewaySenderStatus(){

    Integer lnPort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + lnPort + "]");
    createDefaultSetup(props);
    
    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm6.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { nyPort });

    vm3.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { lnPort });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Serial", 2, false, 100, 400, false, false, null, true });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 2, true, 100, 400, false, false, null, true});

    vm4.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { lnPort });
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] {
      "ln_Serial", 2, false, 100, 400, false, false, null, true});
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 2, true, 100, 400, false, false, null, true});
    
    vm5.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { lnPort });
    vm5.invoke(WANCommandTestBase.class, "createSender", new Object[] {
      "ln_Serial", 2, false, 100, 400, false, false, null, true});
    vm5.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 2, true, 100, 400, false, false, null, true});

    pause(10000);
    String command = CliStrings.STATUS_GATEWAYSENDER + " --"
    + CliStrings.STATUS_GATEWAYSENDER__ID + "=ln_Serial";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      TabularResultData tableResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(3, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_RUNNING));
      
      tableResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_NOT_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(2, result_hosts.size());
      
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testGatewaySenderStatus : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
    
    vm3.invoke(WANCommandTestBase.class, "startSender",
        new Object[] { "ln_Serial" });
    vm3.invoke(WANCommandTestBase.class, "startSender",
        new Object[] { "ln_Parallel" });

    vm4.invoke(WANCommandTestBase.class, "startSender",
        new Object[] { "ln_Serial" });
    vm4.invoke(WANCommandTestBase.class, "startSender",
        new Object[] { "ln_Parallel" });

    vm5.invoke(WANCommandTestBase.class, "startSender",
        new Object[] { "ln_Serial" });
    vm5.invoke(WANCommandTestBase.class, "startSender",
        new Object[] { "ln_Parallel" });

    pause(10000);
    command = CliStrings.STATUS_GATEWAYSENDER + " --"
    + CliStrings.STATUS_GATEWAYSENDER__ID + "=ln_Serial";
    cmdResult = executeCommand(command);
    if (cmdResult != null) {
      TabularResultData tableResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(3, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_NOT_RUNNING));
      
      tableResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_NOT_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(2, result_hosts.size());
      
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testGatewaySenderStatus : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
  }

  
  public void testGatewaySenderStatus_OnMember(){

    Integer lnPort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + lnPort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm6.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { nyPort });

    vm3.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { lnPort });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Serial", 2, false, 100, 400, false, false, null, true });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 2, true, 100, 400, false, false, null, true});

    vm4.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { lnPort });
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] {
      "ln_Serial", 2, false, 100, 400, false, false, null, true});
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 2, true, 100, 400, false, false, null, true});
    
    vm5.invoke(WANCommandTestBase.class, "createCache",
        new Object[] { lnPort });

    final DistributedMember vm1Member = (DistributedMember) vm3.invoke(
        WANCommandTestBase.class, "getMember");
    
    pause(10000);
    String command = CliStrings.STATUS_GATEWAYSENDER + " --"
    + CliStrings.STATUS_GATEWAYSENDER__ID + "=ln_Serial --"
        + CliStrings.STATUS_GATEWAYSENDER__MEMBER + "=" + vm1Member.getId();
    
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info("testGatewaySenderStatus_OnMember : " + strCmdResult + ">>>>> ");
      TabularResultData tableResultData =
          ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(1, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_RUNNING));
        
    
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
    
    vm3.invoke(WANCommandTestBase.class, "startSender",
        new Object[] { "ln_Serial" });
    vm3.invoke(WANCommandTestBase.class, "startSender",
        new Object[] { "ln_Parallel" });

    vm4.invoke(WANCommandTestBase.class, "startSender",
        new Object[] { "ln_Serial" });
    vm4.invoke(WANCommandTestBase.class, "startSender",
        new Object[] { "ln_Parallel" });

    pause(10000);
    command = CliStrings.STATUS_GATEWAYSENDER + " --"
    + CliStrings.STATUS_GATEWAYSENDER__ID + "=ln_Serial --"
        + CliStrings.STATUS_GATEWAYSENDER__MEMBER + "=" + vm1Member.getId();
    
    cmdResult = executeCommand(command);
    if (cmdResult != null) {
//      TabularResultData tableResultData =
//        (TabularResultData) cmdResult.getResultData();
//      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
//      assertEquals(1, result_Status.size());
//      assertFalse(result_Status.contains(CliStrings.GATEWAY_NOT_RUNNING));
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info("testGatewaySenderStatus_OnMember : " + strCmdResult + ">>>>> ");
      TabularResultData tableResultData =
          ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(1, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_NOT_RUNNING));
      
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
    
    final DistributedMember vm5Member = (DistributedMember) vm5.invoke(
        WANCommandTestBase.class, "getMember");
   
    command = CliStrings.STATUS_GATEWAYSENDER + " --"
    + CliStrings.STATUS_GATEWAYSENDER__ID + "=ln_Serial --"
        + CliStrings.STATUS_GATEWAYSENDER__MEMBER + "=" + vm5Member.getId();
    cmdResult = executeCommand(command);
    if (cmdResult != null) {
//      ErrorResultData errorResultData =
//        (ErrorResultData) cmdResult.getResultData();
      assertTrue(cmdResult != null);
      
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info("testGatewaySenderStatus_OnMember : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
  }
  
  public void testGatewaySenderStatus_OnGroups(){

    Integer lnPort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + lnPort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm7.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { nyPort });

    vm3.invoke(WANCommandTestBase.class, "createCacheWithGroups",
        new Object[] { lnPort, "Serial_Sender, Paralle_Sender"});
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Serial", 2, false, 100, 400, false, false, null, true });
    vm3.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 2, true, 100, 400, false, false, null, true});

    vm4.invoke(WANCommandTestBase.class, "createCacheWithGroups",
        new Object[] { lnPort,"Serial_Sender, Paralle_Sender"});
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] {
      "ln_Serial", 2, false, 100, 400, false, false, null, true});
    vm4.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 2, true, 100, 400, false, false, null, true});
    
    vm5.invoke(WANCommandTestBase.class, "createCacheWithGroups",
        new Object[] { lnPort,"Paralle_Sender"});
    vm5.invoke(WANCommandTestBase.class, "createSender", new Object[] {
      "ln_Serial", 2, false, 100, 400, false, false, null, true});
    vm5.invoke(WANCommandTestBase.class, "createSender", new Object[] {
        "ln_Parallel", 2, true, 100, 400, false, false, null, true});

    vm6.invoke(WANCommandTestBase.class, "createCacheWithGroups",
        new Object[] { lnPort,"Serial_Sender"});
    
    final DistributedMember vm1Member = (DistributedMember) vm3.invoke(
        WANCommandTestBase.class, "getMember");
    
    pause(10000);
    String command = CliStrings.STATUS_GATEWAYSENDER + " --"
    + CliStrings.STATUS_GATEWAYSENDER__ID + "=ln_Serial --" + CliStrings.STATUS_GATEWAYSENDER__GROUP + "=Serial_Sender";
    
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      TabularResultData tableResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(2, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_RUNNING));
      
      tableResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_NOT_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(1, result_hosts.size());
      
      
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testGatewaySenderStatus_OnGroups : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
    
    vm3.invoke(WANCommandTestBase.class, "startSender",
        new Object[] { "ln_Serial" });
    vm3.invoke(WANCommandTestBase.class, "startSender",
        new Object[] { "ln_Parallel" });

    vm4.invoke(WANCommandTestBase.class, "startSender",
        new Object[] { "ln_Serial" });
    vm4.invoke(WANCommandTestBase.class, "startSender",
        new Object[] { "ln_Parallel" });

    pause(10000);
    command = CliStrings.STATUS_GATEWAYSENDER + " --"
    + CliStrings.STATUS_GATEWAYSENDER__ID + "=ln_Serial --" + CliStrings.STATUS_GATEWAYSENDER__GROUP + "=Serial_Sender";
    
    cmdResult = executeCommand(command);
    if (cmdResult != null) {
      TabularResultData tableResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(2, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_NOT_RUNNING));
      
      tableResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_SENDER_NOT_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_SENDER);
      List<String> result_hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(1, result_hosts.size());
      
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testGatewaySenderStatus_OnGroups : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
    } else {
      fail("testListGatewaySender failed as did not get CommandResult");
    }
  }
  
  public void testGatewayReceiverStatus(){

    Integer lnPort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + lnPort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm6.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { nyPort });

    vm3.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { lnPort });
    vm4.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { lnPort });
    vm5.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { lnPort });
    
    pause(10000);
    String command = CliStrings.STATUS_GATEWAYRECEIVER; 
    CommandResult cmdResult = executeCommand(command);
    
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testGatewayReceiverStatus : " + strCmdResult + ">>>>> ");
      
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      
      TabularResultData tableResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(3, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_NOT_RUNNING));
      
      tableResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER_NOT_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> result_hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(2, result_hosts.size());
      
    } else {
      fail("testGatewayReceiverStatus failed as did not get CommandResult");
    }
    
    vm3.invoke(WANCommandTestBase.class, "stopReceiver");
    vm4.invoke(WANCommandTestBase.class, "stopReceiver");
    vm5.invoke(WANCommandTestBase.class, "stopReceiver");
    
    pause(10000);
    
    command = CliStrings.STATUS_GATEWAYRECEIVER; 
    cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testGatewayReceiverStatus : " + strCmdResult + ">>>>> ");
      
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      
      TabularResultData tableResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(3, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_RUNNING));
      
      tableResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER_NOT_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> result_hosts = tableResultData.retrieveAllValues(CliStrings.RESULT_HOST_MEMBER);
      assertEquals(2, result_hosts.size());
      
    } else {
      fail("testGatewayReceiverStatus failed as did not get CommandResult");
    }
  }
  
  public void testGatewayReceiverStatus_OnMember(){

    Integer lnPort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + lnPort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm6.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { nyPort });

    vm3.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { lnPort });
    vm4.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { lnPort });
    vm5.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { lnPort });
    
    final DistributedMember vm3Member = (DistributedMember) vm3.invoke(
        WANCommandTestBase.class, "getMember");
    
    pause(10000);
    String command = CliStrings.STATUS_GATEWAYRECEIVER+ " --"
    + CliStrings.STATUS_GATEWAYRECEIVER__MEMBER + "=" + vm3Member.getId();
    
    CommandResult cmdResult = executeCommand(command);
    
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info("testGatewayReceiverStatus : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      //TabularResultData tableResultData = (TabularResultData) cmdResult.getResultData();
      //List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      //assertEquals(1, result_Status.size());
      //assertFalse(strCmdResult.contains(CliStrings.GATEWAY_NOT_RUNNING));
      TabularResultData tableResultData =
          ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(1, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_NOT_RUNNING));
    } else {
      fail("testGatewayReceiverStatus failed as did not get CommandResult");
    }
    
    vm3.invoke(WANCommandTestBase.class, "stopReceiver");
    vm4.invoke(WANCommandTestBase.class, "stopReceiver");
    vm5.invoke(WANCommandTestBase.class, "stopReceiver");
    
    pause(10000);
    
    command = CliStrings.STATUS_GATEWAYRECEIVER+ " --"
    + CliStrings.STATUS_GATEWAYRECEIVER__MEMBER + "=" + vm3Member.getId();
    
    cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testGatewayReceiverStatus : " + strCmdResult + ">>>>> ");
      
//      TabularResultData tableResultData =
//        (TabularResultData) cmdResult.getResultData();
//      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
//      assertEquals(1, result_Status.size());
//      assertFalse(result_Status.contains(CliStrings.GATEWAY_RUNNING));
      
      TabularResultData tableResultData =
          ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(1, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_RUNNING));
    } else {
      fail("testGatewayReceiverStatus failed as did not get CommandResult");
    }
  }
  
  public void testGatewayReceiverStatus_OnGroups(){

    Integer lnPort = (Integer) vm1.invoke(WANCommandTestBase.class,
        "createFirstLocatorWithDSId", new Object[] { 1 });

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + lnPort + "]");
    createDefaultSetup(props);

    Integer nyPort = (Integer) vm2.invoke(WANCommandTestBase.class,
        "createFirstRemoteLocator", new Object[] { 2, lnPort });

    vm7.invoke(WANCommandTestBase.class, "createAndStartReceiver", new Object[] { nyPort });

    vm3.invoke(WANCommandTestBase.class, "createAndStartReceiverWithGroup", new Object[] { lnPort, "RG1, RG2" });
    vm4.invoke(WANCommandTestBase.class, "createAndStartReceiverWithGroup", new Object[] { lnPort, "RG1, RG2"  });
    vm5.invoke(WANCommandTestBase.class, "createAndStartReceiverWithGroup", new Object[] { lnPort, "RG1"  });
    vm6.invoke(WANCommandTestBase.class, "createAndStartReceiverWithGroup", new Object[] { lnPort, "RG2"  });
    
    pause(10000);
    String command = CliStrings.STATUS_GATEWAYRECEIVER + " --"
        + CliStrings.STATUS_GATEWAYRECEIVER__GROUP + "=RG1";
    
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testGatewayReceiverStatus : " + strCmdResult + ">>>>> ");
      
      assertEquals(Result.Status.OK, cmdResult.getStatus());
      
      TabularResultData tableResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(3, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_NOT_RUNNING));
      
    } else {
      fail("testGatewayReceiverStatus failed as did not get CommandResult");
    }
    
    vm3.invoke(WANCommandTestBase.class, "stopReceiver");
    vm4.invoke(WANCommandTestBase.class, "stopReceiver");
    vm5.invoke(WANCommandTestBase.class, "stopReceiver");

    pause(10000);
    command = CliStrings.STATUS_GATEWAYRECEIVER + " --"+ CliStrings.STATUS_GATEWAYRECEIVER__GROUP + "=RG1";
    
    cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      Log.getLogWriter().info(
          "testGatewayReceiverStatus_OnGroups : " + strCmdResult + ">>>>> ");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData tableResultData =
        ((CompositeResultData)cmdResult.getResultData()).retrieveSection(CliStrings.SECTION_GATEWAY_RECEIVER_AVAILABLE).retrieveTable(CliStrings.TABLE_GATEWAY_RECEIVER);
      
      List<String> result_Status = tableResultData.retrieveAllValues(CliStrings.RESULT_STATUS);
      assertEquals(3, result_Status.size());
      assertFalse(result_Status.contains(CliStrings.GATEWAY_RUNNING));
      
    } else {
      fail("testGatewayReceiverStatus failed as did not get CommandResult");
    }
  }
}
