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

import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.dunit.Assert.*;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;

/**
 * DUnit tests for 'create gateway-receiver' command.
 */
@Category(DistributedTest.class)
public class WanCommandCreateGatewayReceiverDUnitTest extends WANCommandTestBase {

  private static final long serialVersionUID = 1L;

  /**
   * GatewayReceiver with all default attributes
   */
  @Test
  public void testCreateGatewayReceiverWithDefault() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(() -> getLocatorPort());

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(2, punePort));

    vm3.invoke(() -> createCache(punePort));
    vm4.invoke(() -> createCache(punePort));
    vm5.invoke(() -> createCache(punePort));

    String command = CliStrings.CREATE_GATEWAYRECEIVER;
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(4, status.size());// expected size 4 includes the manager node
      // verify there is no error in the status
      for (int i = 0; i < status.size(); i++) {
        assertTrue("GatewayReceiver creation failed with: " + status.get(i),
            status.get(i).indexOf("ERROR:") == -1);
      }
    } else {
      fail("testCreateGatewayReceiver failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
        GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
        GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
        GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null));
  }

  /**
   * GatewayReceiver with given attributes
   */
  @Test
  public void testCreateGatewayReceiver() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(() -> getLocatorPort());

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(2, punePort));

    vm3.invoke(() -> createCache(punePort));
    vm4.invoke(() -> createCache(punePort));
    vm5.invoke(() -> createCache(punePort));

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(4, status.size());// expected size 4 includes the manager node
      // verify there is no error in the status
      for (int i = 0; i < status.size(); i++) {
        assertTrue("GatewayReceiver creation failed with: " + status.get(i),
            status.get(i).indexOf("ERROR:") == -1);
      }
    } else {
      fail("testCreateGatewayReceiver failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null));
  }

  /**
   * GatewayReceiver with given attributes and a single GatewayTransportFilter.
   */
  @Test
  public void testCreateGatewayReceiverWithGatewayTransportFilter() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(() -> getLocatorPort());

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(2, punePort));

    vm3.invoke(() -> createCache(punePort));
    vm4.invoke(() -> createCache(punePort));
    vm5.invoke(() -> createCache(punePort));

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=false" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER
            + "=org.apache.geode.cache30.MyGatewayTransportFilter1";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(4, status.size());// expected size 4 includes the manager node
      // verify there is no error in the status
      for (int i = 0; i < status.size(); i++) {
        assertTrue("GatewayReceiver creation failed with: " + status.get(i),
            status.get(i).indexOf("ERROR:") == -1);
      }
    } else {
      fail("testCreateGatewayReceiver failed as did not get CommandResult");
    }

    List<String> transportFilters = new ArrayList<String>();
    transportFilters.add("org.apache.geode.cache30.MyGatewayTransportFilter1");

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "localhost", 100000,
        512000, transportFilters));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "localhost", 100000,
        512000, transportFilters));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "localhost", 100000,
        512000, transportFilters));
  }

  /**
   * GatewayReceiver with given attributes and multiple GatewayTransportFilters.
   */
  @Test
  public void testCreateGatewayReceiverWithMultipleGatewayTransportFilters() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(() -> getLocatorPort());

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(2, punePort));

    vm3.invoke(() -> createCache(punePort));
    vm4.invoke(() -> createCache(punePort));
    vm5.invoke(() -> createCache(punePort));

    String command = CliStrings.CREATE_GATEWAYRECEIVER + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost" + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER
        + "=org.apache.geode.cache30.MyGatewayTransportFilter1,org.apache.geode.cache30.MyGatewayTransportFilter2";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(4, status.size());// expected size 4 includes the manager node
      // verify there is no error in the status
      for (int i = 0; i < status.size(); i++) {
        assertTrue("GatewayReceiver creation failed with: " + status.get(i),
            status.get(i).indexOf("ERROR:") == -1);
      }
    } else {
      fail("testCreateGatewayReceiver failed as did not get CommandResult");
    }

    List<String> transportFilters = new ArrayList<String>();
    transportFilters.add("org.apache.geode.cache30.MyGatewayTransportFilter1");
    transportFilters.add("org.apache.geode.cache30.MyGatewayTransportFilter2");

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        10000, 11000, "localhost", 100000, 512000, transportFilters));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        10000, 11000, "localhost", 100000, 512000, transportFilters));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        10000, 11000, "localhost", 100000, 512000, transportFilters));
  }

  /**
   * GatewayReceiver with given attributes. Error scenario where startPort is greater than endPort.
   */
  @Test
  public void testCreateGatewayReceiver_Error() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(() -> getLocatorPort());

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(2, punePort));

    vm3.invoke(() -> createCache(punePort));
    vm4.invoke(() -> createCache(punePort));
    vm5.invoke(() -> createCache(punePort));

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS
            + "=localhost" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(4, status.size());// expected size 4 includes the manager
                                     // node
      // verify there is no error in the status
      for (int i = 0; i < status.size(); i++) {
        assertTrue("GatewayReceiver creation should have failed",
            status.get(i).indexOf("ERROR:") != -1);
      }
    } else {
      fail("testCreateGatewayReceiver failed as did not get CommandResult");
    }
  }

  /**
   * GatewayReceiver with given attributes on the given member.
   */
  @Test
  public void testCreateGatewayReceiver_onMember() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(() -> getLocatorPort());

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(2, punePort));

    vm3.invoke(() -> createCache(punePort));
    vm4.invoke(() -> createCache(punePort));
    vm5.invoke(() -> createCache(punePort));

    final DistributedMember vm3Member = (DistributedMember) vm3.invoke(() -> getMember());

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MEMBER + "=" + vm3Member.getId();
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(1, status.size());
      // verify there is no error in the status
      for (int i = 0; i < status.size(); i++) {
        assertTrue("GatewayReceiver creation failed with: " + status.get(i),
            status.get(i).indexOf("ERROR:") == -1);
      }
    } else {
      fail("testCreateGatewayReceiver failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null));
  }

  /**
   * GatewayReceiver with given attributes on multiple members.
   */
  @Category(FlakyTest.class) // GEODE-1355
  @Test
  public void testCreateGatewayReceiver_onMultipleMembers() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(() -> getLocatorPort());

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(2, punePort));

    vm3.invoke(() -> createCache(punePort));
    vm4.invoke(() -> createCache(punePort));
    vm5.invoke(() -> createCache(punePort));

    final DistributedMember vm3Member = (DistributedMember) vm3.invoke(() -> getMember());
    final DistributedMember vm4Member = (DistributedMember) vm4.invoke(() -> getMember());

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MEMBER + "=" + vm3Member.getId() + ","
            + vm4Member.getId();
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(2, status.size());
      // verify there is no error in the status
      for (int i = 0; i < status.size(); i++) {
        assertTrue("GatewayReceiver creation failed with: " + status.get(i),
            status.get(i).indexOf("ERROR:") == -1);
      }
    } else {
      fail("testCreateGatewayReceiver failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null));
  }

  /**
   * GatewayReceiver with given attributes on the given group.
   */
  @Test
  public void testCreateGatewayReceiver_onGroup() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(() -> getLocatorPort());

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(2, punePort));

    vm3.invoke(() -> createCacheWithGroups(punePort, "receiverGroup1"));
    vm4.invoke(() -> createCacheWithGroups(punePort, "receiverGroup1"));
    vm5.invoke(() -> createCacheWithGroups(punePort, "receiverGroup1"));

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__GROUP + "=receiverGroup1";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(3, status.size());//
      // verify there is no error in the status
      for (int i = 0; i < status.size(); i++) {
        assertTrue("GatewayReceiver creation failed with: " + status.get(i),
            status.get(i).indexOf("ERROR:") == -1);
      }
    } else {
      fail("testCreateGatewayReceiver failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null));
  }

  /**
   * GatewayReceiver with given attributes on the given group. Only 2 of 3 members are part of the
   * group.
   */
  @Test
  public void testCreateGatewayReceiver_onGroup_Scenario2() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(() -> getLocatorPort());

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(2, punePort));

    vm3.invoke(() -> createCacheWithGroups(punePort, "receiverGroup1"));
    vm4.invoke(() -> createCacheWithGroups(punePort, "receiverGroup1"));
    vm5.invoke(() -> createCacheWithGroups(punePort, "receiverGroup2"));

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__GROUP + "=receiverGroup1";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(2, status.size());//
      // verify there is no error in the status
      for (int i = 0; i < status.size(); i++) {
        assertTrue("GatewayReceiver creation failed with: " + status.get(i),
            status.get(i).indexOf("ERROR:") == -1);
      }
    } else {
      fail("testCreateGatewayReceiver failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null));
  }

  /**
   * GatewayReceiver with given attributes on multiple groups.
   */
  @Test
  public void testCreateGatewayReceiver_onMultipleGroups() {

    VM puneLocator = Host.getLocator();
    int punePort = (Integer) puneLocator.invoke(() -> getLocatorPort());

    Properties props = getDistributedSystemProperties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "localhost[" + punePort + "]");
    setUpJmxManagerOnVm0ThenConnect(props);

    Integer nyPort = (Integer) vm2.invoke(() -> createFirstRemoteLocator(2, punePort));

    vm3.invoke(() -> createCacheWithGroups(punePort, "receiverGroup1"));
    vm4.invoke(() -> createCacheWithGroups(punePort, "receiverGroup1"));
    vm5.invoke(() -> createCacheWithGroups(punePort, "receiverGroup2"));

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__GROUP + "=receiverGroup1,receiverGroup2";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(3, status.size());//
      // verify there is no error in the status
      for (int i = 0; i < status.size(); i++) {
        assertTrue("GatewayReceiver creation failed with: " + status.get(i),
            status.get(i).indexOf("ERROR:") == -1);
      }
    } else {
      fail("testCreateGatewayReceiver failed as did not get CommandResult");
    }

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null));
  }

}
