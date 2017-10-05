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
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.internal.net.SocketCreator;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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

/**
 * DUnit tests for 'create gateway-receiver' command.
 */
@Category(DistributedTest.class)
public class CreateGatewayReceiverCommandDUnitTest extends WANCommandTestBase {

  private static final long serialVersionUID = 1L;

  /**
   * GatewayReceiver with all default attributes
   */
  @Test
  public void testCreateGatewayReceiverWithDefault() throws Exception {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);
    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));

    vm3.invoke(() -> createCache(dsIdPort));
    vm4.invoke(() -> createCache(dsIdPort));
    vm5.invoke(() -> createCache(dsIdPort));

    String command = CliStrings.CREATE_GATEWAYRECEIVER;
    executeCommandAndVerifyStatus(command, 4);

    // if neither bind-address or hostname-for-senders is set, profile
    // uses AcceptorImpl.getExternalAddress() to derive canonical hostname
    // when the Profile (and ServerLocation) are created
    String hostname = getHostName();

    vm3.invoke(() -> verifyGatewayReceiverProfile(hostname));
    vm4.invoke(() -> verifyGatewayReceiverProfile(hostname));
    vm5.invoke(() -> verifyGatewayReceiverProfile(hostname));

    vm3.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, hostname));
    vm4.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, hostname));
    vm5.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, hostname));

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
        GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null,
        GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
        GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null,
        GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
        GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null,
        GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
  }

  private String getHostName() throws Exception {
    return SocketCreator.getLocalHost().getCanonicalHostName();
  }

  private String getBindAddress() throws Exception {
    return InetAddress.getLocalHost().getHostAddress();
  }

  private void executeCommandAndVerifyStatus(String command, int numGatewayReceivers) {
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      // expected size of 4 includes the manager node when we don't set the receiver groups to
      // ignore it)
      assertEquals(numGatewayReceivers, status.size());
      // verify there is no error in the status
      for (String stat : status) {
        assertTrue("GatewayReceiver creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateGatewayReceiver failed as did not get CommandResult");
    }
  }

  /**
   * GatewayReceiver with given attributes
   */
  @Test
  public void testCreateGatewayReceiver() {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);
    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));

    vm3.invoke(() -> createCache(dsIdPort));
    vm4.invoke(() -> createCache(dsIdPort));
    vm5.invoke(() -> createCache(dsIdPort));

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000";
    executeCommandAndVerifyStatus(command, 4);

    // cannot verify Profile/ServerLocation when manualStart is true

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
  }

  /**
   * GatewayReceiver with hostnameForSenders
   */
  @Test
  public void testCreateGatewayReceiverWithHostnameForSenders() throws Exception {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);
    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));

    vm3.invoke(() -> createCache(dsIdPort));
    vm4.invoke(() -> createCache(dsIdPort));
    vm5.invoke(() -> createCache(dsIdPort));

    String hostnameForSenders = getHostName();
    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=false" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__HOSTNAMEFORSENDERS + "="
            + hostnameForSenders + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000";
    executeCommandAndVerifyStatus(command, 4);

    // verify hostname-for-senders is used when configured
    vm3.invoke(() -> verifyGatewayReceiverProfile(hostnameForSenders));
    vm4.invoke(() -> verifyGatewayReceiverProfile(hostnameForSenders));
    vm5.invoke(() -> verifyGatewayReceiverProfile(hostnameForSenders));

    vm3.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, hostnameForSenders));
    vm4.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, hostnameForSenders));
    vm5.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, hostnameForSenders));

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "", 100000, 512000,
        null, hostnameForSenders));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "", 100000, 512000,
        null, hostnameForSenders));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "", 100000, 512000,
        null, hostnameForSenders));
  }

  /**
   * GatewayReceiver with all default attributes and bind-address in gemfire-properties
   */
  @Test
  public void testCreateGatewayReceiverWithDefaultAndBindProperty() throws Exception {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);
    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));

    String expectedBindAddress = getBindAddress();

    String receiverGroup = "receiverGroup";
    vm3.invoke(() -> createCacheWithBindAddress(dsIdPort, expectedBindAddress, receiverGroup));
    vm4.invoke(() -> createCacheWithBindAddress(dsIdPort, expectedBindAddress, receiverGroup));
    vm5.invoke(() -> createCacheWithBindAddress(dsIdPort, expectedBindAddress, receiverGroup));

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.GROUP + "=" + receiverGroup;
    executeCommandAndVerifyStatus(command, 3);

    // verify bind-address used when provided as a gemfire property
    vm3.invoke(() -> verifyGatewayReceiverProfile(expectedBindAddress));
    vm4.invoke(() -> verifyGatewayReceiverProfile(expectedBindAddress));
    vm5.invoke(() -> verifyGatewayReceiverProfile(expectedBindAddress));

    vm3.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, expectedBindAddress));
    vm4.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, expectedBindAddress));
    vm5.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, expectedBindAddress));

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
        GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null,
        GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
        GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null,
        GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
        GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null,
        GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
  }

  /**
   * GatewayReceiver with all default attributes and server-bind-address in the gemfire properties
   */
  @Test
  public void testCreateGatewayReceiverWithDefaultsAndServerBindAddressProperty() throws Exception {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);
    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));

    String expectedBindAddress = getBindAddress();

    String receiverGroup = "receiverGroup";
    vm3.invoke(
        () -> createCacheWithServerBindAddress(dsIdPort, expectedBindAddress, receiverGroup));
    vm4.invoke(
        () -> createCacheWithServerBindAddress(dsIdPort, expectedBindAddress, receiverGroup));
    vm5.invoke(
        () -> createCacheWithServerBindAddress(dsIdPort, expectedBindAddress, receiverGroup));

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.GROUP + "=" + receiverGroup;
    executeCommandAndVerifyStatus(command, 3);

    // verify server-bind-address used if provided as a gemfire property
    vm3.invoke(() -> verifyGatewayReceiverProfile(expectedBindAddress));
    vm4.invoke(() -> verifyGatewayReceiverProfile(expectedBindAddress));
    vm5.invoke(() -> verifyGatewayReceiverProfile(expectedBindAddress));

    vm3.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, expectedBindAddress));
    vm4.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, expectedBindAddress));
    vm5.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, expectedBindAddress));

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
        GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null,
        GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
        GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null,
        GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
        GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null,
        GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
  }

  /**
   * GatewayReceiver with all default attributes and server-bind-address in the gemfire properties
   */
  @Test
  public void testCreateGatewayReceiverWithDefaultsAndMultipleBindAddressProperties()
      throws Exception {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);
    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));

    String extraBindAddress = "localhost";
    String expectedBindAddress = getBindAddress();
    String receiverGroup = "receiverGroup";

    vm3.invoke(() -> createCacheWithMultipleBindAddressProperties(dsIdPort, extraBindAddress,
        expectedBindAddress, receiverGroup));
    vm4.invoke(() -> createCacheWithMultipleBindAddressProperties(dsIdPort, extraBindAddress,
        expectedBindAddress, receiverGroup));
    vm5.invoke(() -> createCacheWithMultipleBindAddressProperties(dsIdPort, extraBindAddress,
        expectedBindAddress, receiverGroup));

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.GROUP + "=" + receiverGroup;
    executeCommandAndVerifyStatus(command, 3);

    // verify server-bind-address used if provided as a gemfire property
    vm3.invoke(() -> verifyGatewayReceiverProfile(expectedBindAddress));
    vm4.invoke(() -> verifyGatewayReceiverProfile(expectedBindAddress));
    vm5.invoke(() -> verifyGatewayReceiverProfile(expectedBindAddress));

    vm3.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, expectedBindAddress));
    vm4.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, expectedBindAddress));
    vm5.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, expectedBindAddress));

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
        GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null,
        GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
        GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null,
        GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
        GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
        GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null,
        GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
  }


  /**
   * GatewayReceiver with hostnameForSenders
   */
  @Test
  public void testCreateGatewayReceiverWithHostnameForSendersAndServerBindAddressProperty()
      throws Exception {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);
    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));

    String hostnameForSenders = getHostName();
    String serverBindAddress = getBindAddress();

    String receiverGroup = "receiverGroup";
    vm3.invoke(() -> createCacheWithServerBindAddress(dsIdPort, serverBindAddress, receiverGroup));
    vm4.invoke(() -> createCacheWithServerBindAddress(dsIdPort, serverBindAddress, receiverGroup));
    vm5.invoke(() -> createCacheWithServerBindAddress(dsIdPort, serverBindAddress, receiverGroup));

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=false" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__HOSTNAMEFORSENDERS + "="
            + hostnameForSenders + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.GROUP + "=" + receiverGroup;
    executeCommandAndVerifyStatus(command, 3);

    // verify server-bind-address takes precedence over hostname-for-senders
    vm3.invoke(() -> verifyGatewayReceiverProfile(hostnameForSenders));
    vm4.invoke(() -> verifyGatewayReceiverProfile(hostnameForSenders));
    vm5.invoke(() -> verifyGatewayReceiverProfile(hostnameForSenders));

    vm3.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, hostnameForSenders));
    vm4.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, hostnameForSenders));
    vm5.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, hostnameForSenders));

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "", 100000, 512000,
        null, hostnameForSenders));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "", 100000, 512000,
        null, hostnameForSenders));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "", 100000, 512000,
        null, hostnameForSenders));
  }

  /**
   * GatewayReceiver with hostnameForSenders
   */
  @Test
  public void testCreateGatewayReceiverWithHostnameForSendersAndBindAddressProperty()
      throws Exception {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);
    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));

    String hostnameForSenders = getHostName();
    String expectedBindAddress = getBindAddress();

    String receiverGroup = "receiverGroup";
    vm3.invoke(() -> createCacheWithBindAddress(dsIdPort, expectedBindAddress, receiverGroup));
    vm4.invoke(() -> createCacheWithBindAddress(dsIdPort, expectedBindAddress, receiverGroup));
    vm5.invoke(() -> createCacheWithBindAddress(dsIdPort, expectedBindAddress, receiverGroup));

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=false" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__HOSTNAMEFORSENDERS + "="
            + hostnameForSenders + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.GROUP + "=" + receiverGroup;
    executeCommandAndVerifyStatus(command, 3);

    vm3.invoke(() -> verifyGatewayReceiverProfile(hostnameForSenders));
    vm4.invoke(() -> verifyGatewayReceiverProfile(hostnameForSenders));
    vm5.invoke(() -> verifyGatewayReceiverProfile(hostnameForSenders));

    vm3.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, hostnameForSenders));
    vm4.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, hostnameForSenders));
    vm5.invoke(() -> verifyGatewayReceiverServerLocations(dsIdPort, hostnameForSenders));

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "", 100000, 512000,
        null, hostnameForSenders));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "", 100000, 512000,
        null, hostnameForSenders));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "", 100000, 512000,
        null, hostnameForSenders));
  }

  /**
   * GatewayReceiver with given attributes and a single GatewayTransportFilter.
   */
  @Test
  public void testCreateGatewayReceiverWithGatewayTransportFilter() {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);
    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));

    vm3.invoke(() -> createCache(dsIdPort));
    vm4.invoke(() -> createCache(dsIdPort));
    vm5.invoke(() -> createCache(dsIdPort));

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=false" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER
            + "=org.apache.geode.cache30.MyGatewayTransportFilter1";
    executeCommandAndVerifyStatus(command, 4);
    List<String> transportFilters = new ArrayList<String>();
    transportFilters.add("org.apache.geode.cache30.MyGatewayTransportFilter1");

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "localhost", 100000,
        512000, transportFilters, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "localhost", 100000,
        512000, transportFilters, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(true, 10000, 11000, "localhost", 100000,
        512000, transportFilters, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
  }

  /**
   * GatewayReceiver with given attributes and multiple GatewayTransportFilters.
   */
  @Test
  public void testCreateGatewayReceiverWithMultipleGatewayTransportFilters() {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);
    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));

    vm3.invoke(() -> createCache(dsIdPort));
    vm4.invoke(() -> createCache(dsIdPort));
    vm5.invoke(() -> createCache(dsIdPort));

    String command = CliStrings.CREATE_GATEWAYRECEIVER + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost" + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER
        + "=org.apache.geode.cache30.MyGatewayTransportFilter1,org.apache.geode.cache30.MyGatewayTransportFilter2";
    executeCommandAndVerifyStatus(command, 4);
    List<String> transportFilters = new ArrayList<String>();
    transportFilters.add("org.apache.geode.cache30.MyGatewayTransportFilter1");
    transportFilters.add("org.apache.geode.cache30.MyGatewayTransportFilter2");

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        10000, 11000, "localhost", 100000, 512000, transportFilters,
        GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        10000, 11000, "localhost", 100000, 512000, transportFilters,
        GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
        10000, 11000, "localhost", 100000, 512000, transportFilters,
        GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
  }

  /**
   * GatewayReceiver with given attributes. Error scenario where startPort is greater than endPort.
   */
  @Test
  public void testCreateGatewayReceiver_Error() {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);
    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));

    vm3.invoke(() -> createCache(dsIdPort));
    vm4.invoke(() -> createCache(dsIdPort));
    vm5.invoke(() -> createCache(dsIdPort));

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
      for (String stat : status) {
        assertTrue("GatewayReceiver creation should have failed", stat.contains("ERROR:"));
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
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);
    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));

    vm3.invoke(() -> createCache(dsIdPort));
    vm4.invoke(() -> createCache(dsIdPort));
    vm5.invoke(() -> createCache(dsIdPort));

    final DistributedMember vm3Member = vm3.invoke(this::getMember);

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.MEMBER + "=" + vm3Member.getId();
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(1, status.size());
      // verify there is no error in the status
      for (String stat : status) {
        assertTrue("GatewayReceiver creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateGatewayReceiver failed as did not get CommandResult");
    }

    // cannot verify Profile/ServerLocation when manualStart is true

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
  }

  /**
   * GatewayReceiver with given attributes on multiple members.
   */
  @Category(FlakyTest.class) // GEODE-1355
  @Test
  public void testCreateGatewayReceiver_onMultipleMembers() {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);
    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));

    vm3.invoke(() -> createCache(dsIdPort));
    vm4.invoke(() -> createCache(dsIdPort));
    vm5.invoke(() -> createCache(dsIdPort));

    final DistributedMember vm3Member = vm3.invoke(this::getMember);
    final DistributedMember vm4Member = vm4.invoke(this::getMember);

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.MEMBER + "=" + vm3Member.getId() + "," + vm4Member.getId();
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(2, status.size());
      // verify there is no error in the status
      for (String stat : status) {
        assertTrue("GatewayReceiver creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateGatewayReceiver failed as did not get CommandResult");
    }

    // cannot verify Profile/ServerLocation when manualStart is true

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
  }

  /**
   * GatewayReceiver with given attributes on the given group.
   */
  @Test
  public void testCreateGatewayReceiver_onGroup() {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);
    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));

    vm3.invoke(() -> createCacheWithGroups(dsIdPort, "receiverGroup1"));
    vm4.invoke(() -> createCacheWithGroups(dsIdPort, "receiverGroup1"));
    vm5.invoke(() -> createCacheWithGroups(dsIdPort, "receiverGroup1"));

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.GROUP + "=receiverGroup1";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(3, status.size());//
      // verify there is no error in the status
      for (String stat : status) {
        assertTrue("GatewayReceiver creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateGatewayReceiver failed as did not get CommandResult");
    }

    // cannot verify Profile/ServerLocation when manualStart is true

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
  }

  /**
   * GatewayReceiver with given attributes on the given group. Only 2 of 3 members are part of the
   * group.
   */
  @Test
  public void testCreateGatewayReceiver_onGroup_Scenario2() {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);
    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));

    vm3.invoke(() -> createCacheWithGroups(dsIdPort, "receiverGroup1"));
    vm4.invoke(() -> createCacheWithGroups(dsIdPort, "receiverGroup1"));
    vm5.invoke(() -> createCacheWithGroups(dsIdPort, "receiverGroup2"));

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.GROUP + "=receiverGroup1";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(2, status.size());//
      // verify there is no error in the status
      for (String stat : status) {
        assertTrue("GatewayReceiver creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateGatewayReceiver failed as did not get CommandResult");
    }

    // cannot verify Profile/ServerLocation when manualStart is true

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
  }

  /**
   * GatewayReceiver with given attributes on multiple groups.
   */
  @Test
  public void testCreateGatewayReceiver_onMultipleGroups() {
    VM puneLocator = Host.getLocator();
    int dsIdPort = puneLocator.invoke(this::getLocatorPort);
    propsSetUp(dsIdPort);
    vm2.invoke(() -> createFirstRemoteLocator(2, dsIdPort));

    vm3.invoke(() -> createCacheWithGroups(dsIdPort, "receiverGroup1"));
    vm4.invoke(() -> createCacheWithGroups(dsIdPort, "receiverGroup1"));
    vm5.invoke(() -> createCacheWithGroups(dsIdPort, "receiverGroup2"));

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.GROUP + "=receiverGroup1,receiverGroup2";
    CommandResult cmdResult = executeCommand(command);
    if (cmdResult != null) {
      String strCmdResult = commandResultToString(cmdResult);
      getLogWriter().info("testCreateGatewayReceiver stringResult : " + strCmdResult + ">>>>");
      assertEquals(Result.Status.OK, cmdResult.getStatus());

      TabularResultData resultData = (TabularResultData) cmdResult.getResultData();
      List<String> status = resultData.retrieveAllValues("Status");
      assertEquals(3, status.size());//
      // verify there is no error in the status
      for (String stat : status) {
        assertTrue("GatewayReceiver creation failed with: " + stat, !stat.contains("ERROR:"));
      }
    } else {
      fail("testCreateGatewayReceiver failed as did not get CommandResult");
    }

    // cannot verify Profile/ServerLocation when manualStart is true

    vm3.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm4.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
    vm5.invoke(() -> verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000,
        512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS));
  }
}
