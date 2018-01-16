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

import static org.apache.geode.distributed.ConfigurationProperties.BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_BIND_ADDRESS;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.getMemberIdCallable;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifyGatewayReceiverProfile;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifyGatewayReceiverServerLocations;
import static org.apache.geode.internal.cache.wan.wancommand.WANCommandUtils.verifyReceiverCreationWithAttributes;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.GROUP;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;

/**
 * DUnit tests for 'create gateway-receiver' command.
 */
@Category(DistributedTest.class)
public class CreateGatewayReceiverCommandDUnitTest {
  private MemberVM locatorSite1;
  private MemberVM server1, server2, server3;

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void before() throws Exception {
    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    locatorSite1 = clusterStartupRule.startLocatorVM(1, props);

    // Connect Gfsh to locator.
    gfsh.connectAndVerify(locatorSite1);
  }

  private String getHostName() throws Exception {
    return SocketCreator.getLocalHost().getCanonicalHostName();
  }

  private String getBindAddress() throws Exception {
    return InetAddress.getLocalHost().getHostAddress();
  }

  private MemberVM startServerWithGroups(int index, String groups, int locPort) throws Exception {
    Properties props = new Properties();
    props.setProperty(GROUPS, groups);
    return clusterStartupRule.startServerVM(index, props, locPort);
  }

  /**
   * GatewayReceiver with given attributes. Error scenario where the user tries to create more than
   * one receiver per member.
   */
  @Test
  public void testCreateGatewayReceiverErrorWhenGatewayReceiverAlreadyExists() throws Exception {
    Integer locator1Port = locatorSite1.getPort();
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, locator1Port);

    // Initial Creation should succeed
    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS
            + "=localhost" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000";
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3", "server-4", "server-5")
        .tableHasColumnWithValuesContaining("Status",
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");

    VMProvider.invokeInEveryMember(() -> {
      verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START, 10000, 11000,
          "localhost", 100000, 512000, null, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS);
    }, server1, server2, server3);

    // This should fail as there's already a gateway receiver created on the member.
    gfsh.executeAndAssertThat(command).statusIsError()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3", "server-4", "server-5")
        .tableHasColumnWithExactValuesInAnyOrder("Status",
            "ERROR: java.lang.IllegalStateException: A Gateway Receiver already exists on this member.",
            "ERROR: java.lang.IllegalStateException: A Gateway Receiver already exists on this member.",
            "ERROR: java.lang.IllegalStateException: A Gateway Receiver already exists on this member.");
  }

  /**
   * GatewayReceiver with all default attributes
   */
  @Test
  public void testCreateGatewayReceiverWithDefault() throws Exception {
    Integer locator1Port = locatorSite1.getPort();
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, locator1Port);

    // Default attributes.
    String command = CliStrings.CREATE_GATEWAYRECEIVER;
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3", "server-4", "server-5")
        .tableHasColumnWithValuesContaining("Status",
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");

    // If neither bind-address or hostname-for-senders is set, profile
    // uses AcceptorImpl.getExternalAddress() to derive canonical hostname
    // when the Profile (and ServerLocation) are created
    String hostname = getHostName();

    VMProvider.invokeInEveryMember(() -> {
      verifyGatewayReceiverProfile(hostname);
      verifyGatewayReceiverServerLocations(locator1Port, hostname);
      verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
          GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
          GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
          GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null,
          GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS);
    }, server1, server2, server3);
  }

  /**
   * GatewayReceiver with given attributes
   */
  @Test
  public void testCreateGatewayReceiver() throws Exception {
    Integer locator1Port = locatorSite1.getPort();
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, locator1Port);

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000";
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3", "server-4", "server-5")
        .tableHasColumnWithValuesContaining("Status",
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");

    VMProvider.invokeInEveryMember(() -> {
      verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000, 512000, null,
          GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS);
    }, server1, server2, server3);
  }

  /**
   * GatewayReceiver with hostnameForSenders
   */
  @Test
  public void testCreateGatewayReceiverWithHostnameForSenders() throws Exception {
    Integer locator1Port = locatorSite1.getPort();
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, locator1Port);

    String hostnameForSenders = getHostName();
    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=false" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__HOSTNAMEFORSENDERS + "="
            + hostnameForSenders + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000";
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3", "server-4", "server-5")
        .tableHasColumnWithValuesContaining("Status",
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");

    VMProvider.invokeInEveryMember(() -> {
      // verify hostname-for-senders is used when configured
      verifyGatewayReceiverProfile(hostnameForSenders);
      verifyGatewayReceiverServerLocations(locator1Port, hostnameForSenders);
      verifyReceiverCreationWithAttributes(true, 10000, 11000, "", 100000, 512000, null,
          hostnameForSenders);
    }, server1, server2, server3);
  }

  /**
   * GatewayReceiver with all default attributes and bind-address in gemfire-properties
   */
  @Test
  public void testCreateGatewayReceiverWithDefaultAndBindProperty() throws Exception {
    String receiverGroup = "receiverGroup";
    Integer locator1Port = locatorSite1.getPort();
    String expectedBindAddress = getBindAddress();

    Properties props = new Properties();
    props.setProperty(GROUPS, receiverGroup);
    props.setProperty(BIND_ADDRESS, expectedBindAddress);

    server1 = clusterStartupRule.startServerVM(3, props, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, props, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, props, locator1Port);

    String command = CliStrings.CREATE_GATEWAYRECEIVER + " --" + GROUP + "=" + receiverGroup;
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3", "server-4", "server-5")
        .tableHasColumnWithValuesContaining("Status",
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");

    VMProvider.invokeInEveryMember(() -> {
      // verify bind-address used when provided as a gemfire property
      verifyGatewayReceiverProfile(expectedBindAddress);
      verifyGatewayReceiverServerLocations(locator1Port, expectedBindAddress);
      verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
          GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
          GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
          GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null,
          GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS);
    }, server1, server2, server3);
  }

  /**
   * GatewayReceiver with all default attributes and server-bind-address in the gemfire properties
   */
  @Test
  public void testCreateGatewayReceiverWithDefaultsAndServerBindAddressProperty() throws Exception {
    String receiverGroup = "receiverGroup";
    Integer locator1Port = locatorSite1.getPort();
    String expectedBindAddress = getBindAddress();

    Properties props = new Properties();
    props.setProperty(GROUPS, receiverGroup);
    props.setProperty(SERVER_BIND_ADDRESS, expectedBindAddress);

    server1 = clusterStartupRule.startServerVM(3, props, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, props, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, props, locator1Port);

    String command = CliStrings.CREATE_GATEWAYRECEIVER + " --" + GROUP + "=" + receiverGroup;
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3", "server-4", "server-5")
        .tableHasColumnWithValuesContaining("Status",
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");

    VMProvider.invokeInEveryMember(() -> {
      // verify server-bind-address used if provided as a gemfire property
      verifyGatewayReceiverProfile(expectedBindAddress);
      verifyGatewayReceiverServerLocations(locator1Port, expectedBindAddress);
      verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
          GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
          GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
          GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null,
          GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS);
    }, server1, server2, server3);
  }

  /**
   * GatewayReceiver with all default attributes and server-bind-address in the gemfire properties
   */
  @Test
  public void testCreateGatewayReceiverWithDefaultsAndMultipleBindAddressProperties()
      throws Exception {
    String extraBindAddress = "localhost";
    String receiverGroup = "receiverGroup";
    Integer locator1Port = locatorSite1.getPort();
    String expectedBindAddress = getBindAddress();

    Properties props = new Properties();
    props.setProperty(GROUPS, receiverGroup);
    props.setProperty(BIND_ADDRESS, extraBindAddress);
    props.setProperty(SERVER_BIND_ADDRESS, expectedBindAddress);

    server1 = clusterStartupRule.startServerVM(3, props, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, props, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, props, locator1Port);

    String command = CliStrings.CREATE_GATEWAYRECEIVER + " --" + GROUP + "=" + receiverGroup;
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3", "server-4", "server-5")
        .tableHasColumnWithValuesContaining("Status",
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");

    VMProvider.invokeInEveryMember(() -> {
      // verify server-bind-address used if provided as a gemfire property
      verifyGatewayReceiverProfile(expectedBindAddress);
      verifyGatewayReceiverServerLocations(locator1Port, expectedBindAddress);
      verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START,
          GatewayReceiver.DEFAULT_START_PORT, GatewayReceiver.DEFAULT_END_PORT,
          GatewayReceiver.DEFAULT_BIND_ADDRESS, GatewayReceiver.DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS,
          GatewayReceiver.DEFAULT_SOCKET_BUFFER_SIZE, null,
          GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS);
    }, server1, server2, server3);
  }

  /**
   * GatewayReceiver with hostnameForSenders
   */
  @Test
  public void testCreateGatewayReceiverWithHostnameForSendersAndServerBindAddressProperty()
      throws Exception {
    String receiverGroup = "receiverGroup";
    String hostnameForSenders = getHostName();
    String serverBindAddress = getBindAddress();
    Integer locator1Port = locatorSite1.getPort();

    Properties props = new Properties();
    props.setProperty(GROUPS, receiverGroup);
    props.setProperty(SERVER_BIND_ADDRESS, serverBindAddress);

    server1 = clusterStartupRule.startServerVM(3, props, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, props, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, props, locator1Port);

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=false" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__HOSTNAMEFORSENDERS + "="
            + hostnameForSenders + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --" + GROUP + "="
            + receiverGroup;
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3", "server-4", "server-5")
        .tableHasColumnWithValuesContaining("Status",
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");

    VMProvider.invokeInEveryMember(() -> {
      // verify server-bind-address takes precedence over hostname-for-senders
      verifyGatewayReceiverProfile(hostnameForSenders);
      verifyGatewayReceiverServerLocations(locator1Port, hostnameForSenders);
      verifyReceiverCreationWithAttributes(true, 10000, 11000, "", 100000, 512000, null,
          hostnameForSenders);
    }, server1, server2, server3);
  }

  /**
   * GatewayReceiver with hostnameForSenders
   */
  @Test
  public void testCreateGatewayReceiverWithHostnameForSendersAndBindAddressProperty()
      throws Exception {
    String receiverGroup = "receiverGroup";
    String hostnameForSenders = getHostName();
    Integer locator1Port = locatorSite1.getPort();
    String expectedBindAddress = getBindAddress();

    Properties props = new Properties();
    props.setProperty(GROUPS, receiverGroup);
    props.setProperty(BIND_ADDRESS, expectedBindAddress);

    server1 = clusterStartupRule.startServerVM(3, props, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, props, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, props, locator1Port);

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=false" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__HOSTNAMEFORSENDERS + "="
            + hostnameForSenders + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --" + GROUP + "="
            + receiverGroup;
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3", "server-4", "server-5")
        .tableHasColumnWithValuesContaining("Status",
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");

    VMProvider.invokeInEveryMember(() -> {
      verifyGatewayReceiverProfile(hostnameForSenders);
      verifyGatewayReceiverServerLocations(locator1Port, hostnameForSenders);
      verifyReceiverCreationWithAttributes(true, 10000, 11000, "", 100000, 512000, null,
          hostnameForSenders);
    }, server1, server2, server3);
  }

  /**
   * GatewayReceiver with given attributes and a single GatewayTransportFilter.
   */
  @Test
  public void testCreateGatewayReceiverWithGatewayTransportFilter() throws Exception {
    Integer locator1Port = locatorSite1.getPort();
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, locator1Port);

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=false" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER
            + "=org.apache.geode.cache30.MyGatewayTransportFilter1";
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3", "server-4", "server-5")
        .tableHasColumnWithValuesContaining("Status",
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");

    List<String> transportFilters = new ArrayList<>();
    transportFilters.add("org.apache.geode.cache30.MyGatewayTransportFilter1");
    VMProvider.invokeInEveryMember(() -> {
      verifyReceiverCreationWithAttributes(true, 10000, 11000, "localhost", 100000, 512000,
          transportFilters, GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS);
    }, server1, server2, server3);
  }

  /**
   * GatewayReceiver with given attributes and multiple GatewayTransportFilters.
   */
  @Test
  public void testCreateGatewayReceiverWithMultipleGatewayTransportFilters() throws Exception {
    Integer locator1Port = locatorSite1.getPort();
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, locator1Port);

    String command = CliStrings.CREATE_GATEWAYRECEIVER + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost" + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
        + CliStrings.CREATE_GATEWAYRECEIVER__GATEWAYTRANSPORTFILTER
        + "=org.apache.geode.cache30.MyGatewayTransportFilter1,org.apache.geode.cache30.MyGatewayTransportFilter2";
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3", "server-4", "server-5")
        .tableHasColumnWithValuesContaining("Status",
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");

    List<String> transportFilters = new ArrayList<>();
    transportFilters.add("org.apache.geode.cache30.MyGatewayTransportFilter1");
    transportFilters.add("org.apache.geode.cache30.MyGatewayTransportFilter2");

    VMProvider.invokeInEveryMember(() -> {
      verifyReceiverCreationWithAttributes(!GatewayReceiver.DEFAULT_MANUAL_START, 10000, 11000,
          "localhost", 100000, 512000, transportFilters,
          GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS);
    }, server1, server2, server3);
  }

  /**
   * GatewayReceiver with given attributes on the given member.
   */
  @Test
  public void testCreateGatewayReceiverOnSingleMember() throws Exception {
    Integer locator1Port = locatorSite1.getPort();
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, locator1Port);
    final DistributedMember server1Member = server1.invoke(getMemberIdCallable());

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.MEMBER + "=" + server1Member.getId();
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3")
        .tableHasColumnWithValuesContaining("Status",
            "GatewayReceiver created on member \"server-3\"");

    VMProvider.invokeInEveryMember(() -> {
      verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000, 512000, null,
          GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS);
    }, server1);

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getGatewayReceivers()).isEmpty();
    }, server2, server3);
  }

  /**
   * GatewayReceiver with given attributes on multiple members.
   */
  @Test
  public void testCreateGatewayReceiverOnMultipleMembers() throws Exception {
    Integer locator1Port = locatorSite1.getPort();
    server1 = clusterStartupRule.startServerVM(3, locator1Port);
    server2 = clusterStartupRule.startServerVM(4, locator1Port);
    server3 = clusterStartupRule.startServerVM(5, locator1Port);
    final DistributedMember server1Member = server1.invoke(getMemberIdCallable());
    final DistributedMember server2Member = server2.invoke(getMemberIdCallable());

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --"
            + CliStrings.MEMBER + "=" + server1Member.getId() + "," + server2Member.getId();
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3", "server-4")
        .tableHasColumnWithValuesContaining("Status",
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"");

    VMProvider.invokeInEveryMember(() -> {
      verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000, 512000, null,
          GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS);
    }, server1, server2);

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getGatewayReceivers()).isEmpty();
    }, server3);
  }

  /**
   * GatewayReceiver with given attributes on the given group.
   */
  @Test
  public void testCreateGatewayReceiverOnGroup() throws Exception {
    String groups = "receiverGroup1";
    Integer locator1Port = locatorSite1.getPort();
    server1 = startServerWithGroups(3, groups, locator1Port);
    server2 = startServerWithGroups(4, groups, locator1Port);
    server3 = startServerWithGroups(5, groups, locator1Port);

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --" + GROUP
            + "=receiverGroup1";
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3", "server-4", "server-5")
        .tableHasColumnWithValuesContaining("Status",
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");

    VMProvider.invokeInEveryMember(() -> {
      verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000, 512000, null,
          GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS);
    }, server1, server2, server3);
  }

  /**
   * GatewayReceiver with given attributes on the given group. Only 2 of 3 members are part of the
   * group.
   */
  @Test
  public void testCreateGatewayReceiverOnGroupScenario2() throws Exception {
    String group1 = "receiverGroup1";
    String group2 = "receiverGroup2";
    Integer locator1Port = locatorSite1.getPort();
    server1 = startServerWithGroups(3, group1, locator1Port);
    server2 = startServerWithGroups(4, group1, locator1Port);
    server3 = startServerWithGroups(5, group2, locator1Port);

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --" + GROUP
            + "=receiverGroup1";
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3", "server-4")
        .tableHasColumnWithValuesContaining("Status",
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"");

    VMProvider.invokeInEveryMember(() -> {
      verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000, 512000, null,
          GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS);
    }, server1, server2);

    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache.getGatewayReceivers()).isEmpty();
    }, server3);
  }

  /**
   * GatewayReceiver with given attributes on multiple groups.
   */
  @Test
  public void testCreateGatewayReceiverOnMultipleGroups() throws Exception {
    Integer locator1Port = locatorSite1.getPort();
    server1 = startServerWithGroups(3, "receiverGroup1", locator1Port);
    server2 = startServerWithGroups(4, "receiverGroup1", locator1Port);
    server3 = startServerWithGroups(5, "receiverGroup2", locator1Port);

    String command =
        CliStrings.CREATE_GATEWAYRECEIVER + " --" + CliStrings.CREATE_GATEWAYRECEIVER__MANUALSTART
            + "=true" + " --" + CliStrings.CREATE_GATEWAYRECEIVER__BINDADDRESS + "=localhost"
            + " --" + CliStrings.CREATE_GATEWAYRECEIVER__STARTPORT + "=10000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__ENDPORT + "=11000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__MAXTIMEBETWEENPINGS + "=100000" + " --"
            + CliStrings.CREATE_GATEWAYRECEIVER__SOCKETBUFFERSIZE + "=512000" + " --" + GROUP
            + "=receiverGroup1,receiverGroup2";
    gfsh.executeAndAssertThat(command).statusIsSuccess()
        .tableHasColumnWithExactValuesInAnyOrder("Member", "server-3", "server-4", "server-5")
        .tableHasColumnWithValuesContaining("Status",
            "GatewayReceiver created on member \"server-3\"",
            "GatewayReceiver created on member \"server-4\"",
            "GatewayReceiver created on member \"server-5\"");

    VMProvider.invokeInEveryMember(() -> {
      verifyReceiverCreationWithAttributes(false, 10000, 11000, "localhost", 100000, 512000, null,
          GatewayReceiver.DEFAULT_HOSTNAME_FOR_SENDERS);
    }, server1, server2, server3);
  }
}
