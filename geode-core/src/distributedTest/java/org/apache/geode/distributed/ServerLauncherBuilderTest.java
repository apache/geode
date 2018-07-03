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
package org.apache.geode.distributed;

import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import java.net.UnknownHostException;

import joptsimple.OptionException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ServerLauncher.Builder;
import org.apache.geode.distributed.ServerLauncher.Command;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit tests for {@link ServerLauncher.Builder}. Extracted from {@link ServerLauncherTest}.
 */
public class ServerLauncherBuilderTest {

  private InetAddress localHost;
  private String localHostName;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void before() throws Exception {
    localHost = InetAddress.getLocalHost();
    localHostName = localHost.getCanonicalHostName();
  }

  @Test
  public void defaultCommandIsUnspecified() throws Exception {
    assertThat(Builder.DEFAULT_COMMAND).isEqualTo(Command.UNSPECIFIED);
  }

  @Test
  public void getCommandReturnsUnspecifiedByDefault() throws Exception {
    assertThat(new Builder().getCommand()).isEqualTo(Builder.DEFAULT_COMMAND);
  }

  @Test
  public void getCriticalHeapPercentageReturnsNullByDefault() throws Exception {
    assertThat(new Builder().getCriticalHeapPercentage()).isNull();
  }

  @Test
  public void getEvictionHeapPercentageReturnsNullByDefault() throws Exception {
    assertThat(new Builder().getEvictionHeapPercentage()).isNull();
  }

  @Test
  public void getForceReturnsFalseByDefault() {
    assertThat(new Builder().getForce()).isFalse();
  }

  @Test
  public void getHostNameForClientsReturnsNullByDefault() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.getHostNameForClients()).isNull();
  }

  @Test
  public void getMaxConnectionsReturnsNullByDefault() throws Exception {
    assertThat(new Builder().getMaxConnections()).isNull();
  }

  @Test
  public void getMaxMessageCountReturnsNullByDefault() throws Exception {
    assertThat(new Builder().getMaxMessageCount()).isNull();
  }

  @Test
  public void getMaxThreadsReturnsNullByDefault() throws Exception {
    assertThat(new Builder().getMaxThreads()).isNull();
  }

  @Test
  public void getMessageTimeToLiveReturnsNullByDefault() throws Exception {
    assertThat(new Builder().getMessageTimeToLive()).isNull();
  }

  @Test
  public void getMemberNameReturnsNullByDefault() throws Exception {
    assertThat(new Builder().getMemberName()).isNull();
  }

  @Test
  public void getPidReturnsNullByDefault() throws Exception {
    assertThat(new Builder().getPid()).isNull();
  }

  @Test
  public void getRedirectOutputReturnsNullByDefault() throws Exception {
    assertThat(new Builder().getRedirectOutput()).isNull();
  }

  @Test
  public void getServerBindAddressReturnsNullByDefault() throws Exception {
    assertThat(new Builder().getServerBindAddress()).isNull();
  }

  @Test
  public void getServerPortReturnsDefaultPortByDefault() throws Exception {
    assertThat(new Builder().getServerPort()).isEqualTo(Integer.valueOf(CacheServer.DEFAULT_PORT));
  }

  @Test
  public void getSocketBufferSizeReturnsNullByDefault() throws Exception {
    assertThat(new Builder().getSocketBufferSize()).isNull();
  }

  @Test
  public void setCommandReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setCommand(Command.STATUS)).isSameAs(builder);
  }

  @Test
  public void setCriticalHeapPercentageReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setCriticalHeapPercentage(55.5f)).isSameAs(builder);
  }

  @Test
  public void setEvictionHeapPercentageReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setEvictionHeapPercentage(55.55f)).isSameAs(builder);
  }

  @Test
  public void setForceReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setForce(true)).isSameAs(builder);
  }

  @Test
  public void setHostNameForClientsReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setHostNameForClients("Pegasus")).isSameAs(builder);
  }

  @Test
  public void setMaxConnectionsReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setMaxConnections(1000)).isSameAs(builder);
  }

  @Test
  public void setMaxMessageCountReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setMaxMessageCount(50)).isSameAs(builder);
  }

  @Test
  public void setMaxThreadsReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setMaxThreads(16)).isSameAs(builder);
  }

  @Test
  public void setMemberNameReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setMemberName("serverOne")).isSameAs(builder);
  }

  @Test
  public void setPidReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setPid(0)).isSameAs(builder);
  }

  @Test
  public void setServerBindAddressReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setServerBindAddress(null)).isSameAs(builder);
  }

  @Test
  public void setRedirectOutputReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setRedirectOutput(Boolean.TRUE)).isSameAs(builder);
  }

  @Test
  public void setServerPortReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setServerPort(null)).isSameAs(builder);
  }

  @Test
  public void setMessageTimeToLiveReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setMessageTimeToLive(30000)).isSameAs(builder);
  }

  @Test
  public void setSocketBufferSizeReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setSocketBufferSize(32768)).isSameAs(builder);
  }

  @Test
  public void setCommandWithNullResultsInDefaultCommand() throws Exception {
    Builder builder = new Builder();

    new Builder().setCommand(null);

    assertThat(builder.getCommand()).isEqualTo(Builder.DEFAULT_COMMAND);
  }

  @Test
  public void setCommandToStatusResultsInStatus() throws Exception {
    Builder builder = new Builder();

    builder.setCommand(Command.STATUS);

    assertThat(builder.getCommand()).isEqualTo(Command.STATUS);
  }

  @Test
  public void setCriticalHeapPercentageToPercentileUsesValue() throws Exception {
    Builder builder = new Builder();

    builder.setCriticalHeapPercentage(55.5f);

    assertThat(builder.getCriticalHeapPercentage().floatValue()).isEqualTo(55.5f);
  }

  @Test
  public void setCriticalHeapPercentageToNullResultsInNull() throws Exception {
    Builder builder = new Builder();

    builder.setCriticalHeapPercentage(null);

    assertThat(builder.getCriticalHeapPercentage()).isNull();
  }

  @Test
  public void setCriticalHeapPercentageAbove100ThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setCriticalHeapPercentage(100.01f))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setCriticalHeapPercentageBelowZeroThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setCriticalHeapPercentage(-0.01f))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setEvictionHeapPercentageToPercentileUsesValue() throws Exception {
    Builder builder = new Builder();

    builder.setEvictionHeapPercentage(55.55f);

    assertThat(builder.getEvictionHeapPercentage().floatValue()).isEqualTo(55.55f);
  }

  @Test
  public void setEvictionHeapPercentageToNullResultsInNull() throws Exception {
    Builder builder = new Builder();

    builder.setEvictionHeapPercentage(null);

    assertThat(builder.getEvictionHeapPercentage()).isNull();
  }

  @Test
  public void setEvictionHeapPercentageAboveO100ThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setEvictionHeapPercentage(101.0f))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setEvictionHeapPercentageBelowZeroThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setEvictionHeapPercentage(-10.0f))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setForceToTrueUsesValue() throws Exception {
    Builder builder = new Builder();

    builder.setForce(true);

    assertThat(builder.getForce()).isTrue();
  }

  @Test
  public void setHostNameForClientsToStringUsesValue() throws Exception {
    Builder builder = new Builder();

    builder.setHostNameForClients("Pegasus");

    assertThat(builder.getHostNameForClients()).isEqualTo("Pegasus");
  }

  @Test
  public void setHostNameForClientsToBlankStringThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setHostNameForClients(" "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setHostNameForClientsToEmptyStringThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setHostNameForClients(""))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setHostNameForClientsToNullThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setHostNameForClients(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setMaxConnectionsToPositiveIntegerUsesValue() throws Exception {
    Builder builder = new Builder();

    builder.setMaxConnections(1000);

    assertThat(builder.getMaxConnections().intValue()).isEqualTo(1000);
  }

  @Test
  public void setMaxConnectionsToNullResultsInNull() throws Exception {
    Builder builder = new Builder();

    builder.setMaxConnections(null);

    assertThat(builder.getMaxConnections()).isNull();
  }

  @Test
  public void setMaxConnectionsToNegativeValueThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setMaxConnections(-10))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setMaxMessageCountToPositiveIntegerUsesValue() throws Exception {
    Builder builder = new Builder();

    builder.setMaxMessageCount(50);

    assertThat(builder.getMaxMessageCount().intValue()).isEqualTo(50);
  }

  @Test
  public void setMaxMessageCountToNullResultsInNull() throws Exception {
    Builder builder = new Builder();

    builder.setMaxMessageCount(null);

    assertThat(builder.getMaxMessageCount()).isNull();
  }

  @Test
  public void setMaxMessageCountToZeroResultsInIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setMaxMessageCount(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setMaxThreadsToPositiveIntegerUsesValue() throws Exception {
    Builder builder = new Builder();

    builder.setMaxThreads(16);

    assertThat(builder.getMaxThreads().intValue()).isEqualTo(16);
  }

  @Test
  public void setMaxThreadsToNullResultsInNull() throws Exception {
    Builder builder = new Builder();

    builder.setMaxThreads(null);

    assertThat(builder.getMaxThreads()).isNull();
  }

  @Test
  public void setMaxThreadsToNegativeValueThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setMaxThreads(-4))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setMemberNameToStringUsesValue() throws Exception {
    Builder builder = new Builder();

    builder.setMemberName("serverOne");

    assertThat(builder.getMemberName()).isEqualTo("serverOne");
  }

  @Test
  public void setMemberNameToBlankStringThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setMemberName("  "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setMemberNameToEmptyStringThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setMemberName(""))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setMemberNameToNullStringThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setMemberName(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setMessageTimeToLiveToPositiveIntegerUsesValue() throws Exception {
    Builder builder = new Builder();

    builder.setMessageTimeToLive(30000);

    assertThat(builder.getMessageTimeToLive().intValue()).isEqualTo(30000);
  }

  @Test
  public void setMessageTimeToNullResultsInNull() throws Exception {
    Builder builder = new Builder();

    builder.setMessageTimeToLive(null);

    assertThat(builder.getMessageTimeToLive()).isNull();
  }

  @Test
  public void setMessageTimeToLiveToZeroThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setMessageTimeToLive(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setPidToZeroOrGreaterUsesValue() throws Exception {
    Builder builder = new Builder();

    builder.setPid(0);
    assertThat(builder.getPid().intValue()).isEqualTo(0);

    builder.setPid(1);
    assertThat(builder.getPid().intValue()).isEqualTo(1);

    builder.setPid(1024);
    assertThat(builder.getPid().intValue()).isEqualTo(1024);

    builder.setPid(12345);
    assertThat(builder.getPid().intValue()).isEqualTo(12345);
  }

  @Test
  public void setPidToNullResultsInNull() throws Exception {
    Builder builder = new Builder();

    builder.setPid(null);

    assertThat(builder.getPid()).isNull();
  }

  @Test
  public void setPidToNegativeValueThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setPid(-1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setServerBindAddressToNullResultsInNull() throws Exception {
    Builder builder = new Builder();

    builder.setServerBindAddress(null);

    assertThat(builder.getServerBindAddress()).isNull();
  }

  @Test
  public void setServerBindAddressToEmptyStringResultsInNull() throws Exception {
    Builder builder = new Builder();

    builder.setServerBindAddress("");

    assertThat(builder.getServerBindAddress()).isNull();
  }

  @Test
  public void setServerBindAddressToBlankStringResultsInNull() throws Exception {
    Builder builder = new Builder();

    builder.setServerBindAddress("  ");

    assertThat(builder.getServerBindAddress()).isNull();
  }

  @Test
  public void setServerBindAddressToCanonicalLocalHostUsesValue() throws Exception {
    Builder builder = new Builder();

    builder.setServerBindAddress(localHostName);

    assertThat(builder.getServerBindAddress()).isEqualTo(localHost);
  }

  @Test
  public void setServerBindAddressToLocalHostNameUsesValue() throws Exception {
    String host = InetAddress.getLocalHost().getHostName();

    Builder builder = new Builder().setServerBindAddress(host);

    assertThat(builder.getServerBindAddress()).isEqualTo(localHost);
  }

  @Test
  public void setServerBindAddressToUnknownHostThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setServerBindAddress("badHostName.example.com"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasCauseInstanceOf(UnknownHostException.class);
  }

  @Test
  public void setServerBindAddressToNonLocalHostThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setServerBindAddress("yahoo.com"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setServerPortToNullResultsInDefaultPort() throws Exception {
    Builder builder = new Builder();

    builder.setServerPort(null);

    assertThat(builder.getServerPort()).isEqualTo(Integer.valueOf(CacheServer.DEFAULT_PORT));
  }


  @Test
  public void setServerPortToZeroOrGreaterUsesValue() throws Exception {
    Builder builder = new Builder();

    builder.setServerPort(0);
    assertThat(builder.getServerPort().intValue()).isEqualTo(0);
    assertThat(builder.isServerPortSetByUser()).isFalse();

    builder.setServerPort(1);
    assertThat(builder.getServerPort().intValue()).isEqualTo(1);
    assertThat(builder.isServerPortSetByUser()).isTrue();

    builder.setServerPort(80);
    assertThat(builder.getServerPort().intValue()).isEqualTo(80);
    assertThat(builder.isServerPortSetByUser()).isTrue();

    builder.setServerPort(1024);
    assertThat(builder.getServerPort().intValue()).isEqualTo(1024);
    assertThat(builder.isServerPortSetByUser()).isTrue();

    builder.setServerPort(65535);
    assertThat(builder.getServerPort().intValue()).isEqualTo(65535);
    assertThat(builder.isServerPortSetByUser()).isTrue();

    builder.setServerPort(0);
    assertThat(builder.getServerPort().intValue()).isEqualTo(0);
    assertThat(builder.isServerPortSetByUser()).isFalse();
  }

  @Test
  public void setServerPortAboveMaxValueThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setServerPort(65536))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setServerPortToNegativeValueThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setServerPort(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setSocketBufferSizeToPositiveIntegerUsesValue() throws Exception {
    Builder builder = new Builder();

    builder.setSocketBufferSize(32768);

    assertThat(builder.getSocketBufferSize().intValue()).isEqualTo(32768);
  }

  @Test
  public void setSocketBufferSizeToNullResultsInNull() throws Exception {
    Builder builder = new Builder();

    builder.setSocketBufferSize(null);

    assertThat(builder.getSocketBufferSize()).isNull();
  }

  @Test
  public void setSocketBufferSizeToNegativeValueThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setSocketBufferSize(-8192))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void parseArgumentsWithForceSetsForceToTrue() throws Exception {
    Builder builder = new Builder();

    builder.parseArguments("start", "--force");

    assertThat(builder.getForce()).isTrue();
  }

  @Test
  public void parseArgumentsWithNonNumericPortThrowsIllegalArgumentException() {
    assertThatThrownBy(
        () -> new Builder().parseArguments("start", "server1", "--server-port", "oneTwoThree"))
            .isInstanceOf(IllegalArgumentException.class).hasCauseInstanceOf(OptionException.class);
  }

  @Test
  public void parseArgumentsParsesValuesSeparatedWithCommas() throws Exception {
    // given: fresh builder
    Builder builder = new Builder();

    // when: parsing comma-separated arguments
    builder.parseArguments("start", "serverOne", "--assign-buckets", "--disable-default-server",
        "--debug", "--force", "--rebalance", "--redirect-output", "--pid", "1234",
        "--server-bind-address", InetAddress.getLocalHost().getHostAddress(), "--server-port",
        "11235", "--hostname-for-clients", "192.168.99.100");

    // then: getters should return parsed values
    assertThat(builder.getCommand()).isEqualTo(Command.START);
    assertThat(builder.getMemberName()).isEqualTo("serverOne");
    assertThat(builder.getHostNameForClients()).isEqualTo("192.168.99.100");
    assertThat(builder.getAssignBuckets()).isTrue();
    assertThat(builder.getDisableDefaultServer()).isTrue();
    assertThat(builder.getDebug()).isTrue();
    assertThat(builder.getForce()).isTrue();
    assertThat(builder.getHelp()).isFalse();
    assertThat(builder.getRebalance()).isTrue();
    assertThat(builder.getRedirectOutput()).isTrue();
    assertThat(builder.getPid().intValue()).isEqualTo(1234);
    assertThat(builder.getServerBindAddress()).isEqualTo(InetAddress.getLocalHost());
    assertThat(builder.getServerPort().intValue()).isEqualTo(11235);
  }

  @Test
  public void parseArgumentsParsesValuesSeparatedWithEquals() throws Exception {
    // given: fresh builder
    Builder builder = new Builder();

    // when: parsing equals-separated arguments
    builder.parseArguments("start", "serverOne", "--assign-buckets", "--disable-default-server",
        "--debug", "--force", "--rebalance", "--redirect-output", "--pid=1234",
        "--server-bind-address=" + InetAddress.getLocalHost().getHostAddress(),
        "--server-port=11235", "--hostname-for-clients=192.168.99.100");

    // then: getters should return parsed values
    assertThat(builder.getCommand()).isEqualTo(Command.START);
    assertThat(builder.getMemberName()).isEqualTo("serverOne");
    assertThat(builder.getHostNameForClients()).isEqualTo("192.168.99.100");
    assertThat(builder.getAssignBuckets()).isTrue();
    assertThat(builder.getDisableDefaultServer()).isTrue();
    assertThat(builder.getDebug()).isTrue();
    assertThat(builder.getForce()).isTrue();
    assertThat(builder.getHelp()).isFalse();
    assertThat(builder.getRebalance()).isTrue();
    assertThat(builder.getRedirectOutput()).isTrue();
    assertThat(builder.getPid().intValue()).isEqualTo(1234);
    assertThat(builder.getServerBindAddress()).isEqualTo(InetAddress.getLocalHost());
    assertThat(builder.getServerPort().intValue()).isEqualTo(11235);
  }

  @Test
  public void parseCommandWithNullStringArrayResultsInDefaultCommand() throws Exception {
    Builder builder = new Builder();

    builder.parseCommand((String[]) null);

    assertThat(builder.getCommand()).isEqualTo(Builder.DEFAULT_COMMAND);
  }

  @Test
  public void parseCommandWithEmptyStringArrayResultsInDefaultCommand() throws Exception {
    Builder builder = new Builder();

    builder.parseCommand(); // empty String array

    assertThat(builder.getCommand()).isEqualTo(Builder.DEFAULT_COMMAND);
  }

  @Test
  public void parseCommandWithStartResultsInStartCommand() throws Exception {
    Builder builder = new Builder();

    builder.parseCommand(Command.START.getName());

    assertThat(builder.getCommand()).isEqualTo(Command.START);
  }

  @Test
  public void parseCommandWithStatusResultsInStatusCommand() throws Exception {
    Builder builder = new Builder();

    builder.parseCommand("Status");

    assertThat(builder.getCommand()).isEqualTo(Command.STATUS);
  }

  @Test
  public void parseCommandWithMixedCaseResultsInCorrectCase() throws Exception {
    Builder builder = new Builder();

    builder.parseCommand("sToP");

    assertThat(builder.getCommand()).isEqualTo(Command.STOP);
  }

  @Test
  public void parseCommandWithTwoCommandsWithSwitchesUsesFirstCommand() throws Exception {
    Builder builder = new Builder();

    builder.parseCommand("--opt", "START", "-o", Command.STATUS.getName());

    assertThat(builder.getCommand()).isEqualTo(Command.START);
  }

  @Test
  public void parseCommandWithTwoCommandsWithoutSwitchesUsesFirstCommand() throws Exception {
    Builder builder = new Builder();

    builder.parseCommand("START", Command.STATUS.getName());

    assertThat(builder.getCommand()).isEqualTo(Command.START);
  }

  @Test
  public void parseCommandWithBadInputResultsInDefaultCommand() throws Exception {
    Builder builder = new Builder();

    builder.parseCommand("badCommandName", "--start", "stat");

    assertThat(builder.getCommand()).isEqualTo(Builder.DEFAULT_COMMAND);
  }

  @Test
  public void parseMemberNameWithNullStringArrayResultsInNull() throws Exception {
    Builder builder = new Builder();

    builder.parseMemberName((String[]) null);

    assertThat(builder.getMemberName()).isNull();
  }

  @Test
  public void parseMemberNameWithEmptyStringArrayResultsInNull() throws Exception {
    Builder builder = new Builder();

    builder.parseMemberName(); // empty String array

    assertThat(builder.getMemberName()).isNull();
  }

  @Test
  public void parseMemberNameWithCommandAndOptionsResultsInNull() throws Exception {
    Builder builder = new Builder();

    builder.parseMemberName(Command.START.getName(), "--opt", "-o");

    assertThat(builder.getMemberName()).isNull();
  }

  @Test
  public void parseMemberNameWithStringUsesValue() throws Exception {
    Builder builder = new Builder();

    builder.parseMemberName("memberOne");

    assertThat(builder.getMemberName()).isEqualTo("memberOne");
  }

  @Test
  public void buildCreatesServerLauncherWithBuilderValues() throws Exception {
    ServerLauncher launcher = new Builder().setCommand(Command.STOP).setAssignBuckets(true)
        .setForce(true).setMemberName("serverOne").setRebalance(true)
        .setServerBindAddress(InetAddress.getLocalHost().getHostAddress()).setServerPort(11235)
        .setCriticalHeapPercentage(90.0f).setEvictionHeapPercentage(75.0f).setMaxConnections(100)
        .setMaxMessageCount(512).setMaxThreads(8).setMessageTimeToLive(120000)
        .setSocketBufferSize(32768).setRedirectOutput(Boolean.TRUE).build();

    assertThat(launcher.getCommand()).isEqualTo(Command.STOP);
    assertThat(launcher.getMemberName()).isEqualTo("serverOne");
    assertThat(launcher.getServerBindAddress()).isEqualTo(InetAddress.getLocalHost());
    assertThat(launcher.getServerPort().intValue()).isEqualTo(11235);
    assertThat(launcher.getCriticalHeapPercentage().floatValue()).isEqualTo(90.0f);
    assertThat(launcher.getEvictionHeapPercentage().floatValue()).isEqualTo(75.0f);
    assertThat(launcher.getMaxConnections().intValue()).isEqualTo(100);
    assertThat(launcher.getMaxMessageCount().intValue()).isEqualTo(512);
    assertThat(launcher.getMaxThreads().intValue()).isEqualTo(8);
    assertThat(launcher.getMessageTimeToLive().intValue()).isEqualTo(120000);
    assertThat(launcher.getSocketBufferSize().intValue()).isEqualTo(32768);
    assertThat(launcher.isAssignBuckets()).isTrue();
    assertThat(launcher.isDebugging()).isFalse();
    assertThat(launcher.isDisableDefaultServer()).isFalse();
    assertThat(launcher.isForcing()).isTrue();
    assertThat(launcher.isHelping()).isFalse();
    assertThat(launcher.isRebalancing()).isTrue();
    assertThat(launcher.isRedirectingOutput()).isTrue();
    assertThat(launcher.isRunning()).isFalse();
  }

  @Test
  public void buildUsesMemberNameSetInApiProperties() throws Exception {
    ServerLauncher launcher =
        new Builder().setCommand(ServerLauncher.Command.START).set(NAME, "serverABC").build();

    assertThat(launcher.getMemberName()).isNull();
    assertThat(launcher.getProperties().getProperty(NAME)).isEqualTo("serverABC");
  }

  @Test
  public void buildUsesMemberNameSetInSystemProperties() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + NAME, "serverXYZ");

    ServerLauncher launcher = new Builder().setCommand(ServerLauncher.Command.START).build();

    assertThat(launcher.getMemberName()).isNull();
  }
}
