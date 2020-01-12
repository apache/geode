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
import static org.mockito.Mockito.mock;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import joptsimple.OptionException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ServerLauncher.Builder;
import org.apache.geode.distributed.ServerLauncher.Command;
import org.apache.geode.internal.process.ControllableProcess;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Unit tests for {@link ServerLauncher.Builder}. Extracted from {@link ServerLauncherTest}.
 */
public class ServerLauncherBuilderTest {

  private InetAddress localHost;
  private String localHostName;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void before() throws UnknownHostException {
    localHost = InetAddress.getLocalHost();
    localHostName = localHost.getCanonicalHostName();
  }

  @Test
  public void defaultCommandIsUnspecified() {
    assertThat(Builder.DEFAULT_COMMAND).isEqualTo(Command.UNSPECIFIED);
  }

  @Test
  public void getCommandReturnsUnspecifiedByDefault() {
    assertThat(new Builder().getCommand()).isEqualTo(Builder.DEFAULT_COMMAND);
  }

  @Test
  public void getCriticalHeapPercentageReturnsNullByDefault() {
    assertThat(new Builder().getCriticalHeapPercentage()).isNull();
  }

  @Test
  public void getEvictionHeapPercentageReturnsNullByDefault() {
    assertThat(new Builder().getEvictionHeapPercentage()).isNull();
  }

  @Test
  public void getForceReturnsFalseByDefault() {
    assertThat(new Builder().getForce()).isFalse();
  }

  @Test
  public void getHostNameForClientsReturnsNullByDefault() {
    Builder builder = new Builder();

    assertThat(builder.getHostNameForClients()).isNull();
  }

  @Test
  public void getMaxConnectionsReturnsNullByDefault() {
    assertThat(new Builder().getMaxConnections()).isNull();
  }

  @Test
  public void getMaxMessageCountReturnsNullByDefault() {
    assertThat(new Builder().getMaxMessageCount()).isNull();
  }

  @Test
  public void getMaxThreadsReturnsNullByDefault() {
    assertThat(new Builder().getMaxThreads()).isNull();
  }

  @Test
  public void getMessageTimeToLiveReturnsNullByDefault() {
    assertThat(new Builder().getMessageTimeToLive()).isNull();
  }

  @Test
  public void getMemberNameReturnsNullByDefault() {
    assertThat(new Builder().getMemberName()).isNull();
  }

  @Test
  public void getPidReturnsNullByDefault() {
    assertThat(new Builder().getPid()).isNull();
  }

  @Test
  public void getRedirectOutputReturnsNullByDefault() {
    assertThat(new Builder().getRedirectOutput()).isNull();
  }

  @Test
  public void getServerBindAddressReturnsNullByDefault() {
    assertThat(new Builder().getServerBindAddress()).isNull();
  }

  @Test
  public void getServerPortReturnsDefaultPortByDefault() {
    assertThat(new Builder().getServerPort()).isEqualTo(Integer.valueOf(CacheServer.DEFAULT_PORT));
  }

  @Test
  public void getSocketBufferSizeReturnsNullByDefault() {
    assertThat(new Builder().getSocketBufferSize()).isNull();
  }

  @Test
  public void setCommandReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setCommand(Command.STATUS)).isSameAs(builder);
  }

  @Test
  public void setCriticalHeapPercentageReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setCriticalHeapPercentage(55.5f)).isSameAs(builder);
  }

  @Test
  public void setEvictionHeapPercentageReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setEvictionHeapPercentage(55.55f)).isSameAs(builder);
  }

  @Test
  public void setForceReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setForce(true)).isSameAs(builder);
  }

  @Test
  public void setHostNameForClientsReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setHostNameForClients("Pegasus")).isSameAs(builder);
  }

  @Test
  public void setMaxConnectionsReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setMaxConnections(1000)).isSameAs(builder);
  }

  @Test
  public void setMaxMessageCountReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setMaxMessageCount(50)).isSameAs(builder);
  }

  @Test
  public void setMaxThreadsReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setMaxThreads(16)).isSameAs(builder);
  }

  @Test
  public void setMemberNameReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setMemberName("serverOne")).isSameAs(builder);
  }

  @Test
  public void setPidReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setPid(0)).isSameAs(builder);
  }

  @Test
  public void setServerBindAddressReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setServerBindAddress(null)).isSameAs(builder);
  }

  @Test
  public void setRedirectOutputReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setRedirectOutput(Boolean.TRUE)).isSameAs(builder);
  }

  @Test
  public void setServerPortReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setServerPort(null)).isSameAs(builder);
  }

  @Test
  public void setMessageTimeToLiveReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setMessageTimeToLive(30000)).isSameAs(builder);
  }

  @Test
  public void setSocketBufferSizeReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setSocketBufferSize(32768)).isSameAs(builder);
  }

  @Test
  public void setCommandWithNullResultsInDefaultCommand() {
    Builder builder = new Builder();

    new Builder().setCommand(null);

    assertThat(builder.getCommand()).isEqualTo(Builder.DEFAULT_COMMAND);
  }

  @Test
  public void setCommandToStatusResultsInStatus() {
    Builder builder = new Builder();

    builder.setCommand(Command.STATUS);

    assertThat(builder.getCommand()).isEqualTo(Command.STATUS);
  }

  @Test
  public void setCriticalHeapPercentageToPercentileUsesValue() {
    Builder builder = new Builder();

    builder.setCriticalHeapPercentage(55.5f);

    assertThat(builder.getCriticalHeapPercentage().floatValue()).isEqualTo(55.5f);
  }

  @Test
  public void setCriticalHeapPercentageToNullResultsInNull() {
    Builder builder = new Builder();

    builder.setCriticalHeapPercentage(null);

    assertThat(builder.getCriticalHeapPercentage()).isNull();
  }

  @Test
  public void setCriticalHeapPercentageAbove100ThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setCriticalHeapPercentage(100.01f))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setCriticalHeapPercentageBelowZeroThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setCriticalHeapPercentage(-0.01f))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setEvictionHeapPercentageToPercentileUsesValue() {
    Builder builder = new Builder();

    builder.setEvictionHeapPercentage(55.55f);

    assertThat(builder.getEvictionHeapPercentage().floatValue()).isEqualTo(55.55f);
  }

  @Test
  public void setEvictionHeapPercentageToNullResultsInNull() {
    Builder builder = new Builder();

    builder.setEvictionHeapPercentage(null);

    assertThat(builder.getEvictionHeapPercentage()).isNull();
  }

  @Test
  public void setEvictionHeapPercentageAboveO100ThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setEvictionHeapPercentage(101.0f))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setEvictionHeapPercentageBelowZeroThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setEvictionHeapPercentage(-10.0f))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setForceToTrueUsesValue() {
    Builder builder = new Builder();

    builder.setForce(true);

    assertThat(builder.getForce()).isTrue();
  }

  @Test
  public void setHostNameForClientsToStringUsesValue() {
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
  public void setMaxConnectionsToPositiveIntegerUsesValue() {
    Builder builder = new Builder();

    builder.setMaxConnections(1000);

    assertThat(builder.getMaxConnections().intValue()).isEqualTo(1000);
  }

  @Test
  public void setMaxConnectionsToNullResultsInNull() {
    Builder builder = new Builder();

    builder.setMaxConnections(null);

    assertThat(builder.getMaxConnections()).isNull();
  }

  @Test
  public void setMaxConnectionsToNegativeValueThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setMaxConnections(-10))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setMaxMessageCountToPositiveIntegerUsesValue() {
    Builder builder = new Builder();

    builder.setMaxMessageCount(50);

    assertThat(builder.getMaxMessageCount().intValue()).isEqualTo(50);
  }

  @Test
  public void setMaxMessageCountToNullResultsInNull() {
    Builder builder = new Builder();

    builder.setMaxMessageCount(null);

    assertThat(builder.getMaxMessageCount()).isNull();
  }

  @Test
  public void setMaxMessageCountToZeroResultsInIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setMaxMessageCount(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setMaxThreadsToPositiveIntegerUsesValue() {
    Builder builder = new Builder();

    builder.setMaxThreads(16);

    assertThat(builder.getMaxThreads().intValue()).isEqualTo(16);
  }

  @Test
  public void setMaxThreadsToNullResultsInNull() {
    Builder builder = new Builder();

    builder.setMaxThreads(null);

    assertThat(builder.getMaxThreads()).isNull();
  }

  @Test
  public void setMaxThreadsToNegativeValueThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setMaxThreads(-4))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setMemberNameToStringUsesValue() {
    Builder builder = new Builder();

    builder.setMemberName("serverOne");

    assertThat(builder.getMemberName()).isEqualTo("serverOne");
  }

  @Test
  public void setMemberNameToBlankStringThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setMemberName("  "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setMemberNameToEmptyStringThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setMemberName(""))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setMemberNameToNullStringThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setMemberName(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setMessageTimeToLiveToPositiveIntegerUsesValue() {
    Builder builder = new Builder();

    builder.setMessageTimeToLive(30000);

    assertThat(builder.getMessageTimeToLive().intValue()).isEqualTo(30000);
  }

  @Test
  public void setMessageTimeToNullResultsInNull() {
    Builder builder = new Builder();

    builder.setMessageTimeToLive(null);

    assertThat(builder.getMessageTimeToLive()).isNull();
  }

  @Test
  public void setMessageTimeToLiveToZeroThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setMessageTimeToLive(0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setPidToZeroOrGreaterUsesValue() {
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
  public void setPidToNullResultsInNull() {
    Builder builder = new Builder();

    builder.setPid(null);

    assertThat(builder.getPid()).isNull();
  }

  @Test
  public void setPidToNegativeValueThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setPid(-1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setServerBindAddressToNullResultsInNull() {
    Builder builder = new Builder();

    builder.setServerBindAddress(null);

    assertThat(builder.getServerBindAddress()).isNull();
  }

  @Test
  public void setServerBindAddressToEmptyStringResultsInNull() {
    Builder builder = new Builder();

    builder.setServerBindAddress("");

    assertThat(builder.getServerBindAddress()).isNull();
  }

  @Test
  public void setServerBindAddressToBlankStringResultsInNull() {
    Builder builder = new Builder();

    builder.setServerBindAddress("  ");

    assertThat(builder.getServerBindAddress()).isNull();
  }

  @Test
  public void setServerBindAddressToCanonicalLocalHostUsesValue() {
    Builder builder = new Builder();

    builder.setServerBindAddress(localHostName);

    assertThat(builder.getServerBindAddress()).isEqualTo(localHost);
  }

  @Test
  public void setServerBindAddressToLocalHostNameUsesValue() throws UnknownHostException {
    String host = InetAddress.getLocalHost().getHostName();

    Builder builder = new Builder().setServerBindAddress(host);

    assertThat(builder.getServerBindAddress()).isEqualTo(localHost);
  }

  @Test
  public void setServerBindAddressToUnknownHostThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setServerBindAddress("badHostName.example.com"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasCauseInstanceOf(UnknownHostException.class);
  }

  @Test
  public void setServerBindAddressToNonLocalHostThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setServerBindAddress("yahoo.com"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setServerPortToNullResultsInDefaultPort() {
    Builder builder = new Builder();

    builder.setServerPort(null);

    assertThat(builder.getServerPort()).isEqualTo(Integer.valueOf(CacheServer.DEFAULT_PORT));
  }


  @Test
  public void setServerPortToZeroOrGreaterUsesValue() {
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
  public void setServerPortAboveMaxValueThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setServerPort(65536))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setServerPortToNegativeValueThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setServerPort(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setSocketBufferSizeToPositiveIntegerUsesValue() {
    Builder builder = new Builder();

    builder.setSocketBufferSize(32768);

    assertThat(builder.getSocketBufferSize().intValue()).isEqualTo(32768);
  }

  @Test
  public void setSocketBufferSizeToNullResultsInNull() {
    Builder builder = new Builder();

    builder.setSocketBufferSize(null);

    assertThat(builder.getSocketBufferSize()).isNull();
  }

  @Test
  public void setSocketBufferSizeToNegativeValueThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setSocketBufferSize(-8192))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void parseArgumentsWithForceSetsForceToTrue() {
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
  public void parseArgumentsParsesValuesSeparatedWithCommas() throws UnknownHostException {
    // given: fresh builder
    Builder builder = new Builder();

    // when: parsing comma-separated arguments
    builder.parseArguments("start", "serverOne", "--assign-buckets", "--disable-default-server",
        "--debug", "--force", "--rebalance", "--redirect-output", "--pid", "1234",
        "--server-bind-address", InetAddress.getLocalHost().getHostAddress(), "--server-port",
        "11235", "--hostname-for-clients", "192.168.99.100", "--max-connections", "300",
        "--max-message-count", "1000", "--message-time-to-live", "10000", "--socket-buffer-size",
        "1024", "--max-threads", "900");

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
    assertThat(builder.getMaxConnections()).isEqualTo(300);
    assertThat(builder.getMaxMessageCount()).isEqualTo(1000);
    assertThat(builder.getMessageTimeToLive()).isEqualTo(10000);
    assertThat(builder.getSocketBufferSize()).isEqualTo(1024);
    assertThat(builder.getMaxThreads()).isEqualTo(900);
  }

  @Test
  public void parseArgumentsParsesValuesSeparatedWithEquals() throws UnknownHostException {
    // given: fresh builder
    Builder builder = new Builder();

    // when: parsing equals-separated arguments
    builder.parseArguments("start", "serverOne", "--assign-buckets", "--disable-default-server",
        "--debug", "--force", "--rebalance", "--redirect-output", "--pid=1234",
        "--server-bind-address=" + InetAddress.getLocalHost().getHostAddress(),
        "--server-port=11235", "--hostname-for-clients=192.168.99.100", "--max-connections=300",
        "--max-message-count=1000", "--message-time-to-live=10000", "--socket-buffer-size=1024",
        "--max-threads=900");

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
    assertThat(builder.getMaxConnections()).isEqualTo(300);
    assertThat(builder.getMaxMessageCount()).isEqualTo(1000);
    assertThat(builder.getMessageTimeToLive()).isEqualTo(10000);
    assertThat(builder.getSocketBufferSize()).isEqualTo(1024);
    assertThat(builder.getMaxThreads()).isEqualTo(900);
  }

  @Test
  public void parseCommandWithNullStringArrayResultsInDefaultCommand() {
    Builder builder = new Builder();

    builder.parseCommand((String[]) null);

    assertThat(builder.getCommand()).isEqualTo(Builder.DEFAULT_COMMAND);
  }

  @Test
  public void parseCommandWithEmptyStringArrayResultsInDefaultCommand() {
    Builder builder = new Builder();

    builder.parseCommand(); // empty String array

    assertThat(builder.getCommand()).isEqualTo(Builder.DEFAULT_COMMAND);
  }

  @Test
  public void parseCommandWithStartResultsInStartCommand() {
    Builder builder = new Builder();

    builder.parseCommand(Command.START.getName());

    assertThat(builder.getCommand()).isEqualTo(Command.START);
  }

  @Test
  public void parseCommandWithStatusResultsInStatusCommand() {
    Builder builder = new Builder();

    builder.parseCommand("Status");

    assertThat(builder.getCommand()).isEqualTo(Command.STATUS);
  }

  @Test
  public void parseCommandWithMixedCaseResultsInCorrectCase() {
    Builder builder = new Builder();

    builder.parseCommand("sToP");

    assertThat(builder.getCommand()).isEqualTo(Command.STOP);
  }

  @Test
  public void parseCommandWithTwoCommandsWithSwitchesUsesFirstCommand() {
    Builder builder = new Builder();

    builder.parseCommand("--opt", "START", "-o", Command.STATUS.getName());

    assertThat(builder.getCommand()).isEqualTo(Command.START);
  }

  @Test
  public void parseCommandWithTwoCommandsWithoutSwitchesUsesFirstCommand() {
    Builder builder = new Builder();

    builder.parseCommand("START", Command.STATUS.getName());

    assertThat(builder.getCommand()).isEqualTo(Command.START);
  }

  @Test
  public void parseCommandWithBadInputResultsInDefaultCommand() {
    Builder builder = new Builder();

    builder.parseCommand("badCommandName", "--start", "stat");

    assertThat(builder.getCommand()).isEqualTo(Builder.DEFAULT_COMMAND);
  }

  @Test
  public void parseMemberNameWithNullStringArrayResultsInNull() {
    Builder builder = new Builder();

    builder.parseMemberName((String[]) null);

    assertThat(builder.getMemberName()).isNull();
  }

  @Test
  public void parseMemberNameWithEmptyStringArrayResultsInNull() {
    Builder builder = new Builder();

    builder.parseMemberName(); // empty String array

    assertThat(builder.getMemberName()).isNull();
  }

  @Test
  public void parseMemberNameWithCommandAndOptionsResultsInNull() {
    Builder builder = new Builder();

    builder.parseMemberName(Command.START.getName(), "--opt", "-o");

    assertThat(builder.getMemberName()).isNull();
  }

  @Test
  public void parseMemberNameWithStringUsesValue() {
    Builder builder = new Builder();

    builder.parseMemberName("memberOne");

    assertThat(builder.getMemberName()).isEqualTo("memberOne");
  }

  @Test
  public void buildCreatesServerLauncherWithBuilderValues() throws UnknownHostException {
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
  public void buildUsesMemberNameSetInApiProperties() {
    ServerLauncher launcher =
        new Builder().setCommand(ServerLauncher.Command.START).set(NAME, "serverABC").build();

    assertThat(launcher.getMemberName()).isNull();
    assertThat(launcher.getProperties().getProperty(NAME)).isEqualTo("serverABC");
  }

  @Test
  public void buildUsesMemberNameSetInSystemProperties() {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + NAME, "serverXYZ");

    ServerLauncher launcher = new Builder().setCommand(ServerLauncher.Command.START).build();

    assertThat(launcher.getMemberName()).isNull();
  }

  @Test
  public void getStartupCompletionActionReturnsNullByDefault() {
    assertThat(new Builder().getStartupCompletionAction()).isNull();
  }

  @Test
  public void setStartupCompletionActionReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setStartupCompletionAction(null)).isSameAs(builder);
  }

  @Test
  public void setStartupCompletionActionUsesValue() {
    Runnable myRunnable = () -> {
    };
    Builder builder = new Builder();
    builder.setStartupCompletionAction(myRunnable);
    assertThat(builder.getStartupCompletionAction())
        .isSameAs(myRunnable);

  }

  @Test
  public void getStartupExceptionActionReturnsNullByDefault() {
    assertThat(new Builder().getStartupExceptionAction()).isNull();
  }

  @Test
  public void setStartupExceptionActionReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setStartupExceptionAction(null)).isSameAs(builder);
  }

  @Test
  public void setStartupExceptionActionUsesValue() {
    Consumer<Throwable> myThrowable = (throwable) -> {
    };
    Builder builder = new Builder();
    builder.setStartupExceptionAction(myThrowable);
    assertThat(builder.getStartupExceptionAction())
        .isSameAs(myThrowable);

  }

  @Test
  public void getServerLauncherCacheProviderReturnsNullByDefault() {
    assertThat(new Builder().getServerLauncherCacheProvider()).isNull();
  }

  @Test
  public void setServerLauncherCacheProviderReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setServerLauncherCacheProvider(null)).isSameAs(builder);
  }

  @Test
  public void setServerLauncherCacheProviderUsesValue() {
    Builder builder = new Builder();

    ServerLauncherCacheProvider value = mock(ServerLauncherCacheProvider.class);
    builder.setServerLauncherCacheProvider(value);

    assertThat(builder.getServerLauncherCacheProvider()).isSameAs(value);
  }

  @Test
  public void getControllableProcessFactoryReturnsNullByDefault() {
    assertThat(new Builder().getControllableProcessFactory()).isNull();
  }

  @Test
  public void setControllableProcessFactoryReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setControllableProcessFactory(null)).isSameAs(builder);
  }

  @Test
  public void setControllableProcessFactoryUsesValue() {
    Builder builder = new Builder();

    Supplier<ControllableProcess> controllableProcessFactory =
        () -> mock(ControllableProcess.class);
    builder.setControllableProcessFactory(controllableProcessFactory);

    assertThat(builder.getControllableProcessFactory()).isSameAs(controllableProcessFactory);
  }
}
