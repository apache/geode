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
import static org.apache.geode.internal.DistributionLocator.DEFAULT_LOCATOR_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import java.net.UnknownHostException;

import joptsimple.OptionException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.distributed.LocatorLauncher.Builder;
import org.apache.geode.distributed.LocatorLauncher.Command;
import org.apache.geode.distributed.internal.DistributionConfig;

/**
 * Unit tests for {@link LocatorLauncher.Builder}
 */
public class LocatorLauncherBuilderTest {

  private InetAddress localHost;

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    localHost = InetAddress.getLocalHost();
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
  public void getForceReturnsFalseByDefault() {
    assertThat(new Builder().getForce()).isFalse();
  }

  @Test
  public void getForegroundReturnsFalseByDefault() {
    assertThat(new Builder().getForeground()).isFalse();
  }

  @Test
  public void getBindAddressReturnsNullByDefault() {
    Builder builder = new Builder();

    assertThat(builder.getBindAddress()).isNull();
  }

  @Test
  public void getHostnameForClientsReturnsNullByDefault() {
    assertThat(new Builder().getHostnameForClients()).isNull();
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
    assertThat(new LocatorLauncher.Builder().getRedirectOutput()).isNull();
  }

  @Test
  public void getPortReturnsDefaultLocatorPortByDefault() {
    assertThat(new Builder().getPort()).isEqualTo(Integer.valueOf(DEFAULT_LOCATOR_PORT));
  }

  @Test
  public void setCommandReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setCommand(Command.STATUS)).isSameAs(builder);
  }

  @Test
  public void setForceReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setForce(true)).isSameAs(builder);
  }

  @Test
  public void setForegroundReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setForeground(true)).isSameAs(builder);
  }

  @Test
  public void setHostNameForClientsReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setHostnameForClients("Pegasus")).isSameAs(builder);
  }

  @Test
  public void setMemberNameReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setMemberName("serverOne")).isSameAs(builder);
  }

  @Test
  public void setBindAddressReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setBindAddress(null)).isSameAs(builder);
  }

  @Test
  public void setPortReturnsBuilderInstance() {
    Builder builder = new Builder();

    assertThat(builder.setPort(null)).isSameAs(builder);
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
  public void setBindAddressToNullResultsInNull() {
    Builder builder = new Builder();

    builder.setBindAddress(null);

    assertThat(builder.getBindAddress()).isNull();
  }

  @Test
  public void setBindAddressToEmptyStringResultsInNull() {
    Builder builder = new Builder();

    builder.setBindAddress("");

    assertThat(builder.getBindAddress()).isNull();
  }

  @Test
  public void setBindAddressToBlankStringResultsInNull() {
    Builder builder = new Builder();

    builder.setBindAddress("  ");

    assertThat(builder.getBindAddress()).isNull();
  }

  @Test
  public void setBindAddressToCanonicalLocalHostUsesValue() throws Exception {
    Builder builder = new Builder().setBindAddress(InetAddress.getLocalHost().getHostName());

    assertThat(builder.getBindAddress()).isEqualTo(localHost);
  }

  @Test
  public void setBindAddressToLocalHostNameUsesValue() throws Exception {
    String host = InetAddress.getLocalHost().getHostName();

    Builder builder = new Builder().setBindAddress(host);

    assertThat(builder.getBindAddress()).isEqualTo(localHost);
  }

  @Test
  public void setBindAddressToUnknownHostThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setBindAddress("badhostname.badcompany.bad"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasCauseInstanceOf(UnknownHostException.class);
  }

  @Test
  public void setBindAddressToNonLocalHostThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setBindAddress("yahoo.com"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setForceToTrueUsesValue() {
    ServerLauncher.Builder builder = new ServerLauncher.Builder();

    builder.setForce(true);

    assertThat(builder.getForce()).isTrue();
  }

  @Test
  public void setForegroundToTrueUsesValue() {
    Builder builder = new Builder().setForeground(true);

    assertThat(builder.getForeground()).isTrue();
  }

  @Test
  public void setHostnameForClientsToStringUsesValue() {
    Builder builder = new Builder();

    builder.setHostnameForClients("Pegasus");

    assertThat(builder.getHostnameForClients()).isEqualTo("Pegasus");
  }

  @Test
  public void setHostnameForClientsToBlankStringThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setHostnameForClients(" "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setHostnameForClientsToEmptyStringThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setHostnameForClients(""))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test

  public void setHostnameForClientsToNullThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setHostnameForClients(null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setMemberNameToStringUsesValue() {
    Builder builder = new Builder();

    builder.setMemberName("locatorOne");

    assertThat(builder.getMemberName()).isEqualTo("locatorOne");
  }

  @Test
  public void setMemberNameWithBlankStringThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setMemberName("  "))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setMemberNameWithEmptyStringThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setMemberName(""))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setMemberNameWithNullStringThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setMemberName(null))
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
  public void setPortToNullUsesDefaultLocatorPort() {
    Builder builder = new Builder();

    assertThat(builder.setPort(null).getPort()).isEqualTo(Integer.valueOf(DEFAULT_LOCATOR_PORT));
  }

  @Test
  public void setPortToZeroOrGreaterUsesValue() {
    Builder builder = new Builder();

    builder.setPort(0);
    assertThat(builder.getPort().intValue()).isEqualTo(0);

    builder.setPort(1);
    assertThat(builder.getPort().intValue()).isEqualTo(1);

    builder.setPort(80);
    assertThat(builder.getPort().intValue()).isEqualTo(80);

    builder.setPort(1024);
    assertThat(builder.getPort().intValue()).isEqualTo(1024);

    builder.setPort(65535);
    assertThat(builder.getPort().intValue()).isEqualTo(65535);
  }

  @Test
  public void setPortAboveMaxValueThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setPort(65536))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setPortToNegativeValueThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setPort(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setRedirectOutputReturnsBuilderInstance() {
    LocatorLauncher.Builder builder = new LocatorLauncher.Builder();

    assertThat(builder.setRedirectOutput(Boolean.TRUE)).isSameAs(builder);
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
        () -> new Builder().parseArguments("start", "locator1", "--port", "oneTwoThree"))
            .isInstanceOf(IllegalArgumentException.class).hasCauseInstanceOf(OptionException.class);
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
  public void buildCreatesLocatorLauncherWithBuilderValues() {
    Builder builder = new Builder();

    LocatorLauncher launcher = builder.setCommand(Command.START).setDebug(true)
        .setHostnameForClients("beanstock.vmware.com").setMemberName("Beanstock").setPort(8192)
        .setRedirectOutput(Boolean.TRUE).setForeground(false).build();

    assertThat(launcher.getCommand()).isEqualTo(builder.getCommand());
    assertThat(launcher.isDebugging()).isTrue();
    assertThat(launcher.isForeground()).isFalse();
    assertThat(launcher.getHostnameForClients()).isEqualTo(builder.getHostnameForClients());
    assertThat(launcher.getMemberName()).isEqualTo(builder.getMemberName());
    assertThat(launcher.getPort()).isEqualTo(builder.getPort());
    assertThat(launcher.getWorkingDirectory()).isEqualTo(builder.getWorkingDirectory());
    assertThat(launcher.isRedirectingOutput()).isTrue();

    assertThat(launcher.isHelping()).isFalse();
    assertThat(launcher.isRunning()).isFalse();
  }

  @Test
  public void buildUsesMemberNameSetInApiProperties() {
    LocatorLauncher launcher =
        new Builder().setCommand(LocatorLauncher.Command.START).set(NAME, "locatorABC").build();

    assertThat(launcher.getMemberName()).isNull();
    assertThat(launcher.getProperties().getProperty(NAME)).isEqualTo("locatorABC");
  }

  @Test
  public void buildUsesMemberNameSetInSystemPropertiesOnStart() {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + NAME, "locatorXYZ");

    LocatorLauncher launcher = new Builder().setCommand(LocatorLauncher.Command.START).build();

    assertThat(launcher.getCommand()).isEqualTo(LocatorLauncher.Command.START);
    assertThat(launcher.getMemberName()).isNull();
  }
}
