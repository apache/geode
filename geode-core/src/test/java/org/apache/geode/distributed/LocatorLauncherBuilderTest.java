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
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Unit tests for {@link LocatorLauncher.Builder}. Extracted from {@link LocatorLauncherTest}.
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
  public void defaultCommandIsUnspecified() throws Exception {
    assertThat(Builder.DEFAULT_COMMAND).isEqualTo(Command.UNSPECIFIED);
  }

  @Test
  public void getCommandReturnsUnspecifiedByDefault() throws Exception {
    assertThat(new Builder().getCommand()).isEqualTo(Builder.DEFAULT_COMMAND);
  }

  @Test
  public void getForceReturnsFalseByDefault() {
    assertThat(new Builder().getForce()).isFalse();
  }

  @Test
  public void getBindAddressReturnsNullByDefault() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.getBindAddress()).isNull();
  }

  @Test
  public void getHostnameForClientsReturnsNullByDefault() throws Exception {
    assertThat(new Builder().getHostnameForClients()).isNull();
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
    assertThat(new LocatorLauncher.Builder().getRedirectOutput()).isNull();
  }

  @Test
  public void getPortReturnsDefaultLocatorPortByDefault() throws Exception {
    assertThat(new Builder().getPort()).isEqualTo(Integer.valueOf(DEFAULT_LOCATOR_PORT));
  }

  @Test
  public void setCommandReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setCommand(Command.STATUS)).isSameAs(builder);
  }

  @Test
  public void setForceReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setForce(true)).isSameAs(builder);
  }

  @Test
  public void setHostNameForClientsReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setHostnameForClients("Pegasus")).isSameAs(builder);
  }

  @Test
  public void setMemberNameReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setMemberName("serverOne")).isSameAs(builder);
  }

  @Test
  public void setBindAddressReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setBindAddress(null)).isSameAs(builder);
  }

  @Test
  public void setPortReturnsBuilderInstance() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setPort(null)).isSameAs(builder);
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
  public void setBindAddressToNullResultsInNull() throws Exception {
    Builder builder = new Builder();

    builder.setBindAddress(null);

    assertThat(builder.getBindAddress()).isNull();
  }

  @Test
  public void setBindAddressToEmptyStringResultsInNull() throws Exception {
    Builder builder = new Builder();

    builder.setBindAddress("");

    assertThat(builder.getBindAddress()).isNull();
  }

  @Test
  public void setBindAddressToBlankStringResultsInNull() throws Exception {
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
  public void setForceToTrueUsesValue() throws Exception {
    ServerLauncher.Builder builder = new ServerLauncher.Builder();

    builder.setForce(true);

    assertThat(builder.getForce()).isTrue();
  }

  @Test
  public void setHostnameForClientsToStringUsesValue() throws Exception {
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
  public void setMemberNameToStringUsesValue() throws Exception {
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
  public void setPidToNegativeValueThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setPid(-1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setPortToNullUsesDefaultLocatorPort() throws Exception {
    Builder builder = new Builder();

    assertThat(builder.setPort(null).getPort()).isEqualTo(Integer.valueOf(DEFAULT_LOCATOR_PORT));
  }

  @Test
  public void setPortToZeroOrGreaterUsesValue() throws Exception {
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
  public void setPortAboveMaxValueThrowsIllegalArgumentException() throws Exception {
    assertThatThrownBy(() -> new Builder().setPort(65536))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setPortToNegativeValueThrowsIllegalArgumentException() {
    assertThatThrownBy(() -> new Builder().setPort(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void setRedirectOutputReturnsBuilderInstance() throws Exception {
    LocatorLauncher.Builder builder = new LocatorLauncher.Builder();

    assertThat(builder.setRedirectOutput(Boolean.TRUE)).isSameAs(builder);
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
        () -> new Builder().parseArguments("start", "locator1", "--port", "oneTwoThree"))
            .isInstanceOf(IllegalArgumentException.class).hasCauseInstanceOf(OptionException.class);
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
  public void buildCreatesLocatorLauncherWithBuilderValues() throws Exception {
    Builder builder = new Builder();

    LocatorLauncher launcher = builder.setCommand(Command.START).setDebug(true)
        .setHostnameForClients("beanstock.vmware.com").setMemberName("Beanstock").setPort(8192)
        .setRedirectOutput(Boolean.TRUE).build();

    assertThat(launcher.getCommand()).isEqualTo(builder.getCommand());
    assertThat(launcher.isDebugging()).isTrue();
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
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + NAME, "locatorXYZ");

    LocatorLauncher launcher = new Builder().setCommand(LocatorLauncher.Command.START).build();

    assertThat(launcher.getCommand()).isEqualTo(LocatorLauncher.Command.START);
    assertThat(launcher.getMemberName()).isNull();
  }
}
