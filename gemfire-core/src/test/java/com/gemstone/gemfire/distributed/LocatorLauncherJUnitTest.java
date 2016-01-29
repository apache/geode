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
package com.gemstone.gemfire.distributed;

import static org.junit.Assert.*;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.gemstone.gemfire.distributed.LocatorLauncher.Builder;
import com.gemstone.gemfire.distributed.LocatorLauncher.Command;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import joptsimple.OptionException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

/**
 * The LocatorLauncherJUnitTest class is a test suite of test cases for testing the contract and functionality of
 * launching a GemFire Locator.
 *
 * @author John Blum
 * @author Kirk Lund
 * @see com.gemstone.gemfire.distributed.LocatorLauncher
 * @see com.gemstone.gemfire.distributed.LocatorLauncher.Builder
 * @see com.gemstone.gemfire.distributed.LocatorLauncher.Command
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 7.0
 */
@Category(UnitTest.class)
public class LocatorLauncherJUnitTest {

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  
  @Rule
  public final TestName testName = new TestName();

  @Test(expected = IllegalArgumentException.class)
  public void testBuilderParseArgumentsWithNonNumericPort() {
    try {
      new Builder().parseArguments("start", "locator1", "--port", "oneTwoThree");
    }
    catch (IllegalArgumentException expected) {
      assertTrue(expected.getCause() instanceof OptionException);
      assertTrue(expected.getMessage(), expected.getMessage().contains(
        LocalizedStrings.Launcher_Builder_PARSE_COMMAND_LINE_ARGUMENT_ERROR_MESSAGE.toLocalizedString(
          "Locator", expected.getCause().getMessage())));
      throw expected;
    }
  }

  @Test
  public void testForceDefaultsToFalse() {
    assertFalse(new Builder().getForce());
  }

  @Test
  public void testForceSetToTrue() {
    Builder builder = new Builder();

    builder.parseArguments("start", "--force");

    assertTrue(Boolean.TRUE.equals(builder.getForce()));
  }

  @Test
  public void testSetAndGetCommand() {
    final Builder builder = new Builder();

    assertEquals(Builder.DEFAULT_COMMAND, builder.getCommand());
    assertSame(builder, builder.setCommand(Command.START));
    assertEquals(Command.START, builder.getCommand());
    assertSame(builder, builder.setCommand(Command.STATUS));
    assertEquals(Command.STATUS, builder.getCommand());
    assertSame(builder, builder.setCommand(Command.STOP));
    assertEquals(Command.STOP, builder.getCommand());
    assertSame(builder, builder.setCommand(null));
    assertEquals(Builder.DEFAULT_COMMAND, builder.getCommand());
  }

  @Test
  public void testSetAndGetBindAddress() throws UnknownHostException {
    final Builder builder = new Builder();

    assertNull(builder.getBindAddress());
    assertSame(builder, builder.setBindAddress(null));
    assertNull(builder.getBindAddress());
    assertSame(builder, builder.setBindAddress(""));
    assertNull(builder.getBindAddress());
    assertSame(builder, builder.setBindAddress("  "));
    assertNull(builder.getBindAddress());
    assertSame(builder, builder.setBindAddress(InetAddress.getLocalHost().getCanonicalHostName()));
    assertEquals(InetAddress.getLocalHost(), builder.getBindAddress());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetBindAddressToUnknownHost() {
    try {
      new Builder().setBindAddress("badhostname.badcompany.bad");
    }
    catch (IllegalArgumentException expected) {
      final String expectedMessage = LocalizedStrings.Launcher_Builder_UNKNOWN_HOST_ERROR_MESSAGE.toLocalizedString("Locator");
      assertEquals(expectedMessage, expected.getMessage());
      assertTrue(expected.getCause() instanceof UnknownHostException);
      throw expected;
    }
  }

  @Test
  public void testSetAndGetHostnameForClients() {
    final Builder builder = new Builder();

    assertNull(builder.getHostnameForClients());
    assertSame(builder, builder.setHostnameForClients("Pegasus"));
    assertEquals("Pegasus", builder.getHostnameForClients());
    assertSame(builder, builder.setHostnameForClients(null));
    assertNull(builder.getHostnameForClients());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetHostnameForClientsWithBlankString() {
    try {
      new Builder().setHostnameForClients(" ");
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.LocatorLauncher_Builder_INVALID_HOSTNAME_FOR_CLIENTS_ERROR_MESSAGE
        .toLocalizedString(), expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetHostnameForClientsWithEmptyString() {
    try {
      new Builder().setHostnameForClients("");
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.LocatorLauncher_Builder_INVALID_HOSTNAME_FOR_CLIENTS_ERROR_MESSAGE
        .toLocalizedString(), expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testSetAndGetMemberName() {
    Builder builder = new Builder();

    assertNull(builder.getMemberName());
    assertSame(builder, builder.setMemberName("locatorOne"));
    assertEquals("locatorOne", builder.getMemberName());
    assertSame(builder, builder.setMemberName(null));
    assertNull(builder.getMemberName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetMemberNameWithBlankString() {
    try {
      new Builder().setMemberName("  ");
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_MEMBER_NAME_ERROR_MESSAGE.toLocalizedString("Locator"),
        expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetMemberNameWithEmptyString() {
    try {
      new Builder().setMemberName("");
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_MEMBER_NAME_ERROR_MESSAGE.toLocalizedString("Locator"),
        expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testSetAndGetPid() {
    Builder builder = new Builder();

    assertNull(builder.getPid());
    assertSame(builder, builder.setPid(0));
    assertEquals(0, builder.getPid().intValue());
    assertSame(builder, builder.setPid(1));
    assertEquals(1, builder.getPid().intValue());
    assertSame(builder, builder.setPid(1024));
    assertEquals(1024, builder.getPid().intValue());
    assertSame(builder, builder.setPid(12345));
    assertEquals(12345, builder.getPid().intValue());
    assertSame(builder, builder.setPid(null));
    assertNull(builder.getPid());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetPidToInvalidValue() {
    try {
      new Builder().setPid(-1);
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_PID_ERROR_MESSAGE.toLocalizedString(), expected.getMessage());
      throw expected;
    }
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSetAndGetPort() {
    Builder builder = new Builder();

    assertEquals(LocatorLauncher.DEFAULT_LOCATOR_PORT, builder.getPort());
    assertSame(builder, builder.setPort(65535));
    assertEquals(65535, builder.getPort().intValue());
    assertSame(builder, builder.setPort(1024));
    assertEquals(1024, builder.getPort().intValue());
    assertSame(builder, builder.setPort(80));
    assertEquals(80, builder.getPort().intValue());
    assertSame(builder, builder.setPort(1));
    assertEquals(1, builder.getPort().intValue());
    assertSame(builder, builder.setPort(0));
    assertEquals(0, builder.getPort().intValue());
    assertSame(builder, builder.setPort(null));
    assertEquals(LocatorLauncher.DEFAULT_LOCATOR_PORT, builder.getPort());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetPortToOverflow() {
    try {
      new Builder().setPort(65536);
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_INVALID_PORT_ERROR_MESSAGE.toLocalizedString("Locator"),
        expected.getMessage());
      throw expected;
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetPortToUnderflow() {
    try {
      new Builder().setPort(-1);
    }
    catch (IllegalArgumentException expected) {
      assertEquals(LocalizedStrings.Launcher_Builder_INVALID_PORT_ERROR_MESSAGE.toLocalizedString("Locator"),
        expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testBuild() throws Exception {
    Builder builder = new Builder();

    LocatorLauncher launcher = builder.setCommand(Command.START)
      .setDebug(true)
      .setHostnameForClients("beanstock.vmware.com")
      .setMemberName("Beanstock")
      .setPort(8192)
      .build();

    assertNotNull(launcher);
    assertEquals(builder.getCommand(), launcher.getCommand());
    assertTrue(launcher.isDebugging());
    assertEquals(builder.getHostnameForClients(), launcher.getHostnameForClients());
    assertEquals(builder.getMemberName(), launcher.getMemberName());
    assertEquals(builder.getPort(), launcher.getPort());
    assertEquals(builder.getWorkingDirectory(), launcher.getWorkingDirectory());
    assertFalse(launcher.isHelping());
    assertFalse(launcher.isRunning());
  }

  @Test
  public void testBuildWithMemberNameSetInApiPropertiesOnStart() {
    LocatorLauncher launcher = new Builder()
      .setCommand(LocatorLauncher.Command.START)
      .setMemberName(null)
      .set(DistributionConfig.NAME_NAME, "locatorABC")
      .build();

    assertNotNull(launcher);
    assertEquals(LocatorLauncher.Command.START, launcher.getCommand());
    assertNull(launcher.getMemberName());
    assertEquals("locatorABC", launcher.getProperties().getProperty(DistributionConfig.NAME_NAME));
  }

  @Test
  public void testBuildWithMemberNameSetInSystemPropertiesOnStart() {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + DistributionConfig.NAME_NAME, "locatorXYZ");

    LocatorLauncher launcher = new Builder()
      .setCommand(LocatorLauncher.Command.START)
      .setMemberName(null)
      .build();

    assertNotNull(launcher);
    assertEquals(LocatorLauncher.Command.START, launcher.getCommand());
    assertNull(launcher.getMemberName());
  }
}
