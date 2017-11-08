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

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import org.apache.geode.distributed.LocatorLauncher;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.management.internal.cli.shell.Gfsh;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;

@Category(UnitTest.class)
public class StartLocatorCommandTest {
  private StartLocatorCommand locatorCommands;

  @Before
  public void setup() {
    locatorCommands = new StartLocatorCommand();
  }

  @After
  public void tearDown() {
    locatorCommands = null;
  }

  @Test
  public void testLocatorClasspathOrder() {
    String userClasspath = "/path/to/user/lib/app.jar:/path/to/user/classes";
    String expectedClasspath =
        StartMemberUtils.getGemFireJarPath().concat(File.pathSeparator).concat(userClasspath)
            .concat(File.pathSeparator).concat(System.getProperty("java.class.path"))
            .concat(File.pathSeparator).concat(StartMemberUtils.CORE_DEPENDENCIES_JAR_PATHNAME);
    String actualClasspath = locatorCommands.getLocatorClasspath(true, userClasspath);
    assertEquals(expectedClasspath, actualClasspath);
  }

  @Test
  public void testLocatorCommandLineWithRestAPI() throws Exception {
    LocatorLauncher locatorLauncher =
        new LocatorLauncher.Builder().setCommand(LocatorLauncher.Command.START)
            .setMemberName("testLocatorCommandLineWithRestAPI").setBindAddress("localhost")
            .setPort(11111).build();

    Properties gemfireProperties = new Properties();
    gemfireProperties.setProperty(HTTP_SERVICE_PORT, "8089");
    gemfireProperties.setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");

    String[] commandLineElements = locatorCommands.createStartLocatorCommandLine(locatorLauncher,
        null, null, gemfireProperties, null, false, new String[0], null, null);

    assertNotNull(commandLineElements);
    assertTrue(commandLineElements.length > 0);

    Set<String> expectedCommandLineElements = new HashSet<>(6);

    expectedCommandLineElements.add(locatorLauncher.getCommand().getName());
    expectedCommandLineElements.add(locatorLauncher.getMemberName().toLowerCase());
    expectedCommandLineElements.add(String.format("--port=%1$d", locatorLauncher.getPort()));
    expectedCommandLineElements
        .add("-d" + DistributionConfig.GEMFIRE_PREFIX + "" + HTTP_SERVICE_PORT + "=" + "8089");
    expectedCommandLineElements.add("-d" + DistributionConfig.GEMFIRE_PREFIX + ""
        + HTTP_SERVICE_BIND_ADDRESS + "=" + "localhost");

    for (String commandLineElement : commandLineElements) {
      expectedCommandLineElements.remove(commandLineElement.toLowerCase());
    }

    assertTrue(String.format("Expected ([]); but was (%1$s)", expectedCommandLineElements),
        expectedCommandLineElements.isEmpty());
  }

  @Test
  public void testAddJvmOptionsForOutOfMemoryErrors() {
    final List<String> jvmOptions = new ArrayList<>(1);

    addJvmOptionsForOutOfMemoryErrors(jvmOptions);

    if (SystemUtils.isHotSpotVM()) {
      if (SystemUtils.isWindows()) {
        assertTrue(jvmOptions.contains("-XX:OnOutOfMemoryError=taskkill /F /PID %p"));
      } else {
        assertTrue(jvmOptions.contains("-XX:OnOutOfMemoryError=kill -KILL %p"));
      }
    } else if (SystemUtils.isJ9VM()) {
      assertEquals(1, jvmOptions.size());
      assertTrue(jvmOptions.contains("-Xcheck:memory"));
    } else if (SystemUtils.isJRockitVM()) {
      assertEquals(1, jvmOptions.size());
      assertTrue(jvmOptions.contains("-XXexitOnOutOfMemory"));
    } else {
      assertTrue(jvmOptions.isEmpty());
    }
  }

  private void addJvmOptionsForOutOfMemoryErrors(final List<String> commandLine) {
    if (SystemUtils.isHotSpotVM()) {
      if (SystemUtils.isWindows()) {
        // ProcessBuilder "on Windows" needs every word (space separated) to be
        // a different element in the array/list. See #47312. Need to study why!
        commandLine.add("-XX:OnOutOfMemoryError=taskkill /F /PID %p");
      } else { // All other platforms (Linux, Mac OS X, UNIX, etc)
        commandLine.add("-XX:OnOutOfMemoryError=kill -KILL %p");
      }
    } else if (SystemUtils.isJ9VM()) {
      // NOTE IBM states the following IBM J9 JVM command-line option/switch has side-effects on
      // "performance",
      // as noted in the reference documentation...
      // http://publib.boulder.ibm.com/infocenter/javasdk/v6r0/index.jsp?topic=/com.ibm.java.doc.diagnostics.60/diag/appendixes/cmdline/commands_jvm.html
      commandLine.add("-Xcheck:memory");
    } else if (SystemUtils.isJRockitVM()) {
      // NOTE the following Oracle JRockit JVM documentation was referenced to identify the
      // appropriate JVM option to
      // set when handling OutOfMemoryErrors.
      // http://docs.oracle.com/cd/E13150_01/jrockit_jvm/jrockit/jrdocs/refman/optionXX.html
      commandLine.add("-XXexitOnOutOfMemory");
    }
  }

  private static final String FAKE_HOSTNAME = "someFakeHostname";

  @Rule
  public GfshParserRule commandRule = new GfshParserRule();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private StartLocatorCommand spy;

  @Before
  public void before() throws Exception {
    spy = Mockito.spy(StartLocatorCommand.class);
    doReturn(mock(Gfsh.class)).when(spy).getGfsh();
  }

  @Test
  public void startLocatorWorksWithNoOptions() throws Exception {
    thrown.expect(NullPointerException.class);
    commandRule.executeCommandWithInstance(spy, "start locator");

    ArgumentCaptor<Properties> gemfirePropertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(spy).createStartLocatorCommandLine(any(), any(), any(),
        gemfirePropertiesCaptor.capture(), any(), any(), any(), any(), any());

    Properties gemfireProperties = gemfirePropertiesCaptor.getValue();
    assertThat(gemfireProperties).containsKey(ENABLE_CLUSTER_CONFIGURATION);
    assertThat(gemfireProperties.get(ENABLE_CLUSTER_CONFIGURATION)).isEqualTo("true");
  }

  @Test
  public void startLocatorRespectsJmxManagerHostnameForClients() throws Exception {
    String startLocatorCommand = new CommandStringBuilder("start locator")
        .addOption(JMX_MANAGER_HOSTNAME_FOR_CLIENTS, FAKE_HOSTNAME).toString();

    thrown.expect(NullPointerException.class);
    commandRule.executeCommandWithInstance(spy, startLocatorCommand);


    ArgumentCaptor<Properties> gemfirePropertiesCaptor = ArgumentCaptor.forClass(Properties.class);
    verify(spy).createStartLocatorCommandLine(any(), any(), any(),
        gemfirePropertiesCaptor.capture(), any(), any(), any(), any(), any());

    Properties gemfireProperties = gemfirePropertiesCaptor.getValue();
    assertThat(gemfireProperties).containsKey(JMX_MANAGER_HOSTNAME_FOR_CLIENTS);
    assertThat(gemfireProperties.get(JMX_MANAGER_HOSTNAME_FOR_CLIENTS)).isEqualTo(FAKE_HOSTNAME);
  }
}
