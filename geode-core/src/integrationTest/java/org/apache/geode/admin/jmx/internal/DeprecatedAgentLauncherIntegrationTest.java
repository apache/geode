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
package org.apache.geode.admin.jmx.internal;

import static java.lang.System.lineSeparator;
import static org.apache.geode.admin.jmx.AgentConfig.DEFAULT_PROPERTY_FILE;
import static org.apache.geode.admin.jmx.internal.AgentConfigImpl.AGENT_PROPSFILE_PROPERTY_NAME;
import static org.apache.geode.admin.jmx.internal.AgentLauncher.AGENT_PROPS;
import static org.apache.geode.admin.jmx.internal.AgentLauncher.APPENDTO_LOG_FILE;
import static org.apache.geode.admin.jmx.internal.AgentLauncher.DIR;
import static org.apache.geode.admin.jmx.internal.AgentLauncher.RUNNING;
import static org.apache.geode.admin.jmx.internal.AgentLauncher.SHUTDOWN;
import static org.apache.geode.admin.jmx.internal.AgentLauncher.STARTING;
import static org.apache.geode.admin.jmx.internal.AgentLauncher.VMARGS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assume.assumeFalse;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.admin.jmx.internal.AgentLauncher.Status;
import org.apache.geode.internal.lang.SystemUtils;
import org.apache.geode.test.process.ProcessWrapper;

public class DeprecatedAgentLauncherIntegrationTest {

  private static final long TIMEOUT = getTimeout().toMillis();

  private String classpath;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    assumeFalse(SystemUtils.isWindows());

    this.classpath = System.getProperty("java.class.path");
    assertThat(this.classpath).isNotEmpty();
  }

  @Test
  public void testGetStartOptions() throws Exception {
    final String[] commandLineArguments =
        {"start", "appendto-log-file=true", "log-level=warn", "mcast-port=0",
            "-dir=" + temporaryFolder.getRoot().getAbsolutePath(), "-J-Xms256M", "-J-Xmx1024M"};

    final AgentLauncher launcher = new AgentLauncher("Agent");

    final Map<String, Object> startOptions = launcher.getStartOptions(commandLineArguments);

    assertThat(startOptions).isNotNull();
    assertThat(startOptions.get(APPENDTO_LOG_FILE)).isEqualTo("true");
    assertThat(startOptions.get(DIR)).isEqualTo(temporaryFolder.getRoot());

    final Properties props = (Properties) startOptions.get(AGENT_PROPS);

    assertThat(props).isNotNull();
    assertThat(props).hasSize(2);
    assertThat(props.getProperty("log-level")).isEqualTo("warn");
    assertThat(props.getProperty("mcast-port")).isEqualTo("0");

    final List<String> vmArgs = (List<String>) startOptions.get(VMARGS);

    assertThat(vmArgs).isNotNull();
    assertThat(vmArgs).hasSize(2);
    assertThat(vmArgs).contains("-Xms256M");
    assertThat(vmArgs).contains("-Xmx1024M");

    // now assert the System property 'gfAgentPropertyFile'
    assertThat(System.getProperty(AGENT_PROPSFILE_PROPERTY_NAME))
        .isEqualTo(new File(temporaryFolder.getRoot(), DEFAULT_PROPERTY_FILE).getPath());
  }

  /**
   * Test to verify fix for TRAC #44658.
   * <p>
   *
   * TRAC #44658: Agent ignores 'property-file' command line option (regression in 6.6.2)
   */
  @Test
  public void testGetStartOptionsWithPropertyFileOption() throws Exception {
    final String[] commandLineArguments = {"start",
        "-dir=" + temporaryFolder.getRoot().getAbsolutePath(), "-J-Xms512M", "log-level=warn",
        "mcast-port=0", "property-file=/path/to/custom/property/file.properties",};

    final AgentLauncher launcher = new AgentLauncher("Agent");

    final Map<String, Object> startOptions = launcher.getStartOptions(commandLineArguments);

    assertThat(startOptions).isNotNull();
    assertThat(startOptions).isNotEmpty();
    assertThat(startOptions.get(DIR)).isEqualTo(temporaryFolder.getRoot());

    final Properties props = (Properties) startOptions.get(AGENT_PROPS);

    assertThat(props).isNotNull();
    assertThat(props).hasSize(3);
    assertThat(props.getProperty("log-level")).isEqualTo("warn");
    assertThat(props.getProperty("mcast-port")).isEqualTo("0");
    assertThat(props.getProperty(AgentConfigImpl.PROPERTY_FILE_NAME))
        .isEqualTo("/path/to/custom/property/file.properties");

    final List<String> vmArgs = (List<String>) startOptions.get(VMARGS);

    assertThat(vmArgs).isNotNull();
    assertThat(vmArgs).hasSize(1);
    assertThat(vmArgs).contains("-Xms512M");

    // now assert the System property 'gfAgentPropertyFile'
    assertThat(System.getProperty(AGENT_PROPSFILE_PROPERTY_NAME))
        .isEqualTo("/path/to/custom/property/file.properties");
  }

  @Test
  public void testGetStopOptions() throws Exception {
    final String[] commandLineArguments =
        {"stop", "-dir=" + temporaryFolder.getRoot().getAbsolutePath()};

    final AgentLauncher launcher = new AgentLauncher("Agent");

    final Map<String, Object> stopOptions = launcher.getStopOptions(commandLineArguments);

    assertThat(stopOptions).isNotNull();
    assertThat(stopOptions.get(DIR)).isEqualTo(temporaryFolder.getRoot());
  }

  @Test
  public void testCreateStatus() throws Exception {
    final Status status = AgentLauncher.createStatus("agent", RUNNING, 12345);

    assertThat(status).isNotNull();
    assertThat(status.baseName).isEqualTo("agent");
    assertThat(status.state).isEqualTo(RUNNING);
    assertThat(status.pid).isEqualTo(12345);
    assertThat(status.msg).isNull();
    assertThat(status.exception).isNull();
  }

  @Test
  public void testCreateStatusWithMessageAndException() throws Exception {
    final Status status = AgentLauncher.createStatus("agent", STARTING, 11235, "Test Message!",
        new Exception("Test Exception!"));

    assertThat(status).isNotNull();
    assertThat(status.baseName).isEqualTo("agent");
    assertThat(status.state).isEqualTo(STARTING);
    assertThat(status.pid).isEqualTo(11235);
    assertThat(status.msg).isEqualTo("Test Message!");
    assertThat(status.exception.getMessage()).isEqualTo("Test Exception!");
  }

  @Test
  public void testGetStatusWhenStatusFileDoesNotExists() throws Exception {
    final AgentLauncher launcher = new AgentLauncher("Agent");

    final Status status = launcher.getStatus();

    assertAgentLauncherStatus(status, "Agent", SHUTDOWN, 0);
    assertThat(status.msg)
        .isEqualTo(String.format("%s is not running in the specified working directory: (%s).",
            "Agent", null));
    assertThat(status.exception).isNull();
  }

  @Test
  public void testPause() throws Exception {
    final long t0 = System.currentTimeMillis();
    AgentLauncher.pause(100);
    final long t1 = System.currentTimeMillis();
    assertThat(t1 - t0).isGreaterThanOrEqualTo(100);
  }

  @Test
  public void testStartStatusAndStop() throws Exception {
    final File agentWorkingDirectory = getAgentWorkingDirectory(testName.getMethodName());
    final File agentStatusFile = new File(agentWorkingDirectory, ".agent.ser");

    assertThat(deleteAgentWorkingDirectory(agentWorkingDirectory)).isTrue();

    assertThat(agentWorkingDirectory.mkdir()).isTrue();

    assertThat(agentStatusFile).doesNotExist();

    runAgent("Starting JMX Agent with pid: \\d+", "start", "mcast-port=0", "http-enabled=false",
        "rmi-enabled=false", "snmp-enabled=false", "-classpath=" + this.classpath,
        "-dir=" + agentWorkingDirectory.getAbsolutePath());

    assertThat(agentStatusFile).exists();

    runAgent("Agent pid: \\d+ status: running", "status",
        "-dir=" + agentWorkingDirectory.getAbsolutePath());

    runAgent("The Agent has shut down.", "stop", "-dir=" + agentWorkingDirectory.getAbsolutePath());

    assertThat(agentStatusFile).doesNotExist();
  }

  @Test
  public void testWriteReadAndDeleteStatus() throws Exception {
    final File expectedStatusFile = new File(temporaryFolder.getRoot(), ".agent.ser");
    final AgentLauncher launcher = new AgentLauncher("Agent");

    launcher.getStartOptions(new String[] {"-dir=" + temporaryFolder.getRoot().getAbsolutePath()});

    assertThat(expectedStatusFile).doesNotExist();

    final AgentLauncher.Status expectedStatus = AgentLauncher.createStatus("agent", RUNNING, 13579);

    assertAgentLauncherStatus(expectedStatus, "agent", RUNNING, 13579);

    launcher.writeStatus(expectedStatus, temporaryFolder.getRoot());

    assertThat(expectedStatusFile).exists();

    final AgentLauncher.Status actualStatus = launcher.readStatus(temporaryFolder.getRoot());

    assertThat(actualStatus).isNotNull();
    assertAgentLauncherStatusEquals(expectedStatus, actualStatus);
    assertThat(expectedStatusFile).exists();

    launcher.deleteStatus(temporaryFolder.getRoot());

    assertThat(expectedStatusFile).doesNotExist();
  }

  private void assertAgentLauncherStatusEquals(final AgentLauncher.Status expected,
      final AgentLauncher.Status actual) {
    assertThat(actual.baseName).isEqualTo(expected.baseName);
    assertThat(actual.state).isEqualTo(expected.state);
    assertThat(actual.pid).isEqualTo(expected.pid);
  }

  private void assertAgentLauncherStatus(final AgentLauncher.Status actual,
      final String expectedBasename, final int expectedState, final int expectedPid) {
    assertThat(actual).isNotNull();
    assertThat(actual.baseName).isEqualTo(expectedBasename);
    assertThat(actual.state).isEqualTo(expectedState);
    assertThat(actual.pid).isEqualTo(expectedPid);
  }

  private static boolean deleteAgentWorkingDirectory(final File agentWorkingDirectory) {
    return !agentWorkingDirectory.exists() || deleteFileRecursive(agentWorkingDirectory);
  }

  private static boolean deleteFileRecursive(final File file) {
    boolean result = true;

    if (file.isDirectory()) {
      for (final File childFile : file.listFiles()) {
        result &= deleteFileRecursive(childFile);
      }
    }

    return (result && file.delete());
  }

  private File getAgentWorkingDirectory(final String testCaseName) throws IOException {
    return temporaryFolder.newFolder("AgentLauncherTest_" + testCaseName);
  }

  private static void runAgent(final String processOutputPattern, final String... args)
      throws Exception {
    final ProcessWrapper agentProcess =
        new ProcessWrapper.Builder().mainClass(AgentLauncher.class).mainArguments(args).build();

    agentProcess.execute();

    if (processOutputPattern != null) {
      agentProcess.waitForOutputToMatch(processOutputPattern);
    }
    assertThat(agentProcess.waitFor(TIMEOUT)).as("Expecting process started with:" + lineSeparator()
        + " " + Arrays.asList(args) + lineSeparator() + "with output:" + lineSeparator() + " "
        + agentProcess.getOutput(true) + lineSeparator() + "to terminate with exit code: 0"
        + lineSeparator() + "but waitFor is still waiting after timeout: " + TIMEOUT + " seconds.")
        .isEqualTo(0);
  }

}
