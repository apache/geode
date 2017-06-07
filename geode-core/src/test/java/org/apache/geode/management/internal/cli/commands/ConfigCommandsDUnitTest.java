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

import static org.apache.commons.io.FileUtils.deleteDirectory;
import static org.apache.geode.distributed.ConfigurationProperties.ARCHIVE_FILE_SIZE_LIMIT;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_CONFIGURATION_DIR;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.apache.geode.internal.AvailablePort.SOCKET;
import static org.apache.geode.internal.AvailablePort.getRandomAvailablePort;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.Wait.waitForCriterion;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.logging.LogWriterImpl;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.remote.CommandProcessor;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Dunit class for testing GemFire config commands : export config
 *
 * @since GemFire 7.0
 */
@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class ConfigCommandsDUnitTest extends CliCommandTestBase {

  private File managerConfigFile;
  private File managerPropsFile;
  private File vm1ConfigFile;
  private File vm1PropsFile;
  private File vm2ConfigFile;
  private File vm2PropsFile;
  private File shellConfigFile;
  private File shellPropsFile;
  private File subDir;
  private File subManagerConfigFile;

  @Override
  protected final void postSetUpCliCommandTestBase() throws Exception {
    this.managerConfigFile = this.temporaryFolder.newFile("Manager-cache.xml");
    this.managerPropsFile = this.temporaryFolder.newFile("Manager-gf.properties");
    this.vm1ConfigFile = this.temporaryFolder.newFile("VM1-cache.xml");
    this.vm1PropsFile = this.temporaryFolder.newFile("VM1-gf.properties");
    this.vm2ConfigFile = this.temporaryFolder.newFile("VM2-cache.xml");
    this.vm2PropsFile = this.temporaryFolder.newFile("VM2-gf.properties");
    this.shellConfigFile = this.temporaryFolder.newFile("Shell-cache.xml");
    this.shellPropsFile = this.temporaryFolder.newFile("Shell-gf.properties");
    this.subDir = this.temporaryFolder.newFolder(getName());
    this.subManagerConfigFile = new File(this.subDir, this.managerConfigFile.getName());
  }

  @Test
  public void testDescribeConfig() throws Exception {
    setUpJmxManagerOnVm0ThenConnect(null);
    final String controllerName = "Member2";

    /*
     * Create properties for the controller VM
     */
    final Properties localProps = new Properties();
    localProps.setProperty(MCAST_PORT, "0");
    localProps.setProperty(LOG_LEVEL, "info");
    localProps.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    localProps.setProperty(ENABLE_TIME_STATISTICS, "true");
    localProps.setProperty(NAME, controllerName);
    localProps.setProperty(GROUPS, "G1");
    getSystem(localProps);
    Cache cache = getCache();

    int ports[] = getRandomAvailableTCPPorts(1);
    CacheServer cs = getCache().addCacheServer();
    cs.setPort(ports[0]);
    cs.setMaxThreads(10);
    cs.setMaxConnections(9);
    cs.start();
    try {

      RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
      List<String> jvmArgs = runtimeBean.getInputArguments();

      getLogWriter().info("#SB Actual JVM Args : ");

      for (String jvmArg : jvmArgs) {
        getLogWriter().info("#SB JVM " + jvmArg);
      }

      InternalDistributedSystem system = (InternalDistributedSystem) cache.getDistributedSystem();
      DistributionConfig config = system.getConfig();
      config.setArchiveFileSizeLimit(1000);

      String command = CliStrings.DESCRIBE_CONFIG + " --member=" + controllerName;
      CommandProcessor cmdProcessor = new CommandProcessor();
      cmdProcessor.createCommandStatement(command, Collections.EMPTY_MAP).process();

      CommandResult cmdResult = executeCommand(command);

      String resultStr = commandResultToString(cmdResult);
      getLogWriter().info("#SB Hiding the defaults\n" + resultStr);

      assertEquals(true, cmdResult.getStatus().equals(Status.OK));
      assertEquals(true, resultStr.contains("G1"));
      assertEquals(true, resultStr.contains(controllerName));
      assertEquals(true, resultStr.contains(ARCHIVE_FILE_SIZE_LIMIT));
      assertEquals(true, !resultStr.contains("copy-on-read"));

      cmdResult =
          executeCommand(command + " --" + CliStrings.DESCRIBE_CONFIG__HIDE__DEFAULTS + "=false");
      resultStr = commandResultToString(cmdResult);

      getLogWriter().info("#SB No hiding of defaults\n" + resultStr);

      assertEquals(true, cmdResult.getStatus().equals(Status.OK));
      assertEquals(true, resultStr.contains("is-server"));
      assertEquals(true, resultStr.contains(controllerName));
      assertEquals(true, resultStr.contains("copy-on-read"));

    } finally {
      cs.stop();
    }
  }

  @Category(FlakyTest.class) // GEODE-1449
  @Test
  public void testExportConfig() throws Exception {
    Properties localProps = new Properties();
    localProps.setProperty(NAME, "Manager");
    localProps.setProperty(GROUPS, "Group1");
    setUpJmxManagerOnVm0ThenConnect(localProps);

    // Create a cache in another VM (VM1)
    Host.getHost(0).getVM(1).invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(NAME, "VM1");
        localProps.setProperty(GROUPS, "Group2");
        getSystem(localProps);
        getCache();
      }
    });

    // Create a cache in a 3rd VM (VM2)
    Host.getHost(0).getVM(2).invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(NAME, "VM2");
        localProps.setProperty(GROUPS, "Group2");
        getSystem(localProps);
        getCache();
      }
    });

    // Create a cache in the local VM
    localProps = new Properties();
    localProps.setProperty(NAME, "Shell");
    getSystem(localProps);
    Cache cache = getCache();

    // Test export config for all members
    deleteTestFiles();
    CommandResult cmdResult =
        executeCommand("export config --dir=" + this.temporaryFolder.getRoot().getAbsolutePath());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    assertTrue(this.managerConfigFile + " should exist", this.managerConfigFile.exists());
    assertTrue(this.managerPropsFile + " should exist", this.managerPropsFile.exists());
    assertTrue(this.vm1ConfigFile + " should exist", this.vm1ConfigFile.exists());
    assertTrue(this.vm1PropsFile + " should exist", this.vm1PropsFile.exists());
    assertTrue(this.vm2ConfigFile + " should exist", this.vm2ConfigFile.exists());
    assertTrue(this.vm2PropsFile + " should exist", this.vm2PropsFile.exists());
    assertTrue(this.shellConfigFile + " should exist", this.shellConfigFile.exists());
    assertTrue(this.shellPropsFile + " should exist", this.shellPropsFile.exists());

    // Test exporting member
    deleteTestFiles();
    cmdResult = executeCommand(
        "export config --member=Manager --dir=" + this.temporaryFolder.getRoot().getAbsolutePath());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    assertTrue(this.managerConfigFile + " should exist", this.managerConfigFile.exists());
    assertFalse(this.vm1ConfigFile + " should not exist", this.vm1ConfigFile.exists());
    assertFalse(this.vm2ConfigFile + " should not exist", this.vm2ConfigFile.exists());
    assertFalse(this.shellConfigFile + " should not exist", this.shellConfigFile.exists());

    // Test exporting group
    deleteTestFiles();
    cmdResult = executeCommand(
        "export config --group=Group2 --dir=" + this.temporaryFolder.getRoot().getAbsolutePath());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    assertFalse(this.managerConfigFile + " should not exist", this.managerConfigFile.exists());
    assertTrue(this.vm1ConfigFile + " should exist", this.vm1ConfigFile.exists());
    assertTrue(this.vm2ConfigFile + " should exist", this.vm2ConfigFile.exists());
    assertFalse(this.shellConfigFile + " should not exist", this.shellConfigFile.exists());

    // Test export to directory
    deleteTestFiles();
    cmdResult = executeCommand("export config --dir=" + this.subDir.getAbsolutePath());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    assertFalse(this.managerConfigFile.exists());
    assertTrue(this.subManagerConfigFile.exists());

    // Test the contents of the file
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    CacheXmlGenerator.generate(cache, printWriter, false, false, false);
    String configToMatch = stringWriter.toString();

    deleteTestFiles();
    cmdResult = executeCommand(
        "export config --member=Shell --dir=" + this.temporaryFolder.getRoot().getAbsolutePath());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    char[] fileContents = new char[configToMatch.length()];
    FileReader reader = new FileReader(this.shellConfigFile);
    reader.read(fileContents);

    assertEquals(configToMatch, new String(fileContents));
  }

  @Test
  public void testAlterRuntimeConfig() throws Exception {
    final String controller = "controller";
    String directory = this.temporaryFolder.newFolder(controller).getAbsolutePath();
    String statFilePath = new File(directory, "stat.gfs").getAbsolutePath();

    setUpJmxManagerOnVm0ThenConnect(null);

    Properties localProps = new Properties();
    localProps.setProperty(NAME, controller);
    localProps.setProperty(LOG_LEVEL, "error");
    getSystem(localProps);

    final GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    final DistributionConfig config = cache.getSystem().getConfig();

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.MEMBER, controller);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL, "info");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "50");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, "32");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, "49");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, "2000");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, statFilePath);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED, "true");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "10");

    CommandResult cmdResult = executeCommand(csb.getCommandString());
    String resultString = commandResultToString(cmdResult);

    getLogWriter().info("Result\n");
    getLogWriter().info(resultString);

    assertEquals(true, cmdResult.getStatus().equals(Status.OK));
    assertEquals(LogWriterImpl.INFO_LEVEL, config.getLogLevel());
    assertEquals(50, config.getLogFileSizeLimit());
    assertEquals(32, config.getArchiveDiskSpaceLimit());
    assertEquals(2000, config.getStatisticSampleRate());
    assertEquals("stat.gfs", config.getStatisticArchiveFile().getName());
    assertEquals(true, config.getStatisticSamplingEnabled());
    assertEquals(10, config.getLogDiskSpaceLimit());

    CommandProcessor commandProcessor = new CommandProcessor();
    Result result =
        commandProcessor.createCommandStatement("alter runtime", Collections.EMPTY_MAP).process();
  }

  @Test
  public void testAlterRuntimeConfigRandom() throws Exception {
    IgnoredException.addIgnoredException(
        "java.lang.IllegalArgumentException: Could not set \"log-disk-space-limit\"");
    final String member1 = "VM1";
    final String controller = "controller";

    setUpJmxManagerOnVm0ThenConnect(null);

    Properties localProps = new Properties();
    localProps.setProperty(NAME, controller);
    localProps.setProperty(LOG_LEVEL, "error");
    getSystem(localProps);

    final GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    final DistributionConfig config = cache.getSystem().getConfig();

    Host.getHost(0).getVM(1).invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(NAME, member1);
        getSystem(localProps);
        getCache();
      }
    });

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    CommandResult cmdResult = executeCommand(csb.getCommandString());
    String resultAsString = commandResultToString(cmdResult);

    assertEquals(true, cmdResult.getStatus().equals(Status.ERROR));
    assertTrue(resultAsString.contains(CliStrings.ALTER_RUNTIME_CONFIG__RELEVANT__OPTION__MESSAGE));

    csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "2000000000");
    cmdResult = executeCommand(csb.getCommandString());
    resultAsString = commandResultToString(cmdResult);

    assertEquals(true, cmdResult.getStatus().equals(Status.ERROR));
    assertTrue(
        resultAsString.contains("Could not set \"log-disk-space-limit\" to \"2,000,000,000\""));
  }

  @Test
  public void testAlterRuntimeConfigOnAllMembers() throws Exception {
    final String member1 = "VM1";
    final String controller = "controller";

    String controllerDirectory = this.temporaryFolder.newFolder(controller).getAbsolutePath();
    String controllerStatFilePath = new File(controllerDirectory, "stat.gfs").getAbsolutePath();

    setUpJmxManagerOnVm0ThenConnect(null);

    Properties localProps = new Properties();
    localProps.setProperty(NAME, controller);
    localProps.setProperty(LOG_LEVEL, "error");
    getSystem(localProps);

    final GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    final DistributionConfig config = cache.getSystem().getConfig();

    Host.getHost(0).getVM(1).invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(NAME, member1);
        getSystem(localProps);
        Cache cache = getCache();
      }
    });

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL, "info");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "50");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, "32");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, "49");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, "2000");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE,
        controllerStatFilePath);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED, "true");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "10");

    CommandResult cmdResult = executeCommand(csb.getCommandString());
    String resultString = commandResultToString(cmdResult);

    getLogWriter().info("#SB Result\n");
    getLogWriter().info(resultString);

    assertEquals(true, cmdResult.getStatus().equals(Status.OK));
    assertEquals(LogWriterImpl.INFO_LEVEL, config.getLogLevel());
    assertEquals(50, config.getLogFileSizeLimit());
    assertEquals(49, config.getArchiveFileSizeLimit());
    assertEquals(32, config.getArchiveDiskSpaceLimit());
    assertEquals(2000, config.getStatisticSampleRate());
    assertEquals("stat.gfs", config.getStatisticArchiveFile().getName());
    assertEquals(true, config.getStatisticSamplingEnabled());
    assertEquals(10, config.getLogDiskSpaceLimit());

    // Validate the changes in the vm1
    Host.getHost(0).getVM(1).invoke(new SerializableRunnable() {
      public void run() {
        GemFireCacheImpl cacheVM1 = (GemFireCacheImpl) getCache();
        DistributionConfig configVM1 = cacheVM1.getSystem().getConfig();

        assertEquals(LogWriterImpl.INFO_LEVEL, configVM1.getLogLevel());
        assertEquals(50, configVM1.getLogFileSizeLimit());
        assertEquals(49, configVM1.getArchiveFileSizeLimit());
        assertEquals(32, configVM1.getArchiveDiskSpaceLimit());
        assertEquals(2000, configVM1.getStatisticSampleRate());
        assertEquals("stat.gfs", configVM1.getStatisticArchiveFile().getName());
        assertEquals(true, configVM1.getStatisticSamplingEnabled());
        assertEquals(10, configVM1.getLogDiskSpaceLimit());
      }
    });
  }

  /**
   * Asserts that altering the runtime config correctly updates the shared configuration.
   */
  @Test
  public void testAlterUpdatesSharedConfig() throws Exception {
    final String groupName = getName();
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    jmxPort = ports[0];
    httpPort = ports[1];
    try {
      jmxHost = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException ignore) {
      jmxHost = "localhost";
    }

    // Start the Locator and wait for shared configuration to be available
    final int locatorPort = getRandomAvailablePort(SOCKET);
    final String locatorDirectory = this.temporaryFolder.newFolder("Locator").getAbsolutePath();

    final Properties locatorProps = new Properties();
    locatorProps.setProperty(NAME, "Locator");
    locatorProps.setProperty(MCAST_PORT, "0");
    locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
    locatorProps.setProperty(CLUSTER_CONFIGURATION_DIR, locatorDirectory);
    locatorProps.setProperty(JMX_MANAGER, "true");
    locatorProps.setProperty(JMX_MANAGER_START, "true");
    locatorProps.setProperty(JMX_MANAGER_BIND_ADDRESS, String.valueOf(jmxHost));
    locatorProps.setProperty(JMX_MANAGER_PORT, String.valueOf(jmxPort));
    locatorProps.setProperty(HTTP_SERVICE_PORT, String.valueOf(httpPort));

    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        final File locatorLogFile = new File(locatorDirectory, "locator-" + locatorPort + ".log");
        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locatorPort,
              locatorLogFile, null, locatorProps);

          WaitCriterion wc = new WaitCriterion() {
            @Override
            public boolean done() {
              return locator.isSharedConfigurationRunning();
            }

            @Override
            public String description() {
              return "Waiting for shared configuration to be started";
            }
          };
          waitForCriterion(wc, 5000, 500, true);

        } catch (IOException e) {
          fail("Unable to create a locator with a shared configuration", e);
        }
      }
    });

    connect(jmxHost, jmxPort, httpPort, getDefaultShell());

    // Create a cache in VM 1
    VM vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties localProps = new Properties();
        localProps.setProperty(MCAST_PORT, "0");
        localProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
        localProps.setProperty(LOG_LEVEL, "error");
        localProps.setProperty(GROUPS, groupName);
        getSystem(localProps);

        assertNotNull(getCache());
        assertEquals("error", basicGetSystem().getConfig().getAttribute(LOG_LEVEL));
        return null;
      }
    });

    // Test altering the runtime config
    CommandStringBuilder commandStringBuilder =
        new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    commandStringBuilder.addOption(CliStrings.GROUP, groupName);
    commandStringBuilder.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL, "fine");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());

    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure the shared config was updated
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        ClusterConfigurationService sharedConfig =
            ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
        Properties gemfireProperties = null;

        try {
          gemfireProperties = sharedConfig.getConfiguration(groupName).getGemfireProperties();
        } catch (Exception e) {
          fail("Error occurred in cluster configuration service", e);
        }

        assertEquals("fine", gemfireProperties.get(LOG_LEVEL));
      }
    });
  }

  private void deleteTestFiles() throws IOException {
    this.managerConfigFile.delete();
    this.managerPropsFile.delete();
    this.vm1ConfigFile.delete();
    this.vm1PropsFile.delete();
    this.vm2ConfigFile.delete();
    this.vm2PropsFile.delete();
    this.shellConfigFile.delete();
    this.shellPropsFile.delete();

    deleteDirectory(this.subDir);
  }
}
