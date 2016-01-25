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
package com.gemstone.gemfire.management.internal.cli.commands;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.remote.CommandProcessor;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * Dunit class for testing GemFire config commands : export config
 *
 * @author David Hoots
 * @author Sourabh Bansod
 * @since 7.0
 */
public class ConfigCommandsDUnitTest extends CliCommandTestBase {
  private static final long serialVersionUID = 1L;

  File managerConfigFile = new File("Manager-cache.xml");
  File managerPropsFile = new File("Manager-gf.properties");
  File vm1ConfigFile = new File("VM1-cache.xml");
  File vm1PropsFile = new File("VM1-gf.properties");
  File vm2ConfigFile = new File("VM2-cache.xml");
  File vm2PropsFile = new File("VM2-gf.properties");
  File shellConfigFile = new File("Shell-cache.xml");
  File shellPropsFile = new File("Shell-gf.properties");
  File subDir = new File("ConfigCommandsDUnitTestSubDir");
  File subManagerConfigFile = new File(subDir, managerConfigFile.getName());

  public ConfigCommandsDUnitTest(String name) {
    super(name);
  }

  public void tearDown2() throws Exception {
    deleteTestFiles();
    invokeInEveryVM(new SerializableRunnable() {

      @Override
      public void run() {
        try {
          deleteTestFiles();
        } catch (IOException e) {
          fail("error", e);
        }
      }
    });
    super.tearDown2();
  }

  public void testDescribeConfig() throws ClassNotFoundException, IOException {
    createDefaultSetup(null);
    final String controllerName = "Member2";

    /***
     * Create properties for the controller VM
     */
    final Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    localProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
    localProps.setProperty(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME, "true");
    localProps.setProperty(DistributionConfig.ENABLE_TIME_STATISTICS_NAME, "true");
    localProps.setProperty(DistributionConfig.NAME_NAME, controllerName);
    localProps.setProperty(DistributionConfig.GROUPS_NAME, "G1");
    getSystem(localProps);
    Cache cache = getCache();
    int ports[] = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    CacheServer cs = getCache().addCacheServer();
    cs.setPort(ports[0]);
    cs.setMaxThreads(10);
    cs.setMaxConnections(9);
    cs.start();

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
    assertEquals(true, resultStr.contains("archive-file-size-limit"));
    assertEquals(true, !resultStr.contains("copy-on-read"));

    cmdResult = executeCommand(command + " --" + CliStrings.DESCRIBE_CONFIG__HIDE__DEFAULTS + "=false");
    resultStr = commandResultToString(cmdResult);
    getLogWriter().info("#SB No hiding of defaults\n" + resultStr);

    assertEquals(true, cmdResult.getStatus().equals(Status.OK));
    assertEquals(true, resultStr.contains("is-server"));
    assertEquals(true, resultStr.contains(controllerName));
    assertEquals(true, resultStr.contains("copy-on-read"));

    cs.stop();
  }

  @SuppressWarnings("serial")
  public void testExportConfig() throws IOException {
    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.NAME_NAME, "Manager");
    localProps.setProperty(DistributionConfig.GROUPS_NAME, "Group1");
    createDefaultSetup(localProps);

    // Create a cache in another VM (VM1)
    Host.getHost(0).getVM(1).invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.NAME_NAME, "VM1");
        localProps.setProperty(DistributionConfig.GROUPS_NAME, "Group2");
        getSystem(localProps);
        getCache();
      }
    });

    // Create a cache in a 3rd VM (VM2)
    Host.getHost(0).getVM(2).invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.NAME_NAME, "VM2");
        localProps.setProperty(DistributionConfig.GROUPS_NAME, "Group2");
        getSystem(localProps);
        getCache();
      }
    });

    // Create a cache in the local VM
    localProps = new Properties();
    localProps.setProperty(DistributionConfig.NAME_NAME, "Shell");
    getSystem(localProps);
    Cache cache = getCache();

    // Test export config for all members
    deleteTestFiles();
    CommandResult cmdResult = executeCommand("export config");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    assertTrue(this.managerConfigFile.exists());
    assertTrue(this.managerPropsFile.exists());
    assertTrue(this.vm1ConfigFile.exists());
    assertTrue(this.vm1PropsFile.exists());
    assertTrue(this.vm2ConfigFile.exists());
    assertTrue(this.vm2PropsFile.exists());
    assertTrue(this.shellConfigFile.exists());
    assertTrue(this.shellPropsFile.exists());

    // Test exporting member
    deleteTestFiles();
    cmdResult = executeCommand("export config --member=Manager");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    assertTrue(this.managerConfigFile.exists());
    assertFalse(this.vm1ConfigFile.exists());
    assertFalse(this.vm2ConfigFile.exists());
    assertFalse(this.shellConfigFile.exists());

    // Test exporting group
    deleteTestFiles();
    cmdResult = executeCommand("export config --group=Group2");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    assertFalse(this.managerConfigFile.exists());
    assertTrue(this.vm1ConfigFile.exists());
    assertTrue(this.vm2ConfigFile.exists());
    assertFalse(this.shellConfigFile.exists());

    // Test export to directory
    deleteTestFiles();
    cmdResult = executeCommand("export config --dir=" + subDir.getAbsolutePath());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    assertFalse(this.managerConfigFile.exists());
    assertTrue(this.subManagerConfigFile.exists());

    // Test the contents of the file
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    CacheXmlGenerator.generate(cache, printWriter, false, false, false);
    String configToMatch = stringWriter.toString();

    deleteTestFiles();
    cmdResult = executeCommand("export config --member=Shell");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    char[] fileContents = new char[configToMatch.length()];
    try {
      FileReader reader = new FileReader(shellConfigFile);
      reader.read(fileContents);
    } catch (Exception ex) {
      fail("Unable to read file contents for comparison", ex);
    }

    assertEquals(configToMatch, new String(fileContents));
  }

  public void testAlterRuntimeConfig() throws ClassNotFoundException, IOException {
    final String controller = "controller";
    createDefaultSetup(null);
    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.NAME_NAME, controller);
    localProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "error");
    getSystem(localProps);
    final GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    final DistributionConfig config = cache.getSystem().getConfig();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__MEMBER, controller);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL, "info");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "50");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, "32");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, "49");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, "2000");
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, "stat.gfs");
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
    Result result = commandProcessor.createCommandStatement("alter runtime", Collections.EMPTY_MAP).process();
  }

  public void testAlterRuntimeConfigRandom() {
    final String member1 = "VM1";
    final String controller = "controller";
    createDefaultSetup(null);
    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.NAME_NAME, controller);
    localProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "error");
    getSystem(localProps);
    final GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    final DistributionConfig config = cache.getSystem().getConfig();

    Host.getHost(0).getVM(1).invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.NAME_NAME, member1);
        getSystem(localProps);
        Cache cache = getCache();
      }
    });

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    CommandResult cmdResult = executeCommand(csb.getCommandString());
    String resultAsString = commandResultToString(cmdResult);
    getLogWriter().info("#SB Result\n");
    getLogWriter().info(resultAsString);
    assertEquals(true, cmdResult.getStatus().equals(Status.ERROR));
    assertTrue(resultAsString.contains(CliStrings.ALTER_RUNTIME_CONFIG__RELEVANT__OPTION__MESSAGE));

    csb = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "2000000000");
    cmdResult = executeCommand(csb.getCommandString());
    resultAsString = commandResultToString(cmdResult);
    getLogWriter().info("#SB Result\n");
    getLogWriter().info(resultAsString);
    assertEquals(true, cmdResult.getStatus().equals(Status.ERROR));

  }

  public void testAlterRuntimeConfigOnAllMembers() {
    final String member1 = "VM1";
    final String controller = "controller";
    createDefaultSetup(null);
    Properties localProps = new Properties();
    localProps.setProperty(DistributionConfig.NAME_NAME, controller);
    localProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "error");
    getSystem(localProps);
    final GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
    final DistributionConfig config = cache.getSystem().getConfig();

    Host.getHost(0).getVM(1).invoke(new SerializableRunnable() {
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.NAME_NAME, member1);
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
    csb.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, "stat.gfs");
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
        final DistributionConfig configVM1 = cacheVM1.getSystem().getConfig();
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
   * <p>
   * Disabled: this test frequently fails during unit test runs. See ticket #52204
   */
  public void disabledtestAlterUpdatesSharedConfig() {
    disconnectAllFromDS();

    final String groupName = "testAlterRuntimeConfigSharedConfigGroup";

    // Start the Locator and wait for shared configuration to be available
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    Host.getHost(0).getVM(3).invoke(new SerializableRunnable() {
      @Override
      public void run() {

        final File locatorLogFile = new File("locator-" + locatorPort + ".log");
        final Properties locatorProps = new Properties();
        locatorProps.setProperty(DistributionConfig.NAME_NAME, "Locator");
        locatorProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        locatorProps.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locatorPort, locatorLogFile, null,
              locatorProps);

          DistributedTestCase.WaitCriterion wc = new DistributedTestCase.WaitCriterion() {
            @Override
            public boolean done() {
              return locator.isSharedConfigurationRunning();
            }

            @Override
            public String description() {
              return "Waiting for shared configuration to be started";
            }
          };
          DistributedTestCase.waitForCriterion(wc, 5000, 500, true);
        } catch (IOException ioex) {
          fail("Unable to create a locator with a shared configuration");
        }
      }
    });

    // Start the default manager
    Properties managerProps = new Properties();
    managerProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    managerProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locatorPort);
    createDefaultSetup(managerProps);

    // Create a cache in VM 1
    VM vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        //Make sure no previous shared config is screwing up this test.
        FileUtil.delete(new File("ConfigDiskDir_Locator"));
        FileUtil.delete(new File("cluster_config"));
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        localProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locatorPort);
        localProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "error");
        localProps.setProperty(DistributionConfig.GROUPS_NAME, groupName);
        getSystem(localProps);

        assertNotNull(getCache());
        assertEquals("error", system.getConfig().getAttribute(DistributionConfig.LOG_LEVEL_NAME));
        return null;
      }
    });

    // Test altering the runtime config
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    commandStringBuilder.addOption(CliStrings.ALTER_RUNTIME_CONFIG__GROUP, groupName);
    commandStringBuilder.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL, "fine");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure the shared config was updated
    Host.getHost(0).getVM(3).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        SharedConfiguration sharedConfig = ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
        Properties gemfireProperties;
        try {
          gemfireProperties = sharedConfig.getConfiguration(groupName).getGemfireProperties();
          assertEquals("fine", gemfireProperties.get(DistributionConfig.LOG_LEVEL_NAME));
        } catch (Exception e) {
          fail("Error occurred in cluster configuration service", e);
        }
      }
    });
  }

  private final void deleteTestFiles() throws IOException {
    this.managerConfigFile.delete();
    this.managerPropsFile.delete();
    this.vm1ConfigFile.delete();
    this.vm1PropsFile.delete();
    this.vm2ConfigFile.delete();
    this.vm2PropsFile.delete();
    this.shellConfigFile.delete();
    this.shellPropsFile.delete();

    FileUtils.deleteDirectory(this.subDir);
  }
}
