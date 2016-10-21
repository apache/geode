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

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.SharedConfiguration;
import org.apache.geode.internal.ClassBuilder;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.HeadlessGfsh;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.configuration.SharedConfigurationTestUtils;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.management.internal.cli.CliUtil.getAllNormalMembers;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.*;
import static org.apache.geode.test.dunit.Assert.*;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.NetworkUtils.getIPLiteral;
import static org.apache.geode.test.dunit.Wait.waitForCriterion;

/**
 * DUnit test to test export and import of shared configuration.
 */
@Category(DistributedTest.class)
@SuppressWarnings("unchecked")
public class SharedConfigurationCommandsDUnitTest extends CliCommandTestBase {

  private static final int TIMEOUT = 10000;
  private static final int INTERVAL = 500;

  private final String region1Name = "r1";
  private final String region2Name = "r2";
  private final String logLevel = "info";

  private String groupName;

  private String deployedJarName;
  private File newDeployableJarFile;
  private ClassBuilder classBuilder;

  private String sharedConfigZipFileName;
  private String startArchiveFileName;
  private int[] ports;

  private int locator1Port;
  private String locator1Name;
  private String locator1LogFilePath;

  private int locator2Port;
  private String locator2Name;
  private String locator2LogFilePath;

  private int locator1HttpPort;
  private int locator1JmxPort;
  private String locator1JmxHost;

  @Override
  protected final void postSetUpCliCommandTestBase() throws Exception {
    disconnectAllFromDS();

    this.groupName = getName();
    this.deployedJarName = "DeployCommandsDUnit1.jar";
    this.newDeployableJarFile = new File(
        this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + deployedJarName);
    this.classBuilder = new ClassBuilder();

    this.sharedConfigZipFileName =
        this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + "sharedConfig.zip";
    this.startArchiveFileName =
        this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + "stats.gfs";
    this.ports = getRandomAvailableTCPPorts(4);

    this.locator1Port = this.ports[0];
    this.locator1Name = "locator1-" + this.locator1Port;
    this.locator1LogFilePath = this.temporaryFolder.getRoot().getCanonicalPath() + File.separator
        + "locator-" + this.locator1Port + ".log";

    this.locator2Port = this.ports[1];
    this.locator2Name = "Locator2-" + this.locator2Port;
    this.locator2LogFilePath = this.temporaryFolder.getRoot().getCanonicalPath() + File.separator
        + "locator-" + this.locator2Port + ".log";

    this.locator1HttpPort = ports[2];
    this.locator1JmxPort = ports[3];
    this.locator1JmxHost = getIPLiteral();
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    for (int i = 0; i < 4; i++) {
      getHost(0).getVM(i).invoke(SharedConfigurationTestUtils.cleanupLocator);
    }
  }

  @Category(FlakyTest.class) // GEODE-1519
  @Test
  public void testExportImportSharedConfiguration() throws IOException {
    // Start the Locator and wait for shared configuration to be available
    VM locatorAndMgr = getHost(0).getVM(3);
    Set<DistributedMember> normalMembers1 =
        (Set<DistributedMember>) locatorAndMgr.invoke(new SerializableCallable() {
          @Override
          public Object call() {
            final File locatorLogFile = new File(locator1LogFilePath);

            final Properties locatorProps = new Properties();
            locatorProps.setProperty(NAME, locator1Name);
            locatorProps.setProperty(MCAST_PORT, "0");
            locatorProps.setProperty(LOG_LEVEL, "config");
            locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
            locatorProps.setProperty(JMX_MANAGER, "true");
            locatorProps.setProperty(JMX_MANAGER_START, "true");
            locatorProps.setProperty(JMX_MANAGER_BIND_ADDRESS, String.valueOf(locator1JmxHost));
            locatorProps.setProperty(JMX_MANAGER_PORT, String.valueOf(locator1JmxPort));
            locatorProps.setProperty(HTTP_SERVICE_PORT, String.valueOf(locator1HttpPort));

            try {
              final InternalLocator locator = (InternalLocator) Locator
                  .startLocatorAndDS(locator1Port, locatorLogFile, null, locatorProps);

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
              waitForCriterion(wc, TIMEOUT, INTERVAL, true);

            } catch (IOException e) {
              fail("Unable to create a locator with a shared configuration", e);
            }

            return getAllNormalMembers(CacheFactory.getAnyInstance());
          }
        });

    HeadlessGfsh gfsh = getDefaultShell();
    connect(locator1JmxHost, locator1JmxPort, locator1HttpPort, gfsh);

    // Create a cache in VM 1
    VM dataMember = getHost(0).getVM(1);
    normalMembers1 = (Set<DistributedMember>) dataMember.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        Properties localProps = new Properties();
        localProps.setProperty(MCAST_PORT, "0");
        localProps.setProperty(LOCATORS, "localhost[" + locator1Port + "]");
        localProps.setProperty(GROUPS, groupName);
        localProps.setProperty(NAME, "DataMember");
        getSystem(localProps);
        Cache cache = getCache();
        assertNotNull(cache);
        return getAllNormalMembers(cache);
      }
    });

    // Create a JAR file
    this.classBuilder.writeJarFromName("DeployCommandsDUnitA", this.newDeployableJarFile);

    // Deploy the JAR
    CommandResult cmdResult =
        executeCommand("deploy --jar=" + this.newDeployableJarFile.getCanonicalPath());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Create the region1 on the group
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CREATE_REGION);
    commandStringBuilder.addOption(CREATE_REGION__REGION, region1Name);
    commandStringBuilder.addOption(CREATE_REGION__REGIONSHORTCUT, "REPLICATE");
    commandStringBuilder.addOption(CREATE_REGION__STATISTICSENABLED, "true");
    commandStringBuilder.addOption(CREATE_REGION__GROUP, groupName);

    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    commandStringBuilder = new CommandStringBuilder(CREATE_REGION);
    commandStringBuilder.addOption(CREATE_REGION__REGION, region2Name);
    commandStringBuilder.addOption(CREATE_REGION__REGIONSHORTCUT, "PARTITION");
    commandStringBuilder.addOption(CREATE_REGION__STATISTICSENABLED, "true");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Alter runtime configuration
    commandStringBuilder = new CommandStringBuilder(ALTER_RUNTIME_CONFIG);
    commandStringBuilder.addOption(ALTER_RUNTIME_CONFIG__LOG__LEVEL, logLevel);
    commandStringBuilder.addOption(ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "50");
    commandStringBuilder.addOption(ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, "32");
    commandStringBuilder.addOption(ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, "49");
    commandStringBuilder.addOption(ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, "120");
    commandStringBuilder.addOption(ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE,
        this.startArchiveFileName);
    commandStringBuilder.addOption(ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED, "true");
    commandStringBuilder.addOption(ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "10");
    cmdResult = executeCommand(commandStringBuilder.getCommandString());
    String resultString = commandResultToString(cmdResult);

    getLogWriter().info("#SB Result\n");
    getLogWriter().info(resultString);
    assertEquals(true, cmdResult.getStatus().equals(Status.OK));

    commandStringBuilder = new CommandStringBuilder(STATUS_SHARED_CONFIG);
    cmdResult = executeCommand(commandStringBuilder.getCommandString());
    resultString = commandResultToString(cmdResult);
    getLogWriter().info("#SB Result\n");
    getLogWriter().info(resultString);
    assertEquals(Status.OK, cmdResult.getStatus());

    commandStringBuilder = new CommandStringBuilder(EXPORT_SHARED_CONFIG);
    commandStringBuilder.addOption(EXPORT_SHARED_CONFIG__FILE, this.sharedConfigZipFileName);
    cmdResult = executeCommand(commandStringBuilder.getCommandString());
    resultString = commandResultToString(cmdResult);
    getLogWriter().info("#SB Result\n");
    getLogWriter().info(resultString);
    assertEquals(Status.OK, cmdResult.getStatus());

    // Import into a running system should fail
    commandStringBuilder = new CommandStringBuilder(IMPORT_SHARED_CONFIG);
    commandStringBuilder.addOption(IMPORT_SHARED_CONFIG__ZIP, this.sharedConfigZipFileName);
    cmdResult = executeCommand(commandStringBuilder.getCommandString());
    assertEquals(Status.ERROR, cmdResult.getStatus());

    // Stop the data members and remove the shared configuration in the locator.
    dataMember.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Cache cache = getCache();
        cache.close();
        assertTrue(cache.isClosed());
        disconnectFromDS();
        return null;
      }
    });

    // Clear shared configuration in this locator to test the import shared configuration
    locatorAndMgr.invoke(new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        InternalLocator locator = InternalLocator.getLocator();
        SharedConfiguration sc = locator.getSharedConfiguration();
        assertNotNull(sc);
        sc.clearSharedConfiguration();
        return null;
      }
    });

    // Now execute import shared configuration
    // Now import the shared configuration and it should succeed.
    commandStringBuilder = new CommandStringBuilder(IMPORT_SHARED_CONFIG);
    commandStringBuilder.addOption(IMPORT_SHARED_CONFIG__ZIP, this.sharedConfigZipFileName);
    cmdResult = executeCommand(commandStringBuilder.getCommandString());
    assertEquals(Status.OK, cmdResult.getStatus());

    // Start a new locator , test if it has all the imported shared configuration artifacts
    VM newLocator = getHost(0).getVM(2);
    newLocator.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        final File locatorLogFile = new File(locator2LogFilePath);
        final Properties locatorProps = new Properties();
        locatorProps.setProperty(NAME, locator2Name);
        locatorProps.setProperty(MCAST_PORT, "0");
        locatorProps.setProperty(LOG_LEVEL, "fine");
        locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
        locatorProps.setProperty(LOCATORS, "localhost[" + locator1Port + "]");

        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locator2Port,
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

          SharedConfiguration sc = locator.getSharedConfiguration();
          assertNotNull(sc);
          Configuration groupConfig = sc.getConfiguration(groupName);
          assertNotNull(groupConfig);
          assertTrue(groupConfig.getCacheXmlContent().contains(region1Name));

          Configuration clusterConfig = sc.getConfiguration(SharedConfiguration.CLUSTER_CONFIG);
          assertNotNull(clusterConfig);
          assertTrue(clusterConfig.getCacheXmlContent().contains(region2Name));
          assertTrue(clusterConfig.getJarNames().contains(deployedJarName));
          assertTrue(clusterConfig.getGemfireProperties().getProperty(LOG_LEVEL).equals(logLevel));
          assertTrue(clusterConfig.getGemfireProperties().getProperty(STATISTIC_ARCHIVE_FILE)
              .equals(startArchiveFileName));

        } catch (IOException e) {
          fail("Unable to create a locator with a shared configuration", e);
        } catch (Exception e) {
          fail("Error occurred in cluster configuration service", e);
        }
      }
    });
  }
}
