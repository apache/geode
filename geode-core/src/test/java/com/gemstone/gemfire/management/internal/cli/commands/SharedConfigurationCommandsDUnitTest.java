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

import static com.gemstone.gemfire.distributed.internal.DistributionConfig.*;
import static com.gemstone.gemfire.internal.AvailablePortHelper.*;
import static com.gemstone.gemfire.management.internal.cli.CliUtil.*;
import static com.gemstone.gemfire.management.internal.cli.i18n.CliStrings.*;
import static com.gemstone.gemfire.test.dunit.Assert.*;
import static com.gemstone.gemfire.test.dunit.Host.*;
import static com.gemstone.gemfire.test.dunit.LogWriterUtils.*;
import static com.gemstone.gemfire.test.dunit.NetworkUtils.*;
import static com.gemstone.gemfire.test.dunit.Wait.*;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.internal.ClassBuilder;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.HeadlessGfsh;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.management.internal.configuration.SharedConfigurationTestUtils;
import com.gemstone.gemfire.management.internal.configuration.domain.Configuration;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;

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
    this.newDeployableJarFile = new File(this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + deployedJarName);
    this.classBuilder = new ClassBuilder();

    this.sharedConfigZipFileName = this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + "sharedConfig.zip";
    this.startArchiveFileName = this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + "stats.gfs";
    this.ports = getRandomAvailableTCPPorts(4);

    this.locator1Port = this.ports[0];
    this.locator1Name = "locator1-" + this.locator1Port;
    this.locator1LogFilePath = this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + "locator-" + this.locator1Port + ".log";

    this.locator2Port = this.ports[1];
    this.locator2Name = "Locator2-" + this.locator2Port;
    this.locator2LogFilePath = this.temporaryFolder.getRoot().getCanonicalPath() + File.separator + "locator-" + this.locator2Port + ".log";

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

  @Test
  public void testExportImportSharedConfiguration() throws IOException {
    // Start the Locator and wait for shared configuration to be available
    VM locatorAndMgr = getHost(0).getVM(3);
    Set<DistributedMember> normalMembers1 = (Set<DistributedMember>) locatorAndMgr.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        final File locatorLogFile = new File(locator1LogFilePath);

        final Properties locatorProps = new Properties();
        locatorProps.setProperty(NAME_NAME, locator1Name);
        locatorProps.setProperty(MCAST_PORT_NAME, "0");
        locatorProps.setProperty(LOG_LEVEL_NAME, "config");
        locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
        locatorProps.setProperty(JMX_MANAGER_NAME, "true");
        locatorProps.setProperty(JMX_MANAGER_START_NAME, "true");
        locatorProps.setProperty(JMX_MANAGER_BIND_ADDRESS_NAME, String.valueOf(locator1JmxHost));
        locatorProps.setProperty(JMX_MANAGER_PORT_NAME, String.valueOf(locator1JmxPort));
        locatorProps.setProperty(HTTP_SERVICE_PORT_NAME, String.valueOf(locator1HttpPort));

        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locator1Port, locatorLogFile, null, locatorProps);

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
        localProps.setProperty(MCAST_PORT_NAME, "0");
        localProps.setProperty(LOCATORS_NAME, "localhost:" + locator1Port);
        localProps.setProperty(GROUPS_NAME, groupName);
        localProps.setProperty(NAME_NAME, "DataMember");
        getSystem(localProps);
        Cache cache = getCache();
        assertNotNull(cache);
        return getAllNormalMembers(cache);
      }
    });

    // Create a JAR file
    this.classBuilder.writeJarFromName("DeployCommandsDUnitA", this.newDeployableJarFile);

    // Deploy the JAR
    CommandResult cmdResult = executeCommand("deploy --jar=" + this.newDeployableJarFile.getCanonicalPath());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    //Create the region1 on the group
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

    //Alter runtime configuration
    commandStringBuilder = new CommandStringBuilder(ALTER_RUNTIME_CONFIG);
    commandStringBuilder.addOption(ALTER_RUNTIME_CONFIG__LOG__LEVEL, logLevel);
    commandStringBuilder.addOption(ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "50");
    commandStringBuilder.addOption(ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, "32");
    commandStringBuilder.addOption(ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, "49");
    commandStringBuilder.addOption(ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, "120");
    commandStringBuilder.addOption(ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, this.startArchiveFileName);
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

    //Import into a running system should fail
    commandStringBuilder = new CommandStringBuilder(IMPORT_SHARED_CONFIG);
    commandStringBuilder.addOption(IMPORT_SHARED_CONFIG__ZIP, this.sharedConfigZipFileName);
    cmdResult = executeCommand(commandStringBuilder.getCommandString());
    assertEquals(Status.ERROR, cmdResult.getStatus());

    //Stop the data members and remove the shared configuration in the locator.
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

    //Clear shared configuration in this locator to test the import shared configuration
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

    //Now execute import shared configuration
    //Now import the shared configuration and it should succeed.
    commandStringBuilder = new CommandStringBuilder(IMPORT_SHARED_CONFIG);
    commandStringBuilder.addOption(IMPORT_SHARED_CONFIG__ZIP, this.sharedConfigZipFileName);
    cmdResult = executeCommand(commandStringBuilder.getCommandString());
    assertEquals(Status.OK, cmdResult.getStatus());

    //Start a new locator , test if it has all the imported shared configuration artifacts
    VM newLocator = getHost(0).getVM(2);
    newLocator.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        final File locatorLogFile = new File(locator2LogFilePath);
        final Properties locatorProps = new Properties();
        locatorProps.setProperty(NAME_NAME, locator2Name);
        locatorProps.setProperty(MCAST_PORT_NAME, "0");
        locatorProps.setProperty(LOG_LEVEL_NAME, "fine");
        locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
        locatorProps.setProperty(LOCATORS_NAME, "localhost:" + locator1Port);

        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locator2Port, locatorLogFile, null, locatorProps);

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
          assertTrue(clusterConfig.getGemfireProperties().getProperty(LOG_LEVEL_NAME).equals(logLevel));
          assertTrue(clusterConfig.getGemfireProperties().getProperty(STATISTIC_ARCHIVE_FILE_NAME).equals(startArchiveFileName));

        } catch (IOException e) {
          fail("Unable to create a locator with a shared configuration", e);
        } catch (Exception e) {
          fail("Error occurred in cluster configuration service", e);
        }
      }
    });
  }
}
