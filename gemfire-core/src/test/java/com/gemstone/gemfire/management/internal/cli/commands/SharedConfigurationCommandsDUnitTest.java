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
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.ClassBuilder;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.HeadlessGfsh;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.management.internal.configuration.SharedConfigurationDUnitTest;
import com.gemstone.gemfire.management.internal.configuration.domain.Configuration;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Set;

/***
 * DUnit test to test export and import of shared configuration.
 *
 * @author bansods
 */
public class SharedConfigurationCommandsDUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;
  private static final int TIMEOUT = 10000;
  private static final int INTERVAL = 500;

  public SharedConfigurationCommandsDUnitTest(String name) {
    super(name);
  }

  File newDeployableJarFile = new File("DeployCommandsDUnit1.jar");
  private transient ClassBuilder classBuilder = new ClassBuilder();

  @SuppressWarnings("unchecked")
  public void testExportImportSharedConfiguration() {
    disconnectAllFromDS();

    final String region1Name = "r1";
    final String region2Name = "r2";
    final String groupName = "testRegionSharedConfigGroup";
    final String sharedConfigZipFileName = "sharedConfig.zip";
    final String deployedJarName = "DeployCommandsDUnit1.jar";
    final String logLevel = "info";
    final String startArchiveFileName = "stats.gfs";
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);

    // TODO Sourabh - the code below is similar to CliCommandTestBase.createDefaultSetup(..); we may want to consider
    // refactoring this and combine the duplicate code blocks using either the Template Method and/or Strategy design
    // patterns.  We can talk about this.
    // Start the Locator and wait for shared configuration to be available
    final int locator1Port = ports[0];
    final String locator1Name = "locator1-" + locator1Port;
    VM locatorAndMgr = Host.getHost(0).getVM(3);
    Object[] result = (Object[]) locatorAndMgr.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        int httpPort;
        int jmxPort;
        String jmxHost;

        try {
          jmxHost = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ignore) {
          jmxHost = "localhost";
        }

        final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

        jmxPort = ports[0];
        httpPort = ports[1];

        final File locatorLogFile = new File("locator-" + locator1Port + ".log");

        final Properties locatorProps = new Properties();
        locatorProps.setProperty(DistributionConfig.NAME_NAME, locator1Name);
        locatorProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        locatorProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "config");
        locatorProps.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
        locatorProps.setProperty(DistributionConfig.JMX_MANAGER_NAME, "true");
        locatorProps.setProperty(DistributionConfig.JMX_MANAGER_START_NAME, "true");
        locatorProps.setProperty(DistributionConfig.JMX_MANAGER_BIND_ADDRESS_NAME, String.valueOf(jmxHost));
        locatorProps.setProperty(DistributionConfig.JMX_MANAGER_PORT_NAME, String.valueOf(jmxPort));
        locatorProps.setProperty(DistributionConfig.HTTP_SERVICE_PORT_NAME, String.valueOf(httpPort));

        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locator1Port, locatorLogFile,
              null, locatorProps);
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
          Wait.waitForCriterion(wc, TIMEOUT, INTERVAL, true);
        } catch (IOException ioex) {
          fail("Unable to create a locator with a shared configuration");
        }

        final Object[] result = new Object[4];
        result[0] = jmxHost;
        result[1] = jmxPort;
        result[2] = httpPort;
        result[3] = CliUtil.getAllNormalMembers(CacheFactory.getAnyInstance());

        return result;
      }
    });

    HeadlessGfsh gfsh = getDefaultShell();
    String jmxHost = (String) result[0];
    int jmxPort = (Integer) result[1];
    int httpPort = (Integer) result[2];
    Set<DistributedMember> normalMembers1 = (Set<DistributedMember>) result[3];

    shellConnect(jmxHost, jmxPort, httpPort, gfsh);
    // Create a cache in VM 1
    VM dataMember = Host.getHost(0).getVM(1);
    normalMembers1 = (Set<DistributedMember>) dataMember.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        localProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locator1Port);
        localProps.setProperty(DistributionConfig.GROUPS_NAME, groupName);
        localProps.setProperty(DistributionConfig.NAME_NAME, "DataMember");
        getSystem(localProps);
        Cache cache = getCache();
        assertNotNull(cache);
        return CliUtil.getAllNormalMembers(cache);
      }
    });
    // Create a JAR file
    try {
      this.classBuilder.writeJarFromName("DeployCommandsDUnitA", this.newDeployableJarFile);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    // Deploy the JAR
    CommandResult cmdResult = executeCommand("deploy --jar=" + deployedJarName);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    //Create the region1 on the group
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION, region1Name);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "REPLICATE");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__STATISTICSENABLED, "true");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__GROUP, groupName);

    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION, region2Name);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__STATISTICSENABLED, "true");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    //Alter runtime configuration 
    commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_RUNTIME_CONFIG);
    commandStringBuilder.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__LEVEL, logLevel);
    commandStringBuilder.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__FILE__SIZE__LIMIT, "50");
    commandStringBuilder.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__DISK__SPACE__LIMIT, "32");
    commandStringBuilder.addOption(CliStrings.ALTER_RUNTIME_CONFIG__ARCHIVE__FILE__SIZE__LIMIT, "49");
    commandStringBuilder.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLE__RATE, "120");
    commandStringBuilder.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__ARCHIVE__FILE, startArchiveFileName);
    commandStringBuilder.addOption(CliStrings.ALTER_RUNTIME_CONFIG__STATISTIC__SAMPLING__ENABLED, "true");
    commandStringBuilder.addOption(CliStrings.ALTER_RUNTIME_CONFIG__LOG__DISK__SPACE__LIMIT, "10");
    cmdResult = executeCommand(commandStringBuilder.getCommandString());
    String resultString = commandResultToString(cmdResult);

    LogWriterUtils.getLogWriter().info("#SB Result\n");
    LogWriterUtils.getLogWriter().info(resultString);
    assertEquals(true, cmdResult.getStatus().equals(Status.OK));

    commandStringBuilder = new CommandStringBuilder(CliStrings.STATUS_SHARED_CONFIG);
    cmdResult = executeCommand(commandStringBuilder.getCommandString());
    resultString = commandResultToString(cmdResult);
    LogWriterUtils.getLogWriter().info("#SB Result\n");
    LogWriterUtils.getLogWriter().info(resultString);
    assertEquals(Status.OK, cmdResult.getStatus());

    commandStringBuilder = new CommandStringBuilder(CliStrings.EXPORT_SHARED_CONFIG);
    commandStringBuilder.addOption(CliStrings.EXPORT_SHARED_CONFIG__FILE, sharedConfigZipFileName);
    cmdResult = executeCommand(commandStringBuilder.getCommandString());
    resultString = commandResultToString(cmdResult);
    LogWriterUtils.getLogWriter().info("#SB Result\n");
    LogWriterUtils.getLogWriter().info(resultString);
    assertEquals(Status.OK, cmdResult.getStatus());

    //Import into a running system should fail
    commandStringBuilder = new CommandStringBuilder(CliStrings.IMPORT_SHARED_CONFIG);
    commandStringBuilder.addOption(CliStrings.IMPORT_SHARED_CONFIG__ZIP, sharedConfigZipFileName);
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
    commandStringBuilder = new CommandStringBuilder(CliStrings.IMPORT_SHARED_CONFIG);
    commandStringBuilder.addOption(CliStrings.IMPORT_SHARED_CONFIG__ZIP, sharedConfigZipFileName);
    cmdResult = executeCommand(commandStringBuilder.getCommandString());
    assertEquals(Status.OK, cmdResult.getStatus());

    //Start a new locator , test if it has all the imported shared configuration artifacts
    VM newLocator = Host.getHost(0).getVM(2);
    final int locator2Port = ports[1];
    final String locator2Name = "Locator2-" + locator2Port;

    newLocator.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        final File locatorLogFile = new File("locator-" + locator2Port + ".log");
        final Properties locatorProps = new Properties();
        locatorProps.setProperty(DistributionConfig.NAME_NAME, locator2Name);
        locatorProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        locatorProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "fine");
        locatorProps.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "true");
        locatorProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locator1Port);

        try {
          final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locator2Port, locatorLogFile,
              null, locatorProps);

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
          Wait.waitForCriterion(wc, 5000, 500, true);

          SharedConfiguration sc = locator.getSharedConfiguration();
          assertNotNull(sc);
          Configuration groupConfig = sc.getConfiguration(groupName);
          assertNotNull(groupConfig);
          assertTrue(groupConfig.getCacheXmlContent().contains(region1Name));

          Configuration clusterConfig = sc.getConfiguration(SharedConfiguration.CLUSTER_CONFIG);
          assertNotNull(clusterConfig);
          assertTrue(clusterConfig.getCacheXmlContent().contains(region2Name));
          assertTrue(clusterConfig.getJarNames().contains(deployedJarName));
          assertTrue(
              clusterConfig.getGemfireProperties().getProperty(DistributionConfig.LOG_LEVEL_NAME).equals(logLevel));
          assertTrue(
              clusterConfig.getGemfireProperties().getProperty(DistributionConfig.STATISTIC_ARCHIVE_FILE_NAME).equals(
                  startArchiveFileName));
        } catch (IOException ioex) {
          fail("Unable to create a locator with a shared configuration");
        } catch (Exception e) {
          Assert.fail("Error occurred in cluster configuration service", e);
        }
      }
    });

    //Clean up
    File sharedConfigZipFile = new File(sharedConfigZipFileName);
    FileUtils.deleteQuietly(sharedConfigZipFile);
    FileUtils.deleteQuietly(newDeployableJarFile);
  }

  @Override
  protected final void postTearDownCacheTestCase() throws Exception {
    for (int i = 0; i < 4; i++) {
      Host.getHost(0).getVM(i).invoke(SharedConfigurationDUnitTest.locatorCleanup);
    }
  }
}
