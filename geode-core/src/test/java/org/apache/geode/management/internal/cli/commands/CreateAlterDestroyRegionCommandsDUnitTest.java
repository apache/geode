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
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.awaitility.Awaitility.waitAtMost;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

@Category({DistributedTest.class, FlakyTest.class}) // GEODE-973 GEODE-2009 GEODE-3530
@SuppressWarnings("serial")
public class CreateAlterDestroyRegionCommandsDUnitTest extends CliCommandTestBase {

  private final List<String> filesToBeDeleted = new CopyOnWriteArrayList<>();

  private void waitForRegionMBeanCreation(final String regionPath) {
    Host.getHost(0).getVM(0)
        .invoke(() -> waitAtMost(5, TimeUnit.SECONDS).until(newRegionMBeanIsCreated(regionPath)));
  }

  private Callable<Boolean> newRegionMBeanIsCreated(final String regionPath) {
    return () -> {
      try {
        MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;
        String queryExp =
            MessageFormat.format(ManagementConstants.OBJECTNAME__REGION_MXBEAN, regionPath, "*");
        ObjectName queryExpON = new ObjectName(queryExp);
        return mbeanServer.queryNames(null, queryExpON).size() == 1;
      } catch (MalformedObjectNameException mone) {
        getLogWriter().error(mone);
        fail(mone.getMessage());
        return false;
      }
    };
  }

  /**
   * Asserts that creating, altering and destroying regions correctly updates the shared
   * configuration.
   */
  @Test // FlakyTest: GEODE-2009
  public void testCreateAlterDestroyUpdatesSharedConfig() throws Exception {
    disconnectAllFromDS();
    final int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    jmxPort = ports[0];
    httpPort = ports[1];
    try {
      jmxHost = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException ignore) {
      jmxHost = "localhost";
    }

    final String regionName = "testRegionSharedConfigRegion";
    final String regionPath = "/" + regionName;
    final String groupName = "testRegionSharedConfigGroup";

    // Start the Locator and wait for shared configuration to be available
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    final Properties locatorProps = new Properties();
    locatorProps.setProperty(NAME, "Locator");
    locatorProps.setProperty(MCAST_PORT, "0");
    locatorProps.setProperty(LOG_LEVEL, "fine");
    locatorProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "true");
    locatorProps.setProperty(JMX_MANAGER, "true");
    locatorProps.setProperty(JMX_MANAGER_START, "true");
    locatorProps.setProperty(JMX_MANAGER_BIND_ADDRESS, String.valueOf(jmxHost));
    locatorProps.setProperty(JMX_MANAGER_PORT, String.valueOf(jmxPort));
    locatorProps.setProperty(HTTP_SERVICE_PORT, String.valueOf(httpPort));

    Host.getHost(0).getVM(0).invoke(() -> {
      final File locatorLogFile = new File("locator-" + locatorPort + ".log");
      try {
        final InternalLocator locator = (InternalLocator) Locator.startLocatorAndDS(locatorPort,
            locatorLogFile, null, locatorProps);

        waitAtMost(5, TimeUnit.SECONDS).until(locator::isSharedConfigurationRunning);

        ManagementService managementService =
            ManagementService.getExistingManagementService(GemFireCacheImpl.getInstance());
        assertNotNull(managementService);
        assertTrue(managementService.isManager());
        assertTrue(checkIfCommandsAreLoadedOrNot());

      } catch (IOException ioex) {
        fail("Unable to create a locator with a shared configuration");
      }
    });

    connect(jmxHost, jmxPort, httpPort, getDefaultShell());

    // Create a cache in VM 1
    VM vm = Host.getHost(0).getVM(1);
    vm.invoke(() -> {
      Properties localProps = new Properties();
      localProps.setProperty(MCAST_PORT, "0");
      localProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
      localProps.setProperty(GROUPS, groupName);
      getSystem(localProps);
      assertNotNull(getCache());
    });

    // Test creating the region
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION, regionName);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "REPLICATE");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__STATISTICSENABLED, "true");
    commandStringBuilder.addOption(CliStrings.GROUP, groupName);
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure that the region has been registered with the Manager MXBean
    waitForRegionMBeanCreation(regionPath);

    // Make sure the region exists in the shared config
    Host.getHost(0).getVM(0).invoke(() -> {
      InternalClusterConfigurationService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
      try {
        assertTrue(
            sharedConfig.getConfiguration(groupName).getCacheXmlContent().contains(regionName));
      } catch (Exception e) {
        fail("Error in cluster configuration service", e);
      }
    });

    // Restart the data vm to make sure the changes are in place
    vm = Host.getHost(0).getVM(1);
    vm.invoke(() -> {
      Cache cache = getCache();
      assertNotNull(cache);
      cache.close();
      assertTrue(cache.isClosed());

      Properties localProps = new Properties();
      localProps.setProperty(MCAST_PORT, "0");
      localProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
      localProps.setProperty(GROUPS, groupName);
      localProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
      getSystem(localProps);
      cache = getCache();
      assertNotNull(cache);
      Region region = cache.getRegion(regionName);
      assertNotNull(region);
    });


    // Test altering the region
    commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, regionName);
    commandStringBuilder.addOption(CliStrings.GROUP, groupName);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ENTRYEXPIRATIONTIMETOLIVE, "45635");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ENTRYEXPIRATIONTTLACTION, "DESTROY");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure the region was altered in the shared config
    Host.getHost(0).getVM(0).invoke(() -> {
      InternalClusterConfigurationService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
      try {
        assertTrue(sharedConfig.getConfiguration(groupName).getCacheXmlContent().contains("45635"));
      } catch (Exception e) {
        fail("Error in cluster configuration service");
      }
    });

    // Restart the data vm to make sure the changes are in place
    vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        Cache cache = getCache();
        assertNotNull(cache);
        cache.close();
        assertTrue(cache.isClosed());

        Properties localProps = new Properties();
        localProps.setProperty(MCAST_PORT, "0");
        localProps.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
        localProps.setProperty(GROUPS, groupName);
        localProps.setProperty(USE_CLUSTER_CONFIGURATION, "true");
        getSystem(localProps);
        cache = getCache();
        assertNotNull(cache);
        Region region = cache.getRegion(regionName);
        assertNotNull(region);

        return null;
      }
    });
  }

  @Override
  protected final void preTearDownCliCommandTestBase() throws Exception {
    for (String path : this.filesToBeDeleted) {
      try {
        final File fileToDelete = new File(path);
        FileUtils.forceDelete(fileToDelete);
        if (path.endsWith(".jar")) {
          executeCommand("undeploy --jar=" + fileToDelete.getName());
        }
      } catch (IOException e) {
        getLogWriter().error("Unable to delete file", e);
      }
    }
    this.filesToBeDeleted.clear();
  }
}
