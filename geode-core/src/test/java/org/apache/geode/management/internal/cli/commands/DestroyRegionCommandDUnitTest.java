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
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.awaitility.Awaitility.waitAtMost;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.ManagementConstants;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

@Category({DistributedTest.class, FlakyTest.class}) // GEODE-3530
@SuppressWarnings("serial")
public class DestroyRegionCommandDUnitTest extends CliCommandTestBase {

  @Test
  public void testDestroyDistributedRegion() {
    setUpJmxManagerOnVm0ThenConnect(null);

    for (int i = 1; i <= 2; i++) {
      Host.getHost(0).getVM(i).invoke(() -> {
        final Cache cache = getCache();

        RegionFactory<Object, Object> factory = cache.createRegionFactory(RegionShortcut.PARTITION);
        factory.create("Customer");

        PartitionAttributesFactory paFactory = new PartitionAttributesFactory();
        paFactory.setColocatedWith("Customer");
        factory.setPartitionAttributes(paFactory.create());
        factory.create("Order");
      });
    }

    waitForRegionMBeanCreation("/Customer", 2);
    waitForRegionMBeanCreation("/Order", 2);

    // Test failure when region not found
    String command = "destroy region --name=DOESNOTEXIST";
    getLogWriter().info("testDestroyRegion command=" + command);
    CommandResult cmdResult = executeCommand(command);
    String strr = commandResultToString(cmdResult);
    getLogWriter().info("testDestroyRegion strr=" + strr);
    assertTrue(stringContainsLine(strr, "Could not find.*\"DOESNOTEXIST\".*"));
    assertEquals(Result.Status.ERROR, cmdResult.getStatus());

    // Test unable to destroy with co-location
    command = "destroy region --name=/Customer";
    getLogWriter().info("testDestroyRegion command=" + command);
    cmdResult = executeCommand(command);
    strr = commandResultToString(cmdResult);
    getLogWriter().info("testDestroyRegion strr=" + strr);
    assertEquals(Result.Status.ERROR, cmdResult.getStatus());

    // Test success
    command = "destroy region --name=/Order";
    getLogWriter().info("testDestroyRegion command=" + command);
    cmdResult = executeCommand(command);
    strr = commandResultToString(cmdResult);
    assertTrue(stringContainsLine(strr, ".*Order.*destroyed successfully.*"));
    getLogWriter().info("testDestroyRegion strr=" + strr);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    command = "destroy region --name=/Customer";
    getLogWriter().info("testDestroyRegion command=" + command);
    cmdResult = executeCommand(command);
    strr = commandResultToString(cmdResult);
    assertTrue(stringContainsLine(strr, ".*Customer.*destroyed successfully.*"));
    getLogWriter().info("testDestroyRegion strr=" + strr);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
  }

  @Test
  public void testDestroyLocalRegions() {
    setUpJmxManagerOnVm0ThenConnect(null);

    for (int i = 1; i <= 3; i++) {
      Host.getHost(0).getVM(i).invoke(() -> {
        final Cache cache = getCache();

        RegionFactory<Object, Object> factory = cache.createRegionFactory(RegionShortcut.REPLICATE);
        factory.setScope(Scope.LOCAL);
        factory.create("Customer");
      });
    }

    waitForRegionMBeanCreation("/Customer", 3);

    // Test failure when region not found
    String command = "destroy region --name=DOESNOTEXIST";
    getLogWriter().info("testDestroyRegion command=" + command);
    CommandResult cmdResult = executeCommand(command);
    String strr = commandResultToString(cmdResult);
    getLogWriter().info("testDestroyRegion strr=" + strr);
    assertTrue(stringContainsLine(strr, "Could not find.*\"DOESNOTEXIST\".*"));
    assertEquals(Result.Status.ERROR, cmdResult.getStatus());

    command = "destroy region --name=/Customer";
    getLogWriter().info("testDestroyRegion command=" + command);
    cmdResult = executeCommand(command);
    strr = commandResultToString(cmdResult);
    assertTrue(stringContainsLine(strr, ".*Customer.*destroyed successfully.*"));
    getLogWriter().info("testDestroyRegion strr=" + strr);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    for (int i = 1; i <= 3; i++) {
      final int x = i;
      Host.getHost(0).getVM(i).invoke(
          () -> assertNull("Region still exists in VM " + x, getCache().getRegion("Customer")));
    }
  }

  @Test
  public void testDestroyLocalAndDistributedRegions() {
    setUpJmxManagerOnVm0ThenConnect(null);

    for (int i = 1; i <= 2; i++) {
      Host.getHost(0).getVM(i).invoke(() -> {
        final Cache cache = getCache();
        RegionFactory<Object, Object> factory = cache.createRegionFactory(RegionShortcut.PARTITION);
        factory.create("Customer");
        factory.create("Customer-2");
        factory.create("Customer_3");
      });
    }

    Host.getHost(0).getVM(3).invoke(() -> {
      final Cache cache = getCache();
      RegionFactory<Object, Object> factory = cache.createRegionFactory(RegionShortcut.REPLICATE);
      factory.setScope(Scope.LOCAL);
      factory.create("Customer");
      factory.create("Customer-2");
      factory.create("Customer_3");
    });

    waitForRegionMBeanCreation("/Customer", 3);

    // Test failure when region not found
    String command = "destroy region --name=DOESNOTEXIST";
    getLogWriter().info("testDestroyRegion command=" + command);
    CommandResult cmdResult = executeCommand(command);
    String strr = commandResultToString(cmdResult);
    getLogWriter().info("testDestroyRegion strr=" + strr);
    assertTrue(stringContainsLine(strr, "Could not find.*\"DOESNOTEXIST\".*"));
    assertEquals(Result.Status.ERROR, cmdResult.getStatus());

    command = "destroy region --name=/Customer";
    getLogWriter().info("testDestroyRegion command=" + command);
    cmdResult = executeCommand(command);
    strr = commandResultToString(cmdResult);
    assertTrue(stringContainsLine(strr, ".*Customer.*destroyed successfully.*"));
    getLogWriter().info("testDestroyRegion strr=" + strr);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    command = "destroy region --name=/Customer_3";
    getLogWriter().info("testDestroyRegion command=" + command);
    cmdResult = executeCommand(command);
    strr = commandResultToString(cmdResult);
    assertTrue(stringContainsLine(strr, ".*Customer_3.*destroyed successfully.*"));
    getLogWriter().info("testDestroyRegion strr=" + strr);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    command = "destroy region --name=/Customer-2";
    getLogWriter().info("testDestroyRegion command=" + command);
    cmdResult = executeCommand(command);
    strr = commandResultToString(cmdResult);
    assertTrue(stringContainsLine(strr, ".*Customer-2.*destroyed successfully.*"));
    getLogWriter().info("testDestroyRegion strr=" + strr);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    for (int i = 1; i <= 3; i++) {
      final int x = i;
      Host.getHost(0).getVM(i).invoke(() -> {
        assertNull("Region still exists in VM " + x, getCache().getRegion("Customer"));
        assertNull("Region still exists in VM " + x, getCache().getRegion("Customer-2"));
        assertNull("Region still exists in VM " + x, getCache().getRegion("Customer_3"));
      });
    }
  }

  @Test
  public void testDestroyRegionWithSharedConfig() {
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
    waitForRegionMBeanCreation(regionPath, 1);

    // Make sure the region exists in the shared config
    Host.getHost(0).getVM(0).invoke(() -> {
      ClusterConfigurationService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
      try {
        assertTrue(
            sharedConfig.getConfiguration(groupName).getCacheXmlContent().contains(regionName));
      } catch (Exception e) {
        fail("Error occurred in cluster configuration service");
      }
    });

    // Test destroying the region
    commandStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_REGION);
    commandStringBuilder.addOption(CliStrings.DESTROY_REGION__REGION, regionName);
    cmdResult = executeCommand(commandStringBuilder.toString());
    getLogWriter().info("#SB" + commandResultToString(cmdResult));
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure the region was removed from the shared config
    Host.getHost(0).getVM(0).invoke(() -> {
      ClusterConfigurationService sharedConfig =
          ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
      try {
        assertFalse(
            sharedConfig.getConfiguration(groupName).getCacheXmlContent().contains(regionName));
      } catch (Exception e) {
        fail("Error occurred in cluster configuration service");
      }
    });


    // Restart the data vm to make sure the region is not existing any more
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
      assertNull(region);

      return null;
    });
  }

  private void waitForRegionMBeanCreation(final String regionPath, final int mbeanCount) {
    Host.getHost(0).getVM(0).invoke(() -> waitAtMost(5, TimeUnit.SECONDS)
        .until(newRegionMBeanIsCreated(regionPath, mbeanCount)));
  }

  private Callable<Boolean> newRegionMBeanIsCreated(final String regionPath, final int mbeanCount) {
    return () -> {
      try {
        MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;
        String queryExp =
            MessageFormat.format(ManagementConstants.OBJECTNAME__REGION_MXBEAN, regionPath, "*");
        ObjectName queryExpON = new ObjectName(queryExp);
        return mbeanServer.queryNames(null, queryExpON).size() == mbeanCount;
      } catch (MalformedObjectNameException mone) {
        getLogWriter().error(mone);
        fail(mone.getMessage());
        return false;
      }
    };
  }
}
