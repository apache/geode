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
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.awaitility.Awaitility.waitAtMost;

import org.apache.commons.io.FileUtils;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.ClassBuilder;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionEntryContext;
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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

@Category(DistributedTest.class)
public class CreateAlterDestroyRegionCommandsDUnitTest extends CliCommandTestBase {

  private static final long serialVersionUID = 1L;

  final String alterRegionName = "testAlterRegionRegion";
  final String alterAsyncEventQueueId1 = "testAlterRegionQueue1";
  final String alterAsyncEventQueueId2 = "testAlterRegionQueue2";
  final String alterAsyncEventQueueId3 = "testAlterRegionQueue3";
  final String alterGatewaySenderId1 = "testAlterRegionSender1";
  final String alterGatewaySenderId2 = "testAlterRegionSender2";
  final String alterGatewaySenderId3 = "testAlterRegionSender3";
  final String region46391 = "region46391";
  VM alterVm1;
  String alterVm1Name;
  VM alterVm2;
  String alterVm2Name;

  final List<String> filesToBeDeleted = new CopyOnWriteArrayList<String>();

  /**
   * Asserts that the "compressor" option for the "create region" command succeeds for a recognized
   * compressor.
   */
  @Test
  public void testCreateRegionWithGoodCompressor() {
    setUpJmxManagerOnVm0ThenConnect(null);
    VM vm = Host.getHost(0).getVM(1);

    // Create a cache in vm 1
    vm.invoke(() -> {
      assertNotNull(getCache());
    });

    // Run create region command with compression
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION, "compressedRegion");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "REPLICATE");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__COMPRESSOR,
        RegionEntryContext.DEFAULT_COMPRESSION_PROVIDER);
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure our region exists with compression enabled
    vm.invoke(() -> {
      Region region = getCache().getRegion("compressedRegion");
      assertNotNull(region);
      assertTrue(
          SnappyCompressor.getDefaultInstance().equals(region.getAttributes().getCompressor()));
    });

    // cleanup
    commandStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_REGION);
    commandStringBuilder.addOption(CliStrings.DESTROY_REGION__REGION, "compressedRegion");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
  }

  /**
   * Asserts that the "compressor" option for the "create region" command fails for an unrecognized
   * compressorc.
   */
  @Test
  public void testCreateRegionWithBadCompressor() {
    setUpJmxManagerOnVm0ThenConnect(null);

    VM vm = Host.getHost(0).getVM(1);

    // Create a cache in vm 1
    vm.invoke(() -> {
      assertNotNull(getCache());
    });

    // Create a region with an unrecognized compressor
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION, "compressedRegion");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "REPLICATE");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__COMPRESSOR, "BAD_COMPRESSOR");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.ERROR, cmdResult.getStatus());

    // Assert that our region was not created
    vm.invoke(() -> {
      Region region = getCache().getRegion("compressedRegion");
      assertNull(region);
    });
  }

  /**
   * Asserts that a missing "compressor" option for the "create region" command results in a region
   * with no compression.
   */
  @Test
  public void testCreateRegionWithNoCompressor() {
    setUpJmxManagerOnVm0ThenConnect(null);

    VM vm = Host.getHost(0).getVM(1);

    // Create a cache in vm 1
    vm.invoke(() -> {
      assertNotNull(getCache());
    });

    // Create a region with no compression
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION, "testRegion");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "REPLICATE");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Assert that our newly created region has no compression
    vm.invoke(() -> {
      Region region = getCache().getRegion("testRegion");
      assertNotNull(region);
      assertNull(region.getAttributes().getCompressor());
    });

    // Cleanup
    commandStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_REGION);
    commandStringBuilder.addOption(CliStrings.DESTROY_REGION__REGION, "testRegion");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
  }

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
      Host.getHost(0).getVM(i).invoke(() -> {
        assertNull("Region still exists in VM " + x, getCache().getRegion("Customer"));
      });
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

  private void waitForRegionMBeanCreation(final String regionPath, final int mbeanCount) {
    Host.getHost(0).getVM(0).invoke(() -> {
      waitAtMost(5, TimeUnit.SECONDS).until(newRegionMBeanIsCreated(regionPath, mbeanCount));
    });
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

  @Category(FlakyTest.class) // GEODE-973: random ports, BindException,
                             // java.rmi.server.ExportException: Port already in use
  @Test
  public void testCreateRegion46391() throws IOException {
    setUpJmxManagerOnVm0ThenConnect(null); // GEODE-973: getRandomAvailablePort
    String command = CliStrings.CREATE_REGION + " --" + CliStrings.CREATE_REGION__REGION + "="
        + this.region46391 + " --" + CliStrings.CREATE_REGION__REGIONSHORTCUT + "=REPLICATE";

    getLogWriter().info("testCreateRegion46391 create region command=" + command);

    CommandResult cmdResult = executeCommand(command);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    command = CliStrings.PUT + " --" + CliStrings.PUT__KEY + "=k1" + " --" + CliStrings.PUT__VALUE
        + "=k1" + " --" + CliStrings.PUT__REGIONNAME + "=" + this.region46391;

    getLogWriter().info("testCreateRegion46391 put command=" + command);

    CommandResult cmdResult2 = executeCommand(command);
    assertEquals(Result.Status.OK, cmdResult2.getStatus());

    getLogWriter().info("testCreateRegion46391  cmdResult2=" + commandResultToString(cmdResult2));
    String str1 = "Result      : true";
    String str2 = "Key         : k1";
    String str3 = "Key Class   : java.lang.String";
    String str4 = "Value Class : java.lang.String";
    String str5 = "Old Value   : <NULL>";

    assertTrue(commandResultToString(cmdResult)
        .contains("Region \"/" + this.region46391 + "\" created on"));

    assertTrue(commandResultToString(cmdResult2).contains(str1));
    assertTrue(commandResultToString(cmdResult2).contains(str2));
    assertTrue(commandResultToString(cmdResult2).contains(str3));
    assertTrue(commandResultToString(cmdResult2).contains(str4));
    assertTrue(commandResultToString(cmdResult2).contains(str5));
  }

  @Ignore("bug51924")
  @Test
  public void testAlterRegion() throws IOException {
    setUpJmxManagerOnVm0ThenConnect(null);

    CommandResult cmdResult = executeCommand(CliStrings.LIST_REGION);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    assertTrue(commandResultToString(cmdResult).contains("No Regions Found"));

    Host.getHost(0).getVM(0).invoke(() -> {
      Cache cache = getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION).setStatisticsEnabled(true)
          .create(alterRegionName);
    });

    this.alterVm1 = Host.getHost(0).getVM(1);
    this.alterVm1Name = "VM" + this.alterVm1.getPid();
    this.alterVm1.invoke(() -> {
      Properties localProps = new Properties();
      localProps.setProperty(NAME, alterVm1Name);
      localProps.setProperty(GROUPS, "Group1");
      getSystem(localProps);
      Cache cache = getCache();

      // Setup queues and gateway senders to be used by all tests
      cache.createRegionFactory(RegionShortcut.PARTITION).setStatisticsEnabled(true)
          .create(alterRegionName);
      AsyncEventListener listener = new AsyncEventListener() {
        @Override
        public void close() {
          // Nothing to do
        }

        @Override
        public boolean processEvents(List<AsyncEvent> events) {
          return true;
        }
      };
      cache.createAsyncEventQueueFactory().create(alterAsyncEventQueueId1, listener);
      cache.createAsyncEventQueueFactory().create(alterAsyncEventQueueId2, listener);
      cache.createAsyncEventQueueFactory().create(alterAsyncEventQueueId3, listener);

      GatewaySenderFactory gatewaySenderFactory = cache.createGatewaySenderFactory();
      gatewaySenderFactory.setManualStart(true);
      gatewaySenderFactory.create(alterGatewaySenderId1, 2);
      gatewaySenderFactory.create(alterGatewaySenderId2, 3);
      gatewaySenderFactory.create(alterGatewaySenderId3, 4);
    });

    this.alterVm2 = Host.getHost(0).getVM(2);
    this.alterVm2Name = "VM" + this.alterVm2.getPid();
    this.alterVm2.invoke(() -> {
      Properties localProps = new Properties();
      localProps.setProperty(NAME, alterVm2Name);
      localProps.setProperty(GROUPS, "Group1,Group2");
      getSystem(localProps);
      Cache cache = getCache();

      cache.createRegionFactory(RegionShortcut.PARTITION).setStatisticsEnabled(true)
          .create(alterRegionName);
    });

    deployJarFilesForRegionAlter();
    regionAlterGroupTest();
    regionAlterSetAllTest();
    regionAlterNoChangeTest();
    regionAlterSetDefaultsTest();
    regionAlterManipulatePlugInsTest();

    this.alterVm1.invoke(() -> {
      getCache().getRegion(alterRegionName).destroyRegion();
    });
  }

  private void regionAlterGroupTest() {
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__EVICTIONMAX, "5764");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    String stringResult = commandResultToString(cmdResult);
    assertEquals(4, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm1.invoke(() -> {
      RegionAttributes attributes = getCache().getRegion(alterRegionName).getAttributes();
      assertEquals(5764, attributes.getEvictionAttributes().getMaximum());
    });

    this.alterVm2.invoke(() -> {
      RegionAttributes attributes = getCache().getRegion(alterRegionName).getAttributes();
      assertEquals(5764, attributes.getEvictionAttributes().getMaximum());
    });

    commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, "/" + this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.GROUP, "Group2");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__EVICTIONMAX, "6963");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertFalse(stringContainsLine(stringResult,
        this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm1.invoke(() -> {
      RegionAttributes attributes = getCache().getRegion(alterRegionName).getAttributes();
      assertEquals(5764, attributes.getEvictionAttributes().getMaximum());
    });

    this.alterVm2.invoke(() -> {
      RegionAttributes attributes = getCache().getRegion(alterRegionName).getAttributes();
      assertEquals(6963, attributes.getEvictionAttributes().getMaximum());
    });
  }

  private void regionAlterSetAllTest() {
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, "/" + this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__EVICTIONMAX, "35464");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CLONINGENABLED, "true");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID,
        this.alterAsyncEventQueueId1);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIME, "3453");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIMEACTION,
        "DESTROY");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ENTRYEXPIRATIONTIMETOLIVE, "7563");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ENTRYEXPIRATIONTTLACTION, "DESTROY");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER,
        "com.cadrdunit.RegionAlterCacheListenerA");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELOADER,
        "com.cadrdunit.RegionAlterCacheLoader");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHEWRITER,
        "com.cadrdunit.RegionAlterCacheWriter");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID,
        this.alterGatewaySenderId1);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIME, "6234");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIMEACTION,
        "DESTROY");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGIONEXPIRATIONTTL, "4562");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGIONEXPIRATIONTTLACTION, "DESTROY");

    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    String stringResult = commandResultToString(cmdResult);
    assertEquals(5, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult,
        "Manager.*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm1.invoke(() -> {
      RegionAttributes attributes = getCache().getRegion(alterRegionName).getAttributes();
      assertEquals(35464, attributes.getEvictionAttributes().getMaximum());
      assertEquals(3453, attributes.getEntryIdleTimeout().getTimeout());
      assertTrue(attributes.getEntryIdleTimeout().getAction().isDestroy());
      assertEquals(7563, attributes.getEntryTimeToLive().getTimeout());
      assertTrue(attributes.getEntryTimeToLive().getAction().isDestroy());
      assertEquals(6234, attributes.getRegionIdleTimeout().getTimeout());
      assertTrue(attributes.getRegionIdleTimeout().getAction().isDestroy());
      assertEquals(4562, attributes.getRegionTimeToLive().getTimeout());
      assertTrue(attributes.getRegionTimeToLive().getAction().isDestroy());
      assertEquals(1, attributes.getAsyncEventQueueIds().size());
      assertTrue(attributes.getAsyncEventQueueIds().contains(alterAsyncEventQueueId1));
      assertEquals(1, attributes.getGatewaySenderIds().size());
      assertTrue(attributes.getGatewaySenderIds().contains(alterGatewaySenderId1));
      assertEquals(1, attributes.getCacheListeners().length);
      assertEquals("com.cadrdunit.RegionAlterCacheListenerA",
          attributes.getCacheListeners()[0].getClass().getName());
      assertEquals("com.cadrdunit.RegionAlterCacheWriter",
          attributes.getCacheWriter().getClass().getName());
      assertEquals("com.cadrdunit.RegionAlterCacheLoader",
          attributes.getCacheLoader().getClass().getName());
    });
  }

  private void regionAlterNoChangeTest() {
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, "/" + this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CLONINGENABLED, "true");

    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    String stringResult = commandResultToString(cmdResult);
    assertEquals(4, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm2.invoke(() -> {
      RegionAttributes attributes = getCache().getRegion(alterRegionName).getAttributes();
      assertEquals(35464, attributes.getEvictionAttributes().getMaximum());
      assertEquals(3453, attributes.getEntryIdleTimeout().getTimeout());
      assertTrue(attributes.getEntryIdleTimeout().getAction().isDestroy());
      assertEquals(7563, attributes.getEntryTimeToLive().getTimeout());
      assertTrue(attributes.getEntryTimeToLive().getAction().isDestroy());
      assertEquals(6234, attributes.getRegionIdleTimeout().getTimeout());
      assertTrue(attributes.getRegionIdleTimeout().getAction().isDestroy());
      assertEquals(4562, attributes.getRegionTimeToLive().getTimeout());
      assertTrue(attributes.getRegionTimeToLive().getAction().isDestroy());
      assertEquals(1, attributes.getAsyncEventQueueIds().size());
      assertTrue(attributes.getAsyncEventQueueIds().contains(alterAsyncEventQueueId1));
      assertEquals(1, attributes.getGatewaySenderIds().size());
      assertTrue(attributes.getGatewaySenderIds().contains(alterGatewaySenderId1));
      assertEquals(1, attributes.getCacheListeners().length);
      assertEquals("com.cadrdunit.RegionAlterCacheListenerA",
          attributes.getCacheListeners()[0].getClass().getName());
      assertEquals("com.cadrdunit.RegionAlterCacheWriter",
          attributes.getCacheWriter().getClass().getName());
      assertEquals("com.cadrdunit.RegionAlterCacheLoader",
          attributes.getCacheLoader().getClass().getName());
    });
  }

  private void regionAlterSetDefaultsTest() {
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, "/" + this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__EVICTIONMAX);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CLONINGENABLED);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIME);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ENTRYEXPIRATIONTTLACTION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELOADER);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHEWRITER);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIME);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIMEACTION);

    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    String stringResult = commandResultToString(cmdResult);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    assertEquals(4, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm1.invoke(() -> {
      RegionAttributes attributes = getCache().getRegion(alterRegionName).getAttributes();
      assertEquals(0, attributes.getEvictionAttributes().getMaximum());
      assertEquals(0, attributes.getEntryIdleTimeout().getTimeout());
      assertTrue(attributes.getEntryIdleTimeout().getAction().isDestroy());
      assertEquals(7563, attributes.getEntryTimeToLive().getTimeout());
      assertTrue(attributes.getEntryTimeToLive().getAction().isInvalidate());
      assertEquals(0, attributes.getRegionIdleTimeout().getTimeout());
      assertTrue(attributes.getRegionIdleTimeout().getAction().isInvalidate());
      assertEquals(4562, attributes.getRegionTimeToLive().getTimeout());
      assertTrue(attributes.getRegionTimeToLive().getAction().isDestroy());
      assertEquals(0, attributes.getAsyncEventQueueIds().size());
      assertEquals(0, attributes.getGatewaySenderIds().size());
      assertEquals(0, attributes.getCacheListeners().length);
    });
  }

  private void regionAlterManipulatePlugInsTest() {

    // Start out by putting 3 entries into each of the plug-in sets
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, "/" + this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID,
        this.alterAsyncEventQueueId1);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID,
        this.alterAsyncEventQueueId2);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID,
        this.alterAsyncEventQueueId3);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID,
        this.alterGatewaySenderId1);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID,
        this.alterGatewaySenderId2);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID,
        this.alterGatewaySenderId3);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER,
        "com.cadrdunit.RegionAlterCacheListenerA");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER,
        "com.cadrdunit.RegionAlterCacheListenerB");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER,
        "com.cadrdunit.RegionAlterCacheListenerC");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    String stringResult = commandResultToString(cmdResult);

    assertEquals(4, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm1.invoke(() -> {
      RegionAttributes attributes = getCache().getRegion(alterRegionName).getAttributes();
      assertEquals(3, attributes.getAsyncEventQueueIds().size());
      assertTrue(attributes.getAsyncEventQueueIds().contains(alterAsyncEventQueueId1));
      assertTrue(attributes.getAsyncEventQueueIds().contains(alterAsyncEventQueueId2));
      assertTrue(attributes.getAsyncEventQueueIds().contains(alterAsyncEventQueueId3));
      assertEquals(3, attributes.getGatewaySenderIds().size());
      assertTrue(attributes.getGatewaySenderIds().contains(alterGatewaySenderId1));
      assertTrue(attributes.getGatewaySenderIds().contains(alterGatewaySenderId2));
      assertTrue(attributes.getGatewaySenderIds().contains(alterGatewaySenderId3));
      assertEquals(3, attributes.getCacheListeners().length);
      assertEquals("com.cadrdunit.RegionAlterCacheListenerA",
          attributes.getCacheListeners()[0].getClass().getName());
      assertEquals("com.cadrdunit.RegionAlterCacheListenerB",
          attributes.getCacheListeners()[1].getClass().getName());
      assertEquals("com.cadrdunit.RegionAlterCacheListenerC",
          attributes.getCacheListeners()[2].getClass().getName());
    });

    // Now take 1 entry out of each of the sets
    commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, "/" + this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID,
        this.alterAsyncEventQueueId1);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID,
        this.alterAsyncEventQueueId2);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID,
        this.alterGatewaySenderId1);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID,
        this.alterGatewaySenderId3);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER,
        "com.cadrdunit.RegionAlterCacheListenerB");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER,
        "com.cadrdunit.RegionAlterCacheListenerC");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(4, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm2.invoke(() -> {
      RegionAttributes attributes = getCache().getRegion(alterRegionName).getAttributes();
      assertEquals(2, attributes.getAsyncEventQueueIds().size());
      Iterator iterator = attributes.getAsyncEventQueueIds().iterator();
      assertEquals(alterAsyncEventQueueId1, iterator.next());
      assertEquals(alterAsyncEventQueueId2, iterator.next());
      assertEquals(2, attributes.getGatewaySenderIds().size());
      iterator = attributes.getGatewaySenderIds().iterator();
      assertEquals(alterGatewaySenderId1, iterator.next());
      assertEquals(alterGatewaySenderId3, iterator.next());
      assertEquals(2, attributes.getCacheListeners().length);
      assertEquals("com.cadrdunit.RegionAlterCacheListenerB",
          attributes.getCacheListeners()[0].getClass().getName());
      assertEquals("com.cadrdunit.RegionAlterCacheListenerC",
          attributes.getCacheListeners()[1].getClass().getName());
    });

    // Add 1 back to each of the sets
    commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, "/" + this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID,
        this.alterAsyncEventQueueId1);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID,
        this.alterAsyncEventQueueId2);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID,
        this.alterAsyncEventQueueId3);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID,
        this.alterGatewaySenderId1);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID,
        this.alterGatewaySenderId3);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID,
        this.alterGatewaySenderId2);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER,
        "com.cadrdunit.RegionAlterCacheListenerB");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER,
        "com.cadrdunit.RegionAlterCacheListenerC");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER,
        "com.cadrdunit.RegionAlterCacheListenerA");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(4, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm1.invoke(() -> {
      RegionAttributes attributes = getCache().getRegion(alterRegionName).getAttributes();
      assertEquals(3, attributes.getAsyncEventQueueIds().size());
      assertTrue(attributes.getAsyncEventQueueIds().contains(alterAsyncEventQueueId1));
      assertTrue(attributes.getAsyncEventQueueIds().contains(alterAsyncEventQueueId2));
      assertTrue(attributes.getAsyncEventQueueIds().contains(alterAsyncEventQueueId3));
      assertEquals(3, attributes.getGatewaySenderIds().size());
      assertTrue(attributes.getGatewaySenderIds().contains(alterGatewaySenderId1));
      assertTrue(attributes.getGatewaySenderIds().contains(alterGatewaySenderId3));
      assertTrue(attributes.getGatewaySenderIds().contains(alterGatewaySenderId2));
      assertEquals(3, attributes.getCacheListeners().length);
      assertEquals("com.cadrdunit.RegionAlterCacheListenerB",
          attributes.getCacheListeners()[0].getClass().getName());
      assertEquals("com.cadrdunit.RegionAlterCacheListenerC",
          attributes.getCacheListeners()[1].getClass().getName());
      assertEquals("com.cadrdunit.RegionAlterCacheListenerA",
          attributes.getCacheListeners()[2].getClass().getName());
    });
  }

  @Category(FlakyTest.class) // GEODE-3018
  @Test
  public void testAlterRegionResetCacheListeners() throws IOException {
    setUpJmxManagerOnVm0ThenConnect(null);

    CommandResult cmdResult = executeCommand(CliStrings.LIST_REGION);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    assertTrue(commandResultToString(cmdResult).contains("No Regions Found"));

    Host.getHost(0).getVM(0).invoke(() -> {
      Cache cache = getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION).setStatisticsEnabled(true)
          .create(alterRegionName);
    });

    this.alterVm1 = Host.getHost(0).getVM(1);
    this.alterVm1Name = "VM" + this.alterVm1.getPid();
    this.alterVm1.invoke(() -> {
      Properties localProps = new Properties();
      localProps.setProperty(NAME, alterVm1Name);
      localProps.setProperty(GROUPS, "Group1");
      getSystem(localProps);
      Cache cache = getCache();

      // Setup queues and gateway senders to be used by all tests
      cache.createRegionFactory(RegionShortcut.PARTITION).setStatisticsEnabled(true)
          .create(alterRegionName);
    });

    this.alterVm2 = Host.getHost(0).getVM(2);
    this.alterVm2Name = "VM" + this.alterVm2.getPid();
    this.alterVm2.invoke(() -> {
      Properties localProps = new Properties();
      localProps.setProperty(NAME, alterVm2Name);
      localProps.setProperty(GROUPS, "Group1,Group2");
      getSystem(localProps);
      Cache cache = getCache();

      cache.createRegionFactory(RegionShortcut.PARTITION).setStatisticsEnabled(true)
          .create(alterRegionName);
    });

    deployJarFilesForRegionAlter();

    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, "/" + this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER,
        "com.cadrdunit.RegionAlterCacheListenerA,com.cadrdunit.RegionAlterCacheListenerB,com.cadrdunit.RegionAlterCacheListenerC");

    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    String stringResult = commandResultToString(cmdResult);

    assertEquals(5, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm1.invoke(() -> {
      RegionAttributes attributes = getCache().getRegion(alterRegionName).getAttributes();
      assertEquals(3, attributes.getCacheListeners().length);
      assertEquals("com.cadrdunit.RegionAlterCacheListenerA",
          attributes.getCacheListeners()[0].getClass().getName());
      assertEquals("com.cadrdunit.RegionAlterCacheListenerB",
          attributes.getCacheListeners()[1].getClass().getName());
      assertEquals("com.cadrdunit.RegionAlterCacheListenerC",
          attributes.getCacheListeners()[2].getClass().getName());
    });

    // Add 1 back to each of the sets
    commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, "/" + this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER, "''");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(4, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(stringContainsLine(stringResult,
        this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm1.invoke(() -> {
      RegionAttributes attributes = getCache().getRegion(alterRegionName).getAttributes();
      assertEquals(0, attributes.getCacheListeners().length);
    });
  }

  /**
   * Asserts that creating, altering and destroying regions correctly updates the shared
   * configuration.
   */
  @Category(FlakyTest.class) // GEODE-2009
  @Test
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

        waitAtMost(5, TimeUnit.SECONDS).until(() -> locator.isSharedConfigurationRunning());

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
    waitForRegionMBeanCreation(regionPath, 1);

    // Make sure the region exists in the shared config
    Host.getHost(0).getVM(0).invoke(() -> {
      ClusterConfigurationService sharedConfig =
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
      ClusterConfigurationService sharedConfig =
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

        waitAtMost(5, TimeUnit.SECONDS).until(() -> locator.isSharedConfigurationRunning());
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


  final String PR_STRING = " package com.cadrdunit;"
      + " public class TestPartitionResolver implements org.apache.geode.cache.PartitionResolver { "
      + "   @Override" + "   public void close() {" + "   }" + "   @Override"
      + "   public Object getRoutingObject(org.apache.geode.cache.EntryOperation opDetails) { "
      + "    return null; " + "   }" + "   @Override" + "   public String getName() { "
      + "    return \"TestPartitionResolver\";" + "   }" + " }";

  /**
   * Test Description 1. Deploy a JAR with Custom Partition Resolver 2. Create Region with Partition
   * Resolver 3. Region should get created with no Errors 4. Verify Region Partition Attributes for
   * Partition Resolver
   */
  @Test
  public void testCreateRegionWithPartitionResolver() throws IOException {
    setUpJmxManagerOnVm0ThenConnect(null);
    VM vm = Host.getHost(0).getVM(1);
    // Create a cache in vm 1
    vm.invoke(() -> {
      assertNotNull(getCache());
    });

    ClassBuilder classBuilder = new ClassBuilder();
    // classBuilder.addToClassPath(".");
    final File prJarFile = new File(temporaryFolder.getRoot().getCanonicalPath() + File.separator,
        "myPartitionResolver.jar");
    this.filesToBeDeleted.add(prJarFile.getAbsolutePath());
    byte[] jarBytes =
        classBuilder.createJarFromClassContent("com/cadrdunit/TestPartitionResolver", PR_STRING);
    writeJarBytesToFile(prJarFile, jarBytes);

    CommandResult cmdResult = executeCommand("deploy --jar=" + prJarFile.getAbsolutePath());
    assertEquals(Result.Status.OK, cmdResult.getStatus());


    // Create a region with an unrecognized compressor
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION, "regionWithPartitionResolver");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__PARTITION_RESOLVER,
        "com.cadrdunit.TestPartitionResolver");
    CommandResult cmdResult1 = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult1.getStatus());

    // Assert that our region was not created
    vm.invoke(() -> {
      Region region = getCache().getRegion("regionWithPartitionResolver");
      assertNotNull(region);

      PartitionedRegion pr = (PartitionedRegion) region;
      PartitionAttributes partitionAttributes = pr.getPartitionAttributes();
      assertNotNull(partitionAttributes);
      PartitionResolver partitionResolver = partitionAttributes.getPartitionResolver();
      assertNotNull(partitionResolver);
      assertEquals("TestPartitionResolver", partitionResolver.getName());
    });

    vm.invoke(() -> {
      getCache().getRegion("regionWithPartitionResolver").destroyRegion();
    });
  }

  @Test
  public void testCreateRegionWithInvalidPartitionResolver() {
    setUpJmxManagerOnVm0ThenConnect(null);
    VM vm = Host.getHost(0).getVM(1);
    // Create a cache in vm 1
    vm.invoke(() -> {
      assertNotNull(getCache());
    });

    // Create a region with an unrecognized compressor
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION,
        "testCreateRegionWithInvalidPartitionResolver");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "PARTITION");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__PARTITION_RESOLVER, "a.b.c.d");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.ERROR, cmdResult.getStatus());

    // Assert that our region was not created
    vm.invoke(() -> {
      Region region = getCache().getRegion("testCreateRegionWithInvalidPartitionResolver");
      assertNull(region);
    });
  }

  /**
   * Test Description Try creating region of type REPLICATED and specify partition resolver Region
   * Creation should fail.
   */
  @Test
  public void testCreateRegionForReplicatedRegionWithParitionResolver() {
    setUpJmxManagerOnVm0ThenConnect(null);
    VM vm = Host.getHost(0).getVM(1);
    // Create a cache in vm 1
    vm.invoke(() -> {
      assertNotNull(getCache());
    });

    // Create a region with an unrecognized compressor
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION,
        "testCreateRegionForReplicatedRegionWithParitionResolver");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "REPLICATE");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__PARTITION_RESOLVER, "a.b.c.d");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.ERROR, cmdResult.getStatus());

    // Assert that our region was not created
    vm.invoke(() -> {
      Region region =
          getCache().getRegion("testCreateRegionForReplicatedRegionWithParitionResolver");
      assertNull(region);
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

  /**
   * Deploys JAR files which contain classes to be instantiated by the "alter region" test.
   */
  private void deployJarFilesForRegionAlter() throws IOException {
    ClassBuilder classBuilder = new ClassBuilder();
    final File jarFile1 = new File(new File(".").getAbsolutePath(), "testAlterRegion1.jar");
    this.filesToBeDeleted.add(jarFile1.getAbsolutePath());
    final File jarFile2 = new File(new File(".").getAbsolutePath(), "testAlterRegion2.jar");
    this.filesToBeDeleted.add(jarFile2.getAbsolutePath());
    final File jarFile3 = new File(new File(".").getAbsolutePath(), "testAlterRegion3.jar");
    this.filesToBeDeleted.add(jarFile3.getAbsolutePath());
    final File jarFile4 = new File(new File(".").getAbsolutePath(), "testAlterRegion4.jar");
    this.filesToBeDeleted.add(jarFile4.getAbsolutePath());
    final File jarFile5 = new File(new File(".").getAbsolutePath(), "testAlterRegion5.jar");
    this.filesToBeDeleted.add(jarFile5.getAbsolutePath());

    byte[] jarBytes =
        classBuilder.createJarFromClassContent("com/cadrdunit/RegionAlterCacheListenerA",
            "package com.cadrdunit;" + "import org.apache.geode.cache.util.CacheListenerAdapter;"
                + "public class RegionAlterCacheListenerA extends CacheListenerAdapter {}");
    writeJarBytesToFile(jarFile1, jarBytes);
    CommandResult cmdResult = executeCommand("deploy --jar=testAlterRegion1.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    jarBytes = classBuilder.createJarFromClassContent("com/cadrdunit/RegionAlterCacheListenerB",
        "package com.cadrdunit;" + "import org.apache.geode.cache.util.CacheListenerAdapter;"
            + "public class RegionAlterCacheListenerB extends CacheListenerAdapter {}");
    writeJarBytesToFile(jarFile2, jarBytes);
    cmdResult = executeCommand("deploy --jar=testAlterRegion2.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    jarBytes = classBuilder.createJarFromClassContent("com/cadrdunit/RegionAlterCacheListenerC",
        "package com.cadrdunit;" + "import org.apache.geode.cache.util.CacheListenerAdapter;"
            + "public class RegionAlterCacheListenerC extends CacheListenerAdapter {}");
    writeJarBytesToFile(jarFile3, jarBytes);
    cmdResult = executeCommand("deploy --jar=testAlterRegion3.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    jarBytes = classBuilder.createJarFromClassContent("com/cadrdunit/RegionAlterCacheLoader",
        "package com.cadrdunit;" + "import org.apache.geode.cache.CacheLoader;"
            + "import org.apache.geode.cache.CacheLoaderException;"
            + "import org.apache.geode.cache.LoaderHelper;"
            + "public class RegionAlterCacheLoader implements CacheLoader {"
            + "public void close() {}"
            + "public Object load(LoaderHelper helper) throws CacheLoaderException {return null;}}");
    writeJarBytesToFile(jarFile4, jarBytes);
    cmdResult = executeCommand("deploy --jar=testAlterRegion4.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    jarBytes = classBuilder.createJarFromClassContent("com/cadrdunit/RegionAlterCacheWriter",
        "package com.cadrdunit;" + "import org.apache.geode.cache.util.CacheWriterAdapter;"
            + "public class RegionAlterCacheWriter extends CacheWriterAdapter {}");
    writeJarBytesToFile(jarFile5, jarBytes);
    cmdResult = executeCommand("deploy --jar=testAlterRegion5.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());
  }

  private void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.flush();
    outStream.close();
  }

}
