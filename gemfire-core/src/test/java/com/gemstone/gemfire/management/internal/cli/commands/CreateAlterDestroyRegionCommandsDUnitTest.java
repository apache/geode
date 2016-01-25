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
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.ClassBuilder;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.cache.RegionEntryContext;
import com.gemstone.gemfire.management.cli.Result;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;

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

  public CreateAlterDestroyRegionCommandsDUnitTest(String name) {
    super(name);
  }

  /**
   * Asserts that the "compressor" option for the "create region" command succeeds for a recognized compressor.
   */
  public void testCreateRegionWithGoodCompressor() {
    createDefaultSetup(null);
    VM vm = Host.getHost(0).getVM(1);

    // Create a cache in vm 1
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        assertNotNull(getCache());
      }
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
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Region region = getCache().getRegion("compressedRegion");
        assertNotNull(region);
        assertTrue(SnappyCompressor.getDefaultInstance().equals(region.getAttributes().getCompressor()));
      }
    });

    // cleanup
    commandStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_REGION);
    commandStringBuilder.addOption(CliStrings.DESTROY_REGION__REGION, "compressedRegion");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
  }

  /**
   * Asserts that the "compressor" option for the "create region" command fails for an unrecognized compressorc.
   */
  public void testCreateRegionWithBadCompressor() {
    createDefaultSetup(null);

    VM vm = Host.getHost(0).getVM(1);

    // Create a cache in vm 1
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        assertNotNull(getCache());
      }
    });

    // Create a region with an unrecognized compressor
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION, "compressedRegion");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "REPLICATE");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__COMPRESSOR, "BAD_COMPRESSOR");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.ERROR, cmdResult.getStatus());

    // Assert that our region was not created
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Region region = getCache().getRegion("compressedRegion");
        assertNull(region);
      }
    });
  }

  /**
   * Asserts that a missing "compressor" option for the "create region" command results in a region with no
   * compression.
   */
  public void testCreateRegionWithNoCompressor() {
    createDefaultSetup(null);

    VM vm = Host.getHost(0).getVM(1);

    // Create a cache in vm 1
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        assertNotNull(getCache());
      }
    });

    // Create a region with no compression
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION, "testRegion");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "REPLICATE");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Assert that our newly created region has no compression
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Region region = getCache().getRegion("testRegion");
        assertNotNull(region);
        assertNull(region.getAttributes().getCompressor());
      }
    });

    // Cleanup
    commandStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_REGION);
    commandStringBuilder.addOption(CliStrings.DESTROY_REGION__REGION, "testRegion");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
  }

  public void testDestroyRegion() {
    createDefaultSetup(null);

    for (int i = 1; i <= 2; i++) {
      Host.getHost(0).getVM(i).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          final Cache cache = getCache();

          RegionFactory<Object, Object> factory = cache.createRegionFactory(RegionShortcut.PARTITION);
          factory.create("Customer");

          PartitionAttributesFactory paFactory = new PartitionAttributesFactory();
          paFactory.setColocatedWith("Customer");
          factory.setPartitionAttributes(paFactory.create());
          factory.create("Order");
        }
      });
    }

    // Make sure that the region has been registered with the Manager MXBean
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        DistributedTestCase.WaitCriterion wc = new DistributedTestCase.WaitCriterion() {
          @Override
          public boolean done() {
            try {
              MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;
              String queryExp = MessageFormat.format(ManagementConstants.OBJECTNAME__REGION_MXBEAN,
                  new Object[]{"/Customer", "*"});
              ObjectName queryExpON = new ObjectName(queryExp);
              return !(mbeanServer.queryNames(null, queryExpON).isEmpty());
            } catch (MalformedObjectNameException mone) {
              getLogWriter().error(mone);
              fail(mone.getMessage());
              return false;
            }
          }

          @Override
          public String description() {
            return "Waiting for the region to be registed with the MXBean";
          }
        };

        DistributedTestCase.waitForCriterion(wc, 5000, 500, true);
      }
    });

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


  public void testCreateRegion46391() throws IOException {
    createDefaultSetup(null);
    String command = CliStrings.CREATE_REGION + " --" + CliStrings.CREATE_REGION__REGION + "=" + this.region46391 + " --" + CliStrings.CREATE_REGION__REGIONSHORTCUT + "=REPLICATE";

    getLogWriter().info("testCreateRegion46391 create region command=" + command);

    CommandResult cmdResult = executeCommand(command);
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    command = CliStrings.PUT + " --" + CliStrings.PUT__KEY + "=k1" + " --" + CliStrings.PUT__VALUE + "=k1" + " --" + CliStrings.PUT__REGIONNAME + "=" + this.region46391;

    getLogWriter().info("testCreateRegion46391 put command=" + command);

    CommandResult cmdResult2 = executeCommand(command);
    assertEquals(Result.Status.OK, cmdResult2.getStatus());

    getLogWriter().info("testCreateRegion46391  cmdResult2=" + commandResultToString(cmdResult2));
    String str1 = "Result      : true";
    String str2 = "Key         : k1";
    String str3 = "Key Class   : java.lang.String";
    String str4 = "Value Class : java.lang.String";
    String str5 = "Old Value   : <NULL>";

    assertTrue(commandResultToString(cmdResult).contains("Region \"/" + this.region46391 + "\" created on"));

    assertTrue(commandResultToString(cmdResult2).contains(str1));
    assertTrue(commandResultToString(cmdResult2).contains(str2));
    assertTrue(commandResultToString(cmdResult2).contains(str3));
    assertTrue(commandResultToString(cmdResult2).contains(str4));
    assertTrue(commandResultToString(cmdResult2).contains(str5));
  }

  public void bug51924_testAlterRegion() throws IOException {
    createDefaultSetup(null);

    CommandResult cmdResult = executeCommand(CliStrings.LIST_REGION);
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    assertTrue(commandResultToString(cmdResult).contains("No Regions Found"));

    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        cache.createRegionFactory(RegionShortcut.PARTITION).setStatisticsEnabled(true).create(alterRegionName);
      }
    });

    this.alterVm1 = Host.getHost(0).getVM(1);
    this.alterVm1Name = "VM" + this.alterVm1.getPid();
    this.alterVm1.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.NAME_NAME, alterVm1Name);
        localProps.setProperty(DistributionConfig.GROUPS_NAME, "Group1");
        getSystem(localProps);
        Cache cache = getCache();

        // Setup queues and gateway senders to be used by all tests
        cache.createRegionFactory(RegionShortcut.PARTITION).setStatisticsEnabled(true).create(alterRegionName);
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
      }
    });

    this.alterVm2 = Host.getHost(0).getVM(2);
    this.alterVm2Name = "VM" + this.alterVm2.getPid();
    this.alterVm2.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.NAME_NAME, alterVm2Name);
        localProps.setProperty(DistributionConfig.GROUPS_NAME, "Group1,Group2");
        getSystem(localProps);
        Cache cache = getCache();

        cache.createRegionFactory(RegionShortcut.PARTITION).setStatisticsEnabled(true).create(alterRegionName);
      }
    });

    deployJarFilesForRegionAlter();
    regionAlterGroupTest();
    regionAlterSetAllTest();
    regionAlterNoChangeTest();
    regionAlterSetDefaultsTest();
    regionAlterManipulatePlugInsTest();

    this.alterVm1.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        getCache().getRegion(alterRegionName).destroyRegion();
      }
    });
  }

  private void regionAlterGroupTest() {
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__EVICTIONMAX, "5764");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    String stringResult = commandResultToString(cmdResult);
    assertEquals(4, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(
        stringContainsLine(stringResult, this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(
        stringContainsLine(stringResult, this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm1.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        RegionAttributes attributes = getCache().getRegion(alterRegionName).getAttributes();
        assertEquals(5764, attributes.getEvictionAttributes().getMaximum());
      }
    });

    this.alterVm2.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        RegionAttributes attributes = getCache().getRegion(alterRegionName).getAttributes();
        assertEquals(5764, attributes.getEvictionAttributes().getMaximum());
      }
    });

    commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, "/" + this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GROUP, "Group2");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__EVICTIONMAX, "6963");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(3, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertFalse(
        stringContainsLine(stringResult, this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(
        stringContainsLine(stringResult, this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm1.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        RegionAttributes attributes = getCache().getRegion(alterRegionName).getAttributes();
        assertEquals(5764, attributes.getEvictionAttributes().getMaximum());
      }
    });

    this.alterVm2.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        RegionAttributes attributes = getCache().getRegion(alterRegionName).getAttributes();
        assertEquals(6963, attributes.getEvictionAttributes().getMaximum());
      }
    });
  }

  private void regionAlterSetAllTest() {
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, "/" + this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__EVICTIONMAX, "35464");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CLONINGENABLED, "true");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID, this.alterAsyncEventQueueId1);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIME, "3453");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ENTRYEXPIRATIONIDLETIMEACTION, "DESTROY");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ENTRYEXPIRATIONTIMETOLIVE, "7563");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ENTRYEXPIRATIONTTLACTION, "DESTROY");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER, "com.cadrdunit.RegionAlterCacheListenerA");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELOADER, "com.cadrdunit.RegionAlterCacheLoader");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHEWRITER, "com.cadrdunit.RegionAlterCacheWriter");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, this.alterGatewaySenderId1);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIME, "6234");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGIONEXPIRATIONIDLETIMEACTION, "DESTROY");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGIONEXPIRATIONTTL, "4562");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGIONEXPIRATIONTTLACTION, "DESTROY");

    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    String stringResult = commandResultToString(cmdResult);
    assertEquals(5, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(stringContainsLine(stringResult, "Manager.*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(
        stringContainsLine(stringResult, this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(
        stringContainsLine(stringResult, this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm1.invoke(new SerializableRunnable() {
      @Override
      public void run() {
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
        assertEquals("com.cadrdunit.RegionAlterCacheListenerA", attributes.getCacheListeners()[0].getClass().getName());
        assertEquals("com.cadrdunit.RegionAlterCacheWriter", attributes.getCacheWriter().getClass().getName());
        assertEquals("com.cadrdunit.RegionAlterCacheLoader", attributes.getCacheLoader().getClass().getName());
      }
    });
  }

  private void regionAlterNoChangeTest() {
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, "/" + this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CLONINGENABLED, "true");

    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    String stringResult = commandResultToString(cmdResult);
    assertEquals(4, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(
        stringContainsLine(stringResult, this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(
        stringContainsLine(stringResult, this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm2.invoke(new SerializableRunnable() {
      @Override
      public void run() {
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
        assertEquals("com.cadrdunit.RegionAlterCacheListenerA", attributes.getCacheListeners()[0].getClass().getName());
        assertEquals("com.cadrdunit.RegionAlterCacheWriter", attributes.getCacheWriter().getClass().getName());
        assertEquals("com.cadrdunit.RegionAlterCacheLoader", attributes.getCacheLoader().getClass().getName());
      }
    });
  }

  private void regionAlterSetDefaultsTest() {
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, "/" + this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GROUP, "Group1");
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
    assertTrue(
        stringContainsLine(stringResult, this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(
        stringContainsLine(stringResult, this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm1.invoke(new SerializableRunnable() {
      @Override
      public void run() {
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
      }
    });
  }

  private void regionAlterManipulatePlugInsTest() {

    // Start out by putting 3 entries into each of the plug-in sets
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, "/" + this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID, this.alterAsyncEventQueueId1);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID, this.alterAsyncEventQueueId2);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID, this.alterAsyncEventQueueId3);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, this.alterGatewaySenderId1);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, this.alterGatewaySenderId2);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, this.alterGatewaySenderId3);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER, "com.cadrdunit.RegionAlterCacheListenerA");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER, "com.cadrdunit.RegionAlterCacheListenerB");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER, "com.cadrdunit.RegionAlterCacheListenerC");
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    String stringResult = commandResultToString(cmdResult);

    assertEquals(4, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(
        stringContainsLine(stringResult, this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(
        stringContainsLine(stringResult, this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm1.invoke(new SerializableRunnable() {
      @Override
      public void run() {
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
        assertEquals("com.cadrdunit.RegionAlterCacheListenerA", attributes.getCacheListeners()[0].getClass().getName());
        assertEquals("com.cadrdunit.RegionAlterCacheListenerB", attributes.getCacheListeners()[1].getClass().getName());
        assertEquals("com.cadrdunit.RegionAlterCacheListenerC", attributes.getCacheListeners()[2].getClass().getName());
      }
    });

    // Now take 1 entry out of each of the sets
    commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, "/" + this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID, this.alterAsyncEventQueueId1);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID, this.alterAsyncEventQueueId2);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, this.alterGatewaySenderId1);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, this.alterGatewaySenderId3);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER, "com.cadrdunit.RegionAlterCacheListenerB");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER, "com.cadrdunit.RegionAlterCacheListenerC");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(4, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(
        stringContainsLine(stringResult, this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(
        stringContainsLine(stringResult, this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm2.invoke(new SerializableRunnable() {
      @Override
      public void run() {
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
        assertEquals("com.cadrdunit.RegionAlterCacheListenerB", attributes.getCacheListeners()[0].getClass().getName());
        assertEquals("com.cadrdunit.RegionAlterCacheListenerC", attributes.getCacheListeners()[1].getClass().getName());
      }
    });

    // Add 1 back to each of the sets
    commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, "/" + this.alterRegionName);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GROUP, "Group1");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID, this.alterAsyncEventQueueId1);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID, this.alterAsyncEventQueueId2);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ASYNCEVENTQUEUEID, this.alterAsyncEventQueueId3);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, this.alterGatewaySenderId1);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, this.alterGatewaySenderId3);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GATEWAYSENDERID, this.alterGatewaySenderId2);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER, "com.cadrdunit.RegionAlterCacheListenerB");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER, "com.cadrdunit.RegionAlterCacheListenerC");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__CACHELISTENER, "com.cadrdunit.RegionAlterCacheListenerA");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());
    stringResult = commandResultToString(cmdResult);
    assertEquals(4, countLinesInString(stringResult, false));
    assertEquals(false, stringResult.contains("ERROR"));
    assertTrue(
        stringContainsLine(stringResult, this.alterVm1Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));
    assertTrue(
        stringContainsLine(stringResult, this.alterVm2Name + ".*Region \"/" + this.alterRegionName + "\" altered.*"));

    this.alterVm1.invoke(new SerializableRunnable() {
      @Override
      public void run() {
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
        assertEquals("com.cadrdunit.RegionAlterCacheListenerB", attributes.getCacheListeners()[0].getClass().getName());
        assertEquals("com.cadrdunit.RegionAlterCacheListenerC", attributes.getCacheListeners()[1].getClass().getName());
        assertEquals("com.cadrdunit.RegionAlterCacheListenerA", attributes.getCacheListeners()[2].getClass().getName());
      }
    });
  }

  /**
   * Asserts that creating, altering and destroying regions correctly updates the shared configuration.
   */
  public void testCreateAlterDestroyUpdatesSharedConfig() {
    disconnectAllFromDS();

    final String regionName = "testRegionSharedConfigRegion";
    final String groupName = "testRegionSharedConfigGroup";

    // Start the Locator and wait for shared configuration to be available
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    Host.getHost(0).getVM(3).invoke(new SerializableRunnable() {
      @Override
      public void run() {

        final File locatorLogFile = new File("locator-" + locatorPort + ".log");
        final Properties locatorProps = new Properties();
        locatorProps.setProperty(DistributionConfig.NAME_NAME, "Locator");
        locatorProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        locatorProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "fine");
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
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        localProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locatorPort);
        localProps.setProperty(DistributionConfig.GROUPS_NAME, groupName);
        getSystem(localProps);
        assertNotNull(getCache());
      }
    });

    // Test creating the region
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION, regionName);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "REPLICATE");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__STATISTICSENABLED, "true");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__GROUP, groupName);
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure that the region has been registered with the Manager MXBean
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        DistributedTestCase.WaitCriterion wc = new DistributedTestCase.WaitCriterion() {
          @Override
          public boolean done() {
            try {
              MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;
              String queryExp = MessageFormat.format(ManagementConstants.OBJECTNAME__REGION_MXBEAN,
                  new Object[]{"/" + regionName, "*"});
              ObjectName queryExpON = new ObjectName(queryExp);
              return !(mbeanServer.queryNames(null, queryExpON).isEmpty());
            } catch (MalformedObjectNameException mone) {
              getLogWriter().error(mone);
              fail(mone.getMessage());
              return false;
            }
          }

          @Override
          public String description() {
            return "Waiting for the region to be registed with the MXBean";
          }
        };

        DistributedTestCase.waitForCriterion(wc, 5000, 500, true);
      }
    });

    // Make sure the region exists in the shared config
    Host.getHost(0).getVM(3).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        SharedConfiguration sharedConfig = ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
        try {
          assertTrue(sharedConfig.getConfiguration(groupName).getCacheXmlContent().contains(regionName));
        } catch (Exception e) {
          fail("Error in cluster configuration service", e);
        }
      }
    });

    //Restart the data vm to make sure the changes are in place
    vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Cache cache = getCache();
        assertNotNull(cache);
        cache.close();
        assertTrue(cache.isClosed());

        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        localProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locatorPort);
        localProps.setProperty(DistributionConfig.GROUPS_NAME, groupName);
        localProps.setProperty(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "true");
        getSystem(localProps);
        cache = getCache();
        assertNotNull(cache);
        Region region = cache.getRegion(regionName);
        assertNotNull(region);
      }
    });


    // Test altering the region
    commandStringBuilder = new CommandStringBuilder(CliStrings.ALTER_REGION);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__REGION, regionName);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__GROUP, groupName);
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ENTRYEXPIRATIONTIMETOLIVE, "45635");
    commandStringBuilder.addOption(CliStrings.ALTER_REGION__ENTRYEXPIRATIONTTLACTION, "DESTROY");
    cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure the region was altered in the shared config
    Host.getHost(0).getVM(3).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        SharedConfiguration sharedConfig = ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
        try {
          assertTrue(sharedConfig.getConfiguration(groupName).getCacheXmlContent().contains("45635"));
        } catch (Exception e) {
          fail("Error in cluster configuration service");
        }
      }
    });

    //Restart the data vm to make sure the changes are in place
    vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        Cache cache = getCache();
        assertNotNull(cache);
        cache.close();
        assertTrue(cache.isClosed());

        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        localProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locatorPort);
        localProps.setProperty(DistributionConfig.GROUPS_NAME, groupName);
        localProps.setProperty(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "true");
        getSystem(localProps);
        cache = getCache();
        assertNotNull(cache);
        Region region = cache.getRegion(regionName);
        assertNotNull(region);

        return null;
      }
    });

  }

  public void testDestroyRegionWithSharedConfig() {

    disconnectAllFromDS();

    final String regionName = "testRegionSharedConfigRegion";
    final String groupName = "testRegionSharedConfigGroup";

    // Start the Locator and wait for shared configuration to be available
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    Host.getHost(0).getVM(3).invoke(new SerializableRunnable() {
      @Override
      public void run() {

        final File locatorLogFile = new File("locator-" + locatorPort + ".log");
        final Properties locatorProps = new Properties();
        locatorProps.setProperty(DistributionConfig.NAME_NAME, "Locator");
        locatorProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        locatorProps.setProperty(DistributionConfig.LOG_LEVEL_NAME, "fine");
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
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        localProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locatorPort);
        localProps.setProperty(DistributionConfig.GROUPS_NAME, groupName);
        getSystem(localProps);
        assertNotNull(getCache());
      }
    });

    // Test creating the region
    CommandStringBuilder commandStringBuilder = new CommandStringBuilder(CliStrings.CREATE_REGION);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGION, regionName);
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__REGIONSHORTCUT, "REPLICATE");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__STATISTICSENABLED, "true");
    commandStringBuilder.addOption(CliStrings.CREATE_REGION__GROUP, groupName);
    CommandResult cmdResult = executeCommand(commandStringBuilder.toString());
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure that the region has been registered with the Manager MXBean
    Host.getHost(0).getVM(0).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        DistributedTestCase.WaitCriterion wc = new DistributedTestCase.WaitCriterion() {
          @Override
          public boolean done() {
            try {
              MBeanServer mbeanServer = MBeanJMXAdapter.mbeanServer;
              String queryExp = MessageFormat.format(ManagementConstants.OBJECTNAME__REGION_MXBEAN,
                  new Object[]{"/" + regionName, "*"});
              ObjectName queryExpON = new ObjectName(queryExp);
              return !(mbeanServer.queryNames(null, queryExpON).isEmpty());
            } catch (MalformedObjectNameException mone) {
              getLogWriter().error(mone);
              fail(mone.getMessage());
              return false;
            }
          }

          @Override
          public String description() {
            return "Waiting for the region to be registed with the MXBean";
          }
        };

        DistributedTestCase.waitForCriterion(wc, 5000, 500, true);
      }
    });

    // Make sure the region exists in the shared config
    Host.getHost(0).getVM(3).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        SharedConfiguration sharedConfig = ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
        try {
          assertTrue(sharedConfig.getConfiguration(groupName).getCacheXmlContent().contains(regionName));
        } catch (Exception e) {
          fail("Error occurred in cluster configuration service");
        }
      }
    });

    // Test destroying the region
    commandStringBuilder = new CommandStringBuilder(CliStrings.DESTROY_REGION);
    commandStringBuilder.addOption(CliStrings.DESTROY_REGION__REGION, regionName);
    cmdResult = executeCommand(commandStringBuilder.toString());
    getLogWriter().info("#SB" + commandResultToString(cmdResult));
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    // Make sure the region was removed from the shared config
    Host.getHost(0).getVM(3).invoke(new SerializableRunnable() {
      @Override
      public void run() {
        SharedConfiguration sharedConfig = ((InternalLocator) Locator.getLocator()).getSharedConfiguration();
        try {
          assertFalse(sharedConfig.getConfiguration(groupName).getCacheXmlContent().contains(regionName));
        } catch (Exception e) {
          fail("Error occurred in cluster configuration service");
        }
      }
    });


    //Restart the data vm to make sure the region is not existing any more
    vm = Host.getHost(0).getVM(1);
    vm.invoke(new SerializableCallable() {
      @Override
      public Object call() {
        Cache cache = getCache();
        assertNotNull(cache);
        cache.close();
        assertTrue(cache.isClosed());

        Properties localProps = new Properties();
        localProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        localProps.setProperty(DistributionConfig.LOCATORS_NAME, "localhost:" + locatorPort);
        localProps.setProperty(DistributionConfig.GROUPS_NAME, groupName);
        localProps.setProperty(DistributionConfig.USE_CLUSTER_CONFIGURATION_NAME, "true");
        getSystem(localProps);
        cache = getCache();
        assertNotNull(cache);
        Region region = cache.getRegion(regionName);
        assertNull(region);

        return null;
      }
    });
  }

  @Override
  public void tearDown2() throws Exception {
    for (String path : this.filesToBeDeleted) {
      try {
        final File fileToDelete = new File(path);
        FileUtil.delete(fileToDelete);
        if (path.endsWith(".jar")) {
          executeCommand("undeploy --jar=" + fileToDelete.getName());
        }
      } catch (IOException e) {
        getLogWriter().error("Unable to delete file", e);
      }
    }
    this.filesToBeDeleted.clear();
    super.tearDown2();
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

    byte[] jarBytes = classBuilder.createJarFromClassContent("com/cadrdunit/RegionAlterCacheListenerA",
        "package com.cadrdunit;" + "import com.gemstone.gemfire.cache.util.CacheListenerAdapter;" + "public class RegionAlterCacheListenerA extends CacheListenerAdapter {}");
    writeJarBytesToFile(jarFile1, jarBytes);
    CommandResult cmdResult = executeCommand("deploy --jar=testAlterRegion1.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    jarBytes = classBuilder.createJarFromClassContent("com/cadrdunit/RegionAlterCacheListenerB",
        "package com.cadrdunit;" + "import com.gemstone.gemfire.cache.util.CacheListenerAdapter;" + "public class RegionAlterCacheListenerB extends CacheListenerAdapter {}");
    writeJarBytesToFile(jarFile2, jarBytes);
    cmdResult = executeCommand("deploy --jar=testAlterRegion2.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    jarBytes = classBuilder.createJarFromClassContent("com/cadrdunit/RegionAlterCacheListenerC",
        "package com.cadrdunit;" + "import com.gemstone.gemfire.cache.util.CacheListenerAdapter;" + "public class RegionAlterCacheListenerC extends CacheListenerAdapter {}");
    writeJarBytesToFile(jarFile3, jarBytes);
    cmdResult = executeCommand("deploy --jar=testAlterRegion3.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    jarBytes = classBuilder.createJarFromClassContent("com/cadrdunit/RegionAlterCacheLoader",
        "package com.cadrdunit;" + "import com.gemstone.gemfire.cache.CacheLoader;" + "import com.gemstone.gemfire.cache.CacheLoaderException;" + "import com.gemstone.gemfire.cache.LoaderHelper;" + "public class RegionAlterCacheLoader implements CacheLoader {" + "public void close() {}" + "public Object load(LoaderHelper helper) throws CacheLoaderException {return null;}}");
    writeJarBytesToFile(jarFile4, jarBytes);
    cmdResult = executeCommand("deploy --jar=testAlterRegion4.jar");
    assertEquals(Result.Status.OK, cmdResult.getStatus());

    jarBytes = classBuilder.createJarFromClassContent("com/cadrdunit/RegionAlterCacheWriter",
        "package com.cadrdunit;" + "import com.gemstone.gemfire.cache.util.CacheWriterAdapter;" + "public class RegionAlterCacheWriter extends CacheWriterAdapter {}");
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
