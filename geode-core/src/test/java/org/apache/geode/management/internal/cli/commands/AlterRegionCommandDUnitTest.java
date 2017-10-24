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

import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;

@Category({DistributedTest.class, FlakyTest.class}) // GEODE-3018 GEODE-3530
@SuppressWarnings("serial")
public class AlterRegionCommandDUnitTest extends CliCommandTestBase {

  private final String alterRegionName = "testAlterRegionRegion";
  private final String alterAsyncEventQueueId1 = "testAlterRegionQueue1";
  private final String alterAsyncEventQueueId2 = "testAlterRegionQueue2";
  private final String alterAsyncEventQueueId3 = "testAlterRegionQueue3";
  private final String alterGatewaySenderId1 = "testAlterRegionSender1";
  private final String alterGatewaySenderId2 = "testAlterRegionSender2";
  private final String alterGatewaySenderId3 = "testAlterRegionSender3";
  private VM alterVm1;
  private String alterVm1Name;
  private VM alterVm2;
  private String alterVm2Name;

  private final List<String> filesToBeDeleted = new CopyOnWriteArrayList<>();

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
    this.alterVm1Name = "VM" + this.alterVm1.getId();
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
    this.alterVm2Name = "VM" + this.alterVm2.getId();
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

    this.alterVm1.invoke(() -> getCache().getRegion(alterRegionName).destroyRegion());
  }

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
    this.alterVm1Name = "VM" + this.alterVm1.getId();
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
    this.alterVm2Name = "VM" + this.alterVm2.getId();
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

      assertThat(Arrays.stream(attributes.getCacheListeners()).map(c -> c.getClass().getName())
          .collect(Collectors.toSet())).containsExactlyInAnyOrder(
              "com.cadrdunit.RegionAlterCacheListenerA", "com.cadrdunit.RegionAlterCacheListenerB",
              "com.cadrdunit.RegionAlterCacheListenerC");
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

  private void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.flush();
    outStream.close();
  }
}
