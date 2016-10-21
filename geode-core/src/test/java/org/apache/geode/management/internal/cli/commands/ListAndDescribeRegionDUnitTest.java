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

import static org.apache.geode.distributed.ConfigurationProperties.*;

import org.apache.geode.cache.*;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.cli.util.RegionAttributesNames;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.dunit.Assert.*;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;

@Category(DistributedTest.class)
public class ListAndDescribeRegionDUnitTest extends CliCommandTestBase {

  private static final String REGION1 = "region1";
  private static final String REGION2 = "region2";
  private static final String REGION3 = "region3";
  private static final String SUBREGION1A = "subregion1A";
  private static final String SUBREGION1B = "subregion1B";
  private static final String SUBREGION1C = "subregion1C";
  private static final String PR1 = "PR1";
  private static final String LOCALREGIONONMANAGER = "LocalRegionOnManager";

  static class CacheListener2 extends CacheListenerAdapter {
  }

  static class CacheListener1 extends CacheListenerAdapter {
  }

  private Properties createProperties(String name, String groups) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOG_LEVEL, "info");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.setProperty(NAME, name);
    props.setProperty(GROUPS, groups);
    return props;
  }

  private void createPartitionedRegion1() {
    final Cache cache = getCache();
    // Create the data region
    RegionFactory<String, Integer> dataRegionFactory =
        cache.createRegionFactory(RegionShortcut.PARTITION);
    dataRegionFactory.create(PR1);
  }

  private void setupSystem() {
    final Properties managerProps = createProperties("Manager", "G1");
    setUpJmxManagerOnVm0ThenConnect(managerProps);

    final Properties server1Props = createProperties("Server1", "G2");
    final Host host = Host.getHost(0);
    final VM[] servers = {host.getVM(0), host.getVM(1)};

    // The mananger VM
    servers[0].invoke(new SerializableRunnable() {
      public void run() {
        final Cache cache = getCache();
        RegionFactory<String, Integer> dataRegionFactory =
            cache.createRegionFactory(RegionShortcut.PARTITION);
        dataRegionFactory.setConcurrencyLevel(4);
        EvictionAttributes ea =
            EvictionAttributes.createLIFOEntryAttributes(100, EvictionAction.LOCAL_DESTROY);
        dataRegionFactory.setEvictionAttributes(ea);
        dataRegionFactory.setEnableAsyncConflation(true);

        FixedPartitionAttributes fpa = FixedPartitionAttributes.createFixedPartition("Par1", true);
        PartitionAttributes pa = new PartitionAttributesFactory().setLocalMaxMemory(100)
            .setRecoveryDelay(2).setTotalMaxMemory(200).setRedundantCopies(1)
            .addFixedPartitionAttributes(fpa).create();
        dataRegionFactory.setPartitionAttributes(pa);

        dataRegionFactory.create(PR1);
        createLocalRegion(LOCALREGIONONMANAGER);
      }
    });

    servers[1].invoke(new SerializableRunnable() {
      public void run() {
        getSystem(server1Props);
        final Cache cache = getCache();
        RegionFactory<String, Integer> dataRegionFactory =
            cache.createRegionFactory(RegionShortcut.PARTITION);
        dataRegionFactory.setConcurrencyLevel(4);
        EvictionAttributes ea =
            EvictionAttributes.createLIFOEntryAttributes(100, EvictionAction.LOCAL_DESTROY);
        dataRegionFactory.setEvictionAttributes(ea);
        dataRegionFactory.setEnableAsyncConflation(true);

        FixedPartitionAttributes fpa = FixedPartitionAttributes.createFixedPartition("Par2", 4);
        PartitionAttributes pa = new PartitionAttributesFactory().setLocalMaxMemory(150)
            .setRecoveryDelay(4).setTotalMaxMemory(200).setRedundantCopies(1)
            .addFixedPartitionAttributes(fpa).create();
        dataRegionFactory.setPartitionAttributes(pa);

        dataRegionFactory.create(PR1);
        createRegionsWithSubRegions();
      }
    });
  }

  private void createPartitionedRegion(String regionName) {

    final Cache cache = getCache();
    // Create the data region
    RegionFactory<String, Integer> dataRegionFactory =
        cache.createRegionFactory(RegionShortcut.PARTITION);
    dataRegionFactory.setConcurrencyLevel(4);
    EvictionAttributes ea =
        EvictionAttributes.createLIFOEntryAttributes(100, EvictionAction.LOCAL_DESTROY);
    dataRegionFactory.setEvictionAttributes(ea);
    dataRegionFactory.setEnableAsyncConflation(true);

    FixedPartitionAttributes fpa = FixedPartitionAttributes.createFixedPartition("Par1", true);
    PartitionAttributes pa =
        new PartitionAttributesFactory().setLocalMaxMemory(100).setRecoveryDelay(2)
            .setTotalMaxMemory(200).setRedundantCopies(1).addFixedPartitionAttributes(fpa).create();
    dataRegionFactory.setPartitionAttributes(pa);
    dataRegionFactory.addCacheListener(new CacheListener1());
    dataRegionFactory.addCacheListener(new CacheListener2());
    dataRegionFactory.create(regionName);
  }


  private void createLocalRegion(final String regionName) {
    final Cache cache = getCache();
    // Create the data region
    RegionFactory<String, Integer> dataRegionFactory =
        cache.createRegionFactory(RegionShortcut.LOCAL);
    dataRegionFactory.create(regionName);
  }

  /**
   * Creates a region that uses compression on region entry values.
   *
   * @param regionName a unique region name.
   */
  private void createCompressedRegion(final String regionName) {
    final Cache cache = getCache();

    RegionFactory<String, Integer> dataRegionFactory =
        cache.createRegionFactory(RegionShortcut.REPLICATE);
    dataRegionFactory.setCompressor(SnappyCompressor.getDefaultInstance());
    dataRegionFactory.create(regionName);
  }

  @SuppressWarnings("deprecation")
  private void createRegionsWithSubRegions() {
    final Cache cache = getCache();

    RegionFactory<String, Integer> dataRegionFactory =
        cache.createRegionFactory(RegionShortcut.REPLICATE);
    dataRegionFactory.setConcurrencyLevel(3);
    Region<String, Integer> region1 = dataRegionFactory.create(REGION1);
    region1.createSubregion(SUBREGION1C, region1.getAttributes());
    Region<String, Integer> subregion2 =
        region1.createSubregion(SUBREGION1A, region1.getAttributes());

    subregion2.createSubregion(SUBREGION1B, subregion2.getAttributes());
    dataRegionFactory.create(REGION2);
    dataRegionFactory.create(REGION3);
  }

  @Test
  public void testListRegion() {
    setupSystem();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.LIST_REGION);
    String commandString = csb.toString();
    CommandResult commandResult = executeCommand(commandString);
    String commandResultAsString = commandResultToString(commandResult);
    getLogWriter().info("Command String : " + commandString);
    getLogWriter().info("Output : \n" + commandResultAsString);
    assertEquals(Status.OK, commandResult.getStatus());
    assertTrue(commandResultAsString.contains(PR1));
    assertTrue(commandResultAsString.contains(LOCALREGIONONMANAGER));
    assertTrue(commandResultAsString.contains(REGION1));
    assertTrue(commandResultAsString.contains(REGION2));
    assertTrue(commandResultAsString.contains(REGION3));


    csb = new CommandStringBuilder(CliStrings.LIST_REGION);
    csb.addOption(CliStrings.LIST_REGION__MEMBER, "Manager");
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    commandResultAsString = commandResultToString(commandResult);
    getLogWriter().info("Command String : " + commandString);
    getLogWriter().info("Output : \n" + commandResultAsString);
    assertEquals(Status.OK, commandResult.getStatus());
    assertTrue(commandResultAsString.contains(PR1));
    assertTrue(commandResultAsString.contains(LOCALREGIONONMANAGER));

    csb = new CommandStringBuilder(CliStrings.LIST_REGION);
    csb.addOption(CliStrings.LIST_REGION__MEMBER, "Server1");
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    commandResultAsString = commandResultToString(commandResult);
    getLogWriter().info("Command String : " + commandString);
    getLogWriter().info("Output : \n" + commandResultAsString);
    assertEquals(Status.OK, commandResult.getStatus());
    assertTrue(commandResultAsString.contains(PR1));
    assertTrue(commandResultAsString.contains(REGION1));
    assertTrue(commandResultAsString.contains(REGION2));
    assertTrue(commandResultAsString.contains(REGION3));
    assertTrue(commandResultAsString.contains(SUBREGION1A));

    csb = new CommandStringBuilder(CliStrings.LIST_REGION);
    csb.addOption(CliStrings.LIST_REGION__GROUP, "G1");
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    commandResultAsString = commandResultToString(commandResult);
    getLogWriter().info("Command String : " + commandString);
    getLogWriter().info("Output : \n" + commandResultAsString);
    assertEquals(Status.OK, commandResult.getStatus());
    assertTrue(commandResultAsString.contains(PR1));
    assertTrue(commandResultAsString.contains(LOCALREGIONONMANAGER));

    csb = new CommandStringBuilder(CliStrings.LIST_REGION);
    csb.addOption(CliStrings.LIST_REGION__GROUP, "G2");
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    commandResultAsString = commandResultToString(commandResult);
    getLogWriter().info("Command String : " + commandString);
    getLogWriter().info("Output : \n" + commandResultAsString);
    assertEquals(Status.OK, commandResult.getStatus());
    assertTrue(commandResultAsString.contains(PR1));
    assertTrue(commandResultAsString.contains(REGION1));
    assertTrue(commandResultAsString.contains(REGION2));
    assertTrue(commandResultAsString.contains(REGION3));
    assertTrue(commandResultAsString.contains(SUBREGION1A));
  }

  @Test
  public void testDescribeRegion() {
    setupSystem();
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESCRIBE_REGION);
    csb.addOption(CliStrings.DESCRIBE_REGION__NAME, PR1);
    String commandString = csb.toString();
    CommandResult commandResult = executeCommand(commandString);
    String commandResultAsString = commandResultToString(commandResult);
    getLogWriter().info("Command String : " + commandString);
    getLogWriter().info("Output : \n" + commandResultAsString);
    assertEquals(Status.OK, commandResult.getStatus());
    assertTrue(commandResultAsString.contains(PR1));
    assertTrue(commandResultAsString.contains("Server1"));

    csb = new CommandStringBuilder(CliStrings.DESCRIBE_REGION);
    csb.addOption(CliStrings.DESCRIBE_REGION__NAME, LOCALREGIONONMANAGER);
    commandString = csb.toString();
    commandResult = executeCommand(commandString);
    commandResultAsString = commandResultToString(commandResult);
    getLogWriter().info("Command String : " + commandString);
    getLogWriter().info("Output : \n" + commandResultAsString);
    assertEquals(Status.OK, commandResult.getStatus());
    assertTrue(commandResultAsString.contains(LOCALREGIONONMANAGER));
    assertTrue(commandResultAsString.contains("Manager"));
  }

  /**
   * Asserts that a describe region command issued on a region with compression returns the correct
   * non default region attribute for compression and the correct codec value.
   */
  @Category(FlakyTest.class) // GEODE-1033: HeadlesssGFSH, random port, Snappy dependency
  @Test
  public void testDescribeRegionWithCompressionCodec() {
    final String regionName = "compressedRegion";
    VM vm = Host.getHost(0).getVM(1);

    setupSystem();

    // Create compressed region
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        createCompressedRegion(regionName);
      }
    });

    // Test the describe command; look for compression
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DESCRIBE_REGION);
    csb.addOption(CliStrings.DESCRIBE_REGION__NAME, regionName);
    String commandString = csb.toString();
    CommandResult commandResult = executeCommand(commandString);
    String commandResultAsString = commandResultToString(commandResult);
    assertEquals(Status.OK, commandResult.getStatus());
    assertTrue(commandResultAsString.contains(regionName));
    assertTrue(commandResultAsString.contains(RegionAttributesNames.COMPRESSOR));
    assertTrue(commandResultAsString.contains(RegionEntryContext.DEFAULT_COMPRESSION_PROVIDER));

    // Destroy compressed region
    vm.invoke(new SerializableRunnable() {
      @Override
      public void run() {
        Region region = getCache().getRegion(regionName);
        assertNotNull(region);
        region.destroyRegion();
      }
    });
  }
}
