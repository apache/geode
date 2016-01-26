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
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.RegionEntryContext;
import com.gemstone.gemfire.management.cli.Result.Status;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.result.CommandResult;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;
import com.gemstone.gemfire.management.internal.cli.util.RegionAttributesNames;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

import java.util.Properties;

public class ListAndDescribeRegionDUnitTest extends CliCommandTestBase {

  public ListAndDescribeRegionDUnitTest(String name) {
    super(name);
    // TODO Auto-generated constructor stub
  }

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
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "info");
    props.setProperty(DistributionConfig.STATISTIC_SAMPLING_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.ENABLE_TIME_STATISTICS_NAME, "true");
    props.setProperty(DistributionConfig.NAME_NAME, name);
    props.setProperty(DistributionConfig.GROUPS_NAME, groups);
    return props;
  }

  private void createPartitionedRegion1() {
    final Cache cache = getCache();
    // Create the data region
    RegionFactory<String, Integer> dataRegionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
    dataRegionFactory.create(PR1);
  }

  private void setupSystem() {
    final Properties managerProps = createProperties("Manager", "G1");
    createDefaultSetup(managerProps);

    final Properties server1Props = createProperties("Server1", "G2");
    final Host host = Host.getHost(0);
    final VM[] servers = {host.getVM(0), host.getVM(1)};

    //The mananger VM
    servers[0].invoke(new SerializableRunnable() {
      public void run() {
        final Cache cache = getCache();
        RegionFactory<String, Integer> dataRegionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
        dataRegionFactory.setConcurrencyLevel(4);
        EvictionAttributes ea = EvictionAttributes.createLIFOEntryAttributes(100, EvictionAction.LOCAL_DESTROY);
        dataRegionFactory.setEvictionAttributes(ea);
        dataRegionFactory.setEnableAsyncConflation(true);

        FixedPartitionAttributes fpa = FixedPartitionAttributes.createFixedPartition("Par1", true);
        PartitionAttributes pa = new PartitionAttributesFactory().setLocalMaxMemory(100).setRecoveryDelay(
            2).setTotalMaxMemory(200).setRedundantCopies(1).addFixedPartitionAttributes(fpa).create();
        dataRegionFactory.setPartitionAttributes(pa);

        dataRegionFactory.create(PR1);
        createLocalRegion(LOCALREGIONONMANAGER);
      }
    });

    servers[1].invoke(new SerializableRunnable() {
      public void run() {
        getSystem(server1Props);
        final Cache cache = getCache();
        RegionFactory<String, Integer> dataRegionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
        dataRegionFactory.setConcurrencyLevel(4);
        EvictionAttributes ea = EvictionAttributes.createLIFOEntryAttributes(100, EvictionAction.LOCAL_DESTROY);
        dataRegionFactory.setEvictionAttributes(ea);
        dataRegionFactory.setEnableAsyncConflation(true);

        FixedPartitionAttributes fpa = FixedPartitionAttributes.createFixedPartition("Par2", 4);
        PartitionAttributes pa = new PartitionAttributesFactory().setLocalMaxMemory(150).setRecoveryDelay(
            4).setTotalMaxMemory(200).setRedundantCopies(1).addFixedPartitionAttributes(fpa).create();
        dataRegionFactory.setPartitionAttributes(pa);

        dataRegionFactory.create(PR1);
        createRegionsWithSubRegions();
      }
    });
  }

  private void createPartitionedRegion(String regionName) {

    final Cache cache = getCache();
    // Create the data region
    RegionFactory<String, Integer> dataRegionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);
    dataRegionFactory.setConcurrencyLevel(4);
    EvictionAttributes ea = EvictionAttributes.createLIFOEntryAttributes(100, EvictionAction.LOCAL_DESTROY);
    dataRegionFactory.setEvictionAttributes(ea);
    dataRegionFactory.setEnableAsyncConflation(true);

    FixedPartitionAttributes fpa = FixedPartitionAttributes.createFixedPartition("Par1", true);
    PartitionAttributes pa = new PartitionAttributesFactory().setLocalMaxMemory(100).setRecoveryDelay(
        2).setTotalMaxMemory(200).setRedundantCopies(1).addFixedPartitionAttributes(fpa).create();
    dataRegionFactory.setPartitionAttributes(pa);
    dataRegionFactory.addCacheListener(new CacheListener1());
    dataRegionFactory.addCacheListener(new CacheListener2());
    dataRegionFactory.create(regionName);
  }


  private void createLocalRegion(final String regionName) {
    final Cache cache = getCache();
    // Create the data region
    RegionFactory<String, Integer> dataRegionFactory = cache.createRegionFactory(RegionShortcut.LOCAL);
    dataRegionFactory.create(regionName);
  }

  /**
   * Creates a region that uses compression on region entry values.
   *
   * @param regionName a unique region name.
   */
  private void createCompressedRegion(final String regionName) {
    final Cache cache = getCache();

    RegionFactory<String, Integer> dataRegionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE);
    dataRegionFactory.setCompressor(SnappyCompressor.getDefaultInstance());
    dataRegionFactory.create(regionName);
  }

  @SuppressWarnings("deprecation")
  private void createRegionsWithSubRegions() {
    final Cache cache = getCache();

    RegionFactory<String, Integer> dataRegionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE);
    dataRegionFactory.setConcurrencyLevel(3);
    Region<String, Integer> region1 = dataRegionFactory.create(REGION1);
    region1.createSubregion(SUBREGION1C, region1.getAttributes());
    Region<String, Integer> subregion2 = region1.createSubregion(SUBREGION1A, region1.getAttributes());

    subregion2.createSubregion(SUBREGION1B, subregion2.getAttributes());
    dataRegionFactory.create(REGION2);
    dataRegionFactory.create(REGION3);
  }


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
   * Asserts that a describe region command issued on a region with compression returns the correct non default region
   * attribute for compression and the correct codec value.
   */
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
