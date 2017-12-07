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

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.DESCRIBE_REGION;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.DESCRIBE_REGION__NAME;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.GROUP;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.LIST_REGION;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.MEMBER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.compression.SnappyCompressor;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.cli.util.RegionAttributesNames;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class DescribeRegionDUnitTest implements Serializable {
  private static final String REGION1 = "region1";
  private static final String REGION2 = "region2";
  private static final String REGION3 = "region3";
  private static final String SUBREGION1A = "subregion1A";
  private static final String SUBREGION1B = "subregion1B";
  private static final String SUBREGION1C = "subregion1C";
  private static final String PR1 = "PR1";
  private static final String LOCALREGIONONMANAGER = "LocalRegionOnManager";

  private static final String LOCATOR_NAME = "Locator";
  private static final String SERVER1_NAME = "Server-1";
  private static final String SERVER2_NAME = "Server-2";
  private static final String GROUP1_NAME = "G1";
  private static final String GROUP2_NAME = "G2";
  private static final String GROUP3_NAME = "G3";
  private static final String PART1_NAME = "Par1";
  private static final String PART2_NAME = "Par2";

  @ClassRule
  public static LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @ClassRule
  public static GfshCommandRule gfshCommandRule = new GfshCommandRule();

  @BeforeClass
  public static void setupSystem() throws Exception {
    final Properties locatorProps = createProperties(LOCATOR_NAME, GROUP3_NAME);
    MemberVM locator = lsRule.startLocatorVM(0, locatorProps);

    final Properties managerProps = createProperties(SERVER1_NAME, GROUP1_NAME);
    managerProps.setProperty(LOCATORS, "localhost[" + locator.getPort() + "]");
    MemberVM manager = lsRule.startServerVM(1, managerProps, locator.getPort());

    final Properties serverProps = createProperties(SERVER2_NAME, GROUP2_NAME);
    MemberVM server = lsRule.startServerVM(2, serverProps, locator.getPort());

    manager.invoke(() -> {
      final Cache cache = CacheFactory.getAnyInstance();
      RegionFactory<String, Integer> dataRegionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      dataRegionFactory.setConcurrencyLevel(4);
      EvictionAttributes ea =
          EvictionAttributes.createLIFOEntryAttributes(100, EvictionAction.LOCAL_DESTROY);
      dataRegionFactory.setEvictionAttributes(ea);
      dataRegionFactory.setEnableAsyncConflation(true);

      FixedPartitionAttributes fpa =
          FixedPartitionAttributes.createFixedPartition(PART1_NAME, true);
      PartitionAttributes pa = new PartitionAttributesFactory().setLocalMaxMemory(100)
          .setRecoveryDelay(2).setTotalMaxMemory(200).setRedundantCopies(1)
          .addFixedPartitionAttributes(fpa).create();
      dataRegionFactory.setPartitionAttributes(pa);

      dataRegionFactory.create(PR1);
      createLocalRegion(LOCALREGIONONMANAGER);
    });

    server.invoke(() -> {
      final Cache cache = CacheFactory.getAnyInstance();
      RegionFactory<String, Integer> dataRegionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      dataRegionFactory.setConcurrencyLevel(4);
      EvictionAttributes ea =
          EvictionAttributes.createLIFOEntryAttributes(100, EvictionAction.LOCAL_DESTROY);
      dataRegionFactory.setEvictionAttributes(ea);
      dataRegionFactory.setEnableAsyncConflation(true);

      FixedPartitionAttributes fpa = FixedPartitionAttributes.createFixedPartition(PART2_NAME, 4);
      PartitionAttributes pa = new PartitionAttributesFactory().setLocalMaxMemory(150)
          .setRecoveryDelay(4).setTotalMaxMemory(200).setRedundantCopies(1)
          .addFixedPartitionAttributes(fpa).create();
      dataRegionFactory.setPartitionAttributes(pa);

      dataRegionFactory.create(PR1);
      createRegionsWithSubRegions();
    });

    gfshCommandRule.connectAndVerify(locator);
  }

  @Test
  public void describeRegionsOnServer2() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_REGION);
    csb.addOption(DESCRIBE_REGION__NAME, PR1);
    gfshCommandRule.executeAndAssertThat(csb.toString()).statusIsSuccess().containsOutput(PR1,
        "Server");
  }

  @Test
  public void describeRegionsOnServer1() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_REGION);
    csb.addOption(DESCRIBE_REGION__NAME, LOCALREGIONONMANAGER);
    gfshCommandRule.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput(LOCALREGIONONMANAGER, SERVER1_NAME);
  }

  /**
   * Asserts that a describe region command issued on a region with compression returns the correct
   * non default region attribute for compression and the correct codec value.
   */
  @Test
  public void describeRegionWithCompressionCodec() throws Exception {
    final String regionName = "compressedRegion";
    VM vm = Host.getHost(0).getVM(1);

    // Create compressed region
    vm.invoke(() -> createCompressedRegion(regionName));

    // Test the describe command; look for compression
    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_REGION);
    csb.addOption(DESCRIBE_REGION__NAME, regionName);
    String commandString = csb.toString();
    gfshCommandRule.executeAndAssertThat(commandString).statusIsSuccess().containsOutput(regionName,
        RegionAttributesNames.COMPRESSOR, RegionEntryContext.DEFAULT_COMPRESSION_PROVIDER);

    // Destroy compressed region
    vm.invoke(() -> {
      final Region region = CacheFactory.getAnyInstance().getRegion(regionName);
      assertThat(region).isNotNull();
      region.destroyRegion();
    });
  }

  private static Properties createProperties(String name, String groups) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOG_LEVEL, "info");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.setProperty(NAME, name);
    props.setProperty(GROUPS, groups);
    return props;
  }

  private static void createLocalRegion(final String regionName) {
    final Cache cache = CacheFactory.getAnyInstance();
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
  private static void createCompressedRegion(final String regionName) {
    final Cache cache = CacheFactory.getAnyInstance();

    RegionFactory<String, Integer> dataRegionFactory =
        cache.createRegionFactory(RegionShortcut.REPLICATE);
    dataRegionFactory.setCompressor(SnappyCompressor.getDefaultInstance());
    dataRegionFactory.create(regionName);
  }

  @SuppressWarnings("deprecation")
  private static void createRegionsWithSubRegions() {
    final Cache cache = CacheFactory.getAnyInstance();

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
}
