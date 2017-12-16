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

import static org.apache.geode.management.internal.cli.i18n.CliStrings.DESCRIBE_REGION;
import static org.apache.geode.management.internal.cli.i18n.CliStrings.DESCRIBE_REGION__NAME;
import static org.assertj.core.api.Assertions.assertThat;

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
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(DistributedTest.class)
public class DescribeRegionDUnitTest {
  private static final String REGION1 = "region1";
  private static final String REGION2 = "region2";
  private static final String REGION3 = "region3";
  private static final String SUBREGION1A = "subregion1A";
  private static final String SUBREGION1B = "subregion1B";
  private static final String SUBREGION1C = "subregion1C";
  private static final String PR1 = "PR1";
  private static final String LOCAL_REGION = "LocalRegion";

  private static final String PART1_NAME = "Par1";
  private static final String PART2_NAME = "Par2";

  @ClassRule
  public static LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void setupSystem() throws Exception {
    MemberVM locator = lsRule.startLocatorVM(0);
    MemberVM server1 = lsRule.startServerVM(1, "group1", locator.getPort());
    MemberVM server2 = lsRule.startServerVM(2, "group2", locator.getPort());

    server1.invoke(() -> {
      final Cache cache = LocatorServerStartupRule.getCache();
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
      createLocalRegion(LOCAL_REGION);
    });

    server2.invoke(() -> {
      final Cache cache = LocatorServerStartupRule.getCache();
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

    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create async-event-queue --id=queue1 --group=group1 "
        + "--listener=org.apache.geode.internal.cache.wan.MyAsyncEventListener").statusIsSuccess();

    locator.waitTillAsyncEventQueuesAreReadyOnServers("queue1", 1);
    gfsh.executeAndAssertThat(
        "create region --name=region4 --type=REPLICATE --async-event-queue-id=queue1")
        .statusIsSuccess();

  }

  @Test
  public void describeRegionOnBothServers() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_REGION);
    csb.addOption(DESCRIBE_REGION__NAME, PR1);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess().containsOutput(PR1, "server-1",
        "server-2");
  }

  @Test
  public void describeLocalRegionOnlyOneServer1() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_REGION);
    csb.addOption(DESCRIBE_REGION__NAME, LOCAL_REGION);
    gfsh.executeAndAssertThat(csb.toString()).statusIsSuccess()
        .containsOutput(LOCAL_REGION, "server-1").doesNotContainOutput("server-2");
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
    gfsh.executeAndAssertThat(commandString).statusIsSuccess().containsOutput(regionName,
        RegionAttributesNames.COMPRESSOR, RegionEntryContext.DEFAULT_COMPRESSION_PROVIDER);

    // Destroy compressed region
    vm.invoke(() -> {
      final Region region = CacheFactory.getAnyInstance().getRegion(regionName);
      assertThat(region).isNotNull();
      region.destroyRegion();
    });
  }

  @Test
  public void describeRegionWithAsyncEventQueue() throws Exception {
    gfsh.executeAndAssertThat("describe region --name=region4").statusIsSuccess()
        .containsOutput("async-event-queue-id", "queue1");
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
