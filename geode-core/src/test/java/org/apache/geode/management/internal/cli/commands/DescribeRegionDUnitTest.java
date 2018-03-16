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

import java.util.List;

import org.json.JSONObject;
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
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.management.internal.cli.util.RegionAttributesNames;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;

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
  private static final String COMPRESSED_REGION_NAME = "compressedRegion";

  private static final String HOSTING_AND_ACCESSOR_REGION_NAME = "hostingAndAccessorRegion";

  private static final String PART1_NAME = "Par1";
  private static final String PART2_NAME = "Par2";

  private static MemberVM locator;
  private static MemberVM server1;
  private static MemberVM server2;
  private static MemberVM server3;
  private static MemberVM server4;
  private static MemberVM accessor;

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule(6);

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void setupSystem() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server1 = lsRule.startServerVM(1, "group1", locator.getPort());
    server2 = lsRule.startServerVM(2, "group2", locator.getPort());
    server3 = lsRule.startServerVM(3, locator.getPort());
    server4 = lsRule.startServerVM(4, locator.getPort());
    accessor = lsRule.startServerVM(5, locator.getPort());

    server1.invoke(() -> {
      final Cache cache = ClusterStartupRule.getCache();
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

      dataRegionFactory.setCustomEntryIdleTimeout(new TestCustomIdleExpiry());

      dataRegionFactory.create(PR1);
      createLocalRegion(LOCAL_REGION);
    });
    // Create compressed region
    server1.invoke(() -> createCompressedRegion(COMPRESSED_REGION_NAME));

    server2.invoke(() -> {
      final Cache cache = ClusterStartupRule.getCache();
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

      dataRegionFactory.setCustomEntryIdleTimeout(new TestCustomIdleExpiry());
      dataRegionFactory.setCustomEntryTimeToLive(new TestCustomTTLExpiry());

      dataRegionFactory.create(PR1);
      createRegionsWithSubRegions();
    });

    // Create the PR region on 4 members and an accessor region on the 5th.
    createHostingAndAccessorRegion();


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

  @Test
  public void describeRegionWithCustomExpiry() {
    CommandResult result = gfsh.executeAndAssertThat("describe region --name=" + PR1)
        .statusIsSuccess().getCommandResult();

    JSONObject table = result.getTableContent(0, 0, 0).getInternalJsonObject();
    List<String> names = CommandResult.toList(table.getJSONArray("Name"));
    assertThat(names).containsOnlyOnce(RegionAttributesNames.ENTRY_IDLE_TIME_CUSTOM_EXPIRY);

    List<String> values = CommandResult.toList(table.getJSONArray("Value"));
    assertThat(values).containsOnlyOnce(TestCustomIdleExpiry.class.getName());

    table = result.getTableContent(0, 1, 0).getInternalJsonObject();
    names = CommandResult.toList(table.getJSONArray("Name"));
    assertThat(names).containsOnlyOnce(RegionAttributesNames.ENTRY_TIME_TO_LIVE_CUSTOM_EXPIRY);

    values = CommandResult.toList(table.getJSONArray("Value"));
    assertThat(values).containsOnlyOnce(TestCustomTTLExpiry.class.getName());
  }

  /**
   * Asserts that a describe region command issued on a region with compression returns the correct
   * non default region attribute for compression and the correct codec value.
   */
  @Test
  public void describeRegionWithCompressionCodec() throws Exception {
    // Test the describe command; look for compression
    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_REGION);
    csb.addOption(DESCRIBE_REGION__NAME, COMPRESSED_REGION_NAME);
    String commandString = csb.toString();
    gfsh.executeAndAssertThat(commandString).statusIsSuccess().containsOutput(
        COMPRESSED_REGION_NAME, RegionAttributesNames.COMPRESSOR,
        RegionEntryContext.DEFAULT_COMPRESSION_PROVIDER);
  }

  @Test
  public void describeRegionWithAsyncEventQueue() throws Exception {
    gfsh.executeAndAssertThat("describe region --name=region4").statusIsSuccess()
        .containsOutput("async-event-queue-id", "queue1");
  }

  @Test
  public void testDescribeRegionReturnsDescriptionFromAllMembers() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(DESCRIBE_REGION);
    csb.addOption(DESCRIBE_REGION__NAME, HOSTING_AND_ACCESSOR_REGION_NAME);

    String command = csb.toString();

    CommandResult commandResult =
        gfsh.executeAndAssertThat(command).statusIsSuccess().getCommandResult();

    GfJsonObject hostingMembersRegionDesc = getMembersRegionDesc(commandResult, "Hosting Members");
    GfJsonObject hostingMembersTableContent = hostingMembersRegionDesc
        .getJSONObject("__sections__-0").getJSONObject("__tables__-0").getJSONObject("content");

    GfJsonObject accessorsRegionDesc = getMembersRegionDesc(commandResult, "Accessor Members");
    GfJsonObject accessorsTableContent = accessorsRegionDesc.getJSONObject("__sections__-0")
        .getJSONObject("__tables__-0").getJSONObject("content");

    assertThat(hostingMembersRegionDesc.get("Name").toString())
        .contains(HOSTING_AND_ACCESSOR_REGION_NAME);
    assertThat(hostingMembersRegionDesc.get("Data Policy").toString()).contains("partition");
    assertThat(hostingMembersRegionDesc.get("Hosting Members").toString()).contains("server-1",
        "server-2", "server-3", "server-4");
    assertThat(hostingMembersTableContent.get("Type").toString()).contains("Region");
    assertThat(hostingMembersTableContent.get("Name").toString()).contains("data-policy", "size");
    assertThat(hostingMembersTableContent.get("Value").toString()).contains("PARTITION", "0");

    assertThat(accessorsRegionDesc.get("Name").toString())
        .contains(HOSTING_AND_ACCESSOR_REGION_NAME);
    assertThat(accessorsRegionDesc.get("Data Policy").toString()).contains("partition");
    assertThat(accessorsRegionDesc.get("Accessor Members").toString()).contains("server-5");
    assertThat(accessorsTableContent.get("Type").toString()).contains("Region", "Partition");
    assertThat(accessorsTableContent.get("Name").toString()).contains("data-policy", "size",
        "local-max-memory");
    assertThat(accessorsTableContent.get("Value").toString()).contains("PARTITION", "0", "0");
  }

  private static void createHostingAndAccessorRegion() {
    VMProvider.invokeInEveryMember(() -> {
      Cache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION).create(HOSTING_AND_ACCESSOR_REGION_NAME);
    }, server1, server2, server3, server4);

    accessor.invoke(() -> {
      Cache cache = ClusterStartupRule.getCache();
      cache.createRegionFactory(RegionShortcut.PARTITION_PROXY)
          .create(HOSTING_AND_ACCESSOR_REGION_NAME);
    });
  }

  private GfJsonObject getMembersRegionDesc(CommandResult commandResult, String memberType) {
    if (commandResult.getContent().getJSONObject("__sections__-0").has(memberType))
      return commandResult.getContent().getJSONObject("__sections__-0");
    else
      return commandResult.getContent().getJSONObject("__sections__-1");
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
