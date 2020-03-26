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

import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.NO_REDUNDANT_COPIES_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.REDUNDANCY_NOT_SATISFIED_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.REDUNDANCY_SATISFIED_FOR_REGIONS;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_FOR_REGION_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_FOUND_FOR_ALL_REGIONS;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_FOUND_FOR_REGIONS;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.SATISFIED_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.UNDER_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.ZERO_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.StatusRedundancyCommand.COMMAND_NAME;
import static org.apache.geode.management.internal.cli.commands.StatusRedundancyCommand.EXCLUDE_REGION;
import static org.apache.geode.management.internal.cli.commands.StatusRedundancyCommand.INCLUDE_REGION;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionAttributesImpl;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.assertions.InfoResultModelAssert;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class StatusRedundancyCommandDUnitTest {
  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private int locatorPort;
  private MemberVM locator;
  private List<MemberVM> servers;
  private static int serversToStart = 2;
  private static String satisfiedRedundancyRegionName = "satisfiedRedundancy";
  private static int satisfiableRedundantCopies = serversToStart - 1;
  private static String unsatisfiedRedundancyRegionName = "unsatisfiedRedundancy";
  private static int unsatisfiableRedundancyCopies = serversToStart;
  private static String noConfiguredRedundancyRegionName = "noConfiguredRedundancyRedundancy";
  private static String zeroRedundantCopiesRegionName = "zeroRedundantCopies";
  private List<String> regionNames;

  @Before
  public void setUp() throws Exception {
    servers = new ArrayList<>();
    locator = cluster.startLocatorVM(0);
    locatorPort = locator.getPort();
    IntStream.range(0, serversToStart)
        .forEach(i -> servers.add(cluster.startServerVM(i + 1, locatorPort)));
    gfsh.connectAndVerify(locator);
    regionNames = new ArrayList<>();
    regionNames.add(satisfiedRedundancyRegionName);
    regionNames.add(unsatisfiedRedundancyRegionName);
    regionNames.add(noConfiguredRedundancyRegionName);
    regionNames.add(zeroRedundantCopiesRegionName);
  }

  @Test
  public void statusRedundancyWithNoArgumentsReturnsStatusForAllRegions() {
    createAndPopulateRegions();

    waitForRegionsToBeReadyInManagementService(regionNames);

    String command = new CommandStringBuilder(COMMAND_NAME).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command);
    commandResult.statusIsSuccess();
    InfoResultModelAssert zeroRedundancy = commandResult.hasInfoSection(ZERO_REDUNDANCY_SECTION);
    zeroRedundancy.hasHeader().isEqualTo(NO_REDUNDANT_COPIES_FOR_REGIONS);
    zeroRedundancy.hasOutput().contains(zeroRedundantCopiesRegionName);

    InfoResultModelAssert underRedundancy = commandResult.hasInfoSection(UNDER_REDUNDANCY_SECTION);
    underRedundancy.hasHeader().isEqualTo(REDUNDANCY_NOT_SATISFIED_FOR_REGIONS);
    underRedundancy.hasOutput().contains(unsatisfiedRedundancyRegionName);

    InfoResultModelAssert satisfiedRedundancy =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedRedundancy.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedRedundancy.hasOutput().contains(satisfiedRedundancyRegionName,
        noConfiguredRedundancyRegionName);
  }

  @Test
  public void statusRedundancyWithIncludeRegionArgumentReturnsStatusForOnlyThatRegion() {
    createAndPopulateRegions();

    waitForRegionsToBeReadyInManagementService(regionNames);

    String command = new CommandStringBuilder(COMMAND_NAME)
        .addOption(INCLUDE_REGION, satisfiedRedundancyRegionName)
        .getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command);
    commandResult.statusIsSuccess()
        .hasNoSection(ZERO_REDUNDANCY_SECTION)
        .hasNoSection(UNDER_REDUNDANCY_SECTION)
        .doesNotContainOutput(unsatisfiedRedundancyRegionName)
        .doesNotContainOutput(zeroRedundantCopiesRegionName)
        .doesNotContainOutput(noConfiguredRedundancyRegionName);

    InfoResultModelAssert satisfiedRedundancy =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedRedundancy.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedRedundancy.hasOutput().contains(satisfiedRedundancyRegionName);
  }

  @Test
  public void statusRedundancyWithExcludeRegionArgumentReturnsStatusForAllExceptThatRegion() {
    createAndPopulateRegions();

    waitForRegionsToBeReadyInManagementService(regionNames);

    String command = new CommandStringBuilder(COMMAND_NAME)
        .addOption(EXCLUDE_REGION, zeroRedundantCopiesRegionName)
        .getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command);
    commandResult.statusIsSuccess()
        .hasNoSection(ZERO_REDUNDANCY_SECTION)
        .doesNotContainOutput(zeroRedundantCopiesRegionName);

    InfoResultModelAssert underRedundancy = commandResult.hasInfoSection(UNDER_REDUNDANCY_SECTION);
    underRedundancy.hasHeader().isEqualTo(REDUNDANCY_NOT_SATISFIED_FOR_REGIONS);
    underRedundancy.hasOutput().contains(unsatisfiedRedundancyRegionName);

    InfoResultModelAssert satisfiedRedundancy =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedRedundancy.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedRedundancy.hasOutput().contains(satisfiedRedundancyRegionName,
        noConfiguredRedundancyRegionName);
  }

  @Test
  public void statusRedundancyWithMatchingIncludeAndExcludeRegionArgumentsReturnsStatusForIncludedRegion() {
    createAndPopulateRegions();

    waitForRegionsToBeReadyInManagementService(regionNames);

    String command = new CommandStringBuilder(COMMAND_NAME)
        .addOption(INCLUDE_REGION, satisfiedRedundancyRegionName)
        .addOption(EXCLUDE_REGION, satisfiedRedundancyRegionName)
        .getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command);
    commandResult.statusIsSuccess()
        .hasNoSection(ZERO_REDUNDANCY_SECTION)
        .hasNoSection(UNDER_REDUNDANCY_SECTION)
        .doesNotContainOutput(unsatisfiedRedundancyRegionName)
        .doesNotContainOutput(zeroRedundantCopiesRegionName)
        .doesNotContainOutput(noConfiguredRedundancyRegionName);

    InfoResultModelAssert satisfiedRedundancy =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedRedundancy.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedRedundancy.hasOutput().contains(satisfiedRedundancyRegionName);
  }

  @Test
  public void statusRedundancyWithNoArgumentsReturnsSuccessWhenNoRegionsArePresent() {
    String command = new CommandStringBuilder(COMMAND_NAME).getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess().hasInfoSection(NO_MEMBERS_SECTION)
        .hasHeader()
        .isEqualTo(NO_MEMBERS_FOUND_FOR_ALL_REGIONS);
  }

  @Test
  public void statusRedundancyWithIncludeRegionReturnsErrorWhenAtLeastOneIncludedRegionIsNotPresent() {
    createAndPopulateRegions();

    waitForRegionsToBeReadyInManagementService(regionNames);

    String nonexistentRegion = "fakeRegion";
    String command = new CommandStringBuilder(COMMAND_NAME)
        .addOption(INCLUDE_REGION, nonexistentRegion + "," + satisfiedRedundancyRegionName)
        .getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsError()
        .hasNoSection(ZERO_REDUNDANCY_SECTION)
        .hasNoSection(UNDER_REDUNDANCY_SECTION);

    InfoResultModelAssert noMembersForRegion =
        commandResult.hasInfoSection(NO_MEMBERS_FOR_REGION_SECTION);
    noMembersForRegion.hasHeader().isEqualTo(NO_MEMBERS_FOUND_FOR_REGIONS);
    noMembersForRegion.hasLines().containsExactly(nonexistentRegion);

    InfoResultModelAssert satisfiedRedundancy =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedRedundancy.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedRedundancy.hasOutput().contains(satisfiedRedundancyRegionName);
  }

  private void createAndPopulateRegions() {
    // Create and populate a region on only server1 so that it will not be able to create any
    // redundant copies
    servers.get(0).invoke(() -> {
      PartitionAttributesImpl attributes = new PartitionAttributesImpl();
      attributes.setStartupRecoveryDelay(-1);
      attributes.setRecoveryDelay(-1);
      attributes.setRedundantCopies(satisfiableRedundantCopies);

      InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());

      Region<Object, Object> region =
          cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
              .create(zeroRedundantCopiesRegionName);

      IntStream.range(0, 1000).forEach(i -> region.put("key" + i, "value" + i));
    });

    String serverName = servers.get(0).getName();

    servers.forEach(s -> s.invoke(() -> {
      PartitionAttributesImpl attributes = new PartitionAttributesImpl();
      attributes.setStartupRecoveryDelay(-1);
      attributes.setRecoveryDelay(-1);

      InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());

      // Create a region whose redundancy cannot be satisfied due to not enough members
      attributes.setRedundantCopies(unsatisfiableRedundancyCopies);
      cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
          .create(unsatisfiedRedundancyRegionName);

      // Create a region whose redundancy can be satisfied
      attributes.setRedundantCopies(satisfiableRedundantCopies);
      cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
          .create(satisfiedRedundancyRegionName);

      // Create a region configured to have zero redundant copies
      attributes.setRedundantCopies(0);
      cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
          .create(noConfiguredRedundancyRegionName);

      // Create the zero redundancy region on the other servers
      if (!s.getName().equals(serverName)) {
        attributes.setRedundantCopies(satisfiableRedundantCopies);
        cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
            .create(zeroRedundantCopiesRegionName);
      }
    }));

    servers.get(0).invoke(() -> {
      InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());

      // Populate all other regions
      Region<Object, Object> unsatisfiedRegion = cache.getRegion(unsatisfiedRedundancyRegionName);
      IntStream.range(0, 1000).forEach(i -> unsatisfiedRegion.put("key" + i, "value" + i));
      Region<Object, Object> satisfiedRegion = cache.getRegion(satisfiedRedundancyRegionName);
      IntStream.range(0, 1000).forEach(i -> satisfiedRegion.put("key" + i, "value" + i));
      Region<Object, Object> noConfiguredRedundancyRegion =
          cache.getRegion(noConfiguredRedundancyRegionName);
      IntStream.range(0, 1000)
          .forEach(i -> noConfiguredRedundancyRegion.put("key" + i, "value" + i));
    });

  }

  private void waitForRegionsToBeReadyInManagementService(List<String> regionNames) {
    // There is a race condition that the command can be executed before the management service has
    // completely finished registering the regions, so await here
    locator.invoke(() -> {
      await().until(() -> ManagementService.getManagementService(ClusterStartupRule.getCache())
          .getDistributedSystemMXBean().listRegions().length == regionNames.size());
      for (String regionName : regionNames) {
        await().until(() -> ManagementService.getManagementService(ClusterStartupRule.getCache())
            .getDistributedRegionMXBean("/" + regionName) != null);
        await().until(() -> ManagementService.getManagementService(ClusterStartupRule.getCache())
            .getDistributedRegionMXBean("/" + regionName).getMembers().length == serversToStart);
      }
    });
  }
}
