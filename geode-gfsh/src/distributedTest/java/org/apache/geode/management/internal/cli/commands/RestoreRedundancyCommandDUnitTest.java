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

import static org.apache.geode.cache.PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.NO_REDUNDANT_COPIES_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.PRIMARY_TRANSFERS_COMPLETED;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.PRIMARY_TRANSFER_TIME;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.REDUNDANCY_NOT_SATISFIED_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.REDUNDANCY_SATISFIED_FOR_REGIONS;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_FOR_REGION_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_FOUND_FOR_ALL_REGIONS;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_FOUND_FOR_REGIONS;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.PRIMARIES_INFO_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.SATISFIED_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.UNDER_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.ZERO_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RestoreRedundancyCommand.COMMAND_NAME;
import static org.apache.geode.management.internal.cli.commands.RestoreRedundancyCommand.DONT_REASSIGN_PRIMARIES;
import static org.apache.geode.management.internal.cli.commands.RestoreRedundancyCommand.EXCLUDE_REGION;
import static org.apache.geode.management.internal.cli.commands.RestoreRedundancyCommand.INCLUDE_REGION;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionAttributesImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.assertions.InfoResultModelAssert;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class RestoreRedundancyCommandDUnitTest {
  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private int locatorPort;
  private MemberVM locator;
  private List<MemberVM> servers;
  private static int serversToStart = 3;
  private static String highRedundancyRegionName = "highRedundancy";
  private static int highRedundancyCopies = serversToStart - 1;
  private static String lowRedundancyRegionName = "lowRedundancy";
  private static String colocatedParentRegionName = "colocatedParent";
  private static String colocatedChildRegionName = "colocatedChild";
  private static int singleRedundantCopy = 1;
  private static String zeroRedundancyRegionName = "zeroRedundancy";

  @Before
  public void setUp() throws Exception {
    servers = new ArrayList<>();
    locator = cluster.startLocatorVM(0);
    locatorPort = locator.getPort();
    IntStream.range(0, serversToStart)
        .forEach(i -> servers.add(cluster.startServerVM(i + 1, locatorPort)));
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void restoreRedundancyWithNoArgumentsRestoresRedundancyForAllRegions() {
    List<String> regionNames = getAllRegionNames();
    createAndPopulateRegions(regionNames);

    waitForRegionsToBeReadyInManagementService(regionNames, servers.size());

    String command = new CommandStringBuilder(COMMAND_NAME).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess()
        .hasNoSection(ZERO_REDUNDANCY_SECTION)
        .hasNoSection(UNDER_REDUNDANCY_SECTION);

    InfoResultModelAssert satisfiedSection =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedSection.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedSection.hasOutput().contains(regionNames);

    InfoResultModelAssert primariesSection = commandResult.hasInfoSection(PRIMARIES_INFO_SECTION);
    primariesSection.hasOutput().contains(PRIMARY_TRANSFERS_COMPLETED, PRIMARY_TRANSFER_TIME);

    // Confirm all regions have their configured redundancy and that primaries were balanced
    int numberOfActiveServers = servers.size();
    servers.get(0).invoke(() -> {
      for (String regionName : regionNames) {
        assertRedundancyStatusForRegion(regionName, true);
        assertPrimariesBalanced(regionName, numberOfActiveServers, true);
      }
    });
  }

  @Test
  public void restoreRedundancyWithIncludeRegionArgumentRestoresOnlyThoseRegions() {
    List<String> regionNames = getAllRegionNames();
    createAndPopulateRegions(regionNames);

    waitForRegionsToBeReadyInManagementService(regionNames, servers.size());

    String regionToInclude = highRedundancyRegionName;
    List<String> nonIncludedRegions = new ArrayList<>(regionNames);
    nonIncludedRegions.remove(regionToInclude);

    String command = new CommandStringBuilder(COMMAND_NAME)
        .addOption(INCLUDE_REGION, regionToInclude).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess()
        .hasNoSection(ZERO_REDUNDANCY_SECTION)
        .hasNoSection(UNDER_REDUNDANCY_SECTION)
        .doesNotContainOutput(nonIncludedRegions.toArray(new String[0]));

    InfoResultModelAssert satisfiedSection =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedSection.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedSection.hasOutput().contains(regionToInclude);

    InfoResultModelAssert primariesSection = commandResult.hasInfoSection(PRIMARIES_INFO_SECTION);
    primariesSection.hasOutput().contains(PRIMARY_TRANSFERS_COMPLETED, PRIMARY_TRANSFER_TIME);

    int numberOfActiveServers = servers.size();
    servers.get(0).invoke(() -> {
      // Confirm that redundancy is restored for only the specified region
      for (String regionName : regionNames) {
        boolean shouldBeSatisfied = regionName.equals(regionToInclude);
        assertRedundancyStatusForRegion(regionName, shouldBeSatisfied);
        assertPrimariesBalanced(regionName, numberOfActiveServers, shouldBeSatisfied);
      }
    });
  }

  @Test
  public void restoreRedundancyWithExcludeRegionArgumentRestoresAllExceptThoseRegions() {
    List<String> regionNames = getAllRegionNames();
    createAndPopulateRegions(regionNames);

    waitForRegionsToBeReadyInManagementService(regionNames, servers.size());

    String regionToExclude = highRedundancyRegionName;
    List<String> regionsToInclude = new ArrayList<>(regionNames);
    regionsToInclude.remove(regionToExclude);

    String command = new CommandStringBuilder(COMMAND_NAME)
        .addOption(EXCLUDE_REGION, regionToExclude).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess()
        .hasNoSection(ZERO_REDUNDANCY_SECTION)
        .hasNoSection(UNDER_REDUNDANCY_SECTION)
        .doesNotContainOutput(regionToExclude);

    InfoResultModelAssert satisfiedSection =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedSection.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedSection.hasOutput().contains(regionsToInclude);

    int numberOfActiveServers = servers.size();
    servers.get(0).invoke(() -> {
      // Confirm that redundancy is restored for all but the specified region
      for (String regionName : regionNames) {
        boolean shouldBeSatisfied = !regionName.equals(regionToExclude);
        assertRedundancyStatusForRegion(regionName, shouldBeSatisfied);
        assertPrimariesBalanced(regionName, numberOfActiveServers, shouldBeSatisfied);
      }
    });
  }

  @Test
  public void restoreRedundancyWithMatchingIncludeAndExcludeRegionArgumentsRestoresIncludedRegions() {
    List<String> regionNames = getAllRegionNames();
    createAndPopulateRegions(regionNames);

    waitForRegionsToBeReadyInManagementService(regionNames, servers.size());

    String regionToInclude = highRedundancyRegionName;
    String regionToExclude = highRedundancyRegionName;
    List<String> nonIncludedRegions = new ArrayList<>(regionNames);
    nonIncludedRegions.remove(regionToInclude);

    String command =
        new CommandStringBuilder(COMMAND_NAME).addOption(INCLUDE_REGION, regionToInclude)
            .addOption(EXCLUDE_REGION, regionToExclude).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess()
        .hasNoSection(ZERO_REDUNDANCY_SECTION)
        .hasNoSection(UNDER_REDUNDANCY_SECTION)
        .doesNotContainOutput(nonIncludedRegions.toArray(new String[0]));

    InfoResultModelAssert satisfiedSection =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedSection.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedSection.hasOutput().contains(regionToInclude);

    int numberOfActiveServers = servers.size();
    servers.get(0).invoke(() -> {
      // Confirm that redundancy is restored for only the specified region
      for (String regionName : regionNames) {
        boolean shouldBeSatisfied = regionName.equals(regionToInclude);
        assertRedundancyStatusForRegion(regionName, shouldBeSatisfied);
        assertPrimariesBalanced(regionName, numberOfActiveServers, shouldBeSatisfied);
      }
    });
  }

  @Test
  public void restoreRedundancyWithIncludeRegionArgumentThatIsAParentColocatedRegionRegionRestoresRegionsColocatedWithIt() {
    List<String> regionNames = getAllRegionNames();
    createAndPopulateRegions(regionNames);

    waitForRegionsToBeReadyInManagementService(regionNames, servers.size());

    String regionToInclude = colocatedParentRegionName;
    String colocatedRegion = colocatedChildRegionName;
    List<String> nonIncludedRegions = new ArrayList<>(regionNames);
    nonIncludedRegions.remove(regionToInclude);
    nonIncludedRegions.remove(colocatedRegion);

    String command = new CommandStringBuilder(COMMAND_NAME)
        .addOption(INCLUDE_REGION, regionToInclude).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess()
        .hasNoSection(ZERO_REDUNDANCY_SECTION)
        .hasNoSection(UNDER_REDUNDANCY_SECTION)
        .doesNotContainOutput(nonIncludedRegions.toArray(new String[0]));

    InfoResultModelAssert satisfiedSection =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedSection.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedSection.hasOutput().contains(regionToInclude, colocatedRegion);

    int numberOfActiveServers = servers.size();
    servers.get(0).invoke(() -> {
      // Confirm that redundancy is restored for both colocated regions
      for (String regionName : regionNames) {
        boolean shouldBeSatisfied =
            regionName.equals(regionToInclude) || regionName.equals(colocatedRegion);
        assertRedundancyStatusForRegion(regionName, shouldBeSatisfied);
        assertPrimariesBalanced(regionName, numberOfActiveServers, shouldBeSatisfied);
      }
    });
  }

  @Test
  public void restoreRedundancyReturnsSuccessWhenAtLeastOneRedundantCopyExistsForEveryRegion() {
    List<String> regionNames = getAllRegionNames();
    List<String> satisfiedRegions = new ArrayList<>(regionNames);
    satisfiedRegions.remove(highRedundancyRegionName);
    createAndPopulateRegions(regionNames);

    // Stop the last server. The high redundancy region now cannot satisfy redundancy
    servers.remove(servers.size() - 1).stop();

    waitForRegionsToBeReadyInManagementService(regionNames, servers.size());

    String command = new CommandStringBuilder(COMMAND_NAME).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command);
    commandResult.statusIsSuccess().hasNoSection(ZERO_REDUNDANCY_SECTION);

    InfoResultModelAssert underRedundancy = commandResult.hasInfoSection(UNDER_REDUNDANCY_SECTION);
    underRedundancy.hasHeader().isEqualTo(REDUNDANCY_NOT_SATISFIED_FOR_REGIONS);
    underRedundancy.hasOutput().contains(highRedundancyRegionName);

    InfoResultModelAssert satisfiedRedundancy =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedRedundancy.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedRedundancy.hasOutput().contains(satisfiedRegions);

    int numberOfActiveServers = servers.size();
    servers.get(0).invoke(() -> {
      // Confirm that redundancy is restored for regions with lower redundancy
      for (String regionName : regionNames) {
        boolean shouldBeSatisfied = !regionName.equals(highRedundancyRegionName);
        assertRedundancyStatusForRegion(regionName, shouldBeSatisfied);
        assertPrimariesBalanced(regionName, numberOfActiveServers, true);
      }
    });
  }

  @Test
  public void restoreRedundancyWithNoArgumentsReturnsSuccessWhenNoRegionsArePresent() {
    String command = new CommandStringBuilder(COMMAND_NAME).getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess().hasInfoSection(NO_MEMBERS_SECTION)
        .hasHeader()
        .isEqualTo(NO_MEMBERS_FOUND_FOR_ALL_REGIONS);
  }

  @Test
  public void restoreRedundancyWithIncludeRegionReturnsErrorWhenAtLeastOneIncludedRegionIsNotPresent() {
    List<String> regionNames = new ArrayList<>();
    regionNames.add(lowRedundancyRegionName);
    createAndPopulateRegions(regionNames);

    waitForRegionsToBeReadyInManagementService(regionNames, servers.size());

    String nonexistentRegion = "fakeRegion";
    String command = new CommandStringBuilder(COMMAND_NAME)
        .addOption(INCLUDE_REGION, nonexistentRegion + "," + lowRedundancyRegionName)
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
    satisfiedRedundancy.hasOutput().contains(lowRedundancyRegionName);
  }

  @Test
  public void restoreRedundancyReturnsErrorWhenNoRedundantCopiesExistForAtLeastOneRegionWithConfiguredRedundancy() {
    List<String> regionNames = getAllRegionNames();
    createAndPopulateRegions(regionNames);

    // Stop all but the first server
    for (int i = 0; i < serversToStart - 1; ++i) {
      int last = servers.size() - 1;
      servers.remove(last).stop();
    }

    waitForRegionsToBeReadyInManagementService(regionNames, servers.size());

    List<String> notSatisfiedOutput = new ArrayList<>(regionNames);
    notSatisfiedOutput.remove(zeroRedundancyRegionName);

    String command = new CommandStringBuilder(COMMAND_NAME).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsError()
        .hasNoSection(UNDER_REDUNDANCY_SECTION);

    InfoResultModelAssert zeroRedundancy = commandResult.hasInfoSection(ZERO_REDUNDANCY_SECTION);
    zeroRedundancy.hasHeader().isEqualTo(NO_REDUNDANT_COPIES_FOR_REGIONS);
    zeroRedundancy.hasOutput().contains(notSatisfiedOutput);

    // A zero redundancy region will always report satisfied redundancy
    InfoResultModelAssert satisfiedRedundancy =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedRedundancy.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedRedundancy.hasOutput().contains(zeroRedundancyRegionName);
  }

  @Test
  public void restoreRedundancyDoesNotBalancePrimariesWhenOptionIsUsed() {
    List<String> regionNames = getAllRegionNames();
    createAndPopulateRegions(regionNames);

    waitForRegionsToBeReadyInManagementService(regionNames, servers.size());

    String command = new CommandStringBuilder(COMMAND_NAME)
        .addOption(DONT_REASSIGN_PRIMARIES).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess()
        .hasNoSection(ZERO_REDUNDANCY_SECTION)
        .hasNoSection(UNDER_REDUNDANCY_SECTION);

    InfoResultModelAssert satisfiedSection =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedSection.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedSection.hasOutput().contains(regionNames);

    InfoResultModelAssert primariesSection = commandResult.hasInfoSection(PRIMARIES_INFO_SECTION);
    primariesSection.hasOutput().contains(PRIMARY_TRANSFERS_COMPLETED + 0,
        PRIMARY_TRANSFER_TIME + 0);

    // Confirm all regions have their configured redundancy and that primaries were not balanced
    int numberOfActiveServers = servers.size();
    servers.get(0).invoke(() -> {
      for (String regionName : regionNames) {
        assertRedundancyStatusForRegion(regionName, true);
        assertPrimariesBalanced(regionName, numberOfActiveServers, false);
      }
    });
  }

  // Helper methods

  private List<String> getAllRegionNames() {
    List<String> regionNames = new ArrayList<>();
    regionNames.add(highRedundancyRegionName);
    regionNames.add(lowRedundancyRegionName);
    regionNames.add(colocatedParentRegionName);
    regionNames.add(colocatedChildRegionName);
    regionNames.add(zeroRedundancyRegionName);
    return regionNames;
  }

  private void createAndPopulateRegions(List<String> regionNames) {
    // Create regions on server 1 and populate them.
    servers.get(0).invoke(() -> {
      createRegions(regionNames);
      populateRegions(regionNames);
    });

    // Create regions on other servers. Since recovery delay is infinite, all buckets for these
    // regions remain on server 1 and redundancy is zero
    servers.subList(1, servers.size()).forEach(s -> s.invoke(() -> {
      createRegions(regionNames);
    }));

    int numberOfActiveServers = servers.size();
    servers.get(0).invoke(() -> {
      // Confirm that redundancy is impaired for all regions
      for (String regionName : regionNames) {
        assertRedundancyStatusForRegion(regionName, false);
        assertPrimariesBalanced(regionName, numberOfActiveServers, false);
      }
    });
  }

  private static void createRegions(List<String> regionsToCreate) {
    if (regionsToCreate.contains(highRedundancyRegionName)) {
      createHighRedundancyRegion();
    }
    if (regionsToCreate.contains(lowRedundancyRegionName)) {
      createLowRedundancyRegion();
    }
    // We have to create both colocated regions or neither
    if (regionsToCreate.contains(colocatedParentRegionName)
        || regionsToCreate.contains(colocatedChildRegionName)) {
      createColocatedRegions();
    }
    if (regionsToCreate.contains(zeroRedundancyRegionName)) {
      createZeroRedundancyRegion();
    }
  }

  private static void createHighRedundancyRegion() {
    PartitionAttributesImpl attributes = getAttributes(highRedundancyCopies);
    InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());
    cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
        .create(highRedundancyRegionName);
  }

  private static void createLowRedundancyRegion() {
    PartitionAttributesImpl attributes = getAttributes(singleRedundantCopy);
    InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());
    cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
        .create(lowRedundancyRegionName);
  }

  private static void createColocatedRegions() {
    PartitionAttributesImpl attributes = getAttributes(singleRedundantCopy);
    InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());
    // Create parent region
    cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
        .create(colocatedParentRegionName);

    // Create colocated region
    attributes.setColocatedWith(colocatedParentRegionName);
    cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
        .create(colocatedChildRegionName);
  }

  private static void createZeroRedundancyRegion() {
    PartitionAttributesImpl attributes = getAttributes(0);
    InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());
    cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
        .create(zeroRedundancyRegionName);
  }

  private static PartitionAttributesImpl getAttributes(int redundantCopies) {
    PartitionAttributesImpl attributes = new PartitionAttributesImpl();
    attributes.setStartupRecoveryDelay(-1);
    attributes.setRecoveryDelay(-1);
    attributes.setRedundantCopies(redundantCopies);
    return attributes;
  }

  private static void populateRegions(List<String> regionNames) {
    if (regionNames.isEmpty()) {
      return;
    }
    List<Region<Object, Object>> regions = new ArrayList<>();
    Cache cache = Objects.requireNonNull(ClusterStartupRule.getCache());

    for (String regionName : regionNames) {
      Region<Object, Object> regionToAdd = cache.getRegion(regionName);
      if (regionToAdd != null) {
        regions.add(regionToAdd);
      }
    }

    IntStream.range(0, 10 * GLOBAL_MAX_BUCKETS_DEFAULT)
        .forEach(i -> regions.forEach(region -> region.put("key" + i, "value" + i)));
  }

  private static void assertRedundancyStatusForRegion(String regionName,
      boolean shouldBeSatisfied) {
    // Redundancy is always satisfied for a region with zero configured redundancy
    if (regionName.equals(zeroRedundancyRegionName)) {
      return;
    }
    Cache cache = Objects.requireNonNull(ClusterStartupRule.getCache());

    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);

    assertThat(region.getRedundancyProvider().isRedundancyImpaired(), is(!shouldBeSatisfied));
  }

  private static void assertPrimariesBalanced(String regionName, int numberOfServers,
      boolean shouldBeBalanced) {
    // Primaries cannot be balanced for regions with no redundant copies
    if (regionName.equals(zeroRedundancyRegionName)) {
      return;
    }
    Cache cache = Objects.requireNonNull(ClusterStartupRule.getCache());

    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
    int primariesOnServer = region.getLocalPrimaryBucketsListTestOnly().size();
    // Add one to the expected number of primaries to deal with integer rounding errors
    int expectedPrimaries = (GLOBAL_MAX_BUCKETS_DEFAULT / numberOfServers) + 1;
    // Because of the way reassigning primaries works, it is sometimes only possible to get the
    // difference between the most loaded member and the least loaded member to be 2, not 1 as would
    // be the case for perfect balance
    String message = "Primaries should be balanced, but expectedPrimaries:actualPrimaries = "
        + expectedPrimaries + ":" + primariesOnServer;
    if (shouldBeBalanced) {
      assertThat(message, Math.abs(primariesOnServer - expectedPrimaries),
          is(lessThanOrEqualTo(2)));
    } else {
      assertThat("Primaries should not be balanced",
          Math.abs(primariesOnServer - expectedPrimaries), is(not(lessThanOrEqualTo(2))));
    }
  }

  private void waitForRegionsToBeReadyInManagementService(List<String> regionNames,
      int activeServers) {
    // There is a race condition that the command can be executed before the management service has
    // completely finished registering the regions, so await here
    locator.invoke(() -> {
      await().until(() -> ManagementService.getManagementService(ClusterStartupRule.getCache())
          .getDistributedSystemMXBean().listRegions().length == regionNames.size());
      for (String regionName : regionNames) {
        await().until(() -> ManagementService.getManagementService(ClusterStartupRule.getCache())
            .getDistributedRegionMXBean("/" + regionName) != null);
        await().until(() -> ManagementService.getManagementService(ClusterStartupRule.getCache())
            .getDistributedRegionMXBean("/" + regionName).getMembers().length == activeServers);
      }
    });
  }
}
