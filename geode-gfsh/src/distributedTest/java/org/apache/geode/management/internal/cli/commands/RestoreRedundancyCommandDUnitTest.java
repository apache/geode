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
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl.NO_REDUNDANT_COPIES_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl.PRIMARY_TRANSFERS_COMPLETED;
import static org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl.PRIMARY_TRANSFER_TIME;
import static org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl.REDUNDANCY_NOT_SATISFIED_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.SerializableRestoreRedundancyResultsImpl.REDUNDANCY_SATISFIED_FOR_REGIONS;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.FULLY_SATISFIED_REDUNDANCY;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.NO_MEMBERS_FOR_REGION_HEADER;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.NO_MEMBERS_FOR_REGION_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.NO_MEMBERS_HEADER;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.NO_MEMBERS_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.PARTIALLY_SATISFIED_REDUNDANCY;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.PRIMARIES_INFO_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.SATISFIED_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.SUMMARY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.UNDER_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.ZERO_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommand.ZERO_REDUNDANT_COPIES;
import static org.apache.geode.management.internal.i18n.CliStrings.REDUNDANCY_EXCLUDE_REGION;
import static org.apache.geode.management.internal.i18n.CliStrings.REDUNDANCY_INCLUDE_REGION;
import static org.apache.geode.management.internal.i18n.CliStrings.REDUNDANCY_REASSIGN_PRIMARIES;
import static org.apache.geode.management.internal.i18n.CliStrings.RESTORE_REDUNDANCY;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

import java.util.ArrayList;
import java.util.Collections;
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
  private static final int SERVERS_TO_START = 3;
  private static final String HIGH_REDUNDANCY_REGION_NAME = "highRedundancy";
  private static final int HIGH_REDUNDANCY_COPIES = SERVERS_TO_START - 1;
  private static final String LOW_REDUNDANCY_REGION_NAME = "lowRedundancy";
  private static final String PARENT_REGION_NAME = "colocatedParent";
  private static final String CHILD_REGION_NAME = "colocatedChild";
  private static final int SINGLE_REDUNDANT_COPY = 1;
  private static final String NO_CONFIGURED_REDUNDANCY_REGION_NAME = "noConfiguredRedundancy";

  @Before
  public void setUp() throws Exception {
    servers = new ArrayList<>();
    locator = cluster.startLocatorVM(0);
    locatorPort = locator.getPort();
    IntStream.range(0, SERVERS_TO_START)
        .forEach(i -> servers.add(cluster.startServerVM(i + 1, locatorPort)));
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void restoreRedundancyWithNoArgumentsRestoresRedundancyForAllRegions() {
    List<String> regionNames = getAllRegionNames();
    createAndPopulateRegions(regionNames);

    int numberOfServers = servers.size();
    regionNames.forEach(region -> locator
        .waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + region, numberOfServers));

    String command = new CommandStringBuilder(RESTORE_REDUNDANCY).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess();

    verifyGfshOutput(commandResult, new ArrayList<>(), new ArrayList<>(), regionNames);

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

    int numberOfServers = servers.size();
    regionNames.forEach(region -> locator
        .waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + region, numberOfServers));

    String regionToInclude = HIGH_REDUNDANCY_REGION_NAME;
    List<String> nonIncludedRegions = new ArrayList<>(regionNames);
    nonIncludedRegions.remove(regionToInclude);

    String command = new CommandStringBuilder(RESTORE_REDUNDANCY)
        .addOption(REDUNDANCY_INCLUDE_REGION, regionToInclude).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess()
        .doesNotContainOutput(nonIncludedRegions.toArray(new String[0]));

    verifyGfshOutput(commandResult, new ArrayList<>(), new ArrayList<>(),
        Collections.singletonList(regionToInclude));

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

    int numberOfServers = servers.size();
    regionNames.forEach(region -> locator
        .waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + region, numberOfServers));

    String regionToExclude = HIGH_REDUNDANCY_REGION_NAME;
    List<String> regionsToInclude = new ArrayList<>(regionNames);
    regionsToInclude.remove(regionToExclude);

    String command = new CommandStringBuilder(RESTORE_REDUNDANCY)
        .addOption(REDUNDANCY_EXCLUDE_REGION, regionToExclude).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess()
        .doesNotContainOutput(regionToExclude);

    verifyGfshOutput(commandResult, new ArrayList<>(), new ArrayList<>(), regionsToInclude);

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

    int numberOfServers = servers.size();
    regionNames.forEach(region -> locator
        .waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + region, numberOfServers));

    String regionToIncludeAndExclude = HIGH_REDUNDANCY_REGION_NAME;
    List<String> nonIncludedRegions = new ArrayList<>(regionNames);
    nonIncludedRegions.remove(regionToIncludeAndExclude);

    String command =
        new CommandStringBuilder(RESTORE_REDUNDANCY)
            .addOption(REDUNDANCY_INCLUDE_REGION, regionToIncludeAndExclude)
            .addOption(REDUNDANCY_EXCLUDE_REGION, regionToIncludeAndExclude).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess()
        .doesNotContainOutput(nonIncludedRegions.toArray(new String[0]));

    verifyGfshOutput(commandResult, new ArrayList<>(), new ArrayList<>(),
        Collections.singletonList(regionToIncludeAndExclude));

    int numberOfActiveServers = servers.size();
    servers.get(0).invoke(() -> {
      // Confirm that redundancy is restored for only the specified region
      for (String regionName : regionNames) {
        boolean shouldBeSatisfied = regionName.equals(regionToIncludeAndExclude);
        assertRedundancyStatusForRegion(regionName, shouldBeSatisfied);
        assertPrimariesBalanced(regionName, numberOfActiveServers, shouldBeSatisfied);
      }
    });
  }

  @Test
  public void restoreRedundancyWithIncludeRegionArgumentThatIsAParentColocatedRegionRegionRestoresRegionsColocatedWithIt() {
    List<String> regionNames = getAllRegionNames();
    createAndPopulateRegions(regionNames);

    int numberOfServers = servers.size();
    regionNames.forEach(region -> locator
        .waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + region, numberOfServers));

    List<String> nonIncludedRegions = new ArrayList<>(regionNames);
    nonIncludedRegions.remove(PARENT_REGION_NAME);
    nonIncludedRegions.remove(CHILD_REGION_NAME);

    List<String> colocatedRegions = new ArrayList<>();
    colocatedRegions.add(PARENT_REGION_NAME);
    colocatedRegions.add(CHILD_REGION_NAME);

    String command = new CommandStringBuilder(RESTORE_REDUNDANCY)
        .addOption(REDUNDANCY_INCLUDE_REGION, PARENT_REGION_NAME).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess()
        .doesNotContainOutput(nonIncludedRegions.toArray(new String[0]));

    verifyGfshOutput(commandResult, new ArrayList<>(), new ArrayList<>(), colocatedRegions);

    int numberOfActiveServers = servers.size();
    servers.get(0).invoke(() -> {
      // Confirm that redundancy is restored for both colocated regions
      for (String regionName : regionNames) {
        boolean shouldBeSatisfied =
            regionName.equals(PARENT_REGION_NAME) || regionName.equals(CHILD_REGION_NAME);
        assertRedundancyStatusForRegion(regionName, shouldBeSatisfied);
        assertPrimariesBalanced(regionName, numberOfActiveServers, shouldBeSatisfied);
      }
    });
  }

  @Test
  public void restoreRedundancyReturnsErrorWhenNotAllRegionsHaveFullySatisfiedRedundancy() {
    List<String> regionNames = getAllRegionNames();
    createAndPopulateRegions(regionNames);

    List<String> satisfiedRegions = new ArrayList<>(regionNames);
    satisfiedRegions.remove(HIGH_REDUNDANCY_REGION_NAME);

    // Stop the last server. The high redundancy region now cannot satisfy redundancy
    servers.remove(servers.size() - 1).stop();

    int numberOfServers = servers.size();
    regionNames.forEach(region -> locator
        .waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + region, numberOfServers));

    String command = new CommandStringBuilder(RESTORE_REDUNDANCY).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsError();

    verifyGfshOutput(commandResult, new ArrayList<>(),
        Collections.singletonList(HIGH_REDUNDANCY_REGION_NAME), satisfiedRegions);

    int numberOfActiveServers = servers.size();
    servers.get(0).invoke(() -> {
      // Confirm that redundancy is restored for regions with lower redundancy
      for (String regionName : regionNames) {
        boolean shouldBeSatisfied = !regionName.equals(HIGH_REDUNDANCY_REGION_NAME);
        assertRedundancyStatusForRegion(regionName, shouldBeSatisfied);
        assertPrimariesBalanced(regionName, numberOfActiveServers, true);
      }
    });
  }

  @Test
  public void restoreRedundancyWithNoArgumentsReturnsSuccessWhenNoRegionsArePresent() {
    String command = new CommandStringBuilder(RESTORE_REDUNDANCY).getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess().hasInfoSection(NO_MEMBERS_SECTION)
        .hasHeader()
        .isEqualTo(NO_MEMBERS_HEADER);
  }

  @Test
  public void restoreRedundancyWithIncludeRegionReturnsErrorWhenAtLeastOneIncludedRegionIsNotPresent() {
    List<String> regionNames = new ArrayList<>();
    regionNames.add(LOW_REDUNDANCY_REGION_NAME);
    createAndPopulateRegions(regionNames);

    int numberOfServers = servers.size();
    regionNames.forEach(region -> locator
        .waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + region, numberOfServers));

    String nonexistentRegion = "fakeRegion";
    String command = new CommandStringBuilder(RESTORE_REDUNDANCY)
        .addOption(REDUNDANCY_INCLUDE_REGION, nonexistentRegion + "," + LOW_REDUNDANCY_REGION_NAME)
        .getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsError();

    verifyGfshOutput(commandResult, new ArrayList<>(), new ArrayList<>(), regionNames);

    InfoResultModelAssert noMembersForRegion =
        commandResult.hasInfoSection(NO_MEMBERS_FOR_REGION_SECTION);
    noMembersForRegion.hasHeader().isEqualTo(NO_MEMBERS_FOR_REGION_HEADER);
    noMembersForRegion.hasLines().containsExactly(nonexistentRegion);
  }

  @Test
  public void restoreRedundancyReturnsErrorWhenNoRedundantCopiesExistForAtLeastOneRegionWithConfiguredRedundancy() {
    List<String> regionNames = getAllRegionNames();
    createAndPopulateRegions(regionNames);

    // Stop all but the first server
    for (int i = 0; i < SERVERS_TO_START - 1; ++i) {
      int last = servers.size() - 1;
      servers.remove(last).stop();
    }

    int numberOfServers = servers.size();
    regionNames.forEach(region -> locator
        .waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + region, numberOfServers));

    List<String> zeroRedundancyRegions = new ArrayList<>(regionNames);
    zeroRedundancyRegions.remove(NO_CONFIGURED_REDUNDANCY_REGION_NAME);

    String command = new CommandStringBuilder(RESTORE_REDUNDANCY).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsError();

    // A region configured to have zero redundant copies should always report satisfied redundancy
    verifyGfshOutput(commandResult, zeroRedundancyRegions, new ArrayList<>(),
        Collections.singletonList(
            NO_CONFIGURED_REDUNDANCY_REGION_NAME));
  }

  @Test
  public void restoreRedundancyDoesNotBalancePrimariesWhenOptionIsUsed() {
    List<String> regionNames = getAllRegionNames();
    createAndPopulateRegions(regionNames);

    int numberOfServers = servers.size();
    regionNames.forEach(region -> locator
        .waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + region, numberOfServers));

    String command = new CommandStringBuilder(RESTORE_REDUNDANCY)
        .addOption(REDUNDANCY_REASSIGN_PRIMARIES, "false").getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess();

    verifyGfshOutput(commandResult, new ArrayList<>(), new ArrayList<>(), regionNames);

    // Verify that the command reports no primaries transferred
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

  @Test
  public void restoreRedundancyReturnsCorrectStatusWhenNotAllBucketsHaveBeenCreated() {
    servers.forEach(s -> s.invoke(() -> {
      createLowRedundancyRegion();
      // Put a single entry into the region so that only one bucket gets created
      Objects.requireNonNull(ClusterStartupRule.getCache())
          .getRegion(LOW_REDUNDANCY_REGION_NAME)
          .put("key", "value");
    }));

    int numberOfServers = servers.size();
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + LOW_REDUNDANCY_REGION_NAME,
        numberOfServers);

    String command = new CommandStringBuilder(RESTORE_REDUNDANCY).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess();

    verifyGfshOutput(commandResult, new ArrayList<>(), new ArrayList<>(),
        Collections.singletonList(LOW_REDUNDANCY_REGION_NAME));
  }

  // Helper methods

  private List<String> getAllRegionNames() {
    List<String> regionNames = new ArrayList<>();
    regionNames.add(HIGH_REDUNDANCY_REGION_NAME);
    regionNames.add(LOW_REDUNDANCY_REGION_NAME);
    regionNames.add(PARENT_REGION_NAME);
    regionNames.add(CHILD_REGION_NAME);
    regionNames.add(NO_CONFIGURED_REDUNDANCY_REGION_NAME);
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
    if (regionsToCreate.contains(HIGH_REDUNDANCY_REGION_NAME)) {
      createHighRedundancyRegion();
    }
    if (regionsToCreate.contains(LOW_REDUNDANCY_REGION_NAME)) {
      createLowRedundancyRegion();
    }
    // We have to create both colocated regions or neither
    if (regionsToCreate.contains(PARENT_REGION_NAME)
        || regionsToCreate.contains(CHILD_REGION_NAME)) {
      createColocatedRegions();
    }
    if (regionsToCreate.contains(NO_CONFIGURED_REDUNDANCY_REGION_NAME)) {
      createNoConfiguredRedundancyRegion();
    }
  }

  private static void createHighRedundancyRegion() {
    PartitionAttributesImpl attributes = getAttributes(HIGH_REDUNDANCY_COPIES);
    InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());
    cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
        .create(HIGH_REDUNDANCY_REGION_NAME);
  }

  private static void createLowRedundancyRegion() {
    PartitionAttributesImpl attributes = getAttributes(SINGLE_REDUNDANT_COPY);
    InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());
    cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
        .create(LOW_REDUNDANCY_REGION_NAME);
  }

  private static void createColocatedRegions() {
    PartitionAttributesImpl attributes = getAttributes(SINGLE_REDUNDANT_COPY);
    InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());
    // Create parent region
    cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
        .create(PARENT_REGION_NAME);

    // Create colocated region
    attributes.setColocatedWith(PARENT_REGION_NAME);
    cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
        .create(CHILD_REGION_NAME);
  }

  private static void createNoConfiguredRedundancyRegion() {
    PartitionAttributesImpl attributes = getAttributes(0);
    InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());
    cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
        .create(NO_CONFIGURED_REDUNDANCY_REGION_NAME);
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
    Cache cache = Objects.requireNonNull(ClusterStartupRule.getCache());

    // Populate all the regions
    regionNames.forEach(regionName -> {
      Region<Object, Object> region = cache.getRegion(regionName);
      IntStream.range(0, 5 * GLOBAL_MAX_BUCKETS_DEFAULT)
          .forEach(i -> region.put("key" + i, "value" + i));
    });
  }

  private void verifyGfshOutput(CommandResultAssert result, List<String> expectedZeroCopiesRegions,
      List<String> expectedPartiallySatisfiedRegions, List<String> expectedFullySatisfiedRegions) {
    // Verify summary section
    InfoResultModelAssert summary = result.hasInfoSection(SUMMARY_SECTION);
    summary.hasOutput().contains(ZERO_REDUNDANT_COPIES + expectedZeroCopiesRegions.size());
    summary.hasOutput()
        .contains(PARTIALLY_SATISFIED_REDUNDANCY + expectedPartiallySatisfiedRegions.size());
    summary.hasOutput().contains(FULLY_SATISFIED_REDUNDANCY + expectedFullySatisfiedRegions.size());

    // Verify zero redundancy section
    if (!expectedZeroCopiesRegions.isEmpty()) {
      InfoResultModelAssert zeroRedundancy = result.hasInfoSection(ZERO_REDUNDANCY_SECTION);
      zeroRedundancy.hasHeader().isEqualTo(NO_REDUNDANT_COPIES_FOR_REGIONS);
      zeroRedundancy.hasOutput().contains(expectedZeroCopiesRegions);
    } else {
      result.hasNoSection(ZERO_REDUNDANCY_SECTION);
    }

    // Verify under redundancy section
    if (!expectedPartiallySatisfiedRegions.isEmpty()) {
      InfoResultModelAssert zeroRedundancy = result.hasInfoSection(UNDER_REDUNDANCY_SECTION);
      zeroRedundancy.hasHeader().isEqualTo(REDUNDANCY_NOT_SATISFIED_FOR_REGIONS);
      zeroRedundancy.hasOutput().contains(expectedPartiallySatisfiedRegions);
    } else {
      result.hasNoSection(UNDER_REDUNDANCY_SECTION);
    }

    // Verify fully satisfied section
    if (!expectedFullySatisfiedRegions.isEmpty()) {
      InfoResultModelAssert zeroRedundancy = result.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
      zeroRedundancy.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
      zeroRedundancy.hasOutput().contains(expectedFullySatisfiedRegions);
    } else {
      result.hasNoSection(SATISFIED_REDUNDANCY_SECTION);
    }

    // Verify primaries section exists
    InfoResultModelAssert primariesSection = result.hasInfoSection(PRIMARIES_INFO_SECTION);
    primariesSection.hasOutput().contains(PRIMARY_TRANSFERS_COMPLETED, PRIMARY_TRANSFER_TIME);
  }

  private static void assertRedundancyStatusForRegion(String regionName,
      boolean shouldBeSatisfied) {
    // Redundancy is always satisfied for a region with zero configured redundancy
    if (regionName.equals(NO_CONFIGURED_REDUNDANCY_REGION_NAME)) {
      return;
    }
    Cache cache = Objects.requireNonNull(ClusterStartupRule.getCache());

    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);

    assertThat(region.getRedundancyProvider().isRedundancyImpaired(), is(!shouldBeSatisfied));
  }

  private static void assertPrimariesBalanced(String regionName, int numberOfServers,
      boolean shouldBeBalanced) {
    // Primaries cannot be balanced for regions with no redundant copies
    if (regionName.equals(NO_CONFIGURED_REDUNDANCY_REGION_NAME)) {
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
    String message = "Primaries should be balanced for region " + regionName
        + ", but expectedPrimaries:actualPrimaries = "
        + expectedPrimaries + ":" + primariesOnServer;
    if (shouldBeBalanced) {
      assertThat(message, Math.abs(primariesOnServer - expectedPrimaries),
          is(lessThanOrEqualTo(2)));
    } else {
      assertThat("Primaries should not be balanced for region " + regionName,
          Math.abs(primariesOnServer - expectedPrimaries), is(not(lessThanOrEqualTo(2))));
    }
  }
}
