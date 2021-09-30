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
import static org.apache.geode.management.internal.i18n.CliStrings.STATUS_REDUNDANCY;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
  private static final int SERVERS_TO_START = 2;
  private static final String SATISFIED_REGION = "satisfiedRedundancy";
  private static final int SATISFIABLE_COPIES = SERVERS_TO_START - 1;
  private static final String UNSATISFIED_REGION = "unsatisfiedRedundancy";
  private static final int UNSATISFIABLE_COPIES = SERVERS_TO_START;
  private static final String NO_CONFIGURED_REDUNDANCY_REGION = "noConfiguredRedundancy";
  private static final String ZERO_COPIES_REGION = "zeroRedundantCopies";
  private static final List<String> regionNames = Arrays.asList(SATISFIED_REGION,
      UNSATISFIED_REGION, NO_CONFIGURED_REDUNDANCY_REGION, ZERO_COPIES_REGION);
  private static final String EMPTY_REGION = "emptyRegion";

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
  public void statusRedundancyWithNoArgumentsReturnsStatusForAllRegions() {
    createAndPopulateRegions();

    String command = new CommandStringBuilder(STATUS_REDUNDANCY).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess();

    List<String> satisfiedRegions =
        Arrays.asList(SATISFIED_REGION, NO_CONFIGURED_REDUNDANCY_REGION);

    verifyGfshOutput(commandResult, Collections.singletonList(ZERO_COPIES_REGION),
        Collections.singletonList(UNSATISFIED_REGION), satisfiedRegions);
  }

  @Test
  public void statusRedundancyWithIncludeRegionArgumentReturnsStatusForOnlyThatRegion() {
    createAndPopulateRegions();

    String includedRegion = SATISFIED_REGION;
    List<String> nonIncludedRegions = new ArrayList<>(regionNames);
    nonIncludedRegions.remove(includedRegion);

    String command = new CommandStringBuilder(STATUS_REDUNDANCY)
        .addOption(REDUNDANCY_INCLUDE_REGION, includedRegion)
        .getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess()
        .doesNotContainOutput(nonIncludedRegions.toArray(new String[0]));

    verifyGfshOutput(commandResult, new ArrayList<>(), new ArrayList<>(),
        Collections.singletonList(includedRegion));
  }

  @Test
  public void statusRedundancyWithExcludeRegionArgumentReturnsStatusForAllExceptThatRegion() {
    createAndPopulateRegions();

    String excludedRegion = ZERO_COPIES_REGION;

    String command = new CommandStringBuilder(STATUS_REDUNDANCY)
        .addOption(REDUNDANCY_EXCLUDE_REGION, excludedRegion)
        .getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess()
        .doesNotContainOutput(excludedRegion);

    List<String> satisfiedRegions =
        Arrays.asList(SATISFIED_REGION, NO_CONFIGURED_REDUNDANCY_REGION);

    verifyGfshOutput(commandResult, new ArrayList<>(),
        Collections.singletonList(UNSATISFIED_REGION), satisfiedRegions);
  }

  @Test
  public void statusRedundancyWithMatchingIncludeAndExcludeRegionArgumentsReturnsStatusForIncludedRegion() {
    createAndPopulateRegions();

    String includedAndExcludedRegion = SATISFIED_REGION;
    List<String> nonIncludedRegions = new ArrayList<>(regionNames);
    nonIncludedRegions.remove(includedAndExcludedRegion);

    String command = new CommandStringBuilder(STATUS_REDUNDANCY)
        .addOption(REDUNDANCY_INCLUDE_REGION, includedAndExcludedRegion)
        .addOption(REDUNDANCY_EXCLUDE_REGION, includedAndExcludedRegion)
        .getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess()
        .doesNotContainOutput(nonIncludedRegions.toArray(new String[0]));

    verifyGfshOutput(commandResult, new ArrayList<>(), new ArrayList<>(),
        Collections.singletonList(includedAndExcludedRegion));
  }

  @Test
  public void statusRedundancyWithNoArgumentsReturnsSuccessWhenNoRegionsArePresent() {
    String command = new CommandStringBuilder(STATUS_REDUNDANCY).getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess().hasInfoSection(NO_MEMBERS_SECTION)
        .hasHeader()
        .isEqualTo(NO_MEMBERS_HEADER);
  }

  @Test
  public void statusRedundancyWithIncludeRegionReturnsErrorWhenAtLeastOneIncludedRegionIsNotPresent() {
    createAndPopulateRegions();

    String nonexistentRegion = "fakeRegion";

    String includedRegion = SATISFIED_REGION;
    List<String> nonIncludedRegions = new ArrayList<>(regionNames);
    nonIncludedRegions.remove(includedRegion);

    String command = new CommandStringBuilder(STATUS_REDUNDANCY)
        .addOption(REDUNDANCY_INCLUDE_REGION, nonexistentRegion + "," + includedRegion)
        .getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsError()
        .doesNotContainOutput(nonIncludedRegions.toArray(new String[0]));

    InfoResultModelAssert noMembersForRegion =
        commandResult.hasInfoSection(NO_MEMBERS_FOR_REGION_SECTION);
    noMembersForRegion.hasHeader().isEqualTo(NO_MEMBERS_FOR_REGION_HEADER);
    noMembersForRegion.hasLines().containsExactly(nonexistentRegion);

    verifyGfshOutput(commandResult, new ArrayList<>(), new ArrayList<>(),
        Collections.singletonList(includedRegion));
  }

  @Test
  public void statusRedundancyReturnsRegionWithNoBucketsCreatedStatusAsNoRedundantCopies() {
    createRegion();
    String command = new CommandStringBuilder(STATUS_REDUNDANCY).getCommandString();
    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess();
    verifyGfshOutput(commandResult, Collections.singletonList(EMPTY_REGION), new ArrayList<>(),
        new ArrayList<>());
  }

  @Test
  public void statusRedundancyReturnsEmptyRegionStatusCorrectly() {
    createRegion();
    doPutAndRemove();
    String command = new CommandStringBuilder(STATUS_REDUNDANCY).getCommandString();
    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsSuccess();
    verifyGfshOutput(commandResult, new ArrayList<>(), new ArrayList<>(),
        Collections.singletonList(EMPTY_REGION));
  }

  private void createRegion() {
    servers.forEach(s -> s.invoke(() -> {
      InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());
      cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT).create(EMPTY_REGION);
    }));
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + EMPTY_REGION, 2);
  }

  private void doPutAndRemove() {
    servers.get(0).invoke(() -> {
      InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());
      Region<Object, Object> region = cache.getRegion(EMPTY_REGION);
      region.put(1, 1);
      region.remove(1);
    });
  }

  private void createAndPopulateRegions() {
    servers.forEach(s -> s.invoke(() -> {
      PartitionAttributesImpl attributes = new PartitionAttributesImpl();
      attributes.setStartupRecoveryDelay(-1);
      attributes.setRecoveryDelay(-1);

      InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());

      // Create a region whose redundancy cannot be satisfied due to not enough members
      attributes.setRedundantCopies(UNSATISFIABLE_COPIES);
      cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
          .create(UNSATISFIED_REGION);

      // Create a region whose redundancy can be satisfied
      attributes.setRedundantCopies(SATISFIABLE_COPIES);
      cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
          .create(SATISFIED_REGION);

      // Create a region configured to have zero redundant copies
      attributes.setRedundantCopies(0);
      cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
          .create(NO_CONFIGURED_REDUNDANCY_REGION);
    }));

    servers.get(0).invoke(() -> {
      // Create a region on only server1 so that it will not be able to create any redundant copies
      PartitionAttributesImpl attributes = new PartitionAttributesImpl();
      attributes.setStartupRecoveryDelay(-1);
      attributes.setRecoveryDelay(-1);
      attributes.setRedundantCopies(SATISFIABLE_COPIES);

      InternalCache cache = Objects.requireNonNull(ClusterStartupRule.getCache());

      cache.createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes).create(
          ZERO_COPIES_REGION);

      // Populate all the regions
      regionNames.forEach(regionName -> {
        Region<Object, Object> region = cache.getRegion(regionName);
        IntStream.range(0, 5 * GLOBAL_MAX_BUCKETS_DEFAULT)
            .forEach(i -> region.put("key" + i, "value" + i));
      });
    });

    // Wait for the regions to be ready
    regionNames.forEach(region -> {
      int expectedServers = region.equals(ZERO_COPIES_REGION) ? 1 : SERVERS_TO_START;
      locator.waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + region, expectedServers);
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

    // Verify primaries section is not included
    result.hasNoSection(PRIMARIES_INFO_SECTION);
  }
}
