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
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.NO_REDUNDANT_COPIES_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.REDUNDANCY_NOT_SATISFIED_FOR_REGIONS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyResultsImpl.REDUNDANCY_SATISFIED_FOR_REGIONS;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.FULLY_SATISFIED_REDUNDANCY;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_FOR_REGION_HEADER;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_FOR_REGION_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_HEADER;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.NO_MEMBERS_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.PARTIALLY_SATISFIED_REDUNDANCY;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.SATISFIED_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.SUMMARY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.UNDER_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.ZERO_REDUNDANCY_SECTION;
import static org.apache.geode.management.internal.cli.commands.RedundancyCommandUtils.ZERO_REDUNDANT_COPIES;
import static org.apache.geode.management.internal.cli.commands.StatusRedundancyCommand.COMMAND_NAME;
import static org.apache.geode.management.internal.cli.commands.StatusRedundancyCommand.EXCLUDE_REGION;
import static org.apache.geode.management.internal.cli.commands.StatusRedundancyCommand.INCLUDE_REGION;

import java.util.ArrayList;
import java.util.Arrays;
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

    String command = new CommandStringBuilder(COMMAND_NAME).getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command);
    commandResult.statusIsSuccess();

    verifySummarySection(commandResult, 1, 1, 2);

    InfoResultModelAssert zeroRedundancy = commandResult.hasInfoSection(ZERO_REDUNDANCY_SECTION);
    zeroRedundancy.hasHeader().isEqualTo(NO_REDUNDANT_COPIES_FOR_REGIONS);
    zeroRedundancy.hasOutput().contains(ZERO_COPIES_REGION);

    InfoResultModelAssert underRedundancy = commandResult.hasInfoSection(UNDER_REDUNDANCY_SECTION);
    underRedundancy.hasHeader().isEqualTo(REDUNDANCY_NOT_SATISFIED_FOR_REGIONS);
    underRedundancy.hasOutput().contains(UNSATISFIED_REGION);

    InfoResultModelAssert satisfiedRedundancy =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedRedundancy.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedRedundancy.hasOutput().contains(SATISFIED_REGION,
        NO_CONFIGURED_REDUNDANCY_REGION);
  }

  @Test
  public void statusRedundancyWithIncludeRegionArgumentReturnsStatusForOnlyThatRegion() {
    createAndPopulateRegions();

    String command = new CommandStringBuilder(COMMAND_NAME)
        .addOption(INCLUDE_REGION, SATISFIED_REGION)
        .getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command);
    commandResult.statusIsSuccess()
        .hasNoSection(ZERO_REDUNDANCY_SECTION)
        .hasNoSection(UNDER_REDUNDANCY_SECTION)
        .doesNotContainOutput(UNSATISFIED_REGION)
        .doesNotContainOutput(ZERO_COPIES_REGION)
        .doesNotContainOutput(NO_CONFIGURED_REDUNDANCY_REGION);

    verifySummarySection(commandResult, 0, 0, 1);

    InfoResultModelAssert satisfiedRedundancy =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedRedundancy.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedRedundancy.hasOutput().contains(SATISFIED_REGION);
  }

  @Test
  public void statusRedundancyWithExcludeRegionArgumentReturnsStatusForAllExceptThatRegion() {
    createAndPopulateRegions();

    String command = new CommandStringBuilder(COMMAND_NAME)
        .addOption(EXCLUDE_REGION, ZERO_COPIES_REGION)
        .getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command);
    commandResult.statusIsSuccess()
        .hasNoSection(ZERO_REDUNDANCY_SECTION)
        .doesNotContainOutput(ZERO_COPIES_REGION);

    verifySummarySection(commandResult, 0, 1, 2);

    InfoResultModelAssert underRedundancy = commandResult.hasInfoSection(UNDER_REDUNDANCY_SECTION);
    underRedundancy.hasHeader().isEqualTo(REDUNDANCY_NOT_SATISFIED_FOR_REGIONS);
    underRedundancy.hasOutput().contains(UNSATISFIED_REGION);

    InfoResultModelAssert satisfiedRedundancy =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedRedundancy.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedRedundancy.hasOutput().contains(SATISFIED_REGION,
        NO_CONFIGURED_REDUNDANCY_REGION);
  }

  @Test
  public void statusRedundancyWithMatchingIncludeAndExcludeRegionArgumentsReturnsStatusForIncludedRegion() {
    createAndPopulateRegions();

    String command = new CommandStringBuilder(COMMAND_NAME)
        .addOption(INCLUDE_REGION, SATISFIED_REGION)
        .addOption(EXCLUDE_REGION, SATISFIED_REGION)
        .getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command);
    commandResult.statusIsSuccess()
        .hasNoSection(ZERO_REDUNDANCY_SECTION)
        .hasNoSection(UNDER_REDUNDANCY_SECTION)
        .doesNotContainOutput(UNSATISFIED_REGION)
        .doesNotContainOutput(ZERO_COPIES_REGION)
        .doesNotContainOutput(NO_CONFIGURED_REDUNDANCY_REGION);

    verifySummarySection(commandResult, 0, 0, 1);

    InfoResultModelAssert satisfiedRedundancy =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedRedundancy.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedRedundancy.hasOutput().contains(SATISFIED_REGION);
  }

  @Test
  public void statusRedundancyWithNoArgumentsReturnsSuccessWhenNoRegionsArePresent() {
    String command = new CommandStringBuilder(COMMAND_NAME).getCommandString();
    gfsh.executeAndAssertThat(command).statusIsSuccess().hasInfoSection(NO_MEMBERS_SECTION)
        .hasHeader()
        .isEqualTo(NO_MEMBERS_HEADER);
  }

  @Test
  public void statusRedundancyWithIncludeRegionReturnsErrorWhenAtLeastOneIncludedRegionIsNotPresent() {
    createAndPopulateRegions();

    String nonexistentRegion = "fakeRegion";
    String command = new CommandStringBuilder(COMMAND_NAME)
        .addOption(INCLUDE_REGION, nonexistentRegion + "," + SATISFIED_REGION)
        .getCommandString();

    CommandResultAssert commandResult = gfsh.executeAndAssertThat(command).statusIsError()
        .hasNoSection(ZERO_REDUNDANCY_SECTION)
        .hasNoSection(UNDER_REDUNDANCY_SECTION);

    InfoResultModelAssert noMembersForRegion =
        commandResult.hasInfoSection(NO_MEMBERS_FOR_REGION_SECTION);
    noMembersForRegion.hasHeader().isEqualTo(NO_MEMBERS_FOR_REGION_HEADER);
    noMembersForRegion.hasLines().containsExactly(nonexistentRegion);

    verifySummarySection(commandResult, 0, 0, 1);

    InfoResultModelAssert satisfiedRedundancy =
        commandResult.hasInfoSection(SATISFIED_REDUNDANCY_SECTION);
    satisfiedRedundancy.hasHeader().isEqualTo(REDUNDANCY_SATISFIED_FOR_REGIONS);
    satisfiedRedundancy.hasOutput().contains(SATISFIED_REGION);
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

  private void verifySummarySection(CommandResultAssert commandResult, int zeroCopies,
      int partiallySatisfied, int fullySatisfied) {
    InfoResultModelAssert summary = commandResult.hasInfoSection(SUMMARY_SECTION);
    summary.hasOutput().contains(ZERO_REDUNDANT_COPIES + zeroCopies);
    summary.hasOutput().contains(PARTIALLY_SATISFIED_REDUNDANCY + partiallySatisfied);
    summary.hasOutput().contains(FULLY_SATISFIED_REDUNDANCY + fullySatisfied);
  }
}
