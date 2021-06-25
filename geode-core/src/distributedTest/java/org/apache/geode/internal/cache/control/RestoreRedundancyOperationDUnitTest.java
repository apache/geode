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
package org.apache.geode.internal.cache.control;

import static org.apache.geode.cache.PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT;
import static org.apache.geode.management.runtime.RegionRedundancyStatus.RedundancyStatus.NOT_SATISFIED;
import static org.apache.geode.management.runtime.RegionRedundancyStatus.RedundancyStatus.NO_REDUNDANT_COPIES;
import static org.apache.geode.management.runtime.RegionRedundancyStatus.RedundancyStatus.SATISFIED;
import static org.apache.geode.management.runtime.RestoreRedundancyResults.Status.FAILURE;
import static org.apache.geode.management.runtime.RestoreRedundancyResults.Status.SUCCESS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.internal.cache.PartitionAttributesImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.management.runtime.RegionRedundancyStatus;
import org.apache.geode.management.runtime.RestoreRedundancyResults;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

@RunWith(JUnitParamsRunner.class)
public class RestoreRedundancyOperationDUnitTest {
  private List<MemberVM> servers;
  private static final int SERVERS_TO_START = 3;
  private static final String PARENT_REGION_NAME = "parentColocatedRegion";
  private static final String CHILD_REGION_NAME = "childColocatedRegion";
  private static final int DESIRED_REDUNDANCY_COPIES = 2;
  private static final String LOW_REDUNDANCY_REGION_NAME = "lowRedundancyRegion";
  private static final int LOW_REDUNDANCY_COPIES = 1;
  private static final int ENTRIES = 5 * GLOBAL_MAX_BUCKETS_DEFAULT;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Before
  public void startUp() {
    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    servers = new ArrayList<>();
    IntStream.range(0, SERVERS_TO_START)
        .forEach(i -> servers.add(cluster.startServerVM(i + 1, locatorPort)));

    // Create the regions on server1 and populate with data
    servers.get(0).invoke(() -> {
      Collection<Region<Object, Object>> regions = createRegions();
      regions.forEach(
          region -> IntStream.range(0, ENTRIES).forEach(i -> region.put("key" + i, "value" + i)));
    });

    // Create regions on other servers but do not populate with data
    servers.stream().skip(1).forEach(
        s -> s.invoke((SerializableRunnableIF) RestoreRedundancyOperationDUnitTest::createRegions));

    // Confirm that redundancy is impaired and primaries unbalanced for all regions on all members
    servers.forEach(s -> s.invoke(() -> {
      assertRedundancyStatus(PARENT_REGION_NAME, false);
      assertRedundancyStatus(CHILD_REGION_NAME, false);
      assertRedundancyStatus(LOW_REDUNDANCY_REGION_NAME, false);
      assertPrimariesBalanced(PARENT_REGION_NAME, SERVERS_TO_START, false);
      assertPrimariesBalanced(CHILD_REGION_NAME, SERVERS_TO_START, false);
      assertPrimariesBalanced(LOW_REDUNDANCY_REGION_NAME, SERVERS_TO_START, false);
    }));
  }

  @Test
  public void statsAreUpdatedWhenRestoreRedundancyIsCalled() {
    servers.get(0).invoke(() -> {
      restoreRedundancyAndGetResults(null, null, true);

      ResourceManagerStats stats = Objects.requireNonNull(ClusterStartupRule.getCache())
          .getInternalResourceManager().getStats();

      assertThat(stats.getRestoreRedundanciesInProgress()).isEqualTo(0L);
      assertThat(stats.getRestoreRedundanciesCompleted()).isEqualTo(1L);
      assertThat(stats.getRestoreRedundancyTime()).isGreaterThan(0L);
    });
  }

  @Test
  public void redundancyIsRecoveredAndPrimariesBalancedWhenRestoreRedundancyIsCalledWithNoIncludedOrExcludedRegions() {
    servers.get(0).invoke(() -> {
      RestoreRedundancyResults results = restoreRedundancyAndGetResults(null, null, true);
      assertThat(results.getRegionOperationStatus()).isEqualTo(SUCCESS);
      assertThat(results.getTotalPrimaryTransfersCompleted() > 0).isTrue();
      assertThat(results.getTotalPrimaryTransferTime() > 0).isTrue();
      assertThat(results.getRegionResult(PARENT_REGION_NAME).getStatus()).isEqualTo(SATISFIED);
      assertThat(results.getRegionResult(CHILD_REGION_NAME).getStatus()).isEqualTo(SATISFIED);
      assertThat(results.getRegionResult(LOW_REDUNDANCY_REGION_NAME).getStatus())
          .isEqualTo(SATISFIED);
    });

    servers.forEach(s -> s.invoke(() -> {
      assertRedundancyStatus(PARENT_REGION_NAME, true);
      assertRedundancyStatus(CHILD_REGION_NAME, true);
      assertRedundancyStatus(LOW_REDUNDANCY_REGION_NAME, true);
      assertPrimariesBalanced(PARENT_REGION_NAME, SERVERS_TO_START, true);
      assertPrimariesBalanced(CHILD_REGION_NAME, SERVERS_TO_START, true);
      assertPrimariesBalanced(LOW_REDUNDANCY_REGION_NAME, SERVERS_TO_START, true);
    }));
  }

  @Test
  public void redundancyIsRecoveredAndPrimariesNotBalancedWhenRestoreRedundancyIsCalledWithReassignPrimariesFalse() {
    servers.get(0).invoke(() -> {
      RestoreRedundancyResults results = restoreRedundancyAndGetResults(null, null, false);

      assertThat(results.getRegionOperationStatus()).isEqualTo(SUCCESS);
      assertThat(results.getTotalPrimaryTransfersCompleted()).isEqualTo(0);
      assertThat(results.getTotalPrimaryTransferTime()).isEqualTo(0L);
      assertThat(results.getRegionResult(PARENT_REGION_NAME).getStatus()).isEqualTo(SATISFIED);
      assertThat(results.getRegionResult(CHILD_REGION_NAME).getStatus()).isEqualTo(SATISFIED);
      assertThat(results.getRegionResult(LOW_REDUNDANCY_REGION_NAME).getStatus())
          .isEqualTo(SATISFIED);
    });

    servers.forEach(s -> s.invoke(() -> {
      assertRedundancyStatus(PARENT_REGION_NAME, true);
      assertRedundancyStatus(CHILD_REGION_NAME, true);
      assertRedundancyStatus(LOW_REDUNDANCY_REGION_NAME, true);
      assertPrimariesBalanced(PARENT_REGION_NAME, SERVERS_TO_START, false);
      assertPrimariesBalanced(CHILD_REGION_NAME, SERVERS_TO_START, false);
      assertPrimariesBalanced(LOW_REDUNDANCY_REGION_NAME, SERVERS_TO_START, false);
    }));
  }

  @Test
  public void redundancyIsNotRecoveredAndPrimariesNotBalancedForExcludedNonColocatedRegion() {
    servers.get(0).invoke(() -> {
      RestoreRedundancyResults results = restoreRedundancyAndGetResults(null,
          Collections.singleton(LOW_REDUNDANCY_REGION_NAME), true);

      assertThat(results.getRegionOperationStatus()).isEqualTo(SUCCESS);
      assertThat(results.getRegionResult(PARENT_REGION_NAME).getStatus()).isEqualTo(SATISFIED);
      assertThat(results.getRegionResult(CHILD_REGION_NAME).getStatus()).isEqualTo(SATISFIED);
      assertThat(results.getRegionResult(LOW_REDUNDANCY_REGION_NAME)).isNull();
    });

    servers.forEach(s -> s.invoke(() -> {
      assertRedundancyStatus(PARENT_REGION_NAME, true);
      assertRedundancyStatus(CHILD_REGION_NAME, true);
      assertRedundancyStatus(LOW_REDUNDANCY_REGION_NAME, false);
      assertPrimariesBalanced(PARENT_REGION_NAME, SERVERS_TO_START, true);
      assertPrimariesBalanced(CHILD_REGION_NAME, SERVERS_TO_START, true);
      assertPrimariesBalanced(LOW_REDUNDANCY_REGION_NAME, SERVERS_TO_START, false);
    }));
  }

  @Test
  @Parameters(method = "getIncludeAndExclude")
  @TestCaseName("[{index}] {method}: Include={0}, Exclude={1}")
  public void redundancyIsRecoveredAndPrimariesBalancedForAllColocatedRegionsWhenAtLeastOneIsIncluded(
      String includeRegion, String excludeRegion) {
    servers.get(0).invoke(() -> {
      Set<String> includeSet = includeRegion == null ? null : Collections.singleton(includeRegion);
      Set<String> excludeSet = excludeRegion == null ? null : Collections.singleton(excludeRegion);

      RestoreRedundancyResults results =
          restoreRedundancyAndGetResults(includeSet, excludeSet, true);

      assertThat(results.getRegionOperationStatus()).isEqualTo(SUCCESS);
      assertThat(results.getRegionResult(PARENT_REGION_NAME).getStatus()).isEqualTo(SATISFIED);
      assertThat(results.getRegionResult(CHILD_REGION_NAME).getStatus()).isEqualTo(SATISFIED);
    });

    servers.forEach(s -> s.invoke(() -> {
      assertRedundancyStatus(PARENT_REGION_NAME, true);
      assertRedundancyStatus(CHILD_REGION_NAME, true);
      assertPrimariesBalanced(PARENT_REGION_NAME, SERVERS_TO_START, true);
      assertPrimariesBalanced(CHILD_REGION_NAME, SERVERS_TO_START, true);
    }));
  }

  @Test
  public void restoringRedundancyWithoutEnoughServersToFullySatisfyRedundancyShouldReturnFailureStatusAndBalancePrimaries() {
    servers.remove(servers.size() - 1).stop();
    int activeServers = servers.size();

    servers.get(0).invoke(() -> {
      RestoreRedundancyResults results = restoreRedundancyAndGetResults(null, null, true);
      assertThat(results.getRegionOperationStatus()).isEqualTo(FAILURE);
      assertThat(results.getRegionResult(PARENT_REGION_NAME).getStatus()).isEqualTo(NOT_SATISFIED);
      assertThat(results.getRegionResult(CHILD_REGION_NAME).getStatus()).isEqualTo(NOT_SATISFIED);
      assertThat(results.getRegionResult(LOW_REDUNDANCY_REGION_NAME).getStatus())
          .isEqualTo(SATISFIED);
    });

    servers.forEach(s -> s.invoke(() -> {
      assertRedundancyStatus(PARENT_REGION_NAME, false);
      assertRedundancyStatus(CHILD_REGION_NAME, false);
      assertRedundancyStatus(LOW_REDUNDANCY_REGION_NAME, true);
      assertPrimariesBalanced(PARENT_REGION_NAME, activeServers, true);
      assertPrimariesBalanced(CHILD_REGION_NAME, activeServers, true);
      assertPrimariesBalanced(LOW_REDUNDANCY_REGION_NAME, activeServers, true);
    }));
  }

  @Test
  public void restoringRedundancyWithoutEnoughServersToCreateAnyRedundantCopyShouldReturnFailureStatus() {
    // Stop the last two servers in the list and remove them from the list, leaving us with one
    // server
    servers.remove(servers.size() - 1).stop();
    servers.remove(servers.size() - 1).stop();

    assertThat(servers.size()).isEqualTo(1);

    servers.get(0).invoke(() -> {
      RestoreRedundancyResults results = restoreRedundancyAndGetResults(null, null, true);
      assertThat(results.getRegionOperationStatus()).isEqualTo(FAILURE);
      assertThat(results.getRegionResult(PARENT_REGION_NAME).getStatus())
          .isEqualTo(NO_REDUNDANT_COPIES);
      assertThat(results.getRegionResult(CHILD_REGION_NAME).getStatus())
          .isEqualTo(NO_REDUNDANT_COPIES);
      assertThat(results.getRegionResult(LOW_REDUNDANCY_REGION_NAME).getStatus())
          .isEqualTo(NO_REDUNDANT_COPIES);
      assertRedundancyStatus(PARENT_REGION_NAME, false);
      assertRedundancyStatus(CHILD_REGION_NAME, false);
      assertRedundancyStatus(LOW_REDUNDANCY_REGION_NAME, false);
    });
  }

  @Test
  public void redundancyStatusReportsCorrectRedundancyInformation() {
    getRedundancyStatusAndAssertIfSatisfied(FAILURE, NO_REDUNDANT_COPIES);

    // Restore redundancy to confirm that the result of the redundancyStatus call has changed to
    // reflect the new state of the system
    servers.get(0).invoke(() -> {
      restoreRedundancyAndGetResults(null, null, true);
    });

    getRedundancyStatusAndAssertIfSatisfied(SUCCESS, SATISFIED);
  }

  private static RestoreRedundancyResults restoreRedundancyAndGetResults(
      Set<String> includeRegions, Set<String> excludeRegions, boolean shouldReassign)
      throws InterruptedException, ExecutionException {
    ResourceManager resourceManager =
        Objects.requireNonNull(ClusterStartupRule.getCache()).getResourceManager();
    CompletableFuture<RestoreRedundancyResults> redundancyOpFuture = resourceManager
        .createRestoreRedundancyOperation()
        .includeRegions(includeRegions)
        .excludeRegions(excludeRegions)
        .shouldReassignPrimaries(shouldReassign)
        .start();
    assertThat(resourceManager.getRestoreRedundancyFutures().size()).isEqualTo(1);
    assertThat(resourceManager.getRestoreRedundancyFutures().contains(redundancyOpFuture)).isTrue();
    return redundancyOpFuture.get();
  }

  private static Collection<Region<Object, Object>> createRegions() {
    Collection<Region<Object, Object>> regions = new HashSet<>();
    PartitionAttributesImpl attributes = getAttributesWithRedundancy(DESIRED_REDUNDANCY_COPIES);
    regions.add(Objects.requireNonNull(ClusterStartupRule.getCache())
        .createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
        .create(PARENT_REGION_NAME));

    attributes.setColocatedWith(PARENT_REGION_NAME);
    regions.add(Objects.requireNonNull(ClusterStartupRule.getCache())
        .createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
        .create(CHILD_REGION_NAME));

    PartitionAttributesImpl lowRedundancyAttributes =
        getAttributesWithRedundancy(LOW_REDUNDANCY_COPIES);
    regions.add(Objects.requireNonNull(ClusterStartupRule.getCache())
        .createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(lowRedundancyAttributes).create(LOW_REDUNDANCY_REGION_NAME));

    return regions;
  }

  private static PartitionAttributesImpl getAttributesWithRedundancy(int desiredRedundancy) {
    PartitionAttributesImpl attributes = new PartitionAttributesImpl();
    attributes.setRedundantCopies(desiredRedundancy);
    attributes.setRecoveryDelay(-1);
    attributes.setStartupRecoveryDelay(-1);
    attributes.setTotalNumBuckets(GLOBAL_MAX_BUCKETS_DEFAULT);
    return attributes;
  }

  private static void assertRedundancyStatus(String regionName, boolean shouldBeSatisfied) {
    Cache cache = Objects.requireNonNull(ClusterStartupRule.getCache());

    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);

    String message =
        "Expecting redundancy to " + (shouldBeSatisfied ? "" : "not") + " be satisfied";
    assertThat(region.getRedundancyProvider().isRedundancyImpaired()).as(message)
        .isEqualTo(!shouldBeSatisfied);
  }

  private static void assertPrimariesBalanced(String regionName, int numberOfServers,
      boolean shouldBeBalanced) {
    Cache cache = Objects.requireNonNull(ClusterStartupRule.getCache());

    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
    int primariesOnServer = region.getLocalPrimaryBucketsListTestOnly().size();
    // Add one to account for integer rounding errors when dividing
    int expectedPrimaries = 1 + GLOBAL_MAX_BUCKETS_DEFAULT / numberOfServers;
    // Because of the way reassigning primaries works, it is sometimes only possible to get the
    // difference between the most loaded member and the least loaded member to be 2, not 1 as would
    // be the case for perfect balance
    String message = "Primaries should be balanced, but expectedPrimaries:actualPrimaries = "
        + expectedPrimaries + ":" + primariesOnServer;
    if (shouldBeBalanced) {
      assertThat(Math.abs(primariesOnServer - expectedPrimaries)).as(message)
          .isLessThanOrEqualTo(2);
    } else {
      assertThat(Math.abs(primariesOnServer - expectedPrimaries))
          .as("Primaries should not be balanced").isGreaterThan(2);
    }
  }

  private void getRedundancyStatusAndAssertIfSatisfied(
      RestoreRedundancyResults.Status expectedResultStatus,
      RegionRedundancyStatus.RedundancyStatus expectedRedundancyStatus) {
    servers.forEach(s -> s.invoke(() -> {
      RestoreRedundancyResults results = Objects.requireNonNull(ClusterStartupRule.getCache())
          .getResourceManager()
          .createRestoreRedundancyOperation()
          .redundancyStatus();
      assertThat(results.getRegionOperationStatus()).isEqualTo(expectedResultStatus);
      assertThat(results.getRegionResult(PARENT_REGION_NAME).getStatus())
          .isEqualTo(expectedRedundancyStatus);
      assertThat(results.getRegionResult(CHILD_REGION_NAME).getStatus())
          .isEqualTo(expectedRedundancyStatus);
      assertThat(results.getRegionResult(LOW_REDUNDANCY_REGION_NAME).getStatus())
          .isEqualTo(expectedRedundancyStatus);
    }));
  }

  @SuppressWarnings("unused")
  private Object[] getIncludeAndExclude() {
    return new Object[] {
        new Object[] {PARENT_REGION_NAME, null},
        new Object[] {CHILD_REGION_NAME, null},
        new Object[] {CHILD_REGION_NAME, PARENT_REGION_NAME},
        new Object[] {PARENT_REGION_NAME, CHILD_REGION_NAME},
        new Object[] {null, PARENT_REGION_NAME},
        new Object[] {null, CHILD_REGION_NAME}
    };
  }
}
