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
import static org.apache.geode.cache.control.RestoreRedundancyResults.Status.FAILURE;
import static org.apache.geode.cache.control.RestoreRedundancyResults.Status.SUCCESS;
import static org.apache.geode.internal.cache.control.RestoreRedundancyRegionResult.RedundancyStatus.NOT_SATISFIED;
import static org.apache.geode.internal.cache.control.RestoreRedundancyRegionResult.RedundancyStatus.NO_REDUNDANT_COPIES;
import static org.apache.geode.internal.cache.control.RestoreRedundancyRegionResult.RedundancyStatus.SATISFIED;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

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
import org.apache.geode.cache.control.RestoreRedundancyResults;
import org.apache.geode.internal.cache.PartitionAttributesImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

@RunWith(JUnitParamsRunner.class)
public class RestoreRedundancyOperationDUnitTest {
  private List<MemberVM> servers;
  private static final int serversToStart = 3;
  private static final String parentRegionName = "parentColocatedRegion";
  private static final String childRegionName = "childColocatedRegion";
  private static final int desiredRedundancyCopies = 2;
  private static final String lowRedundancyRegionName = "lowRedundancyRegion";
  private static final int lowRedundancyCopies = 1;
  private static final int numBuckets = GLOBAL_MAX_BUCKETS_DEFAULT;
  private static final int entries = 5 * numBuckets;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Before
  public void startUp() {
    MemberVM locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    servers = new ArrayList<>();
    IntStream.range(0, serversToStart)
        .forEach(i -> servers.add(cluster.startServerVM(i + 1, locatorPort)));

    // Create the regions on server1 and populate with data
    servers.get(0).invoke(() -> {
      Collection<Region<Object, Object>> regions = createRegions();
      regions.forEach(
          region -> IntStream.range(0, entries).forEach(i -> region.put("key" + i, "value" + i)));
    });

    // Create regions on other servers but do not populate with data
    servers.stream().skip(1).forEach(
        s -> s.invoke((SerializableRunnableIF) RestoreRedundancyOperationDUnitTest::createRegions));

    // Confirm that redundancy is impaired and primaries unbalanced for all regions on all members
    servers.forEach(s -> s.invoke(() -> {
      assertRedundancyStatus(parentRegionName, false);
      assertRedundancyStatus(childRegionName, false);
      assertRedundancyStatus(lowRedundancyRegionName, false);
      assertPrimariesBalanced(parentRegionName, serversToStart, false);
      assertPrimariesBalanced(childRegionName, serversToStart, false);
      assertPrimariesBalanced(lowRedundancyRegionName, serversToStart, false);
    }));
  }

  @Test
  public void statsAreUpdatedWhenRestoreRedundnacyIsCalled() {
    servers.get(0).invoke(() -> {
      restoreRedundancyAndGetResults(null, null, false);

      ResourceManagerStats stats = Objects.requireNonNull(ClusterStartupRule.getCache())
          .getInternalResourceManager().getStats();

      assertThat(stats.getRestoreRedundanciesInProgress(), equalTo(0L));
      assertThat(stats.getRestoreRedundanciesCompleted(), equalTo(1L));
      assertThat(stats.getRestoreRedundancyTime(), greaterThan(0L));
    });
  }

  @Test
  public void redundancyIsRecoveredAndPrimariesBalancedWhenRestoreRedundancyIsCalledWithNoIncludedOrExcludedRegions() {
    servers.get(0).invoke(() -> {
      RestoreRedundancyResults results = restoreRedundancyAndGetResults(null, null, false);
      assertThat(results.getStatus(), is(SUCCESS));
      assertThat(results.getTotalPrimaryTransfersCompleted() > 0, is(true));
      assertThat(results.getTotalPrimaryTransferTime() > 0, is(true));
      assertThat(results.getRegionResult(parentRegionName).getStatus(), is(SATISFIED));
      assertThat(results.getRegionResult(childRegionName).getStatus(), is(SATISFIED));
      assertThat(results.getRegionResult(lowRedundancyRegionName).getStatus(), is(SATISFIED));
    });

    servers.forEach(s -> s.invoke(() -> {
      assertRedundancyStatus(parentRegionName, true);
      assertRedundancyStatus(childRegionName, true);
      assertRedundancyStatus(lowRedundancyRegionName, true);
      assertPrimariesBalanced(parentRegionName, serversToStart, true);
      assertPrimariesBalanced(childRegionName, serversToStart, true);
      assertPrimariesBalanced(lowRedundancyRegionName, serversToStart, true);
    }));
  }

  @Test
  public void redundancyIsRecoveredAndPrimariesNotBalancedWhenRestoreRedundancyIsCalledWithDoNotReassignPrimariesTrue() {
    servers.get(0).invoke(() -> {
      RestoreRedundancyResults results = restoreRedundancyAndGetResults(null, null, true);

      assertThat(results.getStatus(), is(SUCCESS));
      assertThat(results.getTotalPrimaryTransfersCompleted(), is(0));
      assertThat(results.getTotalPrimaryTransferTime(), is(0L));
      assertThat(results.getRegionResult(parentRegionName).getStatus(), is(SATISFIED));
      assertThat(results.getRegionResult(childRegionName).getStatus(), is(SATISFIED));
      assertThat(results.getRegionResult(lowRedundancyRegionName).getStatus(), is(SATISFIED));
    });

    servers.forEach(s -> s.invoke(() -> {
      assertRedundancyStatus(parentRegionName, true);
      assertRedundancyStatus(childRegionName, true);
      assertRedundancyStatus(lowRedundancyRegionName, true);
      assertPrimariesBalanced(parentRegionName, serversToStart, false);
      assertPrimariesBalanced(childRegionName, serversToStart, false);
      assertPrimariesBalanced(lowRedundancyRegionName, serversToStart, false);
    }));
  }

  @Test
  public void redundancyIsNotRecoveredAndPrimariesNotBalancedForExcludedNonColocatedRegion() {
    servers.get(0).invoke(() -> {
      RestoreRedundancyResults results = restoreRedundancyAndGetResults(null,
          Collections.singleton(lowRedundancyRegionName), false);

      assertThat(results.getStatus(), is(SUCCESS));
      assertThat(results.getRegionResult(parentRegionName).getStatus(), is(SATISFIED));
      assertThat(results.getRegionResult(childRegionName).getStatus(), is(SATISFIED));
      assertThat(results.getRegionResult(lowRedundancyRegionName), nullValue());
    });

    servers.forEach(s -> s.invoke(() -> {
      assertRedundancyStatus(parentRegionName, true);
      assertRedundancyStatus(childRegionName, true);
      assertRedundancyStatus(lowRedundancyRegionName, false);
      assertPrimariesBalanced(parentRegionName, serversToStart, true);
      assertPrimariesBalanced(childRegionName, serversToStart, true);
      assertPrimariesBalanced(lowRedundancyRegionName, serversToStart, false);
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
          restoreRedundancyAndGetResults(includeSet, excludeSet, false);

      assertThat(results.getStatus(), is(SUCCESS));
      assertThat(results.getRegionResult(parentRegionName).getStatus(), is(SATISFIED));
      assertThat(results.getRegionResult(childRegionName).getStatus(), is(SATISFIED));
    });

    servers.forEach(s -> s.invoke(() -> {
      assertRedundancyStatus(parentRegionName, true);
      assertRedundancyStatus(childRegionName, true);
      assertPrimariesBalanced(parentRegionName, serversToStart, true);
      assertPrimariesBalanced(childRegionName, serversToStart, true);
    }));
  }

  @Test
  public void restoringRedundancyWithEnoughServersToCreateAtLeastOneRedundantCopyShouldReturnSuccessStatusAndBalancePrimaries() {
    servers.remove(servers.size() - 1).stop();
    int activeServers = servers.size();

    servers.get(0).invoke(() -> {
      RestoreRedundancyResults results = restoreRedundancyAndGetResults(null, null, false);
      assertThat(results.getStatus(), is(SUCCESS));
      assertThat(results.getRegionResult(parentRegionName).getStatus(), is(NOT_SATISFIED));
      assertThat(results.getRegionResult(childRegionName).getStatus(), is(NOT_SATISFIED));
      assertThat(results.getRegionResult(lowRedundancyRegionName).getStatus(), is(SATISFIED));
    });

    servers.forEach(s -> s.invoke(() -> {
      assertRedundancyStatus(parentRegionName, false);
      assertRedundancyStatus(childRegionName, false);
      assertRedundancyStatus(lowRedundancyRegionName, true);
      assertPrimariesBalanced(parentRegionName, activeServers, true);
      assertPrimariesBalanced(childRegionName, activeServers, true);
      assertPrimariesBalanced(lowRedundancyRegionName, activeServers, true);
    }));
  }

  @Test
  public void restoringRedundancyWithoutEnoughServersToCreateOneRedundancyCopyShouldReturnFailureStatus() {
    // Stop the last two servers in the list and remove them from the list, leaving us with one
    // server
    servers.remove(servers.size() - 1).stop();
    servers.remove(servers.size() - 1).stop();

    assertThat(servers.size(), is(1));

    servers.get(0).invoke(() -> {
      RestoreRedundancyResults results = restoreRedundancyAndGetResults(null, null, false);
      assertThat(results.getStatus(), is(FAILURE));
      assertThat(results.getRegionResult(parentRegionName).getStatus(), is(NO_REDUNDANT_COPIES));
      assertThat(results.getRegionResult(childRegionName).getStatus(), is(NO_REDUNDANT_COPIES));
      assertThat(results.getRegionResult(lowRedundancyRegionName).getStatus(),
          is(NO_REDUNDANT_COPIES));
      assertRedundancyStatus(parentRegionName, false);
      assertRedundancyStatus(childRegionName, false);
      assertRedundancyStatus(lowRedundancyRegionName, false);
    });
  }

  private static RestoreRedundancyResults restoreRedundancyAndGetResults(
      Set<String> includeRegions, Set<String> excludeRegions, boolean doNotReassignPrimaries)
      throws InterruptedException, ExecutionException {
    ResourceManager resourceManager =
        Objects.requireNonNull(ClusterStartupRule.getCache()).getResourceManager();
    CompletableFuture<RestoreRedundancyResults> redundancyOpFuture = resourceManager
        .createRestoreRedundancyBuilder()
        .includeRegions(includeRegions)
        .excludeRegions(excludeRegions)
        .doNotReassignPrimaries(doNotReassignPrimaries)
        .start();
    assertThat(resourceManager.getRestoreRedundancyOperations().size(), is(1));
    assertThat(resourceManager.getRestoreRedundancyOperations().contains(redundancyOpFuture),
        is(true));
    return redundancyOpFuture.get();
  }

  private static Collection<Region<Object, Object>> createRegions() {
    Collection<Region<Object, Object>> regions = new HashSet<>();
    PartitionAttributesImpl attributes = getAttributesWithRedundancy(desiredRedundancyCopies);
    regions.add(Objects.requireNonNull(ClusterStartupRule.getCache())
        .createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
        .create(parentRegionName));

    attributes.setColocatedWith(parentRegionName);
    regions.add(Objects.requireNonNull(ClusterStartupRule.getCache())
        .createRegionFactory(RegionShortcut.PARTITION).setPartitionAttributes(attributes)
        .create(childRegionName));

    PartitionAttributesImpl lowRedundancyAttributes =
        getAttributesWithRedundancy(lowRedundancyCopies);
    regions.add(Objects.requireNonNull(ClusterStartupRule.getCache())
        .createRegionFactory(RegionShortcut.PARTITION)
        .setPartitionAttributes(lowRedundancyAttributes).create(lowRedundancyRegionName));

    return regions;
  }

  private static PartitionAttributesImpl getAttributesWithRedundancy(int desiredRedundancy) {
    PartitionAttributesImpl attributes = new PartitionAttributesImpl();
    attributes.setRedundantCopies(desiredRedundancy);
    attributes.setRecoveryDelay(-1);
    attributes.setStartupRecoveryDelay(-1);
    attributes.setTotalNumBuckets(numBuckets);
    return attributes;
  }

  private static void assertRedundancyStatus(String regionName, boolean shouldBeSatisfied) {
    Cache cache = Objects.requireNonNull(ClusterStartupRule.getCache());

    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);

    String message =
        "Expecting redundancy to " + (shouldBeSatisfied ? "" : "not") + " be satisfied";
    assertThat(message, region.getRedundancyProvider().isRedundancyImpaired(),
        is(!shouldBeSatisfied));
  }

  private static void assertPrimariesBalanced(String regionName, int numberOfServers,
      boolean shouldBeBalanced) {
    Cache cache = Objects.requireNonNull(ClusterStartupRule.getCache());

    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
    int primariesOnServer = region.getLocalPrimaryBucketsListTestOnly().size();
    // Add one to account for integer rounding errors when dividing
    int expectedPrimaries = 1 + (numBuckets / numberOfServers);
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

  @SuppressWarnings("unused")
  private Object[] getIncludeAndExclude() {
    return new Object[] {
        new Object[] {parentRegionName, null},
        new Object[] {childRegionName, null},
        new Object[] {childRegionName, parentRegionName},
        new Object[] {parentRegionName, childRegionName},
        new Object[] {null, parentRegionName},
        new Object[] {null, childRegionName}
    };
  }
}
