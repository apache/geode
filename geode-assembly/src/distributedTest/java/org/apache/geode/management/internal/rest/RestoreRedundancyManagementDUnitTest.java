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
 *
 */

package org.apache.geode.management.internal.rest;

import static org.apache.geode.cache.PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionAttributesImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.management.api.ClusterManagementOperationResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.operation.RestoreRedundancyRequest;
import org.apache.geode.management.runtime.RegionRedundancyStatus;
import org.apache.geode.management.runtime.RestoreRedundancyResults;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.MemberStarterRule;

/**
 * This class borrows very heavily from the RestoreRedundancyCommandDUnitTest
 *
 */

public class RestoreRedundancyManagementDUnitTest {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  private MemberVM locator1;
  private List<MemberVM> servers;
  private static final int SERVERS_TO_START = 3;
  private static final String HIGH_REDUNDANCY_REGION_NAME = "highRedundancy";
  private static final int HIGH_REDUNDANCY_COPIES = SERVERS_TO_START - 1;
  private static final String LOW_REDUNDANCY_REGION_NAME = "lowRedundancy";
  private static final String PARENT_REGION_NAME = "colocatedParent";
  private static final String CHILD_REGION_NAME = "colocatedChild";
  private static final int SINGLE_REDUNDANT_COPY = 1;
  private static final String NO_CONFIGURED_REDUNDANCY_REGION_NAME = "noConfiguredRedundancy";

  private ClusterManagementService client1;
  private ClusterManagementService client2;

  @Before
  public void setup() {
    locator1 = cluster.startLocatorVM(0, MemberStarterRule::withHttpService);
    int locator1Port = locator1.getPort();
    MemberVM locator2 = cluster.startLocatorVM(1,
        l -> l.withHttpService().withConnectionToLocator(locator1Port));
    servers = new ArrayList<>();
    int locatorPort = locator1.getPort();
    IntStream.range(0, SERVERS_TO_START)
        .forEach(i -> servers.add(cluster.startServerVM(i + 2, locatorPort)));

    client1 = new ClusterManagementServiceBuilder()
        .setHost("localhost")
        .setPort(locator1.getHttpPort())
        .build();
    client2 = new ClusterManagementServiceBuilder()
        .setHost("localhost")
        .setPort(locator2.getHttpPort())
        .build();
  }

  @After
  public void tearDown() {
    client1.close();
    client2.close();
  }

  @Test
  public void restoreRedundancyWithNoArgumentsRestoresRedundancyForAllRegions()
      throws ExecutionException, InterruptedException {

    List<String> regionNames = getAllRegionNames();
    createAndPopulateRegions(regionNames);

    int numberOfServers = servers.size();
    regionNames.forEach(region -> locator1
        .waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + region, numberOfServers));

    RestoreRedundancyRequest restoreRedundancyRequest = new RestoreRedundancyRequest();

    restoreRedundancyRequest.setIncludeRegions(regionNames);

    verifyClusterManagementOperationRequestAndResponse(restoreRedundancyRequest, client1, client1);

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
  public void canReadRestoreRedundancyResultFromDifferentLocator()
      throws ExecutionException, InterruptedException {

    List<String> regionNames = getAllRegionNames();
    createAndPopulateRegions(regionNames);

    int numberOfServers = servers.size();
    regionNames.forEach(region -> locator1
        .waitUntilRegionIsReadyOnExactlyThisManyServers(SEPARATOR + region, numberOfServers));

    RestoreRedundancyRequest restoreRedundancyRequest = new RestoreRedundancyRequest();

    restoreRedundancyRequest.setIncludeRegions(regionNames);

    // Perform the operation on locator1 and use a client connected to locator2 to get the result
    verifyClusterManagementOperationRequestAndResponse(restoreRedundancyRequest, client1, client2);

    // Confirm all regions have their configured redundancy and that primaries were balanced
    int numberOfActiveServers = servers.size();
    servers.get(0).invoke(() -> {
      for (String regionName : regionNames) {
        assertRedundancyStatusForRegion(regionName, true);
        assertPrimariesBalanced(regionName, numberOfActiveServers, true);
      }
    });
  }

  // Helper methods
  private void verifyClusterManagementOperationRequestAndResponse(
      RestoreRedundancyRequest restoreRedundancyRequest, ClusterManagementService startClient,
      ClusterManagementService readResultClient)
      throws InterruptedException, ExecutionException {
    ClusterManagementOperationResult<RestoreRedundancyRequest, RestoreRedundancyResults> startResult =
        startClient.start(restoreRedundancyRequest);

    assertThat(startResult.isSuccessful()).isTrue();

    ClusterManagementOperationResult<RestoreRedundancyRequest, RestoreRedundancyResults> endResult =
        readResultClient.getFuture(restoreRedundancyRequest, startResult.getOperationId()).get();
    RestoreRedundancyResults restoreRedundancyResult = endResult.getOperationResult();

    assertThat(restoreRedundancyResult.getSuccess()).isTrue();

    boolean found;
    for (String regionName : restoreRedundancyRequest.getIncludeRegions()) {
      found = false;
      for (Map.Entry<String, RegionRedundancyStatus> region : restoreRedundancyResult
          .getSatisfiedRedundancyRegionResults().entrySet()) {
        if (region.getValue().getRegionName().compareTo(regionName) == 0) {
          found = true;
          break;
        }
      }
      assertThat(found)
          .describedAs("Satisfied Redundancy List contains region name is true for included")
          .isTrue();
    }

    for (String regionName : restoreRedundancyRequest.getIncludeRegions()) {
      for (Map.Entry<String, RegionRedundancyStatus> region : restoreRedundancyResult
          .getUnderRedundancyRegionResults().entrySet()) {
        assertThat(regionName)
            .describedAs("One of regions we expect to be satisfied is marked as Under Redundancy")
            .isNotEqualTo(region.getValue().getRegionName());
      }
    }

    for (String regionName : restoreRedundancyRequest.getIncludeRegions()) {
      for (Map.Entry<String, RegionRedundancyStatus> region : restoreRedundancyResult
          .getZeroRedundancyRegionResults().entrySet()) {
        assertThat(regionName)
            .describedAs("One of regions we expect to be satisfied is marked as Zero Redundancy")
            .isNotEqualTo(region.getValue().getRegionName());
      }
    }

    List<String> filteredExclude;

    if (restoreRedundancyRequest.getExcludeRegions() != null) {
      filteredExclude = new ArrayList<>(restoreRedundancyRequest.getExcludeRegions());
    } else {
      filteredExclude = new ArrayList<>();
    }

    for (String regionName : restoreRedundancyRequest.getIncludeRegions()) {
      filteredExclude.remove(regionName);
    }

    // Testing for the absence of the region name...
    for (String regionName : filteredExclude) {
      found = false;
      for (Map.Entry<String, RegionRedundancyStatus> region : restoreRedundancyResult
          .getSatisfiedRedundancyRegionResults().entrySet()) {
        if (region.getValue().getRegionName().compareTo(regionName) == 0) {
          found = true;
          break;
        }
      }
      assertThat(found)
          .describedAs("Satisfied Redundancy List contains region name is false for excluded")
          .isFalse();
    }
  }

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

  private static void assertRedundancyStatusForRegion(String regionName,
      boolean shouldBeSatisfied) {
    // Redundancy is always satisfied for a region with zero configured redundancy
    if (regionName.equals(NO_CONFIGURED_REDUNDANCY_REGION_NAME)) {
      return;
    }
    Cache cache = Objects.requireNonNull(ClusterStartupRule.getCache());

    PartitionedRegion region = (PartitionedRegion) cache.getRegion(regionName);
    assertThat(region.getRedundancyProvider().isRedundancyImpaired())
        .isNotEqualTo(shouldBeSatisfied);
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
      assertThat(Math.abs(primariesOnServer - expectedPrimaries)).as(message)
          .isLessThanOrEqualTo(2);
    } else {
      assertThat(Math.abs(primariesOnServer - expectedPrimaries))
          .as("Primaries should not be balanced for region " + regionName).isGreaterThan(2);
    }
  }
}
