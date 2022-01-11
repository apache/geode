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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.distributed.ConfigurationProperties.REDUNDANCY_ZONE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.internal.cache.PartitionAttributesImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

/**
 * The purpose of RebalanceOperationComplexDistributedTest is to test rebalances
 * across zones and to ensure that enforceUniqueZone behavior of redundancy zones
 * is working correctly.
 */
public class RebalanceOperationComplexPart2DistributedTest
    implements Serializable {

  private static final int EXPECTED_BUCKET_COUNT = 113;
  private static final long TIMEOUT_SECONDS = GeodeAwaitility.getTimeout().getSeconds();
  private static final String REGION_NAME = "primary";

  private static final String ZONE_A = "zoneA";
  private static final String ZONE_B = "zoneB";

  private int locatorPort;

  // 6 servers distributed evenly across 2 zones
  private static Map<Integer, String> SERVER_ZONE_MAP;

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(5);

  @Before
  public void setup() {
    // Start the locator
    MemberVM locatorVM = clusterStartupRule.startLocatorVM(0);
    locatorPort = locatorVM.getPort();

  }


  /**
   * Test that we correctly use the redundancy-zone property to determine where to place redundant
   * copies of a buckets and doesn't allow cross redundancy zone deletes. This does not use a
   * rebalance once the servers are down.
   *
   */
  @Test
  public void testRecoveryWithOneServerPermanentlyDownAndOneRestarted() throws Exception {

    SERVER_ZONE_MAP = new HashMap<>();
    SERVER_ZONE_MAP.put(1, ZONE_A);
    SERVER_ZONE_MAP.put(2, ZONE_A);
    SERVER_ZONE_MAP.put(3, ZONE_B);
    SERVER_ZONE_MAP.put(4, ZONE_B);


    // Startup the servers
    for (Map.Entry<Integer, String> entry : SERVER_ZONE_MAP.entrySet()) {
      startServerInRedundancyZone(entry.getKey(), entry.getValue());
    }

    // Put data in the server regions
    clientPopulateServers();

    // Rebalance Server VM will initiate the rebalances in this test
    VM server2 = clusterStartupRule.getVM(2);

    // Take the server 1 offline
    clusterStartupRule.stop(1, false);

    // Baseline rebalance with everything up
    server2.invoke(() -> doRebalance(ClusterStartupRule.getCache().getResourceManager()));

    // Stop server 3 never to start again.
    clusterStartupRule.stop(3, true);

    // Restart the server 1
    startServerInRedundancyZone(1, SERVER_ZONE_MAP.get(1));

    // Rebalance with remaining servers up
    server2.invoke(() -> doRebalance(ClusterStartupRule.getCache().getResourceManager()));

    // print the bucket count on server 4 for debug. Should be 113 because server 3 is still down
    assertThat(getBucketCount(4)).isEqualTo(EXPECTED_BUCKET_COUNT);

    assertThat(getBucketCount(1)).isGreaterThanOrEqualTo(EXPECTED_BUCKET_COUNT / 2)
        .isLessThanOrEqualTo(EXPECTED_BUCKET_COUNT / 2 + 1);

    assertThat(getBucketCount(2)).isGreaterThanOrEqualTo(EXPECTED_BUCKET_COUNT / 2)
        .isLessThanOrEqualTo(EXPECTED_BUCKET_COUNT / 2 + 1);

    // Verify that all bucket counts add up to what they should
    int zoneABucketCount = getZoneABucketCount();
    assertThat(zoneABucketCount).isEqualTo(EXPECTED_BUCKET_COUNT);
  }

  private int getBucketCount(int server) {
    return clusterStartupRule.getVM(server).invoke(() -> {
      PartitionedRegion region =
          (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
      return region.getLocalBucketsListTestOnly().size();
    });
  }



  /**
   * Startup a client to put all the data in the server regions
   */
  protected void clientPopulateServers() throws Exception {
    Properties properties2 = new Properties();
    ClientVM clientVM =
        clusterStartupRule.startClientVM(SERVER_ZONE_MAP.size() + 1, properties2,
            ccf -> ccf.addPoolLocator("localhost", locatorPort));

    clientVM.invoke(() -> {

      Map<Integer, String> putMap = new HashMap<>();
      for (int i = 0; i < 1000; i++) {
        putMap.put(i, "A");
      }

      ClientCache clientCache = ClusterStartupRule.getClientCache();
      ClientRegionFactory<Object, Object> clientRegionFactory =
          clientCache.createClientRegionFactory(
              ClientRegionShortcut.PROXY);
      clientRegionFactory.create(REGION_NAME);
      Region<Integer, String> region = clientCache.getRegion(REGION_NAME);
      region.putAll(putMap);
    });
  }

  /**
   * Startup server *index* in *redundancy zone*
   *
   * @param index - server
   * @param zone - Redundancy zone for the server to be started in
   */
  protected void startServerInRedundancyZone(int index, final String zone) {

    clusterStartupRule.startServerVM(index, s -> s
        .withProperty(REDUNDANCY_ZONE, zone)
        .withConnectionToLocator(locatorPort));

    VM.getVM(index).invoke(() -> {
      RegionFactory<Object, Object> regionFactory =
          ClusterStartupRule.getCache().createRegionFactory(
              RegionShortcut.PARTITION_REDUNDANT);
      PartitionAttributesImpl partitionAttributesImpl = new PartitionAttributesImpl();
      partitionAttributesImpl.setRedundantCopies(1);
      partitionAttributesImpl.setStartupRecoveryDelay(-1);
      partitionAttributesImpl.setRecoveryDelay(-1);
      partitionAttributesImpl.setTotalNumBuckets(EXPECTED_BUCKET_COUNT);
      regionFactory.setPartitionAttributes(partitionAttributesImpl);
      regionFactory.create(REGION_NAME);

    });
  }

  /**
   * Trigger a rebalance of buckets
   *
   */
  protected void doRebalance(ResourceManager manager)
      throws TimeoutException, InterruptedException {
    manager.createRebalanceFactory()
        .start().getResults(TIMEOUT_SECONDS, SECONDS);
    assertThat(manager.getRebalanceOperations()).isEmpty();
  }


  /**
   * Get the bucket count for the region in the redundancy zone
   *
   * @return - the total bucket count for the region in the redundancy zone
   */
  protected int getZoneABucketCount() {
    int bucketCount = 0;
    for (Map.Entry<Integer, String> entry : SERVER_ZONE_MAP.entrySet()) {
      if (entry.getValue().compareTo(RebalanceOperationComplexPart2DistributedTest.ZONE_A) == 0) {
        bucketCount += getBucketCount(entry.getKey());
      }
    }
    return bucketCount;
  }
}
