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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import junitparams.JUnitParamsRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.control.RebalanceResults;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

/**
 * TODO test colocated regions where buckets aren't created for all subregions
 *
 * <p>
 * TODO test colocated regions where members aren't consistent in which regions they have
 */
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class RebalanceOperationComplexDistributedTest extends CacheTestCase {

  private static final long TIMEOUT_SECONDS = GeodeAwaitility.getTimeout().getValue();
  public static final String CLIENT_XML =
      "RebalanceOperationComplex-client.xml";
  public static final String SERVER_XML =
      "RebalanceOperationComplex-server.xml";
  public static final String REGION_NAME = "primary";
  public static final String COLOCATED_REGION_NAME = "colocated";

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(8);

  private MemberVM startServer(int index, String zone, int locatorPort) {
    return clusterStartupRule.startServerVM(index, s -> s.withProperty("cache-xml-file",
        SERVER_XML)
        .withProperty(REDUNDANCY_ZONE, zone)
        .withConnectionToLocator(locatorPort));
  }

  /**
   * Test that we correctly use the redundancy-zone property to determine where to place redundant
   * copies of a buckets and doesn't allow cross redundancy zone deletes.
   */
  @Test
  public void testEnforceZoneWithSixServersAndTwoZones() throws Exception {
    MemberVM locatorVM = clusterStartupRule.startLocatorVM(0);

    // So startup six servers 3 in each redundancy zone
    MemberVM server1 = startServer(1, "zoneA", locatorVM.getPort());
    startServer(2, "zoneA", locatorVM.getPort());
    startServer(3, "zoneA", locatorVM.getPort());
    startServer(4, "zoneB", locatorVM.getPort());
    startServer(5, "zoneB", locatorVM.getPort());
    startServer(6, "zoneB", locatorVM.getPort());


    // startup a client to put all the data.
    Properties properties2 = new Properties();
    properties2.setProperty("cache-xml-file",
        CLIENT_XML);
    ClientVM clientVM =
        clusterStartupRule.startClientVM(7, properties2,
            ccf -> ccf.addPoolLocator("localhost", locatorVM.getPort()));

    clientVM.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Map<Integer, String> map = new HashMap<>();
      for (int i = 0; i < 1000; i++) {
        map.put(i, "A");
      }
      Region<Integer, String> region =
          clientCache.getRegion(REGION_NAME);
      region.putAll(map);
      Region<Integer, String> region2 =
          clientCache.getRegion(COLOCATED_REGION_NAME);
      region2.putAll(map);
    });

    // Baseline rebalance with everything up
    server1.invoke(() -> {
      doRebalance(false, ClusterStartupRule.getCache().getResourceManager());
    });

    // knock server 2 offline
    clusterStartupRule.stop(2);

    // rebalance so that now all the buckets are one server 1 and server 3 reduncancy zone b servers
    // should not be touched.
    server1.invoke(() -> {
      doRebalance(false, ClusterStartupRule.getCache().getResourceManager());
    });

    // bring server 2 backonline
    startServer(2, "zoneA", locatorVM.getPort());

    // do another rebalance to make sure all the buckets are distributed evenly(ish) and there is no
    // cross redundancy zone bucket deletions.
    server1.invoke(() -> {
      doRebalance(false, ClusterStartupRule.getCache().getResourceManager());
    });

    // Verify that all bucket counts add up to what they should
    compareZoneBucketCounts(COLOCATED_REGION_NAME);
    compareZoneBucketCounts(REGION_NAME);
  }

  private void compareZoneBucketCounts(final String regionName) {
    int zoneA = clusterStartupRule.getVM(1).invoke(() -> getBucketCount(regionName));
    zoneA += clusterStartupRule.getVM(2).invoke(() -> getBucketCount(regionName));
    zoneA += clusterStartupRule.getVM(3).invoke(() -> getBucketCount(regionName));

    int zoneB = clusterStartupRule.getVM(4).invoke(() -> getBucketCount(regionName));
    zoneB += clusterStartupRule.getVM(5).invoke(() -> getBucketCount(regionName));
    zoneB += clusterStartupRule.getVM(6).invoke(() -> getBucketCount(regionName));

    assertThat(zoneA).isEqualTo(zoneB).isEqualTo(113);
  }

  private RebalanceResults doRebalance(boolean simulate, ResourceManager manager)
      throws TimeoutException, InterruptedException {
    return doRebalance(simulate, manager, null, null);
  }

  private RebalanceResults doRebalance(boolean simulate, ResourceManager manager,
      Set<String> includes, Set<String> excludes)
      throws TimeoutException, InterruptedException {
    RebalanceResults results;
    if (simulate) {
      results = manager.createRebalanceFactory().includeRegions(includes).excludeRegions(excludes)
          .simulate().getResults(TIMEOUT_SECONDS, SECONDS);
    } else {
      results = manager.createRebalanceFactory().includeRegions(includes).excludeRegions(excludes)
          .start().getResults(TIMEOUT_SECONDS, SECONDS);
    }
    assertThat(manager.getRebalanceOperations()).isEmpty();
    return results;
  }

  private int getBucketCount(String regionName) {
    PartitionedRegion region =
        (PartitionedRegion) ClusterStartupRule.getCache().getRegion(regionName);
    return region.getLocalBucketsListTestOnly().size();
  }
}
