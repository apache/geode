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

import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.junit.After;
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
import org.apache.geode.logging.internal.log4j.api.LogService;
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
  private static final Logger logger = LogService.getLogger();

  private static final String ZONE_A = "zoneA";
  private static final String ZONE_B = "zoneB";

  private int locatorPort;
  private static final AtomicInteger runID = new AtomicInteger(0);
  private String workingDir;

  // 6 servers distributed evenly across 2 zones
  private static Map<Integer, String> SERVER_ZONE_MAP;

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(5);

  @Before
  public void setup() {
    // Start the locator
    MemberVM locatorVM = clusterStartupRule.startLocatorVM(0);
    locatorPort = locatorVM.getPort();

    workingDir = clusterStartupRule.getWorkingDirRoot().getAbsolutePath();

    runID.incrementAndGet();
  }

  @After
  public void after() {
    stopServersAndDeleteDirectories();
  }

  protected void stopServersAndDeleteDirectories() {
    for (Map.Entry<Integer, String> entry : SERVER_ZONE_MAP.entrySet()) {
      clusterStartupRule.stop(entry.getKey(), true);
    }
    cleanOutServerDirectories();
  }

  /**
   * Test that we correctly use the redundancy-zone property to determine where to place redundant
   * copies of a buckets and doesn't allow cross redundancy zone deletes. This does not use a
   * rebalance once the servers are down.
   *
   */
  @Test
  public void testRecoveryWithOneServerPermanentlyDownAndOneRestarted() throws Exception {

    SERVER_ZONE_MAP = new HashMap<Integer, String>() {
      {
        put(1, ZONE_A);
        put(2, ZONE_A);
        put(3, ZONE_B);
        put(4, ZONE_B);
      }
    };

    cleanOutServerDirectories();

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
    assertThat(getBucketCount(4)).isEqualTo(113);

    assertThat(getBucketCount(1)).isGreaterThanOrEqualTo(56).isLessThanOrEqualTo(57);

    assertThat(getBucketCount(2)).isGreaterThanOrEqualTo(56).isLessThanOrEqualTo(57);

    // Verify that all bucket counts add up to what they should
    int zoneABucketCount = getZoneBucketCount(REGION_NAME, ZONE_A);
    assertThat(zoneABucketCount).isEqualTo(EXPECTED_BUCKET_COUNT);
  }

  private int getBucketCount(int server) {
    return clusterStartupRule.getVM(server).invoke(() -> {
      PartitionedRegion region =
          (PartitionedRegion) ClusterStartupRule.getCache().getRegion(REGION_NAME);
      return region.getLocalBucketsListTestOnly().size();
    });
  }

  protected void cleanOutServerDirectory(int server) {
    VM.getVM(server).invoke(() -> {
      String path = workingDir + "/" + "runId-" + runID.get() + "-vm-" + server;
      File temporaryDirectory = new File(path);
      if (temporaryDirectory.exists()) {
        try {
          Arrays.stream(temporaryDirectory.listFiles()).forEach(FileUtils::deleteQuietly);
          Files.delete(temporaryDirectory.toPath());
        } catch (Exception exception) {
          logger.error("The delete of files or directory failed ", exception);
          throw exception;
        }
      }
    });
  }

  protected void cleanOutServerDirectories() {
    for (Map.Entry<Integer, String> entry : SERVER_ZONE_MAP.entrySet()) {
      cleanOutServerDirectory(entry.getKey());
    }
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
              RegionShortcut.PARTITION_REDUNDANT_PERSISTENT);
      PartitionAttributesImpl partitionAttributesImpl = new PartitionAttributesImpl();
      partitionAttributesImpl.setRedundantCopies(1);
      partitionAttributesImpl.setStartupRecoveryDelay(-1);
      partitionAttributesImpl.setRecoveryDelay(-1);
      partitionAttributesImpl.setTotalNumBuckets(113);
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
   * @param regionName - name of the region to get the bucket count of
   * @param zoneName - redundancy zone for which to get the bucket count
   * @return - the total bucket count for the region in the redundancy zone
   */
  protected int getZoneBucketCount(String regionName, String zoneName) {
    int bucketCount = 0;
    for (Map.Entry<Integer, String> entry : SERVER_ZONE_MAP.entrySet()) {
      if (entry.getValue().compareTo(zoneName) == 0) {
        bucketCount +=
            clusterStartupRule.getVM(entry.getKey()).invoke(() -> {
              PartitionedRegion region =
                  (PartitionedRegion) ClusterStartupRule.getCache().getRegion(regionName);

              return region.getLocalBucketsListTestOnly().size();
            });
      }
    }
    return bucketCount;
  }
}
