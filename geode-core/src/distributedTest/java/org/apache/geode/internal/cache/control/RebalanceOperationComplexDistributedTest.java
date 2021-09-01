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
import static org.apache.geode.internal.lang.SystemPropertyHelper.DEFAULT_DISK_DIRS_PROPERTY;
import static org.apache.geode.internal.lang.SystemPropertyHelper.GEODE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.RemoteInvoker;

/**
 * The purpose of RebalanceOperationComplexDistributedTest is to test rebalances
 * across zones and to ensure that enforceUniqueZone behavior of redundancy zones
 * is working correctly.
 */
@RunWith(JUnitParamsRunner.class)
public class RebalanceOperationComplexDistributedTest extends CacheTestCase {
  public static final int EXPECTED_BUCKET_COUNT = 113;
  public static final long TIMEOUT_SECONDS = GeodeAwaitility.getTimeout().getValue();
  public static final String CLIENT_XML = "RebalanceOperationComplex-client.xml";
  public static final String SERVER_XML = "RebalanceOperationComplex-server.xml";
  public static final String REGION_NAME = "primary";
  public static final String COLOCATED_REGION_NAME = "colocated";

  public static final String ZONE_A = "zoneA";
  public static final String ZONE_B = "zoneB";
  private final RemoteInvoker remoteInvoker = new RemoteInvoker();
  public int locatorPort;
  public int runID;
  private File temporaryDirectory;
  // 6 servers distributed evenly across 2 zones
  public static final Map<Integer, String> SERVER_ZONE_MAP = new HashMap<Integer, String>() {
    {
      put(1, ZONE_A);
      put(2, ZONE_A);
      put(3, ZONE_A);
      put(4, ZONE_B);
      put(5, ZONE_B);
      put(6, ZONE_B);
    }
  };

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(8);

  @Before
  public void setup() throws Exception {
  // Start the locator
    MemberVM locatorVM = clusterStartupRule.startLocatorVM(0);
    locatorPort = locatorVM.getPort();

    // configure the servers
    setPersistenceDirectoriesOnServerVMs();

    // Startup the servers
    for (Map.Entry<Integer, String> entry : SERVER_ZONE_MAP.entrySet()) {
      startServerInRedundancyZone(entry.getKey(), entry.getValue());
    }

    // Put data in the server regions
    clientPopulateServers();
  }

  @After
  public void after() throws IOException {
    for (Map.Entry<Integer, String> entry : SERVER_ZONE_MAP.entrySet()) {
      clusterStartupRule.stop(entry.getKey(), true);
      if (temporaryDirectory.exists()) {
        Files.delete(temporaryDirectory.toPath());
      }
    }
  }

  /**
   * Test that we correctly use the redundancy-zone property to determine where to place redundant
   * copies of a buckets and doesn't allow cross redundancy zone deletes.
   */
  @Test
  @Parameters({"1,2", "1,4", "4,1", "5,6"})
  public void testEnforceZoneWithSixServersAndTwoZones(int rebalanceServer, int shutdownServer) {

    VM rebalanceServerVM = clusterStartupRule.getVM(rebalanceServer);

    // Baseline rebalance with everything up
    rebalanceServerVM.invoke(() -> doRebalance(ClusterStartupRule.getCache().getResourceManager()));

    // Take a server offline
    clusterStartupRule.stop(shutdownServer, false);

    // Rebalance so that now all the buckets are on server 1 and server 3 redundancy.
    // Zone b servers should not be touched.
    rebalanceServerVM.invoke(() -> doRebalance(ClusterStartupRule.getCache().getResourceManager()));

    // Restart the shutdownServer
    startServerInRedundancyZone(shutdownServer, SERVER_ZONE_MAP.get(shutdownServer));

    // Do another rebalance to make sure all the buckets are distributed evenly(ish) and there are
    // no cross redundancy zone bucket deletions.
    rebalanceServerVM.invoke(() -> doRebalance(ClusterStartupRule.getCache().getResourceManager()));

    // Verify that all bucket counts add up to what they should
    compareZoneBucketCounts(COLOCATED_REGION_NAME);
    compareZoneBucketCounts(REGION_NAME);
  }

  /**
   * Set the persistence directories on server VMs
   */
  private void setPersistenceDirectoriesOnServerVMs() throws IOException {
    SecureRandom secureRandom = new SecureRandom();
    runID = secureRandom.nextInt();
    for (Map.Entry<Integer, String> entry : SERVER_ZONE_MAP.entrySet()) {
      final String path =
          clusterStartupRule.getWorkingDirRoot().getAbsolutePath() + "/" + "runId-" + runID + "-vm-"
              + entry.getKey();
      temporaryDirectory = new File(path);
      if (!temporaryDirectory.exists()) {
        Files.createDirectory(temporaryDirectory.toPath());
      }
      System.setProperty(GEODE_PREFIX + DEFAULT_DISK_DIRS_PROPERTY, path);
    }
  }

  /**
   * Startup a client to put all the data in the server regions
   */
  private void clientPopulateServers() throws Exception {
    Properties properties2 = new Properties();
    properties2.setProperty("cache-xml-file", CLIENT_XML);
    ClientVM clientVM =
        clusterStartupRule.startClientVM(SERVER_ZONE_MAP.size() + 1, properties2,
            ccf -> ccf.addPoolLocator("localhost", locatorPort));

    clientVM.invoke(() -> {
      Map<Integer, String> putMap = new HashMap<>();
      for (int i = 0; i < 1000; i++) {
        putMap.put(i, "A");
      }

      ClientCache clientCache = ClusterStartupRule.getClientCache();

      Stream.of(REGION_NAME, COLOCATED_REGION_NAME).forEach(regionName -> {
        Region<Integer, String> region = clientCache.getRegion(regionName);
        region.putAll(putMap);
      });
    });
  }

  /**
   * Startup server *index* in *redundancy zone*
   *
   * @param index - server
   * @param zone - Redundancy zone for the server to be started in
   */
  private void startServerInRedundancyZone(int index, final String zone) {
    clusterStartupRule.startServerVM(index, s -> s.withProperty("cache-xml-file",
        SERVER_XML)
        .withProperty(REDUNDANCY_ZONE, zone)
        .withConnectionToLocator(locatorPort));
  }

  /**
   * Trigger a rebalance of buckets
   *
   */
  private void doRebalance(ResourceManager manager)
      throws TimeoutException, InterruptedException {
    manager.createRebalanceFactory()
        .start().getResults(TIMEOUT_SECONDS, SECONDS);
    assertThat(manager.getRebalanceOperations()).isEmpty();
  }

  /**
   * Compare the bucket counts for each zone. They should be equal
   *
   */
  private void compareZoneBucketCounts(final String regionName) {
    int zoneABucketCount = getZoneBucketCount(regionName, ZONE_A);
    int zoneBBucketCount = getZoneBucketCount(regionName, ZONE_B);

    assertThat(zoneABucketCount).isEqualTo(zoneBBucketCount).isEqualTo(EXPECTED_BUCKET_COUNT);
  }

  /**
   * Get the bucket count for the region in the redundancy zone
   *
   * @param regionName - name of the region to get the bucket count of
   * @param zoneName - redundancy zone for which to get the bucket count
   * @return - the total bucket count for the region in the redundancy zone
   */
  private int getZoneBucketCount(String regionName, String zoneName) {
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
