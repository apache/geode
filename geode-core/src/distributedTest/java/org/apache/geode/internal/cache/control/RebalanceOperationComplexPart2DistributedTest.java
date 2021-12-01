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

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.admin.AdminDistributedSystemFactory.defineDistributedSystem;
import static org.apache.geode.admin.AdminDistributedSystemFactory.getDistributedSystem;
import static org.apache.geode.distributed.ConfigurationProperties.REDUNDANCY_ZONE;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.admin.AdminDistributedSystem;
import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.DistributedSystemConfig;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.internal.cache.PartitionAttributesImpl;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

/**
 * The purpose of RebalanceOperationComplexDistributedTest is to test rebalances
 * across zones and to ensure that enforceUniqueZone behavior of redundancy zones
 * is working correctly.
 */
@RunWith(GeodeParamsRunner.class)
public class RebalanceOperationComplexPart2DistributedTest
    implements Serializable {

  public static final int EXPECTED_BUCKET_COUNT = 113;
  public static final long TIMEOUT_SECONDS = GeodeAwaitility.getTimeout().getSeconds();
  public static final String REGION_NAME = "primary";
  public static final Logger logger = LogService.getLogger();

  public static final String ZONE_A = "zoneA";
  public static final String ZONE_B = "zoneB";
  public static final String ZONE_C = "zoneC";

  public int locatorPort;
  public static final AtomicInteger runID = new AtomicInteger(0);
  public String workingDir;

  // 6 servers distributed evenly across 2 zones
  public static Map<Integer, String> SERVER_ZONE_MAP;

  @Rule
  public ClusterStartupRule clusterStartupRule = new ClusterStartupRule(5);

  @Before
  public void setup() throws Exception {
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
   * copies of a buckets and doesn't allow cross redundancy zone deletes.
   *
   */

  @Test
  public void testRecoveryWithRevokedServerAfterRebalance() throws Exception {

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

    // Rebalance Server VM will initiate all of the rebalances in this test
    VM rebalanceServerVM = clusterStartupRule.getVM(2);

    // Baseline rebalance with everything up
    rebalanceServerVM.invoke(() -> doRebalance(ClusterStartupRule.getCache().getResourceManager()));


    clusterStartupRule.stop(3, false);
    rebalanceServerVM.invoke(() -> revokeKnownMissingMembers(1));

    // Take the serverToBeShutdownAndRestarted offline
    clusterStartupRule.stop(1, false);

    // Rebalance so that now all the buckets are on server 2.
    // Zone b servers should not be touched.
    rebalanceServerVM.invoke(() -> doRebalance(ClusterStartupRule.getCache().getResourceManager()));

    // Restart the serverToBeShutdownAndRestarted
    startServerInRedundancyZone(1, SERVER_ZONE_MAP.get(1));

    rebalanceServerVM = clusterStartupRule.getVM(2);

    // Do another rebalance to make sure all the buckets are distributed evenly(ish) and there
    // are no cross redundancy zone bucket deletions.
    rebalanceServerVM.invoke(() -> doRebalance(ClusterStartupRule.getCache().getResourceManager()));

    // Verify that all bucket counts add up to what they should
    int zoneABucketCount = getZoneBucketCount(REGION_NAME, ZONE_A);
    assertThat(zoneABucketCount).isEqualTo(EXPECTED_BUCKET_COUNT);
  }


  @Test
  public void testRecoveryWithRevokedServer() throws Exception {

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

    // Rebalance Server VM will initiate all of the rebalances in this test
    VM rebalanceServerVM = clusterStartupRule.getVM(2);

    // Baseline rebalance with everything up
    rebalanceServerVM.invoke(() -> doRebalance(ClusterStartupRule.getCache().getResourceManager()));
    clusterStartupRule.stop(3, true);

    rebalanceServerVM.invoke(() -> revokeKnownMissingMembers(1));

    // Take the serverToBeShutdownAndRestarted offline
    clusterStartupRule.stop(1, false);

    // Restart the serverToBeShutdownAndRestarted
    startServerInRedundancyZone(1, SERVER_ZONE_MAP.get(1));

    // Do another rebalance to make sure all the buckets are distributed evenly(ish) and there
    // are no cross redundancy zone bucket deletions.
    rebalanceServerVM.invoke(() -> doRebalance(ClusterStartupRule.getCache().getResourceManager()));

    // Verify that all bucket counts add up to what they should
    int zoneABucketCount = getZoneBucketCount(REGION_NAME, ZONE_A);
    assertThat(zoneABucketCount).isEqualTo(EXPECTED_BUCKET_COUNT);
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


  @Test
  public void testThreeZonesHaveTwoCopiesAfterRebalance() throws Exception {
    SERVER_ZONE_MAP = new HashMap<Integer, String>() {
      {
        put(1, ZONE_A);
        put(2, ZONE_B);
        put(3, ZONE_C);
      }
    };
    cleanOutServerDirectories();
    // Startup the servers
    for (Map.Entry<Integer, String> entry : SERVER_ZONE_MAP.entrySet()) {
      startServerInRedundancyZone(entry.getKey(), entry.getValue());
    }

    // Put data in the server regions
    clientPopulateServers();

    // Rebalance Server VM will initiate all of the rebalances in this test
    VM rebalanceServerVM = clusterStartupRule.getVM(2);

    // Baseline rebalance with everything up
    rebalanceServerVM.invoke(() -> doRebalance(ClusterStartupRule.getCache().getResourceManager()));

    // Take the serverToBeShutdownAndRestarted offline
    clusterStartupRule.stop(1, false);

    // Rebalance so that now all the buckets are on the other two servers.
    // Zone b servers should not be touched.
    rebalanceServerVM.invoke(() -> doRebalance(ClusterStartupRule.getCache().getResourceManager()));

    // Restart the serverToBeShutdownAndRestarted
    startServerInRedundancyZone(1, SERVER_ZONE_MAP.get(1));

    // Do another rebalance to make sure all the buckets are distributed evenly(ish) and there are
    // no cross redundancy zone bucket deletions.
    rebalanceServerVM.invoke(() -> doRebalance(ClusterStartupRule.getCache().getResourceManager()));

    int zoneABucketCount = getZoneBucketCount(REGION_NAME, ZONE_A);
    int zoneBBucketCount = getZoneBucketCount(REGION_NAME, ZONE_B);
    int zoneCBucketCount = getZoneBucketCount(REGION_NAME, ZONE_C);
    assertThat(zoneABucketCount + zoneBBucketCount + zoneCBucketCount)
        .isEqualTo(2 * EXPECTED_BUCKET_COUNT);
  }


  private void revokeKnownMissingMembers(final int numExpectedMissing)
      throws AdminException, InterruptedException {
    DistributedSystemConfig config =
        defineDistributedSystem(ClusterStartupRule.getCache().getInternalDistributedSystem(), "");

    AdminDistributedSystem adminDS = getDistributedSystem(config);
    adminDS.connect();

    try {
      adminDS.waitToBeConnected(MINUTES.toMillis(2));

      await().until(() -> {
        Set<PersistentID> missingIds = adminDS.getMissingPersistentMembers();
        if (missingIds.size() != numExpectedMissing) {
          return false;
        }
        for (PersistentID missingId : missingIds) {
          adminDS.revokePersistentMember(missingId.getUUID());
        }
        return true;
      });


    } finally {
      adminDS.disconnect();
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
