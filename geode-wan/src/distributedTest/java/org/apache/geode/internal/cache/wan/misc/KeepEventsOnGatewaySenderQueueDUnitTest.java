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
package org.apache.geode.internal.cache.wan.misc;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.wan.GatewayReceiver;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.tier.sockets.CacheServerStats;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewayReceiverStats;
import org.apache.geode.internal.cache.wan.WANTestBase;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.junit.categories.WanTest;

@Category({WanTest.class})
public class KeepEventsOnGatewaySenderQueueDUnitTest extends WANTestBase {

  public KeepEventsOnGatewaySenderQueueDUnitTest() {
    super();
  }

  @Test
  public void testKeepEventsOnGatewaySenderQueueWithPartitionOfflineException() throws Exception {
    // Start locators
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm2.invoke(() -> createFirstRemoteLocator(2, lnPort));

    String regionName = getTestMethodName() + "_PR";
    String senderId = "ny";

    // Configure receiving site members
    createCacheInVMs(nyPort, vm3, vm4);
    createReceiverInVMs(vm3, vm4);
    vm3.invoke(() -> createPartitionedRegionWithPersistence(regionName, null, 0, 100));
    vm4.invoke(() -> createPartitionedRegionWithPersistence(regionName, null, 0, 100));
    vm4.invoke(() -> assignBuckets(regionName));

    // Configure sending site members
    vm1.invoke(() -> createCache(lnPort));
    vm1.invoke(() -> createSender(senderId, 2, true, 100, 10, false, true, null, false));
    vm1.invoke(() -> disableRemoveFromQueueOnException(senderId));
    vm1.invoke(() -> createPartitionedRegionWithPersistence(regionName, senderId, 0, 100));

    // Asynchronously do puts in sending site member
    AsyncInvocation<Integer> putInvocation = vm1.invokeAsync(() -> doPuts(regionName, 60000l));

    // Repeatedly bounce a receiving site member which will cause PartitionOfflineExceptions
    AsyncInvocation<Integer> closeOpenInvocation =
        vm3.invokeAsync(() -> closeRecreateCache(nyPort, regionName, 3));

    // Once puts are complete, wait for sending site member queue to be empty
    int numPuts = putInvocation.get(120, TimeUnit.SECONDS);
    vm1.invoke(() -> validateQueueSizeStat(senderId, 0));

    // Once the receiving site member bounce has completed, verify region sizes in both sites
    closeOpenInvocation.join(120000);
    vm1.invoke(() -> validateRegionSize(regionName, numPuts));
    vm3.invoke(() -> validateRegionSize(regionName, numPuts));
    vm4.invoke(() -> validateRegionSize(regionName, numPuts));
  }

  @Test
  public void testKeepEventsOnGatewaySenderQueueWithRegionDestroyedException() throws Exception {
    // Start locators
    Integer lnPort = vm0.invoke(() -> createFirstLocatorWithDSId(1));
    Integer nyPort = vm2.invoke(() -> createFirstRemoteLocator(2, lnPort));

    String regionName = getTestMethodName() + "_PR";
    String senderId = "ny";

    // Configure receiving site member
    vm3.invoke(() -> createCache(nyPort));
    vm3.invoke(() -> createReceiver());

    // Configure sending site member
    vm1.invoke(() -> createCache(lnPort));
    vm1.invoke(() -> createSender(senderId, 2, true, 100, 10, false, true, null, false));
    vm1.invoke(() -> disableRemoveFromQueueOnException(senderId));
    vm1.invoke(() -> createPartitionedRegionWithPersistence(regionName, senderId, 0, 100));

    // Do puts in sending site member
    int numPuts = 10;
    vm1.invoke(() -> doPuts(regionName, numPuts));

    // Wait for some retries to occur
    vm3.invoke(() -> waitForEventRetries(10));

    // Create region in receiving site member
    vm3.invoke(() -> createPartitionedRegionWithPersistence(regionName, null, 0, 100));

    // Wait for sending site member queue to be empty
    vm1.invoke(() -> validateQueueSizeStat(senderId, 0));

    // Verify region sizes in both sites
    vm1.invoke(() -> validateRegionSize(regionName, numPuts));
    vm3.invoke(() -> validateRegionSize(regionName, numPuts));
  }

  private void disableRemoveFromQueueOnException(String senderId) {
    AbstractGatewaySender ags = (AbstractGatewaySender) cache.getGatewaySender(senderId);
    ags.setRemoveFromQueueOnException(false);
  }

  private void assignBuckets(String regionName) {
    Region region = cache.getRegion(regionName);
    PartitionRegionHelper.assignBucketsToPartitions(region);
  }

  private int doPuts(String regionName, long timeToRun) throws Exception {
    int numPuts = 0;
    Region region = cache.getRegion(regionName);
    long end = System.currentTimeMillis() + timeToRun;
    while (System.currentTimeMillis() < end) {
      region.put(UUID.randomUUID(), 0);
      numPuts++;
      Thread.sleep(10);
    }
    return numPuts;
  }

  private void closeRecreateCache(int locatorPort, String regionName, int iterations)
      throws Exception {
    for (int i = 0; i < iterations; i++) {
      closeCache();
      Thread.sleep(5000);
      createCache(locatorPort);
      createReceiver();
      createPartitionedRegionWithPersistence(regionName, null, 0, 100);
    }
  }

  private void waitForEventRetries(int numRetries) {
    GatewayReceiverStats stats = getGatewayReceiverStats();
    await()
        .until(() -> stats.getEventsRetried() > numRetries);
  }

  private GatewayReceiverStats getGatewayReceiverStats() {
    Set<GatewayReceiver> gatewayReceivers = cache.getGatewayReceivers();
    GatewayReceiver receiver = gatewayReceivers.iterator().next();
    CacheServerStats stats = ((CacheServerImpl) receiver.getServer()).getAcceptor().getStats();
    assertThat(stats).isInstanceOf(GatewayReceiverStats.class);
    return (GatewayReceiverStats) stats;
  }
}
