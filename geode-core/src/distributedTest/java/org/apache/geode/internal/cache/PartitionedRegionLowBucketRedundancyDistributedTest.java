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
package org.apache.geode.internal.cache;

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

public class PartitionedRegionLowBucketRedundancyDistributedTest implements Serializable {

  public String regionName;

  @Rule
  public ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() {
    regionName = testName.getMethodName() + "_region";
  }

  @Test
  public void testTwoServersWithOneRedundantCopy() throws Exception {
    // Start locator
    MemberVM locator = startupRule.startLocatorVM(0);
    int locatorPort = locator.getPort();

    // Start server1 and create region
    MemberVM server1 = startServerAndCreateRegion(1, locatorPort, PARTITION, 1);

    // Verify lowBucketRedundancyCount == 0 in server1
    server1.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));

    // Do puts in server1
    server1.getVM().invoke(() -> doPuts(500));

    // Verify lowBucketRedundancyCount == 113 in server1
    server1.getVM().invoke(() -> waitForLowBucketRedundancyCount(113));

    // Start server2 and create region
    MemberVM server2 = startServerAndCreateRegion(2, locatorPort, PARTITION, 1);

    // Verify lowBucketRedundancyCount == 0 in both servers
    server1.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));
    server2.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));

    // Stop server2
    server2.stop(false);

    // Verify lowBucketRedundancyCount == 113 in server1
    server1.getVM().invoke(() -> waitForLowBucketRedundancyCount(113));
  }

  @Test
  public void testThreeServersWithTwoRedundantCopies() throws Exception {
    // Start locator
    MemberVM locator = startupRule.startLocatorVM(0);
    int locatorPort = locator.getPort();

    // Start server1 and create region
    MemberVM server1 = startServerAndCreateRegion(1, locatorPort, PARTITION, 2);

    // Verify lowBucketRedundancyCount == 0 in server1
    server1.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));

    // Do puts in server1
    server1.getVM().invoke(() -> doPuts(500));

    // Verify lowBucketRedundancyCount == 113 in server1
    server1.getVM().invoke(() -> waitForLowBucketRedundancyCount(113));

    // Start server2 and create region
    MemberVM server2 = startServerAndCreateRegion(2, locatorPort, PARTITION, 2);

    // Verify lowBucketRedundancyCount == 113 in both servers
    server1.getVM().invoke(() -> waitForLowBucketRedundancyCount(113));
    server2.getVM().invoke(() -> waitForLowBucketRedundancyCount(113));

    // Start server2 and create region
    MemberVM server3 = startServerAndCreateRegion(3, locatorPort, PARTITION, 2);

    // Verify lowBucketRedundancyCount == 113 in server1
    server1.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));
    server2.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));
    server3.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));
  }

  @Test
  public void testFourServersWithPersistentRegionAndOneRedundantCopy() throws Exception {
    // Start locator
    MemberVM locator = startupRule.startLocatorVM(0);
    int locatorPort = locator.getPort();

    // Start servers and create regions
    MemberVM server1 = startServerAndCreateRegion(1, locatorPort, PARTITION_PERSISTENT, 1);
    MemberVM server2 = startServerAndCreateRegion(2, locatorPort, PARTITION_PERSISTENT, 1);
    MemberVM server3 = startServerAndCreateRegion(3, locatorPort, PARTITION_PERSISTENT, 1);
    MemberVM server4 = startServerAndCreateRegion(4, locatorPort, PARTITION_PERSISTENT, 1);

    // Verify lowBucketRedundancyCount == 0 in all servers
    server1.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));
    server2.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));
    server3.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));
    server4.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));

    // Do puts in server1
    server1.getVM().invoke(() -> doPuts(500));

    // Verify lowBucketRedundancyCount == 0 in all servers
    server1.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));
    server2.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));
    server3.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));
    server4.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));

    // Stop servers 1 and 2
    server1.stop(false);
    server2.stop(false);

    server3.getVM().invoke(() -> waitForMembers(1));
    server4.getVM().invoke(() -> waitForMembers(1));

    // Restart servers 1 and 2
    server1 = startupRule.startServerVM(1, locatorPort);
    server2 = startupRule.startServerVM(2, locatorPort);

    // Asynchronously recreate the regions in servers 1 and 2 (since they are recovering persistent
    // data)
    AsyncInvocation recreateRegionInServer1 =
        server1.getVM().invokeAsync(() -> createRegion(PARTITION_PERSISTENT, 1));
    AsyncInvocation recreateRegionInServer2 =
        server2.getVM().invokeAsync(() -> createRegion(PARTITION_PERSISTENT, 1));
    recreateRegionInServer1.await();
    recreateRegionInServer2.await();

    // Verify lowBucketRedundancyCount == 0 in all servers
    server1.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));
    server2.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));
    server3.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));
    server4.getVM().invoke(() -> waitForLowBucketRedundancyCount(0));
  }

  private MemberVM startServerAndCreateRegion(int vmId, int locatorPort, RegionShortcut shortcut,
      int redundantCopies) {
    // Start server
    MemberVM server = startupRule.startServerVM(vmId, locatorPort);

    // Create region
    server.getVM().invoke(() -> createRegion(shortcut, redundantCopies));

    return server;
  }

  private void createRegion(RegionShortcut shortcut, int redundantCopies) {
    PartitionAttributesFactory<?, ?> paf = new PartitionAttributesFactory();
    paf.setRedundantCopies(redundantCopies);
    ClusterStartupRule.getCache().createRegionFactory(shortcut)
        .setPartitionAttributes(paf.create()).create(regionName);
  }

  private void doPuts(int numPuts) {
    Region region = ClusterStartupRule.getCache().getRegion(regionName);
    for (int i = 0; i < numPuts; i++) {
      region.put("key" + i, "value" + i);
    }
  }

  private void waitForLowBucketRedundancyCount(int count) {
    PartitionedRegion region =
        (PartitionedRegion) ClusterStartupRule.getCache().getRegion(regionName);
    await().untilAsserted(
        () -> assertThat(region.getPrStats().getLowRedundancyBucketCount()).isEqualTo(count));
  }

  private void waitForMembers(int count) {
    await().untilAsserted(
        () -> assertThat(ClusterStartupRule.getCache().getMembers().size()).isEqualTo(count));
  }
}
