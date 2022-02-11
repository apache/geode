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

import static org.apache.geode.internal.cache.P2PDeltaPropagationDUnitTest.server2;

import java.io.Serializable;
import java.util.stream.IntStream;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class PersistentRecoveryDUnitTest implements Serializable {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  private static final String REGION_NAME = "testRegion";

  @Test
  public void test() throws InterruptedException {
    int locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    MemberVM locator = cluster.startLocatorVM(0, locatorPort);
    MemberVM server1 = cluster.startServerVM(1, locator.getPort());
    MemberVM server2 = cluster.startServerVM(2, locator.getPort());
    server1.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.PARTITION_REDUNDANT_PERSISTENT).create(REGION_NAME);
    });
    server2.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.PARTITION_REDUNDANT_PERSISTENT).create(REGION_NAME);
    });
    server1.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      IntStream.range(0, 1000).forEach(i -> region.put(i, i));
    });
    server2.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      System.out.println("region size:" + region.size());
    });
    server1.stop(false);
    server2.stop(false);
    server1 = cluster.startServerVM(1, locator.getPort());
    server2 = cluster.startServerVM(2, locator.getPort());
    AsyncInvocation asyncInvocation1 = server1.invokeAsync(() -> {
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
          .create(REGION_NAME);
      DiskStore diskStore = ClusterStartupRule.getCache().findDiskStore("DEFAULT");
      System.out.println("diskStore:" + diskStore);
    });
    AsyncInvocation asyncInvocation2 = server2.invokeAsync(() -> {
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.PARTITION_PERSISTENT)
          .create(REGION_NAME);
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      System.out.println("server2 region size:" + region.size());
    });
    asyncInvocation1.get();
    asyncInvocation2.get();
  }

  @Test
  public void testGEM3103() throws InterruptedException {
    int locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    MemberVM locator = cluster.startLocatorVM(0, locatorPort);
    MemberVM server1 = cluster.startServerVM(1, locator.getPort());
    MemberVM server2 = cluster.startServerVM(2, locator.getPort());
    MemberVM server3 = cluster.startServerVM(3, locator.getPort());
    server1.invoke(() -> {
      ClusterStartupRule.getCache()
              .createRegionFactory(RegionShortcut.PARTITION_PERSISTENT).create(REGION_NAME);
    });
    server2.invoke(() -> {
      ClusterStartupRule.getCache()
              .createRegionFactory(RegionShortcut.PARTITION_PERSISTENT).create(REGION_NAME);
    });
    server1.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      IntStream.range(0, 1000).forEach(i -> region.put(i, i));
    });
    server2.invoke(() -> {
      Region region = ClusterStartupRule.getCache().getRegion(REGION_NAME);
      System.out.println("region size:" + region.size());
      ((PartitionedRegion) region).getDataStore();
    });

  }

}
