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

import static junitparams.JUnitParamsRunner.$;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.RegionShortcut.PARTITION_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_PERSISTENT_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT_PERSISTENT_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_OVERFLOW;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT_OVERFLOW;
import static org.apache.geode.test.dunit.VM.getVM;

import java.io.IOException;
import java.io.Serializable;
import java.util.stream.LongStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRule;

@RunWith(JUnitParamsRunner.class)
public class PartitionedRegionClearPerformanceDUnitTest implements Serializable {

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  private static final String REGION_NAME = "testRegion";

  private static final long NUM_ENTRIES = 2000;

  private static final int[] NUM_BUCKETS = new int[] {1, 10, 113, 227, 467, 1001};

  private static final int NUM_ITERATIONS = 10;

  private VM server1;

  private VM server2;

  private VM server3;

  private VM client;

  private static StringBuilder performanceTestResult = new StringBuilder();

  @Before
  public void setUp() {
    server1 = getVM(0);
    server2 = getVM(1);
    server3 = getVM(2);
    client = getVM(3);
  }

  @AfterClass
  public static void tearDown() {
    System.out.println(performanceTestResult.toString());
  }

  @Test
  @Parameters(method = "getRegionShortcuts")
  public void testPerformance(RegionShortcut shortcut) {
    boolean replicatedTested = false;
    for (int numBuckets : NUM_BUCKETS) {
      if (shortcut.isReplicate()) {
        if (replicatedTested) {
          continue;
        }
        replicatedTested = true;
      }
      long sum = 0;
      for (int i = 0; i < NUM_ITERATIONS; i++) {
        createRegionOnServers(shortcut, numBuckets);
        populationRegion();
        long start = System.nanoTime();
        clearRegion();
        long end = System.nanoTime();
        performanceTestResult.append("Region shortcut: " + shortcut + " numBuckets: " + numBuckets +
            " Iteration: " + i
            + ". Time elapsed for region clear "
            + (end - start) + " nanoseconds.");
        performanceTestResult.append(System.lineSeparator());
        sum = sum + end - start;
        destroyRegion();
        destroyDiskStore();
      }
      performanceTestResult.append("Region shortcut: " + shortcut + " numBuckets: " + numBuckets
          + ". Average time elapsed for region clear "
          + (sum / NUM_ITERATIONS) + " nanoseconds.");
      performanceTestResult.append(System.lineSeparator());
    }
  }

  private void createRegionOnServers(RegionShortcut shortcut, int numBuckets) {
    for (VM vm : new VM[] {server1, server2, server3}) {
      vm.invoke(() -> {
        createRegion(shortcut, numBuckets);
      });
    }
  }

  private void createRegion(RegionShortcut shortcut, int numBuckets)
      throws IOException {
    cacheRule.createCache();
    final CacheServer cacheServer = cacheRule.getCache().addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    if (shortcut.isReplicate()) {
      cacheRule.getCache().createRegionFactory(shortcut).create(REGION_NAME);
    } else {
      cacheRule.getCache()
          .createRegionFactory(shortcut).setPartitionAttributes(new PartitionAttributesFactory<>()
              .setTotalNumBuckets(numBuckets).create())
          .create(REGION_NAME);
    }
  }

  private void destroyRegion() {
    server1.invoke(() -> {
      cacheRule.getCache().getRegion(REGION_NAME).destroyRegion();
    });
  }

  private void populationRegion() {
    client.invoke(() -> {
      final ClientCacheFactory clientCacheFactory = new ClientCacheFactory();
      clientCacheFactory.addPoolLocator("localhost", DistributedTestUtils.getLocatorPort());
      clientCacheRule.createClientCache(clientCacheFactory);
      final Region region;
      if (clientCacheRule.getClientCache().getRegion(REGION_NAME) == null) {
        region = clientCacheRule.getClientCache()
            .createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY).create(REGION_NAME);
      } else {
        region = clientCacheRule.getClientCache().getRegion(REGION_NAME);
      }
      LongStream.range(0, NUM_ENTRIES).forEach(i -> {
        region.put("key" + i, "value" + i);
      });
    });
  }

  private void clearRegion() {
    client.invoke(() -> {
      clientCacheRule.getClientCache().getRegion(REGION_NAME).clear();
    });
  }

  private static Object[] getRegionShortcuts() {
    return $(new Object[] {REPLICATE}, new Object[] {REPLICATE_PERSISTENT},
        new Object[] {REPLICATE_PERSISTENT_OVERFLOW}, new Object[] {REPLICATE_OVERFLOW},
        new Object[] {PARTITION}, new Object[] {PARTITION_REDUNDANT},
        new Object[] {PARTITION_PERSISTENT}, new Object[] {PARTITION_REDUNDANT_PERSISTENT},
        new Object[] {PARTITION_OVERFLOW}, new Object[] {PARTITION_REDUNDANT_OVERFLOW},
        new Object[] {PARTITION_PERSISTENT_OVERFLOW},
        new Object[] {PARTITION_REDUNDANT_PERSISTENT_OVERFLOW});
  }

  private void destroyDiskStore() {
    server1.invoke(() -> {
      DiskStore diskStore =
          cacheRule.getCache().findDiskStore(DiskStoreFactory.DEFAULT_DISK_STORE_NAME);
      if (diskStore != null) {
        diskStore.destroy();
      }
    });
  }
}
