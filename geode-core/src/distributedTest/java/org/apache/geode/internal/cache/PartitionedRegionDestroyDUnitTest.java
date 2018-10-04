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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;

/**
 * This test aims to test the destroyRegion functionality.
 */

public class PartitionedRegionDestroyDUnitTest extends CacheTestCase {

  private static volatile CountDownLatch signalLatch = new CountDownLatch(1);

  private String prNamePrefix;
  private int numberOfRegions;
  private int totalNumBuckets;
  private int redundantCopies;
  private int localMaxMemory;
  private int loopSleepMillis;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Before
  public void setUp() throws Exception {
    prNamePrefix = "PR-";
    numberOfRegions = 2;
    totalNumBuckets = 5;
    redundantCopies = 0;
    localMaxMemory = 200;
    loopSleepMillis = 100;

    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);
    vm3 = getHost(0).getVM(3);
  }

  @Test
  public void testDestroyRegion() throws Exception {
    vm0.invoke(() -> createPartitionedRegions());
    vm1.invoke(() -> createPartitionedRegions());
    vm2.invoke(() -> createPartitionedRegions());
    vm3.invoke(() -> createPartitionedRegions());

    vm1.invoke(() -> {
      try (IgnoredException ie = addIgnoredException(RegionDestroyedException.class)) {
        Cache cache = getCache();
        for (int i = 0; i < numberOfRegions; i++) {
          Region<Integer, String> region = cache.getRegion(prNamePrefix + i);
          // Create enough entries such that all bucket are created, integer keys assumes mod
          // distribution
          int totalEntries = ((PartitionedRegion) region).getTotalNumberOfBuckets() * 2;
          for (int k = 0; k < totalEntries; k++) {
            region.put(k, prNamePrefix + k);
          }
        }
      }
    });

    AsyncInvocation asyncVM2 = vm2.invokeAsync(() -> {
      try (IgnoredException ie = addIgnoredException(RegionDestroyedException.class)) {
        Cache cache = getCache();

        // Grab the regions right away, before they get destroyed by the other thread
        PartitionedRegion regions[] = new PartitionedRegion[numberOfRegions];
        for (int i = 0; i < numberOfRegions; i++) {
          regions[i] = (PartitionedRegion) cache.getRegion(Region.SEPARATOR + prNamePrefix + i);
          assertThat(regions[i]).isNotNull();
        }

        signalLatch.countDown();

        for (int i = 0; i < numberOfRegions; i++) {
          PartitionedRegion region = regions[i];
          int startEntries = region.getTotalNumberOfBuckets() * 20;
          int endEntries = startEntries + region.getTotalNumberOfBuckets();
          boolean isDestroyed = false;
          for (int k = startEntries; k < endEntries; k++) {
            final int key = k;
            try {
              if (isDestroyed) {
                assertThatThrownBy(() -> region.put(key, prNamePrefix + key))
                    .isInstanceOf(RegionDestroyedException.class);
              } else {
                region.put(key, prNamePrefix + key);
              }
            } catch (RegionDestroyedException e) {
              isDestroyed = true;
            }
          }
          Thread.sleep(loopSleepMillis);
        }
      }
    });

    addIgnoredException(ReplyException.class);

    vm2.invoke(() -> signalLatch.await(30, SECONDS));

    vm0.invoke(() -> {
      Cache cache = getCache();
      for (int i = 0; i < numberOfRegions; i++) {
        Region region = cache.getRegion(prNamePrefix + i);
        assertThat(region).isNotNull();

        region.destroyRegion();
        assertThat(region.isDestroyed()).isTrue();

        assertThat(cache.getRegion(prNamePrefix + i)).isNull();
      }
    });

    asyncVM2.await();

    vm0.invoke(() -> validateMetaDataAfterDestroy());
    vm1.invoke(() -> validateMetaDataAfterDestroy());
    vm2.invoke(() -> validateMetaDataAfterDestroy());
    vm3.invoke(() -> validateMetaDataAfterDestroy());
  }

  private void createPartitionedRegions() {
    Cache cache = getCache();

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setRedundantCopies(redundantCopies);
    partitionAttributesFactory.setLocalMaxMemory(localMaxMemory);
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);

    RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION);

    for (int i = 0; i < numberOfRegions; i++) {
      regionFactory.create(prNamePrefix + i);
    }
  }

  private void validateMetaDataAfterDestroy() {
    InternalCache cache = getCache();
    Region rootRegion = PartitionedRegionHelper.getPRRoot(cache);

    await().untilAsserted(() -> assertThat(cache.rootRegions()).isEmpty());

    assertEquals(
        "ThePrIdToPR Map size is:" + PartitionedRegion.getPrIdToPR().size() + " instead of 0",
        numberOfRegions, PartitionedRegion.getPrIdToPR().size());

    assertThat(PartitionedRegion.getPrIdToPR()).hasSize(numberOfRegions);

    for (Object regionObject : rootRegion.subregions(false)) {
      Region region = (Region) regionObject;
      assertThat(region.getName()).doesNotContain(PartitionedRegionHelper.BUCKET_REGION_PREFIX);
    }
  }
}
