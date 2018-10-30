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
package org.apache.geode.internal.cache.eviction;

import static org.apache.geode.distributed.ConfigurationProperties.OFF_HEAP_MEMORY_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.test.junit.categories.EvictionTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;


@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
@Category({EvictionTest.class})
public class EvictionIntegrationTest {

  @Parameterized.Parameters(name = "offHeap={0}")
  public static Collection booleans() {
    return Arrays.asList(true, false);
  }

  @Parameterized.Parameter
  public boolean offHeap;

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withNoCacheServer();

  @Before
  public void setUp() {
    if (offHeap) {
      server.withProperty(OFF_HEAP_MEMORY_SIZE, "200m");
    }
    server.startServer();
  }

  @Test
  public void testEntryLruEvictions() {
    int maxEntry = 3;
    PartitionedRegion pr1 = (PartitionedRegion) server.createPartitionRegion("PR1",
        f -> f.setOffHeap(offHeap)
            .setEvictionAttributes(
                EvictionAttributes.createLRUEntryAttributes(maxEntry,
                    EvictionAction.LOCAL_DESTROY)),
        f -> f.setRedundantCopies(0).setTotalNumBuckets(4));

    // put in one extra entry after maxEntry
    for (int counter = 1; counter <= maxEntry + 1; counter++) {
      pr1.put(counter, new byte[1024 * 1024]);
    }

    assertThat(pr1.getTotalEvictions()).isEqualTo(1);
  }

  @Test
  public void testEntryLru() {
    int maxEntry = 12;
    PartitionedRegion pr1 = (PartitionedRegion) server.createPartitionRegion("PR1",
        f -> f.setOffHeap(offHeap)
            .setEvictionAttributes(
                EvictionAttributes.createLRUEntryAttributes(maxEntry,
                    EvictionAction.LOCAL_DESTROY)),
        f -> f.setRedundantCopies(0).setTotalNumBuckets(4));

    for (int i = 0; i < 3; i++) {
      // assume mod-based hashing for bucket creation
      pr1.put(i, "value0");
      pr1.put(i + pr1.getPartitionAttributes().getTotalNumBuckets(), "value1");
      pr1.put(i + (pr1.getPartitionAttributes().getTotalNumBuckets() * 2), "value2");
    }
    pr1.put(3, "value0");

    for (int i = 0; i < 2; i++) {
      pr1.put(i + pr1.getPartitionAttributes().getTotalNumBuckets() * 3, "value1");
    }
    assertThat(pr1.getTotalEvictions()).isEqualTo(0);
  }

  @Test
  public void testCheckEntryLruEvictionsIn1DataStore() {
    int extraEntries = 10;
    int maxEntries = 20;
    PartitionedRegion pr1 = (PartitionedRegion) server.createPartitionRegion("PR1",
        f -> f.setOffHeap(offHeap)
            .setEvictionAttributes(
                EvictionAttributes.createLRUEntryAttributes(maxEntries,
                    EvictionAction.LOCAL_DESTROY)),
        f -> f.setRedundantCopies(1).setTotalNumBuckets(5));

    for (int counter = 1; counter <= maxEntries + extraEntries; counter++) {
      pr1.put(counter, new byte[1024 * 1024]);
    }

    assertThat(pr1.getTotalEvictions()).isEqualTo(extraEntries);

    for (Map.Entry<Integer, BucketRegion> integerBucketRegionEntry : pr1.getDataStore()
        .getAllLocalBuckets()) {
      final BucketRegion bucketRegion =
          (BucketRegion) ((Map.Entry) integerBucketRegionEntry).getValue();
      if (bucketRegion == null) {
        continue;
      }
      assertThat(bucketRegion.size()).isEqualTo(4);
    }
  }

  @Test
  public void testMemLruForPRAndDR() {
    int maxEntries = 40;
    PartitionedRegion pr1 = (PartitionedRegion) server.createPartitionRegion("PR1",
        f -> f.setOffHeap(offHeap)
            .setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(
                ObjectSizer.DEFAULT,
                EvictionAction.LOCAL_DESTROY)),
        f -> f.setRedundantCopies(0).setTotalNumBuckets(4).setLocalMaxMemory(maxEntries));

    LocalRegion dr1 =
        (LocalRegion) server.createRegion(RegionShortcut.LOCAL, "DR1",
            f -> f.setOffHeap(offHeap).setDataPolicy(DataPolicy.NORMAL).setEvictionAttributes(
                EvictionAttributes.createLRUMemoryAttributes(ObjectSizer.DEFAULT,
                    EvictionAction.LOCAL_DESTROY)));

    assertThat(pr1.getLocalMaxMemory()).isEqualTo(pr1.getEvictionAttributes().getMaximum());
    assertThat(dr1.getEvictionAttributes().getMaximum())
        .isEqualTo(EvictionAttributes.DEFAULT_MEMORY_MAXIMUM);

    for (int i = 0; i < 41; i++) {
      pr1.put(i, new byte[1024 * 1024]);
    }

    assertThat(pr1.getTotalEvictions()).isGreaterThanOrEqualTo(1).isLessThanOrEqualTo(2);
    for (int i = 0; i < 11; i++) {
      dr1.put(i, new byte[1024 * 1024]);
    }

    assertThat(dr1.getTotalEvictions()).isGreaterThanOrEqualTo(1).isLessThanOrEqualTo(2);
  }

  @Test
  public void testEachTaskSize() {
    server.createPartitionRegion("PR1",
        f -> f.setOffHeap(offHeap)
            .setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(
                null, EvictionAction.LOCAL_DESTROY)),
        f -> f.setRedundantCopies(1).setTotalNumBuckets(6));

    server.createPartitionRegion("PR2",
        f -> f.setOffHeap(offHeap)
            .setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(
                null, EvictionAction.LOCAL_DESTROY)),
        f -> f.setRedundantCopies(1).setTotalNumBuckets(10));

    server.createPartitionRegion("PR3",
        f -> f.setOffHeap(offHeap)
            .setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(
                null, EvictionAction.LOCAL_DESTROY)),
        f -> f.setRedundantCopies(1).setTotalNumBuckets(15));

    server.createRegion(RegionShortcut.LOCAL, "DR1",
        f -> f.setOffHeap(offHeap).setDataPolicy(DataPolicy.NORMAL).setEvictionAttributes(
            EvictionAttributes.createLRUHeapAttributes(null, EvictionAction.LOCAL_DESTROY)));

    HeapEvictor evictor = ((GemFireCacheImpl) server.getCache()).getHeapEvictor();
    List<Integer> taskSetSizes = evictor.testOnlyGetSizeOfTasks();

    for (Integer size : taskSetSizes) {
      assertThat(size.intValue()).isEqualTo(8);
    }
  }

}
