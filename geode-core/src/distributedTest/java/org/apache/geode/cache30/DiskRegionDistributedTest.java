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
package org.apache.geode.cache30;

import static org.apache.geode.cache.EvictionAction.OVERFLOW_TO_DISK;
import static org.apache.geode.cache.EvictionAttributes.createLRUEntryAttributes;
import static org.apache.geode.cache.EvictionAttributes.createLRUMemoryAttributes;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PERSISTENT;
import static org.apache.geode.cache.RegionShortcut.REPLICATE_PROXY;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.Scope;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.eviction.EvictionCounters;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

/**
 * Tests the functionality of cache regions whose contents may be written to disk.
 *
 * @since GemFire 3.2
 */
public class DiskRegionDistributedTest implements Serializable {

  private String uniqueName;
  private String regionName;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public DistributedDiskDirRule diskDirsRule = new DistributedDiskDirRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";

    vm0.invoke(() -> cacheRule.createCache());
    vm1.invoke(() -> cacheRule.createCache());
    vm2.invoke(() -> cacheRule.createCache());
    vm3.invoke(() -> cacheRule.createCache());
  }

  /**
   * Makes sure that updates from other VMs cause existing entries to be written to disk.
   */
  @Test
  public void testRemoteUpdates() {
    vm0.invoke(() -> createRegionWithEvictionOverflow());
    vm1.invoke(() -> createRegionWithEvictionOverflow());

    vm0.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(uniqueName);
      EvictionCounters evictionCounters = getEvictionCounters(region);
      int i;
      for (i = 0; evictionCounters.getEvictions() <= 0; i++) {
        region.put(i, new short[250]);
      }

      assertThat(i).isGreaterThan(5);
    });

    vm1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(uniqueName);
      for (int i = 0; i < 10; i++) {
        region.put(i, new int[250]);
      }
    });

    vm0.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(uniqueName);
      EvictionCounters evictionCounters = getEvictionCounters(region);

      await("waiting for evictions to exceed 6")
          .until(() -> evictionCounters.getEvictions() > 6);
    });

    vm0.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(uniqueName);
      for (int i = 0; i < 10000; i++) {
        region.put(String.valueOf(i), String.valueOf(i).getBytes());
      }
    });

    vm1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(uniqueName);
      for (int i = 0; i < 10000; i++) {
        byte[] bytes = (byte[]) region.get(String.valueOf(i));

        assertThat(new String(bytes)).isEqualTo(String.valueOf(i));
      }
    });
  }

  /**
   * Tests overflow with mirrored regions. Note that we have to use {@code byte} array values
   * in this test. Otherwise, the size of the data in the "puter" VM would be different from the
   * size of the data in the receiver VM, thus cause the two VMs to have different LRU eviction
   * behavior.
   */
  @Test
  public void testOverflowMirror() {
    vm0.invoke(() -> createReplicateRegionWithEvictionOverflow());
    vm1.invoke(() -> createReplicateRegionWithEvictionOverflow());

    vm0.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(uniqueName);
      EvictionCounters evictionCounters = getEvictionCounters(region);
      for (int i = 0; evictionCounters.getEvictions() < 10; i++) {
        region.put(i, new byte[1]);
      }

      assertThat(evictionCounters.getEvictions()).isEqualTo(10);
    });

    vm1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(uniqueName);
      EvictionCounters evictionCounters = getEvictionCounters(region);

      assertThat(evictionCounters.getEvictions()).isEqualTo(10);

      // Because we are DISTRIBUTED_ACK, we can rely on the order in which messages arrive and
      // hence the order of the LRU entries.
      for (int i = 0; i < 10; i++) {
        region.get(i);

        assertThat(evictionCounters.getEvictions()).as("No eviction for " + i)
            .isEqualTo(10 + 1 + i);
      }
    });
  }

  /**
   * Tests that invalidates and updates received from different VMs are handled appropriately by
   * overflow regions.
   */
  @Test
  public void testDistributedInvalidate() {
    vm0.invoke(() -> createRegionWithEvictionCountOverflow());
    vm1.invoke(() -> createRegionWithEvictionCountOverflow());

    vm0.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(uniqueName);
      EvictionCounters evictionCounters = getEvictionCounters(region);
      for (int i = 0; evictionCounters.getEvictions() < 10; i++) {
        region.put(i, new byte[1]);
      }

      assertThat(evictionCounters.getEvictions()).isEqualTo(10);
    });

    Object key = 20;

    vm1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(uniqueName);

      assertThat(region.get(key)).isNotNull();

      region.invalidate(key);
    });

    vm0.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(uniqueName);

      await("value for key remains: " + key)
          .until(() -> region.get(key) == null);
    });

    String newValue = "NEW VALUE";

    vm1.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(uniqueName);
      region.put(key, newValue);
    });

    vm0.invoke(() -> {
      Region region = cacheRule.getCache().getRegion(uniqueName);

      await("verify update").until(() -> newValue.equals(region.get(key)));
    });
  }

  @Test
  public void testPersistentReplicateB4NonPersistent() {
    vm1.invoke(() -> {
      RegionFactory regionFactory = cacheRule.getCache().createRegionFactory(REPLICATE_PERSISTENT);
      Region region = regionFactory.create(regionName);

      assertThat(region.getAttributes().getConcurrencyChecksEnabled()).isTrue();
    });

    vm2.invoke(() -> {
      RegionFactory regionFactory =
          cacheRule.getCache().createRegionFactory(RegionShortcut.REPLICATE);
      Region region = regionFactory.create(regionName);

      assertThat(region.getAttributes().getConcurrencyChecksEnabled()).isTrue();
    });

    vm3.invoke(() -> {
      RegionFactory regionFactory = cacheRule.getCache().createRegionFactory();
      regionFactory.setScope(Scope.DISTRIBUTED_ACK);

      Region region = regionFactory.create(regionName);
      assertThat(region.getAttributes().getConcurrencyChecksEnabled()).isTrue();
    });
  }

  @Test
  public void testRRProxyWithPersistentReplicates() {
    vm1.invoke(() -> {
      cacheRule.getCache().createRegionFactory(REPLICATE_PROXY).create(regionName);
    });
    vm2.invoke(() -> {
      cacheRule.getCache().createRegionFactory(REPLICATE_PERSISTENT).create(regionName);
    });

    assertThat(vm1.invoke(() -> cacheRule.getCache().getRegion(regionName).getAttributes()
        .getConcurrencyChecksEnabled())).isTrue();
    assertThat(vm2.invoke(() -> cacheRule.getCache().getRegion(regionName).getAttributes()
        .getConcurrencyChecksEnabled())).isTrue();
  }

  private void createRegionWithEvictionOverflow() {
    RegionFactory regionFactory = cacheRule.getCache().createRegionFactory();
    regionFactory.setEvictionAttributes(createLRUMemoryAttributes(2, null, OVERFLOW_TO_DISK));
    regionFactory.setScope(Scope.DISTRIBUTED_NO_ACK);

    regionFactory.create(uniqueName);
  }

  private void createReplicateRegionWithEvictionOverflow() {
    RegionFactory regionFactory = cacheRule.getCache().createRegionFactory();

    regionFactory.setDataPolicy(DataPolicy.REPLICATE);
    regionFactory.setDiskSynchronous(true);
    regionFactory.setEvictionAttributes(createLRUEntryAttributes(100, OVERFLOW_TO_DISK));
    regionFactory.setScope(Scope.DISTRIBUTED_ACK);

    regionFactory.create(uniqueName);
  }

  private void createRegionWithEvictionCountOverflow() {
    RegionFactory regionFactory = cacheRule.getCache().createRegionFactory();

    regionFactory.setEvictionAttributes(createLRUEntryAttributes(100, OVERFLOW_TO_DISK));
    regionFactory.setScope(Scope.DISTRIBUTED_ACK);

    regionFactory.create(uniqueName);
  }

  /**
   * Returns the {@code EvictionStatistics} for the given region
   */
  private EvictionCounters getEvictionCounters(Region region) {
    return ((InternalRegion) region).getEvictionController().getCounters();
  }
}
