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

import static org.apache.geode.cache.EvictionAlgorithm.NONE;
import static org.apache.geode.cache.EvictionAttributes.createLRUEntryAttributes;
import static org.apache.geode.cache.PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT;
import static org.apache.geode.internal.cache.PartitionedRegion.RETRY_TIMEOUT_PROPERTY;
import static org.apache.geode.internal.cache.PartitionedRegionHelper.BYTES_PER_MB;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;

/**
 * This class is to test LOCAL_MAX_MEMORY property of partition region while creation of bucket.
 */

@SuppressWarnings("serial")
public class PartitionedRegionLocalMaxMemoryDUnitTest extends CacheTestCase {

  private static final String RETRY_TIMEOUT_PROPERTY_VALUE = "20000";
  private static final int LOCAL_MAX_MEMORY = 1;
  private static final long EXPECTED_LOCAL_MAX_MEMORY = LOCAL_MAX_MEMORY * BYTES_PER_MB;
  private static final int REDUNDANCY = 1;

  private String regionName;
  private boolean evict;

  private VM vm0;
  private VM vm1;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setUp() {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);

    regionName = getUniqueName();
    evict = false;
  }

  protected RegionAttributes<?, ?> createRegionAttrsForPR(int redundancy, int localMaxMemory,
      long recoveryDelay, EvictionAttributes evictionAttributes) {
    return PartitionedRegionTestHelper.createRegionAttrsForPR(redundancy, localMaxMemory,
        recoveryDelay, evictionAttributes, null);
  }

  /**
   * This test performs following operations <br>
   * 1. Create Partition region with LOCAL_MAX_MEMORY = 1MB on all the VMs <br>
   * 2. Put objects in partition region so that only one bucket gets created and size of that bucket
   * exceeds LOCAL_MAX_MEMORY <br>
   * 3. Put object such that new bucket gets formed <br>
   * 4. Test should create a new bucket
   */
  @Test
  public void testLocalMaxMemoryInPartitionedRegion() {
    evict = false;

    vm0.invoke(() -> createMultiplePartitionRegion(evict));
    vm1.invoke(() -> createMultiplePartitionRegion(evict));

    vm0.invoke(() -> validateEvictionIsEnabled(evict));
    vm1.invoke(() -> validateEvictionIsEnabled(evict));

    // puts to one bucket to use up LOCAL_MAX_MEMORY
    vm0.invoke(() -> fillDataStoreWithPutsToOneBucket(10));
    long currentAllocatedMemory = vm0.invoke(this::validateDataStoreExceedsLocalMaxMemory);

    // exceed LOCAL_MAX_MEMORY
    vm0.invoke(() -> putOneObjectInPartitionedRegion(21));
    vm0.invoke(() -> validateDataStoreExceeds(currentAllocatedMemory));
  }

  /**
   * This test makes sure that we don't enforce the LOCAL_MAX_MEMORY setting when eviction is
   * enabled.
   */
  @Test
  public void testLocalMaxMemoryInPartitionedRegionWithEviction() {
    evict = true;

    vm0.invoke(() -> createMultiplePartitionRegion(evict));
    vm1.invoke(() -> createMultiplePartitionRegion(evict));

    vm0.invoke(() -> validateEvictionIsEnabled(evict));
    vm1.invoke(() -> validateEvictionIsEnabled(evict));

    // puts to one bucket to use up LOCAL_MAX_MEMORY
    vm0.invoke(() -> fillDataStoreWithPutsToOneBucket(10));

    // exceed LOCAL_MAX_MEMORY
    vm0.invoke(() -> putOneObjectInPartitionedRegion(21));
    vm0.invoke(() -> validateDataStoreExceeds(EXPECTED_LOCAL_MAX_MEMORY));
  }

  private void putOneObjectInPartitionedRegion(final int objectId) {
    Region<TestObjectWithIdentifier, TestObjectWithIdentifier> region =
        getCache().getRegion(regionName);
    TestObjectWithIdentifier kv =
        new TestObjectWithIdentifier("TestObjectWithIdentifier-" + 0, objectId);
    region.put(kv, kv);
  }

  private void fillDataStoreWithPutsToOneBucket(final int objectId) {
    Region<TestObjectWithIdentifier, TestObjectWithIdentifier> region =
        getCache().getRegion(regionName);
    fillAllMemoryWithPuts(region, objectId);

    PartitionedRegionDataStore dataStore = ((PartitionedRegion) region).getDataStore();
    assertThat(dataStore.getLocalBucket2RegionMap()).hasSize(1);
  }

  private void fillAllMemoryWithPuts(
      final Region<TestObjectWithIdentifier, TestObjectWithIdentifier> region, final int objectId) {
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;
    assertThat(partitionedRegion.getLocalMaxMemory()).isEqualTo(LOCAL_MAX_MEMORY);

    PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();
    for (int i = 0; dataStore.currentAllocatedMemory() <= EXPECTED_LOCAL_MAX_MEMORY; i++) {
      TestObjectWithIdentifier id =
          new TestObjectWithIdentifier("TestObjectWithIdentifier-" + i, objectId);
      region.put(id, id);
    }
    assertThat(dataStore.getLocalBucket2RegionMap()).hasSize(1);
  }

  private long validateDataStoreExceedsLocalMaxMemory() {
    Region<TestObjectWithIdentifier, TestObjectWithIdentifier> region =
        getCache().getRegion(regionName);
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;
    assertThat(partitionedRegion.getLocalMaxMemory()).isEqualTo(LOCAL_MAX_MEMORY);

    PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();
    assertThat(dataStore.currentAllocatedMemory())
        .isGreaterThanOrEqualTo(EXPECTED_LOCAL_MAX_MEMORY);

    return dataStore.currentAllocatedMemory();
  }

  private void validateDataStoreExceeds(final long memory) {
    Region<TestObjectWithIdentifier, TestObjectWithIdentifier> region =
        getCache().getRegion(regionName);
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;

    PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();
    assertThat(dataStore.currentAllocatedMemory()).isGreaterThanOrEqualTo(memory);
  }

  private void validateEvictionIsEnabled(final boolean evict) {
    Region<TestObjectWithIdentifier, TestObjectWithIdentifier> region =
        getCache().getRegion(regionName);
    RegionAttributes regionAttributes = region.getAttributes();
    if (evict) {
      assertThat(regionAttributes.getEvictionAttributes().getAlgorithm()).isNotEqualTo(NONE);
    } else {
      assertThat(regionAttributes.getEvictionAttributes().getAlgorithm()).isEqualTo(NONE);
    }
  }

  private void createMultiplePartitionRegion(final boolean evict) {
    String value = setSystemProperty(RETRY_TIMEOUT_PROPERTY, RETRY_TIMEOUT_PROPERTY_VALUE);
    try {
      EvictionAttributes evictionAttributes = getEvictionAttributes(evict);
      getCache().createRegion(regionName, createRegionAttrsForPR(REDUNDANCY, LOCAL_MAX_MEMORY,
          RECOVERY_DELAY_DEFAULT, evictionAttributes));
    } finally {
      setSystemProperty(RETRY_TIMEOUT_PROPERTY, value);
    }
  }

  private EvictionAttributes getEvictionAttributes(final boolean evict) {
    if (evict) {
      return createLRUEntryAttributes(Integer.MAX_VALUE, EvictionAction.LOCAL_DESTROY);
    } else {
      return null;
    }
  }

  private String setSystemProperty(final String property, final String value) {
    if (value == null) {
      return System.clearProperty(property);
    } else {
      return System.setProperty(property, value);
    }
  }
}
