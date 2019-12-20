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

import static org.apache.geode.cache.PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT;
import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;

/**
 * Verifies that destroyRegion, localDestroyRegion and close work properly when PartitionedRegion is
 * a subregion.
 */

public class PartitionedRegionAsSubregionDUnitTest extends CacheTestCase {

  private static final int TOTAL_NUM_BUCKETS = GLOBAL_MAX_BUCKETS_DEFAULT;
  private static final int LOCAL_MAX_MEMORY_MB = 200;
  private static final int REDUNDANT_COPIES = 1;

  private static final String PARTITIONED_REGION_NAME = "PARTITIONED_REGION";
  private static final String PARENT_REGION_NAME = "PARENT_REGION";
  private static final String CHILD_REGION_NAME = "CHILD_REGION";

  private VM vm0;
  private VM vm1;

  @Before
  public void setUp() {
    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);

    createParentRegion(vm0);
    createParentRegion(vm1);

    createChildRegion(vm0);
    createChildRegion(vm1);

    createPartitionedSubregion(vm0);
    createPartitionedSubregion(vm1);

    putToCreateEachBucket(vm0);
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
  }

  @Test
  public void createdPartitionedSubregionIsEmptyAfterDestroyParentRegion() {
    destroyChildRegion(vm0);
    validatePartitionedSubregionDoesNotExist(vm0);
    createChildRegion(vm0);
    createPartitionedSubregion(vm0);
    validateBucketsAreGone(vm0);
  }

  @Test
  public void createdPartitionedSubregionStillHasDataAfterLocalDestroyParentRegion() {
    localDestroyChildRegion(vm0);
    validatePartitionedSubregionDoesNotExist(vm0);
    createChildRegion(vm0);
    createPartitionedSubregion(vm0);
    validateActualBucketsStillExist(vm0);
  }

  @Test
  public void createdPartitionedSubregionStillHasDataAfterCloseParentRegion() {
    closeChildRegion(vm0);
    validatePartitionedSubregionDoesNotExist(vm0);
    createChildRegion(vm0);
    createPartitionedSubregion(vm0);
    validateActualBucketsStillExist(vm0);
  }

  private PartitionAttributesFactory<Integer, Integer> createPartitionAttributesFactory() {
    PartitionAttributesFactory<Integer, Integer> partitionAttributesFactory =
        new PartitionAttributesFactory<>();
    partitionAttributesFactory.setLocalMaxMemory(LOCAL_MAX_MEMORY_MB);
    partitionAttributesFactory.setRedundantCopies(REDUNDANT_COPIES);
    partitionAttributesFactory.setTotalNumBuckets(TOTAL_NUM_BUCKETS);
    return partitionAttributesFactory;
  }

  private void createPartitionedSubregion(final VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();
      Region<Integer, Integer> parentRegion =
          cache.getRegion(SEPARATOR + PARENT_REGION_NAME + SEPARATOR + CHILD_REGION_NAME);

      RegionFactory<Integer, Integer> regionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      regionFactory.setPartitionAttributes(createPartitionAttributesFactory().create());

      regionFactory.createSubregion(parentRegion, PARTITIONED_REGION_NAME);
    });
  }

  private void putToCreateEachBucket(final VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();
      Region<Integer, Integer> region = cache.getRegion(
          PARENT_REGION_NAME + SEPARATOR + CHILD_REGION_NAME + SEPARATOR + PARTITIONED_REGION_NAME);

      // using integer keys creates one bucket per entry
      for (int i = 0; i < TOTAL_NUM_BUCKETS; i++) {
        region.put(i, i);
      }
    });
  }

  private void validatePartitionedSubregionDoesNotExist(final VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();

      Region<Integer, Integer> region = cache.getRegion(
          PARENT_REGION_NAME + SEPARATOR + CHILD_REGION_NAME + SEPARATOR + PARTITIONED_REGION_NAME);
      assertThat(region).isNull();
    });
  }

  private void recreatePartitionedRegion(final VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();

      Region<Integer, Integer> region = cache.getRegion(SEPARATOR + PARENT_REGION_NAME + SEPARATOR
          + CHILD_REGION_NAME + SEPARATOR + PARTITIONED_REGION_NAME);
      assertThat(region).isNull();

      Region<Integer, Integer> parentRegion = cache.getRegion(SEPARATOR + PARENT_REGION_NAME);

      // TODO:
      Region<Integer, Integer> childRegion =
          parentRegion.createSubregion(CHILD_REGION_NAME, parentRegion.getAttributes());

      RegionFactory<Integer, Integer> regionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      regionFactory.setPartitionAttributes(createPartitionAttributesFactory().create());

      region = regionFactory.createSubregion(childRegion, PARTITIONED_REGION_NAME);
      assertThat(region).isNotNull();
    });
  }

  private void validateBucketsAreGone(final VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();
      Region<Integer, Integer> region = cache.getRegion(
          PARENT_REGION_NAME + SEPARATOR + CHILD_REGION_NAME + SEPARATOR + PARTITIONED_REGION_NAME);
      int actualBucketsCount =
          ((PartitionedRegion) region).getRegionAdvisor().getCreatedBucketsCount();
      assertThat(actualBucketsCount).isEqualTo(0);
    });
  }

  private void validateActualBucketsStillExist(final VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();
      Region<Integer, Integer> region = cache.getRegion(
          PARENT_REGION_NAME + SEPARATOR + CHILD_REGION_NAME + SEPARATOR + PARTITIONED_REGION_NAME);
      int actualBucketsCount =
          ((PartitionedRegion) region).getRegionAdvisor().getCreatedBucketsCount();
      assertThat(actualBucketsCount).isEqualTo(TOTAL_NUM_BUCKETS);
    });
  }

  private void destroyChildRegion(final VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();
      Region<Object, Object> region =
          cache.getRegion(PARENT_REGION_NAME + SEPARATOR + CHILD_REGION_NAME);
      region.destroyRegion();
    });
  }

  private void localDestroyChildRegion(final VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();
      Region<Object, Object> region =
          cache.getRegion(PARENT_REGION_NAME + SEPARATOR + CHILD_REGION_NAME);
      region.localDestroyRegion();
    });
  }

  private void closeChildRegion(final VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();
      Region<Object, Object> region =
          cache.getRegion(PARENT_REGION_NAME + SEPARATOR + CHILD_REGION_NAME);
      region.close();
    });
  }

  private void createChildRegion(final VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();
      RegionFactory<Object, Object> regionFactory =
          cache.createRegionFactory(RegionShortcut.REPLICATE);
      Region<Object, Object> parentRegion = cache.getRegion(PARENT_REGION_NAME);
      regionFactory.createSubregion(parentRegion, CHILD_REGION_NAME);
    });
  }

  private void createParentRegion(final VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();
      RegionFactory<Object, Object> regionFactory =
          cache.createRegionFactory(RegionShortcut.REPLICATE);
      regionFactory.create(PARENT_REGION_NAME);
    });
  }
}
