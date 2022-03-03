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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache30.TestCacheLoader;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;

/**
 * This is a dunit test for PartitionedRegion creation and Region API's functionality.
 */

@SuppressWarnings("serial")
public class PartitionedRegionAPIDUnitTest extends CacheTestCase {

  private static final int totalNumBuckets = 5;

  private static final int putRange_1Start = 1;
  private static final int putRange_1End = 5;
  private static final int putRange_2Start = 6;
  private static final int putRange_2End = 10;
  private static final int putRange_3Start = 11;
  private static final int putRange_3End = 15;
  private static final int putRange_4Start = 16;
  private static final int putRange_4End = 20;
  private static final int removeRange_1Start = 2;
  private static final int removeRange_1End = 4;
  private static final int removeRange_2Start = 7;
  private static final int removeRange_2End = 9;

  // Create counters
  private static final int createRange_1Start = 21;
  private static final int createRange_1End = 25;
  private static final int createRange_2Start = 26;
  private static final int createRange_2End = 30;
  private static final int createRange_3Start = 31;
  private static final int createRange_3End = 35;
  private static final int createRange_4Start = 36;
  private static final int createRange_4End = 40;

  // Invalidate Counters
  private static final int invalidateRange_1Start = 41;
  private static final int invalidateRange_1End = 45;
  private static final int invalidateRange_2Start = 46;
  private static final int invalidateRange_2End = 50;

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  private String regionName;
  private int localMaxMemory;
  private String key1;
  private String cacheLoaderArg;

  @Before
  public void setUp() throws Exception {
    regionName = getUniqueName();

    vm0 = getHost(0).getVM(0);
    vm1 = getHost(0).getVM(1);
    vm2 = getHost(0).getVM(2);
    vm3 = getHost(0).getVM(3);

    localMaxMemory = 10;
    key1 = "key1";
    cacheLoaderArg = "loaderArg";
  }

  @After
  public void tearDown() throws Exception {
    disconnectAllFromDS();
  }

  /**
   * This is a PartitionedRegion test for scope = D_ACK. 4 VMs are used to create the PR with and
   * without(Only Accessor) the DataStore.
   */
  @Test
  public void testPartitionedRegionOperationsScopeDistAck() throws Exception {
    vm0.invoke(() -> createPartitionedRegion());
    vm1.invoke(this::createAccessor);
    vm2.invoke(() -> createPartitionedRegion());
    vm3.invoke(this::createAccessor);

    validatePutAndCreateAndKeySetIteratorInDatastore(vm0);
    validatePutAndCreateInAccessor(vm1);
    validateDestroyInAccessor(vm1);
    validatePutAndCreateInDatastore(vm2);
    validateDestroyInDatastore(vm2);
    validatePutAndCreateInAccessorAgain(vm3);
    validateGetAndContainsInAll();

    destroyPartitionedRegion(vm0);
    validateMetaDataAfterRegionDestroyInAll();

    vm0.invoke(() -> createPartitionedRegion());
    vm1.invoke(this::createAccessor);
    vm2.invoke(() -> createPartitionedRegion());
    vm3.invoke(this::createAccessor);

    validateBasicOpsOnDatastore(vm0);
    validateBasicOpsOnAccessor(vm1);

    destroyPartitionedRegion(vm0);
  }

  @Test
  public void testPartitionedRegionConcurrentOperations() throws Exception {
    vm0.invoke(() -> createPartitionedRegion());
    vm1.invoke(this::createAccessor);
    vm2.invoke(() -> createPartitionedRegion());
    vm3.invoke(this::createAccessor);

    validateConcurrentMapOps(vm0);

    destroyPartitionedRegion(vm0);
  }

  @Test
  public void localMaxMemoryShouldDefaultToNonZero() throws Exception {
    vm0.invoke(() -> {
      Cache cache = getCache();
      RegionFactory<String, String> regionFactory =
          cache.createRegionFactory(RegionShortcut.PARTITION);
      Region<String, String> region = regionFactory.create(regionName);

      assertThat(region.getAttributes().getPartitionAttributes().getLocalMaxMemory())
          .isNotEqualTo(0);
    });

    destroyPartitionedRegion(vm0);
  }

  @Test
  public void accessorTriggersCacheLoader() throws Exception {
    vm2.invoke(this::createPartitionedRegionWithCacheLoader);
    vm3.invoke(this::createPartitionedRegionWithCacheLoader);

    // create a "pure" accessor, no data storage
    Cache cache = getCache();

    PartitionAttributesFactory<String, String> partitionAttributesFactory =
        new PartitionAttributesFactory<>();
    partitionAttributesFactory.setRedundantCopies(1);
    partitionAttributesFactory.setLocalMaxMemory(0);

    RegionFactory<String, String> regionFactory =
        cache.createRegionFactory(RegionShortcut.PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    Region<String, String> region = regionFactory.create(regionName);

    assertThat(region.get(key1, cacheLoaderArg)).isEqualTo(cacheLoaderArg);
  }

  private void createPartitionedRegion() {
    Cache cache = getCache();

    PartitionAttributesFactory<String, String> partitionAttributesFactory =
        new PartitionAttributesFactory<>();
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);

    RegionFactory<String, String> regionFactory = cache.createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(regionName);
  }

  private void createAccessor() {
    Cache cache = getCache();

    PartitionAttributesFactory<String, String> partitionAttributesFactory =
        new PartitionAttributesFactory<>();
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);
    partitionAttributesFactory.setLocalMaxMemory(0);

    RegionFactory<String, String> regionFactory = cache.createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(regionName);
  }

  private void createPartitionedRegionWithCacheLoader() {
    Cache cache = getCache();

    CacheLoader<String, String> cacheLoader = new TestCacheLoader() {
      @Override
      public Object load2(final LoaderHelper helper) throws CacheLoaderException {
        assertThat(helper).isNotNull();
        assertThat(helper.getKey()).isEqualTo(key1);
        assertThat(helper.getRegion().getName()).isEqualTo(regionName);
        assertThat(helper.getArgument()).isEqualTo(cacheLoaderArg);
        return helper.getArgument();
      }
    };

    PartitionAttributesFactory<String, String> partitionAttributesFactory =
        new PartitionAttributesFactory<>();
    partitionAttributesFactory.setLocalMaxMemory(localMaxMemory);
    partitionAttributesFactory.setRedundantCopies(1);

    RegionFactory<String, String> regionFactory = cache.createRegionFactory(PARTITION);
    regionFactory.setCacheLoader(cacheLoader);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    PartitionedRegion partitionedRegion = (PartitionedRegion) regionFactory.create(regionName);

    assertThat(partitionedRegion.getDataStore().getCacheLoader()).isSameAs(cacheLoader);
  }

  private void destroyPartitionedRegion(final VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();
      Region<String, String> region = cache.getRegion(regionName);
      region.destroyRegion();

      assertThat(region.isDestroyed()).isTrue();
      assertThat(cache.getRegion(regionName)).isNull();
    });
  }

  private void validateConcurrentMapOps(final VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();
      Region<String, String> region = cache.getRegion(regionName);

      // putIfAbsent returns null if success
      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        String key = Integer.toString(i);
        String value = Integer.toString(i);

        String result = region.putIfAbsent(key, value);

        assertThat(result).isNull();
      }

      assertThat(region.size()).isEqualTo(putRange_1End);
      assertThat(region.isEmpty()).isFalse();

      // putIfAbsent returns existing value if failure
      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        String key = Integer.toString(i);
        String oldValue = Integer.toString(i);
        String newValue = Integer.toString(i + 1);

        String result = region.putIfAbsent(key, newValue);

        assertThat(result).isEqualTo(oldValue);
        assertThat(region.get(key)).isEqualTo(oldValue);
      }

      assertThat(region.size()).isEqualTo(putRange_1End);
      assertThat(region.isEmpty()).isFalse();

      // replace(key, oldValue, newValue) returns true if success
      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        String key = Integer.toString(i);
        String oldValue = Integer.toString(i);
        String newValue = "replaced" + i;

        boolean result = region.replace(key, oldValue, newValue);

        assertThat(result).isTrue();
        assertThat(region.get(key)).isEqualTo(newValue);
      }

      assertThat(region.size()).isEqualTo(putRange_1End);
      assertThat(region.isEmpty()).isFalse();

      // replace(key, oldValue, newValue) returns false if failure
      for (int i = putRange_1Start; i <= putRange_2End; i++) {
        // wrong expected old value
        String key = Integer.toString(i);
        String actualValue = i <= putRange_1End ? "replaced" + i : null;
        String wrongValue = Integer.toString(i);
        String newValue = "not" + i;

        boolean result = region.replace(key, wrongValue, newValue);

        assertThat(result).isFalse();
        assertThat(region.get(key)).isEqualTo(actualValue);
      }

      assertThat(region.size()).isEqualTo(putRange_1End);
      assertThat(region.isEmpty()).isFalse();

      // replace(key, value) returns old value if success
      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        String key = Integer.toString(i);
        String oldValue = "replaced" + i;
        String newValue = "twice replaced" + i;

        String result = region.replace(key, newValue);

        assertThat(result).isEqualTo(oldValue);
        assertThat(region.get(Integer.toString(i))).isEqualTo(newValue);
      }

      assertThat(region.size()).isEqualTo(putRange_1End);
      assertThat(region.isEmpty()).isFalse();

      // replace(key, value) returns null if failure
      for (int i = putRange_2Start; i <= putRange_2End; i++) {
        String key = Integer.toString(i);
        String newValue = "thrice replaced" + i;

        String result = region.replace(key, newValue);

        assertThat(result).isNull();
        assertThat(region.get(key)).isNull();
      }

      assertThat(region.size()).isEqualTo(putRange_1End);
      assertThat(region.isEmpty()).isFalse();

      // remove(key, value) returns false if failure
      for (int i = putRange_1Start; i <= putRange_2End; i++) {
        String key = Integer.toString(i);
        String actualValue = i <= putRange_1End ? "twice replaced" + i : null;
        String wrongValue = Integer.toString(-i);

        boolean result = region.remove(key, wrongValue);

        assertThat(result).isFalse();
        assertThat(region.get(key)).isEqualTo(actualValue);
      }

      assertThat(region.size()).isEqualTo(putRange_1End);
      assertThat(region.isEmpty()).isFalse();

      // remove(key, value) returns true if success
      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        String key = Integer.toString(i);
        String value = "twice replaced" + i;

        boolean result = region.remove(key, value);

        assertThat(result).isTrue();
        assertThat(region.get(key)).isNull();
      }

      assertThat(region.size()).isEqualTo(0);
      assertThat(region.isEmpty()).isTrue();
    });
  }

  private void validateBasicOpsOnDatastore(final VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();
      Region<String, String> region = cache.getRegion(regionName);

      // put
      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        String key = "" + i;
        String value = "" + i;

        String result = region.put(key, value);

        assertThat(result).isNull();
      }

      // create
      for (int i = createRange_1Start; i <= createRange_1End; i++) {
        String key = "" + i;
        String value = i % 2 == 0 ? "" + i : null;

        region.create(key, value);
      }

      // create throws EntryExistsException
      for (int i = createRange_1Start; i <= createRange_1End; i++) {
        String key = "" + i;
        String value = i % 2 == 0 ? "" + i : null;

        try (IgnoredException ignored = addIgnoredException(EntryExistsException.class.getName())) {
          assertThatThrownBy(() -> region.create(key, value))
              .isInstanceOf(EntryExistsException.class);
        }
      }

      // invalidate throws EntryNotFoundException
      for (int i = invalidateRange_1Start; i <= invalidateRange_1End; i++) {
        String key = Integer.toString(i);
        String value = Integer.toString(i);

        // invalidate op throws EntryNotFoundException
        try (IgnoredException ignored =
            addIgnoredException(EntryNotFoundException.class.getName())) {
          assertThatThrownBy(() -> region.invalidate(key))
              .isInstanceOf(EntryNotFoundException.class);
        }

        // repopulate
        region.create(key, value);

        assertThat(region.containsValueForKey(key)).isTrue();
        assertThat(region.get(key)).isEqualTo(value);

        // invalidate op
        region.invalidate(key);

        assertThat(region.containsValueForKey(key)).isFalse();
        assertThat(region.get(key)).isNull();
      }

      // destroy op
      for (int i = invalidateRange_1Start; i <= invalidateRange_1End; i++) {
        String key = Integer.toString(i);

        region.destroy(key);

        assertThat(region.containsKey(key)).isFalse();
        assertThat(region.get(key)).isNull();
      }

      // invalidate op throws EntryNotFoundException
      try (IgnoredException ignored = addIgnoredException(EntryNotFoundException.class.getName())) {
        for (int i = invalidateRange_1Start; i <= invalidateRange_1End; i++) {
          String key = "" + i;

          assertThatThrownBy(() -> region.invalidate(key))
              .isInstanceOf(EntryNotFoundException.class);
        }
      }
    });
  }

  private void validateBasicOpsOnAccessor(final VM vm) {
    vm.invoke(() -> {
      Cache cache = getCache();
      Region<String, String> region = cache.getRegion(regionName);

      // populate region
      for (int i = putRange_2Start; i <= putRange_2End; i++) {
        String key = "" + i;
        String value = "" + i;

        String result = region.put(key, value);

        assertThat(result).isNull();
      }

      // create
      for (int i = createRange_2Start; i <= createRange_2End; i++) {
        String key = "" + i;
        String value = i % 2 == 0 ? "" + i : null;

        region.create(key, value);
      }

      // create throws EntryExistsException
      try (IgnoredException ignored1 = addIgnoredException(EntryExistsException.class.getName());
          IgnoredException ignored2 = addIgnoredException(ReplyException.class.getName())) {
        for (int i = createRange_2Start; i <= createRange_2End; i++) {
          String key = "" + i;
          String value = i % 2 == 0 ? "" + i : null;

          assertThatThrownBy(() -> region.create(key, value))
              .isInstanceOf(EntryExistsException.class);
        }
      }

      // invalidate throws EntryNotFoundException THEN repopulate region
      try (IgnoredException ignored1 = addIgnoredException(EntryNotFoundException.class.getName());
          IgnoredException ignored2 = addIgnoredException(ReplyException.class.getName())) {
        for (int i = invalidateRange_2Start; i <= invalidateRange_2End; i++) {
          String key = Integer.toString(i);
          String value = Integer.toString(i);

          assertThatThrownBy(() -> region.invalidate(key))
              .isInstanceOf(EntryNotFoundException.class);

          region.create(key, value);
        }
      }

      // invalidate
      for (int i = invalidateRange_2Start; i <= invalidateRange_2End; i++) {
        String key = Integer.toString(i);
        String value = Integer.toString(i);

        assertThat(region.containsKey(key)).isTrue();
        assertThat(region.containsValueForKey(key)).isTrue();
        assertThat(region.get(key)).isEqualTo(value);

        region.invalidate(key);

        assertThat(region.containsKey(key)).isTrue();
        assertThat(region.containsValueForKey(key)).isFalse();
        assertThat(region.get(key)).isNull();
      }

      // destroy
      for (int i = invalidateRange_2Start; i <= invalidateRange_2End; i++) {
        String key = Integer.toString(i);

        region.destroy(key);

        assertThat(region.containsKey(key)).isFalse();
        assertThat(region.containsValueForKey(key)).isFalse();
        assertThat(region.get(key)).isNull();
      }

      // invalidate throws EntryNotFoundException
      try (IgnoredException ignored1 = addIgnoredException(EntryNotFoundException.class.getName());
          IgnoredException ignored2 = addIgnoredException(ReplyException.class.getName())) {
        for (int i = invalidateRange_2Start; i <= invalidateRange_2End; i++) {
          String key = Integer.toString(i);

          assertThatThrownBy(() -> region.invalidate(key))
              .isInstanceOf(EntryNotFoundException.class);
        }
      }
    });
  }

  private void validatePutAndCreateAndKeySetIteratorInDatastore(final VM vm0) {
    vm0.invoke(() -> {
      Cache cache = getCache();
      Region<String, String> region = cache.getRegion(regionName);

      // size and isEmpty and keySet
      assertThat(region.size()).isEqualTo(0);
      assertThat(region.isEmpty()).isTrue();
      assertThat(region.keySet().size()).isEqualTo(0);

      // put
      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        region.put(Integer.toString(i), Integer.toString(i));
      }

      // size and isEmpty
      assertThat(region.size()).isEqualTo(putRange_1End);
      assertThat(region.isEmpty()).isFalse();

      // Positive assertion of functionality in a distributed env.
      // For basic functional support (or lack of), please see
      // PartitionedRegionSingleNodeOperationsJUnitTest

      // keys
      Set<String> keySet = region.keySet();
      assertThat(keySet.size()).isEqualTo(putRange_1End);

      // keySet iterator
      Iterator<String> iterator = keySet.iterator();
      while (iterator.hasNext()) {
        assertThatThrownBy(iterator::remove).isInstanceOf(Exception.class);
        String key = iterator.next();
        assertThat(key.getClass()).isEqualTo(String.class);
      }

      assertThatThrownBy(iterator::remove).isInstanceOf(Exception.class);
      assertThat(iterator.hasNext()).isFalse();

      assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
      assertThat(iterator.hasNext()).isFalse();

      // destroy
      try (IgnoredException ignored1 = addIgnoredException(EntryNotFoundException.class.getName());
          IgnoredException ignored2 = addIgnoredException(ReplyException.class.getName())) {
        for (int i = putRange_1Start; i <= putRange_1End; i++) {
          region.destroy(Integer.toString(i));
        }
      }

      // size and isEmpty
      assertThat(region.size()).isEqualTo(0);
      assertThat(region.isEmpty()).isTrue();

      // put
      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        String key = Integer.toString(i);
        String value = Integer.toString(i);

        region.put(key, value);
      }

      // create
      for (int i = createRange_1Start; i <= createRange_1End; i++) {
        String key = Integer.toString(i);
        String value = i % 2 == 0 ? Integer.toString(i) : null;

        region.create(key, value);
      }

      // create throws EntryExistsException
      try (IgnoredException ignored1 = addIgnoredException(EntryExistsException.class.getName());
          IgnoredException ignored2 = addIgnoredException(ReplyException.class.getName())) {
        for (int i = createRange_1Start; i <= createRange_1End; i++) {
          String key = Integer.toString(i);
          String value = i % 2 == 0 ? Integer.toString(i) : null;

          assertThatThrownBy(() -> region.create(key, value))
              .isInstanceOf(EntryExistsException.class);
        }
      }
    });
  }

  private void validatePutAndCreateInAccessor(final VM vm1) {
    vm1.invoke(() -> {
      Cache cache = getCache();
      Region<String, String> region = cache.getRegion(regionName);

      // put
      for (int i = putRange_2Start; i <= putRange_2End; i++) {
        String key = Integer.toString(i);
        String value = Integer.toString(i);

        region.put(key, value);
      }

      // create
      for (int i = createRange_2Start; i <= createRange_2End; i++) {
        String key = Integer.toString(i);
        String value = i % 2 == 0 ? Integer.toString(i) : null;

        region.create(key, value);
      }

      // create throws EntryExistsException
      try (IgnoredException ignored1 = addIgnoredException(EntryExistsException.class.getName());
          IgnoredException ignored2 = addIgnoredException(ReplyException.class.getName())) {
        for (int i = createRange_2Start; i <= createRange_2End; i++) {
          String key = Integer.toString(i);
          String value = i % 2 == 0 ? Integer.toString(i) : null;

          assertThatThrownBy(() -> region.create(key, value))
              .isInstanceOf(EntryExistsException.class);
        }
      }
    });
  }

  private void validateDestroyInAccessor(final VM vm1) {
    vm1.invoke(() -> {
      Cache cache = getCache();
      Region<String, String> region = cache.getRegion(regionName);

      // destroy
      for (int i = removeRange_1Start; i <= removeRange_1End; i++) {
        String key = Integer.toString(i);

        region.destroy(key);
      }

      // destroy throws EntryNotFoundException
      try (IgnoredException ignored1 = addIgnoredException(EntryNotFoundException.class.getName());
          IgnoredException ignored2 = addIgnoredException(ReplyException.class.getName())) {
        for (int i = removeRange_1Start; i <= removeRange_1End; i++) {
          String key = Integer.toString(i);

          assertThatThrownBy(() -> region.destroy(key)).isInstanceOf(EntryNotFoundException.class);
        }
      }
    });
  }

  private void validatePutAndCreateInDatastore(final VM vm2) {
    vm2.invoke(() -> {
      Cache cache = getCache();
      Region<String, String> region = cache.getRegion(regionName);

      // put
      for (int i = putRange_3Start; i <= putRange_3End; i++) {
        String key = Integer.toString(i);
        String value = Integer.toString(i);

        region.put(key, value);
      }

      // create
      for (int i = createRange_3Start; i <= createRange_3End; i++) {
        String key = Integer.toString(i);
        String value = i % 2 == 0 ? Integer.toString(i) : null;

        region.create(key, value);
      }

      // create throws EntryExistsException
      try (IgnoredException ignored1 = addIgnoredException(EntryExistsException.class.getName());
          IgnoredException ignored2 = addIgnoredException(ReplyException.class.getName())) {
        for (int i = createRange_3Start; i <= createRange_3End; i++) {
          String key = Integer.toString(i);
          String value = i % 2 == 0 ? Integer.toString(i) : null;

          assertThatThrownBy(() -> region.create(key, value))
              .isInstanceOf(EntryExistsException.class);
        }
      }
    });
  }

  private void validateDestroyInDatastore(final VM vm2) {
    vm2.invoke(() -> {
      Cache cache = getCache();
      Region<String, String> region = cache.getRegion(regionName);

      // destroy
      for (int i = removeRange_2Start; i <= removeRange_2End; i++) {
        String key = Integer.toString(i);

        region.destroy(key);
      }

      // destroy throws EntryNotFoundException
      try (IgnoredException ignored1 = addIgnoredException(EntryNotFoundException.class.getName());
          IgnoredException ignored2 = addIgnoredException(ReplyException.class.getName())) {
        for (int i = removeRange_2Start; i <= removeRange_2End; i++) {
          String key = Integer.toString(i);

          assertThatThrownBy(() -> region.destroy(key)).isInstanceOf(EntryNotFoundException.class);
        }
      }
    });
  }

  private void validatePutAndCreateInAccessorAgain(final VM vm3) {
    vm3.invoke(() -> {
      Cache cache = getCache();
      Region<String, String> region = cache.getRegion(regionName);

      // populate
      for (int i = putRange_4Start; i <= putRange_4End; i++) {
        String key = Integer.toString(i);
        String value = Integer.toString(i);

        region.put(key, value);
      }

      // create
      for (int i = createRange_4Start; i <= createRange_4End; i++) {
        String key = Integer.toString(i);
        String value = i % 2 == 0 ? Integer.toString(i) : null;

        region.create(key, value);
      }

      // create throws EntryExistsException
      try (IgnoredException ignored1 = addIgnoredException(EntryExistsException.class.getName());
          IgnoredException ignored2 = addIgnoredException(ReplyException.class.getName())) {
        for (int i = createRange_4Start; i <= createRange_4End; i++) {
          String key = Integer.toString(i);
          String value = i % 2 == 0 ? Integer.toString(i) : null;

          assertThatThrownBy(() -> region.create(key, value))
              .isInstanceOf(EntryExistsException.class);
        }
      }
    });
  }

  private void validateGetAndContainsInAll() {
    for (int vm = 0; vm < 4; vm++) {
      getHost(0).getVM(vm).invoke(() -> {
        Cache cache = getCache();
        Region<String, String> region = cache.getRegion(regionName);

        // get
        for (int i = putRange_1Start; i <= putRange_4End; i++) {
          String value = region.get(Integer.toString(i));
          if ((i >= removeRange_1Start && i <= removeRange_1End)
              || (i >= removeRange_2Start && i <= removeRange_2End)) {
            assertThat(value).isNull();
          } else {
            assertThat(value).isNotNull();
          }
        }

        // containsKey
        for (int i = putRange_1Start; i <= putRange_4End; i++) {
          boolean containsKey = region.containsKey(Integer.toString(i));
          if ((i >= removeRange_1Start && i <= removeRange_1End)
              || (i >= removeRange_2Start && i <= removeRange_2End)) {
            assertThat(containsKey).isFalse();
          } else {
            assertThat(containsKey).isTrue();
          }
        }

        // containsValueForKey
        for (int i = putRange_1Start; i <= putRange_4End; i++) {
          boolean containsValueForKey = region.containsValueForKey(Integer.toString(i));
          if ((i >= removeRange_1Start && i <= removeRange_1End)
              || (i >= removeRange_2Start && i <= removeRange_2End)) {
            assertThat(containsValueForKey).isFalse();
          } else {
            assertThat(containsValueForKey).isTrue();
          }
        }
      });
    }
  }

  private void validateMetaDataAfterRegionDestroyInAll() {
    for (int vm = 0; vm < 4; vm++) {
      getHost(0).getVM(vm).invoke(() -> {
        Cache cache = getCache();
        Region<String, String> region = cache.getRegion(regionName);
        assertThat(region).isNull();

        Region rootRegion =
            cache.getRegion(SEPARATOR + PartitionedRegionHelper.PR_ROOT_REGION_NAME);

        Object configObject = rootRegion.get(regionName.substring(1));
        assertThat(configObject).isNull();

        Set<Region> subregions = rootRegion.subregions(false);
        for (Region subregion : subregions) {
          String name = subregion.getName();
          assertThat(name).doesNotContain(PartitionedRegionHelper.BUCKET_REGION_PREFIX);
        }

        boolean containsKey = PartitionedRegion.getPrIdToPR().containsKey(regionName);
        assertThat(containsKey).isFalse();
      });
    }
  }
}
