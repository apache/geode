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
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache30.CacheSerializableRunnable;
import org.apache.geode.cache30.TestCacheLoader;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

/**
 * This is a dunit test for PartitionedRegion creation and Region API's functionality. This test is
 * performed for different region scopes - D_ACK and D_NO_ACK for PartitionedRegion.
 */
@Category(DistributedTest.class)
@SuppressWarnings("serial")
public class PartitionedRegionAPIDUnitTest extends CacheTestCase {

  private static final String REGION_NAME = "PR1";

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
  private VM accessorVM3;
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
    accessorVM3 = vm3;

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
    vm0.invoke("createPartitionedRegion", () -> createPartitionedRegion());
    vm1.invoke("createAccessor", () -> createAccessor());
    vm2.invoke("createPartitionedRegion", () -> createPartitionedRegion());

    accessorVM3.invoke("createAccessor", () -> createAccessor());

    validatePartitionedRegionOps(REGION_NAME);

    vm0.invoke("createPartitionedRegion", () -> createPartitionedRegion());
    vm1.invoke("createAccessor", () -> createAccessor());
    vm2.invoke("createPartitionedRegion", () -> createPartitionedRegion());
    accessorVM3.invoke("createAccessor", () -> createAccessor());

    validateDestroyedPartitionedRegion(REGION_NAME);

    destroyPartitionedRegion(vm0, REGION_NAME);
  }

  /**
   * This is a PartitionedRegion test for the ConcurrentMap operations for scope = D_ACK. 4 VMs are
   * used to create the PR with and without(Only Accessor) the DataStore.
   */
  @Test
  public void testPartitionedRegionConcurrentOperations() throws Exception {
    vm0.invoke("createPartitionedRegion", () -> createPartitionedRegion());
    vm1.invoke("createAccessor", () -> createAccessor());
    vm2.invoke("createPartitionedRegion", () -> createPartitionedRegion());

    accessorVM3.invoke("createAccessor", () -> createAccessor());

    validatePartitionedRegionConcurrentMapOps(vm0, REGION_NAME);
  }

  /**
   * Verify that localMaxMemory is set correctly when using attributes
   */
  @Test
  public void localMaxMemoryShouldNotDefaultToZero() throws Exception {
    vm0.invoke(() -> {
      Cache cache = getCache();
      AttributesFactory attr = new AttributesFactory();
      attr.setDataPolicy(DataPolicy.PARTITION);
      RegionAttributes regionAttribs = attr.create();
      Region partitionedregion = cache.createRegion(regionName, regionAttribs);
      assertThat(partitionedregion).isNotNull();
      assertThat(cache.getRegion(regionName)).isNotNull();
      PartitionAttributes p = regionAttribs.getPartitionAttributes();
      int maxMem = p.getLocalMaxMemory();
      assertThat(maxMem != 0).isTrue();
    });

    destroyPartitionedRegion(vm0, regionName);
  }

  @Test
  public void testCacheLoaderHelper() throws Exception {
    vm2.invoke("createPartitionedRegionWithCacheLoader", () -> createPartitionedRegionWithCacheLoader());
    vm3.invoke("createPartitionedRegionWithCacheLoader", () -> createPartitionedRegionWithCacheLoader());

    // create a "pure" accessor, no data storage
    getCache();

    Region region = new RegionFactory()
        .setPartitionAttributes(
            new PartitionAttributesFactory().setRedundantCopies(1).setLocalMaxMemory(0).create())
        .create(regionName);

    assertThat(region.get(key1, cacheLoaderArg)).isEqualTo(cacheLoaderArg);
  }

  private void createPartitionedRegion() {
    Cache cache = getCache();

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);

    RegionFactory regionFactory = cache.createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(REGION_NAME);
  }

  private void createAccessor() {
    Cache cache = getCache();

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setTotalNumBuckets(totalNumBuckets);
    partitionAttributesFactory.setLocalMaxMemory(0);

    RegionFactory regionFactory = cache.createRegionFactory(PARTITION);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    regionFactory.create(REGION_NAME);
  }

  private void createPartitionedRegionWithCacheLoader() {
    Cache cache = getCache();

    CacheLoader cacheLoader = new TestCacheLoader() {
      @Override
      public Object load2(LoaderHelper helper) throws CacheLoaderException {
        assertThat(helper).isNotNull();
        assertThat(helper.getKey()).isEqualTo(key1);
        assertThat(helper.getRegion().getName()).isEqualTo(regionName);
        assertThat(helper.getArgument()).isEqualTo(cacheLoaderArg);
        return helper.getArgument();
      }
    };

    PartitionAttributesFactory partitionAttributesFactory = new PartitionAttributesFactory();
    partitionAttributesFactory.setLocalMaxMemory(localMaxMemory);
    partitionAttributesFactory.setRedundantCopies(1);

    RegionFactory regionFactory = cache.createRegionFactory(PARTITION);
    regionFactory.setCacheLoader(cacheLoader);
    regionFactory.setPartitionAttributes(partitionAttributesFactory.create());

    PartitionedRegion partitionedRegion = (PartitionedRegion) regionFactory.create(regionName);

    assertThat(partitionedRegion.getDataStore().getCacheLoader()).isSameAs(cacheLoader);
  }

  private void destroyPartitionedRegion(final VM vm, final String regionName) {
    vm.invoke(() -> {
      Cache cache = getCache();
      Region region = cache.getRegion(regionName);
      region.destroyRegion();

      assertThat(region.isDestroyed()).isTrue();
      assertThat(cache.getRegion(regionName)).isNull();
    });
  }

  /**
   * Do putIfAbsent(), replace(Object, Object), replace(Object, Object, Object), remove(Object,
   * Object) operations through VM with PR having both Accessor and Datastore
   */
  private void validatePartitionedRegionConcurrentMapOps(final VM vm, final String regionName) {
    vm.invoke(() -> {
      Cache cache = getCache();
      Region region = cache.getRegion(regionName);

      // test successful putIfAbsent
      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        Object putResult = region.putIfAbsent(Integer.toString(i), Integer.toString(i));
        assertThat(putResult).isNull();
      }
      int size = region.size();
      assertThat(size).isEqualTo(putRange_1End);
      assertThat(region.isEmpty()).isFalse();

      // test unsuccessful putIfAbsent
      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        Object putResult = region.putIfAbsent(Integer.toString(i), Integer.toString(i + 1));
        assertThat(putResult).isEqualTo(Integer.toString(i));
        assertThat(region.get(Integer.toString(i))).isEqualTo(Integer.toString(i));
      }
      size = region.size();
      assertThat(size).isEqualTo(putRange_1End);
      assertThat(region.isEmpty()).isFalse();

      // test successful replace(key, oldValue, newValue)
      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        boolean replaceSucceeded =
            region.replace(Integer.toString(i), Integer.toString(i), "replaced" + i);
        assertThat(replaceSucceeded).isTrue();
        assertThat(region.get(Integer.toString(i))).isEqualTo("replaced" + i);
      }
      size = region.size();
      assertThat(size).isEqualTo(putRange_1End);
      assertThat(region.isEmpty()).isFalse();

      // test unsuccessful replace(key, oldValue, newValue)
      for (int i = putRange_1Start; i <= putRange_2End; i++) {
        // wrong expected old value
        boolean replaceSucceeded = region.replace(Integer.toString(i), Integer.toString(i), "not" + i);
        assertThat(replaceSucceeded).isFalse();
        assertThat(region.get(Integer.toString(i))).isEqualTo(i <= putRange_1End ? "replaced" + i : null);
      }
      size = region.size();
      assertThat(size).isEqualTo(putRange_1End);
      assertThat(region.isEmpty()).isFalse();

      // test successful replace(key, value)
      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        Object replaceResult = region.replace(Integer.toString(i), "twice replaced" + i);
        assertThat(replaceResult).isEqualTo("replaced" + i);
        assertThat(region.get(Integer.toString(i))).isEqualTo("twice replaced" + i);
      }
      size = region.size();
      assertThat(size).isEqualTo(putRange_1End);
      assertThat(region.isEmpty()).isFalse();

      // test unsuccessful replace(key, value)
      for (int i = putRange_2Start; i <= putRange_2End; i++) {
        Object replaceResult = region.replace(Integer.toString(i), "thrice replaced" + i);
        assertThat(replaceResult).isNull();
        assertThat(region.get(Integer.toString(i))).isNull();
      }
      size = region.size();
      assertThat(size).isEqualTo(putRange_1End);
      assertThat(region.isEmpty()).isFalse();

      // test unsuccessful remove(key, value)
      for (int i = putRange_1Start; i <= putRange_2End; i++) {
        boolean removeResult = region.remove(Integer.toString(i), Integer.toString(-i));
        assertThat(removeResult).isFalse();
        assertThat(region.get(Integer.toString(i))).isEqualTo(i <= putRange_1End ? "twice replaced" + i : null);
      }
      size = region.size();
      assertThat(size).isEqualTo(putRange_1End);
      assertThat(region.isEmpty()).isFalse();

      // test successful remove(key, value)
      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        boolean removeResult = region.remove(Integer.toString(i), "twice replaced" + i);
        assertThat(removeResult).isTrue();
        assertThat(region.get(Integer.toString(i))).isNull();
      }
      size = region.size();
      assertThat(size).isEqualTo(0);
      assertThat(region.isEmpty()).isTrue();
    });

    destroyPartitionedRegion(vm, regionName);
  }

  /**
   * Test the Region operations after the PartitionedRegion has been destroyed
   */
  private void validateDestroyedPartitionedRegion(final String regionName) {
    /*
     * do some put(), create(), invalidate() operations for PR with accessor + Datastore and
     * validate.
     */
    vm0.invoke(() -> {
      Cache cache = getCache();
      Region region = cache.getRegion(regionName);

      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        Object key = "" + i;
        Object value = "" + i;
        region.put(key, value);
      }

      // Create Operation
      for (int i = createRange_1Start; i <= createRange_1End; i++) {
        Object key = "" + i;
        Object value = i % 2 == 0 ? "" + i : null;
        region.create(key, value);
      }

      for (int i = createRange_1Start; i <= createRange_1End; i++) {
        Object key = "" + i;
        Object value = i % 2 == 0 ? "" + i : null;

        try (IgnoredException ignored = addIgnoredException(EntryExistsException.class.getName())) {
          assertThatThrownBy(() -> region.create(key, value)).isInstanceOf(EntryExistsException.class);
        }
      }

      // Invalidate Operations
      for (int i = invalidateRange_1Start; i <= invalidateRange_1End; i++) {
        // Check that before creating an entry it throws EntryNotFoundException
        Object key = Integer.toString(i);
        Object value = Integer.toString(i);

        try (IgnoredException ignored = addIgnoredException(EntryNotFoundException.class.getName())) {
          assertThatThrownBy(() -> region.invalidate(key)).isInstanceOf(EntryNotFoundException.class);
        }

        region.create(key, value);
        assertThat(region.containsValueForKey(key)).isTrue();
        assertThat(region.get(key)).isEqualTo(value);

        region.invalidate(key);
        assertThat(region.containsValueForKey(key)).isFalse();
        assertThat(region.get(key)).isNull();
      }

      for (int i = invalidateRange_1Start; i <= invalidateRange_1End; i++) {
        Object key = Integer.toString(i);
        region.destroy(key);
      }

      try (IgnoredException ignored = addIgnoredException(EntryNotFoundException.class.getName())) {
        for (int i = invalidateRange_1Start; i <= invalidateRange_1End; i++) {
          // invalidate for missing entry throws EntryNotFoundException
          Object key = "" + i;
          assertThatThrownBy(() -> region.invalidate(key)).isInstanceOf(EntryNotFoundException.class);
        }
      }
    });

    /*
     * do some put(), create(), invalidate() operations for PR with only accessor and validate.
     */
    vm1.invoke(() -> {
      Cache cache = getCache();
      String exceptionStr = "";
      Region pr = cache.getRegion(regionName);
      assertThat(pr).isNotNull();

      for (int i = putRange_2Start; i <= putRange_2End; i++) {
        pr.put("" + i, "" + i);
      }

      // Create Operation
      for (int i = createRange_2Start; i <= createRange_2End; i++) {
        Object val = null;
        Object key = "" + i;
        if (i % 2 == 0) {
          val = "" + i;
        }
        pr.create(key, val);
      }

      try (IgnoredException ignored1 = addIgnoredException(EntryExistsException.class.getName());
           IgnoredException ignored2 = addIgnoredException(ReplyException.class.getName())) {
        for (int i = createRange_2Start; i <= createRange_2End; i++) {
          Object val = null;
          Object key = "" + i;
          if (i % 2 == 0) {
            val = "" + i;
          }
          final Object value = val;
          assertThatThrownBy(() -> pr.create(key, value)).isInstanceOf(EntryExistsException.class);
        }
      }

      // Invalidate Operations
      try (IgnoredException ignored1 = addIgnoredException(EntryNotFoundException.class.getName());
           IgnoredException ignored2 = addIgnoredException(ReplyException.class.getName())) {
        for (int i = invalidateRange_2Start; i <= invalidateRange_2End; i++) {
          // Check that before creating an entry it throws
          // EntryNotFoundException
          final Object val = Integer.toString(i);
          final Object key = Integer.toString(i);

          assertThatThrownBy(() -> pr.invalidate(key)).isInstanceOf(EntryNotFoundException.class);
          pr.create(key, val);
        }
      }

      for (int i = invalidateRange_2Start; i <= invalidateRange_2End; i++) {
        // Check that before creating an entry it throws
        // EntryNotFoundException
        final Object val = Integer.toString(i);
        final Object key = Integer.toString(i);
        assertThat(pr.get(key)).isEqualTo(val);
        assertThat(pr.containsValueForKey(key)).isTrue();
        pr.invalidate(key);
      }

      for (int i = invalidateRange_2Start; i <= invalidateRange_2End; i++) {
        final Object key = Integer.toString(i);
        Object shouldBeNull = pr.get(key);
        assertThat(shouldBeNull).isNull();
        assertThat(pr.containsValueForKey(key)).isFalse();
      }

      for (int i = invalidateRange_2Start; i <= invalidateRange_2End; i++) {
        final Object key = Integer.toString(i);
        pr.destroy(key);
      }

      try (IgnoredException ignored1 = addIgnoredException(EntryNotFoundException.class.getName());
           IgnoredException ignored2 = addIgnoredException(ReplyException.class.getName())) {
        for (int i = invalidateRange_2Start; i <= invalidateRange_2End; i++) {
          // Check that after deleting an entry, invalidate for that entry
          // throws
          // EntryNotFoundException
          final Object key = Integer.toString(i);
          assertThatThrownBy(() -> pr.invalidate(key));
        }
      }
    });
  }

  private void validatePartitionedRegionOps(final String prName) {
    /*
     * Do put(), create(), invalidate() operations through VM with PR having both Accessor and
     * Datastore
     */
    // String exceptionStr = "";
    vm0.invoke(() -> {
      Cache cache = getCache();
      final Region pr = cache.getRegion(prName);
      assertThat(pr).isNotNull();
      int size = 0;
      // if (pr.getAttributes().getScope() == Scope.DISTRIBUTED_ACK) {
      size = pr.size();
      assertThat(size).isEqualTo(0);
      assertThat(pr.isEmpty()).isTrue();
      assertThat(pr.keySet().size()).isEqualTo(0);
      // }
      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        // System.out.println("Putting entry for key = " + i);
        pr.put(Integer.toString(i), Integer.toString(i));
      }
      // if (pr.getAttributes().getScope() == Scope.DISTRIBUTED_ACK) {
      size = pr.size();
      assertThat(size).isEqualTo(putRange_1End);
      assertThat(pr.isEmpty()).isFalse();

      // Positive assertion of functionality in a distributed env.
      // For basic functional support (or lack of), please see
      // PartitionedRegionSingleNodeOperationsJUnitTest
      assertThat(pr.keySet().size()).isEqualTo(putRange_1End);
      Set ks = pr.keySet();
      Iterator ksI = ks.iterator();
      while (ksI.hasNext()) {
        assertThatThrownBy(() -> ksI.remove()).isInstanceOf(Exception.class);
        Object key = ksI.next();
        assertThat(key.getClass()).isEqualTo(String.class);
        Integer.parseInt((String) key);
      }
      assertThatThrownBy(() -> ksI.remove()).isInstanceOf(Exception.class);
      assertThat(ksI.hasNext()).isFalse();
      assertThatThrownBy(() -> ksI.next()).isInstanceOf(NoSuchElementException.class);
      assertThat(ksI.hasNext()).isFalse();


      String exceptionStr =
          ReplyException.class.getName() + "||" + EntryNotFoundException.class.getName();
      vm1.invoke(addExceptionTag1(exceptionStr));
      vm2.invoke(addExceptionTag1(exceptionStr));
      vm3.invoke(addExceptionTag1(exceptionStr));
      addExceptionTag1(exceptionStr);
      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        // System.out.println("Putting entry for key = " + i);
        try {
          pr.destroy(Integer.toString(i));
        } catch (EntryNotFoundException enfe) {
          throw enfe;
        }
      }
      vm1.invoke(removeExceptionTag1(exceptionStr));
      vm2.invoke(removeExceptionTag1(exceptionStr));
      vm3.invoke(removeExceptionTag1(exceptionStr));
      removeExceptionTag1(exceptionStr);

      // if (pr.getAttributes().getScope() == Scope.DISTRIBUTED_ACK) {
      size = pr.size();
      assertThat(size).isEqualTo(0);
      assertThat(pr.isEmpty()).isTrue();
      // }
      for (int i = putRange_1Start; i <= putRange_1End; i++) {
        // System.out.println("Putting entry for key = " + i);
        pr.put(Integer.toString(i), Integer.toString(i));
      }

      // createInvalidateChange
      for (int i = createRange_1Start; i <= createRange_1End; i++) {
        Object val = null;
        Object key = Integer.toString(i);
        if (i % 2 == 0) {
          val = Integer.toString(i);
        }
        pr.create(key, val);
      }

      try (IgnoredException ignored1 = addIgnoredException(EntryExistsException.class.getName());
           IgnoredException ignored2 = addIgnoredException(ReplyException.class.getName())) {
        for (int i = createRange_1Start; i <= createRange_1End; i++) {
          Object val = null;
          Object key = Integer.toString(i);
          if (i % 2 == 0) {
            val = Integer.toString(i);
          }
          final Object value = val;
          assertThatThrownBy(() -> pr.create(key, value)).isInstanceOf(EntryExistsException.class);
        }
      }

      PartitionedRegion ppr = (PartitionedRegion) pr;
      ppr.dumpAllBuckets(true);
    });

    /*
     * Do put(), create(), invalidate() operations through VM with PR having only Accessor(no data
     * store)
     */
    vm1.invoke(() -> {
      Cache cache = getCache();
      Region pr = cache.getRegion(prName);
      assertThat(pr).isNotNull();

      for (int i = putRange_2Start; i <= putRange_2End; i++) {
        // System.out.println("Putting entry for key = " + i);
        pr.put(Integer.toString(i), Integer.toString(i));
      }

      // createInvalidateChange
      for (int i = createRange_2Start; i <= createRange_2End; i++) {
        Object val = null;
        Object key = Integer.toString(i);
        if (i % 2 == 0) {
          val = Integer.toString(i);
        }
        pr.create(key, val);
      }

      try (IgnoredException ignored1 = addIgnoredException(EntryExistsException.class.getName());
           IgnoredException ignored2 = addIgnoredException(ReplyException.class.getName())) {
        for (int i = createRange_2Start; i <= createRange_2End; i++) {
          Object val = null;
          Object key = Integer.toString(i);
          if (i % 2 == 0) {
            val = Integer.toString(i);
          }
          final Object value = val;
          assertThatThrownBy(() -> pr.create(key, value)).isInstanceOf(EntryExistsException.class);
        }
      }
    });

    /*
     * Do destroy() operations through VM with PR having only Accessor(no data store). It also
     * verifies that EntryNotFoundException is thrown if the entry is already destroyed.
     */
    vm1.invoke(() -> {
      Cache cache = getCache();
      Region pr = cache.getRegion(prName);
      assertThat(pr).isNotNull();
      for (int i = removeRange_1Start; i <= removeRange_1End; i++) {
        // System.out.println("destroying entry for key = " + i);
        final String key = Integer.toString(i);
        pr.destroy(key);
      }

      final String entryNotFoundException = EntryNotFoundException.class.getName();
      getCache().getLogger().info(
          "<ExpectedException action=add>" + entryNotFoundException + "</ExpectedException>");
      String exceptionStr = ReplyException.class.getName() + "||" + entryNotFoundException;
      vm0.invoke(addExceptionTag1(exceptionStr));
      vm2.invoke(addExceptionTag1(exceptionStr));
      vm3.invoke(addExceptionTag1(exceptionStr));
      addExceptionTag1(exceptionStr);
      for (int i = removeRange_1Start; i <= removeRange_1End; i++) {
        final String key = Integer.toString(i);
        assertThatThrownBy(() -> pr.destroy(key)).isInstanceOf(EntryNotFoundException.class);
      }

      vm0.invoke(removeExceptionTag1(exceptionStr));
      vm2.invoke(removeExceptionTag1(exceptionStr));
      vm3.invoke(removeExceptionTag1(exceptionStr));
      removeExceptionTag1(exceptionStr);
      getCache().getLogger().info(
          "<ExpectedException action=remove>" + entryNotFoundException + "</ExpectedException>");
      LogWriterUtils.getLogWriter().fine("All the remove done successfully for vm0.");
    });

    /*
     * Do more put(), create(), invalidate() operations through VM with PR having Accessor + data
     * store
     */
    vm2.invoke(() -> {
      Cache cache = getCache();
      Region pr = cache.getRegion(prName);
      assertThat(pr).isNotNull();

      for (int i = putRange_3Start; i <= putRange_3End; i++) {
        // System.out.println("Putting entry for key = " + i);
        pr.put(Integer.toString(i), Integer.toString(i));
      }

      for (int i = createRange_3Start; i <= createRange_3End; i++) {
        Object val = null;
        Object key = Integer.toString(i);
        if (i % 2 == 0) {
          val = Integer.toString(i);
        }
        pr.create(key, val);
      }
      final String entryExistsException = EntryExistsException.class.getName();
      getCache().getLogger()
          .info("<ExpectedException action=add>" + entryExistsException + "</ExpectedException>");
      String exceptionStr = ReplyException.class.getName() + "||" + entryExistsException;
      vm0.invoke(addExceptionTag1(exceptionStr));
      vm1.invoke(addExceptionTag1(exceptionStr));
      vm3.invoke(addExceptionTag1(exceptionStr));
      addExceptionTag1(exceptionStr);

      for (int i = createRange_3Start; i <= createRange_3End; i++) {
        Object val = null;
        Object key = Integer.toString(i);
        if (i % 2 == 0) {
          val = Integer.toString(i);
        }
        final Object value = val;
        assertThatThrownBy(() -> pr.create(key, value)).isInstanceOf(EntryExistsException.class);
      }

      vm0.invoke(removeExceptionTag1(exceptionStr));
      vm1.invoke(removeExceptionTag1(exceptionStr));
      vm3.invoke(removeExceptionTag1(exceptionStr));
      removeExceptionTag1(exceptionStr);
      getCache().getLogger().info(
          "<ExpectedException action=remove>" + entryExistsException + "</ExpectedException>");
    });

    /*
     * Do more remove() operations through VM with PR having Accessor + data store
     */

    vm2.invoke(() -> {
      int i = 0;
      Cache cache = getCache();
      Region pr = cache.getRegion(prName);
      assertThat(pr).isNotNull();

      String key;
      for (i = removeRange_2Start; i <= removeRange_2End; i++) {
        // System.out.println("destroying entry for key = " + i);
        key = Integer.toString(i);
        try {
          pr.destroy(key);
        } catch (EntryNotFoundException enfe) {
          throw enfe;
        }
      }

      final String entryNotFound = EntryNotFoundException.class.getName();
      getCache().getLogger()
          .info("<ExpectedException action=add>" + entryNotFound + "</ExpectedException>");
      String exceptionStr = ReplyException.class.getName() + "||" + entryNotFound;
      vm0.invoke(addExceptionTag1(exceptionStr));
      vm1.invoke(addExceptionTag1(exceptionStr));
      vm3.invoke(addExceptionTag1(exceptionStr));
      addExceptionTag1(exceptionStr);
      for (i = removeRange_2Start; i <= removeRange_2End; i++) {
        // System.out.println("destroying entry for key = " + i);
        final String stringKey = Integer.toString(i);
        assertThatThrownBy(() -> pr.destroy(stringKey)).isInstanceOf(EntryNotFoundException.class);
      }
      vm0.invoke(removeExceptionTag1(exceptionStr));
      vm1.invoke(removeExceptionTag1(exceptionStr));
      vm3.invoke(removeExceptionTag1(exceptionStr));
      removeExceptionTag1(exceptionStr);
      getCache().getLogger()
          .info("<ExpectedException action=remove>" + entryNotFound + "</ExpectedException>");
    });

    /*
     * Do more put() operations through VM with PR having only Accessor
     */
    vm3.invoke(() -> {
      Cache cache = getCache();
      Region pr = cache.getRegion(prName);
      assertThat(pr).isNotNull();

      for (int i = putRange_4Start; i <= putRange_4End; i++) {
        // System.out.println("Putting entry for key = " + i);
        pr.put(Integer.toString(i), Integer.toString(i));
      }
      for (int i = createRange_4Start; i <= createRange_4End; i++) {
        Object val = null;
        final Object key = Integer.toString(i);
        if (i % 2 == 0) {
          val = Integer.toString(i);
        }
        pr.create(key, val);
      }

      final String entryExistsException = EntryExistsException.class.getName();
      getCache().getLogger()
          .info("<ExpectedException action=add>" + entryExistsException + "</ExpectedException>");
      String exceptionStr = ReplyException.class.getName() + "||" + entryExistsException;
      vm0.invoke(addExceptionTag1(exceptionStr));
      vm1.invoke(addExceptionTag1(exceptionStr));
      vm2.invoke(addExceptionTag1(exceptionStr));
      addExceptionTag1(exceptionStr);

      for (int i = createRange_4Start; i <= createRange_4End; i++) {
        Object val = null;
        final Object key = Integer.toString(i);
        if (i % 2 == 0) {
          val = Integer.toString(i);
        }
        final Object value = val;
        assertThatThrownBy(() -> pr.create(key, value)).isInstanceOf(EntryExistsException.class);
      }

      vm0.invoke(removeExceptionTag1(exceptionStr));
      vm1.invoke(removeExceptionTag1(exceptionStr));
      vm2.invoke(removeExceptionTag1(exceptionStr));
      removeExceptionTag1(exceptionStr);

      getCache().getLogger().info(
          "<ExpectedException action=remove>" + entryExistsException + "</ExpectedException>");
    });

    /*
     * validate the data in PartitionedRegion at different VM's
     */
    CacheSerializableRunnable validateRegionAPIs =
        new CacheSerializableRunnable("validateInserts") {

          @Override
          public void run2() {
            Cache cache = getCache();
            Region pr = cache.getRegion(prName);
            assertThat(pr).isNotNull();

            // Validation with get() operation.
            for (int i = putRange_1Start; i <= putRange_4End; i++) {
              Object val = pr.get(Integer.toString(i));
              if ((i >= removeRange_1Start && i <= removeRange_1End)
                  || (i >= removeRange_2Start && i <= removeRange_2End)) {
                assertThat(val).isNull();
              } else {
                assertThat(val).isNotNull();
              }
            }
            // validation with containsKey() operation.
            for (int i = putRange_1Start; i <= putRange_4End; i++) {
              boolean conKey = pr.containsKey(Integer.toString(i));
              if ((i >= removeRange_1Start && i <= removeRange_1End)
                  || (i >= removeRange_2Start && i <= removeRange_2End)) {
                assertThat(conKey).isFalse();
              } else {
                assertThat(conKey).isTrue();
              }
              LogWriterUtils.getLogWriter().fine("containsKey() Validated entry for key = " + i);
            }

            // validation with containsValueForKey() operation
            for (int i = putRange_1Start; i <= putRange_4End; i++) {
              boolean conKey = pr.containsValueForKey(Integer.toString(i));
              if ((i >= removeRange_1Start && i <= removeRange_1End)
                  || (i >= removeRange_2Start && i <= removeRange_2End)) {
                assertThat(conKey).isFalse();
              } else {
                assertThat(conKey).isTrue();
              }
              LogWriterUtils.getLogWriter()
                  .fine("containsValueForKey() Validated entry for key = " + i);
            }
          }
        };

    // validate the data from all the VM's
    vm0.invoke(validateRegionAPIs);
    vm1.invoke(validateRegionAPIs);
    vm2.invoke(validateRegionAPIs);
    vm3.invoke(validateRegionAPIs);

    /*
     * destroy the Region.
     */
    vm0.invoke(() -> {
      Cache cache = getCache();
      Region pr = cache.getRegion(prName);
      assertThat(pr).isNotNull();
      pr.destroyRegion();
      assertThat(pr.isDestroyed()).isTrue();
      assertThat(cache.getRegion(prName)).isNull();
    });

    /*
     * validate the data after the region.destroy() operation.
     */
    CacheSerializableRunnable validateAfterRegionDestroy =
        new CacheSerializableRunnable("validateInsertsAfterRegionDestroy") {

          @Override
          public void run2() throws CacheException {
            Cache cache = getCache();
            Region pr = null;
            pr = cache.getRegion(prName);
            assertThat(pr).isNull();
            Region rootRegion =
                cache.getRegion(Region.SEPARATOR + PartitionedRegionHelper.PR_ROOT_REGION_NAME);

            Object configObj = rootRegion.get(prName.substring(1));
            assertThat(configObj).isNull();

            Set subreg = rootRegion.subregions(false);
            for (java.util.Iterator itr = subreg.iterator(); itr.hasNext();) {
              Region reg = (Region) itr.next();
              String name = reg.getName();
              assertThat(name).doesNotContain(PartitionedRegionHelper.BUCKET_REGION_PREFIX);
            }
            // verify prIdToPr Map.
            boolean con = PartitionedRegion.prIdToPR.containsKey(REGION_NAME);
            assertThat(con).isFalse();
          }
        };

    // validateAfterRegionDestroy from all VM's

    vm0.invoke(validateAfterRegionDestroy);
    vm1.invoke(validateAfterRegionDestroy);
    vm2.invoke(validateAfterRegionDestroy);
    vm3.invoke(validateAfterRegionDestroy);
  }
}
