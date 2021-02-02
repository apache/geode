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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.partition.PartitionRegionHelper;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

/**
 * Tests the use of the per-delta "forceRecalculateSize" flag.
 */

public class DeltaForceSizingFlagDUnitTest {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  protected MemberVM locator;
  protected MemberVM server1;
  protected MemberVM server2;

  @Before
  public void setup() {
    int locatorPort;
    locator = cluster.startLocatorVM(0);
    locatorPort = locator.getPort();

    server1 = cluster.startServerVM(1, locatorPort);
    server2 = cluster.startServerVM(2, locatorPort);
  }

  @Test
  public void testRRMemLRU() {
    doRRMemLRUTest();
  }

  @Test
  public void testRRMemLRUDeltaAndFlag() {
    doRRMemLRUDeltaTest(true);
  }

  @Test
  public void testRRMemLRUDelta() {
    doRRMemLRUDeltaTest(false);
  }

  @Test
  public void testRRListener() {
    createRR(server1);
    createRR(server2);

    addListener(server1);
    addListener(server2);

    doListenerTestRR(server1.getVM(), server2.getVM());
  }

  @Test
  public void testPRMemLRU() {
    doPRMemLRUTest();
  }

  @Test
  public void testPRMemLRUAndFlagDeltaPutOnPrimary() {
    doPRDeltaTestLRU(false, false, true, false);
  }

  @Test
  public void testPRMemLRUDeltaPutOnPrimary() {
    doPRDeltaTestLRU(false, false, true, false);
  }

  @Test
  public void testPRMemLRUAndFlagDeltaPutOnSecondary() {
    doPRDeltaTestLRU(false, false, false, true);
  }

  @Test
  public void testPRMemLRUDeltaPutOnSecondary() {
    doPRDeltaTestLRU(false, false, false, true);
  }

  @Test
  public void testPRNoLRUDelta() {
    doPRNoLRUDeltaTest(false);
  }

  @Test
  public void testPRNoLRUAndFlagDelta() {
    doPRNoLRUDeltaTest(true);
  }

  @Test
  public void testPRListener() {
    createPR(server1, true);
    createPR(server2, true);

    addListener(server1);
    addListener(server2);

    doListenerTestPR(server1.getVM(), server2.getVM());
  }

  @Test
  public void testPRHeapLRU() {
    VM vm1 = server1.getVM();
    VM vm2 = server2.getVM();

    createPRHeapLRU(server1);
    createPRHeapLRU(server2);

    put(vm1, new TestKey("a"), new TestObject(100, 1000));

    assertValueType(vm1, new TestKey("a"), ValueType.CD_SERIALIZED);
    assertValueType(vm2, new TestKey("a"), ValueType.CD_SERIALIZED);

    assertEquals(1, getObjectSizerInvocations(vm1));
    long origSize0 = getSizeFromPRStats(vm1);
    assertTrue("Size was " + origSize0, 1000 > origSize0);
    assertEquals(1, getObjectSizerInvocations(vm2));
    long origSize1 = getSizeFromPRStats(vm2);
    assertTrue("Size was " + origSize1, 1000 > origSize1);

    get(vm1, new TestKey("a"), new TestObject(100, 1000));

    assertValueType(vm1, new TestKey("a"), ValueType.CD_DESERIALIZED);
    assertValueType(vm2, new TestKey("a"), ValueType.CD_SERIALIZED);
    assertEquals(3, getObjectSizerInvocations(vm1));
    assertEquals(1, getObjectSizerInvocations(vm2));

    // Test what happens when we reach the heap threshold??
  }

  @Test
  public void testRRHeapLRU() {
    VM vm1 = server1.getVM();
    VM vm2 = server2.getVM();

    createRRHeapLRU(server1);
    createRRHeapLRU(server2);

    put(vm1, "a", new TestObject(100, 1000));

    assertValueType(vm1, "a", ValueType.RAW_VALUE);
    assertValueType(vm2, "a", ValueType.CD_SERIALIZED);

    assertEquals(1, getObjectSizerInvocations(vm1));
    assertEquals(0, getObjectSizerInvocations(vm2));

    get(vm2, "a", new TestObject(100, 1000));

    assertValueType(vm1, "a", ValueType.RAW_VALUE);
    assertValueType(vm2, "a", ValueType.CD_DESERIALIZED);
    assertEquals(1, getObjectSizerInvocations(vm1));
    assertEquals(1, getObjectSizerInvocations(vm2));

    // Test what happens when we reach the heap threshold??
  }

  @Test
  public void testPRHeapLRUDeltaWithFlagPutOnPrimary() {
    doPRDeltaTestLRU(false, true, true, false);
  }

  @Test
  public void testPRHeapLRUDeltaPutOnPrimary() {
    doPRDeltaTestLRU(false, true, true, false);
  }

  @Test
  public void testPRHeapLRUDeltaWithFlagPutOnSecondary() {
    doPRDeltaTestLRU(false, true, false, true);
  }

  @Test
  public void testPRHeapLRUDeltaPutOnSecondary() {
    doPRDeltaTestLRU(false, true, false, true);
  }

  // test to cover bug41916
  @Test
  public void testLargeDelta() {
    VM vm1 = server1.getVM();
    VM vm2 = server2.getVM();

    createPR(server1, false);
    createPR(server2, false);
    // make a string bigger than socket-buffer-size which defaults to 32k
    int BIG_DELTA_SIZE = 32 * 1024 * 2;
    StringBuilder sb = new StringBuilder(BIG_DELTA_SIZE);
    for (int i = 0; i < BIG_DELTA_SIZE; i++) {
      sb.append('7');
    }
    TestDelta delta1 = new TestDelta(true, sb.toString());

    assignPRBuckets(server1);
    boolean vm0isPrimary = prHostsBucketForKey(server1, 0);
    if (!vm0isPrimary) {
      assertEquals(true, prHostsBucketForKey(server2, 0));
    }
    VM secondaryVm;
    if (vm0isPrimary) {
      secondaryVm = vm2;
    } else {
      secondaryVm = vm1;
    }

    put(secondaryVm, 0, delta1);
  }

  void doPRDeltaTestLRU(boolean shouldSizeChange, boolean heapLRU, boolean putOnPrimary,
      boolean wasDelta) {
    VM vm1 = server1.getVM();
    VM vm2 = server2.getVM();

    if (heapLRU) {
      createPRHeapLRU(server1);
      createPRHeapLRU(server2);
    } else {// memLRU
      createPR(server1, true);
      createPR(server2, true);
    }
    assignPRBuckets(server1);
    boolean vm0isPrimary = prHostsBucketForKey(server1, 0);
    if (!vm0isPrimary) {
      assertEquals(true, prHostsBucketForKey(server2, 0));
    }
    VM primaryVm;
    VM secondaryVm;
    if (vm0isPrimary) {
      primaryVm = vm1;
      secondaryVm = vm2;
    } else {
      primaryVm = vm2;
      secondaryVm = vm1;
    }

    TestDelta delta1 = new TestDelta(false, "12345", shouldSizeChange);
    if (putOnPrimary) {
      put(primaryVm, 0, delta1);
    } else {
      put(secondaryVm, 0, delta1);
    }
    // if the put is done on the primary then it will be CD_DESERIALIZED on the primary
    // otherwise it will be CD_SERIALIZED on the primary.
    if (putOnPrimary) {
      assertValueType(primaryVm, 0, ValueType.CD_DESERIALIZED);
      assertEquals(1, getObjectSizerInvocations(primaryVm));
    } else {
      assertValueType(primaryVm, 0, ValueType.CD_SERIALIZED);
      assertEquals(0, getObjectSizerInvocations(primaryVm));
    }
    // It will always be CD_SERIALIZED on the secondary.
    assertValueType(secondaryVm, 0, ValueType.CD_SERIALIZED);
    assertEquals(0, getObjectSizerInvocations(secondaryVm));

    long origEvictionSize0 = getSizeFromEvictionStats(primaryVm);
    long origEvictionSize1 = getSizeFromEvictionStats(secondaryVm);
    long origPRSize0 = getSizeFromPRStats(primaryVm);
    long origPRSize1 = getSizeFromPRStats(secondaryVm);
    delta1.info = "1234567890";
    delta1.hasDelta = true;
    // Update the delta
    if (putOnPrimary) {
      put(primaryVm, 0, delta1);
    } else {
      put(secondaryVm, 0, delta1);
    }

    assertValueType(primaryVm, 0, ValueType.CD_DESERIALIZED);
    assertValueType(secondaryVm, 0, ValueType.CD_DESERIALIZED);

    if (shouldSizeChange) {
      assertEquals(2, getObjectSizerInvocations(primaryVm));
      // once when we deserialize the value in the cache
      // and once when we size the new value from applying the delta
      assertEquals(2, getObjectSizerInvocations(secondaryVm));
    } else if (wasDelta) {
      assertEquals(0, getObjectSizerInvocations(primaryVm));
      // 1 sizer invoke since the first value needs to be deserialized
      assertEquals(0, getObjectSizerInvocations(secondaryVm));
    } else {
      assertEquals(1, getObjectSizerInvocations(primaryVm));
      // 1 sizer invoke since the first value needs to be deserialized
      assertEquals(0, getObjectSizerInvocations(secondaryVm));
    }

    long finalEvictionSize0 = getSizeFromEvictionStats(primaryVm);
    long finalEvictionSize1 = getSizeFromEvictionStats(secondaryVm);
    long finalPRSize0 = getSizeFromPRStats(primaryVm);
    long finalPRSize1 = getSizeFromPRStats(secondaryVm);
    if (shouldSizeChange) {
      // I'm not sure what the change in size should be, because we went
      // from serialized to deserialized
      assertTrue(finalEvictionSize0 - origEvictionSize0 != 0);
      assertTrue(finalPRSize0 - origPRSize0 != 0);
      assertTrue(finalEvictionSize1 - origEvictionSize1 != 0);
      assertTrue(finalPRSize1 - origPRSize1 != 0);
    } else {
      assertEquals(0, finalEvictionSize1 - origEvictionSize1);
      assertEquals(0, finalPRSize0 - origPRSize0);
      assertEquals(0, finalPRSize1 - origPRSize1);
    }
  }

  private void addListener(MemberVM memberVM) {
    memberVM.invoke("Add listener", () -> {
      Cache cache = ClusterStartupRule.getCache();
      Region region = cache.getRegion("region");
      try {
        region.getAttributesMutator().addCacheListener(new TestCacheListener());
      } catch (Exception e) {
        Assert.fail("couldn't create index", e);
      }
    });
  }


  private void doListenerTestRR(VM vm0, VM vm1) {
    assertEquals(0, getObjectSizerInvocations(vm0));
    assertEquals(0, getObjectSizerInvocations(vm1));
    put(vm0, "a", new TestObject(100, 100000));
    assertEquals(1, getObjectSizerInvocations(vm0));
    assertEquals(1, getObjectSizerInvocations(vm1));

    long origEvictionSize0 = getSizeFromEvictionStats(vm0);
    long origEvictionSize1 = getSizeFromEvictionStats(vm1);

    assertValueType(vm0, "a", ValueType.RAW_VALUE);
    assertValueType(vm1, "a", ValueType.CD_DESERIALIZED);
    assertTrue(origEvictionSize0 >= 100000);
    assertTrue(origEvictionSize1 >= 100000);

    put(vm0, "a", new TestObject(200, 200000));
    assertEquals(2, getObjectSizerInvocations(vm0));
    assertEquals(2, getObjectSizerInvocations(vm1));

    long finalEvictionSize0 = getSizeFromEvictionStats(vm0);
    long finalEvictionSize1 = getSizeFromEvictionStats(vm1);

    assertValueType(vm0, "a", ValueType.RAW_VALUE);
    assertValueType(vm1, "a", ValueType.CD_DESERIALIZED);
    assertEquals(100000, finalEvictionSize0 - origEvictionSize0);
    assertEquals(100000, finalEvictionSize1 - origEvictionSize1);
  }

  private void doListenerTestPR(VM vm0, VM vm1) {
    assertEquals(0, getObjectSizerInvocations(vm0));
    assertEquals(0, getObjectSizerInvocations(vm1));
    put(vm0, "a", new TestObject(100, 100000));
    assertEquals(1, getObjectSizerInvocations(vm0));
    assertEquals(1, getObjectSizerInvocations(vm1));

    long origEvictionSize0 = getSizeFromEvictionStats(vm0);
    long origEvictionSize1 = getSizeFromEvictionStats(vm1);
    long origPRSize0 = getSizeFromPRStats(vm1);
    long origPRSize1 = getSizeFromPRStats(vm1);

    assertValueType(vm0, "a", ValueType.CD_DESERIALIZED);
    assertValueType(vm1, "a", ValueType.CD_DESERIALIZED);
    assertTrue(origEvictionSize1 >= 100000);
    assertTrue(origEvictionSize0 >= 100000);
    assertTrue(origPRSize0 <= 500);
    assertTrue(origPRSize1 <= 500);

    put(vm0, "a", new TestObject(200, 200000));
    assertEquals(2, getObjectSizerInvocations(vm0));
    assertEquals(2, getObjectSizerInvocations(vm1));

    long finalEvictionSize0 = getSizeFromEvictionStats(vm0);
    long finalEvictionSize1 = getSizeFromEvictionStats(vm1);
    long finalPRSize0 = getSizeFromPRStats(vm0);
    long finalPRSize1 = getSizeFromPRStats(vm1);

    assertValueType(vm0, "a", ValueType.CD_DESERIALIZED);
    assertValueType(vm1, "a", ValueType.CD_DESERIALIZED);
    assertEquals(100000, finalEvictionSize0 - origEvictionSize0);
    assertEquals(100000, finalEvictionSize1 - origEvictionSize1);
    assertEquals(100, finalPRSize0 - origPRSize0);
    assertEquals(100, finalPRSize1 - origPRSize1);
  }


  private void doRRMemLRUTest() {
    VM vm1 = server1.getVM();
    VM vm2 = server2.getVM();

    createRR(server1);
    createRR(server2);
    put(vm1, "a", new TestObject(100, 100000));
    long origEvictionSize0 = getSizeFromEvictionStats(vm1);
    long origEvictionSize1 = getSizeFromEvictionStats(vm2);
    put(vm1, "a", new TestObject(200, 200000));

    assertValueType(vm1, "a", ValueType.RAW_VALUE);
    assertValueType(vm2, "a", ValueType.CD_SERIALIZED);
    assertEquals(2, getObjectSizerInvocations(vm1));

    long finalEvictionSize0 = getSizeFromEvictionStats(vm1);
    long finalEvictionSize1 = getSizeFromEvictionStats(vm2);
    assertEquals(100000, finalEvictionSize0 - origEvictionSize0);
    assertEquals(100, finalEvictionSize1 - origEvictionSize1);

    assertEquals(0, getObjectSizerInvocations(vm2));

    // Do a get to make sure we deserialize the object and calculate
    // the size adjustment
    Object v = new TestObject(200, 200000);
    get(vm2, "a", v);
    int vSize = CachedDeserializableFactory.calcSerializedMemSize(v);

    assertValueType(vm2, "a", ValueType.CD_DESERIALIZED);
    long evictionSizeAfterGet = getSizeFromEvictionStats(vm2);
    assertEquals(1, getObjectSizerInvocations(vm2));
    assertEquals(200000 + CachedDeserializableFactory.overhead() - vSize,
        evictionSizeAfterGet - finalEvictionSize1);

    // Do a put that will trigger an eviction if it is deserialized
    put(vm1, "b", new TestObject(100, 1000000));

    assertEquals(1, getEvictions(vm1));
    assertEquals(0, getEvictions(vm2));

    // Do a get to make sure we deserialize the object and calculate
    // the size adjustment
    get(vm2, "b", new TestObject(100, 1000000));
    assertEquals(1, getEvictions(vm2));
  }

  private void doPRMemLRUTest() {
    VM vm1 = server1.getVM();
    VM vm2 = server2.getVM();

    createPR(server1, true);
    createPR(server2, true);

    put(vm1, 0, new TestObject(100, 100000));
    assertValueType(vm1, 0, ValueType.CD_SERIALIZED);
    assertValueType(vm2, 0, ValueType.CD_SERIALIZED);
    long origEvictionSize0 = getSizeFromEvictionStats(vm1);
    long origEvictionSize1 = getSizeFromEvictionStats(vm2);
    long origPRSize0 = getSizeFromPRStats(vm1);
    long origPRSize1 = getSizeFromPRStats(vm2);
    put(vm1, 0, new TestObject(200, 200000));

    assertEquals(0, getObjectSizerInvocations(vm1));

    long finalEvictionSize0 = getSizeFromEvictionStats(vm1);
    long finalPRSize0 = getSizeFromPRStats(vm1);
    long finalEvictionSize1 = getSizeFromEvictionStats(vm2);
    long finalPRSize1 = getSizeFromPRStats(vm2);
    assertEquals(100, finalEvictionSize0 - origEvictionSize0);
    assertEquals(100, finalEvictionSize1 - origEvictionSize1);
    assertEquals(100, finalPRSize0 - origPRSize0);
    assertEquals(100, finalPRSize1 - origPRSize1);

    assertEquals(0, getObjectSizerInvocations(vm2));

    // Do a get to see if we deserialize the object and calculate
    // the size adjustment
    Object v = new TestObject(200, 200000);
    get(vm1, 0, v);
    int vSize = CachedDeserializableFactory.calcSerializedMemSize(v);
    assertValueType(vm1, 0, ValueType.CD_DESERIALIZED);
    assertValueType(vm2, 0, ValueType.CD_SERIALIZED);
    long evictionSizeAfterGet = getSizeFromEvictionStats(vm1);
    long prSizeAfterGet = getSizeFromPRStats(vm1);
    assertEquals(1, getObjectSizerInvocations(vm1));
    assertEquals(0, getObjectSizerInvocations(vm2));
    assertEquals(200000 + CachedDeserializableFactory.overhead() - vSize,
        evictionSizeAfterGet - finalEvictionSize0);
    assertEquals(0, prSizeAfterGet - finalPRSize0);

    // Do a put that will trigger an eviction if it is deserialized
    // It should not be deserialized.
    put(vm1, 113, new TestObject(100, 1024 * 1024));
    assertValueType(vm1, 113, ValueType.CD_SERIALIZED);
    assertValueType(vm2, 113, ValueType.CD_SERIALIZED);
    long evictionSizeAfterPutVm1 = getSizeFromEvictionStats(vm2);

    assertEquals(0, getEvictions(vm1));
    assertEquals(0, getEvictions(vm2));

    // Do a get to make sure we deserialize the object and calculate
    // the size adjustment which should force an eviction
    get(vm2, 113, new TestObject(100, 1024 * 1024));
    long evictionSizeAfterGetVm1 = getSizeFromEvictionStats(vm2);
    assertValueType(vm1, 113, ValueType.CD_SERIALIZED);
    assertValueType(vm2, 113, ValueType.EVICTED);
    assertEquals(1, getObjectSizerInvocations(vm1)); // from the get of key 0 on vm0
    assertEquals(0, getEvictions(vm1));
    assertEquals(1, getObjectSizerInvocations(vm2));
    assertEquals(2, getEvictions(vm2));
  }

  private void doRRMemLRUDeltaTest(boolean shouldSizeChange) {
    VM vm1 = server1.getVM();
    VM vm2 = server2.getVM();

    createRR(server1);
    createRR(server2);
    TestDelta delta1 = new TestDelta(false, "12345", shouldSizeChange);
    put(vm1, "a", delta1);

    assertValueType(vm1, "a", ValueType.RAW_VALUE);
    assertValueType(vm2, "a", ValueType.CD_SERIALIZED);
    assertEquals(1, getObjectSizerInvocations(vm1));
    assertEquals(0, getObjectSizerInvocations(vm2));

    long origEvictionSize0 = getSizeFromEvictionStats(vm1);
    long origEvictionSize1 = getSizeFromEvictionStats(vm2);
    delta1.info = "1234567890";
    delta1.hasDelta = true;
    // Update the delta
    put(vm1, "a", delta1);

    assertValueType(vm1, "a", ValueType.RAW_VALUE);
    assertValueType(vm2, "a", ValueType.CD_DESERIALIZED);

    assertEquals(2, getObjectSizerInvocations(vm1));

    long finalEvictionSize0 = getSizeFromEvictionStats(vm1);
    long finalEvictionSize1 = getSizeFromEvictionStats(vm2);
    assertEquals(5, finalEvictionSize0 - origEvictionSize0);
    if (shouldSizeChange) {
      assertEquals(1, getObjectSizerInvocations(vm2));
      // I'm not sure what the change in size should be, because we went
      // from serialized to deserialized
      assertTrue(finalEvictionSize1 - origEvictionSize1 != 0);
    } else {
      // we invoke the sizer once when we deserialize the original to apply the delta to it
      assertEquals(0, getObjectSizerInvocations(vm2));
      assertEquals(0, finalEvictionSize1 - origEvictionSize1);
    }
  }

  private void doPRNoLRUDeltaTest(boolean shouldSizeChange) {
    VM vm1 = server1.getVM();
    VM vm2 = server2.getVM();

    createPR(server1, false);
    createPR(server2, false);

    TestDelta delta1 = new TestDelta(false, "12345", shouldSizeChange);
    put(vm1, "a", delta1);
    long origPRSize0 = getSizeFromPRStats(vm1);
    long origPRSize1 = getSizeFromPRStats(vm2);

    // Update the delta
    delta1.info = "1234567890";
    delta1.hasDelta = true;
    put(vm1, "a", delta1);
    long finalPRSize0 = getSizeFromPRStats(vm1);
    long finalPRSize1 = getSizeFromPRStats(vm2);

    if (shouldSizeChange) {
      // I'm not sure what the change in size should be, because we went
      // from serialized to deserialized
      assertTrue(finalPRSize0 - origPRSize0 != 0);
      assertTrue(finalPRSize1 - origPRSize1 != 0);
    } else {
      assertEquals(0, finalPRSize0 - origPRSize0);
      assertEquals(0, finalPRSize1 - origPRSize1);
    }
  }

  private long getSizeFromPRStats(VM vm0) {
    return (Long) vm0.invoke("getSizeFromPRStats", () -> {
      Cache cache = ClusterStartupRule.getCache();
      LocalRegion region = (LocalRegion) cache.getRegion("region");
      if (region instanceof PartitionedRegion) {
        long total = 0;
        PartitionedRegion pr = ((PartitionedRegion) region);
        for (int i = 0; i < pr.getPartitionAttributes().getTotalNumBuckets(); i++) {
          total += pr.getDataStore().getBucketSize(i);
        }
        return total;
      } else {
        return 0L;
      }
    });
  }

  private long getSizeFromEvictionStats(VM vm0) {
    return (Long) vm0.invoke("getSizeFromEvictionStats", () -> {

      Cache cache = ClusterStartupRule.getCache();
      LocalRegion region = (LocalRegion) cache.getRegion("region");
      return getSizeFromEvictionStats(region);
    });
  }

  private long getEvictions(VM vm0) {
    return (Long) vm0.invoke("getEvictions", () -> {
      Cache cache = ClusterStartupRule.getCache();
      LocalRegion region = (LocalRegion) cache.getRegion("region");
      return getEvictions(region);
    });
  }

  private int getObjectSizerInvocations(VM vm0) {
    return (Integer) vm0.invoke("getObjectSizerInvocations", () -> {
      Cache cache = ClusterStartupRule.getCache();
      LocalRegion region = (LocalRegion) cache.getRegion("region");
      return getObjectSizerInvocations(region);
    });
  }

  private void assignPRBuckets(MemberVM memberVM) {
    memberVM.invoke("assignPRBuckets", () -> {
      Cache cache = ClusterStartupRule.getCache();
      PartitionRegionHelper.assignBucketsToPartitions(cache.getRegion("region"));
    });
  }

  private boolean prHostsBucketForKey(MemberVM memberVM, final Object key) {
    Boolean result = (Boolean) memberVM.invoke("prHostsBucketForKey", () -> {
      Cache cache = ClusterStartupRule.getCache();
      DistributedMember myId = cache.getDistributedSystem().getDistributedMember();
      Region region = cache.getRegion("region");
      DistributedMember hostMember = PartitionRegionHelper.getPrimaryMemberForKey(region, key);
      if (hostMember == null) {
        throw new IllegalStateException("bucket for key " + key + " is not hosted!");
      }
      boolean res = Boolean.valueOf(myId.equals(hostMember));
      return res;
    });
    return result.booleanValue();
  }

  private void put(VM vm0, final Object key, final Object value) {
    vm0.invoke("Put data", () -> {
      Cache cache = ClusterStartupRule.getCache();
      LocalRegion region = (LocalRegion) cache.getRegion("region");
      region.put(key, value);
    });
  }

  private void get(VM vm0, final Object key, final Object value) {
    vm0.invoke("Get data", () -> {
      Cache cache = ClusterStartupRule.getCache();
      LocalRegion region = (LocalRegion) cache.getRegion("region");
      assertEquals(value, region.get(key));
    });
  }

  protected static int getObjectSizerInvocations(LocalRegion region) {
    TestObjectSizer sizer = (TestObjectSizer) region.getEvictionAttributes().getObjectSizer();
    int result = sizer.invocations.get();
    region.getCache().getLogger().info("objectSizerInvocations=" + result);
    return result;
  }

  private static long getSizeFromEvictionStats(LocalRegion region) {
    long result = region.getEvictionCounter();
    return result;
  }

  private static long getEvictions(LocalRegion region) {
    return region.getTotalEvictions();
  }

  private void createRR(MemberVM memberVM) {
    memberVM.invoke("Create replicateRegion", () -> {
      Cache cache = ClusterStartupRule.getCache();
      AttributesFactory<Integer, TestDelta> attr = new AttributesFactory<Integer, TestDelta>();
      attr.setDiskSynchronous(true);
      attr.setDataPolicy(DataPolicy.REPLICATE);
      attr.setScope(Scope.DISTRIBUTED_ACK);
      attr.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(1,
          new TestObjectSizer(), EvictionAction.OVERFLOW_TO_DISK));
      attr.setDiskDirs(getMyDiskDirs());
      Region region = cache.createRegion("region", attr.create());
    });
  }

  private void assertValueType(VM vm, final Object key, final ValueType expectedType) {
    vm.invoke("assertValueType", () -> {
      Cache cache = ClusterStartupRule.getCache();
      LocalRegion region = (LocalRegion) cache.getRegion("region");
      Object value = region.getValueInVM(key);
      switch (expectedType) {
        case RAW_VALUE:
          assertTrue("Value was " + value + " type " + value.getClass(),
              !(value instanceof CachedDeserializable));
          break;
        case CD_SERIALIZED:
          assertTrue("Value was " + value + " type " + value.getClass(),
              value instanceof CachedDeserializable);
          assertTrue("Value not serialized",
              ((CachedDeserializable) value).getValue() instanceof byte[]);
          break;
        case CD_DESERIALIZED:
          assertTrue("Value was " + value + " type " + value.getClass(),
              value instanceof CachedDeserializable);
          assertTrue("Value was serialized",
              !(((CachedDeserializable) value).getValue() instanceof byte[]));
          break;
        case EVICTED:
          assertEquals(null, value);
          break;
      }
    });
  }

  private static File[] getMyDiskDirs() {
    long random = new Random().nextLong();
    File file = new File(Long.toString(random));
    file.mkdirs();
    return new File[] {file};
  }

  private void createPR(MemberVM memberVM, final boolean enableLRU) {
    memberVM.invoke("Create partitioned region", () -> {
      Cache cache = ClusterStartupRule.getCache();
      AttributesFactory<Integer, TestDelta> attr = new AttributesFactory<Integer, TestDelta>();
      attr.setDiskSynchronous(true);
      PartitionAttributesFactory<Integer, TestDelta> paf =
          new PartitionAttributesFactory<Integer, TestDelta>();
      paf.setRedundantCopies(1);
      if (enableLRU) {
        paf.setLocalMaxMemory(1); // memlru limit is 1 megabyte
        attr.setEvictionAttributes(EvictionAttributes
            .createLRUMemoryAttributes(new TestObjectSizer(), EvictionAction.OVERFLOW_TO_DISK));
        attr.setDiskDirs(getMyDiskDirs());
      }
      PartitionAttributes<Integer, TestDelta> prAttr = paf.create();
      attr.setPartitionAttributes(prAttr);
      attr.setDataPolicy(DataPolicy.PARTITION);
      attr.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      Region<Integer, TestDelta> region = cache.createRegion("region", attr.create());
    });

  }

  private void createRRHeapLRU(MemberVM memberVM) {
    memberVM.invoke("Create rr-heap-lru", () -> {
      Cache cache = ClusterStartupRule.getCache();
      ResourceManager manager = cache.getResourceManager();
      manager.setCriticalHeapPercentage(95);
      manager.setEvictionHeapPercentage(90);
      AttributesFactory<Integer, TestDelta> attr = new AttributesFactory<Integer, TestDelta>();
      attr.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(new TestObjectSizer(),
          EvictionAction.OVERFLOW_TO_DISK));
      attr.setDiskDirs(getMyDiskDirs());
      attr.setDataPolicy(DataPolicy.REPLICATE);
      attr.setScope(Scope.DISTRIBUTED_ACK);
      attr.setDiskDirs(getMyDiskDirs());
      Region region = cache.createRegion("region", attr.create());
    });

  }

  private void createPRHeapLRU(MemberVM memberVM) {
    memberVM.invoke("Create pr-heap-lru", () -> {
      Cache cache = ClusterStartupRule.getCache();
      ResourceManager manager = cache.getResourceManager();
      manager.setCriticalHeapPercentage(95);
      manager.setEvictionHeapPercentage(90);

      AttributesFactory<Integer, TestDelta> attr = new AttributesFactory<Integer, TestDelta>();
      PartitionAttributesFactory<Integer, TestDelta> paf =
          new PartitionAttributesFactory<Integer, TestDelta>();
      paf.setRedundantCopies(1);
      attr.setEvictionAttributes(EvictionAttributes.createLRUHeapAttributes(new TestObjectSizer(),
          EvictionAction.LOCAL_DESTROY));
      PartitionAttributes<Integer, TestDelta> prAttr = paf.create();
      attr.setPartitionAttributes(prAttr);
      attr.setDataPolicy(DataPolicy.PARTITION);
      attr.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      Region<Integer, TestDelta> region = cache.createRegion("region", attr.create());
    });

  }

  private static class TestObjectSizer implements ObjectSizer {
    private AtomicInteger invocations = new AtomicInteger();

    @Override
    public int sizeof(Object o) {
      if (InternalDistributedSystem.getLogger().fineEnabled()) {
        InternalDistributedSystem.getLogger()
            .fine("TestObjectSizer invoked"/* , new Exception("stack trace") */);
      }
      if (o instanceof TestObject) {
        invocations.incrementAndGet();
        return ((TestObject) o).sizeForSizer;
      }
      if (o instanceof TestDelta) {
        invocations.incrementAndGet();
        return ((TestDelta) o).info.length();
      }
      if (o instanceof Integer) {
        return 0;
      }
      if (o instanceof TestKey) {
        invocations.incrementAndGet();
        return ((TestKey) o).value.length();
      }
      throw new RuntimeException("Unpected type to be sized " + o.getClass() + ", object=" + o);
    }
  }

  private static class TestKey implements DataSerializable {
    String value;

    public TestKey() {

    }

    public TestKey(String value) {
      this.value = value;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      value = DataSerializer.readString(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeString(value, out);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((value == null) ? 0 : value.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof TestKey)) {
        return false;
      }
      TestKey other = (TestKey) obj;
      if (value == null) {
        if (other.value != null) {
          return false;
        }
      } else if (!value.equals(other.value)) {
        return false;
      }
      return true;
    }

  }

  private static class TestObject implements DataSerializable {
    public int sizeForSizer;
    public int sizeForSerialization;

    public TestObject() {

    }


    public TestObject(int sizeForSerialization, int sizeForSizer) {
      super();
      this.sizeForSizer = sizeForSizer;
      this.sizeForSerialization = sizeForSerialization;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      sizeForSizer = in.readInt();
      sizeForSerialization = in.readInt();
      // We don't actually need these things.
      in.skipBytes(sizeForSerialization);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      out.writeInt(sizeForSizer);
      out.writeInt(sizeForSerialization);
      out.write(new byte[sizeForSerialization]);

    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + sizeForSerialization;
      result = prime * result + sizeForSizer;
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof TestObject)) {
        return false;
      }
      TestObject other = (TestObject) obj;
      if (sizeForSerialization != other.sizeForSerialization) {
        return false;
      }
      if (sizeForSizer != other.sizeForSizer) {
        return false;
      }
      return true;
    }

    @Override
    public String toString() {
      return "TestObject [sizeForSerialization=" + sizeForSerialization + ", sizeForSizer="
          + sizeForSizer + "]";
    }
  }

  public static class TestCacheListener extends CacheListenerAdapter {

    @Override
    public void afterCreate(EntryEvent event) {
      // Make sure we deserialize the new value
      event.getRegion().getCache().getLogger().fine("invoked afterCreate with " + event);
      event.getRegion().getCache().getLogger().info(String.format("%s",
          "value is " + event.getNewValue()));
    }

    @Override
    public void afterUpdate(EntryEvent event) {
      // Make sure we deserialize the new value
      event.getRegion().getCache().getLogger().fine("invoked afterUpdate with ");
      event.getRegion().getCache().getLogger().info(String.format("%s",
          "value is " + event.getNewValue()));
    }

  }

  enum ValueType {
    RAW_VALUE, CD_SERIALIZED, CD_DESERIALIZED, EVICTED
  }
}
