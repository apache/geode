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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DiskStoreFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.InterestPolicy;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

/**
 * Tests the use of the per-delta "forceRecalculateSize" flag.
 */

public class DeltaForceSizingFlagDUnitTest {
  private static final String TEST_REGION_NAME = "forceResizeTestRegionName";
  public static final String SMALLER_DELTA_DATA = "12345";
  public static final String LARGER_DELTA_DATA = "1234567890";
  public static final String DELTA_KEY = "a_key";
  public static final String RR_DISK_STORE_NAME = "_forceRecalculateSize_replicate_store";
  private static final Logger logger = LogService.getLogger();

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
  public void testRRMemLRUDelta() {
    doRRMemLRUDeltaTest(false);
  }

  @Test
  public void testRRMemLRUDeltaAndFlag() {
    doRRMemLRUDeltaTest(true);
  }

  @Test
  public void testPRNoLRUDelta() {
    doPRNoLRUDeltaTest(false);
  }

  @Test
  public void testPRNoLRUAndFlagDelta() {
    doPRNoLRUDeltaTest(true);
  }

  private void doRRMemLRUDeltaTest(boolean shouldSizeChange) {
    VM vm1 = server1.getVM();
    VM vm2 = server2.getVM();

    createRR(server1);
    createRR(server2);
    TestDelta delta1 = new TestDelta(false, SMALLER_DELTA_DATA, shouldSizeChange);
    put(vm1, delta1);

    assertValueType(vm1, ValueType.RAW_VALUE);
    assertValueType(vm2, ValueType.CD_SERIALIZED);
    assertThat(getObjectSizerInvocations(vm1)).isEqualTo(1);
    assertThat(getObjectSizerInvocations(vm2)).isEqualTo(0);

    long origEvictionSize0 = getSizeFromEvictionStats(vm1);
    long origEvictionSize1 = getSizeFromEvictionStats(vm2);
    delta1.info = LARGER_DELTA_DATA;
    delta1.hasDelta = true;
    // Update the delta
    put(vm1, delta1);

    assertValueType(vm1, ValueType.RAW_VALUE);
    assertValueType(vm2, ValueType.CD_DESERIALIZED);

    assertThat(getObjectSizerInvocations(vm1)).isEqualTo(2);

    long finalEvictionSize0 = getSizeFromEvictionStats(vm1);
    long finalEvictionSize1 = getSizeFromEvictionStats(vm2);
    assertThat(finalEvictionSize0 - origEvictionSize0).isEqualTo(5);
    if (shouldSizeChange) {
      assertThat(getObjectSizerInvocations(vm2)).isEqualTo(1);
      // I'm not sure what the change in size should be, because we went
      // from serialized to deserialized
      assertThat(finalEvictionSize1 - origEvictionSize1).isNotEqualTo(0);
    } else {
      // we invoke the sizer once when we deserialize the original to apply the delta to it
      assertThat(getObjectSizerInvocations(vm2)).isEqualTo(0);
      assertThat(finalEvictionSize1 - origEvictionSize1).isEqualTo(0);
    }
  }

  private void doPRNoLRUDeltaTest(boolean shouldSizeChange) {
    VM vm1 = server1.getVM();
    VM vm2 = server2.getVM();

    createPR(server1);
    createPR(server2);

    TestDelta delta1 = new TestDelta(false, SMALLER_DELTA_DATA, shouldSizeChange);
    put(vm1, delta1);
    long origPRSize0 = getSizeFromPRStats(vm1);
    long origPRSize1 = getSizeFromPRStats(vm2);

    // Update the delta
    delta1.info = LARGER_DELTA_DATA;
    delta1.hasDelta = true;
    put(vm1, delta1);
    long finalPRSize0 = getSizeFromPRStats(vm1);
    long finalPRSize1 = getSizeFromPRStats(vm2);

    if (shouldSizeChange) {
      // I'm not sure what the change in size should be, because we went
      // from serialized to deserialized
      assertThat(finalPRSize0 - origPRSize0).isNotEqualTo(0);
      assertThat(finalPRSize1 - origPRSize1).isNotEqualTo(0);
    } else {
      assertThat(finalPRSize0 - origPRSize0).isEqualTo(0);
      assertThat(finalPRSize1 - origPRSize1).isEqualTo(0);
    }
  }

  private long getSizeFromPRStats(VM vm0) {
    return vm0.invoke("getSizeFromPRStats", () -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      InternalRegion region = (InternalRegion) cache.getRegion(TEST_REGION_NAME);
      if (region instanceof PartitionedRegion) {
        long total = 0;
        PartitionedRegion pr = (PartitionedRegion) region;
        int totalNumBuckets = pr.getPartitionAttributes().getTotalNumBuckets();
        for (int i = 0; i < totalNumBuckets; i++) {
          total += pr.getDataStore().getBucketSize(i);
        }
        return total;
      } else {
        return 0L;
      }
    });
  }

  private long getSizeFromEvictionStats(VM vm0) {
    return vm0.invoke("getSizeFromEvictionStats", () -> {

      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      InternalRegion region = (InternalRegion) cache.getRegion(TEST_REGION_NAME);
      return region.getEvictionCounter();
    });
  }

  private int getObjectSizerInvocations(VM vm0) {
    return vm0.invoke("getObjectSizerInvocations", () -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      InternalRegion region = (InternalRegion) cache.getRegion(TEST_REGION_NAME);
      return getObjectSizerInvocations(region);
    });
  }

  private void put(VM vm0, final Object value) {
    vm0.invoke("Put data", () -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      InternalRegion region = (InternalRegion) cache.getRegion(TEST_REGION_NAME);
      region.put(DeltaForceSizingFlagDUnitTest.DELTA_KEY, value);
    });
  }

  protected static int getObjectSizerInvocations(InternalRegion region) {
    TestObjectSizer sizer = (TestObjectSizer) region.getEvictionAttributes().getObjectSizer();
    int result = sizer.invocations.get();
    logger.info("objectSizerInvocations=" + result);
    return result;
  }

  private void createRR(MemberVM memberVM) {
    memberVM.invoke("Create replicateRegion", () -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();

      DiskStoreFactory diskStoreFactory = cache.createDiskStoreFactory();
      diskStoreFactory.setDiskDirs(getMyDiskDirs());
      diskStoreFactory.create(RR_DISK_STORE_NAME);

      RegionFactory<Integer, TestDelta> regionFactory = cache.createRegionFactory();
      regionFactory.setDataPolicy(DataPolicy.REPLICATE);
      regionFactory.setDiskStoreName(RR_DISK_STORE_NAME);
      regionFactory.setDiskSynchronous(true);
      regionFactory.setEvictionAttributes(EvictionAttributes.createLRUMemoryAttributes(1,
          new TestObjectSizer(), EvictionAction.OVERFLOW_TO_DISK));
      regionFactory.setScope(Scope.DISTRIBUTED_ACK);

      regionFactory.create(TEST_REGION_NAME);
    });
  }

  private void assertValueType(VM vm, final ValueType expectedType) {
    vm.invoke("assertValueType", () -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();
      InternalRegion region = (InternalRegion) cache.getRegion(TEST_REGION_NAME);
      Object value = region.getValueInVM(DeltaForceSizingFlagDUnitTest.DELTA_KEY);
      switch (expectedType) {
        case RAW_VALUE:
          assertThat(value).isNotInstanceOf(CachedDeserializable.class);
          break;
        case CD_SERIALIZED:
          assertThat(value).isInstanceOf(CachedDeserializable.class);

          Object serializedValue = ((CachedDeserializable) value).getValue();
          assertThat(serializedValue).isInstanceOf(byte[].class);
          break;
        case CD_DESERIALIZED:
          assertThat(value).isInstanceOf(CachedDeserializable.class);

          Object deserializedValue = ((CachedDeserializable) value).getValue();
          assertThat(deserializedValue).isNotInstanceOf(byte[].class);
          break;
        case EVICTED:
          assertThat(value).isNull();
          break;
      }
    });
  }

  private static File[] getMyDiskDirs() {
    long random = new Random().nextLong();
    File file = new File(Long.toString(random));
    assertThat(file.mkdirs()).isTrue();
    return new File[] {file};
  }

  private void createPR(MemberVM memberVM) {
    memberVM.invoke("Create partitioned region", () -> {
      Cache cache = ClusterStartupRule.getCache();
      assertThat(cache).isNotNull();

      PartitionAttributesFactory<Integer, TestDelta> paf =
          new PartitionAttributesFactory<>();
      paf.setRedundantCopies(1);
      PartitionAttributes<Integer, TestDelta> prAttr = paf.create();

      RegionFactory<Integer, TestDelta> regionFactory = cache.createRegionFactory();
      regionFactory.setDataPolicy(DataPolicy.PARTITION);
      regionFactory.setDiskSynchronous(true);
      regionFactory.setPartitionAttributes(prAttr);
      regionFactory.setSubscriptionAttributes(new SubscriptionAttributes(InterestPolicy.ALL));
      regionFactory.create(TEST_REGION_NAME);
    });
  }

  private static class TestObjectSizer implements ObjectSizer {
    private final AtomicInteger invocations = new AtomicInteger();

    @Override
    public int sizeof(Object o) {
      logger.info("TestObjectSizer invoked");
      if (o instanceof TestDelta) {
        invocations.incrementAndGet();
        return ((TestDelta) o).info.length();
      }
      if (o instanceof Integer) {
        return 0;
      }
      throw new RuntimeException("Unexpected type to be sized " + o.getClass() + ", object=" + o);
    }
  }

  enum ValueType {
    RAW_VALUE, CD_SERIALIZED, CD_DESERIALIZED, EVICTED
  }
}
