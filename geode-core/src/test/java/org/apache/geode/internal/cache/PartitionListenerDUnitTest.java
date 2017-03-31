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

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.assertj.core.api.Assertions.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.partition.PartitionListenerAdapter;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;

@SuppressWarnings({"serial", "rawtypes", "deprecation", "unchecked"})
@Category(DistributedTest.class)
public class PartitionListenerDUnitTest extends JUnit4CacheTestCase {

  public PartitionListenerDUnitTest() {
    super();
  }

  @Test
  public void testAfterBucketRemovedCreated() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    // Create the PR in 2 JVMs
    String regionName = getName() + "_region";
    createPR(vm1, regionName, false);
    createPR(vm2, regionName, false);

    // Create the data using an accessor
    createPR(vm0, regionName, true);
    createData(vm0, 0, 1000, "A", regionName);

    // Assert that afterPrimary is invoked for every primary created on the vm
    List<Integer> vm1PrimariesCreated = getPrimariesCreated(vm1, regionName);
    List<Integer> vm2PrimariesCreated = getPrimariesCreated(vm2, regionName);
    List<Integer> vm1ActualPrimaries = getPrimariesOn(vm1, regionName);
    List<Integer> vm2ActualPrimaries = getPrimariesOn(vm2, regionName);

    vm1PrimariesCreated.removeAll(vm1ActualPrimaries);
    vm2PrimariesCreated.removeAll(vm2ActualPrimaries);

    assertThat(vm1PrimariesCreated).isEmpty();
    assertThat(vm2PrimariesCreated).isEmpty();

    // Create the PR in a third JVM and rebalance
    createPR(vm3, regionName, false);
    rebalance(vm3);

    // Verify listener invocations
    // Assert afterRegionCreate is invoked on every VM.
    assertEquals(regionName, getRegionNameFromListener(vm0, regionName));
    assertEquals(regionName, getRegionNameFromListener(vm1, regionName));
    assertEquals(regionName, getRegionNameFromListener(vm2, regionName));
    assertEquals(regionName, getRegionNameFromListener(vm3, regionName));

    // Get all buckets and keys removed from VM1 and VM2
    Map<Integer, List<Integer>> allBucketsAndKeysRemoved = new HashMap<Integer, List<Integer>>();
    allBucketsAndKeysRemoved.putAll(getBucketsAndKeysRemoved(vm1, regionName));
    allBucketsAndKeysRemoved.putAll(getBucketsAndKeysRemoved(vm2, regionName));

    // Get all buckets and keys added to VM3
    Map<Integer, List<Integer>> vm3BucketsAndKeysAdded = getBucketsAndKeysAdded(vm3, regionName);

    // Verify that they are equal
    assertEquals(allBucketsAndKeysRemoved, vm3BucketsAndKeysAdded);

    // Verify afterPrimary is invoked after rebalance.
    List<Integer> vm3PrimariesCreated = getPrimariesCreated(vm3, regionName);
    List<Integer> vm3ActualPrimaries = getPrimariesOn(vm3, regionName);

    vm3ActualPrimaries.removeAll(vm3PrimariesCreated);
    assertThat(vm3ActualPrimaries).isEmpty();
  }

  @Test
  public void testAfterSecondaryIsCalledAfterLosingPrimary() throws Throwable {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    // Create the PR in 2 JVMs
    String regionName = getName() + "_region";
    createPR(vm1, regionName, false);
    createPR(vm2, regionName, false);

    // Create the data using an accessor
    createPR(vm0, regionName, true);
    createData(vm0, 0, 1000, "A", regionName);

    // record the bucket ids for each vm
    List<Integer> vm1ActualPrimaries = getPrimariesOn(vm1, regionName);
    List<Integer> vm2ActualPrimaries = getPrimariesOn(vm2, regionName);

    // Create the PR in a third JVM and rebalance
    createPR(vm3, regionName, false);
    rebalance(vm3);

    // Verify listener invocations
    List<Integer> afterSecondaryCalledForVM1 = getAfterSecondaryCallbackBucketIds(vm1, regionName);
    List<Integer> afterSecondaryCalledForVM2 = getAfterSecondaryCallbackBucketIds(vm2, regionName);

    List<Integer> newVm1ActualPrimaries = getPrimariesOn(vm1, regionName);
    List<Integer> newVM2ActualPrimaries = getPrimariesOn(vm2, regionName);

    // calculate and verify expected afterSecondary calls
    List<Integer> bucketsThatRemainInVM1 = new ArrayList(vm1ActualPrimaries);
    bucketsThatRemainInVM1.retainAll(newVm1ActualPrimaries);
    // All previous primary bucket ids - all overlapping/retained current bucket ids = all the
    // secondary buckets
    int expectedAfterSecondaryCalls = vm1ActualPrimaries.size() - bucketsThatRemainInVM1.size();
    assertEquals(expectedAfterSecondaryCalls, afterSecondaryCalledForVM1.size());

    List<Integer> bucketsThatRemainInVM2 = new ArrayList(vm2ActualPrimaries);
    bucketsThatRemainInVM2.retainAll(newVM2ActualPrimaries);
    // All previous primary bucket ids - all overlapping/retained current bucket ids = all the
    // secondary buckets
    expectedAfterSecondaryCalls = vm2ActualPrimaries.size() - bucketsThatRemainInVM2.size();
    assertEquals(expectedAfterSecondaryCalls, afterSecondaryCalledForVM2.size());

  }

  protected DistributedMember createPR(VM vm, final String regionName, final boolean isAccessor)
      throws Throwable {
    SerializableCallable createPrRegion = new SerializableCallable("withRegion") {

      public Object call() {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        if (isAccessor) {
          paf.setLocalMaxMemory(0);
        }
        paf.addPartitionListener(new TestPartitionListener());
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion(regionName, attr.create());
        return cache.getDistributedSystem().getDistributedMember();
      }
    };
    return (DistributedMember) vm.invoke(createPrRegion);
  }

  protected void createData(VM vm, final int startKey, final int endKey, final String value,
      final String regionName) {
    SerializableRunnable createData = new SerializableRunnable("createData") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);

        for (int i = startKey; i < endKey; i++) {
          region.put(i, value);
        }
      }
    };
    vm.invoke(createData);
  }

  protected List<Integer> getPrimariesOn(VM vm, final String regionName) {
    SerializableCallable getPrimariesOn = new SerializableCallable("getPrimariesOn") {

      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        return new ArrayList<>(
            ((PartitionedRegion) region).getDataStore().getAllLocalPrimaryBucketIds());
      }
    };
    return (List<Integer>) vm.invoke(getPrimariesOn);
  }

  protected List<Integer> getPrimariesCreated(VM vm, final String regionName) {
    SerializableCallable getPrimariesCreated = new SerializableCallable("getPrimariesCreated") {

      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        TestPartitionListener listener = (TestPartitionListener) region.getAttributes()
            .getPartitionAttributes().getPartitionListeners()[0];
        return listener.getPrimariesCreated();
      }
    };
    return (List<Integer>) vm.invoke(getPrimariesCreated);
  }

  protected String getRegionNameFromListener(VM vm, final String regionName) {
    SerializableCallable getRegionName = new SerializableCallable("getRegionName") {

      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        TestPartitionListener listener = (TestPartitionListener) region.getAttributes()
            .getPartitionAttributes().getPartitionListeners()[0];
        return listener.getRegionName();
      }
    };
    return (String) vm.invoke(getRegionName);
  }

  protected Map<Integer, List<Integer>> getBucketsAndKeysRemoved(VM vm, final String regionName) {
    SerializableCallable getBucketsAndKeysRemoved =
        new SerializableCallable("getBucketsAndKeysRemoved") {

          public Object call() {
            Cache cache = getCache();
            Region region = cache.getRegion(regionName);
            TestPartitionListener listener = (TestPartitionListener) region.getAttributes()
                .getPartitionAttributes().getPartitionListeners()[0];
            return listener.getBucketsAndKeysRemoved();
          }
        };
    return (Map<Integer, List<Integer>>) vm.invoke(getBucketsAndKeysRemoved);
  }

  protected Map<Integer, List<Integer>> getBucketsAndKeysAdded(VM vm, final String regionName) {
    SerializableCallable getBucketsAndKeysAdded =
        new SerializableCallable("getBucketsAndKeysAdded") {

          public Object call() {
            Cache cache = getCache();
            Region region = cache.getRegion(regionName);
            TestPartitionListener listener = (TestPartitionListener) region.getAttributes()
                .getPartitionAttributes().getPartitionListeners()[0];
            return listener.getBucketsAndKeysAdded();
          }
        };
    return (Map<Integer, List<Integer>>) vm.invoke(getBucketsAndKeysAdded);
  }

  protected List<Integer> getAfterSecondaryCallbackBucketIds(VM vm, final String regionName) {
    SerializableCallable getAfterSecondaryCallbackBucketIds =
        new SerializableCallable("getAfterSecondaryCallbackBucketIds") {

          public Object call() {
            Cache cache = getCache();
            Region region = cache.getRegion(regionName);
            TestPartitionListener listener = (TestPartitionListener) region.getAttributes()
                .getPartitionAttributes().getPartitionListeners()[0];
            return listener.getAfterSecondaryCallbackBucketIds();
          }
        };
    return (List<Integer>) vm.invoke(getAfterSecondaryCallbackBucketIds);
  }

  protected void rebalance(VM vm) {
    vm.invoke(new SerializableCallable() {

      public Object call() throws Exception {
        RebalanceOperation rebalance =
            getCache().getResourceManager().createRebalanceFactory().start();
        rebalance.getResults();
        return null;
      }
    });
  }

  protected static class TestPartitionListener extends PartitionListenerAdapter {

    private String regionName;

    private final List<Integer> primariesCreated;

    private final List<Integer> afterSecondaryCalled;

    private final Map<Integer, List<Integer>> bucketsAndKeysRemoved;

    private final Map<Integer, List<Integer>> bucketsAndKeysAdded;

    public TestPartitionListener() {
      this.primariesCreated = new ArrayList<>();
      afterSecondaryCalled = new ArrayList<>();
      this.bucketsAndKeysRemoved = new HashMap<Integer, List<Integer>>();
      this.bucketsAndKeysAdded = new HashMap<Integer, List<Integer>>();
    }

    public Map<Integer, List<Integer>> getBucketsAndKeysRemoved() {
      return this.bucketsAndKeysRemoved;
    }

    public Map<Integer, List<Integer>> getBucketsAndKeysAdded() {
      return this.bucketsAndKeysAdded;
    }

    public List<Integer> getPrimariesCreated() {
      return this.primariesCreated;
    }

    public String getRegionName() {
      return this.regionName;
    }

    public void afterRegionCreate(Region<?, ?> region) {
      this.regionName = region.getName();
    }

    public void afterPrimary(int bucketId) {
      this.primariesCreated.add(bucketId);
    }

    public void afterSecondary(int bucketId) {
      this.afterSecondaryCalled.add(bucketId);
    }

    public List<Integer> getAfterSecondaryCallbackBucketIds() {
      return afterSecondaryCalled;
    }

    public void afterBucketRemoved(int bucketId, Iterable<?> keys) {
      Collection<Integer> keysCol = (Collection) keys;
      // If the keys collection is not empty, create a serializable list to hold
      // them and add them to the keys removed.
      if (!keysCol.isEmpty()) {
        List<Integer> keysList = new ArrayList<Integer>();
        for (Integer key : keysCol) {
          keysList.add(key);
        }
        Collections.sort(keysList);
        this.bucketsAndKeysRemoved.put(bucketId, keysList);
      }
    }

    public void afterBucketCreated(int bucketId, Iterable<?> keys) {
      Collection<Integer> keysCol = (Collection) keys;
      // If the keys collection is not empty, create a serializable list to hold
      // them and add them to the keys added.
      if (!keysCol.isEmpty()) {
        List<Integer> keysList = new ArrayList<Integer>();
        for (Integer key : keysCol) {
          keysList.add(key);
        }
        Collections.sort(keysList);
        this.bucketsAndKeysAdded.put(bucketId, keysList);
      }
    }
  }
}
