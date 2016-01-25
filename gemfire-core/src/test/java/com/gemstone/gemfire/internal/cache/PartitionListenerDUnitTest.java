/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.control.RebalanceOperation;
import com.gemstone.gemfire.cache.partition.PartitionListenerAdapter;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

@SuppressWarnings({"serial", "rawtypes", "deprecation", "unchecked"})
public class PartitionListenerDUnitTest extends CacheTestCase {
  
  public PartitionListenerDUnitTest(String name) {
    super(name);
  }

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
    
    // Create the PR in a third JVM and rebalance
    createPR(vm3, regionName, false);
    rebalance(vm3);
    
    // Verify listener invocations
    // Get all buckets and keys removed from VM1 and VM2
    Map<Integer, List<Integer>> allBucketsAndKeysRemoved = new HashMap<Integer, List<Integer>>();
    allBucketsAndKeysRemoved.putAll(getBucketsAndKeysRemoved(vm1, regionName));
    allBucketsAndKeysRemoved.putAll(getBucketsAndKeysRemoved(vm2, regionName));
    
    // Get all buckets and keys added to VM3
    Map<Integer, List<Integer>> vm3BucketsAndKeysAdded = getBucketsAndKeysAdded(vm3, regionName);
    
    // Verify that they are equal
    assertEquals(allBucketsAndKeysRemoved, vm3BucketsAndKeysAdded);
  }
  
  protected DistributedMember createPR(VM vm, final String regionName,
      final boolean isAccessor) throws Throwable {
    SerializableCallable createPrRegion = new SerializableCallable("createRegion") {
      
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

  protected void createData(VM vm, final int startKey, final int endKey,
      final String value, final String regionName) {
    SerializableRunnable createData = new SerializableRunnable("createData") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);

        for (int i=startKey; i<endKey; i++) {
          region.put(i, value);
        }
      }
    };
    vm.invoke(createData);
  }

  protected Map<Integer, List<Integer>> getBucketsAndKeysRemoved(VM vm, final String regionName) {
    SerializableCallable getBucketsAndKeysRemoved = new SerializableCallable("getBucketsAndKeysRemoved") {
      
      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        TestPartitionListener listener = (TestPartitionListener) region.getAttributes().getPartitionAttributes().getPartitionListeners()[0];
        return listener.getBucketsAndKeysRemoved();
      }
    };
    return (Map<Integer, List<Integer>>) vm.invoke(getBucketsAndKeysRemoved);
  }

  protected Map<Integer, List<Integer>> getBucketsAndKeysAdded(VM vm, final String regionName) {
    SerializableCallable getBucketsAndKeysAdded = new SerializableCallable("getBucketsAndKeysAdded") {
      
      public Object call() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);
        TestPartitionListener listener = (TestPartitionListener) region.getAttributes().getPartitionAttributes().getPartitionListeners()[0];
        return listener.getBucketsAndKeysAdded();
      }
    };
    return (Map<Integer, List<Integer>>) vm.invoke(getBucketsAndKeysAdded);
  }
  
  protected void rebalance(VM vm) {
    vm.invoke(new SerializableCallable() {

      public Object call() throws Exception {
        RebalanceOperation rebalance = getCache().getResourceManager()
            .createRebalanceFactory().start();
        rebalance.getResults();
        return null;
      }
    });
  }

  protected static class TestPartitionListener extends PartitionListenerAdapter {
    
    private final Map<Integer, List<Integer>> bucketsAndKeysRemoved;
    
    private final Map<Integer, List<Integer>> bucketsAndKeysAdded;
    
    public TestPartitionListener() {
      this.bucketsAndKeysRemoved = new HashMap<Integer, List<Integer>>();
      this.bucketsAndKeysAdded = new HashMap<Integer, List<Integer>>();
    }
    
    public Map<Integer, List<Integer>> getBucketsAndKeysRemoved() {
      return this.bucketsAndKeysRemoved;
    }
    
    public Map<Integer, List<Integer>> getBucketsAndKeysAdded() {
      return this.bucketsAndKeysAdded;
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
