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

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class PartitionedRegionRedundancyZoneDUnitTest extends JUnit4CacheTestCase {

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    // this makes sure we don't leave anything for the next tests
    // Tests that set redundancy zones causes other jvms connected
    // to the ds to have "enforce-unique-hosts" set to true.
    // See bug 51883.
    disconnectAllFromDS();
  }
  
  /**
   * Test that we don't try to put buckets in the same
   * zone when we don't have enough zones.
   */
  @Test
  public void testNotEnoughZones() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    setRedundancyZone(vm0, "A");
    setRedundancyZone(vm1, "A");
    setRedundancyZone(vm2, "B");
    
    createPR(vm0, 2);
    createPR(vm1, 2);
    createPR(vm2, 2);
    createData(vm0, 0, 6, "A");
    
    int vm0Count = getBucketCount(vm0);
    int vm1Count = getBucketCount(vm1);
    int vm2Count = getBucketCount(vm2);
    
    String counts = "vm0=" + vm0Count +",vm1=" + vm1Count + ",vm2=" +vm2Count;
    //assert zone A has only 1 copy of each bucket and zone B has
    //only one copy of each bucket.
    assertEquals(counts, 6, vm0Count + vm1Count);
    assertEquals(counts, 6, vm2Count);
    
    //Ideally, vm0 and vm1 would each have three buckets. However, due
    //to the way we place buckets, they may be off by one. The problem is that
    //we try to balance primaries first, rather than bucket counts, during bucket placement
    //When trying place the last primary bucket, we may have already placed 2 redundant
    //buckets on vm0 or vm1. At that point we will add another bucket to that
    //member to bring it's primary count up to two.
    assertTrue(counts, vm0Count >= 2 && vm1Count >=2);
  }
  
  protected void checkBucketCount(VM vm0, final int numLocalBuckets) {
    vm0.invoke(new SerializableRunnable("checkLowRedundancy") {

      public void run() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        assertEquals(numLocalBuckets, region.getLocalBucketsListTestOnly().size());
      }
    });
  }
  
  protected int getBucketCount(VM vm0) {
    return (Integer) vm0.invoke(new SerializableCallable("checkLowRedundancy") {

      public Object call() {
        Cache cache = getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        return region.getLocalBucketsListTestOnly().size();
      }
    });
  }
  
  protected DistributedMember createPR(VM vm, int redundancy) throws Exception {
    SerializableCallable createPrRegion = new SerializableCallable("createRegion") {
      public Object call()
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        cache.createRegion("region1", attr.create());
        return cache.getDistributedSystem().getDistributedMember();
      }
    };
    return (DistributedMember) vm.invoke(createPrRegion);
  }

  protected DistributedMember setRedundancyZone(VM vm, final String zone) {
    return (DistributedMember) vm.invoke(new SerializableCallable("set redundancy zone") {
      public Object call() {
        Properties props = new Properties();
        props.setProperty(REDUNDANCY_ZONE, zone);
        DistributedSystem system = getSystem(props);
        return system.getDistributedMember();
        
      }
    });
  }
  
  protected void createData(VM vm, final int startKey, final int endKey, final String value) {
    createData(vm, startKey, endKey,value, "region1");
  }

  protected void createData(VM vm, final int startKey, final int endKey, final String value, final String regionName) {
    SerializableRunnable createData = new SerializableRunnable("createData") {

      public void run() {
        Cache cache = getCache();
        Region region = cache.getRegion(regionName);

        for(int i =startKey; i < endKey; i++) {
          region.put(i, value);
        }
      }
    };
    vm.invoke(createData);
  }
}
