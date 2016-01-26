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

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableCallable;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Test that the bucket size does not go negative when
 * we fault out and in a delta object.
 * @author dsmith
 *
 */
public class DeltaFaultInDUnitTest extends CacheTestCase {
  

  /**
   * @param name
   */
  public DeltaFaultInDUnitTest(String name) {
    super(name);
  }

  public void test() throws Exception {
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    final boolean copyOnRead = false;
    final boolean clone = true;

    SerializableCallable createDataRegion = new SerializableCallable("createDataRegion") {
      public Object call() throws Exception
      {
        Cache cache = getCache();
        cache.setCopyOnRead(copyOnRead);
        cache.createDiskStoreFactory().create("DeltaFaultInDUnitTestData");
        AttributesFactory attr = new AttributesFactory();
        attr.setDiskStoreName("DeltaFaultInDUnitTestData");
        PartitionAttributesFactory paf = new PartitionAttributesFactory();
        paf.setRedundantCopies(1);
        PartitionAttributes prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        attr.setCloningEnabled(clone);
        attr.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
        Region region = cache.createRegion("region1", attr.create());
        
        return null;
      }
    };
    
    vm0.invoke(createDataRegion);
    
    SerializableRunnable createEmptyRegion = new SerializableRunnable("createEmptyRegion") {
      public void run()
      {
        Cache cache = getCache();
        cache.setCopyOnRead(copyOnRead);
        AttributesFactory<Integer, TestDelta> attr = new AttributesFactory<Integer, TestDelta>();
        attr.setCloningEnabled(clone);
        PartitionAttributesFactory<Integer, TestDelta> paf = new PartitionAttributesFactory<Integer, TestDelta>();
        paf.setRedundantCopies(1);
        paf.setLocalMaxMemory(0);
        PartitionAttributes<Integer, TestDelta> prAttr = paf.create();
        attr.setPartitionAttributes(prAttr);
        attr.setDataPolicy(DataPolicy.PARTITION);
        attr.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.OVERFLOW_TO_DISK));
        Region<Integer, TestDelta> region = cache.createRegion("region1", attr.create());

        
        //Put an entry
        region.put(new Integer(0), new TestDelta(false, "initial"));
        
        //Put a delta object that is larger
        region.put(new Integer(0), new TestDelta(true, "initial_plus_some_more_data"));
      }
    };
    
    vm1.invoke(createEmptyRegion);
    
    vm0.invoke(new SerializableRunnable("doPut") {
      public void run()
      {
        Cache cache = getCache();
        Region<Integer, TestDelta> region = cache.getRegion("region1");

        
        //Evict the other object
        region.put(new Integer(113), new TestDelta(false, "bogus"));
        
        //Something was going weird with the LRU list. It was evicting this object.
        //I want to make sure the other object is the one evicted.
        region.get(new Integer(113));
        
        long entriesEvicted = ((AbstractLRURegionMap)((PartitionedRegion)region).entries)._getLruList().stats()
        .getEvictions();
//        assertEquals(1, entriesEvicted);
        
        TestDelta result = region.get(new Integer(0));
        assertEquals("initial_plus_some_more_data", result.info);
      }
    });
  }
  
  private long checkObjects(VM vm, final int serializations, final int deserializations, final int deltas, final int clones) {
    SerializableCallable getSize = new SerializableCallable("check objects") {
      public Object call() {
        GemFireCacheImpl cache = (GemFireCacheImpl) getCache();
        PartitionedRegion region = (PartitionedRegion) cache.getRegion("region1");
        long size = region.getDataStore().getBucketSize(0);
        TestDelta value = (TestDelta) region.get(Integer.valueOf(0));
        value.checkFields(serializations, deserializations, deltas, clones);
        return Long.valueOf(size);
      }
    };
    Object size = vm.invoke(getSize);
    return ((Long) size).longValue();
  }
}
