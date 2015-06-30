/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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

import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;

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
