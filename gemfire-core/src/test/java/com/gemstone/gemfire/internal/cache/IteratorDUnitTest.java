/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheTestCase;

import java.util.Iterator;

import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;

/**
 * Test that keys iterator do not returned keys with removed token as its values
 * @author sbawaska
 *
 */
public class IteratorDUnitTest extends CacheTestCase {

  /**
   * @param name
   */
  public IteratorDUnitTest(String name) {
    super(name);
  }

  public void testKeysIteratorOnLR() throws Exception {
    final String regionName = getUniqueName();
    Region r = getGemfireCache().createRegionFactory(RegionShortcut.REPLICATE).create(regionName);
    r.put("key", "value");
    r.put("key2", "value2");
    r.put("key3", "value3");
    LocalRegion lr = (LocalRegion)r;
    // simulate a removed key
//    lr.getRegionMap().getEntry("key")._setValue(Token.REMOVED_PHASE1);
    lr.getRegionMap().getEntry("key").setValue(lr,Token.REMOVED_PHASE1);
    Iterator it = r.keySet().iterator();
    int numKeys = 0;
    while (it.hasNext()) {
      it.next();
      numKeys++;
    }
    assertEquals(2, numKeys);
  }
  
  public void testKeysIteratorOnPR() {
    Host host = Host.getHost(0);
    VM accessor = host.getVM(0);
    VM datastore = host.getVM(1);
    final String regionName = getUniqueName();
    
    accessor.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getGemfireCache().createRegionFactory(RegionShortcut.PARTITION_PROXY).create(regionName);
        return null;
      }
    });
    datastore.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getGemfireCache().createRegionFactory(RegionShortcut.PARTITION).create(regionName);
        r.put("key", "value");
        r.put("key2", "value2");
        r.put("key3", "value3");
        PartitionedRegion pr = (PartitionedRegion)r;
        BucketRegion br = pr.getBucketRegion("key");
        assertNotNull(br);
        // simulate a removed key
        br.getRegionMap().getEntry("key").setValue(pr,Token.REMOVED_PHASE1);
        return null;
      }
    });
    accessor.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getGemfireCache().getRegion(regionName);
        Iterator it = r.keySet().iterator();
        int numKeys = 0;
        while (it.hasNext()) {
          it.next();
          numKeys++;
        }
        assertEquals(2, numKeys);
        return null;
      }
    });
  }
}
