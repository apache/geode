/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Iterator;
import java.util.Properties;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.internal.util.concurrent.CustomEntryConcurrentHashMap;
import com.gemstone.gemfire.test.junit.categories.HoplogTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest
;

@Category({IntegrationTest.class, HoplogTest.class})
public class HDFSRegionOperationsOffHeapJUnitTest extends HDFSRegionOperationsJUnitTest {
  static {
    System.setProperty("gemfire.trackOffHeapRefCounts", "true");
    System.setProperty("gemfire.trackOffHeapFreedRefCounts", "true");
  }

  @Override
  protected void clearBackingCHM(Region<Integer, String> r) {
    PartitionedRegion pr = (PartitionedRegion)r;
    for (BucketRegion br : pr.getDataStore().getAllLocalBucketRegions()) {
      assertTrue(br.getRegionMap() instanceof HDFSRegionMap);
      CustomEntryConcurrentHashMap chm = ((AbstractRegionMap)br.getRegionMap())._getMap();
      Iterator it = chm.keySet().iterator();
      while (it.hasNext()) {
        Object key = it.next();
        OffHeapRegionEntry re = (OffHeapRegionEntry) chm.remove(key);
        assert re != null;
        re.release();
      }
      // wait here to make sure that the queue has been flushed
    }
    sleep(pr.getFullPath());
  }

  @Override
  public void tearDown() throws Exception {
    
    OffHeapTestUtil.checkOrphans();
    super.tearDown();
  }
  @Override
  protected Region<Integer, String> createRegion(String regionName) {
    RegionFactory<Integer, String> rf = cache.createRegionFactory(RegionShortcut.PARTITION);
    PartitionAttributes prAttr = new PartitionAttributesFactory().setTotalNumBuckets(10).create();
    rf.setPartitionAttributes(prAttr);
    rf.setOffHeap(true);
    rf.setHDFSStoreName(hdfsStore.getName());
    Region<Integer, String> r = rf.create(regionName);
//    addListener(r);
    
    ((PartitionedRegion) r).setQueryHDFS(true);
    return r;
  }
  @Override
  protected Properties getDSProps() {
    Properties props = super.getDSProps();
    props.setProperty("off-heap-memory-size", "50m");
    return props;
  }
  
  

}
