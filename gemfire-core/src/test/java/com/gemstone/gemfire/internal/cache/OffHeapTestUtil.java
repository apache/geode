/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.gopivotal.com/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.internal.offheap.MemoryBlock;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.RefCountChangeInfo;

@SuppressWarnings("deprecation")
public class OffHeapTestUtil {

  public static void checkOrphans() {
    SimpleMemoryAllocatorImpl allocator = null;
    try {
      allocator = SimpleMemoryAllocatorImpl.getAllocator();
    } catch (CacheClosedException ignore) {
      // no off-heap memory so no orphans
      return;
    }
    long end = System.currentTimeMillis() + 5000;
    List<MemoryBlock> orphans = allocator.getOrphans();
    
    //Wait for the orphans to go away
    while(orphans != null && !orphans.isEmpty() && System.currentTimeMillis() < end) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      orphans = allocator.getOrphans();
    }
    
    if(orphans != null && ! orphans.isEmpty()) {
      List<RefCountChangeInfo> info = SimpleMemoryAllocatorImpl.getRefCountInfo(orphans.get(0).getMemoryAddress());
      System.out.println("FOUND ORPHAN!!");
      System.out.println("Sample orphan: " + orphans.get(0));
      System.out.println("Orphan info: " + info);
    }
    Assert.assertEquals(Collections.emptyList(), orphans);
  }

}

