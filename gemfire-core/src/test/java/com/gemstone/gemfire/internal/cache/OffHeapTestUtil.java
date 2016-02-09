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

import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.internal.offheap.MemoryBlock;
import com.gemstone.gemfire.internal.offheap.RefCountChangeInfo;
import com.gemstone.gemfire.internal.offheap.ReferenceCountHelper;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl;

@SuppressWarnings("deprecation")
public class OffHeapTestUtil {

  public static void checkOrphans() { // TODO:KIRK: need to do something special to guarantee proper tearDown
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
      List<RefCountChangeInfo> info = ReferenceCountHelper.getRefCountInfo(orphans.get(0).getMemoryAddress());
      System.out.println("FOUND ORPHAN!!");
      System.out.println("Sample orphan: " + orphans.get(0));
      System.out.println("Orphan info: " + info);
    }
    Assert.assertEquals(Collections.emptyList(), orphans);
  }

}

