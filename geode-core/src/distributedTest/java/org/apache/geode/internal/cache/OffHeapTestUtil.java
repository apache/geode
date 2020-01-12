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

import java.util.List;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.internal.offheap.MemoryAllocatorImpl;
import org.apache.geode.internal.offheap.MemoryBlock;
import org.apache.geode.internal.offheap.RefCountChangeInfo;
import org.apache.geode.internal.offheap.ReferenceCountHelper;

public class OffHeapTestUtil {

  public static void checkOrphans(InternalCache cache) {
    MemoryAllocatorImpl allocator;
    try {
      allocator = MemoryAllocatorImpl.getAllocator();
    } catch (CacheClosedException ignore) {
      // no off-heap memory so no orphans
      return;
    }

    long end = System.currentTimeMillis() + 5000;
    List<MemoryBlock> orphans = allocator.getOrphans(cache);

    // Wait for the orphans to go away
    while (orphans != null && !orphans.isEmpty() && System.currentTimeMillis() < end) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      orphans = allocator.getOrphans(null);
    }

    if (orphans != null && !orphans.isEmpty()) {
      List<RefCountChangeInfo> info =
          ReferenceCountHelper.getRefCountInfo(orphans.get(0).getAddress());
      System.out.println("FOUND ORPHAN!!");
      System.out.println("Sample orphan: " + orphans.get(0));
      System.out.println("Orphan info: " + info);
    }

    assertThat(orphans).isEmpty();
  }
}
