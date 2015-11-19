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
package com.gemstone.gemfire.internal.offheap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.OutOfOffHeapMemoryException;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class SimpleMemoryAllocatorJUnitTest {

  private static int round(int multiple, int v) {
    return ((v+multiple-1)/multiple)*multiple;
  }
  @Test
  public void testBasics() {
    int BATCH_SIZE = com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.BATCH_SIZE;
    int TINY_MULTIPLE = com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.TINY_MULTIPLE;
//    int BIG_MULTIPLE = com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.FreeListManager.BIG_MULTIPLE;
    int HUGE_MULTIPLE = com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.HUGE_MULTIPLE;
    int perObjectOverhead = com.gemstone.gemfire.internal.offheap.Chunk.OFF_HEAP_HEADER_SIZE;
    int maxTiny = com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.MAX_TINY-perObjectOverhead;
//    int MIN_BIG_SIZE = round(BIG_MULTIPLE, maxTiny+perObjectOverhead+1)-perObjectOverhead;
//    int maxBig = com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.FreeListManager.MAX_BIG-perObjectOverhead;
    int minHuge = maxTiny+1;
    int TOTAL_MEM = (maxTiny+perObjectOverhead)*BATCH_SIZE /*+ (maxBig+perObjectOverhead)*BATCH_SIZE*/ + round(TINY_MULTIPLE, minHuge+1+perObjectOverhead)*BATCH_SIZE + (TINY_MULTIPLE+perObjectOverhead)*BATCH_SIZE /*+ (MIN_BIG_SIZE+perObjectOverhead)*BATCH_SIZE*/ + round(TINY_MULTIPLE, minHuge+perObjectOverhead+1);
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(TOTAL_MEM);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      assertEquals(TOTAL_MEM, ma.getFreeMemory());
      assertEquals(TOTAL_MEM, ma.freeList.getFreeFragmentMemory());
      assertEquals(0, ma.freeList.getFreeTinyMemory());
//      assertEquals(0, ma.freeList.getFreeBigMemory());
      assertEquals(0, ma.freeList.getFreeHugeMemory());
      MemoryChunk tinymc = ma.allocate(maxTiny, null);
      assertEquals(TOTAL_MEM-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
      assertEquals(round(TINY_MULTIPLE, maxTiny+perObjectOverhead)*(BATCH_SIZE-1), ma.freeList.getFreeTinyMemory());
//      MemoryChunk bigmc = ma.allocate(maxBig);
//      assertEquals(TOTAL_MEM-round(BIG_MULTIPLE, maxBig+perObjectOverhead)-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
//      assertEquals(round(BIG_MULTIPLE, maxBig+perObjectOverhead)*(BATCH_SIZE-1), ma.getFreeList().getFreeBigMemory());
      MemoryChunk hugemc = ma.allocate(minHuge, null);
      assertEquals(TOTAL_MEM-round(TINY_MULTIPLE, minHuge+perObjectOverhead)/*-round(BIG_MULTIPLE, maxBig+perObjectOverhead)*/-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
      long freeSlab = ma.freeList.getFreeFragmentMemory();
      long oldFreeHugeMemory = ma.freeList.getFreeHugeMemory();
      assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead)*(BATCH_SIZE-1), oldFreeHugeMemory);
      hugemc.release();
      assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead), ma.freeList.getFreeHugeMemory()-oldFreeHugeMemory);
      assertEquals(TOTAL_MEM/*-round(BIG_MULTIPLE, maxBig+perObjectOverhead)*/-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
//      long oldFreeBigMemory = ma.freeList.getFreeBigMemory();
//      bigmc.free();
//      assertEquals(round(BIG_MULTIPLE, maxBig+perObjectOverhead), ma.freeList.getFreeBigMemory()-oldFreeBigMemory);
      assertEquals(TOTAL_MEM-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
      long oldFreeTinyMemory = ma.freeList.getFreeTinyMemory();
      tinymc.release();
      assertEquals(round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.freeList.getFreeTinyMemory()-oldFreeTinyMemory);
      assertEquals(TOTAL_MEM, ma.getFreeMemory());
      // now lets reallocate from the free lists
      tinymc = ma.allocate(maxTiny, null);
      assertEquals(oldFreeTinyMemory, ma.freeList.getFreeTinyMemory());
      assertEquals(TOTAL_MEM-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
//      bigmc = ma.allocate(maxBig);
//      assertEquals(oldFreeBigMemory, ma.freeList.getFreeBigMemory());
//      assertEquals(TOTAL_MEM-round(BIG_MULTIPLE, maxBig+perObjectOverhead)-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
      hugemc = ma.allocate(minHuge, null);
      assertEquals(oldFreeHugeMemory, ma.freeList.getFreeHugeMemory());
      assertEquals(TOTAL_MEM-round(TINY_MULTIPLE, minHuge+perObjectOverhead)/*-round(BIG_MULTIPLE, maxBig+perObjectOverhead)*/-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
      hugemc.release();
      assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead), ma.freeList.getFreeHugeMemory()-oldFreeHugeMemory);
      assertEquals(TOTAL_MEM/*-round(BIG_MULTIPLE, maxBig+perObjectOverhead)*/-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
//      bigmc.free();
//      assertEquals(round(BIG_MULTIPLE, maxBig+perObjectOverhead), ma.freeList.getFreeBigMemory()-oldFreeBigMemory);
      assertEquals(TOTAL_MEM-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
      tinymc.release();
      assertEquals(round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.freeList.getFreeTinyMemory()-oldFreeTinyMemory);
      assertEquals(TOTAL_MEM, ma.getFreeMemory());
      // None of the reallocates should have come from the slab.
      assertEquals(freeSlab, ma.freeList.getFreeFragmentMemory());
      tinymc = ma.allocate(1, null);
      assertEquals(round(TINY_MULTIPLE, 1+perObjectOverhead), tinymc.getSize());
      assertEquals(freeSlab-(round(TINY_MULTIPLE, 1+perObjectOverhead)*BATCH_SIZE), ma.freeList.getFreeFragmentMemory());
      freeSlab = ma.freeList.getFreeFragmentMemory();
      tinymc.release();
      assertEquals(round(TINY_MULTIPLE, maxTiny+perObjectOverhead)+(round(TINY_MULTIPLE, 1+perObjectOverhead)*BATCH_SIZE), ma.freeList.getFreeTinyMemory()-oldFreeTinyMemory);
//      bigmc = ma.allocate(MIN_BIG_SIZE);
//      assertEquals(MIN_BIG_SIZE+perObjectOverhead, bigmc.getSize());
//      assertEquals(freeSlab-((MIN_BIG_SIZE+perObjectOverhead)*BATCH_SIZE), ma.freeList.getFreeFragmentMemory());
      
      hugemc = ma.allocate(minHuge+1, null);
      assertEquals(round(TINY_MULTIPLE, minHuge+1+perObjectOverhead), hugemc.getSize());
      assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead)*(BATCH_SIZE-1), ma.freeList.getFreeHugeMemory());
      hugemc.release();
      assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead)*BATCH_SIZE, ma.freeList.getFreeHugeMemory());
      hugemc = ma.allocate(minHuge, null);
      assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead)*(BATCH_SIZE-1), ma.freeList.getFreeHugeMemory());
      if (BATCH_SIZE > 1) {
        MemoryChunk hugemc2 = ma.allocate(minHuge, null);
        assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead)*(BATCH_SIZE-2), ma.freeList.getFreeHugeMemory());
        hugemc2.release();
        assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead)*(BATCH_SIZE-1), ma.freeList.getFreeHugeMemory());
      }
      hugemc.release();
      assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead)*BATCH_SIZE, ma.freeList.getFreeHugeMemory());
      // now that we do compaction the following allocate works.
      hugemc = ma.allocate(minHuge + HUGE_MULTIPLE + HUGE_MULTIPLE-1, null);

      //      assertEquals(minHuge+minHuge+1, ma.freeList.getFreeHugeMemory());
//      hugemc.free();
//      assertEquals(minHuge+minHuge+1+minHuge + HUGE_MULTIPLE + HUGE_MULTIPLE-1, ma.freeList.getFreeHugeMemory());
//      hugemc = ma.allocate(minHuge + HUGE_MULTIPLE);
//      assertEquals(minHuge + HUGE_MULTIPLE + HUGE_MULTIPLE-1, hugemc.getSize());
//      assertEquals(minHuge+minHuge+1, ma.freeList.getFreeHugeMemory());
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
  
  @Test
  public void testChunkCreateDirectByteBuffer() {
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(1024*1024);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      ByteBuffer bb = ByteBuffer.allocate(1024);
      for (int i=0; i < 1024; i++) {
        bb.put((byte) i);
      }
      bb.position(0);
      Chunk c = (Chunk) ma.allocateAndInitialize(bb.array(), false, false, null);
      assertEquals(1024, c.getDataSize());
      if (!Arrays.equals(bb.array(), c.getRawBytes())) {
        fail("arrays are not equal. Expected " + Arrays.toString(bb.array()) + " but found: " + Arrays.toString(c.getRawBytes()));
      }
      ByteBuffer dbb = c.createDirectByteBuffer();
      assertEquals(true, dbb.isDirect());
      assertEquals(bb, dbb);
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
  
  @Test
  public void testCompaction() {
    final int perObjectOverhead = com.gemstone.gemfire.internal.offheap.Chunk.OFF_HEAP_HEADER_SIZE;
    final int BIG_ALLOC_SIZE = 150000;
    final int SMALL_ALLOC_SIZE = BIG_ALLOC_SIZE/2;
    final int TOTAL_MEM = BIG_ALLOC_SIZE;
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(TOTAL_MEM);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      MemoryChunk bmc = ma.allocate(BIG_ALLOC_SIZE-perObjectOverhead, null);
      try {
        MemoryChunk smc = ma.allocate(SMALL_ALLOC_SIZE-perObjectOverhead, null);
        fail("Expected out of memory");
      } catch (OutOfOffHeapMemoryException expected) {
      }
      bmc.release();
      assertEquals(TOTAL_MEM, ma.freeList.getFreeMemory());
      MemoryChunk smc1 = ma.allocate(SMALL_ALLOC_SIZE-perObjectOverhead, null);
      MemoryChunk smc2 = ma.allocate(SMALL_ALLOC_SIZE-perObjectOverhead, null);
      smc2.release();
      assertEquals(TOTAL_MEM-SMALL_ALLOC_SIZE, ma.freeList.getFreeMemory());
      try {
        bmc = ma.allocate(BIG_ALLOC_SIZE-perObjectOverhead, null);
        fail("Expected out of memory");
      } catch (OutOfOffHeapMemoryException expected) {
      }
      smc1.release();
      assertEquals(TOTAL_MEM, ma.freeList.getFreeMemory());
      bmc = ma.allocate(BIG_ALLOC_SIZE-perObjectOverhead, null);
      bmc.release();
      assertEquals(TOTAL_MEM, ma.freeList.getFreeMemory());
      ArrayList<MemoryChunk> mcs = new ArrayList<MemoryChunk>();
      for (int i=0; i < BIG_ALLOC_SIZE/(8+perObjectOverhead); i++) {
        mcs.add(ma.allocate(8, null));
      }
      checkMcs(mcs);
      assertEquals(0, ma.freeList.getFreeMemory());
      try {
        ma.allocate(8, null);
        fail("expected out of memory");
      } catch (OutOfOffHeapMemoryException expected) {
      }
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals(8+perObjectOverhead, ma.freeList.getFreeMemory());
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals((8+perObjectOverhead)*2, ma.freeList.getFreeMemory());
      ma.allocate(16, null).release(); // allocates and frees 16+perObjectOverhead; still have perObjectOverhead
      assertEquals((8+perObjectOverhead)*2, ma.freeList.getFreeMemory());
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals((8+perObjectOverhead)*3, ma.freeList.getFreeMemory());
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals((8+perObjectOverhead)*4, ma.freeList.getFreeMemory());
      // At this point I should have 8*4 + perObjectOverhead*4 of free memory
      ma.allocate(8*4+perObjectOverhead*3, null).release();
      assertEquals((8+perObjectOverhead)*4, ma.freeList.getFreeMemory());
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals((8+perObjectOverhead)*5, ma.freeList.getFreeMemory());
      // At this point I should have 8*5 + perObjectOverhead*5 of free memory
      try {
        ma.allocate((8*5+perObjectOverhead*4)+1, null);
        fail("expected out of memory");
      } catch (OutOfOffHeapMemoryException expected) {
      }
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals((8+perObjectOverhead)*6, ma.freeList.getFreeMemory());
      checkMcs(mcs);
      // At this point I should have 8*6 + perObjectOverhead*6 of free memory
      MemoryChunk mc24 = ma.allocate(24, null);
      checkMcs(mcs);
      assertEquals((8+perObjectOverhead)*6 - (24+perObjectOverhead), ma.freeList.getFreeMemory());
      // At this point I should have 8*3 + perObjectOverhead*5 of free memory
      MemoryChunk mc16 = ma.allocate(16, null);
      checkMcs(mcs);
      assertEquals((8+perObjectOverhead)*6 - (24+perObjectOverhead) - (16+perObjectOverhead), ma.freeList.getFreeMemory());
      // At this point I should have 8*1 + perObjectOverhead*4 of free memory
      mcs.add(ma.allocate(8, null));
      checkMcs(mcs);
      assertEquals((8+perObjectOverhead)*6 - (24+perObjectOverhead) - (16+perObjectOverhead) - (8+perObjectOverhead), ma.freeList.getFreeMemory());
      // At this point I should have 8*0 + perObjectOverhead*3 of free memory
      MemoryChunk mcDO = ma.allocate(perObjectOverhead*2, null);
      checkMcs(mcs);
      // At this point I should have 8*0 + perObjectOverhead*0 of free memory
      assertEquals(0, ma.freeList.getFreeMemory());
      try {
        ma.allocate(1, null);
        fail("expected out of memory");
      } catch (OutOfOffHeapMemoryException expected) {
      }
      checkMcs(mcs);
      assertEquals(0, ma.freeList.getFreeMemory());
      mcDO.release();
      assertEquals((perObjectOverhead*3), ma.freeList.getFreeMemory());
      mcs.remove(mcs.size()-1).release();
      assertEquals((perObjectOverhead*3)+(8+perObjectOverhead), ma.freeList.getFreeMemory());
      mc16.release();
      assertEquals((perObjectOverhead*3)+(8+perObjectOverhead)+(16+perObjectOverhead), ma.freeList.getFreeMemory());
      mc24.release();
      assertEquals((perObjectOverhead*3)+(8+perObjectOverhead)+(16+perObjectOverhead)+(24+perObjectOverhead), ma.freeList.getFreeMemory());
      
      long freeMem = ma.freeList.getFreeMemory();
      for (MemoryChunk mc: mcs) {
        mc.release();
        assertEquals(freeMem+(8+perObjectOverhead), ma.freeList.getFreeMemory());
        freeMem += (8+perObjectOverhead);
      }
      mcs.clear();
      assertEquals(TOTAL_MEM, ma.freeList.getFreeMemory());
      bmc = ma.allocate(BIG_ALLOC_SIZE-perObjectOverhead, null);
      bmc.release();
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
  
  long expectedMemoryUsage;
  boolean memoryUsageEventReceived;
  @Test
  public void testUsageEventListener() {
    final int perObjectOverhead = com.gemstone.gemfire.internal.offheap.Chunk.OFF_HEAP_HEADER_SIZE;
    final int SMALL_ALLOC_SIZE = 1000;
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(3000);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      MemoryUsageListener listener = new MemoryUsageListener() {
        @Override
        public void updateMemoryUsed(final long bytesUsed) {
          SimpleMemoryAllocatorJUnitTest.this.memoryUsageEventReceived = true;
          assertEquals(SimpleMemoryAllocatorJUnitTest.this.expectedMemoryUsage, bytesUsed);
        }
      };
      ma.addMemoryUsageListener(listener);
      
      this.expectedMemoryUsage = SMALL_ALLOC_SIZE;
      this.memoryUsageEventReceived = false;
      MemoryChunk smc = ma.allocate(SMALL_ALLOC_SIZE-perObjectOverhead, null);
      assertEquals(true, this.memoryUsageEventReceived);
      
      this.expectedMemoryUsage = SMALL_ALLOC_SIZE * 2;
      this.memoryUsageEventReceived = false;
      smc = ma.allocate(SMALL_ALLOC_SIZE-perObjectOverhead, null);
      assertEquals(true, this.memoryUsageEventReceived);
      
      ma.removeMemoryUsageListener(listener);
      
      this.expectedMemoryUsage = SMALL_ALLOC_SIZE * 2;
      this.memoryUsageEventReceived = false;
      smc = ma.allocate(SMALL_ALLOC_SIZE-perObjectOverhead, null);
      assertEquals(false, this.memoryUsageEventReceived);
      
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
  private void checkMcs(ArrayList<MemoryChunk> mcs) {
    for (MemoryChunk mc: mcs) {
      assertEquals(8+8, mc.getSize());
    }
  }
  
  @Test
  public void testOutOfOffHeapMemory() {
    final int perObjectOverhead = com.gemstone.gemfire.internal.offheap.Chunk.OFF_HEAP_HEADER_SIZE;
    final int BIG_ALLOC_SIZE = 150000;
    final int SMALL_ALLOC_SIZE = BIG_ALLOC_SIZE/2;
    final int TOTAL_MEM = BIG_ALLOC_SIZE;
    final UnsafeMemoryChunk slab = new UnsafeMemoryChunk(TOTAL_MEM);
    final AtomicReference<OutOfOffHeapMemoryException> ooom = new AtomicReference<OutOfOffHeapMemoryException>();
    final OutOfOffHeapMemoryListener oooml = new OutOfOffHeapMemoryListener() {
      @Override
      public void outOfOffHeapMemory(OutOfOffHeapMemoryException cause) {
        ooom.set(cause);
      }
      @Override
      public void close() {
      }
    };
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.create(oooml, new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      // make a big allocation
      MemoryChunk bmc = ma.allocate(BIG_ALLOC_SIZE-perObjectOverhead, null);
      assertNull(ooom.get());
      // drive the ma to ooom with small allocations
      try {
        MemoryChunk smc = ma.allocate(SMALL_ALLOC_SIZE-perObjectOverhead, null);
        fail("Expected out of memory");
      } catch (OutOfOffHeapMemoryException expected) {
      }
      assertNotNull(ooom.get());
      assertTrue(ooom.get().getMessage().contains("Out of off-heap memory. Could not allocate size of "));
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
}
