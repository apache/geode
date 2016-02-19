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

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.OutOfOffHeapMemoryException;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.internal.logging.NullLogWriter;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class SimpleMemoryAllocatorJUnitTest {
  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private static int round(int multiple, int v) {
    return ((v+multiple-1)/multiple)*multiple;
  }
  @Test
  public void testNullGetAllocator() {
    try {
      SimpleMemoryAllocatorImpl.getAllocator();
      fail("expected CacheClosedException");
    } catch (CacheClosedException expected) {
    }
  }
  @Test
  public void testConstructor() {
    try {
      SimpleMemoryAllocatorImpl.createForUnitTest(null, null, null);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
  }
  /**
   * Logger that remembers the last severe message
   */
  private static class LastSevereLogger extends NullLogWriter {
    private String lastSevereMessage;
    private Throwable lastSevereThrowable;
    
    private void setLastSevere(String msg, Throwable ex) {
      this.lastSevereMessage = msg;
      this.lastSevereThrowable = ex;
    }
    public String getLastSevereMessage() {
      return this.lastSevereMessage;
    }
    public Throwable getLastSevereThrowable() {
      return this.lastSevereThrowable;
    }
    @Override
    public void severe(String msg, Throwable ex) {
      setLastSevere(msg, ex);
    }
    @Override
    public void severe(String msg) {
      setLastSevere(msg, null);
    }
    @Override
    public void severe(Throwable ex) {
      setLastSevere(null, ex);
    }
  }
  @Test
  public void testCreate() {
    System.setProperty(SimpleMemoryAllocatorImpl.FREE_OFF_HEAP_MEMORY_PROPERTY, "false");
    {
      NullOutOfOffHeapMemoryListener listener = new NullOutOfOffHeapMemoryListener();
      NullOffHeapMemoryStats stats = new NullOffHeapMemoryStats();
      LastSevereLogger logger = new LastSevereLogger();
      try {
        SimpleMemoryAllocatorImpl.createForUnitTest(listener, stats, logger, 10, 950, 100,
            new AddressableMemoryChunkFactory() {
          @Override
          public AddressableMemoryChunk create(int size) {
            throw new OutOfMemoryError("expected");
          }
        });
      } catch (OutOfMemoryError expected) {
      }
      assertTrue(listener.isClosed());
      assertTrue(stats.isClosed());
      assertEquals(null, logger.getLastSevereThrowable());
      assertEquals(null, logger.getLastSevereMessage());
     }
    {
      NullOutOfOffHeapMemoryListener listener = new NullOutOfOffHeapMemoryListener();
      NullOffHeapMemoryStats stats = new NullOffHeapMemoryStats();
      LastSevereLogger logger = new LastSevereLogger();
      int MAX_SLAB_SIZE = 100;
      try {
        AddressableMemoryChunkFactory factory = new AddressableMemoryChunkFactory() {
          private int createCount = 0;
          @Override
          public AddressableMemoryChunk create(int size) {
            createCount++;
            if (createCount == 1) {
              return new UnsafeMemoryChunk(size);
            } else {
              throw new OutOfMemoryError("expected");
            }
          }
        };
        SimpleMemoryAllocatorImpl.createForUnitTest(listener, stats, logger, 10, 950, MAX_SLAB_SIZE, factory);
      } catch (OutOfMemoryError expected) {
      }
      assertTrue(listener.isClosed());
      assertTrue(stats.isClosed());
      assertEquals(null, logger.getLastSevereThrowable());
      assertEquals("Off-heap memory creation failed after successfully allocating " + MAX_SLAB_SIZE + " bytes of off-heap memory.", logger.getLastSevereMessage());
    }
    {
      NullOutOfOffHeapMemoryListener listener = new NullOutOfOffHeapMemoryListener();
      NullOffHeapMemoryStats stats = new NullOffHeapMemoryStats();
      AddressableMemoryChunkFactory factory = new AddressableMemoryChunkFactory() {
        @Override
        public AddressableMemoryChunk create(int size) {
          return new UnsafeMemoryChunk(size);
        }
      };
      MemoryAllocator ma = 
        SimpleMemoryAllocatorImpl.createForUnitTest(listener, stats, new NullLogWriter(), 10, 950, 100, factory);
      try {
        assertFalse(listener.isClosed());
        assertFalse(stats.isClosed());
        ma.close();
        assertTrue(listener.isClosed());
        assertFalse(stats.isClosed());
        listener = new NullOutOfOffHeapMemoryListener();
        NullOffHeapMemoryStats stats2 = new NullOffHeapMemoryStats();
        {
          UnsafeMemoryChunk slab = new UnsafeMemoryChunk(1024);
          try {
            SimpleMemoryAllocatorImpl.createForUnitTest(listener, stats2, new UnsafeMemoryChunk[]{slab});
          } catch (IllegalStateException expected) {
            assertTrue("unexpected message: " + expected.getMessage(), 
                expected.getMessage().equals("attempted to reuse existing off-heap memory even though new off-heap memory was allocated"));
          } finally {
            slab.release();
          }
          assertFalse(stats.isClosed());
          assertTrue(listener.isClosed());
          assertTrue(stats2.isClosed());
        }
        listener = new NullOutOfOffHeapMemoryListener();
        stats2 = new NullOffHeapMemoryStats();
        MemoryAllocator ma2 = SimpleMemoryAllocatorImpl.createForUnitTest(listener, stats2, new NullLogWriter(), 10, 950, 100, factory);
        assertSame(ma, ma2);
        assertTrue(stats.isClosed());
        assertFalse(listener.isClosed());
        assertFalse(stats2.isClosed());
        stats = stats2;
        ma.close();
        assertTrue(listener.isClosed());
        assertFalse(stats.isClosed());
      } finally {
        SimpleMemoryAllocatorImpl.freeOffHeapMemory();
      }
      assertTrue(stats.isClosed());
    }
  }
  @Test
  public void testBasics() {
    int BATCH_SIZE = 1;
    int TINY_MULTIPLE = com.gemstone.gemfire.internal.offheap.FreeListManager.TINY_MULTIPLE;
    int HUGE_MULTIPLE = com.gemstone.gemfire.internal.offheap.FreeListManager.HUGE_MULTIPLE;
    int perObjectOverhead = com.gemstone.gemfire.internal.offheap.ObjectChunk.OFF_HEAP_HEADER_SIZE;
    int maxTiny = com.gemstone.gemfire.internal.offheap.FreeListManager.MAX_TINY-perObjectOverhead;
    int minHuge = maxTiny+1;
    int TOTAL_MEM = (maxTiny+perObjectOverhead)*BATCH_SIZE /*+ (maxBig+perObjectOverhead)*BATCH_SIZE*/ + round(TINY_MULTIPLE, minHuge+1+perObjectOverhead)*BATCH_SIZE + (TINY_MULTIPLE+perObjectOverhead)*BATCH_SIZE /*+ (MIN_BIG_SIZE+perObjectOverhead)*BATCH_SIZE*/ + round(TINY_MULTIPLE, minHuge+perObjectOverhead+1);
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(TOTAL_MEM);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      assertEquals(TOTAL_MEM, ma.getFreeMemory());
      assertEquals(TOTAL_MEM, ma.freeList.getFreeFragmentMemory());
      assertEquals(0, ma.freeList.getFreeTinyMemory());
      assertEquals(0, ma.freeList.getFreeHugeMemory());
      MemoryChunk tinymc = ma.allocate(maxTiny);
      assertEquals(TOTAL_MEM-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
      assertEquals(round(TINY_MULTIPLE, maxTiny+perObjectOverhead)*(BATCH_SIZE-1), ma.freeList.getFreeTinyMemory());
      MemoryChunk hugemc = ma.allocate(minHuge);
      assertEquals(TOTAL_MEM-round(TINY_MULTIPLE, minHuge+perObjectOverhead)/*-round(BIG_MULTIPLE, maxBig+perObjectOverhead)*/-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
      long freeSlab = ma.freeList.getFreeFragmentMemory();
      long oldFreeHugeMemory = ma.freeList.getFreeHugeMemory();
      assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead)*(BATCH_SIZE-1), oldFreeHugeMemory);
      hugemc.release();
      assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead), ma.freeList.getFreeHugeMemory()-oldFreeHugeMemory);
      assertEquals(TOTAL_MEM/*-round(BIG_MULTIPLE, maxBig+perObjectOverhead)*/-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
      assertEquals(TOTAL_MEM-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
      long oldFreeTinyMemory = ma.freeList.getFreeTinyMemory();
      tinymc.release();
      assertEquals(round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.freeList.getFreeTinyMemory()-oldFreeTinyMemory);
      assertEquals(TOTAL_MEM, ma.getFreeMemory());
      // now lets reallocate from the free lists
      tinymc = ma.allocate(maxTiny);
      assertEquals(oldFreeTinyMemory, ma.freeList.getFreeTinyMemory());
      assertEquals(TOTAL_MEM-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
      hugemc = ma.allocate(minHuge);
      assertEquals(oldFreeHugeMemory, ma.freeList.getFreeHugeMemory());
      assertEquals(TOTAL_MEM-round(TINY_MULTIPLE, minHuge+perObjectOverhead)/*-round(BIG_MULTIPLE, maxBig+perObjectOverhead)*/-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
      hugemc.release();
      assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead), ma.freeList.getFreeHugeMemory()-oldFreeHugeMemory);
      assertEquals(TOTAL_MEM/*-round(BIG_MULTIPLE, maxBig+perObjectOverhead)*/-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
      assertEquals(TOTAL_MEM-round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.getFreeMemory());
      tinymc.release();
      assertEquals(round(TINY_MULTIPLE, maxTiny+perObjectOverhead), ma.freeList.getFreeTinyMemory()-oldFreeTinyMemory);
      assertEquals(TOTAL_MEM, ma.getFreeMemory());
      // None of the reallocates should have come from the slab.
      assertEquals(freeSlab, ma.freeList.getFreeFragmentMemory());
      tinymc = ma.allocate(1);
      assertEquals(round(TINY_MULTIPLE, 1+perObjectOverhead), tinymc.getSize());
      assertEquals(freeSlab-(round(TINY_MULTIPLE, 1+perObjectOverhead)*BATCH_SIZE), ma.freeList.getFreeFragmentMemory());
      freeSlab = ma.freeList.getFreeFragmentMemory();
      tinymc.release();
      assertEquals(round(TINY_MULTIPLE, maxTiny+perObjectOverhead)+(round(TINY_MULTIPLE, 1+perObjectOverhead)*BATCH_SIZE), ma.freeList.getFreeTinyMemory()-oldFreeTinyMemory);
      
      hugemc = ma.allocate(minHuge+1);
      assertEquals(round(TINY_MULTIPLE, minHuge+1+perObjectOverhead), hugemc.getSize());
      assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead)*(BATCH_SIZE-1), ma.freeList.getFreeHugeMemory());
      hugemc.release();
      assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead)*BATCH_SIZE, ma.freeList.getFreeHugeMemory());
      hugemc = ma.allocate(minHuge);
      assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead)*(BATCH_SIZE-1), ma.freeList.getFreeHugeMemory());
      if (BATCH_SIZE > 1) {
        MemoryChunk hugemc2 = ma.allocate(minHuge);
        assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead)*(BATCH_SIZE-2), ma.freeList.getFreeHugeMemory());
        hugemc2.release();
        assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead)*(BATCH_SIZE-1), ma.freeList.getFreeHugeMemory());
      }
      hugemc.release();
      assertEquals(round(TINY_MULTIPLE, minHuge+perObjectOverhead)*BATCH_SIZE, ma.freeList.getFreeHugeMemory());
      // now that we do compaction the following allocate works.
      hugemc = ma.allocate(minHuge + HUGE_MULTIPLE + HUGE_MULTIPLE-1);
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
  
  @Test
  public void testChunkCreateDirectByteBuffer() {
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(1024*1024);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      ByteBuffer bb = ByteBuffer.allocate(1024);
      for (int i=0; i < 1024; i++) {
        bb.put((byte) i);
      }
      bb.position(0);
      ObjectChunk c = (ObjectChunk) ma.allocateAndInitialize(bb.array(), false, false);
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
  public void testDebugLog() {
    SimpleMemoryAllocatorImpl.debugLog("test debug log", false);
    SimpleMemoryAllocatorImpl.debugLog("test debug log", true);
  }
  @Test
  public void testGetLostChunks() {
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(1024*1024);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      assertEquals(Collections.emptyList(), ma.getLostChunks());
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
  @Test
  public void testFindSlab() {
    final int SLAB_SIZE = 1024*1024;
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(SLAB_SIZE);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      assertEquals(0, ma.findSlab(slab.getMemoryAddress()));
      assertEquals(0, ma.findSlab(slab.getMemoryAddress()+SLAB_SIZE-1));
      try {
        ma.findSlab(slab.getMemoryAddress()-1);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
      }
      try {
        ma.findSlab(slab.getMemoryAddress()+SLAB_SIZE);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
      }
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
  @Test
  public void testValidateAddressAndSize() {
    final int SLAB_SIZE = 1024*1024;
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(SLAB_SIZE);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      try {
        SimpleMemoryAllocatorImpl.validateAddress(0L);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
        assertEquals("Unexpected exception message: " + expected.getMessage(), true, expected.getMessage().contains("addr was smaller than expected"));
      }
      try {
        SimpleMemoryAllocatorImpl.validateAddress(1L);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
        assertEquals("Unexpected exception message: " + expected.getMessage(), true, expected.getMessage().contains("Valid addresses must be in one of the following ranges:"));
      }
      SimpleMemoryAllocatorImpl.validateAddressAndSizeWithinSlab(slab.getMemoryAddress(), SLAB_SIZE, false);
      SimpleMemoryAllocatorImpl.validateAddressAndSizeWithinSlab(slab.getMemoryAddress(), SLAB_SIZE, true);
      SimpleMemoryAllocatorImpl.validateAddressAndSizeWithinSlab(slab.getMemoryAddress(), -1, true);
      try {
        SimpleMemoryAllocatorImpl.validateAddressAndSizeWithinSlab(slab.getMemoryAddress()-1, SLAB_SIZE, true);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
        assertEquals("Unexpected exception message: " + expected.getMessage(), true, expected.getMessage().equals(" address 0x" + Long.toString(slab.getMemoryAddress()-1, 16) + " does not address the original slab memory"));
      }
      try {
        SimpleMemoryAllocatorImpl.validateAddressAndSizeWithinSlab(slab.getMemoryAddress(), SLAB_SIZE+1, true);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
        assertEquals("Unexpected exception message: " + expected.getMessage(), true, expected.getMessage().equals(" address 0x" + Long.toString(slab.getMemoryAddress()+SLAB_SIZE, 16) + " does not address the original slab memory"));
      }
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
  @Test
  public void testMemoryInspection() {
    final int SLAB_SIZE = 1024*1024;
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(SLAB_SIZE);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      MemoryInspector inspector = ma.getMemoryInspector();
      assertNotNull(inspector);
      assertEquals(null, inspector.getFirstBlock());
      assertEquals(Collections.emptyList(), inspector.getSnapshot());
      assertEquals(Collections.emptyList(), inspector.getAllocatedBlocks());
      assertEquals(null, inspector.getBlockAfter(null));
      inspector.createSnapshot();
      // call this twice for code coverage
      inspector.createSnapshot();
      try {
        assertEquals(inspector.getAllBlocks(), inspector.getSnapshot());
        MemoryBlock firstBlock = inspector.getFirstBlock();
        assertNotNull(firstBlock);
        assertEquals(1024*1024, firstBlock.getBlockSize());
        assertEquals("N/A", firstBlock.getDataType());
        assertEquals(-1, firstBlock.getFreeListId());
        assertTrue(firstBlock.getMemoryAddress() > 0);
        assertNull(firstBlock.getNextBlock());
        assertEquals(0, firstBlock.getRefCount());
        assertEquals(0, firstBlock.getSlabId());
        assertEquals(MemoryBlock.State.UNUSED, firstBlock.getState());
        assertFalse(firstBlock.isCompressed());
        assertFalse(firstBlock.isSerialized());
        assertEquals(null, inspector.getBlockAfter(firstBlock));
      } finally {
        inspector.clearSnapshot();
      }
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void testClose() {
    System.setProperty(SimpleMemoryAllocatorImpl.FREE_OFF_HEAP_MEMORY_PROPERTY, "false");
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(1024*1024);
    boolean freeSlab = true;
    UnsafeMemoryChunk[] slabs = new UnsafeMemoryChunk[]{slab};
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), slabs);
      ma.close();
      ma.close();
      System.setProperty(SimpleMemoryAllocatorImpl.FREE_OFF_HEAP_MEMORY_PROPERTY, "true");
      try {
        ma = SimpleMemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), slabs);
        ma.close();
        freeSlab = false;
        ma.close();
      } finally {
        System.clearProperty(SimpleMemoryAllocatorImpl.FREE_OFF_HEAP_MEMORY_PROPERTY);
      }
    } finally {
      if (freeSlab) {
        SimpleMemoryAllocatorImpl.freeOffHeapMemory();
      }
    }
    
  }
  
  @Test
  public void testCompaction() {
    final int perObjectOverhead = com.gemstone.gemfire.internal.offheap.ObjectChunk.OFF_HEAP_HEADER_SIZE;
    final int BIG_ALLOC_SIZE = 150000;
    final int SMALL_ALLOC_SIZE = BIG_ALLOC_SIZE/2;
    final int TOTAL_MEM = BIG_ALLOC_SIZE;
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(TOTAL_MEM);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      MemoryChunk bmc = ma.allocate(BIG_ALLOC_SIZE-perObjectOverhead);
      try {
        MemoryChunk smc = ma.allocate(SMALL_ALLOC_SIZE-perObjectOverhead);
        fail("Expected out of memory");
      } catch (OutOfOffHeapMemoryException expected) {
      }
      bmc.release();
      assertEquals(TOTAL_MEM, ma.freeList.getFreeMemory());
      MemoryChunk smc1 = ma.allocate(SMALL_ALLOC_SIZE-perObjectOverhead);
      MemoryChunk smc2 = ma.allocate(SMALL_ALLOC_SIZE-perObjectOverhead);
      smc2.release();
      assertEquals(TOTAL_MEM-SMALL_ALLOC_SIZE, ma.freeList.getFreeMemory());
      try {
        bmc = ma.allocate(BIG_ALLOC_SIZE-perObjectOverhead);
        fail("Expected out of memory");
      } catch (OutOfOffHeapMemoryException expected) {
      }
      smc1.release();
      assertEquals(TOTAL_MEM, ma.freeList.getFreeMemory());
      bmc = ma.allocate(BIG_ALLOC_SIZE-perObjectOverhead);
      bmc.release();
      assertEquals(TOTAL_MEM, ma.freeList.getFreeMemory());
      ArrayList<MemoryChunk> mcs = new ArrayList<MemoryChunk>();
      for (int i=0; i < BIG_ALLOC_SIZE/(8+perObjectOverhead); i++) {
        mcs.add(ma.allocate(8));
      }
      checkMcs(mcs);
      assertEquals(0, ma.freeList.getFreeMemory());
      try {
        ma.allocate(8);
        fail("expected out of memory");
      } catch (OutOfOffHeapMemoryException expected) {
      }
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals(8+perObjectOverhead, ma.freeList.getFreeMemory());
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals((8+perObjectOverhead)*2, ma.freeList.getFreeMemory());
      ma.allocate(16).release(); // allocates and frees 16+perObjectOverhead; still have perObjectOverhead
      assertEquals((8+perObjectOverhead)*2, ma.freeList.getFreeMemory());
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals((8+perObjectOverhead)*3, ma.freeList.getFreeMemory());
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals((8+perObjectOverhead)*4, ma.freeList.getFreeMemory());
      // At this point I should have 8*4 + perObjectOverhead*4 of free memory
      ma.allocate(8*4+perObjectOverhead*3).release();
      assertEquals((8+perObjectOverhead)*4, ma.freeList.getFreeMemory());
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals((8+perObjectOverhead)*5, ma.freeList.getFreeMemory());
      // At this point I should have 8*5 + perObjectOverhead*5 of free memory
      try {
        ma.allocate((8*5+perObjectOverhead*4)+1);
        fail("expected out of memory");
      } catch (OutOfOffHeapMemoryException expected) {
      }
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals((8+perObjectOverhead)*6, ma.freeList.getFreeMemory());
      checkMcs(mcs);
      // At this point I should have 8*6 + perObjectOverhead*6 of free memory
      MemoryChunk mc24 = ma.allocate(24);
      checkMcs(mcs);
      assertEquals((8+perObjectOverhead)*6 - (24+perObjectOverhead), ma.freeList.getFreeMemory());
      // At this point I should have 8*3 + perObjectOverhead*5 of free memory
      MemoryChunk mc16 = ma.allocate(16);
      checkMcs(mcs);
      assertEquals((8+perObjectOverhead)*6 - (24+perObjectOverhead) - (16+perObjectOverhead), ma.freeList.getFreeMemory());
      // At this point I should have 8*1 + perObjectOverhead*4 of free memory
      mcs.add(ma.allocate(8));
      checkMcs(mcs);
      assertEquals((8+perObjectOverhead)*6 - (24+perObjectOverhead) - (16+perObjectOverhead) - (8+perObjectOverhead), ma.freeList.getFreeMemory());
      // At this point I should have 8*0 + perObjectOverhead*3 of free memory
      MemoryChunk mcDO = ma.allocate(perObjectOverhead*2);
      checkMcs(mcs);
      // At this point I should have 8*0 + perObjectOverhead*0 of free memory
      assertEquals(0, ma.freeList.getFreeMemory());
      try {
        ma.allocate(1);
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
      bmc = ma.allocate(BIG_ALLOC_SIZE-perObjectOverhead);
      bmc.release();
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
  
  long expectedMemoryUsage;
  boolean memoryUsageEventReceived;
  @Test
  public void testUsageEventListener() {
    final int perObjectOverhead = com.gemstone.gemfire.internal.offheap.ObjectChunk.OFF_HEAP_HEADER_SIZE;
    final int SMALL_ALLOC_SIZE = 1000;
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(3000);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
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
      MemoryChunk smc = ma.allocate(SMALL_ALLOC_SIZE-perObjectOverhead);
      assertEquals(true, this.memoryUsageEventReceived);
      
      this.expectedMemoryUsage = SMALL_ALLOC_SIZE * 2;
      this.memoryUsageEventReceived = false;
      smc = ma.allocate(SMALL_ALLOC_SIZE-perObjectOverhead);
      assertEquals(true, this.memoryUsageEventReceived);
      
      MemoryUsageListener unaddedListener = new MemoryUsageListener() {
        @Override
        public void updateMemoryUsed(final long bytesUsed) {
          throw new IllegalStateException("Should never be called");
        }
      };
      ma.removeMemoryUsageListener(unaddedListener);
      
      ma.removeMemoryUsageListener(listener);
      
      ma.removeMemoryUsageListener(unaddedListener);

      this.expectedMemoryUsage = SMALL_ALLOC_SIZE * 2;
      this.memoryUsageEventReceived = false;
      smc = ma.allocate(SMALL_ALLOC_SIZE-perObjectOverhead);
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
    final int perObjectOverhead = com.gemstone.gemfire.internal.offheap.ObjectChunk.OFF_HEAP_HEADER_SIZE;
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
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.createForUnitTest(oooml, new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      // make a big allocation
      MemoryChunk bmc = ma.allocate(BIG_ALLOC_SIZE-perObjectOverhead);
      assertNull(ooom.get());
      // drive the ma to ooom with small allocations
      try {
        MemoryChunk smc = ma.allocate(SMALL_ALLOC_SIZE-perObjectOverhead);
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
