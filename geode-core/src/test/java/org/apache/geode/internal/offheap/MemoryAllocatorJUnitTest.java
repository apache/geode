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
package org.apache.geode.internal.offheap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.OutOfOffHeapMemoryException;
import org.apache.geode.cache.CacheClosedException;

public class MemoryAllocatorJUnitTest {

  private long expectedMemoryUsage;
  private boolean memoryUsageEventReceived;

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private static int round(int multiple, int v) {
    return ((v + multiple - 1) / multiple) * multiple;
  }

  @Test
  public void testNullGetAllocator() {
    try {
      MemoryAllocatorImpl.getAllocator();
      fail("expected CacheClosedException");
    } catch (CacheClosedException expected) {
    }
  }

  @Test
  public void testConstructor() {
    try {
      MemoryAllocatorImpl.createForUnitTest(null, null, null);
      fail("expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
    }
  }

  @Test
  public void testCreate() {
    System.setProperty(MemoryAllocatorImpl.FREE_OFF_HEAP_MEMORY_PROPERTY, "false");
    {
      NullOutOfOffHeapMemoryListener listener = new NullOutOfOffHeapMemoryListener();
      NullOffHeapMemoryStats stats = new NullOffHeapMemoryStats();
      try {
        MemoryAllocatorImpl.createForUnitTest(listener, stats, 10, 950, 100, size -> {
          throw new OutOfMemoryError("expected");
        });
      } catch (OutOfMemoryError expected) {
      }
      assertTrue(listener.isClosed());
      assertTrue(stats.isClosed());
    }
    {
      NullOutOfOffHeapMemoryListener listener = new NullOutOfOffHeapMemoryListener();
      NullOffHeapMemoryStats stats = new NullOffHeapMemoryStats();
      int MAX_SLAB_SIZE = 100;
      try {
        SlabFactory factory = new SlabFactory() {
          private int createCount = 0;

          @Override
          public Slab create(int size) {
            createCount++;
            if (createCount == 1) {
              return new SlabImpl(size);
            } else {
              throw new OutOfMemoryError("expected");
            }
          }
        };
        MemoryAllocatorImpl.createForUnitTest(listener, stats, 10, 950, MAX_SLAB_SIZE, factory);
      } catch (OutOfMemoryError expected) {
      }
      assertTrue(listener.isClosed());
      assertTrue(stats.isClosed());
    }
    {
      NullOutOfOffHeapMemoryListener listener = new NullOutOfOffHeapMemoryListener();
      NullOffHeapMemoryStats stats = new NullOffHeapMemoryStats();
      SlabFactory factory = SlabImpl::new;
      MemoryAllocator ma =
          MemoryAllocatorImpl.createForUnitTest(listener, stats, 10, 950, 100, factory);
      try {
        assertFalse(listener.isClosed());
        assertFalse(stats.isClosed());
        ma.close();
        assertTrue(listener.isClosed());
        assertFalse(stats.isClosed());
        listener = new NullOutOfOffHeapMemoryListener();
        NullOffHeapMemoryStats stats2 = new NullOffHeapMemoryStats();
        {
          SlabImpl slab = new SlabImpl(1024);
          try {
            MemoryAllocatorImpl.createForUnitTest(listener, stats2, new SlabImpl[] {slab});
          } catch (IllegalStateException expected) {
            assertTrue("unexpected message: " + expected.getMessage(), expected.getMessage().equals(
                "attempted to reuse existing off-heap memory even though new off-heap memory was allocated"));
          } finally {
            slab.free();
          }
          assertFalse(stats.isClosed());
          assertTrue(listener.isClosed());
          assertTrue(stats2.isClosed());
        }
        listener = new NullOutOfOffHeapMemoryListener();
        stats2 = new NullOffHeapMemoryStats();
        MemoryAllocator ma2 =
            MemoryAllocatorImpl.createForUnitTest(listener, stats2, 10, 950, 100, factory);
        assertSame(ma, ma2);
        assertTrue(stats.isClosed());
        assertFalse(listener.isClosed());
        assertFalse(stats2.isClosed());
        stats = stats2;
        ma.close();
        assertTrue(listener.isClosed());
        assertFalse(stats.isClosed());
      } finally {
        MemoryAllocatorImpl.freeOffHeapMemory();
      }
      assertTrue(stats.isClosed());
    }
  }

  @Test
  public void testBasics() {
    int BATCH_SIZE = 1;
    int TINY_MULTIPLE = FreeListManager.TINY_MULTIPLE;
    int HUGE_MULTIPLE = FreeListManager.HUGE_MULTIPLE;
    int perObjectOverhead = OffHeapStoredObject.HEADER_SIZE;
    int maxTiny = FreeListManager.MAX_TINY - perObjectOverhead;
    int minHuge = maxTiny + 1;
    int TOTAL_MEM = (maxTiny + perObjectOverhead) * BATCH_SIZE
        /* + (maxBig+perObjectOverhead)*BATCH_SIZE */ + round(TINY_MULTIPLE,
            minHuge + 1 + perObjectOverhead) * BATCH_SIZE
        + (TINY_MULTIPLE + perObjectOverhead)
            * BATCH_SIZE /* + (MIN_BIG_SIZE+perObjectOverhead)*BATCH_SIZE */
        + round(TINY_MULTIPLE, minHuge + perObjectOverhead + 1);
    SlabImpl slab = new SlabImpl(TOTAL_MEM);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      assertEquals(TOTAL_MEM, ma.getFreeMemory());
      assertEquals(TOTAL_MEM, ma.freeList.getFreeFragmentMemory());
      assertEquals(0, ma.freeList.getFreeTinyMemory());
      assertEquals(0, ma.freeList.getFreeHugeMemory());
      StoredObject tinymc = ma.allocate(maxTiny);
      assertEquals(TOTAL_MEM - round(TINY_MULTIPLE, maxTiny + perObjectOverhead),
          ma.getFreeMemory());
      assertEquals(round(TINY_MULTIPLE, maxTiny + perObjectOverhead) * (BATCH_SIZE - 1),
          ma.freeList.getFreeTinyMemory());
      StoredObject hugemc = ma.allocate(minHuge);
      assertEquals(TOTAL_MEM
          - round(TINY_MULTIPLE,
              minHuge + perObjectOverhead)/*-round(BIG_MULTIPLE, maxBig+perObjectOverhead)*/
          - round(TINY_MULTIPLE, maxTiny + perObjectOverhead), ma.getFreeMemory());
      long freeSlab = ma.freeList.getFreeFragmentMemory();
      long oldFreeHugeMemory = ma.freeList.getFreeHugeMemory();
      assertEquals(round(TINY_MULTIPLE, minHuge + perObjectOverhead) * (BATCH_SIZE - 1),
          oldFreeHugeMemory);
      hugemc.release();
      assertEquals(round(TINY_MULTIPLE, minHuge + perObjectOverhead),
          ma.freeList.getFreeHugeMemory() - oldFreeHugeMemory);
      assertEquals(TOTAL_MEM/*-round(BIG_MULTIPLE, maxBig+perObjectOverhead)*/ - round(
          TINY_MULTIPLE, maxTiny + perObjectOverhead), ma.getFreeMemory());
      assertEquals(TOTAL_MEM - round(TINY_MULTIPLE, maxTiny + perObjectOverhead),
          ma.getFreeMemory());
      long oldFreeTinyMemory = ma.freeList.getFreeTinyMemory();
      tinymc.release();
      assertEquals(round(TINY_MULTIPLE, maxTiny + perObjectOverhead),
          ma.freeList.getFreeTinyMemory() - oldFreeTinyMemory);
      assertEquals(TOTAL_MEM, ma.getFreeMemory());
      // now lets reallocate from the free lists
      tinymc = ma.allocate(maxTiny);
      assertEquals(oldFreeTinyMemory, ma.freeList.getFreeTinyMemory());
      assertEquals(TOTAL_MEM - round(TINY_MULTIPLE, maxTiny + perObjectOverhead),
          ma.getFreeMemory());
      hugemc = ma.allocate(minHuge);
      assertEquals(oldFreeHugeMemory, ma.freeList.getFreeHugeMemory());
      assertEquals(TOTAL_MEM
          - round(TINY_MULTIPLE,
              minHuge + perObjectOverhead)/*-round(BIG_MULTIPLE, maxBig+perObjectOverhead)*/
          - round(TINY_MULTIPLE, maxTiny + perObjectOverhead), ma.getFreeMemory());
      hugemc.release();
      assertEquals(round(TINY_MULTIPLE, minHuge + perObjectOverhead),
          ma.freeList.getFreeHugeMemory() - oldFreeHugeMemory);
      assertEquals(TOTAL_MEM/*-round(BIG_MULTIPLE, maxBig+perObjectOverhead)*/ - round(
          TINY_MULTIPLE, maxTiny + perObjectOverhead), ma.getFreeMemory());
      assertEquals(TOTAL_MEM - round(TINY_MULTIPLE, maxTiny + perObjectOverhead),
          ma.getFreeMemory());
      tinymc.release();
      assertEquals(round(TINY_MULTIPLE, maxTiny + perObjectOverhead),
          ma.freeList.getFreeTinyMemory() - oldFreeTinyMemory);
      assertEquals(TOTAL_MEM, ma.getFreeMemory());
      // None of the reallocates should have come from the slab.
      assertEquals(freeSlab, ma.freeList.getFreeFragmentMemory());
      tinymc = ma.allocate(1);
      assertEquals(round(TINY_MULTIPLE, 1 + perObjectOverhead), tinymc.getSize());
      assertEquals(freeSlab - (round(TINY_MULTIPLE, 1 + perObjectOverhead) * BATCH_SIZE),
          ma.freeList.getFreeFragmentMemory());
      freeSlab = ma.freeList.getFreeFragmentMemory();
      tinymc.release();
      assertEquals(
          round(TINY_MULTIPLE, maxTiny + perObjectOverhead)
              + (round(TINY_MULTIPLE, 1 + perObjectOverhead) * BATCH_SIZE),
          ma.freeList.getFreeTinyMemory() - oldFreeTinyMemory);

      hugemc = ma.allocate(minHuge + 1);
      assertEquals(round(TINY_MULTIPLE, minHuge + 1 + perObjectOverhead), hugemc.getSize());
      assertEquals(round(TINY_MULTIPLE, minHuge + perObjectOverhead) * (BATCH_SIZE - 1),
          ma.freeList.getFreeHugeMemory());
      hugemc.release();
      assertEquals(round(TINY_MULTIPLE, minHuge + perObjectOverhead) * BATCH_SIZE,
          ma.freeList.getFreeHugeMemory());
      hugemc = ma.allocate(minHuge);
      assertEquals(round(TINY_MULTIPLE, minHuge + perObjectOverhead) * (BATCH_SIZE - 1),
          ma.freeList.getFreeHugeMemory());
      if (BATCH_SIZE > 1) {
        StoredObject hugemc2 = ma.allocate(minHuge);
        assertEquals(round(TINY_MULTIPLE, minHuge + perObjectOverhead) * (BATCH_SIZE - 2),
            ma.freeList.getFreeHugeMemory());
        hugemc2.release();
        assertEquals(round(TINY_MULTIPLE, minHuge + perObjectOverhead) * (BATCH_SIZE - 1),
            ma.freeList.getFreeHugeMemory());
      }
      hugemc.release();
      assertEquals(round(TINY_MULTIPLE, minHuge + perObjectOverhead) * BATCH_SIZE,
          ma.freeList.getFreeHugeMemory());
      // now that we do defragmentation the following allocate works.
      hugemc = ma.allocate(minHuge + HUGE_MULTIPLE + HUGE_MULTIPLE - 1);
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void testChunkCreateDirectByteBuffer() {
    SlabImpl slab = new SlabImpl(1024 * 1024);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      ByteBuffer bb = ByteBuffer.allocate(1024);
      for (int i = 0; i < 1024; i++) {
        bb.put((byte) i);
      }
      bb.position(0);
      OffHeapStoredObject c =
          (OffHeapStoredObject) ma.allocateAndInitialize(bb.array(), false, false);
      assertEquals(1024, c.getDataSize());
      if (!Arrays.equals(bb.array(), c.getRawBytes())) {
        fail("arrays are not equal. Expected " + Arrays.toString(bb.array()) + " but found: "
            + Arrays.toString(c.getRawBytes()));
      }
      ByteBuffer dbb = c.createDirectByteBuffer();
      assertEquals(true, dbb.isDirect());
      assertEquals(bb, dbb);
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void testDebugLog() {
    MemoryAllocatorImpl.debugLog("test debug log", false);
    MemoryAllocatorImpl.debugLog("test debug log", true);
  }

  @Test
  public void testGetLostChunks() {
    SlabImpl slab = new SlabImpl(1024 * 1024);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      assertEquals(Collections.emptyList(), ma.getLostChunks(null));
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void testFindSlab() {
    final int SLAB_SIZE = 1024 * 1024;
    SlabImpl slab = new SlabImpl(SLAB_SIZE);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      assertEquals(0, ma.findSlab(slab.getMemoryAddress()));
      assertEquals(0, ma.findSlab(slab.getMemoryAddress() + SLAB_SIZE - 1));
      try {
        ma.findSlab(slab.getMemoryAddress() - 1);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
      }
      try {
        ma.findSlab(slab.getMemoryAddress() + SLAB_SIZE);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
      }
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void testValidateAddressAndSize() {
    final int SLAB_SIZE = 1024 * 1024;
    SlabImpl slab = new SlabImpl(SLAB_SIZE);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      try {
        MemoryAllocatorImpl.validateAddress(0L);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
        assertEquals("Unexpected exception message: " + expected.getMessage(), true,
            expected.getMessage().contains("addr was smaller than expected"));
      }
      try {
        MemoryAllocatorImpl.validateAddress(1L);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
        assertEquals("Unexpected exception message: " + expected.getMessage(), true, expected
            .getMessage().contains("Valid addresses must be in one of the following ranges:"));
      }
      MemoryAllocatorImpl.validateAddressAndSizeWithinSlab(slab.getMemoryAddress(), SLAB_SIZE,
          false);
      MemoryAllocatorImpl.validateAddressAndSizeWithinSlab(slab.getMemoryAddress(), SLAB_SIZE,
          true);
      MemoryAllocatorImpl.validateAddressAndSizeWithinSlab(slab.getMemoryAddress(), -1, true);
      try {
        MemoryAllocatorImpl.validateAddressAndSizeWithinSlab(slab.getMemoryAddress() - 1, SLAB_SIZE,
            true);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
        assertEquals("Unexpected exception message: " + expected.getMessage(), true,
            expected.getMessage()
                .equals(" address 0x" + Long.toString(slab.getMemoryAddress() - 1, 16)
                    + " does not address the original slab memory"));
      }
      try {
        MemoryAllocatorImpl.validateAddressAndSizeWithinSlab(slab.getMemoryAddress(), SLAB_SIZE + 1,
            true);
        fail("expected IllegalStateException");
      } catch (IllegalStateException expected) {
        assertEquals("Unexpected exception message: " + expected.getMessage(), true,
            expected.getMessage()
                .equals(" address 0x" + Long.toString(slab.getMemoryAddress() + SLAB_SIZE, 16)
                    + " does not address the original slab memory"));
      }
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void testMemoryInspection() {
    final int SLAB_SIZE = 1024 * 1024;
    SlabImpl slab = new SlabImpl(SLAB_SIZE);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
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
        assertEquals(1024 * 1024, firstBlock.getBlockSize());
        assertEquals("N/A", firstBlock.getDataType());
        assertEquals(-1, firstBlock.getFreeListId());
        assertTrue(firstBlock.getAddress() > 0);
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
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void testClose() {
    System.setProperty(MemoryAllocatorImpl.FREE_OFF_HEAP_MEMORY_PROPERTY, "false");
    SlabImpl slab = new SlabImpl(1024 * 1024);
    boolean freeSlab = true;
    SlabImpl[] slabs = new SlabImpl[] {slab};
    try {
      MemoryAllocatorImpl ma = MemoryAllocatorImpl.createForUnitTest(
          new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), slabs);
      ma.close();
      ma.close();
      System.setProperty(MemoryAllocatorImpl.FREE_OFF_HEAP_MEMORY_PROPERTY, "true");
      try {
        ma = MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
            new NullOffHeapMemoryStats(), slabs);
        ma.close();
        freeSlab = false;
        ma.close();
      } finally {
        System.clearProperty(MemoryAllocatorImpl.FREE_OFF_HEAP_MEMORY_PROPERTY);
      }
    } finally {
      if (freeSlab) {
        MemoryAllocatorImpl.freeOffHeapMemory();
      }
    }

  }

  @Test
  public void testDefragmentation() {
    final int perObjectOverhead = OffHeapStoredObject.HEADER_SIZE;
    final int BIG_ALLOC_SIZE = 150000;
    final int SMALL_ALLOC_SIZE = BIG_ALLOC_SIZE / 2;
    final int TOTAL_MEM = BIG_ALLOC_SIZE;
    SlabImpl slab = new SlabImpl(TOTAL_MEM);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      StoredObject bmc = ma.allocate(BIG_ALLOC_SIZE - perObjectOverhead);
      try {
        StoredObject smc = ma.allocate(SMALL_ALLOC_SIZE - perObjectOverhead);
        fail("Expected out of memory");
      } catch (OutOfOffHeapMemoryException expected) {
      }
      bmc.release();
      assertEquals(TOTAL_MEM, ma.freeList.getFreeMemory());
      StoredObject smc1 = ma.allocate(SMALL_ALLOC_SIZE - perObjectOverhead);
      StoredObject smc2 = ma.allocate(SMALL_ALLOC_SIZE - perObjectOverhead);
      smc2.release();
      assertEquals(TOTAL_MEM - SMALL_ALLOC_SIZE, ma.freeList.getFreeMemory());
      try {
        bmc = ma.allocate(BIG_ALLOC_SIZE - perObjectOverhead);
        fail("Expected out of memory");
      } catch (OutOfOffHeapMemoryException expected) {
      }
      smc1.release();
      assertEquals(TOTAL_MEM, ma.freeList.getFreeMemory());
      bmc = ma.allocate(BIG_ALLOC_SIZE - perObjectOverhead);
      bmc.release();
      assertEquals(TOTAL_MEM, ma.freeList.getFreeMemory());
      ArrayList<StoredObject> mcs = new ArrayList<>();
      for (int i = 0; i < BIG_ALLOC_SIZE / (8 + perObjectOverhead); i++) {
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
      assertEquals(8 + perObjectOverhead, ma.freeList.getFreeMemory());
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals((8 + perObjectOverhead) * 2, ma.freeList.getFreeMemory());
      ma.allocate(16).release(); // allocates and frees 16+perObjectOverhead; still have
                                 // perObjectOverhead
      assertEquals((8 + perObjectOverhead) * 2, ma.freeList.getFreeMemory());
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals((8 + perObjectOverhead) * 3, ma.freeList.getFreeMemory());
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals((8 + perObjectOverhead) * 4, ma.freeList.getFreeMemory());
      // At this point I should have 8*4 + perObjectOverhead*4 of free memory
      ma.allocate(8 * 4 + perObjectOverhead * 3).release();
      assertEquals((8 + perObjectOverhead) * 4, ma.freeList.getFreeMemory());
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals((8 + perObjectOverhead) * 5, ma.freeList.getFreeMemory());
      // At this point I should have 8*5 + perObjectOverhead*5 of free memory
      try {
        ma.allocate((8 * 5 + perObjectOverhead * 4) + 1);
        fail("expected out of memory");
      } catch (OutOfOffHeapMemoryException expected) {
      }
      mcs.remove(0).release(); // frees 8+perObjectOverhead
      assertEquals((8 + perObjectOverhead) * 6, ma.freeList.getFreeMemory());
      checkMcs(mcs);
      // At this point I should have 8*6 + perObjectOverhead*6 of free memory
      StoredObject mc24 = ma.allocate(24);
      checkMcs(mcs);
      assertEquals((8 + perObjectOverhead) * 6 - (24 + perObjectOverhead),
          ma.freeList.getFreeMemory());
      // At this point I should have 8*3 + perObjectOverhead*5 of free memory
      StoredObject mc16 = ma.allocate(16);
      checkMcs(mcs);
      assertEquals(
          (8 + perObjectOverhead) * 6 - (24 + perObjectOverhead) - (16 + perObjectOverhead),
          ma.freeList.getFreeMemory());
      // At this point I should have 8*1 + perObjectOverhead*4 of free memory
      mcs.add(ma.allocate(8));
      checkMcs(mcs);
      assertEquals((8 + perObjectOverhead) * 6 - (24 + perObjectOverhead) - (16 + perObjectOverhead)
          - (8 + perObjectOverhead), ma.freeList.getFreeMemory());
      // At this point I should have 8*0 + perObjectOverhead*3 of free memory
      StoredObject mcDO = ma.allocate(perObjectOverhead * 2);
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
      assertEquals((perObjectOverhead * 3), ma.freeList.getFreeMemory());
      mcs.remove(mcs.size() - 1).release();
      assertEquals((perObjectOverhead * 3) + (8 + perObjectOverhead), ma.freeList.getFreeMemory());
      mc16.release();
      assertEquals((perObjectOverhead * 3) + (8 + perObjectOverhead) + (16 + perObjectOverhead),
          ma.freeList.getFreeMemory());
      mc24.release();
      assertEquals((perObjectOverhead * 3) + (8 + perObjectOverhead) + (16 + perObjectOverhead)
          + (24 + perObjectOverhead), ma.freeList.getFreeMemory());

      long freeMem = ma.freeList.getFreeMemory();
      for (StoredObject mc : mcs) {
        mc.release();
        assertEquals(freeMem + (8 + perObjectOverhead), ma.freeList.getFreeMemory());
        freeMem += (8 + perObjectOverhead);
      }
      mcs.clear();
      assertEquals(TOTAL_MEM, ma.freeList.getFreeMemory());
      bmc = ma.allocate(BIG_ALLOC_SIZE - perObjectOverhead);
      bmc.release();
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void testUsageEventListener() {
    final int perObjectOverhead = OffHeapStoredObject.HEADER_SIZE;
    final int SMALL_ALLOC_SIZE = 1000;
    SlabImpl slab = new SlabImpl(3000);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      MemoryUsageListener listener = bytesUsed -> {
        memoryUsageEventReceived = true;
        assertEquals(expectedMemoryUsage, bytesUsed);
      };
      ma.addMemoryUsageListener(listener);

      expectedMemoryUsage = SMALL_ALLOC_SIZE;
      memoryUsageEventReceived = false;
      StoredObject smc = ma.allocate(SMALL_ALLOC_SIZE - perObjectOverhead);
      assertEquals(true, memoryUsageEventReceived);

      expectedMemoryUsage = SMALL_ALLOC_SIZE * 2;
      memoryUsageEventReceived = false;
      smc = ma.allocate(SMALL_ALLOC_SIZE - perObjectOverhead);
      assertEquals(true, memoryUsageEventReceived);

      MemoryUsageListener unaddedListener = bytesUsed -> {
        throw new IllegalStateException("Should never be called");
      };
      ma.removeMemoryUsageListener(unaddedListener);

      ma.removeMemoryUsageListener(listener);

      ma.removeMemoryUsageListener(unaddedListener);

      expectedMemoryUsage = SMALL_ALLOC_SIZE * 2;
      memoryUsageEventReceived = false;
      smc = ma.allocate(SMALL_ALLOC_SIZE - perObjectOverhead);
      assertEquals(false, memoryUsageEventReceived);

    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  private void checkMcs(ArrayList<StoredObject> mcs) {
    for (StoredObject mc : mcs) {
      assertEquals(8 + 8, mc.getSize());
    }
  }

  @Test
  public void testOutOfOffHeapMemory() {
    final int perObjectOverhead = OffHeapStoredObject.HEADER_SIZE;
    final int BIG_ALLOC_SIZE = 150000;
    final int SMALL_ALLOC_SIZE = BIG_ALLOC_SIZE / 2;
    final int TOTAL_MEM = BIG_ALLOC_SIZE;
    final SlabImpl slab = new SlabImpl(TOTAL_MEM);
    final AtomicReference<OutOfOffHeapMemoryException> ooom =
        new AtomicReference<>();
    final OutOfOffHeapMemoryListener oooml = new OutOfOffHeapMemoryListener() {
      @Override
      public void outOfOffHeapMemory(OutOfOffHeapMemoryException cause) {
        ooom.set(cause);
      }

      @Override
      public void close() {}
    };
    try {
      MemoryAllocatorImpl ma = MemoryAllocatorImpl.createForUnitTest(oooml,
          new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      // make a big allocation
      StoredObject bmc = ma.allocate(BIG_ALLOC_SIZE - perObjectOverhead);
      assertNull(ooom.get());
      // drive the ma to ooom with small allocations
      try {
        StoredObject smc = ma.allocate(SMALL_ALLOC_SIZE - perObjectOverhead);
        fail("Expected out of memory");
      } catch (OutOfOffHeapMemoryException expected) {
      }
      assertNotNull(ooom.get());
      assertTrue(
          ooom.get().getMessage().contains("Out of off-heap memory. Could not allocate size of "));
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
}
