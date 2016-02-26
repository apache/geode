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
import static org.mockito.Mockito.*;
import static com.googlecode.catchexception.CatchException.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.OutOfOffHeapMemoryException;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class FreeListManagerTest {
  static {
    ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
  }

  private final int DEFAULT_SLAB_SIZE = 1024*1024*5;
  private final SimpleMemoryAllocatorImpl ma = mock(SimpleMemoryAllocatorImpl.class);
  private final OffHeapMemoryStats stats = mock(OffHeapMemoryStats.class);
  private TestableFreeListManager freeListManager;
  

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    when(ma.getStats()).thenReturn(stats);
  }

  @After
  public void tearDown() throws Exception {
    if (this.freeListManager != null) {
      this.freeListManager.freeSlabs();
    }
  }
  
  private static TestableFreeListManager createFreeListManager(SimpleMemoryAllocatorImpl ma, AddressableMemoryChunk[] slabs) {
    return new TestableFreeListManager(ma, slabs);
  }
  
  private void setUpSingleSlabManager() {
    setUpSingleSlabManager(DEFAULT_SLAB_SIZE);
  }
  private void setUpSingleSlabManager(int slabSize) {
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(slabSize);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {slab});
  }

  @Test
  public void usedMemoryIsZeroOnDefault() {
    setUpSingleSlabManager();
    assertThat(this.freeListManager.getUsedMemory()).isZero();
  }

  @Test
  public void freeMemoryIsSlabSizeOnDefault() {
    setUpSingleSlabManager();
    assertThat(this.freeListManager.getFreeMemory()).isEqualTo(DEFAULT_SLAB_SIZE);
  }
  
  @Test
  public void totalMemoryIsSlabSizeOnDefault() {
    setUpSingleSlabManager();
    assertThat(this.freeListManager.getTotalMemory()).isEqualTo(DEFAULT_SLAB_SIZE);
  }
  
  @Test
  public void allocateTinyChunkHasCorrectSize() {
    setUpSingleSlabManager();
    int tinySize = 10;

    ObjectChunk c = this.freeListManager.allocate(tinySize);
    
    validateChunkSizes(c, tinySize);
  }
  
  private void validateChunkSizes(ObjectChunk c, int dataSize) {
    assertThat(c).isNotNull();
    assertThat(c.getDataSize()).isEqualTo(dataSize);
    assertThat(c.getSize()).isEqualTo(computeExpectedSize(dataSize));
  }

  @Test
  public void allocateTinyChunkFromFreeListHasCorrectSize() {
    setUpSingleSlabManager();
    int tinySize = 10;
    
    ObjectChunk c = this.freeListManager.allocate(tinySize);
    ObjectChunk.release(c.getMemoryAddress(), this.freeListManager);
    c = this.freeListManager.allocate(tinySize);

    validateChunkSizes(c, tinySize);
  }
  
  @Test
  public void allocateTinyChunkFromEmptyFreeListHasCorrectSize() {
    setUpSingleSlabManager();
    int dataSize = 10;
    
    ObjectChunk c = this.freeListManager.allocate(dataSize);
    ObjectChunk.release(c.getMemoryAddress(), this.freeListManager);
    this.freeListManager.allocate(dataSize);
    // free list will now be empty
    c = this.freeListManager.allocate(dataSize);

    validateChunkSizes(c, dataSize);
  }

  @Test
  public void allocateHugeChunkHasCorrectSize() {
    setUpSingleSlabManager();
    int hugeSize = FreeListManager.MAX_TINY+1;

    ObjectChunk c = this.freeListManager.allocate(hugeSize);

    validateChunkSizes(c, hugeSize);
  }
  
  @Test
  public void allocateHugeChunkFromEmptyFreeListHasCorrectSize() {
    setUpSingleSlabManager();
    int dataSize = FreeListManager.MAX_TINY+1;
    
    ObjectChunk c = this.freeListManager.allocate(dataSize);
    ObjectChunk.release(c.getMemoryAddress(), this.freeListManager);
    this.freeListManager.allocate(dataSize);
    // free list will now be empty
    c = this.freeListManager.allocate(dataSize);
    
    validateChunkSizes(c, dataSize);
  }

  @Test
  public void allocateHugeChunkFromFragmentWithItemInFreeListHasCorrectSize() {
    setUpSingleSlabManager();
    int dataSize = FreeListManager.MAX_TINY+1+1024;
    
    ObjectChunk c = this.freeListManager.allocate(dataSize);
    ObjectChunk.release(c.getMemoryAddress(), this.freeListManager);
    dataSize = FreeListManager.MAX_TINY+1;
    c = this.freeListManager.allocate(dataSize);
    
    validateChunkSizes(c, dataSize);
  }
  @Test
  public void freeTinyMemoryDefault() {
    setUpSingleSlabManager();
    
    assertThat(this.freeListManager.getFreeTinyMemory()).isZero();
  }
  @Test
  public void freeTinyMemoryEqualToChunkSize() {
    setUpSingleSlabManager();
    int dataSize = 10;
    
    ObjectChunk c = this.freeListManager.allocate(dataSize);
    ObjectChunk.release(c.getMemoryAddress(), this.freeListManager);
    
    assertThat(this.freeListManager.getFreeTinyMemory()).isEqualTo(computeExpectedSize(dataSize));
  }
   
  @Test
  public void freeTinyMemoryWithTwoTinyFreeListsEqualToChunkSize() {
    setUpSingleSlabManager();
    int dataSize = 10;
    
    ObjectChunk c = this.freeListManager.allocate(dataSize);
    ObjectChunk.release(c.getMemoryAddress(), this.freeListManager);
    
    int dataSize2 = 100;
    
    ObjectChunk c2 = this.freeListManager.allocate(dataSize2);
    ObjectChunk.release(c2.getMemoryAddress(), this.freeListManager);
    
    assertThat(this.freeListManager.getFreeTinyMemory()).isEqualTo(computeExpectedSize(dataSize)+computeExpectedSize(dataSize2));
  }
   
  @Test
  public void freeHugeMemoryDefault() {
    setUpSingleSlabManager();
    
    assertThat(this.freeListManager.getFreeHugeMemory()).isZero();
  }
  @Test
  public void freeHugeMemoryEqualToChunkSize() {
    setUpSingleSlabManager();
    int dataSize = FreeListManager.MAX_TINY+1+1024;
    
    ObjectChunk c = this.freeListManager.allocate(dataSize);
    ObjectChunk.release(c.getMemoryAddress(), this.freeListManager);
    
    assertThat(this.freeListManager.getFreeHugeMemory()).isEqualTo(computeExpectedSize(dataSize));
  }
  
  @Test
  public void freeFragmentMemoryDefault() {
    setUpSingleSlabManager();
    
    assertThat(this.freeListManager.getFreeFragmentMemory()).isEqualTo(DEFAULT_SLAB_SIZE);
  }
  
  @Test
  public void freeFragmentMemorySomeOfFragmentAllocated() {
    setUpSingleSlabManager();
    ObjectChunk c = this.freeListManager.allocate(DEFAULT_SLAB_SIZE/4-8);
    
    assertThat(this.freeListManager.getFreeFragmentMemory()).isEqualTo((DEFAULT_SLAB_SIZE/4)*3);
  }
  
  @Test
  public void freeFragmentMemoryMostOfFragmentAllocated() {
    setUpSingleSlabManager();
    ObjectChunk c = this.freeListManager.allocate(DEFAULT_SLAB_SIZE-8-8);
    
    assertThat(this.freeListManager.getFreeFragmentMemory()).isZero();
  }
  
  private int computeExpectedSize(int dataSize) {
    return ((dataSize + ObjectChunk.OFF_HEAP_HEADER_SIZE + 7) / 8) * 8;
  }

  @Test(expected = AssertionError.class)
  public void allocateZeroThrowsAssertion() {
    setUpSingleSlabManager();
    this.freeListManager.allocate(0);
  }
  
  @Test
  public void allocateFromMultipleSlabs() {
    int SMALL_SLAB = 16;
    int MEDIUM_SLAB = 128;
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(DEFAULT_SLAB_SIZE);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {
        new UnsafeMemoryChunk(SMALL_SLAB), 
        new UnsafeMemoryChunk(SMALL_SLAB), 
        new UnsafeMemoryChunk(MEDIUM_SLAB), 
        slab});
    this.freeListManager.allocate(SMALL_SLAB-8+1);
    this.freeListManager.allocate(DEFAULT_SLAB_SIZE-8);
    this.freeListManager.allocate(SMALL_SLAB-8+1);
    assertThat(this.freeListManager.getFreeMemory()).isEqualTo(SMALL_SLAB*2+MEDIUM_SLAB-((SMALL_SLAB+8)*2));
    assertThat(this.freeListManager.getUsedMemory()).isEqualTo(DEFAULT_SLAB_SIZE+(SMALL_SLAB+8)*2);
    assertThat(this.freeListManager.getTotalMemory()).isEqualTo(DEFAULT_SLAB_SIZE+MEDIUM_SLAB+SMALL_SLAB*2);
  }
  
  @Test
  public void compactWithLargeChunkSizeReturnsFalse() {
    int SMALL_SLAB = 16;
    int MEDIUM_SLAB = 128;
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(DEFAULT_SLAB_SIZE);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {
        new UnsafeMemoryChunk(SMALL_SLAB), 
        new UnsafeMemoryChunk(SMALL_SLAB), 
        new UnsafeMemoryChunk(MEDIUM_SLAB), 
        slab});
    ArrayList<ObjectChunk> chunks = new ArrayList<>();
    chunks.add(this.freeListManager.allocate(SMALL_SLAB-8+1));
    chunks.add(this.freeListManager.allocate(DEFAULT_SLAB_SIZE/2-8));
    chunks.add(this.freeListManager.allocate(DEFAULT_SLAB_SIZE/2-8));
    chunks.add(this.freeListManager.allocate(SMALL_SLAB-8+1));
    for (ObjectChunk c: chunks) {
      ObjectChunk.release(c.getMemoryAddress(), this.freeListManager);
    }
    this.freeListManager.firstCompact = false;
    assertThat(this.freeListManager.compact(DEFAULT_SLAB_SIZE+1)).isFalse();
  }
  
  @Test
  public void compactWithChunkSizeOfMaxSlabReturnsTrue() {
    int SMALL_SLAB = 16;
    int MEDIUM_SLAB = 128;
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(DEFAULT_SLAB_SIZE);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {
        new UnsafeMemoryChunk(SMALL_SLAB), 
        new UnsafeMemoryChunk(SMALL_SLAB), 
        new UnsafeMemoryChunk(MEDIUM_SLAB), 
        slab});
    ArrayList<ObjectChunk> chunks = new ArrayList<>();
    chunks.add(this.freeListManager.allocate(SMALL_SLAB-8+1));
    chunks.add(this.freeListManager.allocate(DEFAULT_SLAB_SIZE/2-8));
    chunks.add(this.freeListManager.allocate(DEFAULT_SLAB_SIZE/2-8));
    chunks.add(this.freeListManager.allocate(SMALL_SLAB-8+1));
    for (ObjectChunk c: chunks) {
      ObjectChunk.release(c.getMemoryAddress(), this.freeListManager);
    }
    
    assertThat(this.freeListManager.compact(DEFAULT_SLAB_SIZE)).isTrue();
    //assertThat(this.freeListManager.getFragmentList()).hasSize(4); // TODO intermittently fails because Fragments may be merged
  }
  
  @Test
  public void compactWithLiveChunks() {
    int SMALL_SLAB = 16;
    int MEDIUM_SLAB = 128;
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(DEFAULT_SLAB_SIZE);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {
        new UnsafeMemoryChunk(SMALL_SLAB), 
        new UnsafeMemoryChunk(SMALL_SLAB), 
        new UnsafeMemoryChunk(MEDIUM_SLAB), 
        slab});
    ArrayList<ObjectChunk> chunks = new ArrayList<>();
    chunks.add(this.freeListManager.allocate(SMALL_SLAB-8+1));
    this.freeListManager.allocate(DEFAULT_SLAB_SIZE/2-8);
    chunks.add(this.freeListManager.allocate(DEFAULT_SLAB_SIZE/2-8));
    this.freeListManager.allocate(SMALL_SLAB-8+1);
    for (ObjectChunk c: chunks) {
      ObjectChunk.release(c.getMemoryAddress(), this.freeListManager);
    }
    
    assertThat(this.freeListManager.compact(DEFAULT_SLAB_SIZE/2)).isTrue();
  }
  
  @Test
  public void compactAfterAllocatingAll() {
    setUpSingleSlabManager();
    ObjectChunk c = freeListManager.allocate(DEFAULT_SLAB_SIZE-8);
    this.freeListManager.firstCompact = false;
    assertThat(this.freeListManager.compact(1)).isFalse();
    // call compact twice for extra code coverage
    assertThat(this.freeListManager.compact(1)).isFalse();
    assertThat(this.freeListManager.getFragmentList()).isEmpty();
  }
  
  @Test
  public void afterAllocatingAllOneSizeCompactToAllocateDifferentSize() {
    setUpSingleSlabManager();
    ArrayList<ObjectChunk> chunksToFree = new ArrayList<>();
    ArrayList<ObjectChunk> chunksToFreeLater = new ArrayList<>();
    int ALLOCATE_COUNT = 1000;
    ObjectChunk bigChunk = freeListManager.allocate(DEFAULT_SLAB_SIZE-8-(ALLOCATE_COUNT*32)-256-256);
    for (int i=0; i < ALLOCATE_COUNT; i++) {
      ObjectChunk c = freeListManager.allocate(24);
      if (i%3 != 2) {
        chunksToFree.add(c);
      } else {
        chunksToFreeLater.add(c);
      }
    }
    ObjectChunk c1 = freeListManager.allocate(64-8);
    ObjectChunk c2 = freeListManager.allocate(64-8);
    ObjectChunk c3 = freeListManager.allocate(64-8);
    ObjectChunk c4 = freeListManager.allocate(64-8);

    ObjectChunk mediumChunk1 = freeListManager.allocate(128-8);
    ObjectChunk mediumChunk2 = freeListManager.allocate(128-8);

    ObjectChunk.release(bigChunk.getMemoryAddress(), freeListManager);
    int s = chunksToFree.size();
    for (int i=s/2; i >=0; i--) {
      ObjectChunk c = chunksToFree.get(i);
      ObjectChunk.release(c.getMemoryAddress(), freeListManager);
    }
    for (int i=(s/2)+1; i < s; i++) {
      ObjectChunk c = chunksToFree.get(i);
      ObjectChunk.release(c.getMemoryAddress(), freeListManager);
    }
    ObjectChunk.release(c3.getMemoryAddress(), freeListManager);
    ObjectChunk.release(c1.getMemoryAddress(), freeListManager);
    ObjectChunk.release(c2.getMemoryAddress(), freeListManager);
    ObjectChunk.release(c4.getMemoryAddress(), freeListManager);
    ObjectChunk.release(mediumChunk1.getMemoryAddress(), freeListManager);
    ObjectChunk.release(mediumChunk2.getMemoryAddress(), freeListManager);
    this.freeListManager.firstCompact = false;
    assertThat(freeListManager.compact(DEFAULT_SLAB_SIZE-(ALLOCATE_COUNT*32))).isFalse();
    for (int i=0; i < ((256*2)/96); i++) {
      ObjectChunk.release(chunksToFreeLater.get(i).getMemoryAddress(), freeListManager);
    }
    assertThat(freeListManager.compact(DEFAULT_SLAB_SIZE-(ALLOCATE_COUNT*32))).isTrue();
  }
  
  @Test
  public void afterAllocatingAndFreeingCompact() {
    int slabSize = 1024*3;
    setUpSingleSlabManager(slabSize);
    ObjectChunk bigChunk1 = freeListManager.allocate(slabSize/3-8);
    ObjectChunk bigChunk2 = freeListManager.allocate(slabSize/3-8);
    ObjectChunk bigChunk3 = freeListManager.allocate(slabSize/3-8);
    this.freeListManager.firstCompact = false;
    assertThat(freeListManager.compact(1)).isFalse();
    ObjectChunk.release(bigChunk3.getMemoryAddress(), freeListManager);
    ObjectChunk.release(bigChunk2.getMemoryAddress(), freeListManager);
    ObjectChunk.release(bigChunk1.getMemoryAddress(), freeListManager);
    assertThat(freeListManager.compact(slabSize)).isTrue();
  }
  
  @Test
  public void compactWithEmptyTinyFreeList() {
    setUpSingleSlabManager();
    Fragment originalFragment = this.freeListManager.getFragmentList().get(0);
    ObjectChunk c = freeListManager.allocate(16);
    ObjectChunk.release(c.getMemoryAddress(), this.freeListManager);
    c = freeListManager.allocate(16);
    this.freeListManager.firstCompact = false;
    assertThat(this.freeListManager.compact(1)).isTrue();
    assertThat(this.freeListManager.getFragmentList()).hasSize(1);
    Fragment compactedFragment = this.freeListManager.getFragmentList().get(0);
    assertThat(compactedFragment.getSize()).isEqualTo(originalFragment.getSize()-(16+8));
    assertThat(compactedFragment.getMemoryAddress()).isEqualTo(originalFragment.getMemoryAddress()+(16+8));
  }
  
  @Test
  public void allocationsThatLeaveLessThanMinChunkSizeFreeInAFragment() {
    int SMALL_SLAB = 16;
    int MEDIUM_SLAB = 128;
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(DEFAULT_SLAB_SIZE);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {
        new UnsafeMemoryChunk(SMALL_SLAB), 
        new UnsafeMemoryChunk(SMALL_SLAB), 
        new UnsafeMemoryChunk(MEDIUM_SLAB), 
        slab});
    this.freeListManager.allocate(DEFAULT_SLAB_SIZE-8-(ObjectChunk.MIN_CHUNK_SIZE-1));
    this.freeListManager.allocate(MEDIUM_SLAB-8-(ObjectChunk.MIN_CHUNK_SIZE-1));
    
    assertThat(this.freeListManager.compact(SMALL_SLAB)).isTrue();
  }
 @Test
  public void maxAllocationUsesAllMemory() {
    setUpSingleSlabManager();

    this.freeListManager.allocate(DEFAULT_SLAB_SIZE-8);

    assertThat(this.freeListManager.getFreeMemory()).isZero();
    assertThat(this.freeListManager.getUsedMemory()).isEqualTo(DEFAULT_SLAB_SIZE);
  }


  @Test
  public void overMaxAllocationFails() {
    setUpSingleSlabManager();
    OutOfOffHeapMemoryListener ooohml = mock(OutOfOffHeapMemoryListener.class);
    when(this.ma.getOutOfOffHeapMemoryListener()).thenReturn(ooohml);

    catchException(this.freeListManager).allocate(DEFAULT_SLAB_SIZE-7);

    verify(ooohml).outOfOffHeapMemory(caughtException());
  }
  
  @Test(expected = AssertionError.class)
  public void allocateNegativeThrowsAssertion() {
    setUpSingleSlabManager();
    this.freeListManager.allocate(-123);
  }
  
  @Test
  public void hugeMultipleLessThanZeroIsIllegal() {
    try {
      FreeListManager.verifyHugeMultiple(-1);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      assertThat(expected.getMessage()).contains("HUGE_MULTIPLE must be >= 0 and <= " + FreeListManager.HUGE_MULTIPLE + " but it was -1");
    }
  }
  @Test
  public void hugeMultipleGreaterThan256IsIllegal() {
    try {
      FreeListManager.verifyHugeMultiple(257);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      assertThat(expected.getMessage()).contains("HUGE_MULTIPLE must be >= 0 and <= 256 but it was 257");
    }
  }
  @Test
  public void hugeMultipleof256IsLegal() {
    FreeListManager.verifyHugeMultiple(256);
  }
  
  @Test
  public void offHeapFreeListCountLessThanZeroIsIllegal() {
    try {
      FreeListManager.verifyOffHeapFreeListCount(-1);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      assertThat(expected.getMessage()).contains("gemfire.OFF_HEAP_FREE_LIST_COUNT must be >= 1.");
    }
  }
  @Test
  public void offHeapFreeListCountOfZeroIsIllegal() {
    try {
      FreeListManager.verifyOffHeapFreeListCount(0);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      assertThat(expected.getMessage()).contains("gemfire.OFF_HEAP_FREE_LIST_COUNT must be >= 1.");
    }
  }
  @Test
  public void offHeapFreeListCountOfOneIsLegal() {
    FreeListManager.verifyOffHeapFreeListCount(1);
  }
  @Test
  public void offHeapAlignmentLessThanZeroIsIllegal() {
    try {
      FreeListManager.verifyOffHeapAlignment(-1);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      assertThat(expected.getMessage()).contains("gemfire.OFF_HEAP_ALIGNMENT must be a multiple of 8");
    }
  }
  @Test
  public void offHeapAlignmentNotAMultipleOf8IsIllegal() {
    try {
      FreeListManager.verifyOffHeapAlignment(9);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      assertThat(expected.getMessage()).contains("gemfire.OFF_HEAP_ALIGNMENT must be a multiple of 8");
    }
  }
  @Test
  public void offHeapAlignmentGreaterThan256IsIllegal() {
    try {
      FreeListManager.verifyOffHeapAlignment(256+8);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      assertThat(expected.getMessage()).contains("gemfire.OFF_HEAP_ALIGNMENT must be <= 256");
    }
  }
  @Test
  public void offHeapAlignmentOf256IsLegal() {
    FreeListManager.verifyOffHeapAlignment(256);
  }
  
  @Test
  public void okToReuseNull() {
    setUpSingleSlabManager();
    assertThat(this.freeListManager.okToReuse(null)).isTrue();
  }
  
  @Test
  public void okToReuseSameSlabs() {
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(DEFAULT_SLAB_SIZE);
    UnsafeMemoryChunk[] slabs = new UnsafeMemoryChunk[] {slab};
    this.freeListManager = createFreeListManager(ma, slabs);
    assertThat(this.freeListManager.okToReuse(slabs)).isTrue();
  }
  @Test
  public void notOkToReuseDifferentSlabs() {
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(DEFAULT_SLAB_SIZE);
    UnsafeMemoryChunk[] slabs = new UnsafeMemoryChunk[] {slab};
    this.freeListManager = createFreeListManager(ma, slabs);
    UnsafeMemoryChunk[] slabs2 = new UnsafeMemoryChunk[] {slab};
    assertThat(this.freeListManager.okToReuse(slabs2)).isFalse();
  }
  @Test
  public void firstSlabAlwaysLargest() {
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {
        new UnsafeMemoryChunk(10), 
        new UnsafeMemoryChunk(100)});
    assertThat(this.freeListManager.getLargestSlabSize()).isEqualTo(10);
  }

  @Test
  public void findSlab() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(10);
    long address = chunk.getMemoryAddress();
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk});
    assertThat(this.freeListManager.findSlab(address)).isEqualTo(0);
    assertThat(this.freeListManager.findSlab(address+9)).isEqualTo(0);
    catchException(this.freeListManager).findSlab(address-1);
    assertThat((Exception)caughtException())
    .isExactlyInstanceOf(IllegalStateException.class)
    .hasMessage("could not find a slab for addr " + (address-1));
    catchException(this.freeListManager).findSlab(address+10);
    assertThat((Exception)caughtException())
    .isExactlyInstanceOf(IllegalStateException.class)
    .hasMessage("could not find a slab for addr " + (address+10));
  }
  
  @Test
  public void findSecondSlab() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(10);
    long address = chunk.getMemoryAddress();
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(DEFAULT_SLAB_SIZE);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {slab, chunk});
    assertThat(this.freeListManager.findSlab(address)).isEqualTo(1);
    assertThat(this.freeListManager.findSlab(address+9)).isEqualTo(1);
    catchException(this.freeListManager).findSlab(address-1);
    assertThat((Exception)caughtException())
    .isExactlyInstanceOf(IllegalStateException.class)
    .hasMessage("could not find a slab for addr " + (address-1));
    catchException(this.freeListManager).findSlab(address+10);
    assertThat((Exception)caughtException())
    .isExactlyInstanceOf(IllegalStateException.class)
    .hasMessage("could not find a slab for addr " + (address+10));
  }
  
  @Test
  public void validateAddressWithinSlab() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(10);
    long address = chunk.getMemoryAddress();
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk});
    assertThat(this.freeListManager.validateAddressAndSizeWithinSlab(address, -1)).isTrue();
    assertThat(this.freeListManager.validateAddressAndSizeWithinSlab(address+9, -1)).isTrue();
    assertThat(this.freeListManager.validateAddressAndSizeWithinSlab(address-1, -1)).isFalse();
    assertThat(this.freeListManager.validateAddressAndSizeWithinSlab(address+10, -1)).isFalse();
  }
  
  @Test
  public void validateAddressAndSizeWithinSlab() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(10);
    long address = chunk.getMemoryAddress();
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk});
    assertThat(this.freeListManager.validateAddressAndSizeWithinSlab(address, 1)).isTrue();
    assertThat(this.freeListManager.validateAddressAndSizeWithinSlab(address, 10)).isTrue();
    catchException(this.freeListManager).validateAddressAndSizeWithinSlab(address, 0);
    assertThat((Exception)caughtException())
    .isExactlyInstanceOf(IllegalStateException.class)
    .hasMessage(" address 0x" + Long.toString(address+0-1, 16) + " does not address the original slab memory");
    catchException(this.freeListManager).validateAddressAndSizeWithinSlab(address, 11);
    assertThat((Exception)caughtException())
    .isExactlyInstanceOf(IllegalStateException.class)
    .hasMessage(" address 0x" + Long.toString(address+11-1, 16) + " does not address the original slab memory");
  }
  
  @Test
  public void descriptionOfOneSlab() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(10);
    long address = chunk.getMemoryAddress();
    long endAddress = address+10;
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk});
    StringBuilder sb = new StringBuilder();
    this.freeListManager.getSlabDescriptions(sb);
    assertThat(sb.toString()).isEqualTo("[" + Long.toString(address, 16) + ".." + Long.toString(endAddress, 16) + "] ");
  }

  @Test
  public void orderBlocksContainsFragment() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(10);
    long address = chunk.getMemoryAddress();
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk});
    List<MemoryBlock> ob = this.freeListManager.getOrderedBlocks();
    assertThat(ob).hasSize(1);
    assertThat(ob.get(0).getMemoryAddress()).isEqualTo(address);
    assertThat(ob.get(0).getBlockSize()).isEqualTo(10);
  }
  
  @Test
  public void orderBlocksContainsTinyFree() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(96);
    long address = chunk.getMemoryAddress();
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk});
    ObjectChunk c = this.freeListManager.allocate(24);
    ObjectChunk c2 = this.freeListManager.allocate(24);
    ObjectChunk.release(c.getMemoryAddress(), this.freeListManager);

    List<MemoryBlock> ob = this.freeListManager.getOrderedBlocks();
    assertThat(ob).hasSize(3);
  }

  @Test
  public void allocatedBlocksEmptyIfNoAllocations() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(10);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk});
    List<MemoryBlock> ob = this.freeListManager.getAllocatedBlocks();
    assertThat(ob).hasSize(0);
  }

  @Test
  public void allocatedBlocksEmptyAfterFree() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(96);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk});
    ObjectChunk c = this.freeListManager.allocate(24);
    ObjectChunk.release(c.getMemoryAddress(), this.freeListManager);
    List<MemoryBlock> ob = this.freeListManager.getAllocatedBlocks();
    assertThat(ob).hasSize(0);
  }

  @Test
  public void allocatedBlocksHasAllocatedChunk() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(96);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk});
    ObjectChunk c = this.freeListManager.allocate(24);
    List<MemoryBlock> ob = this.freeListManager.getAllocatedBlocks();
    assertThat(ob).hasSize(1);
    assertThat(ob.get(0).getMemoryAddress()).isEqualTo(c.getMemoryAddress());
  }
  
  @Test
  public void allocatedBlocksHasBothAllocatedChunks() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(96);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk});
    ObjectChunk c = this.freeListManager.allocate(24);
    ObjectChunk c2 = this.freeListManager.allocate(33);
    List<MemoryBlock> ob = this.freeListManager.getAllocatedBlocks();
    assertThat(ob).hasSize(2);
  }
  
  @Test
  public void allocateFromFragmentWithBadIndexesReturnsNull() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(96);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk});
    assertThat(this.freeListManager.allocateFromFragment(-1, 32)).isNull();
    assertThat(this.freeListManager.allocateFromFragment(1, 32)).isNull();
  }

  @Test
  public void testLogging() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(32);
    UnsafeMemoryChunk chunk2 = new UnsafeMemoryChunk(1024*1024*5);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk, chunk2});
    ObjectChunk c = this.freeListManager.allocate(24);
    ObjectChunk c2 = this.freeListManager.allocate(1024*1024);
    ObjectChunk.release(c.getMemoryAddress(), this.freeListManager);
    ObjectChunk.release(c2.getMemoryAddress(), this.freeListManager);
    
    LogWriter lw = mock(LogWriter.class);
    this.freeListManager.logOffHeapState(lw, 1024);
  }
  
  @Test
  public void fragmentationShouldBeZeroIfNumberOfFragmentsIsZero() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(10);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk});
    
    FreeListManager spy = spy(this.freeListManager);
    
    when(spy.getFragmentCount()).thenReturn(0);
    
    assertThat(spy.getFragmentation()).isZero();
  }
  
  @Test
  public void fragmentationShouldBeZeroIfNumberOfFragmentsIsOne() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(10);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk});
    
    FreeListManager spy = spy(this.freeListManager);
    
    when(spy.getFragmentCount()).thenReturn(1);
    
    assertThat(spy.getFragmentation()).isZero();
  }
  
  @Test
  public void fragmentationShouldBeZeroIfUsedMemoryIsZero() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(10);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk});
    
    FreeListManager spy = spy(this.freeListManager);
    
    when(spy.getUsedMemory()).thenReturn(0L);
    
    assertThat(spy.getFragmentation()).isZero();
  }
  
  @Test
  public void fragmentationShouldBe100IfAllFreeMemoryIsFragmentedAsMinChunks() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(10);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk});
    
    FreeListManager spy = spy(this.freeListManager);
    
    when(spy.getUsedMemory()).thenReturn(1L);
    when(spy.getFragmentCount()).thenReturn(2);
    when(spy.getFreeMemory()).thenReturn((long)ObjectChunk.MIN_CHUNK_SIZE * 2);
    
    assertThat(spy.getFragmentation()).isEqualTo(100);
  }
  
  @Test
  public void fragmentationShouldBeRoundedToNearestInteger() {
    UnsafeMemoryChunk chunk = new UnsafeMemoryChunk(10);
    this.freeListManager = createFreeListManager(ma, new UnsafeMemoryChunk[] {chunk});
    
    FreeListManager spy = spy(this.freeListManager);
    
    when(spy.getUsedMemory()).thenReturn(1L);
    when(spy.getFragmentCount()).thenReturn(4);
    when(spy.getFreeMemory()).thenReturn((long)ObjectChunk.MIN_CHUNK_SIZE * 8);
    
    assertThat(spy.getFragmentation()).isEqualTo(50); //Math.rint(50.0)
    
    when(spy.getUsedMemory()).thenReturn(1L);
    when(spy.getFragmentCount()).thenReturn(3);
    when(spy.getFreeMemory()).thenReturn((long)ObjectChunk.MIN_CHUNK_SIZE * 8);
    
    assertThat(spy.getFragmentation()).isEqualTo(38); //Math.rint(37.5)
    
    when(spy.getUsedMemory()).thenReturn(1L);
    when(spy.getFragmentCount()).thenReturn(6);
    when(spy.getFreeMemory()).thenReturn((long)ObjectChunk.MIN_CHUNK_SIZE * 17);
    
    assertThat(spy.getFragmentation()).isEqualTo(35); //Math.rint(35.29)
    
    when(spy.getUsedMemory()).thenReturn(1L);
    when(spy.getFragmentCount()).thenReturn(6);
    when(spy.getFreeMemory()).thenReturn((long)ObjectChunk.MIN_CHUNK_SIZE * 9);
    
    assertThat(spy.getFragmentation()).isEqualTo(67); //Math.rint(66.66)
  }
  
  /**
   * Just like Fragment except that the first time allocate is called
   * it returns false indicating that the allocate failed.
   * In a real system this would only happen if a concurrent allocate
   * happened. This allows better code coverage.
   */
  private static class TestableFragment extends Fragment {
    private boolean allocateCalled = false;
    public TestableFragment(long addr, int size) {
      super(addr, size);
    }
    @Override
    public boolean allocate(int oldOffset, int newOffset) {
      if (!allocateCalled) {
        allocateCalled = true;
        return false;
      }
      return super.allocate(oldOffset, newOffset);
    }
  }
  private static class TestableFreeListManager extends FreeListManager {
    private boolean firstTime = true;
    
    @Override
    protected Fragment createFragment(long addr, int size) {
      return new TestableFragment(addr, size);
    }

    @Override
    protected SyncChunkStack createFreeListForEmptySlot(AtomicReferenceArray<SyncChunkStack> freeLists, int idx) {
      if (this.firstTime) {
        this.firstTime = false;
        SyncChunkStack clq = super.createFreeListForEmptySlot(freeLists, idx);
        if (!freeLists.compareAndSet(idx, null, clq)) {
          fail("this should never happen. Indicates a concurrent modification");
        }
      }
      return super.createFreeListForEmptySlot(freeLists, idx);
    }

    public boolean firstCompact = true;
    @Override
    protected void afterCompactCountFetched() {
      if (this.firstCompact) {
        this.firstCompact = false;
        // Force compact into thinking a concurrent compaction happened.
        this.compactCount.incrementAndGet();
      }
    }
    
    public TestableFreeListManager(SimpleMemoryAllocatorImpl ma, AddressableMemoryChunk[] slabs) {
      super(ma, slabs);
    }
    
  }
}
