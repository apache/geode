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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.OutOfOffHeapMemoryException;
import org.apache.geode.distributed.internal.DistributionConfig;

public class FreeListManagerTest {

  private final int DEFAULT_SLAB_SIZE = 1024 * 1024 * 5;

  private final MemoryAllocatorImpl ma = mock(MemoryAllocatorImpl.class);
  private final OffHeapMemoryStats stats = mock(OffHeapMemoryStats.class);
  private TestableFreeListManager freeListManager;

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

  private static TestableFreeListManager createFreeListManager(MemoryAllocatorImpl ma,
      Slab[] slabs) {
    return new TestableFreeListManager(ma, slabs);
  }

  private static TestableFreeListManager createFreeListManager(MemoryAllocatorImpl ma, Slab[] slabs,
      int maxCombine) {
    return new TestableFreeListManager(ma, slabs, maxCombine);
  }

  private void setUpSingleSlabManager() {
    setUpSingleSlabManager(DEFAULT_SLAB_SIZE);
  }

  private void setUpSingleSlabManager(int slabSize) {
    Slab slab = new SlabImpl(slabSize);
    this.freeListManager = createFreeListManager(ma, new Slab[] {slab});
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

    OffHeapStoredObject c = this.freeListManager.allocate(tinySize);

    validateChunkSizes(c, tinySize);
  }

  private void validateChunkSizes(OffHeapStoredObject c, int dataSize) {
    assertThat(c).isNotNull();
    assertThat(c.getDataSize()).isEqualTo(dataSize);
    assertThat(c.getSize()).isEqualTo(computeExpectedSize(dataSize));
  }

  @Test
  public void allocateTinyChunkFromFreeListHasCorrectSize() {
    setUpSingleSlabManager();
    int tinySize = 10;

    OffHeapStoredObject c = this.freeListManager.allocate(tinySize);
    OffHeapStoredObject.release(c.getAddress(), this.freeListManager);
    c = this.freeListManager.allocate(tinySize);

    validateChunkSizes(c, tinySize);
  }

  @Test
  public void allocateTinyChunkFromEmptyFreeListHasCorrectSize() {
    setUpSingleSlabManager();
    int dataSize = 10;

    OffHeapStoredObject c = this.freeListManager.allocate(dataSize);
    OffHeapStoredObject.release(c.getAddress(), this.freeListManager);
    this.freeListManager.allocate(dataSize);
    // free list will now be empty
    c = this.freeListManager.allocate(dataSize);

    validateChunkSizes(c, dataSize);
  }

  @Test
  public void allocateHugeChunkHasCorrectSize() {
    setUpSingleSlabManager();
    int hugeSize = FreeListManager.MAX_TINY + 1;

    OffHeapStoredObject c = this.freeListManager.allocate(hugeSize);

    validateChunkSizes(c, hugeSize);
  }

  @Test
  public void allocateHugeChunkFromEmptyFreeListHasCorrectSize() {
    setUpSingleSlabManager();
    int dataSize = FreeListManager.MAX_TINY + 1;

    OffHeapStoredObject c = this.freeListManager.allocate(dataSize);
    OffHeapStoredObject.release(c.getAddress(), this.freeListManager);
    this.freeListManager.allocate(dataSize);
    // free list will now be empty
    c = this.freeListManager.allocate(dataSize);

    validateChunkSizes(c, dataSize);
  }

  @Test
  public void allocateHugeChunkFromFragmentWithItemInFreeListHasCorrectSize() {
    setUpSingleSlabManager();
    int dataSize = FreeListManager.MAX_TINY + 1 + 1024;

    OffHeapStoredObject c = this.freeListManager.allocate(dataSize);
    OffHeapStoredObject.release(c.getAddress(), this.freeListManager);
    dataSize = FreeListManager.MAX_TINY + 1;
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

    OffHeapStoredObject c = this.freeListManager.allocate(dataSize);
    OffHeapStoredObject.release(c.getAddress(), this.freeListManager);

    assertThat(this.freeListManager.getFreeTinyMemory()).isEqualTo(computeExpectedSize(dataSize));
  }

  @Test
  public void freeTinyMemoryWithTwoTinyFreeListsEqualToChunkSize() {
    setUpSingleSlabManager();
    int dataSize = 10;

    OffHeapStoredObject c = this.freeListManager.allocate(dataSize);
    OffHeapStoredObject.release(c.getAddress(), this.freeListManager);

    int dataSize2 = 100;

    OffHeapStoredObject c2 = this.freeListManager.allocate(dataSize2);
    OffHeapStoredObject.release(c2.getAddress(), this.freeListManager);

    assertThat(this.freeListManager.getFreeTinyMemory())
        .isEqualTo(computeExpectedSize(dataSize) + computeExpectedSize(dataSize2));
  }

  @Test
  public void freeHugeMemoryDefault() {
    setUpSingleSlabManager();

    assertThat(this.freeListManager.getFreeHugeMemory()).isZero();
  }

  @Test
  public void freeHugeMemoryEqualToChunkSize() {
    setUpSingleSlabManager();
    int dataSize = FreeListManager.MAX_TINY + 1 + 1024;

    OffHeapStoredObject c = this.freeListManager.allocate(dataSize);
    OffHeapStoredObject.release(c.getAddress(), this.freeListManager);

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
    OffHeapStoredObject c = this.freeListManager.allocate(DEFAULT_SLAB_SIZE / 4 - 8);

    assertThat(this.freeListManager.getFreeFragmentMemory()).isEqualTo((DEFAULT_SLAB_SIZE / 4) * 3);
  }

  @Test
  public void freeFragmentMemoryMostOfFragmentAllocated() {
    setUpSingleSlabManager();
    OffHeapStoredObject c = this.freeListManager.allocate(DEFAULT_SLAB_SIZE - 8 - 8);

    assertThat(this.freeListManager.getFreeFragmentMemory()).isZero();
  }

  private int computeExpectedSize(int dataSize) {
    return ((dataSize + OffHeapStoredObject.HEADER_SIZE + 7) / 8) * 8;
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
    Slab slab = new SlabImpl(DEFAULT_SLAB_SIZE);
    this.freeListManager = createFreeListManager(ma, new Slab[] {new SlabImpl(SMALL_SLAB),
        new SlabImpl(SMALL_SLAB), new SlabImpl(MEDIUM_SLAB), slab});
    this.freeListManager.allocate(SMALL_SLAB - 8 + 1);
    this.freeListManager.allocate(DEFAULT_SLAB_SIZE - 8);
    this.freeListManager.allocate(SMALL_SLAB - 8 + 1);
    assertThat(this.freeListManager.getFreeMemory())
        .isEqualTo(SMALL_SLAB * 2 + MEDIUM_SLAB - ((SMALL_SLAB + 8) * 2));
    assertThat(this.freeListManager.getUsedMemory())
        .isEqualTo(DEFAULT_SLAB_SIZE + (SMALL_SLAB + 8) * 2);
    assertThat(this.freeListManager.getTotalMemory())
        .isEqualTo(DEFAULT_SLAB_SIZE + MEDIUM_SLAB + SMALL_SLAB * 2);
  }

  @Test
  public void defragmentWithLargeChunkSizeReturnsFalse() {
    int SMALL_SLAB = 16;
    int MEDIUM_SLAB = 128;
    Slab slab = new SlabImpl(DEFAULT_SLAB_SIZE);
    this.freeListManager = createFreeListManager(ma, new Slab[] {new SlabImpl(SMALL_SLAB),
        new SlabImpl(SMALL_SLAB), new SlabImpl(MEDIUM_SLAB), slab});
    ArrayList<OffHeapStoredObject> chunks = new ArrayList<>();
    chunks.add(this.freeListManager.allocate(SMALL_SLAB - 8 + 1));
    chunks.add(this.freeListManager.allocate(DEFAULT_SLAB_SIZE / 2 - 8));
    chunks.add(this.freeListManager.allocate(DEFAULT_SLAB_SIZE / 2 - 8));
    chunks.add(this.freeListManager.allocate(SMALL_SLAB - 8 + 1));
    for (OffHeapStoredObject c : chunks) {
      OffHeapStoredObject.release(c.getAddress(), this.freeListManager);
    }
    this.freeListManager.firstDefragmentation = false;
    assertThat(this.freeListManager.defragment(DEFAULT_SLAB_SIZE + 1)).isFalse();
  }

  @Test
  public void testSlabImplToString() {
    Slab slab = new SlabImpl(DEFAULT_SLAB_SIZE);
    String slabAsString = slab.toString();
    assertThat(slabAsString.contains("MemoryAddress=" + slab.getMemoryAddress()));
    assertThat(slabAsString.contains("Size=" + DEFAULT_SLAB_SIZE));
  }

  @Test
  public void defragmentWithChunkSizeOfMaxSlabReturnsTrue() {
    int SMALL_SLAB = 16;
    int MEDIUM_SLAB = 128;
    Slab slab = new SlabImpl(DEFAULT_SLAB_SIZE, true);
    this.freeListManager = createFreeListManager(ma, new Slab[] {new SlabImpl(SMALL_SLAB, true),
        new SlabImpl(SMALL_SLAB, true), new SlabImpl(MEDIUM_SLAB, true), slab});
    ArrayList<OffHeapStoredObject> chunks = new ArrayList<>();
    chunks.add(this.freeListManager.allocate(SMALL_SLAB - 8 + 1));
    chunks.add(this.freeListManager.allocate(DEFAULT_SLAB_SIZE / 2 - 8));
    chunks.add(this.freeListManager.allocate(DEFAULT_SLAB_SIZE / 2 - 8));
    chunks.add(this.freeListManager.allocate(SMALL_SLAB - 8 + 1));
    for (OffHeapStoredObject c : chunks) {
      OffHeapStoredObject.release(c.getAddress(), this.freeListManager);
    }

    this.freeListManager.firstDefragmentation = false;
    assertThat(this.freeListManager.defragment(DEFAULT_SLAB_SIZE)).isTrue();
    assertThat(this.freeListManager.getFragmentList()).hasSize(4);
  }

  @Test
  public void defragmentWithLiveChunks() {
    int SMALL_SLAB = 16;
    int MEDIUM_SLAB = 128;
    Slab slab = new SlabImpl(DEFAULT_SLAB_SIZE);
    this.freeListManager = createFreeListManager(ma, new Slab[] {new SlabImpl(SMALL_SLAB),
        new SlabImpl(SMALL_SLAB), new SlabImpl(MEDIUM_SLAB), slab});
    ArrayList<OffHeapStoredObject> chunks = new ArrayList<>();
    chunks.add(this.freeListManager.allocate(SMALL_SLAB - 8 + 1));
    this.freeListManager.allocate(DEFAULT_SLAB_SIZE / 2 - 8);
    chunks.add(this.freeListManager.allocate(DEFAULT_SLAB_SIZE / 2 - 8));
    this.freeListManager.allocate(SMALL_SLAB - 8 + 1);
    for (OffHeapStoredObject c : chunks) {
      OffHeapStoredObject.release(c.getAddress(), this.freeListManager);
    }

    this.freeListManager.firstDefragmentation = false;
    assertThat(this.freeListManager.defragment(DEFAULT_SLAB_SIZE / 2)).isTrue();
  }

  @Test
  public void defragmentWhenDisallowingCombine() {
    int SMALL_SLAB = 16;
    int MEDIUM_SLAB = 128;
    Slab slab = new SlabImpl(DEFAULT_SLAB_SIZE);
    this.freeListManager = createFreeListManager(ma, new Slab[] {new SlabImpl(SMALL_SLAB),
        new SlabImpl(SMALL_SLAB), new SlabImpl(MEDIUM_SLAB), slab}, DEFAULT_SLAB_SIZE / 2);
    ArrayList<OffHeapStoredObject> chunks = new ArrayList<>();
    chunks.add(this.freeListManager.allocate(SMALL_SLAB - 8 + 1));
    chunks.add(this.freeListManager.allocate(DEFAULT_SLAB_SIZE / 2 - 8));
    chunks.add(this.freeListManager.allocate(DEFAULT_SLAB_SIZE / 2 - 8));
    this.freeListManager.allocate(SMALL_SLAB - 8 + 1);
    for (OffHeapStoredObject c : chunks) {
      OffHeapStoredObject.release(c.getAddress(), this.freeListManager);
    }

    this.freeListManager.firstDefragmentation = false;
    assertThat(this.freeListManager.defragment((DEFAULT_SLAB_SIZE / 2) + 1)).isFalse();
    assertThat(this.freeListManager.defragment(DEFAULT_SLAB_SIZE / 2)).isTrue();
  }

  @Test
  public void defragmentAfterAllocatingAll() {
    setUpSingleSlabManager();
    OffHeapStoredObject c = freeListManager.allocate(DEFAULT_SLAB_SIZE - 8);
    this.freeListManager.firstDefragmentation = false;
    assertThat(this.freeListManager.defragment(1)).isFalse();
    // call defragment twice for extra code coverage
    assertThat(this.freeListManager.defragment(1)).isFalse();
    assertThat(this.freeListManager.getFragmentList()).isEmpty();
  }

  @Test
  public void afterAllocatingAllOneSizeDefragmentToAllocateDifferentSize() {
    setUpSingleSlabManager();
    ArrayList<OffHeapStoredObject> chunksToFree = new ArrayList<>();
    ArrayList<OffHeapStoredObject> chunksToFreeLater = new ArrayList<>();
    int ALLOCATE_COUNT = 1000;
    OffHeapStoredObject bigChunk =
        freeListManager.allocate(DEFAULT_SLAB_SIZE - 8 - (ALLOCATE_COUNT * 32) - 256 - 256);
    for (int i = 0; i < ALLOCATE_COUNT; i++) {
      OffHeapStoredObject c = freeListManager.allocate(24);
      if (i % 3 != 2) {
        chunksToFree.add(c);
      } else {
        chunksToFreeLater.add(c);
      }
    }
    OffHeapStoredObject c1 = freeListManager.allocate(64 - 8);
    OffHeapStoredObject c2 = freeListManager.allocate(64 - 8);
    OffHeapStoredObject c3 = freeListManager.allocate(64 - 8);
    OffHeapStoredObject c4 = freeListManager.allocate(64 - 8);

    OffHeapStoredObject mediumChunk1 = freeListManager.allocate(128 - 8);
    OffHeapStoredObject mediumChunk2 = freeListManager.allocate(128 - 8);

    OffHeapStoredObject.release(bigChunk.getAddress(), freeListManager);
    int s = chunksToFree.size();
    for (int i = s / 2; i >= 0; i--) {
      OffHeapStoredObject c = chunksToFree.get(i);
      OffHeapStoredObject.release(c.getAddress(), freeListManager);
    }
    for (int i = (s / 2) + 1; i < s; i++) {
      OffHeapStoredObject c = chunksToFree.get(i);
      OffHeapStoredObject.release(c.getAddress(), freeListManager);
    }
    OffHeapStoredObject.release(c3.getAddress(), freeListManager);
    OffHeapStoredObject.release(c1.getAddress(), freeListManager);
    OffHeapStoredObject.release(c2.getAddress(), freeListManager);
    OffHeapStoredObject.release(c4.getAddress(), freeListManager);
    OffHeapStoredObject.release(mediumChunk1.getAddress(), freeListManager);
    OffHeapStoredObject.release(mediumChunk2.getAddress(), freeListManager);
    this.freeListManager.firstDefragmentation = false;
    assertThat(freeListManager.defragment(DEFAULT_SLAB_SIZE - (ALLOCATE_COUNT * 32))).isFalse();
    for (int i = 0; i < ((256 * 2) / 96); i++) {
      OffHeapStoredObject.release(chunksToFreeLater.get(i).getAddress(), freeListManager);
    }
    assertThat(freeListManager.defragment(DEFAULT_SLAB_SIZE - (ALLOCATE_COUNT * 32))).isTrue();
  }

  @Test
  public void afterAllocatingAndFreeingDefragment() {
    int slabSize = 1024 * 3;
    setUpSingleSlabManager(slabSize);
    OffHeapStoredObject bigChunk1 = freeListManager.allocate(slabSize / 3 - 8);
    OffHeapStoredObject bigChunk2 = freeListManager.allocate(slabSize / 3 - 8);
    OffHeapStoredObject bigChunk3 = freeListManager.allocate(slabSize / 3 - 8);
    this.freeListManager.firstDefragmentation = false;
    assertThat(freeListManager.defragment(1)).isFalse();
    OffHeapStoredObject.release(bigChunk3.getAddress(), freeListManager);
    OffHeapStoredObject.release(bigChunk2.getAddress(), freeListManager);
    OffHeapStoredObject.release(bigChunk1.getAddress(), freeListManager);
    assertThat(freeListManager.defragment(slabSize)).isTrue();
  }

  @Test
  public void defragmentWithEmptyTinyFreeList() {
    setUpSingleSlabManager();
    Fragment originalFragment = this.freeListManager.getFragmentList().get(0);
    OffHeapStoredObject c = freeListManager.allocate(16);
    OffHeapStoredObject.release(c.getAddress(), this.freeListManager);
    c = freeListManager.allocate(16);
    this.freeListManager.firstDefragmentation = false;
    assertThat(this.freeListManager.defragment(1)).isTrue();
    assertThat(this.freeListManager.getFragmentList()).hasSize(1);
    Fragment defragmentedFragment = this.freeListManager.getFragmentList().get(0);
    assertThat(defragmentedFragment.getSize()).isEqualTo(originalFragment.getSize() - (16 + 8));
    assertThat(defragmentedFragment.getAddress())
        .isEqualTo(originalFragment.getAddress() + (16 + 8));
  }

  @Test
  public void allocationsThatLeaveLessThanMinChunkSizeFreeInAFragment() {
    int SMALL_SLAB = 16;
    int MEDIUM_SLAB = 128;
    Slab slab = new SlabImpl(DEFAULT_SLAB_SIZE);
    this.freeListManager = createFreeListManager(ma, new Slab[] {new SlabImpl(SMALL_SLAB),
        new SlabImpl(SMALL_SLAB), new SlabImpl(MEDIUM_SLAB), slab});
    this.freeListManager.allocate(DEFAULT_SLAB_SIZE - 8 - (OffHeapStoredObject.MIN_CHUNK_SIZE - 1));
    this.freeListManager.allocate(MEDIUM_SLAB - 8 - (OffHeapStoredObject.MIN_CHUNK_SIZE - 1));

    this.freeListManager.firstDefragmentation = false;
    assertThat(this.freeListManager.defragment(SMALL_SLAB)).isTrue();
  }

  @Test
  public void maxAllocationUsesAllMemory() {
    setUpSingleSlabManager();

    this.freeListManager.allocate(DEFAULT_SLAB_SIZE - 8);

    assertThat(this.freeListManager.getFreeMemory()).isZero();
    assertThat(this.freeListManager.getUsedMemory()).isEqualTo(DEFAULT_SLAB_SIZE);
  }


  @Test
  public void overMaxAllocationFails() {
    setUpSingleSlabManager();
    OutOfOffHeapMemoryListener ooohml = mock(OutOfOffHeapMemoryListener.class);
    when(this.ma.getOutOfOffHeapMemoryListener()).thenReturn(ooohml);

    Throwable thrown = catchThrowable(() -> this.freeListManager.allocate(DEFAULT_SLAB_SIZE - 7));

    verify(ooohml).outOfOffHeapMemory((OutOfOffHeapMemoryException) thrown);
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
      assertThat(expected.getMessage()).contains(
          "HUGE_MULTIPLE must be >= 0 and <= " + FreeListManager.HUGE_MULTIPLE + " but it was -1");
    }
  }

  @Test
  public void hugeMultipleGreaterThan256IsIllegal() {
    try {
      FreeListManager.verifyHugeMultiple(257);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      assertThat(expected.getMessage())
          .contains("HUGE_MULTIPLE must be >= 0 and <= 256 but it was 257");
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
      assertThat(expected.getMessage())
          .contains(DistributionConfig.GEMFIRE_PREFIX + "OFF_HEAP_FREE_LIST_COUNT must be >= 1.");
    }
  }

  @Test
  public void offHeapFreeListCountOfZeroIsIllegal() {
    try {
      FreeListManager.verifyOffHeapFreeListCount(0);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      assertThat(expected.getMessage())
          .contains(DistributionConfig.GEMFIRE_PREFIX + "OFF_HEAP_FREE_LIST_COUNT must be >= 1.");
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
      assertThat(expected.getMessage()).contains(
          DistributionConfig.GEMFIRE_PREFIX + "OFF_HEAP_ALIGNMENT must be a multiple of 8");
    }
  }

  @Test
  public void offHeapAlignmentNotAMultipleOf8IsIllegal() {
    try {
      FreeListManager.verifyOffHeapAlignment(9);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      assertThat(expected.getMessage()).contains(
          DistributionConfig.GEMFIRE_PREFIX + "OFF_HEAP_ALIGNMENT must be a multiple of 8");
    }
  }

  @Test
  public void offHeapAlignmentGreaterThan256IsIllegal() {
    try {
      FreeListManager.verifyOffHeapAlignment(256 + 8);
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
      assertThat(expected.getMessage())
          .contains(DistributionConfig.GEMFIRE_PREFIX + "OFF_HEAP_ALIGNMENT must be <= 256");
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
    Slab slab = new SlabImpl(DEFAULT_SLAB_SIZE);
    Slab[] slabs = new Slab[] {slab};
    this.freeListManager = createFreeListManager(ma, slabs);
    assertThat(this.freeListManager.okToReuse(slabs)).isTrue();
  }

  @Test
  public void notOkToReuseDifferentSlabs() {
    Slab slab = new SlabImpl(DEFAULT_SLAB_SIZE);
    Slab[] slabs = new Slab[] {slab};
    this.freeListManager = createFreeListManager(ma, slabs);
    Slab[] slabs2 = new Slab[] {slab};
    assertThat(this.freeListManager.okToReuse(slabs2)).isFalse();
  }

  @Test
  public void firstSlabAlwaysLargest() {
    this.freeListManager =
        createFreeListManager(ma, new Slab[] {new SlabImpl(10), new SlabImpl(100)});
    assertThat(this.freeListManager.getLargestSlabSize()).isEqualTo(10);
  }

  @Test
  public void findSlab() {
    Slab chunk = new SlabImpl(10);
    long address = chunk.getMemoryAddress();
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});
    assertThat(this.freeListManager.findSlab(address)).isEqualTo(0);
    assertThat(this.freeListManager.findSlab(address + 9)).isEqualTo(0);

    Throwable thrown = catchThrowable(() -> this.freeListManager.findSlab(address - 1));
    assertThat(thrown).isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("could not find a slab for addr " + (address - 1));

    thrown = catchThrowable(() -> this.freeListManager.findSlab(address + 10));
    assertThat(thrown).isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("could not find a slab for addr " + (address + 10));
  }

  @Test
  public void findSecondSlab() {
    Slab chunk = new SlabImpl(10);
    long address = chunk.getMemoryAddress();
    Slab slab = new SlabImpl(DEFAULT_SLAB_SIZE);
    this.freeListManager = createFreeListManager(ma, new Slab[] {slab, chunk});
    assertThat(this.freeListManager.findSlab(address)).isEqualTo(1);
    assertThat(this.freeListManager.findSlab(address + 9)).isEqualTo(1);

    Throwable thrown = catchThrowable(() -> this.freeListManager.findSlab(address - 1));
    assertThat(thrown).isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("could not find a slab for addr " + (address - 1));

    thrown = catchThrowable(() -> this.freeListManager.findSlab(address + 10));
    assertThat(thrown).isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage("could not find a slab for addr " + (address + 10));
  }

  @Test
  public void validateAddressWithinSlab() {
    Slab chunk = new SlabImpl(10);
    long address = chunk.getMemoryAddress();
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});
    assertThat(this.freeListManager.validateAddressAndSizeWithinSlab(address, -1)).isTrue();
    assertThat(this.freeListManager.validateAddressAndSizeWithinSlab(address + 9, -1)).isTrue();
    assertThat(this.freeListManager.validateAddressAndSizeWithinSlab(address - 1, -1)).isFalse();
    assertThat(this.freeListManager.validateAddressAndSizeWithinSlab(address + 10, -1)).isFalse();
  }

  @Test
  public void validateAddressAndSizeWithinSlab() {
    Slab chunk = new SlabImpl(10);
    long address = chunk.getMemoryAddress();
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});
    assertThat(this.freeListManager.validateAddressAndSizeWithinSlab(address, 1)).isTrue();
    assertThat(this.freeListManager.validateAddressAndSizeWithinSlab(address, 10)).isTrue();

    Throwable thrown =
        catchThrowable(() -> this.freeListManager.validateAddressAndSizeWithinSlab(address, 0));
    assertThat(thrown).isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(" address 0x" + Long.toString(address + 0 - 1, 16)
            + " does not address the original slab memory");

    thrown =
        catchThrowable(() -> this.freeListManager.validateAddressAndSizeWithinSlab(address, 11));
    assertThat(thrown).isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(" address 0x" + Long.toString(address + 11 - 1, 16)
            + " does not address the original slab memory");
  }

  @Test
  public void descriptionOfOneSlab() {
    Slab chunk = new SlabImpl(10);
    long address = chunk.getMemoryAddress();
    long endAddress = address + 10;
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});
    StringBuilder sb = new StringBuilder();
    this.freeListManager.getSlabDescriptions(sb);
    assertThat(sb.toString())
        .isEqualTo("[" + Long.toString(address, 16) + ".." + Long.toString(endAddress, 16) + "] ");
  }

  @Test
  public void orderBlocksContainsFragment() {
    Slab chunk = new SlabImpl(10);
    long address = chunk.getMemoryAddress();
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});
    List<MemoryBlock> ob = this.freeListManager.getOrderedBlocks();
    assertThat(ob).hasSize(1);
    assertThat(ob.get(0).getAddress()).isEqualTo(address);
    assertThat(ob.get(0).getBlockSize()).isEqualTo(10);
  }

  @Test
  public void orderBlocksContainsTinyFree() {
    Slab chunk = new SlabImpl(96);
    long address = chunk.getMemoryAddress();
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});
    OffHeapStoredObject c = this.freeListManager.allocate(24);
    OffHeapStoredObject c2 = this.freeListManager.allocate(24);
    OffHeapStoredObject.release(c.getAddress(), this.freeListManager);

    List<MemoryBlock> ob = this.freeListManager.getOrderedBlocks();
    assertThat(ob).hasSize(3);
  }

  @Test
  public void allocatedBlocksEmptyIfNoAllocations() {
    Slab chunk = new SlabImpl(10);
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});
    List<MemoryBlock> ob = this.freeListManager.getAllocatedBlocks();
    assertThat(ob).hasSize(0);
  }

  @Test
  public void allocatedBlocksEmptyAfterFree() {
    Slab chunk = new SlabImpl(96);
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});
    OffHeapStoredObject c = this.freeListManager.allocate(24);
    OffHeapStoredObject.release(c.getAddress(), this.freeListManager);
    List<MemoryBlock> ob = this.freeListManager.getAllocatedBlocks();
    assertThat(ob).hasSize(0);
  }

  @Test
  public void allocatedBlocksHasAllocatedChunk() {
    Slab chunk = new SlabImpl(96);
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});
    OffHeapStoredObject c = this.freeListManager.allocate(24);
    List<MemoryBlock> ob = this.freeListManager.getAllocatedBlocks();
    assertThat(ob).hasSize(1);
    assertThat(ob.get(0).getAddress()).isEqualTo(c.getAddress());
  }

  @Test
  public void allocatedBlocksHasBothAllocatedChunks() {
    Slab chunk = new SlabImpl(96);
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});
    OffHeapStoredObject c = this.freeListManager.allocate(24);
    OffHeapStoredObject c2 = this.freeListManager.allocate(33);
    List<MemoryBlock> ob = this.freeListManager.getAllocatedBlocks();
    assertThat(ob).hasSize(2);
  }

  @Test
  public void allocateFromFragmentWithBadIndexesReturnsNull() {
    Slab chunk = new SlabImpl(96);
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});
    assertThat(this.freeListManager.allocateFromFragment(-1, 32)).isNull();
    assertThat(this.freeListManager.allocateFromFragment(1, 32)).isNull();
  }

  @Test
  public void testLogging() {
    Slab chunk = new SlabImpl(32);
    Slab chunk2 = new SlabImpl(1024 * 1024 * 5);
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk, chunk2});
    OffHeapStoredObject c = this.freeListManager.allocate(24);
    OffHeapStoredObject c2 = this.freeListManager.allocate(1024 * 1024);
    OffHeapStoredObject.release(c.getAddress(), this.freeListManager);
    OffHeapStoredObject.release(c2.getAddress(), this.freeListManager);

    Logger lw = mock(Logger.class);
    this.freeListManager.logOffHeapState(lw, 1024);
  }

  @Test
  public void fragmentationShouldBeZeroIfNumberOfFragmentsIsZero() {
    SlabImpl chunk = new SlabImpl(10);
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});

    FreeListManager spy = spy(this.freeListManager);

    when(spy.getFragmentCount()).thenReturn(0);

    assertThat(spy.getFragmentation()).isZero();
  }

  @Test
  public void fragmentationShouldBeZeroIfNumberOfFragmentsIsOne() {
    SlabImpl chunk = new SlabImpl(10);
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});

    FreeListManager spy = spy(this.freeListManager);

    when(spy.getFragmentCount()).thenReturn(1);

    assertThat(spy.getFragmentation()).isZero();
  }

  @Test
  public void fragmentationShouldBeZeroIfUsedMemoryIsZero() {
    SlabImpl chunk = new SlabImpl(10);
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});

    FreeListManager spy = spy(this.freeListManager);

    when(spy.getUsedMemory()).thenReturn(0L);

    assertThat(spy.getFragmentation()).isZero();
  }

  @Test
  public void fragmentationShouldBe100IfAllFreeMemoryIsFragmentedAsMinChunks() {
    SlabImpl chunk = new SlabImpl(10);
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});

    FreeListManager spy = spy(this.freeListManager);

    when(spy.getUsedMemory()).thenReturn(1L);
    when(spy.getFragmentCount()).thenReturn(2);
    when(spy.getFreeMemory()).thenReturn((long) OffHeapStoredObject.MIN_CHUNK_SIZE * 2);

    assertThat(spy.getFragmentation()).isEqualTo(100);
  }

  @Test
  public void fragmentationShouldBeRoundedToNearestInteger() {
    SlabImpl chunk = new SlabImpl(10);
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});

    FreeListManager spy = spy(this.freeListManager);

    when(spy.getUsedMemory()).thenReturn(1L);
    when(spy.getFragmentCount()).thenReturn(4);
    when(spy.getFreeMemory()).thenReturn((long) OffHeapStoredObject.MIN_CHUNK_SIZE * 8);

    assertThat(spy.getFragmentation()).isEqualTo(50); // Math.rint(50.0)

    when(spy.getUsedMemory()).thenReturn(1L);
    when(spy.getFragmentCount()).thenReturn(3);
    when(spy.getFreeMemory()).thenReturn((long) OffHeapStoredObject.MIN_CHUNK_SIZE * 8);

    assertThat(spy.getFragmentation()).isEqualTo(38); // Math.rint(37.5)

    when(spy.getUsedMemory()).thenReturn(1L);
    when(spy.getFragmentCount()).thenReturn(6);
    when(spy.getFreeMemory()).thenReturn((long) OffHeapStoredObject.MIN_CHUNK_SIZE * 17);

    assertThat(spy.getFragmentation()).isEqualTo(35); // Math.rint(35.29)

    when(spy.getUsedMemory()).thenReturn(1L);
    when(spy.getFragmentCount()).thenReturn(6);
    when(spy.getFreeMemory()).thenReturn((long) OffHeapStoredObject.MIN_CHUNK_SIZE * 9);

    assertThat(spy.getFragmentation()).isEqualTo(67); // Math.rint(66.66)
  }

  @Test
  public void isAdjacentBoundaryConditions() {
    SlabImpl chunk = new SlabImpl(10);
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});

    assertThat(!this.freeListManager.isAdjacent(Long.MAX_VALUE - 4, 4, Long.MAX_VALUE + 1));
    assertThat(this.freeListManager.isAdjacent(Long.MAX_VALUE - 4, 4, Long.MAX_VALUE));
    assertThat(this.freeListManager.isAdjacent(-8L, 4, -4L));
    long lowAddr = Long.MAX_VALUE;
    long highAddr = lowAddr + 4;
    assertThat(this.freeListManager.isAdjacent(lowAddr, 4, highAddr));
    assertThat(!this.freeListManager.isAdjacent(lowAddr, 4, highAddr - 1));
    assertThat(!this.freeListManager.isAdjacent(lowAddr, 4, highAddr + 1));
    lowAddr = highAddr;
    highAddr = lowAddr + 4;
    assertThat(this.freeListManager.isAdjacent(lowAddr, 4, highAddr));
    assertThat(!this.freeListManager.isAdjacent(highAddr, 4, lowAddr));
  }

  @Test
  public void isSmallEnoughBoundaryConditions() {
    SlabImpl chunk = new SlabImpl(10);
    this.freeListManager = createFreeListManager(ma, new Slab[] {chunk});

    assertThat(this.freeListManager.isSmallEnough(Integer.MAX_VALUE));
    assertThat(this.freeListManager.isSmallEnough(Integer.MAX_VALUE - 1));
    assertThat(!this.freeListManager.isSmallEnough(Integer.MAX_VALUE + 1L));
    assertThat(!this.freeListManager.isSmallEnough(Long.MAX_VALUE));
  }

  /**
   * Just like Fragment except that the first time allocate is called it returns false indicating
   * that the allocate failed. In a real system this would only happen if a concurrent allocate
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
    private boolean firstDefragmentation = true;
    private final int maxCombine;

    public TestableFreeListManager(MemoryAllocatorImpl ma, Slab[] slabs) {
      this(ma, slabs, 0);
    }

    public TestableFreeListManager(MemoryAllocatorImpl ma, Slab[] slabs, int maxCombine) {
      super(ma, slabs);
      this.maxCombine = maxCombine;
    }

    @Override
    protected Fragment createFragment(long addr, int size) {
      return new TestableFragment(addr, size);
    }

    @Override
    protected OffHeapStoredObjectAddressStack createFreeListForEmptySlot(
        AtomicReferenceArray<OffHeapStoredObjectAddressStack> freeLists, int idx) {
      if (this.firstTime) {
        this.firstTime = false;
        OffHeapStoredObjectAddressStack clq = super.createFreeListForEmptySlot(freeLists, idx);
        if (!freeLists.compareAndSet(idx, null, clq)) {
          fail("this should never happen. Indicates a concurrent modification");
        }
      }
      return super.createFreeListForEmptySlot(freeLists, idx);
    }

    @Override
    protected void afterDefragmentationCountFetched() {
      if (this.firstDefragmentation) {
        this.firstDefragmentation = false;
        // Force defragmentation into thinking a concurrent defragmentation happened.
        this.defragmentationCount.incrementAndGet();
      } else {
        super.afterDefragmentationCountFetched();
      }
    }

    @Override
    boolean isSmallEnough(long size) {
      if (this.maxCombine != 0) {
        return size <= this.maxCombine;
      } else {
        return super.isSmallEnough(size);
      }
    }


  }
}
