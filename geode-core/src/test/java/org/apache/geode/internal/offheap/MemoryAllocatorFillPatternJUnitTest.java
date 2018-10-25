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

import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.DistributionConfig;

/**
 * Tests fill pattern validation for the {@link MemoryAllocatorImpl}.
 */
public class MemoryAllocatorFillPatternJUnitTest {

  /** Size of single test slab. */
  private static final int SLAB_SIZE = 1024 * 1024 * 50;

  /** Canned data for write operations. */
  private static final byte[] WRITE_BYTES = new String("Some string data.").getBytes();

  /** Chunk size for basic huge allocation test. */
  private static final int HUGE_CHUNK_SIZE = 1024 * 200;

  /** The number of chunks to allocate in order to force defragmentation. */
  private static final int DEFRAGMENTATION_CHUNKS = 3;

  /** Our slab size divided in three (with some padding for safety). */
  private static final int DEFRAGMENTATION_CHUNK_SIZE = (SLAB_SIZE / DEFRAGMENTATION_CHUNKS) - 1024;

  /** This should force defragmentation when allocated. */
  private static final int FORCE_DEFRAGMENTATION_CHUNK_SIZE = DEFRAGMENTATION_CHUNK_SIZE * 2;

  /** Our test victim. */
  private MemoryAllocatorImpl allocator = null;

  /** Our test victim's memory slab. */
  private SlabImpl slab = null;

  /**
   * Enables fill validation and creates the test victim.
   */
  @Before
  public void setUp() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "validateOffHeapWithFill", "true");
    this.slab = new SlabImpl(SLAB_SIZE);
    this.allocator = MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
        new NullOffHeapMemoryStats(), new SlabImpl[] {this.slab});
  }

  /**
   * Frees off heap memory.
   */
  @After
  public void tearDown() throws Exception {
    MemoryAllocatorImpl.freeOffHeapMemory();
    System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "validateOffHeapWithFill");
  }

  /**
   * This tests the fill pattern for a single tiny Chunk allocation.
   *
   */
  @Test
  public void testFillPatternBasicForTinyAllocations() {
    doFillPatternBasic(1024);
  }

  /**
   * This tests the fill pattern for a single huge Chunk allocation.
   *
   */
  @Test
  public void testFillPatternBasicForHugeAllocations() {
    doFillPatternBasic(HUGE_CHUNK_SIZE);
  }

  private void doFillPatternBasic(final int chunkSize) {
    /*
     * Pull a chunk off the fragment. This will have no fill because it is a "fresh" chunk.
     */
    OffHeapStoredObject chunk1 = (OffHeapStoredObject) this.allocator.allocate(chunkSize);

    /*
     * Chunk should have valid fill from initial fragment allocation.
     */
    chunk1.validateFill();

    // "Dirty" the chunk so the release has something to fill over
    chunk1.writeDataBytes(OffHeapStoredObject.MIN_CHUNK_SIZE + 1, WRITE_BYTES);

    // This should free the Chunk (ref count == 1)
    chunk1.release();

    /*
     * This chunk should have a fill because it was reused from the free list (assuming no
     * fragmentation at this point...)
     */
    OffHeapStoredObject chunk = (OffHeapStoredObject) this.allocator.allocate(chunkSize);

    // Make sure we have a fill this time
    chunk.validateFill();

    // Give the fill code something to write over during the release
    chunk.writeDataBytes(OffHeapStoredObject.MIN_CHUNK_SIZE + 1, WRITE_BYTES);
    chunk.release();

    // Again, make sure the release implemented the fill
    chunk.validateFill();

    // "Dirty up" the free chunk
    chunk.writeDataBytes(OffHeapStoredObject.MIN_CHUNK_SIZE + 1, WRITE_BYTES);

    Throwable thrown = catchThrowable(() -> chunk.validateFill());
    assertTrue(thrown instanceof IllegalStateException);
    assertEquals(
        "Fill pattern violated for chunk " + chunk.getAddress() + " with size " + chunk.getSize(),
        thrown.getMessage());

  }

  /**
   * This tests that fill validation is working properly on newly created fragments after a
   * defragmentation.
   *
   */
  @Test
  public void testFillPatternAfterDefragmentation() {
    /*
     * Stores our allocated memory.
     */
    OffHeapStoredObject[] allocatedChunks = new OffHeapStoredObject[DEFRAGMENTATION_CHUNKS];

    /*
     * Use up most of our memory Our memory looks like [ ][ ][ ]
     */
    for (int i = 0; i < allocatedChunks.length; ++i) {
      allocatedChunks[i] =
          (OffHeapStoredObject) this.allocator.allocate(DEFRAGMENTATION_CHUNK_SIZE);
      allocatedChunks[i].validateFill();
    }

    /*
     * Release some of our allocated chunks.
     */
    for (int i = 0; i < 2; ++i) {
      allocatedChunks[i].release();
      allocatedChunks[i].validateFill();
    }

    /*
     * Now, allocate another chunk that is slightly larger than one of our initial chunks. This
     * should force a defragmentation causing our memory to look like [ ][ ].
     */
    OffHeapStoredObject slightlyLargerChunk =
        (OffHeapStoredObject) this.allocator.allocate(FORCE_DEFRAGMENTATION_CHUNK_SIZE);

    /*
     * Make sure the defragmented memory has the fill validation.
     */
    slightlyLargerChunk.validateFill();
  }
}
