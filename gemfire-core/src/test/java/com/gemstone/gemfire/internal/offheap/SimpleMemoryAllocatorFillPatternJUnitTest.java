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
import static com.googlecode.catchexception.CatchException.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junit.framework.TestCase;

/**
 * Tests fill pattern validation for the {@link SimpleMemoryAllocatorImpl}.
 * @author rholmes
 */
@Category(UnitTest.class)
public class SimpleMemoryAllocatorFillPatternJUnitTest {
  
  /** Size of single test slab.*/
  private static final int SLAB_SIZE = 1024 * 1024 * 50;
  
  /** Canned data for write operations. */
  private static final byte[] WRITE_BYTES = new String("Some string data.").getBytes();
  
  /** Chunk size for basic huge allocation test. */
  private static final int HUGE_CHUNK_SIZE = 1024 * 200;
  
  /** The number of chunks to allocate in order to force compaction. */
  private static final int COMPACTION_CHUNKS = 3;
  
  /** Our slab size divided in three (with some padding for safety). */
  private static final int COMPACTION_CHUNK_SIZE = (SLAB_SIZE / COMPACTION_CHUNKS) - 1024;
  
  /** This should force compaction when allocated. */
  private static final int FORCE_COMPACTION_CHUNK_SIZE = COMPACTION_CHUNK_SIZE * 2;

  /** Our test victim. */
  private SimpleMemoryAllocatorImpl allocator = null;
  
  /** Our test victim's memory slab. */
  private UnsafeMemoryChunk slab = null;

  /**
   * Enables fill validation and creates the test victim.
   */
  @Before
  public void setUp() throws Exception {
    System.setProperty("gemfire.validateOffHeapWithFill", "true");
    this.slab = new UnsafeMemoryChunk(SLAB_SIZE);
    this.allocator = SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{this.slab});
  }

  /**
   * Frees off heap memory.
   */
  @After
  public void tearDown() throws Exception {
    SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    System.clearProperty("gemfire.validateOffHeapWithFill");
  }

  /**
   * This tests the fill pattern for a single tiny Chunk allocation.
   * @throws Exception
   */
  @Test
  public void testFillPatternBasicForTinyAllocations() throws Exception {
    doFillPatternBasic(1024);
  }
  
  /**
   * This tests the fill pattern for a single huge Chunk allocation.
   * @throws Exception
   */
  @Test
  public void testFillPatternBasicForHugeAllocations() throws Exception {
    doFillPatternBasic(HUGE_CHUNK_SIZE);
  }
  
  private void doFillPatternBasic(final int chunkSize) {
    /*
     * Pull a chunk off the fragment.  This will have no fill because
     * it is a "fresh" chunk.
     */
    Chunk chunk = (Chunk) this.allocator.allocate(chunkSize, null);

    /*
     * Chunk should have valid fill from initial fragment allocation.
     */
    chunk.validateFill();
         
    // "Dirty" the chunk so the release has something to fill over
    chunk.writeBytes(Chunk.MIN_CHUNK_SIZE + 1, WRITE_BYTES);

    // This should free the Chunk (ref count == 1)
    chunk.release();

    /*
     * This chunk should have a fill because it was reused from the
     * free list (assuming no fragmentation at this point...)
     */
    chunk = (Chunk) this.allocator.allocate(chunkSize, null);
    
    // Make sure we have a fill this time
    chunk.validateFill();
    
    // Give the fill code something to write over during the release
    chunk.writeBytes(Chunk.MIN_CHUNK_SIZE + 1, WRITE_BYTES);
    chunk.release();

    // Again, make sure the release implemented the fill
    chunk.validateFill();

    // "Dirty up" the free chunk
    chunk.writeBytes(Chunk.MIN_CHUNK_SIZE + 1, WRITE_BYTES);
    
    catchException(chunk).validateFill();
    assertTrue(caughtException() instanceof IllegalStateException);
    assertEquals("Fill pattern violated for chunk " + chunk.getMemoryAddress() + " with size " + chunk.getSize(), caughtException().getMessage());
    
  }

  /**
   * This tests that fill validation is working properly on newly created fragments after
   * a compaction.
   * @throws Exception
   */
  @Test
  public void testFillPatternAfterCompaction() throws Exception {
    /*
     * Stores our allocated memory.
     */
    Chunk[] allocatedChunks = new Chunk[COMPACTION_CHUNKS];
    
    /*
     * Use up most of our memory
     * Our memory looks like [      ][      ][      ]
     */
    for(int i =0;i < allocatedChunks.length;++i) {
      allocatedChunks[i] = (Chunk) this.allocator.allocate(COMPACTION_CHUNK_SIZE, null);
      allocatedChunks[i].validateFill();
    }

    /*
     * Release some of our allocated chunks.
     */
    for(int i=0;i < 2;++i) {
      allocatedChunks[i].release();
      allocatedChunks[i].validateFill();      
    }
    
    /*
     * Now, allocate another chunk that is slightly larger than one of
     * our initial chunks.  This should force a compaction causing our
     * memory to look like [            ][      ].
     */
    Chunk slightlyLargerChunk = (Chunk) this.allocator.allocate(FORCE_COMPACTION_CHUNK_SIZE, null);
    
    /*
     * Make sure the compacted memory has the fill validation.
     */
    slightlyLargerChunk.validateFill();
  }
}
