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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;
import junit.framework.TestCase;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Tests fill pattern validation for the {@link SimpleMemoryAllocatorImpl}.
 * @author rholmes
 */
@Category(UnitTest.class)
public class SimpleMemoryAllocatorFillPatternJUnitTest {
  /**
   * Chunk operation types.
   * @author rholmes
   */
  static enum Operation {
    ALLOCATE,
    FREE,
    WRITE;
    
    // Unfortunately we cannot use ThreadLocalRandom in order to maintain 1.6 compatibility...
    private static Random random = new Random(System.currentTimeMillis());
    
    // Holds all Operation values
    private static Operation[] values = Operation.values(); 
    
    static Operation randomOperation() {
      return values[random.nextInt(values.length)];
    }
  };
  
  /** Number of worker threads for advanced tests. */
  private static final int WORKER_THREAD_COUNT = 5;
  
  /** Size of single test slab.*/
  private static final int SLAB_SIZE = 1024 * 1024 * 50;
  
  /** Maximum number of bytes a worker thread can allocate during advanced tests.  */
  private static final int MAX_WORKER_ALLOCATION_TOTAL_SIZE = SLAB_SIZE / WORKER_THREAD_COUNT / 2;
  
  /** Maximum allocation for a single Chunk.  */
  private static final int MAX_WORKER_ALLOCATION_SIZE = 512;
  
  /** Canned data for write operations. */
  private static final byte[] WRITE_BYTES = new String("Some string data.").getBytes();
  
  /** Minimum size for write operations. */
  private static final int MIN_WORKER_ALLOCATION_SIZE = WRITE_BYTES.length;

  /** Runtime for worker threads. */
  private static final long RUN_TIME_IN_MILLIS = 1 * 1000 * 60;
  
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
    /*
     * Pull a chunk off the fragment.  This will have no fill because
     * it is a "fresh" chunk.
     */
    Chunk chunk = (Chunk) this.allocator.allocate(1024, null);

    /*
     * Chunk should have valid fill from initial fragment allocation.
     */    
    try {
      chunk.validateFill();
    } catch(IllegalStateException e) {
      fail(e.getMessage());
    }
    
    // "Dirty" the chunk so the release has something to fill over
    chunk.writeBytes(Chunk.MIN_CHUNK_SIZE + 1, WRITE_BYTES);

    // This should free the Chunk (ref count == 1)
    chunk.release();

    /*
     * This chunk should have a fill because it was reused from the
     * free list (assuming no fragmentation at this point...)
     */
    chunk = (Chunk) this.allocator.allocate(1024, null);
    
    // Make sure we have a fill this time
    try {
      chunk.validateFill();
    } catch(IllegalStateException e) {
      TestCase.fail("Chunk fill validation failed: " + e.getMessage());
    }
    
    // Give the fill code something to write over during the release
    chunk.writeBytes(Chunk.MIN_CHUNK_SIZE + 1, WRITE_BYTES);
    chunk.release();

    // Again, make sure the release implemented the fill
    try {
      chunk.validateFill();
    } catch(IllegalStateException e) {
      TestCase.fail("Chunk fill validation failed: " + e.getMessage());
    }

    // "Dirty up" the free chunk
    chunk.writeBytes(Chunk.MIN_CHUNK_SIZE + 1, WRITE_BYTES);
    
    boolean failure = false;

    // One final check for validateFill()
    try {
      chunk.validateFill();
    } catch(IllegalStateException e) {
      failure = true;
    }
    
    assertTrue(failure);
  }
  
  /**
   * This tests the fill pattern for a single huge Chunk allocation.
   * @throws Exception
   */
  @Test
  public void testFillPatternBasicForHugeAllocations() throws Exception {
    /*
     * Pull a chunk off the fragment.  This will have no fill because
     * it is a "fresh" chunk.
     */
    Chunk chunk = (Chunk) this.allocator.allocate(HUGE_CHUNK_SIZE, null);

    /*
     * Chunk should have valid fill from initial fragment allocation.
     */
    try {
      chunk.validateFill();
    } catch(IllegalStateException e) {
      fail(e.getMessage());
    }
         
    // "Dirty" the chunk so the release has something to fill over
    chunk.writeBytes(Chunk.MIN_CHUNK_SIZE + 1, WRITE_BYTES);

    // This should free the Chunk (ref count == 1)
    chunk.release();

    /*
     * This chunk should have a fill because it was reused from the
     * free list (assuming no fragmentation at this point...)
     */
    chunk = (Chunk) this.allocator.allocate(HUGE_CHUNK_SIZE, null);
    
    // Make sure we have a fill this time
    try {
      chunk.validateFill();
    } catch(IllegalStateException e) {
      TestCase.fail("Chunk fill validation failed: " + e.getMessage());
    }
    
    // Give the fill code something to write over during the release
    chunk.writeBytes(Chunk.MIN_CHUNK_SIZE + 1, WRITE_BYTES);
    chunk.release();

    // Again, make sure the release implemented the fill
    try {
      chunk.validateFill();
    } catch(IllegalStateException e) {
      TestCase.fail("Chunk fill validation failed: " + e.getMessage());
    }

    // "Dirty up" the free chunk
    chunk.writeBytes(Chunk.MIN_CHUNK_SIZE + 1, WRITE_BYTES);
    
    boolean failure = false;

    // One final check for validateFill()
    try {
      chunk.validateFill();
    } catch(IllegalStateException e) {
      failure = true;
    }
    
    assertTrue(failure);
  }

  /**
   * This test hammers a SimpleMemoryAllocatorImpl with multiple threads exercising
   * the fill validation of tiny Chunks for one minute.  This, of course, exercises many aspects of
   * the SimpleMemoryAllocatorImpl and its helper classes.
   * @throws Exception
   */
  @Test
  public void testFillPatternAdvancedForTinyAllocations() throws Exception { 
    // Used to manage worker thread completion
    final CountDownLatch latch = new CountDownLatch(WORKER_THREAD_COUNT);
    
    // Use to track any errors the worker threads will encounter
    final List<Throwable> threadErrorList = Collections.synchronizedList(new LinkedList<Throwable>());
    
    /*
     * Start up a number of worker threads.  These threads will randomly allocate, free,
     * and write to Chunks.
     */
    for(int i = 0;i < WORKER_THREAD_COUNT;++i) {
      new Thread(new Runnable() {
        // Total allocation in bytes for this thread
        private int totalAllocation = 0;
        
        // List of Chunks allocated by this thread
        private List<Chunk> chunks = new LinkedList<Chunk>();
        
        // Time to end thread execution
        private long endTime = System.currentTimeMillis() + RUN_TIME_IN_MILLIS;
        
        // Randomizer used for random Chunk size allocation
        private Random random = new Random(endTime);
        
        /**
         * Returns an allocation size between a min and max constraint.
         */
        private int allocationSize() {
          int allocation = random.nextInt(MAX_WORKER_ALLOCATION_SIZE+1);
          
          while(allocation < MIN_WORKER_ALLOCATION_SIZE) {
            allocation = random.nextInt(MAX_WORKER_ALLOCATION_SIZE+1);
          }

          return allocation;
        }
        
        /**
         * Allocates a chunk and adds it to the thread's Chunk list.
         */
        private void allocate() {          
          int allocation = allocationSize();
          Chunk chunk = (Chunk) allocator.allocate(allocation, null);

          // This should always work just after allocation
          chunk.validateFill();          

          chunks.add(chunk);
          totalAllocation += chunk.getSize();
        }
        
        /**
         * Frees a random chunk from the Chunk list.
         */
        private void free() {
          Chunk chunk = chunks.remove(random.nextInt(chunks.size()));
          totalAllocation -= chunk.getSize();
          
          /*
           * Chunk is filled here but another thread may have already grabbed it so we
           * cannot validate the fill.
           */
          chunk.release();
        }
        
        /**
         * Writes canned data to a random Chunk from the Chunk list.
         */
        private void write() {
          Chunk chunk = chunks.get(random.nextInt(chunks.size()));
          chunk.writeBytes(0, WRITE_BYTES);
        }
        
        /**
         * Randomly selects Chunk operations and executes them
         * for a period of time.  Collects any error thrown during execution.
         */
        @Override
        public void run() {
          try {
            for(long currentTime = System.currentTimeMillis();currentTime < endTime;currentTime = System.currentTimeMillis()) {
              Operation op = (totalAllocation == 0 ? Operation.ALLOCATE : (totalAllocation >= MAX_WORKER_ALLOCATION_TOTAL_SIZE ? Operation.FREE : Operation.randomOperation()));
              switch(op) {
              case ALLOCATE:
                allocate();
                break;
              case FREE:
                free();
                break;
              case WRITE:
                write();
                break;
              }
            }
          } catch (Throwable t) {
            threadErrorList.add(t);
          } finally {
            latch.countDown();
          }
       }        
      }).start();
    }
    
    // Make sure each thread ended cleanly
    assertTrue(latch.await(2, TimeUnit.MINUTES));
    
    // Fail on the first error we find
    if(!threadErrorList.isEmpty()) {
      fail(threadErrorList.get(0).getMessage());
    }
  }

  /**
   * This test hammers a SimpleMemoryAllocatorImpl with multiple threads exercising
   * the fill validation of huge Chunks for one minute.  This, of course, exercises many aspects of
   * the SimpleMemoryAllocatorImpl and its helper classes.
   * @throws Exception
   */
  @Test
  public void testFillPatternAdvancedForHugeAllocations() throws Exception { 
    // Used to manage worker thread completion
    final CountDownLatch latch = new CountDownLatch(WORKER_THREAD_COUNT);
    
    // Use to track any errors the worker threads will encounter
    final List<Throwable> threadErrorList = Collections.synchronizedList(new LinkedList<Throwable>());
    
    /*
     * Start up a number of worker threads.  These threads will randomly allocate, free,
     * and write to Chunks.
     */
    for(int i = 0;i < WORKER_THREAD_COUNT;++i) {
      new Thread(new Runnable() {
        // Total allocation in bytes for this thread
        private int totalAllocation = 0;
        
        // List of Chunks allocated by this thread
        private List<Chunk> chunks = new LinkedList<Chunk>();
        
        // Time to end thread execution
        private long endTime = System.currentTimeMillis() + RUN_TIME_IN_MILLIS;
        
        // Randomizer used for random Chunk size allocation
        private Random random = new Random(endTime);
        
        /**
         * Returns an allocation size between a min and max constraint.
         */
        private int allocationSize() {
          return HUGE_CHUNK_SIZE;
        }
        
        /**
         * Allocates a chunk and adds it to the thread's Chunk list.
         */
        private void allocate() {          
          int allocation = allocationSize();
          Chunk chunk = (Chunk) allocator.allocate(allocation, null);
          
          // This should always work just after allocation
          chunk.validateFill();
          
          chunks.add(chunk);
          totalAllocation += chunk.getSize();
        }
        
        /**
         * Frees a random chunk from the Chunk list.
         */
        private void free() {
          Chunk chunk = chunks.remove(random.nextInt(chunks.size()));
          totalAllocation -= chunk.getSize();
          
          /*
           * Chunk is filled here but another thread may have already grabbed it so we
           * cannot validate the fill.
           */
          chunk.release(); 
        }
        
        /**
         * Writes canned data to a random Chunk from the Chunk list.
         */
        private void write() {
          Chunk chunk = chunks.get(random.nextInt(chunks.size()));
          chunk.writeBytes(0, WRITE_BYTES);
        }
        
        /**
         * Randomly selects Chunk operations and executes them
         * for a period of time.  Collects any error thrown during execution.
         */
        @Override
        public void run() {
          try {
            for(long currentTime = System.currentTimeMillis();currentTime < endTime;currentTime = System.currentTimeMillis()) {
              Operation op = (totalAllocation == 0 ? Operation.ALLOCATE : (totalAllocation >= MAX_WORKER_ALLOCATION_TOTAL_SIZE ? Operation.FREE : Operation.randomOperation()));
              switch(op) {
              case ALLOCATE:
                allocate();
                break;
              case FREE:
                free();
                break;
              case WRITE:
                write();
                break;
              }
            }
          } catch (Throwable t) {
            threadErrorList.add(t);
          } finally {
            latch.countDown();
          }
       }        
      }).start();
    }
    
    // Make sure each thread ended cleanly
    assertTrue(latch.await(2, TimeUnit.MINUTES));
    
    // Fail on the first error we find
    if(!threadErrorList.isEmpty()) {
      fail(threadErrorList.get(0).getMessage());
    }
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
