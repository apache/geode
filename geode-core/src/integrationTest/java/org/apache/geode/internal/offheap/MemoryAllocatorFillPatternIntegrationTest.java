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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.OffHeapTest;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Tests fill pattern validation for the {@link MemoryAllocatorImpl}.
 */
@Category({OffHeapTest.class})
public class MemoryAllocatorFillPatternIntegrationTest {
  private static final Random random = ThreadLocalRandom.current();

  /**
   * Chunk operation types.
   */
  enum Operation {
    ALLOCATE, FREE, WRITE;

    // Holds all Operation values
    private static final Operation[] values = Operation.values();

    static Operation randomOperation() {
      return values[random.nextInt(values.length)];
    }
  }

  /** Number of worker threads for advanced tests. */
  private static final int WORKER_THREAD_COUNT = 5;

  /** Size of single test slab. */
  private static final int SLAB_SIZE = 1024 * 1024 * 50;

  /** Maximum number of bytes a worker thread can allocate during advanced tests. */
  private static final int MAX_WORKER_ALLOCATION_TOTAL_SIZE = SLAB_SIZE / WORKER_THREAD_COUNT / 2;

  /** Maximum allocation for a single Chunk. */
  private static final int MAX_WORKER_ALLOCATION_SIZE = 512;

  /** Canned data for write operations. */
  private static final byte[] WRITE_BYTES = "Some string data.".getBytes();

  /** Minimum size for write operations. */
  private static final int MIN_WORKER_ALLOCATION_SIZE = WRITE_BYTES.length;

  /** Runtime for worker threads. */
  private static final long RUN_TIME_IN_MILLIS = 1 * 1000 * 5;

  /** Chunk size for basic huge allocation test. */
  private static final int HUGE_CHUNK_SIZE = 1024 * 200;

  /** Our test victim. */
  private MemoryAllocatorImpl allocator = null;

  /** Our test victim's memory slab. */
  private SlabImpl slab = null;

  /**
   * Enables fill validation and creates the test victim.
   */
  @Before
  public void setUp() throws Exception {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "validateOffHeapWithFill", "true");
    slab = new SlabImpl(SLAB_SIZE);
    allocator = MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
        new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
  }

  /**
   * Frees off heap memory.
   */
  @After
  public void tearDown() throws Exception {
    MemoryAllocatorImpl.freeOffHeapMemory();
    System.clearProperty(GeodeGlossary.GEMFIRE_PREFIX + "validateOffHeapWithFill");
  }

  /**
   * This test hammers a MemoryAllocatorImpl with multiple threads exercising the fill validation of
   * tiny Chunks for one minute. This, of course, exercises many aspects of the MemoryAllocatorImpl
   * and its helper classes.
   *
   */
  @Test
  public void testFillPatternAdvancedForTinyAllocations() throws Exception {
    doFillPatternAdvancedTest(new ChunkSizer() {
      @Override
      public int allocationSize() {
        int allocation = random.nextInt(MAX_WORKER_ALLOCATION_SIZE + 1);

        while (allocation < MIN_WORKER_ALLOCATION_SIZE) {
          allocation = random.nextInt(MAX_WORKER_ALLOCATION_SIZE + 1);
        }
        return allocation;
      }
    });
  }

  /**
   * This test hammers a MemoryAllocatorImpl with multiple threads exercising the fill validation of
   * huge Chunks for one minute. This, of course, exercises many aspects of the MemoryAllocatorImpl
   * and its helper classes.
   *
   */
  @Test
  public void testFillPatternAdvancedForHugeAllocations() throws Exception {
    doFillPatternAdvancedTest(new ChunkSizer() {
      @Override
      public int allocationSize() {
        return HUGE_CHUNK_SIZE;
      }
    });
  }

  private interface ChunkSizer {
    int allocationSize();
  }

  private void doFillPatternAdvancedTest(final ChunkSizer chunkSizer) throws InterruptedException {
    // Used to manage worker thread completion
    final CountDownLatch latch = new CountDownLatch(WORKER_THREAD_COUNT);

    // Use to track any errors the worker threads will encounter
    final List<Throwable> threadErrorList =
        Collections.synchronizedList(new LinkedList<>());

    /*
     * Start up a number of worker threads. These threads will randomly allocate, free, and write to
     * Chunks.
     */
    for (int i = 0; i < WORKER_THREAD_COUNT; ++i) {
      new Thread(new Runnable() {
        // Total allocation in bytes for this thread
        private int totalAllocation = 0;

        // List of Chunks allocated by this thread
        private final List<OffHeapStoredObject> chunks = new LinkedList<>();

        // Time to end thread execution
        private final long endTime = System.currentTimeMillis() + RUN_TIME_IN_MILLIS;

        /**
         * Allocates a chunk and adds it to the thread's Chunk list.
         */
        private void allocate() {
          int allocation = chunkSizer.allocationSize();
          OffHeapStoredObject chunk = (OffHeapStoredObject) allocator.allocate(allocation);

          // This should always work just after allocation
          chunk.validateFill();

          chunks.add(chunk);
          totalAllocation += chunk.getSize();
        }

        /**
         * Frees a random chunk from the Chunk list.
         */
        private void free() {
          OffHeapStoredObject chunk = chunks.remove(random.nextInt(chunks.size()));
          totalAllocation -= chunk.getSize();

          /*
           * Chunk is filled here but another thread may have already grabbed it so we cannot
           * validate the fill.
           */
          chunk.release();
        }

        /**
         * Writes canned data to a random Chunk from the Chunk list.
         */
        private void write() {
          OffHeapStoredObject chunk = chunks.get(random.nextInt(chunks.size()));
          chunk.writeDataBytes(0, WRITE_BYTES);
        }

        /**
         * Randomly selects Chunk operations and executes them for a period of time. Collects any
         * error thrown during execution.
         */
        @Override
        public void run() {
          try {
            for (long currentTime = System.currentTimeMillis(); currentTime < endTime; currentTime =
                System.currentTimeMillis()) {
              Operation op = (totalAllocation == 0 ? Operation.ALLOCATE
                  : (totalAllocation >= MAX_WORKER_ALLOCATION_TOTAL_SIZE ? Operation.FREE
                      : Operation.randomOperation()));
              switch (op) {
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
    if (!threadErrorList.isEmpty()) {
      fail(threadErrorList.get(0).getMessage());
    }
  }

}
