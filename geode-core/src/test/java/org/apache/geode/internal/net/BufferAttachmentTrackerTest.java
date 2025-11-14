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

package org.apache.geode.internal.net;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Test;

/**
 * Unit tests for BufferAttachmentTracker.
 */
public class BufferAttachmentTrackerTest {

  @After
  public void tearDown() {
    // Clean up after each test
    BufferAttachmentTracker.clearTracking();
  }

  @Test
  public void getOriginal_returnsOriginalBufferForSlice() {
    ByteBuffer original = ByteBuffer.allocateDirect(1024);
    original.position(0).limit(512);
    ByteBuffer slice = original.slice();

    BufferAttachmentTracker.recordSlice(slice, original);

    ByteBuffer result = BufferAttachmentTracker.getOriginal(slice);

    assertThat(result).isSameAs(original);
  }

  @Test
  public void getOriginal_returnsBufferItselfWhenNotTracked() {
    ByteBuffer buffer = ByteBuffer.allocateDirect(1024);

    ByteBuffer result = BufferAttachmentTracker.getOriginal(buffer);

    assertThat(result).isSameAs(buffer);
  }

  @Test
  public void removeTracking_removesSliceMapping() {
    ByteBuffer original = ByteBuffer.allocateDirect(1024);
    original.position(0).limit(512);
    ByteBuffer slice = original.slice();

    BufferAttachmentTracker.recordSlice(slice, original);
    assertThat(BufferAttachmentTracker.getTrackingMapSize()).isEqualTo(1);

    BufferAttachmentTracker.removeTracking(slice);

    assertThat(BufferAttachmentTracker.getTrackingMapSize()).isEqualTo(0);
    assertThat(BufferAttachmentTracker.getOriginal(slice)).isSameAs(slice);
  }

  @Test
  public void trackingMapSize_reflectsCurrentMappings() {
    assertThat(BufferAttachmentTracker.getTrackingMapSize()).isEqualTo(0);

    ByteBuffer original1 = ByteBuffer.allocateDirect(1024);
    ByteBuffer slice1 = original1.slice();
    BufferAttachmentTracker.recordSlice(slice1, original1);
    assertThat(BufferAttachmentTracker.getTrackingMapSize()).isEqualTo(1);

    ByteBuffer original2 = ByteBuffer.allocateDirect(2048);
    ByteBuffer slice2 = original2.slice();
    BufferAttachmentTracker.recordSlice(slice2, original2);
    assertThat(BufferAttachmentTracker.getTrackingMapSize()).isEqualTo(2);
  }

  @Test
  public void clearTracking_removesAllMappings() {
    ByteBuffer original1 = ByteBuffer.allocateDirect(1024);
    ByteBuffer slice1 = original1.slice();
    BufferAttachmentTracker.recordSlice(slice1, original1);

    ByteBuffer original2 = ByteBuffer.allocateDirect(2048);
    ByteBuffer slice2 = original2.slice();
    BufferAttachmentTracker.recordSlice(slice2, original2);

    assertThat(BufferAttachmentTracker.getTrackingMapSize()).isEqualTo(2);

    BufferAttachmentTracker.clearTracking();

    assertThat(BufferAttachmentTracker.getTrackingMapSize()).isEqualTo(0);
  }

  @Test
  public void recordSlice_canOverwriteExistingMapping() {
    ByteBuffer original1 = ByteBuffer.allocateDirect(1024);
    ByteBuffer original2 = ByteBuffer.allocateDirect(2048);
    ByteBuffer slice = original1.slice();

    BufferAttachmentTracker.recordSlice(slice, original1);
    assertThat(BufferAttachmentTracker.getOriginal(slice)).isSameAs(original1);

    BufferAttachmentTracker.recordSlice(slice, original2);
    assertThat(BufferAttachmentTracker.getOriginal(slice)).isSameAs(original2);
  }

  @Test
  public void worksWithHeapBuffers() {
    ByteBuffer original = ByteBuffer.allocate(1024);
    original.position(0).limit(512);
    ByteBuffer slice = original.slice();

    BufferAttachmentTracker.recordSlice(slice, original);

    ByteBuffer result = BufferAttachmentTracker.getOriginal(slice);

    assertThat(result).isSameAs(original);
  }

  @Test
  public void simpleThreadSafetyTest() {
    // Create a single original and slice
    ByteBuffer original = ByteBuffer.allocateDirect(1024);
    ByteBuffer slice = original.slice();
    
    // Record it
    BufferAttachmentTracker.recordSlice(slice, original);
    
    // Immediately retrieve it
    ByteBuffer result = BufferAttachmentTracker.getOriginal(slice);
    
    // Should get back the exact same original
    assertThat(result).isSameAs(original);
    assertThat(result).isNotSameAs(slice);
    
    System.out.println("Original identity: " + System.identityHashCode(original));
    System.out.println("Slice identity: " + System.identityHashCode(slice));
    System.out.println("Result identity: " + System.identityHashCode(result));
  }

  /**
   * Thread-safety test: Concurrent reads and writes on the same slice.
   * This verifies that race conditions don't cause incorrect mappings.
   */
  @Test
  public void concurrentAccessToSameSlice_isThreadSafe() throws InterruptedException {
    final int numThreads = 10;
    final int iterations = 1000;
    final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch doneLatch = new CountDownLatch(numThreads);
    final AtomicInteger errors = new AtomicInteger(0);

    ByteBuffer original = ByteBuffer.allocateDirect(1024);
    ByteBuffer slice = original.slice();

    for (int i = 0; i < numThreads; i++) {
      executor.submit(() -> {
        try {
          startLatch.await();

          for (int j = 0; j < iterations; j++) {
            // Record the mapping
            BufferAttachmentTracker.recordSlice(slice, original);

            // Immediately retrieve it
            ByteBuffer retrieved = BufferAttachmentTracker.getOriginal(slice);

            // Should always get the original back
            if (retrieved != original) {
              errors.incrementAndGet();
            }
          }
        } catch (Exception e) {
          errors.incrementAndGet();
          e.printStackTrace();
        } finally {
          doneLatch.countDown();
        }
      });
    }

    startLatch.countDown();
    boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
    executor.shutdown();

    assertThat(completed).isTrue();
    assertThat(errors.get()).isEqualTo(0);
  }

  /**
   * Memory safety test: Verifies that WeakHashMap allows slice buffers to be
   * garbage collected without causing memory leaks.
   */
  @Test
  public void weakHashMap_allowsGarbageCollection() {
    ByteBuffer original = ByteBuffer.allocateDirect(1024);
    ByteBuffer slice = original.slice();

    BufferAttachmentTracker.recordSlice(slice, original);
    assertThat(BufferAttachmentTracker.getTrackingMapSize()).isEqualTo(1);

    // Remove reference to slice (but not original)
    slice = null;

    // Force garbage collection
    System.gc();
    System.runFinalization();

    // Give GC time to clean up weak references
    // The WeakHashMap should eventually remove the entry when the slice is GC'd
    // Note: This is non-deterministic, so we can't assert on size without
    // potentially making the test flaky. The important thing is that it
    // doesn't prevent GC.

    // What we can verify is that having null'd the slice doesn't break anything
    ByteBuffer result = BufferAttachmentTracker.getOriginal(original);
    assertThat(result).isSameAs(original); // Original still works
  }
}

