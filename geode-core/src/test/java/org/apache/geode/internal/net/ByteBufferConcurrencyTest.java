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

import static java.lang.Thread.sleep;
import static org.apache.geode.internal.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.test.concurrency.ConcurrentTestRunner;
import org.apache.geode.test.concurrency.ParallelExecutor;
import org.apache.geode.test.concurrency.RunnableWithException;
import org.apache.geode.test.concurrency.loop.LoopRunnerConfig;

@RunWith(ConcurrentTestRunner.class)
@LoopRunnerConfig(count = 100)
public class ByteBufferConcurrencyTest {

  /*
   * Milliseconds to hold onto the buffer for, when simulating application access.
   */
  public static final int USE_BUFFER_FOR_MILLIS = 2;
  public static final int PARALLEL_TASK_COUNT = 10;

  @Test
  public void concurrentDestructAndOpenCloseShouldReturnToPoolOnce(final ParallelExecutor executor)
      throws Exception {
    final BufferPool poolMock = mock(BufferPool.class);
    final ByteBuffer someBuffer = ByteBuffer.allocate(1);
    final ByteBufferVendor vendor =
        new ByteBufferVendor(someBuffer, BufferPool.BufferType.TRACKED_SENDER,
            poolMock);

    final RunnableWithException useBuffer = () -> {
      try (final ByteBufferSharing sharing = vendor.open()) {
        useBuffer(sharing);
      } catch (IOException e) {
        // It's ok to get an IOException if the sharing was destruct()ed before this runs
      }
    };

    for (int i = 0; i < PARALLEL_TASK_COUNT; ++i) {
      executor.inParallel(useBuffer);
    }
    executor.inParallel(() -> {
      vendor.destruct();
    });
    for (int i = 0; i < PARALLEL_TASK_COUNT; ++i) {
      executor.inParallel(useBuffer);
    }

    executor.execute();

    verify(poolMock, times(1)).releaseBuffer(any(), any());
  }

  private void useBuffer(final ByteBufferSharing sharing) throws IOException, InterruptedException {
    sharing.getBuffer();
    /*
     * Give up the thread, in an attempt to maximize the probability that
     * a competing task might try to access this buffer now.
     */
    sleep(USE_BUFFER_FOR_MILLIS);
  }

  @Test
  public void concurrentDestructAndOpenShouldNotAllowUseOfReturnedBuffer(
      final ParallelExecutor executor)
      throws Exception {
    final BufferPool poolMock = mock(BufferPool.class);
    final AtomicBoolean returned = new AtomicBoolean(false);
    doAnswer(arguments -> {
      returned.set(true);
      return null;
    }).when(poolMock).releaseBuffer(any(), any());

    final ByteBuffer someBuffer = ByteBuffer.allocate(1);
    final ByteBufferVendor vendor =
        new ByteBufferVendor(someBuffer, BufferPool.BufferType.TRACKED_SENDER,
            poolMock);

    final RunnableWithException accessBufferAndVerify = () -> {
      try {
        try (final ByteBufferSharing sharing = vendor.open()) {
          useBuffer(sharing);
          // The above buffer should not have been returned to the pool at this point!
          assertFalse(returned.get());
        }
      } catch (IOException e) {
        // It's ok to get an IOException if the sharing was destruct()ed before this runs
      }
    };

    for (int i = 0; i < PARALLEL_TASK_COUNT; ++i) {
      executor.inParallel(accessBufferAndVerify);
    }
    executor.inParallel(() -> {
      vendor.destruct();
    });
    for (int i = 0; i < PARALLEL_TASK_COUNT; ++i) {
      executor.inParallel(accessBufferAndVerify);
    }

    executor.execute();

    verify(poolMock, times(1)).releaseBuffer(any(), any());
  }

  // Exclusive access test
  @Test
  public void concurrentAccessToSharingShouldBeExclusive(final ParallelExecutor executor)
      throws Exception {
    final BufferPool poolMock = mock(BufferPool.class);
    final ByteBuffer someBuffer = ByteBuffer.allocate(1);
    final ByteBufferVendor vendor =
        new ByteBufferVendor(someBuffer, BufferPool.BufferType.TRACKED_SENDER,
            poolMock);

    final AtomicBoolean inUse = new AtomicBoolean(false);

    final RunnableWithException useBufferAndCheckAccess = () -> {
      try (final ByteBufferSharing sharing = vendor.open()) {
        assertFalse(inUse.getAndSet(true));
        useBuffer(sharing);
        assertTrue(inUse.getAndSet(false));
      }
    };

    for (int i = 0; i < PARALLEL_TASK_COUNT; ++i) {
      executor.inParallel(useBufferAndCheckAccess);
    }
    executor.execute();
    verify(poolMock, times(0)).releaseBuffer(any(), any());
    vendor.destruct();
    verify(poolMock, times(1)).releaseBuffer(any(), any());
  }

  @Test
  public void concurrentAccessToSharingShouldBeExclusiveWithExtraCloses(
      final ParallelExecutor executor)
      throws Exception {
    final BufferPool poolMock = mock(BufferPool.class);
    final ByteBuffer someBuffer = ByteBuffer.allocate(1);
    final ByteBufferVendor vendor =
        new ByteBufferVendor(someBuffer, BufferPool.BufferType.TRACKED_SENDER,
            poolMock);

    final AtomicBoolean inUse = new AtomicBoolean(false);
    final RunnableWithException useBufferAndCheckAccess = () -> {
      Assertions.assertThatThrownBy(() -> {
        try (final ByteBufferSharing sharing = vendor.open()) {
          assertFalse(inUse.getAndSet(true));
          useBuffer(sharing);
          assertTrue(inUse.getAndSet(false));
          sharing.close(); // extra close() is what we're testing
        }
      }).isInstanceOf(IllegalMonitorStateException.class);
    };
    for (int i = 0; i < PARALLEL_TASK_COUNT; ++i) {
      executor.inParallel(useBufferAndCheckAccess);
    }
    executor.execute();

    verify(poolMock, times(0)).releaseBuffer(any(), any());
    vendor.destruct();
    verify(poolMock, times(1)).releaseBuffer(any(), any());
  }

}
