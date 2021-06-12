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

  private BufferPool poolMock;

  @Test
  public void concurrentDestructAndOpenCloseShouldReturnToPoolOnce(ParallelExecutor executor)
      throws Exception {
    poolMock = mock(BufferPool.class);
    ByteBuffer someBuffer = ByteBuffer.allocate(1);
    ByteBufferVendor sharing =
        new ByteBufferVendor(someBuffer, BufferPool.BufferType.TRACKED_SENDER,
            poolMock);
    executor.inParallel(() -> {
      sharing.destruct();
    });
    executor.inParallel(() -> {
      try {
        try (ByteBufferSharing localSharing = sharing.open()) {
          localSharing.getBuffer();
        }
      } catch (IOException e) {
        // It's ok to get an IOException if the sharing was destroyed before this runs
      }
    });
    executor.execute();

    verify(poolMock, times(1)).releaseBuffer(any(), any());
  }

  @Test
  public void concurrentDestructAndOpenShouldNotAllowUseOfReturnedBuffer(ParallelExecutor executor)
      throws Exception {
    poolMock = mock(BufferPool.class);
    AtomicBoolean returned = new AtomicBoolean(false);
    doAnswer(arguments -> {
      returned.set(true);
      return null;
    }).when(poolMock).releaseBuffer(any(), any());

    ByteBuffer someBuffer = ByteBuffer.allocate(1);
    ByteBufferVendor sharing =
        new ByteBufferVendor(someBuffer, BufferPool.BufferType.TRACKED_SENDER,
            poolMock);

    executor.inParallel(() -> {
      sharing.destruct();
    });

    executor.inParallel(() -> {
      try {
        try (ByteBufferSharing localSharing = sharing.open()) {
          ByteBuffer buffer = localSharing.getBuffer();

          // The above buffer should not have been returned to the pool at this point!
          assertFalse(returned.get());
        }
      } catch (IOException e) {
        // It's ok to get an IOException if the sharing was destroyed before this runs
      }
    });
    executor.execute();

    verify(poolMock, times(1)).releaseBuffer(any(), any());
  }

  // Exclusive access test
  @Test
  public void concurrentAccessToSharingShouldBeExclusive(ParallelExecutor executor)
      throws Exception {
    poolMock = mock(BufferPool.class);
    ByteBuffer someBuffer = ByteBuffer.allocate(1);
    ByteBufferVendor sharing =
        new ByteBufferVendor(someBuffer, BufferPool.BufferType.TRACKED_SENDER,
            poolMock);

    final AtomicBoolean inUse = new AtomicBoolean(false);
    final RunnableWithException useBufferAndCheckAccess = () -> {
      try (ByteBufferSharing localSharing = sharing.open()) {
        assertFalse(inUse.getAndSet(true));
        localSharing.getBuffer();
        assertTrue(inUse.getAndSet(false));
      }
    };
    executor.inParallel(useBufferAndCheckAccess);
    executor.inParallel(useBufferAndCheckAccess);
    executor.execute();
    verify(poolMock, times(0)).releaseBuffer(any(), any());
    sharing.destruct();
    verify(poolMock, times(1)).releaseBuffer(any(), any());
  }

  @Test
  public void concurrentAccessToSharingShouldBeExclusiveWithExtraCloses(ParallelExecutor executor)
      throws Exception {
    poolMock = mock(BufferPool.class);
    ByteBuffer someBuffer = ByteBuffer.allocate(1);
    ByteBufferVendor sharing =
        new ByteBufferVendor(someBuffer, BufferPool.BufferType.TRACKED_SENDER,
            poolMock);

    final AtomicBoolean inUse = new AtomicBoolean(false);
    final RunnableWithException useBufferAndCheckAccess = () -> {
      Assertions.assertThatThrownBy(() -> {
        try (ByteBufferSharing localSharing = sharing.open()) {
          assertFalse(inUse.getAndSet(true));
          localSharing.getBuffer();
          assertTrue(inUse.getAndSet(false));
          localSharing.close();
        }
      }).isInstanceOf(IllegalMonitorStateException.class);
    };
    executor.inParallel(useBufferAndCheckAccess);
    executor.inParallel(useBufferAndCheckAccess);
    executor.inParallel(useBufferAndCheckAccess);
    executor.execute();


    verify(poolMock, times(0)).releaseBuffer(any(), any());
    sharing.destruct();
    verify(poolMock, times(1)).releaseBuffer(any(), any());
  }


  // Extra closes with concurrent access?
}
