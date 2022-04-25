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

import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.junitpioneer.jupiter.SetSystemProperty;

import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.internal.net.BufferPool.PooledByteBuffer;

public class BufferPoolTest {

  private BufferPool bufferPool;

  @BeforeEach
  public void setup() {
    bufferPool = new BufferPool(mock(DMStats.class));
  }

  @Test
  public void expandBuffer() {
    PooledByteBuffer pooledBuffer = new PooledByteBuffer(ByteBuffer.allocate(256));
    ByteBuffer buffer = pooledBuffer.getByteBuffer();
    buffer.clear();
    for (int i = 0; i < 256; i++) {
      byte b = (byte) (i & 0xff);
      buffer.put(b);
    }
    createAndVerifyNewWriteBuffer(pooledBuffer);

    createAndVerifyNewWriteBuffer(pooledBuffer);


    createAndVerifyNewReadBuffer(pooledBuffer);

    createAndVerifyNewReadBuffer(pooledBuffer);


  }

  private void createAndVerifyNewWriteBuffer(PooledByteBuffer pooledBuffer) {
    ByteBuffer buffer = pooledBuffer.getByteBuffer();
    buffer.position(buffer.capacity());
    PooledByteBuffer newPooledBuffer =
        bufferPool.expandWriteBufferIfNeeded(BufferPool.BufferType.UNTRACKED, pooledBuffer, 500);
    ByteBuffer newBuffer = newPooledBuffer.getByteBuffer();
    assertThat(newBuffer.position()).isEqualTo(buffer.position());
    assertThat(newBuffer.capacity()).isEqualTo(500);
    newBuffer.flip();
    for (int i = 0; i < 256; i++) {
      byte expected = (byte) (i & 0xff);
      byte actual = (byte) (newBuffer.get() & 0xff);
      assertThat(actual).isEqualTo(expected);
    }
  }

  private void createAndVerifyNewReadBuffer(PooledByteBuffer pooledBuffer) {
    ByteBuffer buffer = pooledBuffer.getByteBuffer();
    buffer.position(0);
    buffer.limit(256);
    PooledByteBuffer newPooledBuffer =
        bufferPool.expandReadBufferIfNeeded(BufferPool.BufferType.UNTRACKED, pooledBuffer, 500);
    ByteBuffer newBuffer = newPooledBuffer.getByteBuffer();
    assertThat(newBuffer.position()).isZero();
    assertThat(newBuffer.capacity()).isEqualTo(500);
    for (int i = 0; i < 256; i++) {
      byte expected = (byte) (i & 0xff);
      byte actual = (byte) (newBuffer.get() & 0xff);
      assertThat(actual).isEqualTo(expected);
    }
  }


  // the fixed numbers in this test came from a distributed unit test failure
  @Test
  public void bufferPositionAndLimitForReadAreCorrectAfterExpansion() {
    PooledByteBuffer pooledBuffer = new PooledByteBuffer(ByteBuffer.allocate(33842));
    ByteBuffer buffer = pooledBuffer.getByteBuffer();
    buffer.position(7);
    buffer.limit(16384);
    PooledByteBuffer newPooledBuffer =
        bufferPool.expandReadBufferIfNeeded(BufferPool.BufferType.UNTRACKED, pooledBuffer,
            40899);
    ByteBuffer newBuffer = newPooledBuffer.getByteBuffer();
    assertThat(newBuffer.capacity()).isGreaterThanOrEqualTo(40899);
    // buffer should be ready to read the same amount of data
    assertThat(newBuffer.position()).isEqualTo(0);
    assertThat(newBuffer.limit()).isEqualTo(16384 - 7);
  }


  @Test
  public void bufferPositionAndLimitForWriteAreCorrectAfterExpansion() {
    PooledByteBuffer pooledBuffer = new PooledByteBuffer(ByteBuffer.allocate(33842));
    ByteBuffer buffer = pooledBuffer.getByteBuffer();
    buffer.position(16384);
    buffer.limit(buffer.capacity());
    PooledByteBuffer newPooledBuffer =
        bufferPool.expandWriteBufferIfNeeded(BufferPool.BufferType.UNTRACKED, pooledBuffer,
            40899);
    ByteBuffer newBuffer = newPooledBuffer.getByteBuffer();
    assertThat(newBuffer.capacity()).isGreaterThanOrEqualTo(40899);
    // buffer should have the same amount of data as the old one
    assertThat(newBuffer.position()).isEqualTo(16384);
    assertThat(newBuffer.limit()).isEqualTo(newBuffer.capacity());
  }


  @Test
  public void checkBufferSizeAfterAllocation() {
    PooledByteBuffer pooledBuffer = bufferPool.acquireDirectReceiveBuffer(100);
    ByteBuffer buffer = pooledBuffer.getByteBuffer();

    PooledByteBuffer newPooledBuffer =
        bufferPool.acquireDirectReceiveBuffer(10000);
    ByteBuffer newBuffer = newPooledBuffer.getByteBuffer();

    assertThat(buffer.isDirect()).isTrue();
    assertThat(newBuffer.isDirect()).isTrue();
    assertThat(buffer.capacity()).isEqualTo(100);
    assertThat(newBuffer.capacity()).isEqualTo(10000);

    // buffer should be ready to read the same amount of data
    assertThat(buffer.position()).isEqualTo(0);
    assertThat(buffer.limit()).isEqualTo(100);
    assertThat(newBuffer.position()).isEqualTo(0);
    assertThat(newBuffer.limit()).isEqualTo(10000);
  }

  @Test
  public void checkBufferSizeAfterAcquire() {
    PooledByteBuffer pooledBuffer = bufferPool.acquireDirectReceiveBuffer(100);
    ByteBuffer buffer = pooledBuffer.getByteBuffer();
    PooledByteBuffer newPooledBuffer = bufferPool.acquireDirectReceiveBuffer(10000);
    ByteBuffer newBuffer = newPooledBuffer.getByteBuffer();

    assertThat(buffer.capacity()).isEqualTo(100);
    assertThat(newBuffer.capacity()).isEqualTo(10000);
    assertThat(buffer.isDirect()).isTrue();
    assertThat(newBuffer.isDirect()).isTrue();
    assertThat(pooledBuffer.getOriginal().capacity())
        .isGreaterThanOrEqualTo(BufferPool.SMALL_BUFFER_SIZE);
    assertThat(newPooledBuffer.getOriginal().capacity())
        .isGreaterThanOrEqualTo(BufferPool.MEDIUM_BUFFER_SIZE);

    assertThat(buffer.position()).isEqualTo(0);
    assertThat(buffer.limit()).isEqualTo(100);
    assertThat(newBuffer.position()).isEqualTo(0);
    assertThat(newBuffer.limit()).isEqualTo(10000);

    bufferPool.releaseReceiveBuffer(pooledBuffer);
    bufferPool.releaseReceiveBuffer(newPooledBuffer);

    pooledBuffer = bufferPool.acquireDirectReceiveBuffer(1000);
    buffer = pooledBuffer.getByteBuffer();
    newPooledBuffer = bufferPool.acquireDirectReceiveBuffer(15000);
    newBuffer = newPooledBuffer.getByteBuffer();

    assertThat(buffer.capacity()).isEqualTo(1000);
    assertThat(newBuffer.capacity()).isEqualTo(15000);
    assertThat(buffer.isDirect()).isTrue();
    assertThat(newBuffer.isDirect()).isTrue();
    assertThat(pooledBuffer.getOriginal().capacity())
        .isGreaterThanOrEqualTo(BufferPool.SMALL_BUFFER_SIZE);
    assertThat(newPooledBuffer.getOriginal().capacity())
        .isGreaterThanOrEqualTo(BufferPool.MEDIUM_BUFFER_SIZE);

    assertThat(buffer.position()).isEqualTo(0);
    assertThat(buffer.limit()).isEqualTo(1000);
    assertThat(newBuffer.position()).isEqualTo(0);
    assertThat(newBuffer.limit()).isEqualTo(15000);
  }

  private static final String P2P_NO_DIRECT_BUFFERS = "p2p.nodirectBuffers";
  private static final String USE_HEAP_BUFFERS = GEMFIRE_PREFIX + "BufferPool.useHeapBuffers";

  @Test
  @ClearSystemProperty(key = P2P_NO_DIRECT_BUFFERS)
  @ClearSystemProperty(key = USE_HEAP_BUFFERS)
  public void verifyDirectBuffersUsedByDefault() {
    assertThat(BufferPool.computeUseDirectBuffers()).isTrue();
  }

  @Test
  @SetSystemProperty(key = P2P_NO_DIRECT_BUFFERS, value = "false")
  @SetSystemProperty(key = USE_HEAP_BUFFERS, value = "false")
  public void verifyDirectBuffersUsedIfBothPropsFalse() {
    assertThat(BufferPool.computeUseDirectBuffers()).isTrue();
  }

  @Test
  @SetSystemProperty(key = P2P_NO_DIRECT_BUFFERS, value = "true")
  @ClearSystemProperty(key = USE_HEAP_BUFFERS)
  public void verifyDirectBuffersUnusedIfnodirectBuffersTrue() {
    assertThat(BufferPool.computeUseDirectBuffers()).isFalse();
  }

  @Test
  @SetSystemProperty(key = USE_HEAP_BUFFERS, value = "true")
  @ClearSystemProperty(key = P2P_NO_DIRECT_BUFFERS)
  public void verifyDirectBuffersUnusedIfuseHeapBuffersTrue() {
    assertThat(BufferPool.computeUseDirectBuffers()).isFalse();
  }

  @Test
  @SetSystemProperty(key = P2P_NO_DIRECT_BUFFERS, value = "true")
  @SetSystemProperty(key = USE_HEAP_BUFFERS, value = "true")
  public void verifyDirectBuffersUnusedIfBothPropsTrue() {
    assertThat(BufferPool.computeUseDirectBuffers()).isFalse();
  }

}
