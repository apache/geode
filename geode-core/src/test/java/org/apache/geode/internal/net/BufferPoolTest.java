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
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.internal.DMStats;

public class BufferPoolTest {

  private BufferPool bufferPool;

  @Before
  public void setup() {
    bufferPool = new BufferPool(mock(DMStats.class));
  }

  @Test
  public void expandBuffer() throws Exception {
    ByteBuffer buffer = ByteBuffer.allocate(256);
    buffer.clear();
    for (int i = 0; i < 256; i++) {
      byte b = (byte) (i & 0xff);
      buffer.put(b);
    }
    createAndVerifyNewWriteBuffer(buffer, false);

    createAndVerifyNewWriteBuffer(buffer, true);


    createAndVerifyNewReadBuffer(buffer, false);

    createAndVerifyNewReadBuffer(buffer, true);


  }

  private void createAndVerifyNewWriteBuffer(ByteBuffer buffer, boolean useDirectBuffer) {
    buffer.position(buffer.capacity());
    ByteBuffer newBuffer =
        bufferPool.expandWriteBufferIfNeeded(BufferPool.BufferType.UNTRACKED, buffer, 500);
    assertEquals(buffer.position(), newBuffer.position());
    assertEquals(500, newBuffer.capacity());
    newBuffer.flip();
    for (int i = 0; i < 256; i++) {
      byte expected = (byte) (i & 0xff);
      byte actual = (byte) (newBuffer.get() & 0xff);
      assertEquals(expected, actual);
    }
  }

  private void createAndVerifyNewReadBuffer(ByteBuffer buffer, boolean useDirectBuffer) {
    buffer.position(0);
    buffer.limit(256);
    ByteBuffer newBuffer =
        bufferPool.expandReadBufferIfNeeded(BufferPool.BufferType.UNTRACKED, buffer, 500);
    assertEquals(0, newBuffer.position());
    assertEquals(500, newBuffer.capacity());
    for (int i = 0; i < 256; i++) {
      byte expected = (byte) (i & 0xff);
      byte actual = (byte) (newBuffer.get() & 0xff);
      assertEquals(expected, actual);
    }
  }


  // the fixed numbers in this test came from a distributed unit test failure
  @Test
  public void bufferPositionAndLimitForReadAreCorrectAfterExpansion() throws Exception {
    ByteBuffer buffer = ByteBuffer.allocate(33842);
    buffer.position(7);
    buffer.limit(16384);
    ByteBuffer newBuffer =
        bufferPool.expandReadBufferIfNeeded(BufferPool.BufferType.UNTRACKED, buffer,
            40899);
    assertThat(newBuffer.capacity()).isGreaterThanOrEqualTo(40899);
    // buffer should be ready to read the same amount of data
    assertThat(newBuffer.position()).isEqualTo(0);
    assertThat(newBuffer.limit()).isEqualTo(16384 - 7);
  }


  @Test
  public void bufferPositionAndLimitForWriteAreCorrectAfterExpansion() throws Exception {
    ByteBuffer buffer = ByteBuffer.allocate(33842);
    buffer.position(16384);
    buffer.limit(buffer.capacity());
    ByteBuffer newBuffer =
        bufferPool.expandWriteBufferIfNeeded(BufferPool.BufferType.UNTRACKED, buffer,
            40899);
    assertThat(newBuffer.capacity()).isGreaterThanOrEqualTo(40899);
    // buffer should have the same amount of data as the old one
    assertThat(newBuffer.position()).isEqualTo(16384);
    assertThat(newBuffer.limit()).isEqualTo(newBuffer.capacity());
  }

  @Test
  public void checkBufferSizeAfterAllocation() throws Exception {
    ByteBuffer buffer = bufferPool.acquireDirectReceiveBuffer(100);

    ByteBuffer newBuffer =
        bufferPool.acquireDirectReceiveBuffer(10000);
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
  public void checkBufferSizeAfterAcquire() throws Exception {
    ByteBuffer buffer = bufferPool.acquireDirectReceiveBuffer(100);

    ByteBuffer newBuffer =
        bufferPool.acquireDirectReceiveBuffer(10000);
    assertThat(buffer.capacity()).isEqualTo(100);
    assertThat(newBuffer.capacity()).isEqualTo(10000);
    assertThat(buffer.isDirect()).isTrue();
    assertThat(newBuffer.isDirect()).isTrue();

    assertThat(buffer.position()).isEqualTo(0);
    assertThat(buffer.limit()).isEqualTo(100);
    assertThat(newBuffer.position()).isEqualTo(0);
    assertThat(newBuffer.limit()).isEqualTo(10000);

    bufferPool.releaseReceiveBuffer(buffer);
    bufferPool.releaseReceiveBuffer(newBuffer);

    buffer = bufferPool.acquireDirectReceiveBuffer(1000);
    newBuffer =
        bufferPool.acquireDirectReceiveBuffer(15000);

    assertThat(buffer.capacity()).isEqualTo(1000);
    assertThat(newBuffer.capacity()).isEqualTo(15000);
    assertThat(buffer.isDirect()).isTrue();
    assertThat(newBuffer.isDirect()).isTrue();

    assertThat(buffer.position()).isEqualTo(0);
    assertThat(buffer.limit()).isEqualTo(1000);
    assertThat(newBuffer.position()).isEqualTo(0);
    assertThat(newBuffer.limit()).isEqualTo(15000);
  }

}
