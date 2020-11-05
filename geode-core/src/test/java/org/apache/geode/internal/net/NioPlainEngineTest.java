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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.distributed.internal.DMStats;

public class NioPlainEngineTest {

  private DMStats mockStats;
  private NioPlainEngine nioEngine;
  private BufferPool bufferPool;

  @Before
  public void setUp() throws Exception {
    mockStats = mock(DMStats.class);
    bufferPool = new BufferPool(mockStats);
    nioEngine = new NioPlainEngine(bufferPool);
  }

  @Test
  public void unwrap() {
    ByteBuffer buffer = ByteBuffer.allocate(100);
    buffer.position(0).limit(buffer.capacity());
    nioEngine.unwrap(buffer);
    assertThat(buffer.position()).isEqualTo(buffer.limit());
  }

  @Test
  public void ensureWrappedCapacity() {
    ByteBuffer wrappedBuffer = bufferPool.acquireDirectReceiveBuffer(100);
    wrappedBuffer.put(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    nioEngine.lastReadPosition = 10;
    int requestedCapacity = 210;
    ByteBuffer result = nioEngine.ensureWrappedCapacity(requestedCapacity, wrappedBuffer,
        BufferPool.BufferType.TRACKED_RECEIVER);
    verify(mockStats, times(2)).incReceiverBufferSize(any(Integer.class), any(Boolean.class));
    assertThat(result.capacity()).isGreaterThanOrEqualTo(requestedCapacity);
    assertThat(result).isNotSameAs(wrappedBuffer);
    // make sure that data was transferred to the new buffer
    for (int i = 0; i < 10; i++) {
      assertThat(result.get(i)).isEqualTo(wrappedBuffer.get(i));
    }
  }

  @Test
  public void ensureWrappedCapacityWithEnoughExistingCapacityAndConsumedDataPresent() {
    int requestedCapacity = 210;
    final int consumedDataPresentInBuffer = 100;
    final int unconsumedDataPresentInBuffer = 10;
    // the buffer will have enough capacity but will need to be compacted
    ByteBuffer wrappedBuffer =
        ByteBuffer.allocate(requestedCapacity + unconsumedDataPresentInBuffer);
    wrappedBuffer.put(new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
    nioEngine.lastProcessedPosition = consumedDataPresentInBuffer;
    // previous read left 10 bytes
    nioEngine.lastReadPosition = consumedDataPresentInBuffer + unconsumedDataPresentInBuffer;
    ByteBuffer result =
        wrappedBuffer = nioEngine.ensureWrappedCapacity(requestedCapacity, wrappedBuffer,
            BufferPool.BufferType.UNTRACKED);
    assertThat(result.capacity()).isEqualTo(requestedCapacity + unconsumedDataPresentInBuffer);
    assertThat(result).isSameAs(wrappedBuffer);
    // make sure that data was transferred to the new buffer
    for (int i = 0; i < 10; i++) {
      assertThat(result.get(i)).isEqualTo(wrappedBuffer.get(i));
    }
    assertThat(nioEngine.lastProcessedPosition).isEqualTo(0);
    assertThat(nioEngine.lastReadPosition).isEqualTo(10);
  }

  @Test
  public void readAtLeast() throws Exception {
    final int amountToRead = 150;
    final int individualRead = 60;
    final int preexistingBytes = 10;
    ByteBuffer wrappedBuffer = ByteBuffer.allocate(1000);
    SocketChannel mockChannel = mock(SocketChannel.class);

    // simulate some socket reads
    when(mockChannel.read(any(ByteBuffer.class))).thenAnswer(new Answer<Integer>() {
      @Override
      public Integer answer(InvocationOnMock invocation) throws Throwable {
        ByteBuffer buffer = invocation.getArgument(0);
        buffer.position(buffer.position() + individualRead);
        return individualRead;
      }
    });

    nioEngine.lastReadPosition = 10;

    ByteBuffer data = nioEngine.readAtLeast(mockChannel, amountToRead, wrappedBuffer);
    verify(mockChannel, times(3)).read(isA(ByteBuffer.class));
    assertThat(data.position()).isEqualTo(0);
    assertThat(data.limit()).isEqualTo(amountToRead);
    assertThat(nioEngine.lastReadPosition).isEqualTo(individualRead * 3 + preexistingBytes);
    assertThat(nioEngine.lastProcessedPosition).isEqualTo(amountToRead);

    data = nioEngine.readAtLeast(mockChannel, amountToRead, wrappedBuffer);
    verify(mockChannel, times(5)).read(any(ByteBuffer.class));
    // at end of last readAtLeast data
    assertThat(data.position()).isEqualTo(amountToRead);
    // we read amountToRead bytes
    assertThat(data.limit()).isEqualTo(amountToRead * 2);
    // we did 2 more reads from the network
    assertThat(nioEngine.lastReadPosition).isEqualTo(individualRead * 5 + preexistingBytes);
    // the next read will start at the end of consumed data
    assertThat(nioEngine.lastProcessedPosition).isEqualTo(amountToRead * 2);

  }

  @Test(expected = EOFException.class)
  public void readAtLeastThrowsEOFException() throws Exception {
    final int amountToRead = 150;
    ByteBuffer wrappedBuffer = ByteBuffer.allocate(1000);
    SocketChannel mockChannel = mock(SocketChannel.class);

    // simulate some socket reads
    when(mockChannel.read(any(ByteBuffer.class))).thenReturn(-1);

    nioEngine.lastReadPosition = 10;

    nioEngine.readAtLeast(mockChannel, amountToRead, wrappedBuffer);
  }

}
