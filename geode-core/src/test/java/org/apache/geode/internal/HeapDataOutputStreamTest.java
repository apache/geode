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
package org.apache.geode.internal.serialization;

import static org.apache.geode.internal.serialization.HeapDataOutputStream.SMALLEST_CHUNK_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class HeapDataOutputStreamTest {

  @Test
  public void shouldBeMockable() throws Exception {
    HeapDataOutputStream mockHeapDataOutputStream = mock(HeapDataOutputStream.class);
    Version mockVersion = mock(Version.class);
    when(mockHeapDataOutputStream.getVersion()).thenReturn(mockVersion);
    assertThat(mockHeapDataOutputStream.getVersion()).isEqualTo(mockVersion);
  }

  @Test
  public void toByteBufferWithStartPositionAndNoChunksReturnsCorrectByteBuffer()
      throws IOException {
    HeapDataOutputStream heapDataOutputStream = new HeapDataOutputStream(SMALLEST_CHUNK_SIZE, null);
    heapDataOutputStream.write(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

    ByteBuffer result = heapDataOutputStream.toByteBuffer(9);

    assertThat(result.remaining()).isEqualTo(1);
    assertThat(result.get(0)).isEqualTo((byte) 10);
  }

  @Test
  public void toByteBufferWithStartPositionAndChunksReturnsCorrectByteBuffer() throws IOException {
    HeapDataOutputStream heapDataOutputStream = new HeapDataOutputStream(SMALLEST_CHUNK_SIZE, null);
    heapDataOutputStream.write(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    byte[] chunk = new byte[SMALLEST_CHUNK_SIZE];
    for (byte i = 0; i < SMALLEST_CHUNK_SIZE; i++) {
      chunk[i] = i;
    }
    heapDataOutputStream.write(chunk);

    ByteBuffer result = heapDataOutputStream.toByteBuffer(9);

    assertThat(result.remaining()).isEqualTo(SMALLEST_CHUNK_SIZE + 1);
    assertThat(result.get(0)).isEqualTo((byte) 10);
    for (byte i = 0; i < SMALLEST_CHUNK_SIZE; i++) {
      assertThat(result.get(i + 1)).isEqualTo(i);
    }
  }
}
