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

package org.apache.geode.internal.tcp;

import static org.apache.geode.internal.tcp.BufferDebuggingCircularBuffer.fastForwardOffset;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

public class BufferDebuggingCircularBufferTest {

  @Test
  public void testFirstPutGet() {
    final BufferDebuggingCircularBuffer c =
        new BufferDebuggingCircularBuffer(5);
    putBytes(asBytes(1, 2, 3), c);
    final byte[] outBytes = getBytes(c, 3);
    assertThat(outBytes).isEqualTo(asBytes(1, 2, 3));
  }

  @Test
  public void testFillThenGet() {
    final BufferDebuggingCircularBuffer c =
        new BufferDebuggingCircularBuffer(3);
    putBytes(asBytes(1, 2, 3), c);
    final byte[] outBytes = getBytes(c, 3);
    assertThat(outBytes).isEqualTo(asBytes(1, 2, 3));
  }

  @Test
  public void testWrapThenGet() {
    final BufferDebuggingCircularBuffer c =
        new BufferDebuggingCircularBuffer(2);
    putBytes(asBytes(1, 2, 3), c);
    final byte[] outBytes1 = getBytes(c, 2);
    assertThat(outBytes1).isEqualTo(asBytes(2, 3));
  }

  @Test
  public void testFillThenWrapThenGet() {
    final BufferDebuggingCircularBuffer c =
        new BufferDebuggingCircularBuffer(3);
    putBytes(asBytes(1, 2, 3), c);
    putBytes(asBytes(4), c);
    final byte[] outBytes = getBytes(c, 3);
    assertThat(outBytes).isEqualTo(asBytes(2, 3, 4));
  }

  @NotNull
  private byte[] getBytes(final BufferDebuggingCircularBuffer c, final int n) {
    final byte[] outBytes = new byte[n];
    c.get(outBytes, 0, n);
    return outBytes;
  }

  private void putBytes(final byte[] inBytes, final BufferDebuggingCircularBuffer c) {
    final ByteBuffer inBuf = ByteBuffer.wrap(inBytes);
    c.put(inBuf);
  }

  private byte[] asBytes(final int... ints) {
    final byte[] bytes = new byte[ints.length];
    for (int i = 0; i < ints.length; ++i) {
      bytes[i] = (byte) ints[i];
    }
    return bytes;
  }

  @Test
  public void fastForwardOffsetTest() {
    assertThat(fastForwardOffset(0, 0)).isEqualTo(0);
    assertThat(fastForwardOffset(0, 1)).isEqualTo(0);
    assertThat(fastForwardOffset(1, 0)).isEqualTo(0);
    assertThat(fastForwardOffset(1, 1)).isEqualTo(0);

    assertThat(fastForwardOffset(5, 3)).isEqualTo(2);
    assertThat(fastForwardOffset(5, 3)).isEqualTo(2);

    assertThat(fastForwardOffset(3, 5)).isEqualTo(0);
  }
}
