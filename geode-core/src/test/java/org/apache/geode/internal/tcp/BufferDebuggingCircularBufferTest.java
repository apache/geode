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

import org.junit.Test;

public class BufferDebuggingCircularBufferTest {

  @Test
  public void testFirstPutGet() {
    final BufferDebuggingCircularBuffer c =
        new BufferDebuggingCircularBuffer(5);
    final byte[] inBytes = {0x01, 0x02, 0x03};
    final ByteBuffer inBuf = ByteBuffer.wrap(inBytes);
    c.put(inBuf);
    final byte[] outBytes = new byte[3];
    c.get(outBytes, 0, 3);
    assertThat(outBytes).isEqualTo(inBytes);
  }

  @Test
  public void testFillThenGet() {
    final BufferDebuggingCircularBuffer c =
        new BufferDebuggingCircularBuffer(3);
    final byte[] inBytes = {0x01, 0x02, 0x03};
    final ByteBuffer inBuf = ByteBuffer.wrap(inBytes);
    c.put(inBuf);
    final byte[] outBytes = new byte[3];
    c.get(outBytes, 0, 3);
    assertThat(outBytes).isEqualTo(inBytes);
  }

  @Test
  public void testFillThenWrapThenGet() {
    final BufferDebuggingCircularBuffer c =
        new BufferDebuggingCircularBuffer(3);
    final byte[] inBytes = {0x01, 0x02, 0x03};
    final ByteBuffer inBuf = ByteBuffer.wrap(inBytes);
    c.put(inBuf);
    final byte[] inBytes2 = {0x04};
    final ByteBuffer inBuf2 = ByteBuffer.wrap(inBytes2);
    c.put(inBuf2);
    final byte[] outBytes = new byte[3];
    c.get(outBytes, 0, 3);
    final byte[] expect = {0x02, 0x03, 0x04};
    assertThat(outBytes).isEqualTo(expect);
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
