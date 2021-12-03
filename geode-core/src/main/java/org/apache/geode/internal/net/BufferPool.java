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

import java.nio.ByteBuffer;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.tcp.Connection;
import org.apache.geode.util.internal.GeodeGlossary;

public interface BufferPool {
  @VisibleForTesting
  int SMALL_BUFFER_SIZE = Connection.SMALL_BUFFER_SIZE;
  @VisibleForTesting
  int MEDIUM_BUFFER_SIZE = DistributionConfig.DEFAULT_SOCKET_BUFFER_SIZE;
  /**
   * use direct ByteBuffers instead of heap ByteBuffers for NIO operations
   */
  boolean useDirectBuffers = !Boolean.getBoolean("p2p.nodirectBuffers")
      || Boolean.getBoolean(GeodeGlossary.GEMFIRE_PREFIX + "BufferPool.useHeapBuffers");

  ByteBuffer acquireDirectSenderBuffer(int size);

  ByteBuffer acquireDirectReceiveBuffer(int size);

  ByteBuffer acquireNonDirectSenderBuffer(int size);

  ByteBuffer acquireNonDirectReceiveBuffer(int size);

  void releaseSenderBuffer(ByteBuffer bb);

  void releaseReceiveBuffer(ByteBuffer bb);

  ByteBuffer expandReadBufferIfNeeded(BufferType type, ByteBuffer existing,
      int desiredCapacity);

  ByteBuffer expandWriteBufferIfNeeded(BufferType type, ByteBuffer existing,
      int desiredCapacity);

  ByteBuffer acquireDirectBuffer(BufferType type, int capacity);

  void releaseBuffer(BufferType type, @NotNull ByteBuffer buffer);

  @VisibleForTesting
  ByteBuffer getPoolableBuffer(ByteBuffer buffer);

  /**
   * Buffers may be acquired from the Buffers pool or they may be allocated using Buffer.allocate().
   * This enum is used to note the different types. Tracked buffers come from the Buffers pool and
   * need to be released when we're done using them.
   */
  public enum BufferType {
    UNTRACKED, TRACKED_SENDER, TRACKED_RECEIVER
  }
}
