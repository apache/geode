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

import java.io.IOException;
import java.nio.ByteBuffer;


/**
 * When a {@link ByteBufferSharing} is acquired in a try-with-resources the buffer is available (for
 * reading and modification) within the scope of that try block.
 *
 * Releases managed ByteBuffer back to pool after last reference is dropped.
 */
public interface ByteBufferSharing extends AutoCloseable {

  /**
   * Call this method only within a try-with-resource in which this {@link ByteBufferSharing} was
   * acquired. Retain the reference only within the scope of that try-with-resources.
   *
   * @return the buffer: manipulable only within the scope of the try-with-resources
   * @throws IOException if the buffer is no longer accessible
   */
  ByteBuffer getBuffer() throws IOException;

  /**
   * Expand the buffer if needed. This may return a different object so be sure to pay attention to
   * the return value if you need access to the potentially- expanded buffer.
   *
   * Subsequent calls to {@link #getBuffer()} will return that new buffer too.
   *
   * This variant is for use when the buffer is being written to.
   *
   * @return the same buffer or a different (bigger) buffer
   * @throws IOException if the buffer is no longer accessible
   */
  ByteBuffer expandWriteBufferIfNeeded(final int newCapacity) throws IOException;

  /**
   * Expand the buffer if needed. This may return a different object so be sure to pay attention to
   * the return value if you need access to the potentially- expanded buffer.
   *
   * Subsequent calls to {@link #getBuffer()} will return that new buffer too.
   *
   * This variant is for use when the buffer is being read from.
   *
   * @return the same buffer or a different (bigger) buffer
   * @throws IOException if the buffer is no longer accessible
   */
  ByteBuffer expandReadBufferIfNeeded(final int newCapacity) throws IOException;

  /**
   * Override {@link AutoCloseable#close()} without throws clause since we don't need one.
   */
  @Override
  void close();
}
