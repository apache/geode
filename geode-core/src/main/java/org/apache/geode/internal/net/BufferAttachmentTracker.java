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
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Tracks the relationship between sliced ByteBuffers and their original parent buffers.
 * This replaces the need to access internal JDK implementation classes, using only
 * public Java APIs instead.
 *
 * When ByteBuffer.slice() is called, it creates a new buffer that shares content with
 * the original. We need to track this relationship so that when returning buffers to
 * the pool, we return the original pooled buffer, not the slice.
 *
 * This class uses WeakHashMap so that slice buffers can be garbage collected when no
 * longer referenced, without preventing cleanup of the tracking entry.
 */
class BufferAttachmentTracker {

  /**
   * Maps sliced buffers to their original parent buffers.
   * Uses WeakHashMap with slice buffer as key so entries are automatically
   * cleaned up when the slice is no longer referenced.
   */
  private static final Map<ByteBuffer, ByteBuffer> sliceToOriginal =
      new WeakHashMap<>();

  /**
   * Records that a slice buffer was created from an original buffer.
   *
   * @param slice the sliced ByteBuffer
   * @param original the original ByteBuffer that was sliced
   */
  static synchronized void recordSlice(ByteBuffer slice, ByteBuffer original) {
    sliceToOriginal.put(slice, original);
  }

  /**
   * Retrieves the original buffer for a given buffer, which may be a slice.
   * If the buffer is not a slice (not tracked), returns the buffer itself.
   *
   * @param buffer the buffer to look up, which may be a slice
   * @return the original pooled buffer, or the buffer itself if not a slice
   */
  static synchronized ByteBuffer getOriginal(ByteBuffer buffer) {
    ByteBuffer original = sliceToOriginal.get(buffer);
    return original != null ? original : buffer;
  }

  /**
   * Removes tracking for a buffer. Should be called when returning a buffer
   * to the pool to avoid memory leaks in the tracking map.
   *
   * @param buffer the buffer to stop tracking
   */
  static synchronized void removeTracking(ByteBuffer buffer) {
    sliceToOriginal.remove(buffer);
  }

  /**
   * For testing: returns the current size of the tracking map.
   */
  static synchronized int getTrackingMapSize() {
    return sliceToOriginal.size();
  }

  /**
   * For testing: clears all tracking entries.
   */
  static synchronized void clearTracking() {
    sliceToOriginal.clear();
  }
}
