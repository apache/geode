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
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Tracks the relationship between sliced ByteBuffers and their original parent buffers.
 * This replaces the need to access internal JDK implementation classes, using only
 * public Java APIs instead.
 *
 * When ByteBuffer.slice() is called, it creates a new buffer that shares content with
 * the original. We need to track this relationship so that when returning buffers to
 * the pool, we return the original pooled buffer, not the slice.
 *
 * This class uses IdentityHashMap (synchronized) which provides thread-safe access
 * using object identity rather than equals(). This is critical because ByteBuffer.equals()
 * compares buffer content and can throw IndexOutOfBoundsException if buffer position/limit
 * is modified after being used as a map key. Callers must explicitly call removeTracking()
 * to clean up entries when buffers are returned to the pool.
 */
class BufferAttachmentTracker {

  /**
   * Maps sliced buffers to their original parent buffers using object identity.
   * Uses synchronized IdentityHashMap for thread-safe access without relying on
   * ByteBuffer.equals() or hashCode(), which can be problematic when buffer state changes.
   * Entries must be explicitly removed via removeTracking() to prevent memory leaks.
   *
   * Note: This static mutable field is intentionally designed for global buffer tracking
   * across the application. The PMD.StaticFieldsMustBeImmutable warning is suppressed
   * because:
   * 1. Mutable shared state is required to track buffer relationships across all threads
   * 2. IdentityHashMap uses object identity (==) avoiding equals()/hashCode() issues
   * 3. Collections.synchronizedMap provides thread-safe operations
   * 4. This is the most efficient design for this use case
   */
  @SuppressWarnings("PMD.StaticFieldsMustBeImmutable")
  private static final Map<ByteBuffer, ByteBuffer> sliceToOriginal =
      Collections.synchronizedMap(new IdentityHashMap<>());

  /**
   * Records that a slice buffer was created from an original buffer.
   *
   * @param slice the sliced ByteBuffer
   * @param original the original ByteBuffer that was sliced
   */
  static void recordSlice(ByteBuffer slice, ByteBuffer original) {
    sliceToOriginal.put(slice, original);
  }

  /**
   * Retrieves the original buffer for a given buffer, which may be a slice.
   * If the buffer is not a slice (not tracked), returns the buffer itself.
   *
   * @param buffer the buffer to look up, which may be a slice
   * @return the original pooled buffer, or the buffer itself if not a slice
   */
  static ByteBuffer getOriginal(ByteBuffer buffer) {
    ByteBuffer original = sliceToOriginal.get(buffer);
    return original != null ? original : buffer;
  }

  /**
   * Removes tracking for a buffer. Should be called when returning a buffer
   * to the pool to avoid memory leaks in the tracking map.
   *
   * @param buffer the buffer to stop tracking
   */
  static void removeTracking(ByteBuffer buffer) {
    sliceToOriginal.remove(buffer);
  }

  /**
   * For testing: returns the current size of the tracking map.
   */
  static int getTrackingMapSize() {
    return sliceToOriginal.size();
  }

  /**
   * For testing: clears all tracking entries.
   */
  static void clearTracking() {
    sliceToOriginal.clear();
  }
}
