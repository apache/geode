/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package org.apache.geode.internal.shared.unsafe;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;

import org.apache.geode.internal.shared.BufferAllocator;

/**
 * Generic implementation of {@link BufferAllocator} for direct ByteBuffers
 * using Java NIO API.
 */
public class DirectBufferAllocator extends BufferAllocator {

  /**
   * Overhead of allocation on off-heap memory is kept fixed at 8 even though
   * actual overhead will be dependent on the malloc implementation.
   */
  public static final int DIRECT_OBJECT_OVERHEAD = 8;

  /**
   * The owner of direct buffers that are stored in Regions and tracked in UMM.
   */
  public static final String DIRECT_STORE_OBJECT_OWNER =
      "GEODE_DIRECT_STORE_OBJECTS";

  public static final String DIRECT_STORE_DATA_FRAME_OUTPUT =
      "DIRECT_" + STORE_DATA_FRAME_OUTPUT;

  private static final DirectBufferAllocator globalInstance =
      new DirectBufferAllocator();

  private static volatile DirectBufferAllocator instance = globalInstance;

  public static DirectBufferAllocator instance() {
    return instance;
  }

  public DirectBufferAllocator initialize() {
    DirectBufferAllocator.setInstance(this);
    return this;
  }

  public static synchronized void setInstance(DirectBufferAllocator allocator) {
    instance = allocator;
  }

  public static synchronized void resetInstance() {
    instance = globalInstance;
  }

  protected DirectBufferAllocator() {}

  public RuntimeException lowMemoryException(String op, int required) {
    return new RuntimeException();
  }

  public void changeOwnerToStorage(ByteBuffer buffer, int capacity,
      BiConsumer<String, Object> changeOwner) {}

  @Override
  public ByteBuffer allocate(int size, String owner) {
    return allocateForStorage(size);
  }

  @Override
  public ByteBuffer allocateWithFallback(int size, String owner) {
    try {
      return allocateForStorage(size);
    } catch (RuntimeException re) {
      if (instance() != globalInstance) {
        return globalInstance.allocateForStorage(size);
      } else {
        throw re;
      }
    }
  }

  @Override
  public ByteBuffer allocateForStorage(int size) {
    ByteBuffer buffer = ByteBuffer.allocateDirect(size);
    return buffer;
  }

  @Override
  public void clearPostAllocate(ByteBuffer buffer) {
    // clear till the capacity and not limit since former will be a factor
    // of 8 and hence more efficient in Unsafe.setMemory
    fill(buffer, (byte) 0, 0, buffer.capacity());
  }

  @Override
  public Object baseObject(ByteBuffer buffer) {
    return null;
  }

  @Override
  public long baseOffset(ByteBuffer buffer) {
    return UnsafeHolder.getDirectBufferAddress(buffer);
  }

  @Override
  public ByteBuffer expand(ByteBuffer buffer, int required, String owner) {
    assert required > 0 : "expand: unexpected required = " + required;

    final int currentUsed = buffer.limit();
    if (currentUsed + required > buffer.capacity()) {
      final int newLength = BufferAllocator.expandedSize(currentUsed, required);
      final ByteBuffer newBuffer = ByteBuffer.allocateDirect(newLength)
          .order(buffer.order());
      buffer.rewind();
      newBuffer.put(buffer);
      UnsafeHolder.releaseDirectBuffer(buffer);
      newBuffer.rewind(); // position at start as per the contract of expand
      return newBuffer;
    } else {
      buffer.limit(currentUsed + required);
      return buffer;
    }
  }

  @Override
  public ByteBuffer fromBytesToStorage(byte[] bytes, int offset, int length) {
    final ByteBuffer buffer = allocateForStorage(length);
    buffer.put(bytes, offset, length);
    // move to the start
    buffer.rewind();
    return buffer;
  }

  @Override
  public ByteBuffer transfer(ByteBuffer buffer, String owner) {
    if (buffer.isDirect()) {
      return buffer;
    } else {
      return super.transfer(buffer, owner);
    }
  }

  @Override
  public boolean isDirect() {
    return true;
  }

  @Override
  public void close() {
    UnsafeHolder.releasePendingReferences();
  }
}
