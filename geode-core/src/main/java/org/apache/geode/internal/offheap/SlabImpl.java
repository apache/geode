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
package org.apache.geode.internal.offheap;

/**
 * Implements the Slab interface using AddressableMemoryManager.
 *
 * @since Geode 1.0
 */
public class SlabImpl implements Slab {
  private final long address;
  private final int size;

  public SlabImpl(int size) {
    this(size, false);
  }

  /**
   * This constructor is used by unit test to add padding to prevent intermittent combining of
   * fragments during defragmentation.
   */
  public SlabImpl(int size, boolean withPadding) {
    this(AddressableMemoryManager.allocate(size + (withPadding ? 4 : 0)), size);
  }

  public SlabImpl(long addr, int size) {
    address = addr;
    this.size = size;
  }

  @Override
  public int getSize() {
    return size;
  }

  @Override
  public long getMemoryAddress() {
    return address;
  }

  @Override
  public void free() {
    AddressableMemoryManager.free(address);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append("{");
    sb.append("MemoryAddress=").append(getMemoryAddress());
    sb.append(", Size=").append(getSize());
    sb.append("}");
    return sb.toString();
  }
}
