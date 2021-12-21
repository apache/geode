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
 * Represents a slice of an OffHeapStoredObject. A slice is a subsequence of the bytes stored in an
 * OffHeapStoredObject.
 */
public class OffHeapStoredObjectSlice extends OffHeapStoredObject {
  private final int offset;
  private final int capacity;

  public OffHeapStoredObjectSlice(OffHeapStoredObject objectChunk, int position, int limit) {
    super(objectChunk);
    offset = objectChunk.getBaseDataOffset() + position;
    capacity = limit - position;
  }

  @Override
  public int getDataSize() {
    return capacity;
  }

  @Override
  protected long getBaseDataAddress() {
    return super.getBaseDataAddress() + offset;
  }

  @Override
  protected int getBaseDataOffset() {
    return offset;
  }
}
