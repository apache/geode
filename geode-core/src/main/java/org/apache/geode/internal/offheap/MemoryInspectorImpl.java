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

import java.util.Collections;
import java.util.List;

/**
 * Implementation of MemoryInspector that provides for inspection of meta-data for off-heap memory
 * blocks
 */
public class MemoryInspectorImpl implements MemoryInspector {
  /** The inspection snapshot for MemoryInspector */
  private List<MemoryBlock> memoryBlocks;

  private final FreeListManager freeList;

  public MemoryInspectorImpl(FreeListManager freeList) {
    this.freeList = freeList;
  }

  @Override
  public synchronized void clearSnapshot() {
    memoryBlocks = null;
  }

  @Override
  public synchronized void createSnapshot() {
    List<MemoryBlock> value = memoryBlocks;
    if (value == null) {
      value = getOrderedBlocks();
      memoryBlocks = value;
    }
  }

  @Override
  public synchronized List<MemoryBlock> getSnapshot() {
    List<MemoryBlock> value = memoryBlocks;
    if (value == null) {
      return Collections.emptyList();
    } else {
      return value;
    }
  }


  @Override
  public MemoryBlock getFirstBlock() {
    final List<MemoryBlock> value = getSnapshot();
    if (value.isEmpty()) {
      return null;
    } else {
      return value.get(0);
    }
  }

  @Override
  public List<MemoryBlock> getAllBlocks() {
    return getOrderedBlocks();
  }

  @Override
  public List<MemoryBlock> getAllocatedBlocks() {
    return freeList.getAllocatedBlocks();
  }

  @Override
  public MemoryBlock getBlockAfter(MemoryBlock block) {
    if (block == null) {
      return null;
    }
    List<MemoryBlock> blocks = getSnapshot();
    int nextBlock = blocks.indexOf(block) + 1;
    if (nextBlock > 0 && blocks.size() > nextBlock) {
      return blocks.get(nextBlock);
    } else {
      return null;
    }
  }

  private List<MemoryBlock> getOrderedBlocks() {
    return freeList.getOrderedBlocks();
  }

}
