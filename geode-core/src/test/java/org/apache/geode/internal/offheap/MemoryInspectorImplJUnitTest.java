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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;


public class MemoryInspectorImplJUnitTest {

  private FreeListManager freeList;
  private MemoryInspector inspector;

  @Before
  public void setUp() {
    freeList = mock(FreeListManager.class);
    inspector = new MemoryInspectorImpl(freeList);
  }

  @Test
  public void getSnapshotBeforeCreateSnapshotReturnsEmptyList() {
    assertTrue(inspector.getSnapshot().isEmpty());
  }

  @Test
  public void getAllBlocksBeforeCreateSnapshotReturnsEmptyList() {
    assertTrue(inspector.getAllBlocks().isEmpty());
  }

  @Test
  public void getAllocatedBlocksBeforeCreateSnapshotReturnsEmptyList() {
    assertTrue(inspector.getAllocatedBlocks().isEmpty());
  }

  @Test
  public void getFirstBlockBeforeCreateSnapshotReturnsNull() {
    assertNull(inspector.getFirstBlock());
  }

  @Test
  public void getBlockAfterWithNullBeforeCreateSnapshotReturnsNull() {
    assertNull(inspector.getBlockAfter(null));
  }

  @Test
  public void getBlockAfterBeforeCreateSnapshotReturnsNull() {
    MemoryBlock block = mock(MemoryBlock.class);
    assertNull(inspector.getBlockAfter(block));
  }

  @Test
  public void canClearUncreatedSnapshot() {
    inspector.clearSnapshot();
    assertTrue(inspector.getSnapshot().isEmpty());
  }

  private void createSnapshot(List<MemoryBlock> memoryBlocks) {
    when(freeList.getOrderedBlocks()).thenReturn(memoryBlocks);
    inspector.createSnapshot();
  }

  @Test
  public void createSnapshotCallsGetOrderedBlocks() {
    List<MemoryBlock> emptyList = new ArrayList<MemoryBlock>();
    createSnapshot(emptyList);
    verify(freeList, times(1)).getOrderedBlocks();
    assertSame(emptyList, inspector.getSnapshot());
  }

  @Test
  public void createSnapshotIsIdempotent() {
    List<MemoryBlock> emptyList = new ArrayList<MemoryBlock>();
    createSnapshot(emptyList);
    when(freeList.getOrderedBlocks()).thenReturn(null);
    inspector.createSnapshot();
    verify(freeList, times(1)).getOrderedBlocks();
    assertSame(emptyList, inspector.getSnapshot());
  }

  @Test
  public void clearSnapshotAfterCreatingOneReturnsEmptyList() {
    List<MemoryBlock> emptyList = new ArrayList<MemoryBlock>();
    createSnapshot(emptyList);

    inspector.clearSnapshot();
    assertTrue(inspector.getSnapshot().isEmpty());
  }

  @Test
  public void getFirstBlockReturnsFirstBlockFromSnapshot() {
    List<MemoryBlock> fakeSnapshot = setupFakeSnapshot();

    assertSame(fakeSnapshot.get(0), inspector.getFirstBlock());
  }

  @Test
  public void getFirstBlockAfterReturnsCorrectBlock() {
    List<MemoryBlock> fakeSnapshot = setupFakeSnapshot();

    assertSame(fakeSnapshot.get(1), inspector.getBlockAfter(fakeSnapshot.get(0)));
  }

  @Test
  public void getFirstBlockAfterReturnsNullForLastBlock() {
    List<MemoryBlock> fakeSnapshot = setupFakeSnapshot();

    assertNull(inspector.getBlockAfter(fakeSnapshot.get(1)));
  }

  private List<MemoryBlock> setupFakeSnapshot() {
    MemoryBlock mock1 = mock(MemoryBlock.class);
    MemoryBlock mock2 = mock(MemoryBlock.class);
    List<MemoryBlock> memoryBlocks = new ArrayList<>();
    memoryBlocks.add(mock1);
    memoryBlocks.add(mock2);
    createSnapshot(memoryBlocks);
    return memoryBlocks;
  }
}
