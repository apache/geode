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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.mockito.listeners.InvocationListener;
import org.mockito.listeners.MethodInvocationReport;


public class OffHeapStoredObjectAddressStackJUnitTest {

  @Test
  public void addressZeroCausesStackToBeEmpty() {
    OffHeapStoredObjectAddressStack stack = new OffHeapStoredObjectAddressStack(0L);
    assertEquals(true, stack.isEmpty());
  }

  @Test
  public void defaultStackIsEmpty() {
    OffHeapStoredObjectAddressStack stack = new OffHeapStoredObjectAddressStack();
    assertEquals(true, stack.isEmpty());
  }

  @Test
  public void defaultStackReturnsZeroFromTop() {
    OffHeapStoredObjectAddressStack stack = new OffHeapStoredObjectAddressStack();
    assertEquals(0L, stack.getTopAddress());
  }

  @Test
  public void defaultStackReturnsZeroFromPoll() {
    OffHeapStoredObjectAddressStack stack = new OffHeapStoredObjectAddressStack();
    assertEquals(0L, stack.poll());
  }

  @Test
  public void defaultStackReturnsZeroFromClear() {
    OffHeapStoredObjectAddressStack stack = new OffHeapStoredObjectAddressStack();
    assertEquals(0L, stack.clear());
    assertEquals(true, stack.isEmpty());
  }

  @Test
  public void defaultStackLogsNothing() {
    OffHeapStoredObjectAddressStack stack = new OffHeapStoredObjectAddressStack();
    Logger lw = mock(Logger.class, withSettings().invocationListeners(new InvocationListener() {
      @Override
      public void reportInvocation(MethodInvocationReport methodInvocationReport) {
        fail("Unexpected invocation");
      }
    }));
    stack.logSizes(lw, "should not be used");
  }

  @Test
  public void defaultStackComputeSizeIsZero() {
    OffHeapStoredObjectAddressStack stack = new OffHeapStoredObjectAddressStack();
    assertEquals(0L, stack.computeTotalSize());
  }

  @Test
  public void stackCreatedWithAddressIsNotEmpty() {
    SlabImpl slab = new SlabImpl(1024);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      OffHeapStoredObject chunk = (OffHeapStoredObject) ma.allocate(100);

      OffHeapStoredObjectAddressStack stack =
          new OffHeapStoredObjectAddressStack(chunk.getAddress());
      assertEquals(false, stack.isEmpty());
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void stackWithChunkIsNotEmpty() {
    SlabImpl slab = new SlabImpl(1024);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      OffHeapStoredObject chunk = (OffHeapStoredObject) ma.allocate(100);

      OffHeapStoredObjectAddressStack stack = new OffHeapStoredObjectAddressStack();
      stack.offer(chunk.getAddress());
      assertEquals(false, stack.isEmpty());
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void stackWithChunkTopEqualsAddress() {
    SlabImpl slab = new SlabImpl(1024);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      OffHeapStoredObject chunk = (OffHeapStoredObject) ma.allocate(100);

      long addr = chunk.getAddress();
      OffHeapStoredObjectAddressStack stack = new OffHeapStoredObjectAddressStack();
      stack.offer(addr);
      assertEquals(addr, stack.getTopAddress());
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void addressZeroOfferCausesFailedAssertion() {
    OffHeapStoredObjectAddressStack stack = new OffHeapStoredObjectAddressStack(0L);
    try {
      stack.offer(0);
      fail("expected AssertionError");
    } catch (AssertionError expected) {
    }
  }


  @Test
  public void stackWithChunkClearReturnsAddressAndEmptiesStack() {
    SlabImpl slab = new SlabImpl(1024);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      OffHeapStoredObject chunk = (OffHeapStoredObject) ma.allocate(100);

      long addr = chunk.getAddress();
      OffHeapStoredObjectAddressStack stack = new OffHeapStoredObjectAddressStack();
      stack.offer(addr);
      long clearAddr = stack.clear();
      assertEquals(addr, clearAddr);
      assertEquals(true, stack.isEmpty());
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void stackWithChunkPollReturnsAddressAndEmptiesStack() {
    SlabImpl slab = new SlabImpl(1024);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      OffHeapStoredObject chunk = (OffHeapStoredObject) ma.allocate(100);

      long addr = chunk.getAddress();
      OffHeapStoredObjectAddressStack stack = new OffHeapStoredObjectAddressStack();
      stack.offer(addr);
      long pollAddr = stack.poll();
      assertEquals(addr, pollAddr);
      assertEquals(true, stack.isEmpty());
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void stackWithChunkTotalSizeIsChunkSize() {
    SlabImpl slab = new SlabImpl(1024);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      OffHeapStoredObject chunk = (OffHeapStoredObject) ma.allocate(100);
      int chunkSize = chunk.getSize();

      long addr = chunk.getAddress();
      OffHeapStoredObjectAddressStack stack = new OffHeapStoredObjectAddressStack();
      stack.offer(addr);
      assertEquals(chunkSize, stack.computeTotalSize());
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }


  @Test
  public void stackWithChunkLogShowsMsgAndSize() {
    SlabImpl slab = new SlabImpl(1024);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      OffHeapStoredObject chunk = (OffHeapStoredObject) ma.allocate(100);
      int chunkSize = chunk.getSize();

      long addr = chunk.getAddress();
      OffHeapStoredObjectAddressStack stack = new OffHeapStoredObjectAddressStack();
      stack.offer(addr);
      Logger lw = mock(Logger.class);
      stack.logSizes(lw, "foo");
      verify(lw).info("foo" + chunkSize);
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  private class TestableSyncChunkStack extends OffHeapStoredObjectAddressStack {
    public boolean doConcurrentMod = true;
    public int chunk2Size;
    private final MemoryAllocatorImpl ma;

    TestableSyncChunkStack(MemoryAllocatorImpl ma) {
      this.ma = ma;
    }

    @Override
    protected void testHookDoConcurrentModification() {
      if (doConcurrentMod) {
        doConcurrentMod = false;
        OffHeapStoredObject chunk2 = (OffHeapStoredObject) ma.allocate(50);
        chunk2Size = chunk2.getSize();
        offer(chunk2.getAddress());
      }
    }
  }

  @Test
  public void stackWithChunkTotalSizeIsChunkSizeWithConcurrentMod() {
    SlabImpl slab = new SlabImpl(1024);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      OffHeapStoredObject chunk = (OffHeapStoredObject) ma.allocate(100);
      int chunkSize = chunk.getSize();

      long addr = chunk.getAddress();
      TestableSyncChunkStack stack = new TestableSyncChunkStack(ma);
      stack.offer(addr);
      long totalSize = stack.computeTotalSize();
      assertEquals("chunkSize=" + chunkSize + " chunk2Size=" + stack.chunk2Size,
          chunkSize + stack.chunk2Size, totalSize);
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }


  @Test
  public void stackWithChunkLogShowsMsgAndSizeWithConcurrentMod() {
    SlabImpl slab = new SlabImpl(1024);
    try {
      MemoryAllocatorImpl ma =
          MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
              new NullOffHeapMemoryStats(), new SlabImpl[] {slab});
      OffHeapStoredObject chunk = (OffHeapStoredObject) ma.allocate(100);
      int chunkSize = chunk.getSize();

      long addr = chunk.getAddress();
      TestableSyncChunkStack stack = new TestableSyncChunkStack(ma);
      stack.offer(addr);
      Logger lw = mock(Logger.class);
      stack.logSizes(lw, "foo");
      verify(lw).info("foo" + chunkSize);
      verify(lw).info("foo" + stack.chunk2Size);
    } finally {
      MemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
}
