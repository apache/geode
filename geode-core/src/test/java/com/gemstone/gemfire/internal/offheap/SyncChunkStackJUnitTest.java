/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.offheap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.listeners.InvocationListener;
import org.mockito.listeners.MethodInvocationReport;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class SyncChunkStackJUnitTest {
  static {
    ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void addressZeroCausesStackToBeEmpty() {
    SyncChunkStack stack = new SyncChunkStack(0L);
    assertEquals(true, stack.isEmpty());
  }

  @Test
  public void defaultStackIsEmpty() {
    SyncChunkStack stack = new SyncChunkStack();
    assertEquals(true, stack.isEmpty());
  }

  @Test
  public void defaultStackReturnsZeroFromTop() {
    SyncChunkStack stack = new SyncChunkStack();
    assertEquals(0L, stack.getTopAddress());
  }
  
  @Test
  public void defaultStackReturnsZeroFromPoll() {
    SyncChunkStack stack = new SyncChunkStack();
    assertEquals(0L, stack.poll());
  }
  
  @Test
  public void defaultStackReturnsZeroFromClear() {
    SyncChunkStack stack = new SyncChunkStack();
    assertEquals(0L, stack.clear());
    assertEquals(true, stack.isEmpty());
  }
  
  @Test
  public void defaultStackLogsNothing() {
    SyncChunkStack stack = new SyncChunkStack();
    LogWriter lw = mock(LogWriter.class, withSettings().invocationListeners(new InvocationListener() {
      @Override
      public void reportInvocation(MethodInvocationReport methodInvocationReport) {
        fail("Unexpected invocation");
      }
    }));
    stack.logSizes(lw, "should not be used");
  }
  
  @Test
  public void defaultStackComputeSizeIsZero() {
    SyncChunkStack stack = new SyncChunkStack();
    assertEquals(0L, stack.computeTotalSize());
  }
  
  @Test
  public void stackCreatedWithAddressIsNotEmpty() {
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(1024);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      Chunk chunk = (Chunk) ma.allocate(100, null);

      SyncChunkStack stack = new SyncChunkStack(chunk.getMemoryAddress());
      assertEquals(false, stack.isEmpty());
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void stackWithChunkIsNotEmpty() {
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(1024);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      Chunk chunk = (Chunk) ma.allocate(100, null);

      SyncChunkStack stack = new SyncChunkStack();
      stack.offer(chunk.getMemoryAddress());
      assertEquals(false, stack.isEmpty());
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void stackWithChunkTopEqualsAddress() {
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(1024);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      Chunk chunk = (Chunk) ma.allocate(100, null);

      long addr = chunk.getMemoryAddress();
      SyncChunkStack stack = new SyncChunkStack();
      stack.offer(addr);
      assertEquals(addr, stack.getTopAddress());
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void addressZeroOfferCausesFailedAssertion() {
    SyncChunkStack stack = new SyncChunkStack(0L);
    try {
      stack.offer(0);
      fail("expected AssertionError");
    } catch (AssertionError expected) {
    }
  }


  @Test
  public void stackWithChunkClearReturnsAddressAndEmptiesStack() {
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(1024);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      Chunk chunk = (Chunk) ma.allocate(100, null);

      long addr = chunk.getMemoryAddress();
      SyncChunkStack stack = new SyncChunkStack();
      stack.offer(addr);
      long clearAddr = stack.clear();
      assertEquals(addr, clearAddr);
      assertEquals(true, stack.isEmpty());
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void stackWithChunkPollReturnsAddressAndEmptiesStack() {
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(1024);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      Chunk chunk = (Chunk) ma.allocate(100, null);

      long addr = chunk.getMemoryAddress();
      SyncChunkStack stack = new SyncChunkStack();
      stack.offer(addr);
      long pollAddr = stack.poll();
      assertEquals(addr, pollAddr);
      assertEquals(true, stack.isEmpty());
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }

  @Test
  public void stackWithChunkTotalSizeIsChunkSize() {
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(1024);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      Chunk chunk = (Chunk) ma.allocate(100, null);
      int chunkSize = chunk.getSize();

      long addr = chunk.getMemoryAddress();
      SyncChunkStack stack = new SyncChunkStack();
      stack.offer(addr);
      assertEquals(chunkSize, stack.computeTotalSize());
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }


  @Test
  public void stackWithChunkLogShowsMsgAndSize() {
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(1024);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      Chunk chunk = (Chunk) ma.allocate(100, null);
      int chunkSize = chunk.getSize();

      long addr = chunk.getMemoryAddress();
      SyncChunkStack stack = new SyncChunkStack();
      stack.offer(addr);
      LogWriter lw = mock(LogWriter.class);
      stack.logSizes(lw, "foo");
      verify(lw).info("foo"+chunkSize);
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
  
  private class TestableSyncChunkStack extends SyncChunkStack {
    public boolean doConcurrentMod = true;
    public int chunk2Size;
    private SimpleMemoryAllocatorImpl ma;
    TestableSyncChunkStack(SimpleMemoryAllocatorImpl ma) {
      this.ma = ma;
    }
    @Override
    protected void testHookDoConcurrentModification() {
      if (doConcurrentMod) {
        doConcurrentMod = false;
        Chunk chunk2 = (Chunk) ma.allocate(50, null);
        this.chunk2Size = chunk2.getSize();
        this.offer(chunk2.getMemoryAddress());
      }
    }
  }
  @Test
  public void stackWithChunkTotalSizeIsChunkSizeWithConcurrentMod() {
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(1024);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      Chunk chunk = (Chunk) ma.allocate(100, null);
      int chunkSize = chunk.getSize();

      long addr = chunk.getMemoryAddress();
      TestableSyncChunkStack stack = new TestableSyncChunkStack(ma);
      stack.offer(addr);
      long totalSize = stack.computeTotalSize();
      assertEquals("chunkSize=" + chunkSize + " chunk2Size=" + stack.chunk2Size, chunkSize + stack.chunk2Size, totalSize);
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }


  @Test
  public void stackWithChunkLogShowsMsgAndSizeWithConcurrentMod() {
    UnsafeMemoryChunk slab = new UnsafeMemoryChunk(1024);
    try {
      SimpleMemoryAllocatorImpl ma = SimpleMemoryAllocatorImpl.create(new NullOutOfOffHeapMemoryListener(), new NullOffHeapMemoryStats(), new UnsafeMemoryChunk[]{slab});
      Chunk chunk = (Chunk) ma.allocate(100, null);
      int chunkSize = chunk.getSize();

      long addr = chunk.getMemoryAddress();
      TestableSyncChunkStack stack = new TestableSyncChunkStack(ma);
      stack.offer(addr);
      LogWriter lw = mock(LogWriter.class);
      stack.logSizes(lw, "foo");
      verify(lw).info("foo"+chunkSize);
      verify(lw).info("foo"+stack.chunk2Size);
    } finally {
      SimpleMemoryAllocatorImpl.freeOffHeapMemory();
    }
  }
}
