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
package org.apache.geode.internal.cache;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.entries.DiskEntry.Helper.Flushable;
import org.apache.geode.internal.cache.entries.DiskEntry.Helper.OffHeapValueWrapper;
import org.apache.geode.internal.offheap.MemoryAllocatorImpl;
import org.apache.geode.internal.offheap.NullOffHeapMemoryStats;
import org.apache.geode.internal.offheap.NullOutOfOffHeapMemoryListener;
import org.apache.geode.internal.offheap.SlabImpl;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class OffHeapValueWrapperJUnitTest {

  private OffHeapValueWrapper createChunkValueWrapper(byte[] bytes, boolean isSerialized) {
    StoredObject c =
        MemoryAllocatorImpl.getAllocator().allocateAndInitialize(bytes, isSerialized, false);
    return new OffHeapValueWrapper(c);
  }

  @Before
  public void setUp() throws Exception {
    MemoryAllocatorImpl.createForUnitTest(new NullOutOfOffHeapMemoryListener(),
        new NullOffHeapMemoryStats(), new SlabImpl[] {new SlabImpl(1024 * 1024)});
  }

  @After
  public void tearDown() throws Exception {
    MemoryAllocatorImpl.freeOffHeapMemory();
  }

  @Test
  public void testIsSerialized() {
    assertEquals(true, createChunkValueWrapper(new byte[16], true).isSerialized());
    assertEquals(false, createChunkValueWrapper(new byte[16], false).isSerialized());
  }

  @Test
  public void testGetUserBits() {
    assertEquals((byte) 1, createChunkValueWrapper(new byte[16], true).getUserBits());
    assertEquals((byte) 0, createChunkValueWrapper(new byte[16], false).getUserBits());
  }

  @Test
  public void testGetLength() {
    assertEquals(32, createChunkValueWrapper(new byte[32], true).getLength());
    assertEquals(17, createChunkValueWrapper(new byte[17], false).getLength());
  }

  @Test
  public void testGetBytesAsString() {
    assertEquals("byte[0, 0, 0, 0, 0, 0, 0, 0]",
        createChunkValueWrapper(new byte[8], false).getBytesAsString());
  }

  @Test
  public void testSendTo() throws IOException {
    final ByteBuffer bb = ByteBuffer.allocateDirect(18);
    bb.limit(8);
    OffHeapValueWrapper vw = createChunkValueWrapper(new byte[] {1, 2, 3, 4, 5, 6, 7, 8}, false);
    vw.sendTo(bb, new Flushable() {
      @Override
      public void flush() throws IOException {
        fail("should not have been called");
      }

      @Override
      public void flush(ByteBuffer bb, ByteBuffer chunkbb) throws IOException {
        fail("should not have been called");
      }
    });
    assertEquals(8, bb.position());
    bb.flip();
    assertEquals(1, bb.get());
    assertEquals(2, bb.get());
    assertEquals(3, bb.get());
    assertEquals(4, bb.get());
    assertEquals(5, bb.get());
    assertEquals(6, bb.get());
    assertEquals(7, bb.get());
    assertEquals(8, bb.get());

    bb.clear();
    bb.limit(8);
    vw = createChunkValueWrapper(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, false);
    final int[] flushCalls = new int[1];
    vw.sendTo(bb, new Flushable() {
      @Override
      public void flush() throws IOException {
        if (flushCalls[0] != 0) {
          fail("expected flush to only be called once");
        }
        flushCalls[0]++;
        assertEquals(8, bb.position());
        for (int i = 0; i < 8; i++) {
          assertEquals(i + 1, bb.get(i));
        }
        bb.clear();
        bb.limit(8);
      }

      @Override
      public void flush(ByteBuffer bb, ByteBuffer chunkbb) throws IOException {
        fail("should not have been called");
      }
    });
    assertEquals(1, bb.position());
    bb.flip();
    assertEquals(9, bb.get());

    bb.clear();
    bb.limit(8);
    flushCalls[0] = 0;
    vw = createChunkValueWrapper(
        new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}, false);
    vw.sendTo(bb, new Flushable() {
      @Override
      public void flush() throws IOException {
        if (flushCalls[0] > 1) {
          fail("expected flush to only be called twice");
        }
        assertEquals(8, bb.position());
        for (int i = 0; i < 8; i++) {
          assertEquals((flushCalls[0] * 8) + i + 1, bb.get(i));
        }
        flushCalls[0]++;
        bb.clear();
        bb.limit(8);
      }

      @Override
      public void flush(ByteBuffer bb, ByteBuffer chunkbb) throws IOException {
        fail("should not have been called");
      }
    });
    assertEquals(1, bb.position());
    bb.flip();
    assertEquals(17, bb.get());

    // now test with a chunk that will not fit in bb.
    bb.clear();
    flushCalls[0] = 0;
    bb.put((byte) 0);
    vw = createChunkValueWrapper(
        new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, false);
    vw.sendTo(bb, new Flushable() {
      @Override
      public void flush() throws IOException {
        fail("should not have been called");
      }

      @Override
      public void flush(ByteBuffer bb, ByteBuffer chunkbb) throws IOException {
        flushCalls[0]++;
        assertEquals(1, bb.position());
        bb.flip();
        assertEquals(0, bb.get());
        assertEquals(19, chunkbb.remaining());
        for (int i = 1; i <= 19; i++) {
          assertEquals(i, chunkbb.get());
        }
      }
    });
    assertEquals(1, flushCalls[0]);
  }
}
