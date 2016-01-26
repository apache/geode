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
import static org.mockito.Mockito.mock;
import static org.hamcrest.CoreMatchers.*;

import java.nio.ByteBuffer;
import java.util.Formatter;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.VMCachedDeserializable;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class FragmentJUnitTest {

  private SimpleMemoryAllocatorImpl ma;
  private OutOfOffHeapMemoryListener ooohml;
  private OffHeapMemoryStats stats;
  private LogWriter lw;
  private UnsafeMemoryChunk.Factory umcFactory;

  static {
    ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
  }
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
      System.setProperty("gemfire.OFF_HEAP_DO_EXPENSIVE_VALIDATION", "true");
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Before
  public void setUp() throws Exception {
    ooohml = mock(OutOfOffHeapMemoryListener.class);
    stats = mock(OffHeapMemoryStats.class);
    lw = mock(LogWriter.class);
    umcFactory = new UnsafeMemoryChunk.Factory(){
      @Override
      public UnsafeMemoryChunk create(int size) {
        return new UnsafeMemoryChunk(size);
      }
    };

// static SimpleMemoryAllocatorImpl create(OutOfOffHeapMemoryListener ooohml, OffHeapMemoryStats stats, LogWriter lw, 
//    int slabCount, long offHeapMemorySize, long maxSlabSize, UnsafeMemoryChunk.Factory memChunkFactory) {
/*
    UnsafeMemoryChunk[] slabs = ma.getSlabs();
    Fragment[] tmp = new Fragment[slabs.length];
    for (int i=0; i < slabs.length; i++) {
      tmp[i] = new Fragment(slabs[i].getMemoryAddress(), slabs[i].getSize());
    }
*/
  }

  @After
  public void tearDown() throws Exception {
    SimpleMemoryAllocatorImpl.freeOffHeapMemory();
  }
/*
  @Override
  public Object getValue() {
    return Long.valueOf(Long.MAX_VALUE);
  }

  @Override
  public byte[] getValueAsByteArray() {
    return convertValueToByteArray(getValue());
  }

  private byte[] convertValueToByteArray(Object value) {
    return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong((Long) value).array();
  }

  @Override
  public Object convertByteArrayToObject(byte[] valueInByteArray) {
    return ByteBuffer.wrap(valueInByteArray).getLong();
  }

  @Override
  public Object convertSerializedByteArrayToObject(byte[] valueInSerializedByteArray) {
    return EntryEventImpl.deserialize(valueInSerializedByteArray);
  }

  @Override
  public GemFireChunk createValueAsUnserializedStoredObject(Object value) {
    byte[] valueInByteArray;
    if (value instanceof Long) {
      valueInByteArray = convertValueToByteArray(value);
    } else {
      valueInByteArray = (byte[]) value;
    }

    boolean isSerialized = false;
    boolean isCompressed = false;

    return createChunk(valueInByteArray, isSerialized, isCompressed);
  }

  @Override
  public GemFireChunk createValueAsSerializedStoredObject(Object value) {
    byte[] valueInSerializedByteArray = EntryEventImpl.serialize(value);

    boolean isSerialized = true;
    boolean isCompressed = false;

    return createChunk(valueInSerializedByteArray, isSerialized, isCompressed);
  }

  private GemFireChunk createChunk(byte[] v, boolean isSerialized, boolean isCompressed) {
    GemFireChunk chunk = (GemFireChunk) ma.allocateAndInitialize(v, isSerialized, isCompressed, GemFireChunk.TYPE);
    return chunk;
  }
*/
  
  @Test(expected = IllegalStateException.class)
  public void fragmentConstructorValidatesNon8ByteAlignedAddress() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
//   System.out.println("slab address is: " + (new Formatter()).format("%X", slabs[0].getMemoryAddress()));
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress() + 2, 0);
    assertThat(fragment.freeSpace(), is(0));
  }

  @Test(expected = IllegalStateException.class)
  public void fragmentConstructorValidatesNonSlabAddress() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
//   System.out.println("slab address is: " + (new Formatter()).format("%X", slabs[0].getMemoryAddress()));
    Fragment fragment = new Fragment(1024L, 0);
    assertThat(fragment.freeSpace(), is(0));
  }

  @Test
  public void zeroSizeFragmentHasNoFreeSpace() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), 0);
    assertThat(fragment.freeSpace(), is(0));
  }

  @Test
  public void unallocatedFragmentHasFreeSpaceEqualToFragmentSize() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    assertThat(fragment.getSize(), is((int)OffHeapStorage.MIN_SLAB_SIZE));
    assertThat(fragment.freeSpace(), is((int)OffHeapStorage.MIN_SLAB_SIZE));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getNextBlockThrowsExceptionForFragment() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    MemoryBlock mb = fragment.getNextBlock();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getSlabIdThrowsExceptionForFragment() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    int id = fragment.getSlabId();
  }
  
}
