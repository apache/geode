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

import org.assertj.core.api.SoftAssertions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.*;
//import org.junit.contrib.java.lang.system.RestoreSystemProperties;
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
  
  @Rule public final ProvideSystemProperty myPropertyHasMyValue = new ProvideSystemProperty("gemfire.OFF_HEAP_DO_EXPENSIVE_VALIDATION", "true");

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
      //System.setProperty("gemfire.OFF_HEAP_DO_EXPENSIVE_VALIDATION", "true");
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
  public void fragmentConstructorThrowsExceptionForNon8ByteAlignedAddress() {
    int numSlabs = 2;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
//   System.out.println("slab address is: " + (new Formatter()).format("%X", slabs[0].getMemoryAddress()));
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress() + 2, 0);
    assertThat(fragment.freeSpace(), is(0));
  }

  @Test(expected = IllegalStateException.class)
  public void fragmentConstructorThrowsExceptionForNonSlabAddress() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
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
    SoftAssertions softly = new SoftAssertions();
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    softly.assertThat(fragment.getSize()).isEqualTo((int)OffHeapStorage.MIN_SLAB_SIZE + 1);
    softly.assertThat(fragment.freeSpace()).isEqualTo((int)OffHeapStorage.MIN_SLAB_SIZE + 1);
    softly.assertAll();
  }

  @Test
  public void allocatingFromFragmentReducesFreeSpace() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    assertThat(fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256), is(true));
    assertThat(fragment.freeSpace(), is(768));
    assertThat(fragment.getFreeIndex(), is(256));
  }

  @Test
  public void fragementAllocationIsUnsafeWithRespectToAllocationSize() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    assertThat(fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + (int)OffHeapStorage.MIN_SLAB_SIZE + 8), is(true));
    assertThat(fragment.freeSpace(), is(-8));
  }

  @Test
  public void getBlockSizeReturnsFreeSpace() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    assertThat(fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256), is(true));
    assertThat(fragment.getBlockSize(), is(fragment.freeSpace()));
  }

  @Test
  public void getMemoryAdressIsAlwaysFragmentBaseAddress() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    assertThat(fragment.getMemoryAddress(), is(slabs[0].getMemoryAddress()));
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    assertThat(fragment.getMemoryAddress(), is(slabs[0].getMemoryAddress()));
  }
  
  @Test
  public void getStateIsAlwaysStateUNUSED() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    assertThat(fragment.getState(), is(equalTo(MemoryBlock.State.UNUSED)));
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    assertThat(fragment.getState(), is(equalTo(MemoryBlock.State.UNUSED)));
  }

  @Test
  public void getFreeListIdIsAlwaysMinus1() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    assertThat(fragment.getFreeListId(), is(-1));
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    assertThat(fragment.getFreeListId(), is(-1));
  }

  @Test
  public void getRefCountIsAlwaysZero() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    assertThat(fragment.getRefCount(), is(0));
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    assertThat(fragment.getRefCount(), is(0));
  }

  @Test
  public void getDataTypeIsAlwaysNA() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    assertThat(fragment.getDataType(), is("N/A"));
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    assertThat(fragment.getDataType(), is("N/A"));
  }

  @Test
  public void isSerializedIsAlwaysFalse() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    assertThat(fragment.isSerialized(), is(false));
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    assertThat(fragment.isSerialized(), is(false));
  }

  @Test
  public void isCompressedIsAlwaysFalse() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    assertThat(fragment.isCompressed(), is(false));
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    assertThat(fragment.isCompressed(), is(false));
  }

  @Test
  public void getDataValueIsAlwaysNull() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    assertThat(fragment.getDataValue(), is(nullValue()));
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    assertThat(fragment.getDataValue(), is(nullValue()));
  }

  @Test
  public void getChunkTypeIsAlwaysNull() {
    int numSlabs = 1;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    assertThat(fragment.getChunkType(), is(nullValue()));
    fragment.allocate(fragment.getFreeIndex(), fragment.getFreeIndex() + 256);
    assertThat(fragment.getChunkType(), is(nullValue()));
  }

  @Test
  public void fragmentEqualsComparesMemoryBlockAddresses() {
    int numSlabs = 2;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment0 = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    Fragment sameFragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    Fragment fragment1 = new Fragment(slabs[1].getMemoryAddress(), slabs[1].getSize());
    assertThat(fragment0.equals(sameFragment), is(true));
    assertThat(fragment0.equals(fragment1), is(false));
  }

  @Test
  public void fragmentEqualsIsFalseForNonFragmentObjects() {
    int numSlabs = 2;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment0 = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    Fragment sameFragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    Fragment fragment1 = new Fragment(slabs[1].getMemoryAddress(), slabs[1].getSize());
    assertThat(fragment0.equals(slabs[0]), is(false));
  }

  @Test
  public void fragmentHashCodeIsHashCodeOfItsMemoryAddress() {
    int numSlabs = 2;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment0 = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    Fragment fragment1 = new Fragment(slabs[1].getMemoryAddress(), slabs[1].getSize());
    Long fragmentAddress = fragment0.getMemoryAddress();
    assertThat(fragment0.hashCode(), is(equalTo(fragmentAddress.hashCode())));
    assertThat(fragment0.hashCode(), is(not(equalTo(fragment1.hashCode()))));
  }

  @Test
  public void test() {
    int numSlabs = 2;
    UnsafeMemoryChunk[] slabs;
    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, numSlabs, OffHeapStorage.MIN_SLAB_SIZE * numSlabs, OffHeapStorage.MIN_SLAB_SIZE, umcFactory);
    slabs = ma.getSlabs();
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    Long fragmentAddress = fragment.getMemoryAddress();
    fragment.fill();
    assertThat(fragment.hashCode(), is(equalTo(fragmentAddress.hashCode())));
  }

////
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
