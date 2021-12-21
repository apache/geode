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

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;

import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.offheap.MemoryBlock.State;

public class TinyMemoryBlockJUnitTest {

  private MemoryAllocatorImpl ma;
  private OutOfOffHeapMemoryListener ooohml;
  private OffHeapMemoryStats stats;
  private Slab[] slabs;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public JUnitSoftAssertions softly = new JUnitSoftAssertions();

  @Before
  public void setUp() throws Exception {
    slabs = new Slab[] {new SlabImpl((int) OffHeapStorage.MIN_SLAB_SIZE),
        new SlabImpl((int) OffHeapStorage.MIN_SLAB_SIZE),
        new SlabImpl((int) OffHeapStorage.MIN_SLAB_SIZE)};
    ooohml = mock(OutOfOffHeapMemoryListener.class);
    stats = mock(OffHeapMemoryStats.class);
    ma = MemoryAllocatorImpl.createForUnitTest(ooohml, stats, slabs);
  }

  @After
  public void tearDown() throws Exception {
    MemoryAllocatorImpl.freeOffHeapMemory();
  }

  protected Object getValue() {
    return Long.valueOf(Long.MAX_VALUE);
  }

  private StoredObject createChunk(byte[] v, boolean isSerialized, boolean isCompressed) {
    StoredObject chunk = ma.allocateAndInitialize(v, isSerialized, isCompressed);
    return chunk;
  }

  private StoredObject createValueAsSerializedStoredObject(Object value, boolean isCompressed) {
    byte[] valueInSerializedByteArray = EntryEventImpl.serialize(value);

    boolean isSerialized = true;

    StoredObject createdObject =
        createChunk(valueInSerializedByteArray, isSerialized, isCompressed);
    return createdObject;
  }

  private byte[] convertValueToByteArray(Object value) {
    return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong((Long) value).array();
  }

  private StoredObject createValueAsUnserializedStoredObject(Object value, boolean isCompressed) {
    byte[] valueInByteArray;
    if (value instanceof Long) {
      valueInByteArray = convertValueToByteArray(value);
    } else {
      valueInByteArray = (byte[]) value;
    }

    boolean isSerialized = false;

    StoredObject createdObject = createChunk(valueInByteArray, isSerialized, isCompressed);
    return createdObject;
  }

  @Test
  public void constructorReturnsNonNullMemoryBlock() {
    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(slabs[0].getMemoryAddress(), 0);
    softly.assertThat(mb).isNotNull();
  }

  @Test
  public void stateAlwaysEqualsDeallocated() {
    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(slabs[0].getMemoryAddress(), 0);
    softly.assertThat(mb.getState()).isEqualTo(State.DEALLOCATED);
  }

  @Test
  public void getMemoryAddressReturnsAddressBlockWasContructedFrom() {
    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(slabs[0].getMemoryAddress(), 0);
    softly.assertThat(mb.getAddress()).isEqualTo(slabs[0].getMemoryAddress());
  }

  @Test
  public void getBlockSizeReturnsReturnsSizeOfUnderlyingChunk() {
    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(
        new OffHeapStoredObject(slabs[0].getMemoryAddress(), slabs[0].getSize()).getAddress(), 0);
    softly.assertThat(mb.getBlockSize()).isEqualTo(slabs[0].getSize());
  }

  @Test
  public void getNextBlockThrowsUnsupportedOperationException() {
    expectedException.expect(UnsupportedOperationException.class);

    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(
        new OffHeapStoredObject(slabs[0].getMemoryAddress(), slabs[0].getSize()).getAddress(), 0);
    mb.getNextBlock();
    fail("getNextBlock failed to throw UnsupportedOperationException");
  }

  @Test
  public void getSlabIdThrowsUnsupportedOperationException() {
    expectedException.expect(UnsupportedOperationException.class);

    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(
        new OffHeapStoredObject(slabs[0].getMemoryAddress(), slabs[0].getSize()).getAddress(), 0);
    mb.getSlabId();
    fail("getSlabId failed to throw UnsupportedOperationException");
  }

  @Test
  public void getFreeListIdReturnsIdBlockWasConstructedWith() {
    MemoryBlock mb0 = new TestableFreeListManager.TinyMemoryBlock(
        new OffHeapStoredObject(slabs[0].getMemoryAddress(), slabs[0].getSize()).getAddress(), 0);
    MemoryBlock mb1 = new TestableFreeListManager.TinyMemoryBlock(
        new OffHeapStoredObject(slabs[1].getMemoryAddress(), slabs[1].getSize()).getAddress(), 1);
    softly.assertThat(mb0.getFreeListId()).isEqualTo(0);
    softly.assertThat(mb1.getFreeListId()).isEqualTo(1);
  }

  @Test
  public void getRefCountReturnsZero() {
    MemoryBlock mb0 = new TestableFreeListManager.TinyMemoryBlock(
        new OffHeapStoredObject(slabs[0].getMemoryAddress(), slabs[0].getSize()).getAddress(), 0);
    MemoryBlock mb1 = new TestableFreeListManager.TinyMemoryBlock(
        new OffHeapStoredObject(slabs[1].getMemoryAddress(), slabs[1].getSize()).getAddress(), 1);
    softly.assertThat(mb0.getRefCount()).isEqualTo(0);
    softly.assertThat(mb1.getRefCount()).isEqualTo(0);
  }

  @Test
  public void getDataTypeReturnsNA() {
    Object obj = getValue();
    boolean compressed = false;

    StoredObject storedObject0 = createValueAsSerializedStoredObject(obj, compressed);
    MemoryBlock mb =
        new TestableFreeListManager.TinyMemoryBlock(((MemoryBlock) storedObject0).getAddress(), 0);
    softly.assertThat(mb.getDataType()).isEqualTo("N/A");
  }

  @Test
  public void getDataValueReturnsNull() {
    Object obj = getValue();
    boolean compressed = false;

    StoredObject storedObject0 = createValueAsSerializedStoredObject(obj, compressed);
    MemoryBlock mb =
        new TestableFreeListManager.TinyMemoryBlock(((MemoryBlock) storedObject0).getAddress(), 0);
    softly.assertThat(mb.getDataValue()).isNull();
  }

  @Test
  public void isSerializedReturnsFalse() {
    Object obj = getValue();
    boolean compressed = false;

    StoredObject storedObject0 = createValueAsSerializedStoredObject(obj, compressed);
    StoredObject storedObject1 = createValueAsUnserializedStoredObject(obj, compressed);
    MemoryBlock mb0 =
        new TestableFreeListManager.TinyMemoryBlock(((MemoryBlock) storedObject0).getAddress(), 0);
    MemoryBlock mb1 =
        new TestableFreeListManager.TinyMemoryBlock(((MemoryBlock) storedObject1).getAddress(), 0);
    softly.assertThat(mb0.isSerialized()).isFalse();
    softly.assertThat(mb1.isSerialized()).isFalse();
  }

  @Test
  public void isCompressedReturnsFalse() {
    Object obj = getValue();
    boolean compressed = false;
    StoredObject storedObject0 = createValueAsUnserializedStoredObject(obj, compressed);
    StoredObject storedObject1 = createValueAsUnserializedStoredObject(obj, compressed = true);
    MemoryBlock mb0 =
        new TestableFreeListManager.TinyMemoryBlock(((MemoryBlock) storedObject0).getAddress(), 0);
    MemoryBlock mb1 =
        new TestableFreeListManager.TinyMemoryBlock(((MemoryBlock) storedObject1).getAddress(), 0);
    softly.assertThat(mb0.isCompressed()).isFalse();
    softly.assertThat(mb1.isCompressed()).isFalse();
  }

  @Test
  public void equalsComparesAddressesOfTinyMemoryBlocks() {
    MemoryBlock mb0 = new TestableFreeListManager.TinyMemoryBlock(slabs[0].getMemoryAddress(), 0);
    MemoryBlock mb1 = new TestableFreeListManager.TinyMemoryBlock(slabs[0].getMemoryAddress(), 0);
    MemoryBlock mb2 = new TestableFreeListManager.TinyMemoryBlock(slabs[1].getMemoryAddress(), 0);
    softly.assertThat(mb0.equals(mb1)).isTrue();
    softly.assertThat(mb0.equals(mb2)).isFalse();
  }

  @Test
  public void equalsNotTinyMemoryBlockReturnsFalse() {
    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(slabs[0].getMemoryAddress(), 0);
    softly.assertThat(mb.equals(slabs[0])).isFalse();
  }

  @Test
  public void hashCodeReturnsHashOfUnderlyingMemory() {
    MemoryBlock mb = new TestableFreeListManager.TinyMemoryBlock(slabs[0].getMemoryAddress(), 0);
    softly.assertThat(mb.hashCode()).isEqualTo(
        new OffHeapStoredObject(slabs[0].getMemoryAddress(), slabs[0].getSize()).hashCode());
  }

  private static class TestableFreeListManager extends FreeListManager {
    TestableFreeListManager(MemoryAllocatorImpl ma, final Slab[] slabs) {
      super(ma, slabs);
    }
  }

}
