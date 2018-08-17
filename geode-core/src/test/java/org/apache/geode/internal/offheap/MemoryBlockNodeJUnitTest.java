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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import org.assertj.core.api.JUnitSoftAssertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.ExpectedException;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.DSCODE;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.offheap.MemoryBlock.State;

public class MemoryBlockNodeJUnitTest {

  private MemoryAllocatorImpl ma;
  private OutOfOffHeapMemoryListener ooohml;
  private OffHeapMemoryStats stats;
  private Slab[] slabs = {new SlabImpl((int) OffHeapStorage.MIN_SLAB_SIZE),
      new SlabImpl((int) OffHeapStorage.MIN_SLAB_SIZE * 2)};
  private StoredObject storedObject = null;

  @Rule
  public final ProvideSystemProperty myPropertyHasMyValue = new ProvideSystemProperty(
      DistributionConfig.GEMFIRE_PREFIX + "OFF_HEAP_DO_EXPENSIVE_VALIDATION", "true");

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public JUnitSoftAssertions softly = new JUnitSoftAssertions();

  @Before
  public void setUp() {
    ooohml = mock(OutOfOffHeapMemoryListener.class);
    stats = mock(OffHeapMemoryStats.class);
    ma = (MemoryAllocatorImpl) MemoryAllocatorImpl.createForUnitTest(ooohml, stats, slabs);
  }

  @After
  public void tearDown() {
    MemoryAllocatorImpl.freeOffHeapMemory();
  }

  private Object getValue() {
    return Long.valueOf(Long.MAX_VALUE);
  }

  private StoredObject createValueAsUnserializedStoredObject(Object value) {
    StoredObject createdObject = createValueAsUnserializedStoredObject(value, false);
    return createdObject;
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

  private byte[] convertValueToByteArray(Object value) {
    return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong((Long) value).array();
  }

  private StoredObject createChunk(byte[] v, boolean isSerialized, boolean isCompressed) {
    StoredObject chunk = (StoredObject) ma.allocateAndInitialize(v, isSerialized, isCompressed);
    return chunk;
  }


  private StoredObject createValueAsSerializedStoredObject(Object value) {
    StoredObject createdObject = createValueAsSerializedStoredObject(value, false);
    return createdObject;
  }

  private StoredObject createValueAsSerializedStoredObject(Object value, boolean isCompressed) {
    byte[] valueInSerializedByteArray = EntryEventImpl.serialize(value);

    boolean isSerialized = true;

    StoredObject createdObject =
        createChunk(valueInSerializedByteArray, isSerialized, isCompressed);
    return createdObject;
  }


  private void allocateOffHeapDeserialized() {
    Object regionEntryValue = getValue();
    storedObject = createValueAsUnserializedStoredObject(regionEntryValue);
  }

  @Test
  public void memoryBlockNodesConstructedWithNullBlockArgumentThrowNPEForOperations() {
    expectedException.expect(NullPointerException.class);

    MemoryBlock mb = new MemoryBlockNode(ma, null);
    Long addr = mb.getAddress();
    fail(
        "Operations on MemoryBlockNodes with null block argument expected to throw NullPointerException ");
  }

  @Test
  public void zeroLengthMemoryBlockCausesAssertionErrorInConstructor() {
    expectedException.expect(AssertionError.class);

    MemoryBlock mb = new MemoryBlockNode(ma, (MemoryBlock) ma.allocate(0));
    softly.assertThat(mb.getBlockSize()).isEqualTo(0);
  }

  @Test
  public void getStateReturnsStateOfBlock() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    MemoryBlock mb = new MemoryBlockNode(ma, fragment);
    allocateOffHeapDeserialized();
    softly.assertThat(mb.getState()).isEqualTo(fragment.getState());
  }

  @Test
  public void getMemoryAddressReturnsAddressOfBlock() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    MemoryBlock mb = new MemoryBlockNode(ma, fragment);
    softly.assertThat(mb.getAddress()).isEqualTo(fragment.getAddress());
  }

  @Test
  public void getBlockSizeReturnsSizeOfBlock() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    MemoryBlock mb = new MemoryBlockNode(ma, fragment);
    softly.assertThat(mb.getBlockSize()).isEqualTo(fragment.getBlockSize());
  }

  @Test
  public void getNextBlockOfSingleBlockReturnsNull() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    MemoryBlock mb = new MemoryBlockNode(ma, fragment);
    softly.assertThat(mb.getNextBlock()).isNull();
  }

  @Test
  public void getSlabIdReturnsIdOfSlabBlockWasConstructedFrom() {
    for (int i = 0; i < slabs.length; ++i) {
      Fragment fragment = new Fragment(slabs[i].getMemoryAddress(), slabs[i].getSize());
      MemoryBlock mb = new MemoryBlockNode(ma, fragment);
      softly.assertThat(mb.getSlabId()).isEqualTo(i);
    }
  }

  @Test
  public void getFreeListIdReturnsIdFromUnderlyingBlock() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    MemoryBlock mb = new MemoryBlockNode(ma, fragment);
    softly.assertThat(mb.getFreeListId()).isEqualTo(-1);
  }

  @Test
  public void getRefCountReturnsRefCountFromUnderlyingBlock() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    MemoryBlock mb = new MemoryBlockNode(ma, fragment);
    softly.assertThat(mb.getRefCount()).isEqualTo(0);
    Object obj = getValue();
    storedObject = createValueAsSerializedStoredObject(obj);
    MemoryBlock mb2 = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    softly.assertThat(mb2.getRefCount()).isEqualTo(1);
  }

  @Test
  public void getDataTypeReturnsTypeFromUnderlyingBlock() {
    Object obj = getValue();
    storedObject = createValueAsSerializedStoredObject(obj);
    MemoryBlock mb2 = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    softly.assertThat(mb2.getDataType()).isEqualTo("java.lang.Long");
  }

  @Test
  public void getHashCodeReturnsCodeFromUnderlyingBlock() {
    Object obj = getValue();
    storedObject = createValueAsSerializedStoredObject(obj);
    MemoryBlock mb2 = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    softly.assertThat(mb2.hashCode()).isEqualTo(storedObject.hashCode());
  }

  @Test
  public void equalsComparisonWithNonMemoryBlockNodeReturnsFalse() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    Object obj = getValue();
    storedObject = createValueAsSerializedStoredObject(obj);
    MemoryBlock mb2 = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    softly.assertThat(mb2.equals(fragment)).isEqualTo(false);
  }

  @Test
  public void equalsComparisonWithAnotherMemoryBlockReturnsFalse() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    MemoryBlock mb = new MemoryBlockNode(ma, fragment);
    Object obj = getValue();
    storedObject = createValueAsSerializedStoredObject(obj);
    MemoryBlock mb2 = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    softly.assertThat(mb2.equals(mb)).isEqualTo(false);
    softly.assertThat(mb.equals(mb2)).isEqualTo(false);
  }

  @Test
  public void equalsComparisonToSelfReturnsTrue() {
    Object obj = getValue();
    storedObject = createValueAsSerializedStoredObject(obj);
    MemoryBlock mb = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    softly.assertThat(mb.equals(mb)).isEqualTo(true);
  }

  @Test
  public void equalsComparisonWhenUnderlyingBlocksHaveSameMemoryAddressReturnsTrue() {
    Object obj = getValue();
    storedObject = createValueAsSerializedStoredObject(obj);
    MemoryBlock mb1 = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    MemoryBlock mb2 = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    softly.assertThat(mb1.equals(mb2)).isEqualTo(true);
  }

  @Test
  public void getDataTypeOfSerializedCompressedReturnsTypeFromUnderlyingBlock() {
    Object obj = getValue();
    storedObject = createValueAsSerializedStoredObject(obj, true);
    MemoryBlock mb = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    softly.assertThat(mb.getDataType()).isEqualTo("compressed object of size 9");
  }

  @Test
  public void getDataTypeOfUnserializedNotCompressedReturnsTypeFromUnderlyingBlock() {
    Object obj = getValue();
    storedObject = createValueAsUnserializedStoredObject(obj, false);
    MemoryBlock mb = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    softly.assertThat(mb.getDataType()).isEqualTo("byte[8]" + "");
  }

  @Test
  public void getDataTypeOfUnserializedCompressedReturnsTypeFromUnderlyingBlock() {

    Object obj = getValue();
    storedObject = createValueAsUnserializedStoredObject(obj, true);
    MemoryBlock mb = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    softly.assertThat(mb.getDataType()).isEqualTo("compressed byte[8]");
  }

  @Test
  public void getDataValueSerializedNotCompressedReturnsFromUnderlyingBlock() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    MemoryBlock mb = new MemoryBlockNode(ma, fragment);
    softly.assertThat(mb.getDataValue()).isNull();
    Object obj = getValue();
    storedObject = createValueAsSerializedStoredObject(obj);
    MemoryBlock mb2 = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    softly.assertThat(mb2.getDataValue()).isEqualTo(getValue());
  }

  @Test
  public void getDataValueSerializedCompressedReturnsBytesFromUnderlyingBlock() {
    Object obj = getValue();
    storedObject = createValueAsSerializedStoredObject(obj, true);
    MemoryBlock mb = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    byte[] storedObjectBytes = new byte[storedObject.getValueSizeInBytes()];
    storedObject.readDataBytes(0, storedObjectBytes);
    softly.assertThat(mb.getDataValue()).isEqualTo(storedObjectBytes);
  }

  @Test
  public void getDataValueUnserializedCompressedReturnsBytesFromUnderlyingBlock() {
    Object obj = getValue();
    storedObject = createValueAsUnserializedStoredObject(obj, true);
    MemoryBlock mb = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    byte[] storedObjectBytes = new byte[storedObject.getValueSizeInBytes()];
    storedObject.readDataBytes(0, storedObjectBytes);
    softly.assertThat(mb.getDataValue()).isEqualTo(storedObjectBytes);
  }

  @Test
  public void getDataValueUnserializedNotCompressedReturnsBytesFromUnderlyingBlock() {
    Object obj = getValue();
    storedObject = createValueAsUnserializedStoredObject(obj, false);
    MemoryBlock mb = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    byte[] storedObjectBytes = new byte[storedObject.getValueSizeInBytes()];
    storedObject.readDataBytes(0, storedObjectBytes);
    softly.assertThat(mb.getDataValue()).isEqualTo(storedObjectBytes);
  }

  @Test
  public void getDataValueWithIllegalDataTypeCatchesIOException() {
    Object obj = getValue();
    storedObject = createValueAsSerializedStoredObject(obj);
    storedObject.writeDataByte(0, DSCODE.ILLEGAL.toByte());
    MemoryBlock mb = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    System.setErr(new PrintStream(errContent));
    softly.assertThat(mb.getDataValue()).isEqualTo("IOException:Unknown header byte: -127");
  }

  @Test
  public void getDataValueCatchesCacheClosedException() {
    Object obj = getValue();
    storedObject = createValueAsSerializedStoredObject(obj);
    OffHeapStoredObject spyStoredObject = spy((OffHeapStoredObject) storedObject);
    doReturn("java.lang.Long").when(spyStoredObject).getDataType();
    doAnswer((m) -> {
      throw new CacheClosedException("Unit test forced exception");
    }).when(spyStoredObject)
        .getRawBytes();
    ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    System.setErr(new PrintStream(errContent));
    MemoryBlock mb = new MemoryBlockNode(ma, spyStoredObject);
    softly.assertThat(mb.getDataValue())
        .isEqualTo("CacheClosedException:Unit test forced exception");
  }

  @SuppressWarnings("unchecked")
  @Test
  public void getDataValueCatchesClassNotFoundException() throws Exception {
    Object obj = getValue();
    storedObject = createValueAsSerializedStoredObject(obj);
    OffHeapStoredObject spyStoredObject = spy((OffHeapStoredObject) storedObject);
    doReturn("java.lang.Long").when(spyStoredObject).getDataType();
    doAnswer((m) -> {
      throw new ClassNotFoundException();
    }).when(spyStoredObject).getRawBytes();
    ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    System.setErr(new PrintStream(errContent));
    MemoryBlock mb = new MemoryBlockNode(ma, (MemoryBlock) spyStoredObject);
    softly.assertThat(mb.getDataValue()).isEqualTo("ClassNotFoundException:null");
  }

  @Test
  public void toStringOfUnusedBlockReturnsStateUnusedAndTypeNA() {
    Fragment fragment = new Fragment(slabs[0].getMemoryAddress(), slabs[0].getSize());
    MemoryBlock mb = new MemoryBlockNode(ma, fragment);
    softly.assertThat(mb.toString()).matches(
        "MemoryBlock\\{MemoryAddress=\\d*, State=UNUSED, BlockSize=1024, SlabId=0, FreeListId=NONE, RefCount=0, isSerialized=false, isCompressed=false, DataType=N/A, DataValue=null}");
  }

  @Test
  public void toStringOfAllocatedBlockReturnsStateAllocateddAndTypeOfData() {
    Object obj = getValue();
    storedObject = createValueAsSerializedStoredObject(obj);
    MemoryBlock mb = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    softly.assertThat(mb.toString()).matches(
        "MemoryBlock\\{MemoryAddress=\\d*, State=ALLOCATED, BlockSize=\\d*, SlabId=0, FreeListId=NONE, RefCount=1, isSerialized=true, isCompressed=false, DataType=java.lang.Long, DataValue=9223372036854775807}");
  }

  @Test
  public void toStringOfAllocatedBlockWithUnserializedValueReturnsByteArrayType() {
    Object obj = getValue();
    storedObject = createValueAsUnserializedStoredObject(obj);
    MemoryBlock mb = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    softly.assertThat(mb.toString()).matches(
        "MemoryBlock\\{MemoryAddress=\\d*, State=ALLOCATED, BlockSize=\\d*, SlabId=0, FreeListId=NONE, RefCount=1, isSerialized=false, isCompressed=false, DataType=byte\\[8], DataValue=\\[127, -1, -1, -1, -1, -1, -1, -1]}");
  }

  @Test
  public void toStringWithStateDeallocatedResultsInFreeListIdHuge() {
    Object obj = getValue();
    storedObject = createValueAsSerializedStoredObject(obj);
    MemoryBlock mb = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    MemoryBlock spyMb = spy(mb);
    softly.assertThat(spyMb.toString()).matches(
        "MemoryBlock\\{MemoryAddress=\\d*, State=ALLOCATED, BlockSize=\\d*, SlabId=0, FreeListId=NONE, RefCount=1, isSerialized=true, isCompressed=false, DataType=java.lang.Long, DataValue=9223372036854775807}");
    when(spyMb.getState()).thenReturn(State.DEALLOCATED);
    when(spyMb.getRefCount()).thenReturn(0);
    softly.assertThat(spyMb.toString()).matches(
        "MemoryBlock\\{MemoryAddress=\\d*, State=DEALLOCATED, BlockSize=\\d*, SlabId=0, FreeListId=HUGE, RefCount=0, isSerialized=true, isCompressed=false, DataType=java.lang.Long, DataValue=9223372036854775807}");
  }

  @Test
  public void toStringWithFreeListNotMinus1() {
    Object obj = getValue();
    storedObject = createValueAsSerializedStoredObject(obj);
    MemoryBlock mb = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    MemoryBlock spyMb = spy(mb);
    softly.assertThat(spyMb.toString()).matches(
        "MemoryBlock\\{MemoryAddress=\\d*, State=ALLOCATED, BlockSize=\\d*, SlabId=0, FreeListId=NONE, RefCount=1, isSerialized=true, isCompressed=false, DataType=java.lang.Long, DataValue=9223372036854775807}");
    when(spyMb.getState()).thenReturn(State.DEALLOCATED);
    when(spyMb.getRefCount()).thenReturn(0);
    softly.assertThat(spyMb.toString()).matches(
        "MemoryBlock\\{MemoryAddress=\\d*, State=DEALLOCATED, BlockSize=\\d*, SlabId=0, FreeListId=HUGE, RefCount=0, isSerialized=true, isCompressed=false, DataType=java.lang.Long, DataValue=9223372036854775807}");
    when(spyMb.getFreeListId()).thenReturn(0);
    softly.assertThat(spyMb.toString()).matches(
        "MemoryBlock\\{MemoryAddress=\\d*, State=DEALLOCATED, BlockSize=\\d*, SlabId=0, FreeListId=0, RefCount=0, isSerialized=true, isCompressed=false, DataType=java.lang.Long, DataValue=9223372036854775807}");
  }

  @Test
  public void toStringOfAllocatedBlockWithLargeByteArrayValueShowsValueAsArraySize() {
    Object obj = new byte[1024];
    storedObject = createValueAsUnserializedStoredObject(obj);
    MemoryBlock mb = new MemoryBlockNode(ma, (MemoryBlock) storedObject);
    softly.assertThat(mb.toString()).matches(
        "MemoryBlock\\{MemoryAddress=\\d*, State=ALLOCATED, BlockSize=\\d*, SlabId=1, FreeListId=NONE, RefCount=1,"
            + " isSerialized=false, isCompressed=false, DataType=byte\\[1024], DataValue=<byte array of length 1024>}");
  }
}
