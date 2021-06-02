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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.compression.Compressor;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.BytesAndBitsForCompactor;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.offheap.MemoryBlock.State;
import org.apache.geode.internal.serialization.DSCODE;
import org.apache.geode.internal.serialization.KnownVersion;

public class OffHeapStoredObjectJUnitTest extends AbstractStoredObjectTestBase {

  private MemoryAllocator ma;
  private static Boolean assertionsEnabled;

  @BeforeClass
  public static void setUpOnce() {
    try {
      assert false;
      assertionsEnabled = false;
    } catch (AssertionError e) {
      assertionsEnabled = true;
    }
    ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    System.out.println("assertionsEnabled = " + assertionsEnabled);
  }

  @AfterClass
  public static void tearDownOnce() {
    ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(assertionsEnabled);
  }

  @Before
  public void setUp() {
    OutOfOffHeapMemoryListener ooohml = mock(OutOfOffHeapMemoryListener.class);
    OffHeapMemoryStats stats = mock(OffHeapMemoryStats.class);

    ma = MemoryAllocatorImpl.create(ooohml, stats, 3, OffHeapStorage.MIN_SLAB_SIZE * 3,
        OffHeapStorage.MIN_SLAB_SIZE);
  }

  @After
  public void tearDown() {
    MemoryAllocatorImpl.freeOffHeapMemory();
  }

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
  public OffHeapStoredObject createValueAsUnserializedStoredObject(Object value) {
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
  public OffHeapStoredObject createValueAsSerializedStoredObject(Object value) {
    byte[] valueInSerializedByteArray = EntryEventImpl.serialize(value);

    boolean isSerialized = true;
    boolean isCompressed = false;

    return createChunk(valueInSerializedByteArray, isSerialized, isCompressed);
  }

  private OffHeapStoredObject createChunk(byte[] v, boolean isSerialized, boolean isCompressed) {
    OffHeapStoredObject chunk =
        (OffHeapStoredObject) ma.allocateAndInitialize(v, isSerialized, isCompressed);
    return chunk;
  }

  @Test
  public void chunkCanBeCreatedFromAnotherChunk() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());

    OffHeapStoredObject newChunk = new OffHeapStoredObject(chunk);

    assertNotNull(newChunk);
    assertThat(newChunk.getAddress()).isEqualTo(chunk.getAddress());

    chunk.release();
  }

  @Test
  public void chunkCanBeCreatedWithOnlyMemoryAddress() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());

    OffHeapStoredObject newChunk = new OffHeapStoredObject(chunk.getAddress());

    assertNotNull(newChunk);
    assertThat(newChunk.getAddress()).isEqualTo(chunk.getAddress());

    chunk.release();
  }

  @Test
  public void chunkSliceCanBeCreatedFromAnotherChunk() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());

    int position = 1;
    int end = 2;

    OffHeapStoredObject newChunk = (OffHeapStoredObject) chunk.slice(position, end);

    assertNotNull(newChunk);
    assertThat(newChunk.getClass()).isEqualTo(OffHeapStoredObjectSlice.class);
    assertThat(newChunk.getAddress()).isEqualTo(chunk.getAddress());

    chunk.release();
  }

  @Test
  public void fillSerializedValueShouldFillWrapperWithSerializedValueIfValueIsSerialized() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());

    // mock the things
    BytesAndBitsForCompactor wrapper = mock(BytesAndBitsForCompactor.class);

    byte userBits = 0;
    byte serializedUserBits = 1;
    chunk.fillSerializedValue(wrapper, userBits);

    verify(wrapper, times(1)).setOffHeapData(chunk, serializedUserBits);

    chunk.release();
  }

  @Test
  public void fillSerializedValueShouldFillWrapperWithDeserializedValueIfValueIsNotSerialized() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());

    // mock the things
    BytesAndBitsForCompactor wrapper = mock(BytesAndBitsForCompactor.class);

    byte userBits = 1;
    chunk.fillSerializedValue(wrapper, userBits);

    verify(wrapper, times(1)).setOffHeapData(chunk, userBits);

    chunk.release();
  }

  @Test
  public void getShortClassNameShouldReturnShortClassName() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.getShortClassName()).isEqualTo("OffHeapStoredObject");

    chunk.release();
  }

  @Test
  public void chunksAreEqualsOnlyByAddress() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());

    OffHeapStoredObject newChunk = new OffHeapStoredObject(chunk.getAddress());
    assertThat(chunk.equals(newChunk)).isTrue();

    OffHeapStoredObject chunkWithSameValue = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.equals(chunkWithSameValue)).isFalse();

    Object someObject = getValue();
    assertThat(chunk.equals(someObject)).isFalse();

    chunk.release();
    chunkWithSameValue.release();
  }

  @Test
  public void chunksShouldBeComparedBySize() {
    OffHeapStoredObject chunk1 = createValueAsSerializedStoredObject(getValue());

    OffHeapStoredObject chunk2 = chunk1;
    assertThat(chunk1.compareTo(chunk2)).isEqualTo(0);

    OffHeapStoredObject chunkWithSameValue = createValueAsSerializedStoredObject(getValue());
    assertThat(chunk1.compareTo(chunkWithSameValue))
        .isEqualTo(Long.signum(chunk1.getAddress() - chunkWithSameValue.getAddress()));

    OffHeapStoredObject chunk3 = createValueAsSerializedStoredObject(Long.MAX_VALUE);
    OffHeapStoredObject chunk4 = createValueAsSerializedStoredObject(Long.MAX_VALUE);

    int newSizeForChunk3 = 2;
    int newSizeForChunk4 = 3;

    assertThat(chunk3.compareTo(chunk4))
        .isEqualTo(Integer.signum(newSizeForChunk3 - newSizeForChunk4));

    chunk1.release();
    chunk4.release();
  }

  @Test
  public void setSerializedShouldSetTheSerializedBit() {
    Object regionEntryValue = getValue();
    byte[] regionEntryValueAsBytes = convertValueToByteArray(regionEntryValue);

    boolean isSerialized = false;
    boolean isCompressed = false;

    OffHeapStoredObject chunk = (OffHeapStoredObject) ma
        .allocateAndInitialize(regionEntryValueAsBytes, isSerialized, isCompressed);

    int headerBeforeSerializedBitSet = AddressableMemoryManager
        .readIntVolatile(chunk.getAddress() + OffHeapStoredObject.REF_COUNT_OFFSET);

    assertThat(chunk.isSerialized()).isFalse();

    chunk.setSerialized(true); // set to true

    assertThat(chunk.isSerialized()).isTrue();

    int headerAfterSerializedBitSet = AddressableMemoryManager
        .readIntVolatile(chunk.getAddress() + OffHeapStoredObject.REF_COUNT_OFFSET);

    assertThat(headerAfterSerializedBitSet)
        .isEqualTo(headerBeforeSerializedBitSet | OffHeapStoredObject.IS_SERIALIZED_BIT);

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void setSerialziedShouldThrowExceptionIfChunkIsAlreadyReleased() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.release();
    chunk.setSerialized(true);

    chunk.release();
  }

  @Test
  public void setCompressedShouldSetTheCompressedBit() {
    Object regionEntryValue = getValue();
    byte[] regionEntryValueAsBytes = convertValueToByteArray(regionEntryValue);

    boolean isSerialized = false;
    boolean isCompressed = false;

    OffHeapStoredObject chunk = (OffHeapStoredObject) ma
        .allocateAndInitialize(regionEntryValueAsBytes, isSerialized, isCompressed);

    int headerBeforeCompressedBitSet = AddressableMemoryManager
        .readIntVolatile(chunk.getAddress() + OffHeapStoredObject.REF_COUNT_OFFSET);

    assertThat(chunk.isCompressed()).isFalse();

    chunk.setCompressed(true); // set to true

    assertThat(chunk.isCompressed()).isTrue();

    int headerAfterCompressedBitSet = AddressableMemoryManager
        .readIntVolatile(chunk.getAddress() + OffHeapStoredObject.REF_COUNT_OFFSET);

    assertThat(headerAfterCompressedBitSet)
        .isEqualTo(headerBeforeCompressedBitSet | OffHeapStoredObject.IS_COMPRESSED_BIT);

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void setCompressedShouldThrowExceptionIfChunkIsAlreadyReleased() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.release();
    chunk.setCompressed(true);

    chunk.release();
  }

  @Test
  public void setDataSizeShouldSetTheDataSizeBits() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());

    int beforeSize = chunk.getDataSize();

    chunk.setDataSize(2);

    int afterSize = chunk.getDataSize();

    assertThat(afterSize).isEqualTo(2);
    assertThat(afterSize).isNotEqualTo(beforeSize);

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void setDataSizeShouldThrowExceptionIfChunkIsAlreadyReleased() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.release();
    chunk.setDataSize(1);

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void initializeUseCountShouldThrowIllegalStateExceptionIfChunkIsAlreadyRetained() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.retain();
    chunk.initializeUseCount();

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void initializeUseCountShouldThrowIllegalStateExceptionIfChunkIsAlreadyReleased() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.release();
    chunk.initializeUseCount();

    chunk.release();
  }

  @Test
  public void isSerializedPdxInstanceShouldReturnTrueIfItsPDXInstance() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());

    byte[] serailizedValue = chunk.getSerializedValue();
    serailizedValue[0] = DSCODE.PDX.toByte();
    chunk.setSerializedValue(serailizedValue);

    assertThat(chunk.isSerializedPdxInstance()).isTrue();

    serailizedValue = chunk.getSerializedValue();
    serailizedValue[0] = DSCODE.PDX_ENUM.toByte();
    chunk.setSerializedValue(serailizedValue);

    assertThat(chunk.isSerializedPdxInstance()).isTrue();

    serailizedValue = chunk.getSerializedValue();
    serailizedValue[0] = DSCODE.PDX_INLINE_ENUM.toByte();
    chunk.setSerializedValue(serailizedValue);

    assertThat(chunk.isSerializedPdxInstance()).isTrue();

    chunk.release();
  }

  @Test
  public void isSerializedPdxInstanceShouldReturnFalseIfItsNotPDXInstance() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());
    assertThat(chunk.isSerializedPdxInstance()).isFalse();

    chunk.release();
  }

  @Test
  public void checkDataEqualsByChunk() {
    OffHeapStoredObject chunk1 = createValueAsSerializedStoredObject(getValue());
    OffHeapStoredObject sameAsChunk1 = chunk1;

    assertThat(chunk1.checkDataEquals(sameAsChunk1)).isTrue();

    OffHeapStoredObject unserializedChunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk1.checkDataEquals(unserializedChunk)).isFalse();

    OffHeapStoredObject chunkDifferBySize = createValueAsSerializedStoredObject(getValue());
    chunkDifferBySize.setSize(0);
    assertThat(chunk1.checkDataEquals(chunkDifferBySize)).isFalse();

    OffHeapStoredObject chunkDifferByValue =
        createValueAsSerializedStoredObject(Long.MAX_VALUE - 1);
    assertThat(chunk1.checkDataEquals(chunkDifferByValue)).isFalse();

    OffHeapStoredObject newChunk1 = createValueAsSerializedStoredObject(getValue());
    assertThat(chunk1.checkDataEquals(newChunk1)).isTrue();

    chunk1.release();
    unserializedChunk.release();
    chunkDifferBySize.release();
    chunkDifferByValue.release();
    newChunk1.release();
  }

  @Test
  public void checkDataEqualsBySerializedValue() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());
    assertThat(chunk.checkDataEquals(new byte[1])).isFalse();

    OffHeapStoredObject chunkDifferByValue =
        createValueAsSerializedStoredObject(Long.MAX_VALUE - 1);
    assertThat(chunk.checkDataEquals(chunkDifferByValue.getSerializedValue())).isFalse();

    OffHeapStoredObject newChunk = createValueAsSerializedStoredObject(getValue());
    assertThat(chunk.checkDataEquals(newChunk.getSerializedValue())).isTrue();

    chunk.release();
    chunkDifferByValue.release();
    newChunk.release();
  }

  @Test
  public void getDecompressedBytesShouldReturnDecompressedBytesIfCompressed() {
    Object regionEntryValue = getValue();
    byte[] regionEntryValueAsBytes = convertValueToByteArray(regionEntryValue);

    boolean isSerialized = true;
    boolean isCompressed = true;

    OffHeapStoredObject chunk = (OffHeapStoredObject) ma
        .allocateAndInitialize(regionEntryValueAsBytes, isSerialized, isCompressed);

    RegionEntryContext regionContext = mock(RegionEntryContext.class);
    CachePerfStats cacheStats = mock(CachePerfStats.class);
    Compressor compressor = mock(Compressor.class);

    long startTime = 10000L;

    // mock required things
    when(regionContext.getCompressor()).thenReturn(compressor);
    when(compressor.decompress(regionEntryValueAsBytes)).thenReturn(regionEntryValueAsBytes);
    when(regionContext.getCachePerfStats()).thenReturn(cacheStats);
    when(cacheStats.startDecompression()).thenReturn(startTime);

    // invoke the thing
    byte[] bytes = chunk.getDecompressedBytes(regionContext);

    // verify the thing happened
    verify(cacheStats, atLeastOnce()).startDecompression();
    verify(compressor, times(1)).decompress(regionEntryValueAsBytes);
    verify(cacheStats, atLeastOnce()).endDecompression(startTime);

    assertArrayEquals(regionEntryValueAsBytes, bytes);

    chunk.release();
  }

  @Test
  public void incSizeShouldIncrementSize() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());

    int beforeSize = chunk.getSize();

    chunk.incSize(1);
    assertThat(chunk.getSize()).isEqualTo(beforeSize + 1);

    chunk.incSize(2);
    assertThat(chunk.getSize()).isEqualTo(beforeSize + 1 + 2);

    chunk.release();
  }

  @Test
  public void readyForFreeShouldResetTheRefCount() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());

    int refCountBeforeFreeing = chunk.getRefCount();
    assertThat(refCountBeforeFreeing).isEqualTo(1);

    chunk.readyForFree();

    int refCountAfterFreeing = chunk.getRefCount();
    assertThat(refCountAfterFreeing).isEqualTo(0);
  }

  @Test(expected = IllegalStateException.class)
  public void readyForAllocationShouldThrowExceptionIfAlreadyAllocated() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());

    // chunk is already allocated when we created it, so calling readyForAllocation should throw
    // exception.
    chunk.readyForAllocation();

    chunk.release();
  }

  @Test
  public void checkIsAllocatedShouldReturnIfAllocated() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());
    chunk.checkIsAllocated();

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void checkIsAllocatedShouldThrowExceptionIfNotAllocated() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());
    chunk.release();
    chunk.checkIsAllocated();

    chunk.release();
  }

  @Test
  public void sendToShouldWriteSerializedValueToDataOutputIfValueIsSerialized() throws IOException {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());
    OffHeapStoredObject spyChunk = spy(chunk);

    HeapDataOutputStream dataOutput = mock(HeapDataOutputStream.class);
    ByteBuffer directByteBuffer = ByteBuffer.allocate(1024);

    doReturn(directByteBuffer).when(spyChunk).createDirectByteBuffer();
    doNothing().when(dataOutput).write(directByteBuffer);

    spyChunk.sendTo(dataOutput);

    verify(dataOutput, times(1)).write(directByteBuffer);

    chunk.release();
  }

  @Test
  public void sendToShouldWriteUnserializedValueToDataOutputIfValueIsUnserialized()
      throws IOException {
    byte[] regionEntryValue = getValueAsByteArray();
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(regionEntryValue);

    // writeByte is a final method and cannot be mocked, so creating a real one
    HeapDataOutputStream dataOutput = new HeapDataOutputStream(KnownVersion.CURRENT);

    chunk.sendTo(dataOutput);

    byte[] actual = dataOutput.toByteArray();

    byte[] expected = new byte[regionEntryValue.length + 2];
    expected[0] = DSCODE.BYTE_ARRAY.toByte();
    expected[1] = (byte) regionEntryValue.length;
    System.arraycopy(regionEntryValue, 0, expected, 2, regionEntryValue.length);

    assertNotNull(dataOutput);
    assertThat(actual).isEqualTo(expected);

    chunk.release();
  }

  @Test
  public void sendAsByteArrayShouldWriteValueToDataOutput() throws IOException {
    byte[] regionEntryValue = getValueAsByteArray();
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(regionEntryValue);

    // writeByte is a final method and cannot be mocked, so creating a real one
    HeapDataOutputStream dataOutput = new HeapDataOutputStream(KnownVersion.CURRENT);

    chunk.sendAsByteArray(dataOutput);

    byte[] actual = dataOutput.toByteArray();

    byte[] expected = new byte[regionEntryValue.length + 1];
    expected[0] = (byte) regionEntryValue.length;
    System.arraycopy(regionEntryValue, 0, expected, 1, regionEntryValue.length);

    assertNotNull(dataOutput);
    assertThat(actual).isEqualTo(expected);

    chunk.release();
  }

  @Test
  public void createDirectByteBufferShouldCreateAByteBuffer() {
    byte[] regionEntryValue = getValueAsByteArray();

    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(regionEntryValue);

    ByteBuffer buffer = chunk.createDirectByteBuffer();

    byte[] actual = new byte[regionEntryValue.length];
    buffer.get(actual);

    assertArrayEquals(regionEntryValue, actual);

    chunk.release();
  }

  @Test
  public void getDirectByteBufferShouldCreateAByteBuffer() {
    byte[] regionEntryValue = getValueAsByteArray();
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(regionEntryValue);

    ByteBuffer buffer = chunk.createDirectByteBuffer();
    long bufferAddress = AddressableMemoryManager.getDirectByteBufferAddress(buffer);

    // returned address should be starting of the value (after skipping HEADER_SIZE bytes)
    assertEquals(chunk.getAddress() + OffHeapStoredObject.HEADER_SIZE, bufferAddress);

    chunk.release();
  }

  @Test(expected = AssertionError.class)
  public void getAddressForReadingDataShouldFailIfItsOutsideOfChunk() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());
    chunk.getAddressForReadingData(0, chunk.getDataSize() + 1);

    chunk.release();
  }

  @Test
  public void getAddressForReadingDataShouldReturnDataAddressFromGivenOffset() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());

    int offset = 1;
    long requestedAddress = chunk.getAddressForReadingData(offset, 1);

    assertThat(requestedAddress).isEqualTo(chunk.getBaseDataAddress() + offset);

    chunk.release();
  }

  @Test
  public void getSizeInBytesShouldReturnSize() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());
    assertThat(chunk.getSizeInBytes()).isEqualTo(chunk.getSize());

    chunk.release();
  }

  @Test(expected = AssertionError.class)
  public void getAddressForReadingDataShouldFailIfOffsetIsNegative() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());
    chunk.getAddressForReadingData(-1, 1);

    chunk.release();
  }

  @Test(expected = AssertionError.class)
  public void getAddressForReadingDataShouldFailIfSizeIsNegative() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());
    chunk.getAddressForReadingData(1, -1);

    chunk.release();
  }

  @Test(expected = AssertionError.class)
  public void readByteAndWriteByteShouldFailIfOffsetIsOutside() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());

    chunk.readDataByte(chunk.getDataSize() + 1);

    chunk.writeDataByte(chunk.getDataSize() + 1, Byte.MAX_VALUE);

    chunk.release();
  }

  @Test
  public void writeByteShouldWriteAtCorrectLocation() {
    OffHeapStoredObject chunk = createValueAsSerializedStoredObject(getValue());

    byte valueBeforeWrite = chunk.readDataByte(2);

    Byte expected = Byte.MAX_VALUE;
    chunk.writeDataByte(2, expected);

    Byte actual = chunk.readDataByte(2);

    assertThat(actual).isNotEqualTo(valueBeforeWrite);
    assertThat(actual).isEqualTo(expected);

    chunk.release();
  }

  @Test
  public void retainShouldIncrementRefCount() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.getRefCount()).isEqualTo(1);

    chunk.retain();
    assertThat(chunk.getRefCount()).isEqualTo(2);

    chunk.retain();
    assertThat(chunk.getRefCount()).isEqualTo(3);

    chunk.release();
    chunk.release();
    chunk.release();
    boolean retainAfterRelease = chunk.retain();

    assertThat(retainAfterRelease).isFalse();
  }

  @Test(expected = IllegalStateException.class)
  public void retainShouldThrowExceptionAfterMaxNumberOfTimesRetained() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());

    // loop though and invoke retain for MAX_REF_COUNT-1 times, as create chunk above counted as one
    // reference
    for (int i = 0; i < OffHeapStoredObject.MAX_REF_COUNT - 1; i++) {
      chunk.retain();
    }

    // invoke for the one more time should throw exception
    chunk.retain();
  }

  @Test
  public void releaseShouldDecrementRefCount() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.getRefCount()).isEqualTo(1);

    chunk.retain();
    chunk.retain();
    assertThat(chunk.getRefCount()).isEqualTo(3);

    chunk.release();
    assertThat(chunk.getRefCount()).isEqualTo(2);

    chunk.release();
    assertThat(chunk.getRefCount()).isEqualTo(1);

    chunk.retain();
    chunk.release();
    assertThat(chunk.getRefCount()).isEqualTo(1);

    chunk.release();
    assertThat(chunk.getRefCount()).isEqualTo(0);
  }

  @Test(expected = IllegalStateException.class)
  public void releaseShouldThrowExceptionIfChunkIsAlreadyReleased() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.release();
    chunk.release();
  }

  @Test
  public void testToString() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());

    String expected = ":<dataSize=" + chunk.getDataSize() + " refCount=" + chunk.getRefCount()
        + " addr=" + Long.toHexString(chunk.getAddress()) + ">";
    assertThat(chunk.toString()).endsWith(expected);

    chunk.release();
  }

  @Test
  public void getStateShouldReturnAllocatedIfRefCountIsGreaterThanZero() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    assertEquals(State.ALLOCATED, chunk.getState());

    chunk.release();
  }

  @Test
  public void getStateShouldReturnDeallocatedIfRefCountIsZero() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.release();
    assertEquals(State.DEALLOCATED, chunk.getState());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getNextBlockShouldThrowUnSupportedOperationException() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.getNextBlock();

    chunk.release();
  }

  @Test
  public void getBlockSizeShouldBeSameSameGetSize() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    assertEquals(chunk.getSize(), chunk.getBlockSize());

    chunk.release();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getSlabIdShouldThrowUnSupportedOperationException() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.getSlabId();

    chunk.release();
  }

  @Test
  public void getFreeListIdShouldReturnMinusOne() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.getFreeListId()).isEqualTo(-1);

    chunk.release();
  }

  @Test
  public void getDataTypeShouldReturnNull() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.getDataType()).isNull();

    chunk.release();
  }

  @Test
  public void getDataDataShouldReturnNull() {
    OffHeapStoredObject chunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.getDataValue()).isNull();
  }

  @Test(expected = AssertionError.class)
  public void getRawBytesShouldThrowExceptionIfValueIsCompressed() {
    Object regionEntryValue = getValue();
    byte[] regionEntryValueAsBytes = convertValueToByteArray(regionEntryValue);

    boolean isSerialized = true;
    boolean isCompressed = true;

    OffHeapStoredObject chunk = (OffHeapStoredObject) ma
        .allocateAndInitialize(regionEntryValueAsBytes, isSerialized, isCompressed);

    chunk.getRawBytes();

    chunk.release();
  }

  @Test
  public void getSerializedValueShouldSerializeTheValue() {
    Object regionEntryValue = getValue();
    byte[] regionEntryValueAsBytes = convertValueToByteArray(regionEntryValue);

    boolean isSerialized = false;
    boolean isCompressed = false;

    OffHeapStoredObject chunk = (OffHeapStoredObject) ma
        .allocateAndInitialize(regionEntryValueAsBytes, isSerialized, isCompressed);

    byte[] serializedValue = chunk.getSerializedValue();

    assertThat(serializedValue).isEqualTo(EntryEventImpl.serialize(regionEntryValueAsBytes));

    chunk.release();
  }

  @Test
  public void fillShouldFillTheChunk() {
    boolean isSerialized = false;
    boolean isCompressed = false;

    OffHeapStoredObject chunk =
        (OffHeapStoredObject) ma.allocateAndInitialize(new byte[100], isSerialized, isCompressed);

    // first fill the unused part with FILL_PATTERN
    OffHeapStoredObject.fill(chunk.getAddress());

    // Validate that it is filled
    chunk.validateFill();

    chunk.release();
  }
}
