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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.BytesAndBitsForCompactor;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.RegionEntryContext;
import com.gemstone.gemfire.internal.offheap.MemoryBlock.State;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class GemFireChunkJUnitTest extends AbstractStoredObjectTestBase {

  private MemoryAllocator ma;

  static {
    ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
  }

  @Before
  public void setUp() {
    OutOfOffHeapMemoryListener ooohml = mock(OutOfOffHeapMemoryListener.class);
    OffHeapMemoryStats stats = mock(OffHeapMemoryStats.class);
    LogWriter lw = mock(LogWriter.class);

    ma = SimpleMemoryAllocatorImpl.create(ooohml, stats, lw, 3, OffHeapStorage.MIN_SLAB_SIZE * 3, OffHeapStorage.MIN_SLAB_SIZE);
  }

  @After
  public void tearDown() {
    SimpleMemoryAllocatorImpl.freeOffHeapMemory();
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

  @Test
  public void chunkCanBeCreatedFromAnotherChunk() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());

    GemFireChunk newChunk = new GemFireChunk(chunk);

    assertNotNull(newChunk);
    assertThat(newChunk.getMemoryAddress()).isEqualTo(chunk.getMemoryAddress());

    chunk.release();
  }

  @Test
  public void chunkCanBeCreatedWithOnlyMemoryAddress() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());

    GemFireChunk newChunk = new GemFireChunk(chunk.getMemoryAddress());

    assertNotNull(newChunk);
    assertThat(newChunk.getMemoryAddress()).isEqualTo(chunk.getMemoryAddress());

    chunk.release();
  }

  @Test
  public void chunkSliceCanBeCreatedFromAnotherChunk() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());

    int position = 1;
    int end = 2;

    GemFireChunk newChunk = (GemFireChunk) chunk.slice(position, end);

    assertNotNull(newChunk);
    assertThat(newChunk.getClass()).isEqualTo(GemFireChunkSlice.class);
    assertThat(newChunk.getMemoryAddress()).isEqualTo(chunk.getMemoryAddress());

    chunk.release();
  }

  @Test
  public void fillSerializedValueShouldFillWrapperWithSerializedValueIfValueIsSerialized() {
    GemFireChunk chunk = createValueAsSerializedStoredObject(getValue());

    // mock the things
    BytesAndBitsForCompactor wrapper = mock(BytesAndBitsForCompactor.class);

    byte userBits = 0;
    byte serializedUserBits = 1;
    chunk.fillSerializedValue(wrapper, userBits);

    verify(wrapper, times(1)).setChunkData(chunk, serializedUserBits);

    chunk.release();
  }

  @Test
  public void fillSerializedValueShouldFillWrapperWithDeserializedValueIfValueIsNotSerialized() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());

    // mock the things
    BytesAndBitsForCompactor wrapper = mock(BytesAndBitsForCompactor.class);

    byte userBits = 1;
    chunk.fillSerializedValue(wrapper, userBits);

    verify(wrapper, times(1)).setChunkData(chunk, userBits);

    chunk.release();
  }

  @Test
  public void getShortClassNameShouldReturnShortClassName() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.getShortClassName()).isEqualTo("GemFireChunk");

    chunk.release();
  }

  @Test
  public void chunksAreEqualsOnlyByAddress() {
    GemFireChunk chunk = createValueAsSerializedStoredObject(getValue());

    GemFireChunk newChunk = new GemFireChunk(chunk.getMemoryAddress());
    assertThat(chunk.equals(newChunk)).isTrue();

    GemFireChunk chunkWithSameValue = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.equals(chunkWithSameValue)).isFalse();

    Object someObject = getValue();
    assertThat(chunk.equals(someObject)).isFalse();

    chunk.release();
    chunkWithSameValue.release();
  }

  @Test
  public void chunksShouldBeComparedBySize() {
    GemFireChunk chunk1 = createValueAsSerializedStoredObject(getValue());

    GemFireChunk chunk2 = chunk1;
    assertThat(chunk1.compareTo(chunk2)).isEqualTo(0);

    GemFireChunk chunkWithSameValue = createValueAsSerializedStoredObject(getValue());
    assertThat(chunk1.compareTo(chunkWithSameValue)).isEqualTo(Long.signum(chunk1.getMemoryAddress() - chunkWithSameValue.getMemoryAddress()));

    GemFireChunk chunk3 = createValueAsSerializedStoredObject(Long.MAX_VALUE);
    GemFireChunk chunk4 = createValueAsSerializedStoredObject(Long.MAX_VALUE);

    int newSizeForChunk3 = 2;
    int newSizeForChunk4 = 3;

    assertThat(chunk3.compareTo(chunk4)).isEqualTo(Integer.signum(newSizeForChunk3 - newSizeForChunk4));

    chunk1.release();
    chunk4.release();
  }

  @Test
  public void setSerializedShouldSetTheSerializedBit() {
    Object regionEntryValue = getValue();
    byte[] regionEntryValueAsBytes = convertValueToByteArray(regionEntryValue);

    boolean isSerialized = false;
    boolean isCompressed = false;

    GemFireChunk chunk = (GemFireChunk) ma.allocateAndInitialize(regionEntryValueAsBytes, isSerialized, isCompressed, GemFireChunk.TYPE);

    int headerBeforeSerializedBitSet = UnsafeMemoryChunk.readAbsoluteIntVolatile(chunk.getMemoryAddress() + 4/* REF_COUNT_OFFSET */);

    assertThat(chunk.isSerialized()).isFalse();

    chunk.setSerialized(true); // set to true

    assertThat(chunk.isSerialized()).isTrue();

    int headerAfterSerializedBitSet = UnsafeMemoryChunk.readAbsoluteIntVolatile(chunk.getMemoryAddress() + 4/* REF_COUNT_OFFSET */);

    assertThat(headerAfterSerializedBitSet).isEqualTo(headerBeforeSerializedBitSet | 0x80000000/* IS_SERIALIZED_BIT */);

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void setSerialziedShouldThrowExceptionIfChunkIsAlreadyReleased() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
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

    GemFireChunk chunk = (GemFireChunk) ma.allocateAndInitialize(regionEntryValueAsBytes, isSerialized, isCompressed, GemFireChunk.TYPE);

    int headerBeforeCompressedBitSet = UnsafeMemoryChunk.readAbsoluteIntVolatile(chunk.getMemoryAddress() + 4/* REF_COUNT_OFFSET */);

    assertThat(chunk.isCompressed()).isFalse();

    chunk.setCompressed(true); // set to true

    assertThat(chunk.isCompressed()).isTrue();

    int headerAfterCompressedBitSet = UnsafeMemoryChunk.readAbsoluteIntVolatile(chunk.getMemoryAddress() + 4/* REF_COUNT_OFFSET */);

    assertThat(headerAfterCompressedBitSet).isEqualTo(headerBeforeCompressedBitSet | 0x40000000/* IS_SERIALIZED_BIT */);

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void setCompressedShouldThrowExceptionIfChunkIsAlreadyReleased() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.release();
    chunk.setCompressed(true);

    chunk.release();
  }

  @Test
  public void setDataSizeShouldSetTheDataSizeBits() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());

    int beforeSize = chunk.getDataSize();

    chunk.setDataSize(2);

    int afterSize = chunk.getDataSize();

    assertThat(afterSize).isEqualTo(2);
    assertThat(afterSize).isNotEqualTo(beforeSize);

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void setDataSizeShouldThrowExceptionIfChunkIsAlreadyReleased() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.release();
    chunk.setDataSize(1);

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void initializeUseCountShouldThrowIllegalStateExceptionIfChunkIsAlreadyRetained() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.retain();
    chunk.initializeUseCount();

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void initializeUseCountShouldThrowIllegalStateExceptionIfChunkIsAlreadyReleased() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.release();
    chunk.initializeUseCount();

    chunk.release();
  }

  @Test
  public void isSerializedPdxInstanceShouldReturnTrueIfItsPDXInstance() {
    GemFireChunk chunk = createValueAsSerializedStoredObject(getValue());

    byte[] serailizedValue = chunk.getSerializedValue();
    serailizedValue[0] = DSCODE.PDX;
    chunk.setSerializedValue(serailizedValue);

    assertThat(chunk.isSerializedPdxInstance()).isTrue();

    serailizedValue = chunk.getSerializedValue();
    serailizedValue[0] = DSCODE.PDX_ENUM;
    chunk.setSerializedValue(serailizedValue);

    assertThat(chunk.isSerializedPdxInstance()).isTrue();

    serailizedValue = chunk.getSerializedValue();
    serailizedValue[0] = DSCODE.PDX_INLINE_ENUM;
    chunk.setSerializedValue(serailizedValue);

    assertThat(chunk.isSerializedPdxInstance()).isTrue();

    chunk.release();
  }

  @Test
  public void isSerializedPdxInstanceShouldReturnFalseIfItsNotPDXInstance() {
    GemFireChunk chunk = createValueAsSerializedStoredObject(getValue());
    assertThat(chunk.isSerializedPdxInstance()).isFalse();

    chunk.release();
  }

  @Test
  public void checkDataEqualsByChunk() {
    GemFireChunk chunk1 = createValueAsSerializedStoredObject(getValue());
    GemFireChunk sameAsChunk1 = chunk1;

    assertThat(chunk1.checkDataEquals(sameAsChunk1)).isTrue();

    GemFireChunk unserializedChunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk1.checkDataEquals(unserializedChunk)).isFalse();

    GemFireChunk chunkDifferBySize = createValueAsSerializedStoredObject(getValue());
    chunkDifferBySize.setSize(0);
    assertThat(chunk1.checkDataEquals(chunkDifferBySize)).isFalse();

    GemFireChunk chunkDifferByValue = createValueAsSerializedStoredObject(Long.MAX_VALUE - 1);
    assertThat(chunk1.checkDataEquals(chunkDifferByValue)).isFalse();

    GemFireChunk newChunk1 = createValueAsSerializedStoredObject(getValue());
    assertThat(chunk1.checkDataEquals(newChunk1)).isTrue();

    chunk1.release();
    unserializedChunk.release();
    chunkDifferBySize.release();
    chunkDifferByValue.release();
    newChunk1.release();
  }

  @Test
  public void checkDataEqualsBySerializedValue() {
    GemFireChunk chunk = createValueAsSerializedStoredObject(getValue());
    assertThat(chunk.checkDataEquals(new byte[1])).isFalse();

    GemFireChunk chunkDifferByValue = createValueAsSerializedStoredObject(Long.MAX_VALUE - 1);
    assertThat(chunk.checkDataEquals(chunkDifferByValue.getSerializedValue())).isFalse();

    GemFireChunk newChunk = createValueAsSerializedStoredObject(getValue());
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

    GemFireChunk chunk = (GemFireChunk) ma.allocateAndInitialize(regionEntryValueAsBytes, isSerialized, isCompressed, GemFireChunk.TYPE);

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
    GemFireChunk chunk = createValueAsSerializedStoredObject(getValue());

    int beforeSize = chunk.getSize();

    chunk.incSize(1);
    assertThat(chunk.getSize()).isEqualTo(beforeSize + 1);

    chunk.incSize(2);
    assertThat(chunk.getSize()).isEqualTo(beforeSize + 1 + 2);

    chunk.release();
  }

  @Test
  public void readyForFreeShouldResetTheRefCount() {
    Chunk chunk = createValueAsSerializedStoredObject(getValue());

    int refCountBeforeFreeing = chunk.getRefCount();
    assertThat(refCountBeforeFreeing).isEqualTo(1);

    chunk.readyForFree();

    int refCountAfterFreeing = chunk.getRefCount();
    assertThat(refCountAfterFreeing).isEqualTo(0);
  }

  @Test(expected = IllegalStateException.class)
  public void readyForAllocationShouldThrowExceptionIfAlreadyAllocated() {
    Chunk chunk = createValueAsSerializedStoredObject(getValue());

    // chunk is already allocated when we created it, so calling readyForAllocation should throw exception.
    chunk.readyForAllocation(GemFireChunk.TYPE);

    chunk.release();
  }

  @Test
  public void checkIsAllocatedShouldReturnIfAllocated() {
    Chunk chunk = createValueAsSerializedStoredObject(getValue());
    chunk.checkIsAllocated();

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void checkIsAllocatedShouldThrowExceptionIfNotAllocated() {
    Chunk chunk = createValueAsSerializedStoredObject(getValue());
    chunk.release();
    chunk.checkIsAllocated();

    chunk.release();
  }

  @Test
  public void sendToShouldWriteSerializedValueToDataOutputIfValueIsSerialized() throws IOException {
    Chunk chunk = createValueAsSerializedStoredObject(getValue());
    Chunk spyChunk = spy(chunk);

    HeapDataOutputStream dataOutput = mock(HeapDataOutputStream.class);
    ByteBuffer directByteBuffer = ByteBuffer.allocate(1024);

    doReturn(directByteBuffer).when(spyChunk).createDirectByteBuffer();
    doNothing().when(dataOutput).write(directByteBuffer);

    spyChunk.sendTo(dataOutput);

    verify(dataOutput, times(1)).write(directByteBuffer);

    chunk.release();
  }

  @Test
  public void sendToShouldWriteUnserializedValueToDataOutputIfValueIsUnserialized() throws IOException {
    byte[] regionEntryValue = getValueAsByteArray();
    GemFireChunk chunk = createValueAsUnserializedStoredObject(regionEntryValue);

    // writeByte is a final method and cannot be mocked, so creating a real one
    HeapDataOutputStream dataOutput = new HeapDataOutputStream(Version.CURRENT);

    chunk.sendTo(dataOutput);

    byte[] actual = dataOutput.toByteArray();

    byte[] expected = new byte[regionEntryValue.length + 2];
    expected[0] = DSCODE.BYTE_ARRAY;
    expected[1] = (byte) regionEntryValue.length;
    System.arraycopy(regionEntryValue, 0, expected, 2, regionEntryValue.length);

    assertNotNull(dataOutput);
    assertThat(actual).isEqualTo(expected);

    chunk.release();
  }

  @Test
  public void sendAsByteArrayShouldWriteValueToDataOutput() throws IOException {
    byte[] regionEntryValue = getValueAsByteArray();
    GemFireChunk chunk = createValueAsUnserializedStoredObject(regionEntryValue);

    // writeByte is a final method and cannot be mocked, so creating a real one
    HeapDataOutputStream dataOutput = new HeapDataOutputStream(Version.CURRENT);

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

    GemFireChunk chunk = createValueAsUnserializedStoredObject(regionEntryValue);

    ByteBuffer buffer = chunk.createDirectByteBuffer();

    byte[] actual = new byte[regionEntryValue.length];
    buffer.get(actual);

    assertArrayEquals(regionEntryValue, actual);

    chunk.release();
  }

  @Test
  public void getDirectByteBufferShouldCreateAByteBuffer() {
    byte[] regionEntryValue = getValueAsByteArray();
    GemFireChunk chunk = createValueAsUnserializedStoredObject(regionEntryValue);

    ByteBuffer buffer = chunk.createDirectByteBuffer();
    long bufferAddress = Chunk.getDirectByteBufferAddress(buffer);

    // returned address should be starting of the value (after skipping HEADER_SIZE bytes)
    assertEquals(chunk.getMemoryAddress() + Chunk.OFF_HEAP_HEADER_SIZE, bufferAddress);

    chunk.release();
  }

  @Test(expected = AssertionError.class)
  public void getAddressForReadingShouldFailIfItsOutsideOfChunk() {
    GemFireChunk chunk = createValueAsSerializedStoredObject(getValue());
    chunk.getAddressForReading(0, chunk.getDataSize() + 1);

    chunk.release();
  }

  @Test
  public void getAddressForReadingShouldReturnDataAddressFromGivenOffset() {
    GemFireChunk chunk = createValueAsSerializedStoredObject(getValue());

    int offset = 1;
    long requestedAddress = chunk.getAddressForReading(offset, 1);

    assertThat(requestedAddress).isEqualTo(chunk.getBaseDataAddress() + offset);

    chunk.release();
  }

  @Test
  public void getSizeInBytesShouldReturnSize() {
    GemFireChunk chunk = createValueAsSerializedStoredObject(getValue());
    assertThat(chunk.getSizeInBytes()).isEqualTo(chunk.getSize());

    chunk.release();
  }

  @Test(expected = AssertionError.class)
  public void getUnsafeAddressShouldFailIfOffsetIsNegative() {
    GemFireChunk chunk = createValueAsSerializedStoredObject(getValue());
    chunk.getUnsafeAddress(-1, 1);

    chunk.release();
  }

  @Test(expected = AssertionError.class)
  public void getUnsafeAddressShouldFailIfSizeIsNegative() {
    GemFireChunk chunk = createValueAsSerializedStoredObject(getValue());
    chunk.getUnsafeAddress(1, -1);

    chunk.release();
  }

  @Test(expected = AssertionError.class)
  public void getUnsafeAddressShouldFailIfItsOutsideOfChunk() {
    GemFireChunk chunk = createValueAsSerializedStoredObject(getValue());
    chunk.getUnsafeAddress(0, chunk.getDataSize() + 1);

    chunk.release();
  }

  @Test
  public void getUnsafeAddressShouldReturnUnsafeAddress() {
    GemFireChunk chunk = createValueAsSerializedStoredObject(getValue());

    int offset = 1;
    long unsafeAddress = chunk.getUnsafeAddress(offset, 1);

    assertThat(unsafeAddress).isEqualTo(chunk.getBaseDataAddress() + offset);

    chunk.release();
  }

  @Test(expected = AssertionError.class)
  public void readByteAndWriteByteShouldFailIfOffsetIsOutside() {
    GemFireChunk chunk = createValueAsSerializedStoredObject(getValue());

    chunk.readByte(chunk.getDataSize() + 1);

    chunk.writeByte(chunk.getDataSize() + 1, Byte.MAX_VALUE);

    chunk.release();
  }

  @Test
  public void writeByteShouldWriteAtCorrectLocation() {
    GemFireChunk chunk = createValueAsSerializedStoredObject(getValue());

    byte valueBeforeWrite = chunk.readByte(2);

    Byte expected = Byte.MAX_VALUE;
    chunk.writeByte(2, expected);

    Byte actual = chunk.readByte(2);

    assertThat(actual).isNotEqualTo(valueBeforeWrite);
    assertThat(actual).isEqualTo(expected);

    chunk.release();
  }

  @Test
  public void retainShouldIncrementRefCount() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
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
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());

    // max retain Chunk.MAX_REF_COUNT
    int MAX_REF_COUNT = 0xFFFF;

    // loop though and invoke retain for MAX_REF_COUNT-1 times, as create chunk above counted as one reference
    for (int i = 0; i < MAX_REF_COUNT - 1; i++)
      chunk.retain();

    // invoke for the one more time should throw exception
    chunk.retain();
  }

  @Test
  public void releaseShouldDecrementRefCount() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
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
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.release();
    chunk.release();
  }

  @Test
  public void testToStringForOffHeapByteSource() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());

    String expected = ":<dataSize=" + chunk.getDataSize() + " refCount=" + chunk.getRefCount() + " addr=" + Long.toHexString(chunk.getMemoryAddress()) + ">";
    assertThat(chunk.toStringForOffHeapByteSource()).endsWith(expected);

    // test toString
    Chunk spy = spy(chunk);
    spy.toString();
    verify(spy, times(1)).toStringForOffHeapByteSource();

    chunk.release();
  }

  @Test
  public void getStateShouldReturnAllocatedIfRefCountIsGreaterThanZero() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
    assertEquals(State.ALLOCATED, chunk.getState());

    chunk.release();
  }

  @Test
  public void getStateShouldReturnDeallocatedIfRefCountIsZero() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.release();
    assertEquals(State.DEALLOCATED, chunk.getState());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getNextBlockShouldThrowUnSupportedOperationException() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.getNextBlock();

    chunk.release();
  }

  @Test
  public void getBlockSizeShouldBeSameSameGetSize() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
    assertEquals(chunk.getSize(), chunk.getBlockSize());

    chunk.release();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void copyBytesShouldThrowUnSupportedOperationException() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.copyBytes(1, 2, 1);

    chunk.release();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getSlabIdShouldThrowUnSupportedOperationException() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.getSlabId();

    chunk.release();
  }

  @Test
  public void getFreeListIdShouldReturnMinusOne() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.getFreeListId()).isEqualTo(-1);

    chunk.release();
  }

  @Test
  public void getDataTypeShouldReturnNull() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.getDataType()).isNull();

    chunk.release();
  }

  @Test
  public void getDataDataShouldReturnNull() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.getDataValue()).isNull();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getRawBytesShouldThrowExceptionIfValueIsCompressed() {
    Object regionEntryValue = getValue();
    byte[] regionEntryValueAsBytes = convertValueToByteArray(regionEntryValue);

    boolean isSerialized = true;
    boolean isCompressed = true;

    GemFireChunk chunk = (GemFireChunk) ma.allocateAndInitialize(regionEntryValueAsBytes, isSerialized, isCompressed, GemFireChunk.TYPE);

    chunk.getRawBytes();

    chunk.release();
  }

  @Test
  public void getSerializedValueShouldSerializeTheValue() {
    Object regionEntryValue = getValue();
    byte[] regionEntryValueAsBytes = convertValueToByteArray(regionEntryValue);

    boolean isSerialized = false;
    boolean isCompressed = false;

    GemFireChunk chunk = (GemFireChunk) ma.allocateAndInitialize(regionEntryValueAsBytes, isSerialized, isCompressed, GemFireChunk.TYPE);

    byte[] serializedValue = chunk.getSerializedValue();

    assertThat(serializedValue).isEqualTo(EntryEventImpl.serialize(regionEntryValueAsBytes));

    chunk.release();
  }

  @Test
  public void getSrcTypeOrdinalFromAddressShouldReturnOrdinal() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());

    assertThat(Chunk.getSrcTypeOrdinal(chunk.getMemoryAddress())).isEqualTo(4);

    chunk.release();
  }

  @Test
  public void getSrcTypeOrdinalFromRawBitsShouldReturnOrdinal() {
    GemFireChunk chunk = createValueAsUnserializedStoredObject(getValue());

    int rawBits = UnsafeMemoryChunk.readAbsoluteIntVolatile(chunk.getMemoryAddress() + 4 /* REF_COUNT_OFFSET */);
    assertThat(Chunk.getSrcTypeOrdinalFromRawBits(rawBits)).isEqualTo(4);

    chunk.release();
  }

  @Test
  public void fillShouldFillTheUnusedSpace() {
    Object regionEntryValue = getValue();
    byte[] regionEntryValueAsBytes = convertValueToByteArray(regionEntryValue);

    boolean isSerialized = false;
    boolean isCompressed = false;

    GemFireChunk chunk = (GemFireChunk) ma.allocateAndInitialize(new byte[100], isSerialized, isCompressed, GemFireChunk.TYPE);
    chunk.setSerializedValue(regionEntryValueAsBytes);

    // first fill the unused part with FILL_PATTERN
    Chunk.fill(chunk.getMemoryAddress());

    // Validate that it is filled
    chunk.validateFill();

    chunk.release();
  }
}
