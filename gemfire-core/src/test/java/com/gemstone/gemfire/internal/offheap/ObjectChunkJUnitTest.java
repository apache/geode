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
public class ObjectChunkJUnitTest extends AbstractStoredObjectTestBase {

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
  public ObjectChunk createValueAsUnserializedStoredObject(Object value) {
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
  public ObjectChunk createValueAsSerializedStoredObject(Object value) {
    byte[] valueInSerializedByteArray = EntryEventImpl.serialize(value);

    boolean isSerialized = true;
    boolean isCompressed = false;

    return createChunk(valueInSerializedByteArray, isSerialized, isCompressed);
  }

  private ObjectChunk createChunk(byte[] v, boolean isSerialized, boolean isCompressed) {
    ObjectChunk chunk = (ObjectChunk) ma.allocateAndInitialize(v, isSerialized, isCompressed);
    return chunk;
  }

  @Test
  public void chunkCanBeCreatedFromAnotherChunk() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());

    ObjectChunk newChunk = new ObjectChunk(chunk);

    assertNotNull(newChunk);
    assertThat(newChunk.getMemoryAddress()).isEqualTo(chunk.getMemoryAddress());

    chunk.release();
  }

  @Test
  public void chunkCanBeCreatedWithOnlyMemoryAddress() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());

    ObjectChunk newChunk = new ObjectChunk(chunk.getMemoryAddress());

    assertNotNull(newChunk);
    assertThat(newChunk.getMemoryAddress()).isEqualTo(chunk.getMemoryAddress());

    chunk.release();
  }

  @Test
  public void chunkSliceCanBeCreatedFromAnotherChunk() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());

    int position = 1;
    int end = 2;

    ObjectChunk newChunk = (ObjectChunk) chunk.slice(position, end);

    assertNotNull(newChunk);
    assertThat(newChunk.getClass()).isEqualTo(ObjectChunkSlice.class);
    assertThat(newChunk.getMemoryAddress()).isEqualTo(chunk.getMemoryAddress());

    chunk.release();
  }

  @Test
  public void fillSerializedValueShouldFillWrapperWithSerializedValueIfValueIsSerialized() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());

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
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());

    // mock the things
    BytesAndBitsForCompactor wrapper = mock(BytesAndBitsForCompactor.class);

    byte userBits = 1;
    chunk.fillSerializedValue(wrapper, userBits);

    verify(wrapper, times(1)).setChunkData(chunk, userBits);

    chunk.release();
  }

  @Test
  public void getShortClassNameShouldReturnShortClassName() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.getShortClassName()).isEqualTo("ObjectChunk");

    chunk.release();
  }

  @Test
  public void chunksAreEqualsOnlyByAddress() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());

    ObjectChunk newChunk = new ObjectChunk(chunk.getMemoryAddress());
    assertThat(chunk.equals(newChunk)).isTrue();

    ObjectChunk chunkWithSameValue = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.equals(chunkWithSameValue)).isFalse();

    Object someObject = getValue();
    assertThat(chunk.equals(someObject)).isFalse();

    chunk.release();
    chunkWithSameValue.release();
  }

  @Test
  public void chunksShouldBeComparedBySize() {
    ObjectChunk chunk1 = createValueAsSerializedStoredObject(getValue());

    ObjectChunk chunk2 = chunk1;
    assertThat(chunk1.compareTo(chunk2)).isEqualTo(0);

    ObjectChunk chunkWithSameValue = createValueAsSerializedStoredObject(getValue());
    assertThat(chunk1.compareTo(chunkWithSameValue)).isEqualTo(Long.signum(chunk1.getMemoryAddress() - chunkWithSameValue.getMemoryAddress()));

    ObjectChunk chunk3 = createValueAsSerializedStoredObject(Long.MAX_VALUE);
    ObjectChunk chunk4 = createValueAsSerializedStoredObject(Long.MAX_VALUE);

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

    ObjectChunk chunk = (ObjectChunk) ma.allocateAndInitialize(regionEntryValueAsBytes, isSerialized, isCompressed);

    int headerBeforeSerializedBitSet = UnsafeMemoryChunk.readAbsoluteIntVolatile(chunk.getMemoryAddress() + ObjectChunk.REF_COUNT_OFFSET);

    assertThat(chunk.isSerialized()).isFalse();

    chunk.setSerialized(true); // set to true

    assertThat(chunk.isSerialized()).isTrue();

    int headerAfterSerializedBitSet = UnsafeMemoryChunk.readAbsoluteIntVolatile(chunk.getMemoryAddress() + ObjectChunk.REF_COUNT_OFFSET);

    assertThat(headerAfterSerializedBitSet).isEqualTo(headerBeforeSerializedBitSet | ObjectChunk.IS_SERIALIZED_BIT);

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void setSerialziedShouldThrowExceptionIfChunkIsAlreadyReleased() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
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

    ObjectChunk chunk = (ObjectChunk) ma.allocateAndInitialize(regionEntryValueAsBytes, isSerialized, isCompressed);

    int headerBeforeCompressedBitSet = UnsafeMemoryChunk.readAbsoluteIntVolatile(chunk.getMemoryAddress() + ObjectChunk.REF_COUNT_OFFSET);

    assertThat(chunk.isCompressed()).isFalse();

    chunk.setCompressed(true); // set to true

    assertThat(chunk.isCompressed()).isTrue();

    int headerAfterCompressedBitSet = UnsafeMemoryChunk.readAbsoluteIntVolatile(chunk.getMemoryAddress() + ObjectChunk.REF_COUNT_OFFSET);

    assertThat(headerAfterCompressedBitSet).isEqualTo(headerBeforeCompressedBitSet | ObjectChunk.IS_COMPRESSED_BIT);

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void setCompressedShouldThrowExceptionIfChunkIsAlreadyReleased() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.release();
    chunk.setCompressed(true);

    chunk.release();
  }

  @Test
  public void setDataSizeShouldSetTheDataSizeBits() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());

    int beforeSize = chunk.getDataSize();

    chunk.setDataSize(2);

    int afterSize = chunk.getDataSize();

    assertThat(afterSize).isEqualTo(2);
    assertThat(afterSize).isNotEqualTo(beforeSize);

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void setDataSizeShouldThrowExceptionIfChunkIsAlreadyReleased() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.release();
    chunk.setDataSize(1);

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void initializeUseCountShouldThrowIllegalStateExceptionIfChunkIsAlreadyRetained() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.retain();
    chunk.initializeUseCount();

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void initializeUseCountShouldThrowIllegalStateExceptionIfChunkIsAlreadyReleased() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.release();
    chunk.initializeUseCount();

    chunk.release();
  }

  @Test
  public void isSerializedPdxInstanceShouldReturnTrueIfItsPDXInstance() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());

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
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());
    assertThat(chunk.isSerializedPdxInstance()).isFalse();

    chunk.release();
  }

  @Test
  public void checkDataEqualsByChunk() {
    ObjectChunk chunk1 = createValueAsSerializedStoredObject(getValue());
    ObjectChunk sameAsChunk1 = chunk1;

    assertThat(chunk1.checkDataEquals(sameAsChunk1)).isTrue();

    ObjectChunk unserializedChunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk1.checkDataEquals(unserializedChunk)).isFalse();

    ObjectChunk chunkDifferBySize = createValueAsSerializedStoredObject(getValue());
    chunkDifferBySize.setSize(0);
    assertThat(chunk1.checkDataEquals(chunkDifferBySize)).isFalse();

    ObjectChunk chunkDifferByValue = createValueAsSerializedStoredObject(Long.MAX_VALUE - 1);
    assertThat(chunk1.checkDataEquals(chunkDifferByValue)).isFalse();

    ObjectChunk newChunk1 = createValueAsSerializedStoredObject(getValue());
    assertThat(chunk1.checkDataEquals(newChunk1)).isTrue();

    chunk1.release();
    unserializedChunk.release();
    chunkDifferBySize.release();
    chunkDifferByValue.release();
    newChunk1.release();
  }

  @Test
  public void checkDataEqualsBySerializedValue() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());
    assertThat(chunk.checkDataEquals(new byte[1])).isFalse();

    ObjectChunk chunkDifferByValue = createValueAsSerializedStoredObject(Long.MAX_VALUE - 1);
    assertThat(chunk.checkDataEquals(chunkDifferByValue.getSerializedValue())).isFalse();

    ObjectChunk newChunk = createValueAsSerializedStoredObject(getValue());
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

    ObjectChunk chunk = (ObjectChunk) ma.allocateAndInitialize(regionEntryValueAsBytes, isSerialized, isCompressed);

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
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());

    int beforeSize = chunk.getSize();

    chunk.incSize(1);
    assertThat(chunk.getSize()).isEqualTo(beforeSize + 1);

    chunk.incSize(2);
    assertThat(chunk.getSize()).isEqualTo(beforeSize + 1 + 2);

    chunk.release();
  }

  @Test
  public void readyForFreeShouldResetTheRefCount() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());

    int refCountBeforeFreeing = chunk.getRefCount();
    assertThat(refCountBeforeFreeing).isEqualTo(1);

    chunk.readyForFree();

    int refCountAfterFreeing = chunk.getRefCount();
    assertThat(refCountAfterFreeing).isEqualTo(0);
  }

  @Test(expected = IllegalStateException.class)
  public void readyForAllocationShouldThrowExceptionIfAlreadyAllocated() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());

    // chunk is already allocated when we created it, so calling readyForAllocation should throw exception.
    chunk.readyForAllocation();

    chunk.release();
  }

  @Test
  public void checkIsAllocatedShouldReturnIfAllocated() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());
    chunk.checkIsAllocated();

    chunk.release();
  }

  @Test(expected = IllegalStateException.class)
  public void checkIsAllocatedShouldThrowExceptionIfNotAllocated() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());
    chunk.release();
    chunk.checkIsAllocated();

    chunk.release();
  }

  @Test
  public void sendToShouldWriteSerializedValueToDataOutputIfValueIsSerialized() throws IOException {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());
    ObjectChunk spyChunk = spy(chunk);

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
    ObjectChunk chunk = createValueAsUnserializedStoredObject(regionEntryValue);

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
    ObjectChunk chunk = createValueAsUnserializedStoredObject(regionEntryValue);

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

    ObjectChunk chunk = createValueAsUnserializedStoredObject(regionEntryValue);

    ByteBuffer buffer = chunk.createDirectByteBuffer();

    byte[] actual = new byte[regionEntryValue.length];
    buffer.get(actual);

    assertArrayEquals(regionEntryValue, actual);

    chunk.release();
  }

  @Test
  public void getDirectByteBufferShouldCreateAByteBuffer() {
    byte[] regionEntryValue = getValueAsByteArray();
    ObjectChunk chunk = createValueAsUnserializedStoredObject(regionEntryValue);

    ByteBuffer buffer = chunk.createDirectByteBuffer();
    long bufferAddress = ObjectChunk.getDirectByteBufferAddress(buffer);

    // returned address should be starting of the value (after skipping HEADER_SIZE bytes)
    assertEquals(chunk.getMemoryAddress() + ObjectChunk.OFF_HEAP_HEADER_SIZE, bufferAddress);

    chunk.release();
  }

  @Test(expected = AssertionError.class)
  public void getAddressForReadingShouldFailIfItsOutsideOfChunk() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());
    chunk.getAddressForReading(0, chunk.getDataSize() + 1);

    chunk.release();
  }

  @Test
  public void getAddressForReadingShouldReturnDataAddressFromGivenOffset() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());

    int offset = 1;
    long requestedAddress = chunk.getAddressForReading(offset, 1);

    assertThat(requestedAddress).isEqualTo(chunk.getBaseDataAddress() + offset);

    chunk.release();
  }

  @Test
  public void getSizeInBytesShouldReturnSize() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());
    assertThat(chunk.getSizeInBytes()).isEqualTo(chunk.getSize());

    chunk.release();
  }

  @Test(expected = AssertionError.class)
  public void getUnsafeAddressShouldFailIfOffsetIsNegative() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());
    chunk.getUnsafeAddress(-1, 1);

    chunk.release();
  }

  @Test(expected = AssertionError.class)
  public void getUnsafeAddressShouldFailIfSizeIsNegative() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());
    chunk.getUnsafeAddress(1, -1);

    chunk.release();
  }

  @Test(expected = AssertionError.class)
  public void getUnsafeAddressShouldFailIfItsOutsideOfChunk() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());
    chunk.getUnsafeAddress(0, chunk.getDataSize() + 1);

    chunk.release();
  }

  @Test
  public void getUnsafeAddressShouldReturnUnsafeAddress() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());

    int offset = 1;
    long unsafeAddress = chunk.getUnsafeAddress(offset, 1);

    assertThat(unsafeAddress).isEqualTo(chunk.getBaseDataAddress() + offset);

    chunk.release();
  }

  @Test(expected = AssertionError.class)
  public void readByteAndWriteByteShouldFailIfOffsetIsOutside() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());

    chunk.readByte(chunk.getDataSize() + 1);

    chunk.writeByte(chunk.getDataSize() + 1, Byte.MAX_VALUE);

    chunk.release();
  }

  @Test
  public void writeByteShouldWriteAtCorrectLocation() {
    ObjectChunk chunk = createValueAsSerializedStoredObject(getValue());

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
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
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
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());

    // loop though and invoke retain for MAX_REF_COUNT-1 times, as create chunk above counted as one reference
    for (int i = 0; i < ObjectChunk.MAX_REF_COUNT - 1; i++)
      chunk.retain();

    // invoke for the one more time should throw exception
    chunk.retain();
  }

  @Test
  public void releaseShouldDecrementRefCount() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
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
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.release();
    chunk.release();
  }

  @Test
  public void testToStringForOffHeapByteSource() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());

    String expected = ":<dataSize=" + chunk.getDataSize() + " refCount=" + chunk.getRefCount() + " addr=" + Long.toHexString(chunk.getMemoryAddress()) + ">";
    assertThat(chunk.toStringForOffHeapByteSource()).endsWith(expected);

    // test toString
    ObjectChunk spy = spy(chunk);
    spy.toString();
    verify(spy, times(1)).toStringForOffHeapByteSource();

    chunk.release();
  }

  @Test
  public void getStateShouldReturnAllocatedIfRefCountIsGreaterThanZero() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
    assertEquals(State.ALLOCATED, chunk.getState());

    chunk.release();
  }

  @Test
  public void getStateShouldReturnDeallocatedIfRefCountIsZero() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.release();
    assertEquals(State.DEALLOCATED, chunk.getState());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getNextBlockShouldThrowUnSupportedOperationException() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.getNextBlock();

    chunk.release();
  }

  @Test
  public void getBlockSizeShouldBeSameSameGetSize() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
    assertEquals(chunk.getSize(), chunk.getBlockSize());

    chunk.release();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void copyBytesShouldThrowUnSupportedOperationException() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.copyBytes(1, 2, 1);

    chunk.release();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getSlabIdShouldThrowUnSupportedOperationException() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
    chunk.getSlabId();

    chunk.release();
  }

  @Test
  public void getFreeListIdShouldReturnMinusOne() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.getFreeListId()).isEqualTo(-1);

    chunk.release();
  }

  @Test
  public void getDataTypeShouldReturnNull() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.getDataType()).isNull();

    chunk.release();
  }

  @Test
  public void getDataDataShouldReturnNull() {
    ObjectChunk chunk = createValueAsUnserializedStoredObject(getValue());
    assertThat(chunk.getDataValue()).isNull();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getRawBytesShouldThrowExceptionIfValueIsCompressed() {
    Object regionEntryValue = getValue();
    byte[] regionEntryValueAsBytes = convertValueToByteArray(regionEntryValue);

    boolean isSerialized = true;
    boolean isCompressed = true;

    ObjectChunk chunk = (ObjectChunk) ma.allocateAndInitialize(regionEntryValueAsBytes, isSerialized, isCompressed);

    chunk.getRawBytes();

    chunk.release();
  }

  @Test
  public void getSerializedValueShouldSerializeTheValue() {
    Object regionEntryValue = getValue();
    byte[] regionEntryValueAsBytes = convertValueToByteArray(regionEntryValue);

    boolean isSerialized = false;
    boolean isCompressed = false;

    ObjectChunk chunk = (ObjectChunk) ma.allocateAndInitialize(regionEntryValueAsBytes, isSerialized, isCompressed);

    byte[] serializedValue = chunk.getSerializedValue();

    assertThat(serializedValue).isEqualTo(EntryEventImpl.serialize(regionEntryValueAsBytes));

    chunk.release();
  }

  @Test
  public void fillShouldFillTheChunk() {
    boolean isSerialized = false;
    boolean isCompressed = false;

    ObjectChunk chunk = (ObjectChunk) ma.allocateAndInitialize(new byte[100], isSerialized, isCompressed);

    // first fill the unused part with FILL_PATTERN
    ObjectChunk.fill(chunk.getMemoryAddress());

    // Validate that it is filled
    chunk.validateFill();

    chunk.release();
  }
}
