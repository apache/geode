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

import static org.apache.geode.internal.offheap.OffHeapRegionEntryHelperInstance.COMPRESSED_BIT;
import static org.apache.geode.internal.offheap.OffHeapRegionEntryHelperInstance.DESTROYED_ADDRESS;
import static org.apache.geode.internal.offheap.OffHeapRegionEntryHelperInstance.END_OF_STREAM_ADDRESS;
import static org.apache.geode.internal.offheap.OffHeapRegionEntryHelperInstance.INVALID_ADDRESS;
import static org.apache.geode.internal.offheap.OffHeapRegionEntryHelperInstance.LOCAL_INVALID_ADDRESS;
import static org.apache.geode.internal.offheap.OffHeapRegionEntryHelperInstance.NOT_AVAILABLE_ADDRESS;
import static org.apache.geode.internal.offheap.OffHeapRegionEntryHelperInstance.NULL_ADDRESS;
import static org.apache.geode.internal.offheap.OffHeapRegionEntryHelperInstance.REMOVED_PHASE1_ADDRESS;
import static org.apache.geode.internal.offheap.OffHeapRegionEntryHelperInstance.REMOVED_PHASE2_ADDRESS;
import static org.apache.geode.internal.offheap.OffHeapRegionEntryHelperInstance.SERIALIZED_BIT;
import static org.apache.geode.internal.offheap.OffHeapRegionEntryHelperInstance.TOMBSTONE_ADDRESS;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.compression.Compressor;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.DiskId;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.entries.OffHeapRegionEntry;
import org.apache.geode.internal.cache.entries.VersionedStatsDiskRegionEntryOffHeap;
import org.apache.geode.internal.serialization.DSCODE;

public class OffHeapRegionEntryHelperInstanceTest {

  private static final long VALUE_IS_NOT_ENCODABLE = 0L;

  private MemoryAllocator memoryAllocator;
  private ReferenceCounterInstance referenceCounter;
  private OffHeapStoredObject offHeapStoredObject;

  private OffHeapRegionEntryHelperInstance offHeapRegionEntryHelperInstance;

  @Before
  public void setUp() {
    OutOfOffHeapMemoryListener listener = mock(OutOfOffHeapMemoryListener.class);
    OffHeapMemoryStats stats = mock(OffHeapMemoryStats.class);
    Function<Long, OffHeapStoredObject> offHeapStoredObjectFactory =
        uncheckedCast(mock(Function.class));
    offHeapStoredObject = mock(OffHeapStoredObject.class);
    referenceCounter = mock(ReferenceCounterInstance.class);

    when(offHeapStoredObjectFactory.apply(anyLong()))
        .thenReturn(offHeapStoredObject);

    memoryAllocator =
        MemoryAllocatorImpl.create(listener, stats, 1, OffHeapStorage.MIN_SLAB_SIZE,
            OffHeapStorage.MIN_SLAB_SIZE);

    offHeapRegionEntryHelperInstance =
        spy(new OffHeapRegionEntryHelperInstance(ohAddress -> offHeapStoredObject,
            referenceCounter));
  }

  @After
  public void tearDown() {
    MemoryAllocatorImpl.freeOffHeapMemory();
  }

  @Test
  public void encodeDataAsAddressShouldReturnZeroIfValueIsGreaterThanSevenBytes() {
    byte[] valueInBytes =
        ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(Long.MAX_VALUE).array();

    assertThat(valueInBytes.length)
        .isGreaterThanOrEqualTo(OffHeapRegionEntryHelperInstance.MAX_LENGTH_FOR_DATA_AS_ADDRESS);

    long encodedAddress =
        offHeapRegionEntryHelperInstance.encodeDataAsAddress(valueInBytes, false, false);

    assertThat(encodedAddress)
        .isEqualTo(VALUE_IS_NOT_ENCODABLE);
  }

  @Test
  public void encodeDataAsAddressShouldEncodeLongIfItsSerializedAndIfItsNotTooBig() {
    byte[] valueInBytes = EntryEventImpl.serialize(0L);
    boolean isSerialized = true;
    boolean isCompressed = false;

    long encodedAddress =
        offHeapRegionEntryHelperInstance.encodeDataAsAddress(valueInBytes, isSerialized,
            isCompressed);

    long expectedEncodedAddress = 123L;
    assertThat(encodedAddress)
        .isEqualTo(expectedEncodedAddress);

    assertSerializedAndCompressedBits(encodedAddress, isSerialized, isCompressed);
  }

  @Test
  public void encodeDataAsAddressShouldReturnZeroIfValueIsLongAndItIsSerializedAndBig() {
    byte[] valueInBytes = EntryEventImpl.serialize(Long.MAX_VALUE);

    long encodedAddress =
        offHeapRegionEntryHelperInstance.encodeDataAsAddress(valueInBytes, true,
            false);

    assertThat(encodedAddress)
        .isEqualTo(VALUE_IS_NOT_ENCODABLE);
  }

  @Test
  public void encodeDataAsAddressShouldReturnZeroIfValueIsLargerThanEightBytesAndNotLong() {
    byte[] someValue = new byte[8];
    someValue[0] = DSCODE.CLASS.toByte();

    long encodedAddress =
        offHeapRegionEntryHelperInstance.encodeDataAsAddress(someValue, true, false);

    assertThat(encodedAddress)
        .isEqualTo(VALUE_IS_NOT_ENCODABLE);
  }

  @Test
  public void encodeDataAsAddressShouldReturnValidAddressIfValueIsLesserThanSevenBytes() {
    byte[] valueInBytes =
        ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(Integer.MAX_VALUE).array();
    boolean isSerialized = false;
    boolean isCompressed = false;

    long encodedAddress =
        offHeapRegionEntryHelperInstance.encodeDataAsAddress(valueInBytes, isSerialized,
            isCompressed);

    long expectedAddress = 549755813697L;
    assertThat(encodedAddress)
        .isEqualTo(expectedAddress);

    assertSerializedAndCompressedBits(encodedAddress, isSerialized, isCompressed);
  }

  @Test
  public void encodeDataAsAddressShouldSetSerializedBitIfSerialized() {
    byte[] valueInBytes = EntryEventImpl.serialize(Integer.MAX_VALUE);
    boolean isSerialized = true;
    boolean isCompressed = false;

    long encodedAddress =
        offHeapRegionEntryHelperInstance.encodeDataAsAddress(valueInBytes, isSerialized,
            isCompressed);

    long expectedAddress = 63221918596947L;
    assertThat(expectedAddress)
        .isEqualTo(encodedAddress);

    assertSerializedAndCompressedBits(encodedAddress, isSerialized, isCompressed);
  }

  @Test
  public void encodeDataAsAddressShouldSetSerializedBitIfCompressed() {
    byte[] valueInBytes =
        ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(Integer.MAX_VALUE).array();
    boolean isSerialized = false;
    boolean isCompressed = true;

    long encodedAddress =
        offHeapRegionEntryHelperInstance.encodeDataAsAddress(valueInBytes, isSerialized,
            isCompressed);

    long expectedAddress = 549755813701L;
    assertThat(encodedAddress)
        .isEqualTo(expectedAddress);

    assertSerializedAndCompressedBits(encodedAddress, isSerialized, isCompressed);
  }

  @Test
  public void encodeDataAsAddressShouldSetBothSerializedAndCompressedBitsIfSerializedAndCompressed() {
    byte[] valueInBytes = EntryEventImpl.serialize(Integer.MAX_VALUE);
    boolean isSerialized = true;
    boolean isCompressed = true;

    long encodedAddress =
        offHeapRegionEntryHelperInstance.encodeDataAsAddress(valueInBytes, isSerialized,
            isCompressed);

    long expectedAddress = 63221918596951L;
    assertThat(expectedAddress)
        .isEqualTo(encodedAddress);

    assertSerializedAndCompressedBits(encodedAddress, isSerialized, isCompressed);
  }

  @Test
  public void decodeUncompressedAddressToBytesShouldReturnActualBytes() {
    long encodedAddress = 549755813697L;
    int value = Integer.MAX_VALUE;

    byte[] actual =
        offHeapRegionEntryHelperInstance.decodeUncompressedAddressToBytes(encodedAddress);

    byte[] expectedValue = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    assertThat(actual)
        .isEqualTo(expectedValue);
  }

  @Test
  public void decodeUncompressedAddressToBytesShouldDecodeLongIfItsSerializedAndIfItsNotTooBig() {
    byte[] actual = offHeapRegionEntryHelperInstance.decodeUncompressedAddressToBytes(123L);

    byte[] expectedValue = EntryEventImpl.serialize(0L);
    assertThat(actual)
        .isEqualTo(expectedValue);
  }

  @Test
  public void decodeUncompressedAddressToBytesWithCompressedAddressShouldThrowException() {
    long encodedAddress = 549755813703L;

    Throwable thrown = catchThrowable(() -> {
      offHeapRegionEntryHelperInstance.decodeUncompressedAddressToBytes(encodedAddress);
    });

    assertThat(thrown)
        .isInstanceOf(AssertionError.class);
  }

  @Test
  public void decodeCompressedDataAsAddressToRawBytes() {
    long encodedAddress = 549755813703L;
    byte[] expected = new byte[] {127, -1, -1, -1};

    byte[] bytes = offHeapRegionEntryHelperInstance.decodeAddressToRawBytes(encodedAddress);

    assertThat(bytes)
        .isEqualTo(expected);
  }

  @Test
  public void encodedAddressShouldBeDecodableEvenIfValueIsSerialized() {
    int value = Integer.MAX_VALUE;
    byte[] serializedValue = EntryEventImpl.serialize(value);

    long encodedAddress =
        offHeapRegionEntryHelperInstance.encodeDataAsAddress(serializedValue, true, false);

    int actualValue =
        (int) offHeapRegionEntryHelperInstance.decodeAddressToObject(encodedAddress);

    assertThat(actualValue)
        .isEqualTo(value);
  }

  @Test
  public void encodedAddressShouldBeDecodableEvenIfValueIsUnserialized() {
    int value = Integer.MAX_VALUE;
    byte[] unSerializedValue = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    long encodedAddress =
        offHeapRegionEntryHelperInstance.encodeDataAsAddress(unSerializedValue, false, false);

    byte[] actualValue =
        (byte[]) offHeapRegionEntryHelperInstance.decodeAddressToObject(encodedAddress);

    assertThat(actualValue)
        .isEqualTo(unSerializedValue);
  }

  @Test
  public void isSerializedShouldReturnTrueIfSerialized() {
    assertThat(offHeapRegionEntryHelperInstance.isSerialized(1000010L)).isTrue();
  }

  @Test
  public void isSerializedShouldReturnFalseIfNotSerialized() {
    assertThat(offHeapRegionEntryHelperInstance.isSerialized(1000000L)).isFalse();
  }

  @Test
  public void isCompressedShouldReturnTrueIfCompressed() {
    assertThat(offHeapRegionEntryHelperInstance.isCompressed(1000100L)).isTrue();
  }

  @Test
  public void isCompressedShouldReturnFalseIfNotCompressed() {
    assertThat(offHeapRegionEntryHelperInstance.isCompressed(1000000L)).isFalse();
  }

  @Test
  public void isOffHeapShouldReturnTrueIfAddressIsOnOffHeap() {
    OffHeapStoredObject value = createChunk(Long.MAX_VALUE);

    assertThat(offHeapRegionEntryHelperInstance.isOffHeap(value.getAddress())).isTrue();
  }

  @Test
  public void isOffHeapShouldReturnFalseIfAddressIsAnEncodedAddress() {
    byte[] data = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(Integer.MAX_VALUE).array();

    long address = offHeapRegionEntryHelperInstance.encodeDataAsAddress(data, false, false);

    assertThat(offHeapRegionEntryHelperInstance.isOffHeap(address)).isFalse();
  }

  @Test
  public void isOffHeapShouldReturnFalseForAnyTokenAddress() {
    assertThat(offHeapRegionEntryHelperInstance.isOffHeap(NULL_ADDRESS)).isFalse();
    assertThat(offHeapRegionEntryHelperInstance.isOffHeap(INVALID_ADDRESS)).isFalse();
    assertThat(offHeapRegionEntryHelperInstance.isOffHeap(LOCAL_INVALID_ADDRESS)).isFalse();
    assertThat(offHeapRegionEntryHelperInstance.isOffHeap(DESTROYED_ADDRESS)).isFalse();
    assertThat(offHeapRegionEntryHelperInstance.isOffHeap(REMOVED_PHASE1_ADDRESS)).isFalse();
    assertThat(offHeapRegionEntryHelperInstance.isOffHeap(REMOVED_PHASE2_ADDRESS)).isFalse();
    assertThat(offHeapRegionEntryHelperInstance.isOffHeap(END_OF_STREAM_ADDRESS)).isFalse();
    assertThat(offHeapRegionEntryHelperInstance.isOffHeap(NOT_AVAILABLE_ADDRESS)).isFalse();
    assertThat(offHeapRegionEntryHelperInstance.isOffHeap(TOMBSTONE_ADDRESS)).isFalse();
  }

  @Test
  public void setValueShouldChangeTheRegionEntryAddressToNewAddress() {
    // mock region entry
    OffHeapRegionEntry regionEntry = mock(OffHeapRegionEntry.class);

    // some old address
    long oldAddress = 1L;

    // testing when the newValue is a chunk
    OffHeapStoredObject newValue = createChunk(Long.MAX_VALUE);
    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(oldAddress);
    when(regionEntry.setAddress(oldAddress, newValue.getAddress())).thenReturn(Boolean.TRUE);

    // invoke the method under test
    offHeapRegionEntryHelperInstance.setValue(regionEntry, newValue);

    // verify oldAddress is replaced with newAddress
    verify(regionEntry).setAddress(oldAddress, newValue.getAddress());
    // resetting the spy in-order to re-use
    reset(regionEntry);

    // testing when the newValue is DataAsAddress
    TinyStoredObject newAddress1 = new TinyStoredObject(2L);
    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(oldAddress);
    when(regionEntry.setAddress(oldAddress, newAddress1.getAddress())).thenReturn(true);
    offHeapRegionEntryHelperInstance.setValue(regionEntry, newAddress1);

    // verify oldAddress is replaced with newAddress
    verify(regionEntry).setAddress(oldAddress, newAddress1.getAddress());
    reset(regionEntry);

    // Testing when newValue is Token Objects

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(oldAddress);
    when(regionEntry.setAddress(oldAddress, NULL_ADDRESS)).thenReturn(true);
    offHeapRegionEntryHelperInstance.setValue(regionEntry, null);

    // verify oldAddress is replaced with newAddress
    verify(regionEntry).setAddress(oldAddress, NULL_ADDRESS);
    reset(regionEntry);

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(oldAddress);
    when(regionEntry.setAddress(oldAddress, INVALID_ADDRESS)).thenReturn(true);
    offHeapRegionEntryHelperInstance.setValue(regionEntry, Token.INVALID);

    // verify oldAddress is replaced with newAddress
    verify(regionEntry).setAddress(oldAddress, INVALID_ADDRESS);
    reset(regionEntry);

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(oldAddress);
    when(regionEntry.setAddress(oldAddress, LOCAL_INVALID_ADDRESS)).thenReturn(true);
    offHeapRegionEntryHelperInstance.setValue(regionEntry, Token.LOCAL_INVALID);

    // verify oldAddress is replaced with newAddress
    verify(regionEntry).setAddress(oldAddress, LOCAL_INVALID_ADDRESS);
    reset(regionEntry);

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(oldAddress);
    when(regionEntry.setAddress(oldAddress, DESTROYED_ADDRESS)).thenReturn(true);
    offHeapRegionEntryHelperInstance.setValue(regionEntry, Token.DESTROYED);

    // verify oldAddress is replaced with newAddress
    verify(regionEntry).setAddress(oldAddress, DESTROYED_ADDRESS);
    reset(regionEntry);

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(oldAddress);
    when(regionEntry.setAddress(oldAddress, REMOVED_PHASE1_ADDRESS)).thenReturn(true);
    offHeapRegionEntryHelperInstance.setValue(regionEntry, Token.REMOVED_PHASE1);

    // verify oldAddress is replaced with newAddress
    verify(regionEntry).setAddress(oldAddress, REMOVED_PHASE1_ADDRESS);
    reset(regionEntry);

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(oldAddress);
    when(regionEntry.setAddress(oldAddress, REMOVED_PHASE2_ADDRESS)).thenReturn(true);
    offHeapRegionEntryHelperInstance.setValue(regionEntry, Token.REMOVED_PHASE2);

    // verify oldAddress is replaced with newAddress
    verify(regionEntry).setAddress(oldAddress, REMOVED_PHASE2_ADDRESS);
    reset(regionEntry);

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(oldAddress);
    when(regionEntry.setAddress(oldAddress, END_OF_STREAM_ADDRESS)).thenReturn(true);
    offHeapRegionEntryHelperInstance.setValue(regionEntry, Token.END_OF_STREAM);

    // verify oldAddress is replaced with newAddress
    verify(regionEntry).setAddress(oldAddress, END_OF_STREAM_ADDRESS);
    reset(regionEntry);

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(oldAddress);
    when(regionEntry.setAddress(oldAddress, NOT_AVAILABLE_ADDRESS)).thenReturn(true);
    offHeapRegionEntryHelperInstance.setValue(regionEntry, Token.NOT_AVAILABLE);

    // verify oldAddress is replaced with newAddress
    verify(regionEntry).setAddress(oldAddress, NOT_AVAILABLE_ADDRESS);
    reset(regionEntry);

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(oldAddress);
    when(regionEntry.setAddress(oldAddress, TOMBSTONE_ADDRESS)).thenReturn(true);
    offHeapRegionEntryHelperInstance.setValue(regionEntry, Token.TOMBSTONE);

    // verify oldAddress is replaced with newAddress
    verify(regionEntry).setAddress(oldAddress, TOMBSTONE_ADDRESS);
  }

  @Test
  public void setValueShouldChangeTheRegionEntryAddressToNewAddressAndReleaseOldValueIfItsOnOffHeap() {
    OffHeapStoredObject oldValue = createChunk(Long.MAX_VALUE);
    OffHeapStoredObject newValue = createChunk(Long.MAX_VALUE - 1);
    OffHeapRegionEntry regionEntry = mock(OffHeapRegionEntry.class);

    // mock Chunk static methods - in-order to verify that release is called
    doNothing().when(offHeapStoredObject).release();

    // mock region entry methods required for test
    when(regionEntry.getAddress())
        .thenReturn(oldValue.getAddress());
    when(regionEntry.setAddress(oldValue.getAddress(), newValue.getAddress()))
        .thenReturn(Boolean.TRUE);

    // invoke the method under test
    offHeapRegionEntryHelperInstance.setValue(regionEntry, newValue);

    // verify oldAddress is changed to newAddress
    verify(regionEntry)
        .setAddress(oldValue.getAddress(), newValue.getAddress());

    // verify oldAddress is released
    verify(referenceCounter)
        .release(oldValue.getAddress());
  }

  @Test
  public void setValueShouldChangeTheRegionEntryAddressToNewAddressAndDoesNothingIfOldAddressIsAnEncodedAddress() {
    byte[] oldData =
        ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(Integer.MAX_VALUE).array();
    byte[] newData =
        ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(Integer.MAX_VALUE - 1).array();
    long oldAddress = offHeapRegionEntryHelperInstance.encodeDataAsAddress(oldData, false, false);
    StoredObject newAddress = new TinyStoredObject(
        offHeapRegionEntryHelperInstance.encodeDataAsAddress(newData, false, false));
    OffHeapRegionEntry regionEntry = mock(OffHeapRegionEntry.class);

    // mock region entry methods required for test
    when(regionEntry.getAddress())
        .thenReturn(oldAddress);
    when(regionEntry.setAddress(oldAddress, newAddress.getAddress()))
        .thenReturn(true);

    // invoke the method under test
    offHeapRegionEntryHelperInstance.setValue(regionEntry, newAddress);

    // verify oldAddress is changed to newAddress
    verify(regionEntry)
        .setAddress(oldAddress, newAddress.getAddress());

    // verify that release is never called as the old address is not on offheap
    verifyNoInteractions(offHeapStoredObject);
  }

  @Test
  public void setValueShouldChangeTheRegionEntryAddressToNewAddressAndDoesNothingIfOldAddressIsATokenAddress() {
    long oldAddress = REMOVED_PHASE1_ADDRESS;
    long newAddress = REMOVED_PHASE2_ADDRESS;
    OffHeapRegionEntry regionEntry = mock(OffHeapRegionEntry.class);

    when(regionEntry.getAddress())
        .thenReturn(oldAddress);
    when(regionEntry.setAddress(oldAddress, newAddress))
        .thenReturn(true);

    // invoke the method under test
    offHeapRegionEntryHelperInstance.setValue(regionEntry, Token.REMOVED_PHASE2);

    // verify oldAddress is changed to newAddress
    verify(regionEntry)
        .setAddress(oldAddress, newAddress);
  }

  @Test
  public void setValueShouldThrowIllegalExceptionIfNewValueCannotBeConvertedToAddress() {
    OffHeapRegionEntry regionEntry = mock(OffHeapRegionEntry.class);

    when(regionEntry.getAddress())
        .thenReturn(1L);

    // invoke the method under test with some object other than Chunk/DataAsAddress/Token
    Throwable thrown = catchThrowable(() -> {
      offHeapRegionEntryHelperInstance.setValue(regionEntry, new Object());
    });

    assertThat(thrown)
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void getValueAsTokenShouldReturnNotATokenIfValueIsOnOffHeap() {
    OffHeapStoredObject chunk = createChunk(Long.MAX_VALUE);
    OffHeapRegionEntry regionEntry = mock(OffHeapRegionEntry.class);

    when(regionEntry.getAddress())
        .thenReturn(chunk.getAddress());

    Token token = offHeapRegionEntryHelperInstance.getValueAsToken(regionEntry);

    assertThat(token)
        .isEqualTo(Token.NOT_A_TOKEN);
  }

  @Test
  public void getValueAsTokenShouldReturnNotATokenIfValueIsEncoded() {
    byte[] data = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(Integer.MAX_VALUE).array();
    long address = offHeapRegionEntryHelperInstance.encodeDataAsAddress(data, false, false);
    OffHeapRegionEntry regionEntry = mock(OffHeapRegionEntry.class);

    when(regionEntry.getAddress())
        .thenReturn(address);

    Token token = offHeapRegionEntryHelperInstance.getValueAsToken(regionEntry);

    assertThat(token)
        .isEqualTo(Token.NOT_A_TOKEN);
  }

  @Test
  public void getValueAsTokenShouldReturnAValidToken() {
    OffHeapRegionEntry regionEntry = mock(OffHeapRegionEntry.class);

    when(regionEntry.getAddress()).thenReturn(NULL_ADDRESS);
    Token token = offHeapRegionEntryHelperInstance.getValueAsToken(regionEntry);
    assertThat(token).isNull();

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(INVALID_ADDRESS);
    token = offHeapRegionEntryHelperInstance.getValueAsToken(regionEntry);
    assertThat(token).isEqualTo(Token.INVALID);

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(LOCAL_INVALID_ADDRESS);
    token = offHeapRegionEntryHelperInstance.getValueAsToken(regionEntry);
    assertThat(token).isEqualTo(Token.LOCAL_INVALID);

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(DESTROYED_ADDRESS);
    token = offHeapRegionEntryHelperInstance.getValueAsToken(regionEntry);
    assertThat(token).isEqualTo(Token.DESTROYED);

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(REMOVED_PHASE1_ADDRESS);
    token = offHeapRegionEntryHelperInstance.getValueAsToken(regionEntry);
    assertThat(token).isEqualTo(Token.REMOVED_PHASE1);

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(REMOVED_PHASE2_ADDRESS);
    token = offHeapRegionEntryHelperInstance.getValueAsToken(regionEntry);
    assertThat(token).isEqualTo(Token.REMOVED_PHASE2);

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(END_OF_STREAM_ADDRESS);
    token = offHeapRegionEntryHelperInstance.getValueAsToken(regionEntry);
    assertThat(token).isEqualTo(Token.END_OF_STREAM);

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(NOT_AVAILABLE_ADDRESS);
    token = offHeapRegionEntryHelperInstance.getValueAsToken(regionEntry);
    assertThat(token).isEqualTo(Token.NOT_AVAILABLE);

    // mock region entry methods required for test
    when(regionEntry.getAddress()).thenReturn(TOMBSTONE_ADDRESS);
    token = offHeapRegionEntryHelperInstance.getValueAsToken(regionEntry);
    assertThat(token).isEqualTo(Token.TOMBSTONE);
  }

  @Test
  public void addressToObjectShouldReturnValueFromChunk() {
    OffHeapRegionEntryHelperInstance offHeapRegionEntryHelperInstance =
        new OffHeapRegionEntryHelperInstance();
    OffHeapStoredObject expected = createChunk(Long.MAX_VALUE);

    Object actual =
        offHeapRegionEntryHelperInstance.addressToObject(expected.getAddress(), false, null);

    assertThat(actual)
        .isInstanceOf(OffHeapStoredObject.class)
        .isEqualTo(expected);
  }

  @Test
  public void addressToObjectShouldReturnCachedDeserializableFromChunkIfAskedToDecompress() {
    byte[] data = EntryEventImpl.serialize(Long.MAX_VALUE);
    RegionEntryContext regionContext = mock(RegionEntryContext.class);
    CachePerfStats cacheStats = mock(CachePerfStats.class);
    Compressor compressor = mock(Compressor.class);

    when(regionContext.getCompressor())
        .thenReturn(compressor);
    when(compressor.decompress(data))
        .thenReturn(data);
    when(regionContext.getCachePerfStats())
        .thenReturn(cacheStats);
    when(cacheStats.startDecompression())
        .thenReturn(10000L);

    MemoryBlock chunk =
        (MemoryBlock) memoryAllocator.allocateAndInitialize(data, true, true);
    offHeapRegionEntryHelperInstance =
        spy(new OffHeapRegionEntryHelperInstance(OffHeapStoredObject::new, referenceCounter));

    Object actual =
        offHeapRegionEntryHelperInstance.addressToObject(chunk.getAddress(), true, regionContext);

    assertThat(actual)
        .isInstanceOf(VMCachedDeserializable.class);

    long actualValue = (long) ((CachedDeserializable) actual).getDeserializedForReading();

    assertThat(actualValue)
        .isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void addressToObjectShouldReturnDecompressedValueFromChunkIfAskedToDecompress() {
    byte[] data = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(Long.MAX_VALUE).array();
    RegionEntryContext regionContext = mock(RegionEntryContext.class);
    CachePerfStats cacheStats = mock(CachePerfStats.class);
    Compressor compressor = mock(Compressor.class);

    when(regionContext.getCompressor())
        .thenReturn(compressor);
    when(compressor.decompress(data))
        .thenReturn(data);
    when(regionContext.getCachePerfStats())
        .thenReturn(cacheStats);
    when(cacheStats.startDecompression())
        .thenReturn(10000L);

    MemoryBlock chunk = (MemoryBlock) memoryAllocator.allocateAndInitialize(data, false, true);
    offHeapRegionEntryHelperInstance =
        spy(new OffHeapRegionEntryHelperInstance(OffHeapStoredObject::new, referenceCounter));

    Object actual =
        offHeapRegionEntryHelperInstance.addressToObject(chunk.getAddress(), true, regionContext);

    assertThat(actual)
        .isInstanceOf(byte[].class)
        .isEqualTo(data);
  }

  @Test
  public void addressToObjectShouldReturnValueFromDataAsAddress() {
    byte[] data = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(Integer.MAX_VALUE).array();
    long address = offHeapRegionEntryHelperInstance.encodeDataAsAddress(data, false, false);

    Object actual = offHeapRegionEntryHelperInstance.addressToObject(address, false, null);

    TinyStoredObject expected = new TinyStoredObject(address);

    assertThat(actual)
        .isInstanceOf(TinyStoredObject.class)
        .isEqualTo(expected);
  }

  @Test
  public void addressToObjectShouldReturnCachedDeserializableFromSerializedDataAsAddressIfAskedToDecompress() {
    byte[] data = EntryEventImpl.serialize(Integer.MAX_VALUE);
    RegionEntryContext regionContext = mock(RegionEntryContext.class);
    CachePerfStats cacheStats = mock(CachePerfStats.class);
    Compressor compressor = mock(Compressor.class);

    when(regionContext.getCompressor())
        .thenReturn(compressor);
    when(compressor.decompress(data))
        .thenReturn(data);
    when(regionContext.getCachePerfStats())
        .thenReturn(cacheStats);
    when(cacheStats.startDecompression())
        .thenReturn(10000L);

    long address =
        offHeapRegionEntryHelperInstance.encodeDataAsAddress(data, true, true);

    Object actual = offHeapRegionEntryHelperInstance.addressToObject(address, true, regionContext);

    assertThat(actual)
        .isInstanceOf(VMCachedDeserializable.class);

    int actualValue = (int) ((CachedDeserializable) actual).getDeserializedForReading();

    assertThat(actualValue)
        .isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void addressToObjectShouldReturnDecompressedValueFromDataAsAddressIfAskedToDecompress() {
    byte[] data = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(Integer.MAX_VALUE).array();
    RegionEntryContext regionContext = mock(RegionEntryContext.class);
    CachePerfStats cacheStats = mock(CachePerfStats.class);
    Compressor compressor = mock(Compressor.class);

    when(regionContext.getCompressor())
        .thenReturn(compressor);
    when(compressor.decompress(data))
        .thenReturn(data);
    when(regionContext.getCachePerfStats())
        .thenReturn(cacheStats);
    when(cacheStats.startDecompression())
        .thenReturn(10000L);

    long address =
        offHeapRegionEntryHelperInstance.encodeDataAsAddress(data, false, true);

    Object actual = offHeapRegionEntryHelperInstance.addressToObject(address, true, regionContext);

    assertThat(actual)
        .isInstanceOf(byte[].class)
        .isEqualTo(data);
  }

  @Test
  public void addressToObjectShouldReturnToken() {
    Token token = (Token) offHeapRegionEntryHelperInstance
        .addressToObject(NULL_ADDRESS, false, null);
    assertThat(token).isNull();

    token = (Token) offHeapRegionEntryHelperInstance
        .addressToObject(INVALID_ADDRESS, false, null);
    assertThat(token).isEqualTo(Token.INVALID);

    token = (Token) offHeapRegionEntryHelperInstance
        .addressToObject(LOCAL_INVALID_ADDRESS, false, null);
    assertThat(token).isEqualTo(Token.LOCAL_INVALID);

    token = (Token) offHeapRegionEntryHelperInstance
        .addressToObject(DESTROYED_ADDRESS, false, null);
    assertThat(token).isEqualTo(Token.DESTROYED);

    token = (Token) offHeapRegionEntryHelperInstance
        .addressToObject(REMOVED_PHASE1_ADDRESS, false, null);
    assertThat(token).isEqualTo(Token.REMOVED_PHASE1);

    token = (Token) offHeapRegionEntryHelperInstance
        .addressToObject(REMOVED_PHASE2_ADDRESS, false, null);
    assertThat(token).isEqualTo(Token.REMOVED_PHASE2);

    token = (Token) offHeapRegionEntryHelperInstance
        .addressToObject(END_OF_STREAM_ADDRESS, false, null);
    assertThat(token).isEqualTo(Token.END_OF_STREAM);

    token = (Token) offHeapRegionEntryHelperInstance
        .addressToObject(NOT_AVAILABLE_ADDRESS, false, null);
    assertThat(token).isEqualTo(Token.NOT_AVAILABLE);

    token = (Token) offHeapRegionEntryHelperInstance
        .addressToObject(TOMBSTONE_ADDRESS, false, null);
    assertThat(token).isEqualTo(Token.TOMBSTONE);
  }

  @Test
  public void getSerializedLengthFromDataAsAddressShouldReturnValidLength() {
    byte[] data = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(Integer.MAX_VALUE).array();
    long address =
        offHeapRegionEntryHelperInstance.encodeDataAsAddress(data, false, true);
    TinyStoredObject tinyStoredObject = new TinyStoredObject(address);

    int actualLength = offHeapRegionEntryHelperInstance.getSerializedLength(tinyStoredObject);

    assertThat(actualLength)
        .isEqualTo(data.length);
  }

  @Test
  public void getSerializedLengthFromDataAsAddressShouldReturnZeroForNonEncodedAddress() {
    TinyStoredObject nonEncodedAddress = new TinyStoredObject(100000L);

    int actualLength = offHeapRegionEntryHelperInstance.getSerializedLength(nonEncodedAddress);

    assertThat(actualLength)
        .isZero();
  }

  @Test
  public void releaseEntryShouldSetValueToRemovePhase2() {
    OffHeapRegionEntry regionEntry = mock(OffHeapRegionEntry.class);

    when(regionEntry.getAddress())
        .thenReturn(1L);
    when(regionEntry.setAddress(1L, REMOVED_PHASE2_ADDRESS))
        .thenReturn(Boolean.TRUE);

    offHeapRegionEntryHelperInstance.releaseEntry(regionEntry);

    verify(offHeapRegionEntryHelperInstance)
        .setValue(regionEntry, Token.REMOVED_PHASE2);
  }

  @Test
  public void releaseEntryShouldSetValueToRemovePhase2AndSetsAsyncToFalseForDiskEntry() {
    OffHeapRegionEntry regionEntry = mock(VersionedStatsDiskRegionEntryOffHeap.class);
    DiskId diskId = spy(DiskId.class);

    when(regionEntry.getAddress())
        .thenReturn(1L);
    when(regionEntry.setAddress(1L, REMOVED_PHASE2_ADDRESS))
        .thenReturn(Boolean.TRUE);
    when(((DiskEntry) regionEntry).getDiskId())
        .thenReturn(diskId);
    when(diskId.isPendingAsync())
        .thenReturn(Boolean.TRUE);

    offHeapRegionEntryHelperInstance.releaseEntry(regionEntry);

    verify(offHeapRegionEntryHelperInstance)
        .setValue(regionEntry, Token.REMOVED_PHASE2);
    verify(diskId)
        .setPendingAsync(Boolean.FALSE);
  }

  @Test
  public void doWithOffHeapClearShouldSetTheThreadLocalToTrue() {
    // verify that threadlocal is not set
    assertThat(OffHeapClearRequired.doesClearNeedToCheckForOffHeap()).isFalse();

    OffHeapClearRequired.doWithOffHeapClear(() -> {
      // verify that threadlocal is set when offheap is cleared
      assertThat(OffHeapClearRequired.doesClearNeedToCheckForOffHeap()).isTrue();
    });

    // verify that threadlocal is reset after offheap is cleared
    assertThat(OffHeapClearRequired.doesClearNeedToCheckForOffHeap()).isFalse();
  }

  private OffHeapStoredObject createChunk(Object value) {
    byte[] bytes = EntryEventImpl.serialize(value);

    StoredObject chunk = memoryAllocator.allocateAndInitialize(bytes, true, false);

    return (OffHeapStoredObject) chunk;
  }

  private static void assertSerializedAndCompressedBits(long encodedAddress,
      boolean shouldSerializedBitBeSet, boolean shouldCompressedBitBeSet) {
    boolean isSerializedBitSet = (encodedAddress & SERIALIZED_BIT) == SERIALIZED_BIT;

    assertThat(isSerializedBitSet)
        .isEqualTo(shouldSerializedBitBeSet);

    boolean isCompressedBitSet = (encodedAddress & COMPRESSED_BIT) == COMPRESSED_BIT;

    assertThat(isCompressedBitSet)
        .isEqualTo(shouldCompressedBitBeSet);
  }
}
