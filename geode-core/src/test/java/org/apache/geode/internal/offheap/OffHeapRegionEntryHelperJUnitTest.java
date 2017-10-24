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

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.geode.compression.Compressor;
import org.apache.geode.internal.DSCODE;
import org.apache.geode.internal.cache.CachePerfStats;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.DiskId;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.entries.OffHeapRegionEntry;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.entries.VersionedStatsDiskRegionEntryOffHeap;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("*.UnitTest")
@PrepareForTest({OffHeapStoredObject.class, OffHeapRegionEntryHelper.class})
public class OffHeapRegionEntryHelperJUnitTest {

  private static final Long VALUE_IS_NOT_ENCODABLE = 0L;

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

  private MemoryAllocator ma;

  @Before
  public void setUp() {
    OutOfOffHeapMemoryListener ooohml = mock(OutOfOffHeapMemoryListener.class);
    OffHeapMemoryStats stats = mock(OffHeapMemoryStats.class);

    ma = MemoryAllocatorImpl.create(ooohml, stats, 1, OffHeapStorage.MIN_SLAB_SIZE * 1,
        OffHeapStorage.MIN_SLAB_SIZE);
  }

  @After
  public void tearDown() {
    MemoryAllocatorImpl.freeOffHeapMemory();
  }

  private OffHeapStoredObject createChunk(Object value) {
    byte[] v = EntryEventImpl.serialize(value);

    boolean isSerialized = true;
    boolean isCompressed = false;

    OffHeapStoredObject chunk =
        (OffHeapStoredObject) ma.allocateAndInitialize(v, isSerialized, isCompressed);

    return chunk;
  }

  @Test
  public void encodeDataAsAddressShouldReturnZeroIfValueIsGreaterThanSevenBytes() {
    Long value = Long.MAX_VALUE;

    byte[] valueInBytes = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    boolean isSerialized = false;
    boolean isCompressed = false;

    assertThat(valueInBytes.length)
        .isGreaterThanOrEqualTo(OffHeapRegionEntryHelper.MAX_LENGTH_FOR_DATA_AS_ADDRESS);

    long encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(valueInBytes, isSerialized, isCompressed);

    assertThat(encodedAddress).isEqualTo(VALUE_IS_NOT_ENCODABLE);
  }

  @Test
  public void encodeDataAsAddressShouldEncodeLongIfItsSerializedAndIfItsNotTooBig() {
    Long value = 0L;
    long expectedEncodedAddress = 123L;

    byte[] valueInBytes = EntryEventImpl.serialize(value);
    boolean isSerialized = true;
    boolean isCompressed = false;

    long encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(valueInBytes, isSerialized, isCompressed);

    assertThat(encodedAddress).isEqualTo(expectedEncodedAddress);
    assertSerializedAndCompressedBits(encodedAddress, isSerialized, isCompressed);
  }

  @Test
  public void encodeDataAsAddressShouldReturnZeroIfValueIsLongAndItIsSerializedAndBig() {
    Long value = Long.MAX_VALUE;

    byte[] valueInBytes = EntryEventImpl.serialize(value);
    boolean isSerialized = true;
    boolean isCompressed = false;

    long encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(valueInBytes, isSerialized, isCompressed);

    assertThat(encodedAddress).isEqualTo(VALUE_IS_NOT_ENCODABLE);
  }

  @Test
  public void encodeDataAsAddressShouldReturnZeroIfValueIsLargerThanEightBytesAndNotLong() {
    byte[] someValue = new byte[8];
    someValue[0] = DSCODE.CLASS;

    boolean isSerialized = true;
    boolean isCompressed = false;

    long encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(someValue, isSerialized, isCompressed);

    assertThat(encodedAddress).isEqualTo(VALUE_IS_NOT_ENCODABLE);
  }

  @Test
  public void encodeDataAsAddressShouldReturnValidAddressIfValueIsLesserThanSevenBytes() {
    long expectedAddress = 549755813697L;

    Integer value = Integer.MAX_VALUE;

    byte[] valueInBytes = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    boolean isSerialized = false;
    boolean isCompressed = false;

    long encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(valueInBytes, isSerialized, isCompressed);

    assertThat(encodedAddress).isEqualTo(expectedAddress);
    assertSerializedAndCompressedBits(encodedAddress, isSerialized, isCompressed);
  }

  @Test
  public void encodeDataAsAssressShouldSetSerialziedBitIfSerizliaed() {
    long expectedAddress = 63221918596947L;

    Integer value = Integer.MAX_VALUE;

    byte[] valueInBytes = EntryEventImpl.serialize(value);
    boolean isSerialized = true;
    boolean isCompressed = false;

    long encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(valueInBytes, isSerialized, isCompressed);

    assertThat(expectedAddress).isEqualTo(encodedAddress);
    assertSerializedAndCompressedBits(encodedAddress, isSerialized, isCompressed);
  }

  @Test
  public void encodeDataAsAssressShouldSetSerialziedBitIfCompressed() {
    long expectedAddress = 549755813701L;

    Integer value = Integer.MAX_VALUE;

    byte[] valueInBytes = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    boolean isSerialized = false;
    boolean isCompressed = true;

    long encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(valueInBytes, isSerialized, isCompressed);

    assertThat(encodedAddress).isEqualTo(expectedAddress);
    assertSerializedAndCompressedBits(encodedAddress, isSerialized, isCompressed);
  }

  @Test
  public void encodeDataAsAssressShouldSetBothSerialziedAndCompressedBitsIfSerializedAndCompressed() {
    long expectedAddress = 63221918596951L;

    Integer value = Integer.MAX_VALUE;

    byte[] valueInBytes = EntryEventImpl.serialize(value);
    boolean isSerialized = true;
    boolean isCompressed = true;

    long encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(valueInBytes, isSerialized, isCompressed);

    assertThat(expectedAddress).isEqualTo(encodedAddress);
    assertSerializedAndCompressedBits(encodedAddress, isSerialized, isCompressed);
  }

  private void assertSerializedAndCompressedBits(long encodedAddress,
      boolean shouldSerializedBitBeSet, boolean shouldCompressedBitBeSet) {
    boolean isSerializedBitSet = (encodedAddress
        & OffHeapRegionEntryHelper.SERIALIZED_BIT) == OffHeapRegionEntryHelper.SERIALIZED_BIT ? true
            : false;
    boolean isCompressedBitSet = (encodedAddress
        & OffHeapRegionEntryHelper.COMPRESSED_BIT) == OffHeapRegionEntryHelper.COMPRESSED_BIT ? true
            : false;

    assertThat(isSerializedBitSet).isEqualTo(shouldSerializedBitBeSet);
    assertThat(isCompressedBitSet).isEqualTo(shouldCompressedBitBeSet);
  }

  @Test
  public void decodeUncompressedAddressToBytesShouldReturnActualBytes() {
    long encodedAddress = 549755813697L;
    Integer value = Integer.MAX_VALUE;

    byte[] actual = OffHeapRegionEntryHelper.decodeUncompressedAddressToBytes(encodedAddress);
    byte[] expectedValue = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();

    assertThat(actual).isEqualTo(expectedValue);
  }

  @Test
  public void decodeUncompressedAddressToBytesShouldDecodeLongIfItsSerializedAndIfItsNotTooBig() {
    Long value = 0L;
    long encodedAddress = 123L;

    byte[] actual = OffHeapRegionEntryHelper.decodeUncompressedAddressToBytes(encodedAddress);
    byte[] expectedValue = EntryEventImpl.serialize(value);

    assertThat(actual).isEqualTo(expectedValue);
  }

  @Test(expected = AssertionError.class)
  public void decodeUncompressedAddressToBytesWithCompressedAddressShouldThrowException() {
    long encodedAddress = 549755813703L;
    OffHeapRegionEntryHelper.decodeUncompressedAddressToBytes(encodedAddress);
  }

  @Test
  public void decodeCompressedDataAsAddressToRawBytes() {
    long encodedAddress = 549755813703L;
    byte[] expected = new byte[] {127, -1, -1, -1};

    byte[] bytes = OffHeapRegionEntryHelper.decodeAddressToRawBytes(encodedAddress);

    assertThat(bytes).isEqualTo(expected);
  }

  @Test
  public void encodedAddressShouldBeDecodableEvenIfValueIsSerialized() {
    Integer value = Integer.MAX_VALUE;

    byte[] serializedValue = EntryEventImpl.serialize(value);
    boolean isSerialized = true;
    boolean isCompressed = false;

    long encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(serializedValue, isSerialized, isCompressed);

    Integer actualValue = (Integer) OffHeapRegionEntryHelper.decodeAddressToObject(encodedAddress);

    assertThat(actualValue).isEqualTo(value);
  }

  @Test
  public void encodedAddressShouldBeDecodableEvenIfValueIsUnserialized() {
    Integer value = Integer.MAX_VALUE;

    byte[] unSerializedValue = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    boolean isSerialized = false;
    boolean isCompressed = false;

    long encodedAddress =
        OffHeapRegionEntryHelper.encodeDataAsAddress(unSerializedValue, isSerialized, isCompressed);

    byte[] actualValue = (byte[]) OffHeapRegionEntryHelper.decodeAddressToObject(encodedAddress);

    assertThat(actualValue).isEqualTo(unSerializedValue);
  }

  @Test
  public void isSerializedShouldReturnTrueIfSerialized() {
    assertThat(OffHeapRegionEntryHelper.isSerialized(1000010L)).isTrue();
  }

  @Test
  public void isSerializedShouldReturnFalseIfNotSerialized() {
    assertThat(OffHeapRegionEntryHelper.isSerialized(1000000L)).isFalse();
  }

  @Test
  public void isCompressedShouldReturnTrueIfCompressed() {
    assertThat(OffHeapRegionEntryHelper.isCompressed(1000100L)).isTrue();
  }

  @Test
  public void isCompressedShouldReturnFalseIfNotCompressed() {
    assertThat(OffHeapRegionEntryHelper.isCompressed(1000000L)).isFalse();
  }

  @Test
  public void isOffHeapShouldReturnTrueIfAddressIsOnOffHeap() {
    OffHeapStoredObject value = createChunk(Long.MAX_VALUE);
    assertThat(OffHeapRegionEntryHelper.isOffHeap(value.getAddress())).isTrue();
  }

  @Test
  public void isOffHeapShouldReturnFalseIfAddressIsAnEncodedAddress() {
    byte[] data =
        ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt((Integer) Integer.MAX_VALUE).array();
    long address = OffHeapRegionEntryHelper.encodeDataAsAddress(data, false, false);
    assertThat(OffHeapRegionEntryHelper.isOffHeap(address)).isFalse();
  }

  @Test
  public void isOffHeapShouldReturnFalseForAnyTokenAddress() {
    assertThat(OffHeapRegionEntryHelper.isOffHeap(OffHeapRegionEntryHelper.NULL_ADDRESS)).isFalse();
    assertThat(OffHeapRegionEntryHelper.isOffHeap(OffHeapRegionEntryHelper.INVALID_ADDRESS))
        .isFalse();
    assertThat(OffHeapRegionEntryHelper.isOffHeap(OffHeapRegionEntryHelper.LOCAL_INVALID_ADDRESS))
        .isFalse();
    assertThat(OffHeapRegionEntryHelper.isOffHeap(OffHeapRegionEntryHelper.DESTROYED_ADDRESS))
        .isFalse();
    assertThat(OffHeapRegionEntryHelper.isOffHeap(OffHeapRegionEntryHelper.REMOVED_PHASE1_ADDRESS))
        .isFalse();
    assertThat(OffHeapRegionEntryHelper.isOffHeap(OffHeapRegionEntryHelper.REMOVED_PHASE2_ADDRESS))
        .isFalse();
    assertThat(OffHeapRegionEntryHelper.isOffHeap(OffHeapRegionEntryHelper.END_OF_STREAM_ADDRESS))
        .isFalse();
    assertThat(OffHeapRegionEntryHelper.isOffHeap(OffHeapRegionEntryHelper.NOT_AVAILABLE_ADDRESS))
        .isFalse();
    assertThat(OffHeapRegionEntryHelper.isOffHeap(OffHeapRegionEntryHelper.TOMBSTONE_ADDRESS))
        .isFalse();
  }

  @Test
  public void setValueShouldChangeTheRegionEntryAddressToNewAddress() {
    // mock region entry
    OffHeapRegionEntry re = mock(OffHeapRegionEntry.class);

    // some old address
    long oldAddress = 1L;

    // testing when the newValue is a chunk
    OffHeapStoredObject newValue = createChunk(Long.MAX_VALUE);
    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(oldAddress);
    when(re.setAddress(oldAddress, newValue.getAddress())).thenReturn(Boolean.TRUE);

    // invoke the method under test
    OffHeapRegionEntryHelper.setValue(re, newValue);

    // verify oldAddress is replaced with newAddress
    verify(re, times(1)).setAddress(oldAddress, newValue.getAddress());
    // resetting the spy in-order to re-use
    reset(re);

    // testing when the newValue is DataAsAddress
    TinyStoredObject newAddress1 = new TinyStoredObject(2L);
    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(oldAddress);
    when(re.setAddress(oldAddress, newAddress1.getAddress())).thenReturn(Boolean.TRUE);
    OffHeapRegionEntryHelper.setValue(re, newAddress1);

    // verify oldAddress is replaced with newAddress
    verify(re, times(1)).setAddress(oldAddress, newAddress1.getAddress());
    reset(re);

    // Testing when newValue is Token Objects

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(oldAddress);
    when(re.setAddress(oldAddress, OffHeapRegionEntryHelper.NULL_ADDRESS)).thenReturn(Boolean.TRUE);
    OffHeapRegionEntryHelper.setValue(re, null);

    // verify oldAddress is replaced with newAddress
    verify(re, times(1)).setAddress(oldAddress, OffHeapRegionEntryHelper.NULL_ADDRESS);
    reset(re);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(oldAddress);
    when(re.setAddress(oldAddress, OffHeapRegionEntryHelper.INVALID_ADDRESS))
        .thenReturn(Boolean.TRUE);
    OffHeapRegionEntryHelper.setValue(re, Token.INVALID);

    // verify oldAddress is replaced with newAddress
    verify(re, times(1)).setAddress(oldAddress, OffHeapRegionEntryHelper.INVALID_ADDRESS);
    reset(re);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(oldAddress);
    when(re.setAddress(oldAddress, OffHeapRegionEntryHelper.LOCAL_INVALID_ADDRESS))
        .thenReturn(Boolean.TRUE);
    OffHeapRegionEntryHelper.setValue(re, Token.LOCAL_INVALID);

    // verify oldAddress is replaced with newAddress
    verify(re, times(1)).setAddress(oldAddress, OffHeapRegionEntryHelper.LOCAL_INVALID_ADDRESS);
    reset(re);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(oldAddress);
    when(re.setAddress(oldAddress, OffHeapRegionEntryHelper.DESTROYED_ADDRESS))
        .thenReturn(Boolean.TRUE);
    OffHeapRegionEntryHelper.setValue(re, Token.DESTROYED);

    // verify oldAddress is replaced with newAddress
    verify(re, times(1)).setAddress(oldAddress, OffHeapRegionEntryHelper.DESTROYED_ADDRESS);
    reset(re);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(oldAddress);
    when(re.setAddress(oldAddress, OffHeapRegionEntryHelper.REMOVED_PHASE1_ADDRESS))
        .thenReturn(Boolean.TRUE);
    OffHeapRegionEntryHelper.setValue(re, Token.REMOVED_PHASE1);

    // verify oldAddress is replaced with newAddress
    verify(re, times(1)).setAddress(oldAddress, OffHeapRegionEntryHelper.REMOVED_PHASE1_ADDRESS);
    reset(re);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(oldAddress);
    when(re.setAddress(oldAddress, OffHeapRegionEntryHelper.REMOVED_PHASE2_ADDRESS))
        .thenReturn(Boolean.TRUE);
    OffHeapRegionEntryHelper.setValue(re, Token.REMOVED_PHASE2);

    // verify oldAddress is replaced with newAddress
    verify(re, times(1)).setAddress(oldAddress, OffHeapRegionEntryHelper.REMOVED_PHASE2_ADDRESS);
    reset(re);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(oldAddress);
    when(re.setAddress(oldAddress, OffHeapRegionEntryHelper.END_OF_STREAM_ADDRESS))
        .thenReturn(Boolean.TRUE);
    OffHeapRegionEntryHelper.setValue(re, Token.END_OF_STREAM);

    // verify oldAddress is replaced with newAddress
    verify(re, times(1)).setAddress(oldAddress, OffHeapRegionEntryHelper.END_OF_STREAM_ADDRESS);
    reset(re);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(oldAddress);
    when(re.setAddress(oldAddress, OffHeapRegionEntryHelper.NOT_AVAILABLE_ADDRESS))
        .thenReturn(Boolean.TRUE);
    OffHeapRegionEntryHelper.setValue(re, Token.NOT_AVAILABLE);

    // verify oldAddress is replaced with newAddress
    verify(re, times(1)).setAddress(oldAddress, OffHeapRegionEntryHelper.NOT_AVAILABLE_ADDRESS);
    reset(re);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(oldAddress);
    when(re.setAddress(oldAddress, OffHeapRegionEntryHelper.TOMBSTONE_ADDRESS))
        .thenReturn(Boolean.TRUE);
    OffHeapRegionEntryHelper.setValue(re, Token.TOMBSTONE);

    // verify oldAddress is replaced with newAddress
    verify(re, times(1)).setAddress(oldAddress, OffHeapRegionEntryHelper.TOMBSTONE_ADDRESS);
    reset(re);
  }

  @Test
  public void setValueShouldChangeTheRegionEntryAddressToNewAddressAndReleaseOldValueIfItsOnOffHeap() {
    // mock region entry
    OffHeapRegionEntry re = mock(OffHeapRegionEntry.class);

    OffHeapStoredObject oldValue = createChunk(Long.MAX_VALUE);
    OffHeapStoredObject newValue = createChunk(Long.MAX_VALUE - 1);

    // mock Chunk static methods - in-order to verify that release is called
    PowerMockito.spy(OffHeapStoredObject.class);
    PowerMockito.doNothing().when(OffHeapStoredObject.class);
    OffHeapStoredObject.release(oldValue.getAddress());

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(oldValue.getAddress());
    when(re.setAddress(oldValue.getAddress(), newValue.getAddress())).thenReturn(Boolean.TRUE);

    // invoke the method under test
    OffHeapRegionEntryHelper.setValue(re, newValue);

    // verify oldAddress is changed to newAddress
    verify(re, times(1)).setAddress(oldValue.getAddress(), newValue.getAddress());

    // verify oldAddress is released
    PowerMockito.verifyStatic();
    OffHeapStoredObject.release(oldValue.getAddress());
  }

  @Test
  public void setValueShouldChangeTheRegionEntryAddressToNewAddressAndDoesNothingIfOldAddressIsAnEncodedAddress() {
    // mock region entry
    OffHeapRegionEntry re = mock(OffHeapRegionEntry.class);

    byte[] oldData =
        ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt((Integer) Integer.MAX_VALUE).array();
    long oldAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(oldData, false, false);

    byte[] newData = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE)
        .putInt((Integer) Integer.MAX_VALUE - 1).array();
    TinyStoredObject newAddress =
        new TinyStoredObject(OffHeapRegionEntryHelper.encodeDataAsAddress(newData, false, false));

    // mock Chunk static methods - in-order to verify that release is never called
    PowerMockito.spy(OffHeapStoredObject.class);
    PowerMockito.doNothing().when(OffHeapStoredObject.class);
    OffHeapStoredObject.release(oldAddress);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(oldAddress);
    when(re.setAddress(oldAddress, newAddress.getAddress())).thenReturn(Boolean.TRUE);

    // invoke the method under test
    OffHeapRegionEntryHelper.setValue(re, newAddress);

    // verify oldAddress is changed to newAddress
    verify(re, times(1)).setAddress(oldAddress, newAddress.getAddress());

    // verify that release is never called as the old address is not on offheap
    PowerMockito.verifyStatic(never());
    OffHeapStoredObject.release(oldAddress);
  }

  @Test
  public void setValueShouldChangeTheRegionEntryAddressToNewAddressAndDoesNothingIfOldAddressIsATokenAddress() {
    // mock region entry
    OffHeapRegionEntry re = mock(OffHeapRegionEntry.class);

    long oldAddress = OffHeapRegionEntryHelper.REMOVED_PHASE1_ADDRESS;

    Token newValue = Token.REMOVED_PHASE2;
    long newAddress = OffHeapRegionEntryHelper.REMOVED_PHASE2_ADDRESS;

    // mock Chunk static methods - in-order to verify that release is never called
    PowerMockito.spy(OffHeapStoredObject.class);
    PowerMockito.doNothing().when(OffHeapStoredObject.class);
    OffHeapStoredObject.release(oldAddress);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(oldAddress);
    when(re.setAddress(oldAddress, newAddress)).thenReturn(Boolean.TRUE);

    // invoke the method under test
    OffHeapRegionEntryHelper.setValue(re, newValue);

    // verify oldAddress is changed to newAddress
    verify(re, times(1)).setAddress(oldAddress, newAddress);

    // verify that release is never called as the old address is not on offheap
    PowerMockito.verifyStatic(never());
    OffHeapStoredObject.release(oldAddress);
  }

  @Test(expected = IllegalStateException.class)
  public void setValueShouldThrowIllegalExceptionIfNewValueCannotBeConvertedToAddress() {
    // mock region entry
    OffHeapRegionEntry re = mock(OffHeapRegionEntry.class);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(1L);

    // invoke the method under test with some object other than Chunk/DataAsAddress/Token
    OffHeapRegionEntryHelper.setValue(re, new Object());
  }

  @Test
  public void getValueAsTokenShouldReturnNotATokenIfValueIsOnOffHeap() {
    // mock region entry
    OffHeapRegionEntry re = mock(OffHeapRegionEntry.class);

    OffHeapStoredObject chunk = createChunk(Long.MAX_VALUE);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(chunk.getAddress());
    Token token = OffHeapRegionEntryHelper.getValueAsToken(re);

    assertThat(token).isEqualTo(Token.NOT_A_TOKEN);
  }

  @Test
  public void getValueAsTokenShouldReturnNotATokenIfValueIsEncoded() {
    // mock region entry
    OffHeapRegionEntry re = mock(OffHeapRegionEntry.class);

    byte[] data = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(Integer.MAX_VALUE).array();
    long address = OffHeapRegionEntryHelper.encodeDataAsAddress(data, false, false);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(address);
    Token token = OffHeapRegionEntryHelper.getValueAsToken(re);

    assertThat(token).isEqualTo(Token.NOT_A_TOKEN);
  }

  @Test
  public void getValueAsTokenShouldReturnAValidToken() {
    // mock region entry
    OffHeapRegionEntry re = mock(OffHeapRegionEntry.class);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(OffHeapRegionEntryHelper.NULL_ADDRESS);
    Token token = OffHeapRegionEntryHelper.getValueAsToken(re);

    assertThat(token).isNull();

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(OffHeapRegionEntryHelper.INVALID_ADDRESS);
    token = OffHeapRegionEntryHelper.getValueAsToken(re);

    assertThat(token).isEqualTo(Token.INVALID);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(OffHeapRegionEntryHelper.LOCAL_INVALID_ADDRESS);
    token = OffHeapRegionEntryHelper.getValueAsToken(re);

    assertThat(token).isEqualTo(Token.LOCAL_INVALID);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(OffHeapRegionEntryHelper.DESTROYED_ADDRESS);
    token = OffHeapRegionEntryHelper.getValueAsToken(re);

    assertThat(token).isEqualTo(Token.DESTROYED);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(OffHeapRegionEntryHelper.REMOVED_PHASE1_ADDRESS);
    token = OffHeapRegionEntryHelper.getValueAsToken(re);

    assertThat(token).isEqualTo(Token.REMOVED_PHASE1);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(OffHeapRegionEntryHelper.REMOVED_PHASE2_ADDRESS);
    token = OffHeapRegionEntryHelper.getValueAsToken(re);

    assertThat(token).isEqualTo(Token.REMOVED_PHASE2);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(OffHeapRegionEntryHelper.END_OF_STREAM_ADDRESS);
    token = OffHeapRegionEntryHelper.getValueAsToken(re);

    assertThat(token).isEqualTo(Token.END_OF_STREAM);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(OffHeapRegionEntryHelper.NOT_AVAILABLE_ADDRESS);
    token = OffHeapRegionEntryHelper.getValueAsToken(re);

    assertThat(token).isEqualTo(Token.NOT_AVAILABLE);

    // mock region entry methods required for test
    when(re.getAddress()).thenReturn(OffHeapRegionEntryHelper.TOMBSTONE_ADDRESS);
    token = OffHeapRegionEntryHelper.getValueAsToken(re);

    assertThat(token).isEqualTo(Token.TOMBSTONE);
  }

  @Test
  public void addressToObjectShouldReturnValueFromChunk() {
    OffHeapStoredObject expected = createChunk(Long.MAX_VALUE);
    Object actual = OffHeapRegionEntryHelper.addressToObject(expected.getAddress(), false, null);

    assertThat(actual).isInstanceOf(OffHeapStoredObject.class);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void addressToObjectShouldReturnCachedDeserializableFromChunkIfAskedToDecompress() {
    byte[] data = EntryEventImpl.serialize(Long.MAX_VALUE);
    boolean isSerialized = true;
    boolean isCompressed = true;

    OffHeapStoredObject chunk =
        (OffHeapStoredObject) ma.allocateAndInitialize(data, isSerialized, isCompressed);

    // create the mock context
    RegionEntryContext regionContext = mock(RegionEntryContext.class);
    CachePerfStats cacheStats = mock(CachePerfStats.class);
    Compressor compressor = mock(Compressor.class);

    long startTime = 10000L;

    // mock required things
    when(regionContext.getCompressor()).thenReturn(compressor);
    when(compressor.decompress(data)).thenReturn(data);
    when(regionContext.getCachePerfStats()).thenReturn(cacheStats);
    when(cacheStats.startDecompression()).thenReturn(startTime);

    Object actual =
        OffHeapRegionEntryHelper.addressToObject(chunk.getAddress(), true, regionContext);

    assertThat(actual).isInstanceOf(VMCachedDeserializable.class);

    Long actualValue = (Long) ((VMCachedDeserializable) actual).getDeserializedForReading();
    assertThat(actualValue).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void addressToObjectShouldReturnDecompressedValueFromChunkIfAskedToDecompress() {
    byte[] data = ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(Long.MAX_VALUE).array();
    boolean isSerialized = false;
    boolean isCompressed = true;

    OffHeapStoredObject chunk =
        (OffHeapStoredObject) ma.allocateAndInitialize(data, isSerialized, isCompressed);

    // create the mock context
    RegionEntryContext regionContext = mock(RegionEntryContext.class);
    CachePerfStats cacheStats = mock(CachePerfStats.class);
    Compressor compressor = mock(Compressor.class);

    long startTime = 10000L;

    // mock required things
    when(regionContext.getCompressor()).thenReturn(compressor);
    when(compressor.decompress(data)).thenReturn(data);
    when(regionContext.getCachePerfStats()).thenReturn(cacheStats);
    when(cacheStats.startDecompression()).thenReturn(startTime);

    Object actual =
        OffHeapRegionEntryHelper.addressToObject(chunk.getAddress(), true, regionContext);

    assertThat(actual).isInstanceOf(byte[].class);
    assertThat(actual).isEqualTo(data);
  }

  @Test
  public void addressToObjectShouldReturnValueFromDataAsAddress() {
    byte[] data = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(Integer.MAX_VALUE).array();
    long address = OffHeapRegionEntryHelper.encodeDataAsAddress(data, false, false);

    TinyStoredObject expected = new TinyStoredObject(address);
    Object actual = OffHeapRegionEntryHelper.addressToObject(address, false, null);

    assertThat(actual).isInstanceOf(TinyStoredObject.class);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void addressToObjectShouldReturnCachedDeserializableFromSerializedDataAsAddressIfAskedToDecompress() {
    byte[] data = EntryEventImpl.serialize(Integer.MAX_VALUE);
    boolean isSerialized = true;
    boolean isCompressed = true;

    long address = OffHeapRegionEntryHelper.encodeDataAsAddress(data, isSerialized, isCompressed);

    // create the mock context
    RegionEntryContext regionContext = mock(RegionEntryContext.class);
    CachePerfStats cacheStats = mock(CachePerfStats.class);
    Compressor compressor = mock(Compressor.class);

    long startTime = 10000L;

    // mock required things
    when(regionContext.getCompressor()).thenReturn(compressor);
    when(compressor.decompress(data)).thenReturn(data);
    when(regionContext.getCachePerfStats()).thenReturn(cacheStats);
    when(cacheStats.startDecompression()).thenReturn(startTime);

    Object actual = OffHeapRegionEntryHelper.addressToObject(address, true, regionContext);

    assertThat(actual).isInstanceOf(VMCachedDeserializable.class);

    Integer actualValue = (Integer) ((VMCachedDeserializable) actual).getDeserializedForReading();
    assertThat(actualValue).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void addressToObjectShouldReturnDecompressedValueFromDataAsAddressIfAskedToDecompress() {
    byte[] data = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(Integer.MAX_VALUE).array();
    boolean isSerialized = false;
    boolean isCompressed = true;

    long address = OffHeapRegionEntryHelper.encodeDataAsAddress(data, isSerialized, isCompressed);

    // create the mock context
    RegionEntryContext regionContext = mock(RegionEntryContext.class);
    CachePerfStats cacheStats = mock(CachePerfStats.class);
    Compressor compressor = mock(Compressor.class);

    long startTime = 10000L;

    // mock required things
    when(regionContext.getCompressor()).thenReturn(compressor);
    when(compressor.decompress(data)).thenReturn(data);
    when(regionContext.getCachePerfStats()).thenReturn(cacheStats);
    when(cacheStats.startDecompression()).thenReturn(startTime);

    Object actual = OffHeapRegionEntryHelper.addressToObject(address, true, regionContext);

    assertThat(actual).isInstanceOf(byte[].class);
    assertThat(actual).isEqualTo(data);
  }

  @Test
  public void addressToObjectShouldReturnToken() {
    Token token = (Token) OffHeapRegionEntryHelper
        .addressToObject(OffHeapRegionEntryHelper.NULL_ADDRESS, false, null);
    assertThat(token).isNull();

    token = (Token) OffHeapRegionEntryHelper
        .addressToObject(OffHeapRegionEntryHelper.INVALID_ADDRESS, false, null);
    assertThat(token).isEqualTo(Token.INVALID);

    token = (Token) OffHeapRegionEntryHelper
        .addressToObject(OffHeapRegionEntryHelper.LOCAL_INVALID_ADDRESS, false, null);
    assertThat(token).isEqualTo(Token.LOCAL_INVALID);

    token = (Token) OffHeapRegionEntryHelper
        .addressToObject(OffHeapRegionEntryHelper.DESTROYED_ADDRESS, false, null);
    assertThat(token).isEqualTo(Token.DESTROYED);

    token = (Token) OffHeapRegionEntryHelper
        .addressToObject(OffHeapRegionEntryHelper.REMOVED_PHASE1_ADDRESS, false, null);
    assertThat(token).isEqualTo(Token.REMOVED_PHASE1);

    token = (Token) OffHeapRegionEntryHelper
        .addressToObject(OffHeapRegionEntryHelper.REMOVED_PHASE2_ADDRESS, false, null);
    assertThat(token).isEqualTo(Token.REMOVED_PHASE2);

    token = (Token) OffHeapRegionEntryHelper
        .addressToObject(OffHeapRegionEntryHelper.END_OF_STREAM_ADDRESS, false, null);
    assertThat(token).isEqualTo(Token.END_OF_STREAM);

    token = (Token) OffHeapRegionEntryHelper
        .addressToObject(OffHeapRegionEntryHelper.NOT_AVAILABLE_ADDRESS, false, null);
    assertThat(token).isEqualTo(Token.NOT_AVAILABLE);

    token = (Token) OffHeapRegionEntryHelper
        .addressToObject(OffHeapRegionEntryHelper.TOMBSTONE_ADDRESS, false, null);
    assertThat(token).isEqualTo(Token.TOMBSTONE);
  }

  @Test
  public void getSerializedLengthFromDataAsAddressShouldReturnValidLength() {
    byte[] data = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(Integer.MAX_VALUE).array();
    boolean isSerialized = false;
    boolean isCompressed = true;

    long address = OffHeapRegionEntryHelper.encodeDataAsAddress(data, isSerialized, isCompressed);
    TinyStoredObject daa = new TinyStoredObject(address);

    int actualLength = OffHeapRegionEntryHelper.getSerializedLength(daa);

    assertThat(actualLength).isEqualTo(data.length);
  }

  @Test
  public void getSerializedLengthFromDataAsAddressShouldReturnZeroForNonEncodedAddress() {
    TinyStoredObject nonEncodedAddress = new TinyStoredObject(100000L);
    int actualLength = OffHeapRegionEntryHelper.getSerializedLength(nonEncodedAddress);
    assertThat(actualLength).isZero();
  }

  @Test
  public void releaseEntryShouldSetValueToRemovePhase2() {
    // mock region entry
    OffHeapRegionEntry re = mock(OffHeapRegionEntry.class);
    when(re.getAddress()).thenReturn(1L);
    when(re.setAddress(1L, OffHeapRegionEntryHelper.REMOVED_PHASE2_ADDRESS))
        .thenReturn(Boolean.TRUE);

    // mock required methods
    PowerMockito.spy(OffHeapRegionEntryHelper.class);
    PowerMockito.doNothing().when(OffHeapRegionEntryHelper.class);
    OffHeapRegionEntryHelper.setValue(re, Token.REMOVED_PHASE2);

    OffHeapRegionEntryHelper.releaseEntry(re);

    PowerMockito.verifyStatic();
    OffHeapRegionEntryHelper.setValue(re, Token.REMOVED_PHASE2);
  }

  @Test
  public void releaseEntryShouldSetValueToRemovePhase2AndSetsAsyncToFalseForDiskEntry() {
    // mock region entry
    OffHeapRegionEntry re = mock(VersionedStatsDiskRegionEntryOffHeap.class);
    when(re.getAddress()).thenReturn(1L);
    when(re.setAddress(1L, OffHeapRegionEntryHelper.REMOVED_PHASE2_ADDRESS))
        .thenReturn(Boolean.TRUE);

    DiskId spy = Mockito.spy(DiskId.class);
    when(((DiskEntry) re).getDiskId()).thenReturn(spy);
    when(spy.isPendingAsync()).thenReturn(Boolean.TRUE);

    // mock required methods
    PowerMockito.spy(OffHeapRegionEntryHelper.class);
    PowerMockito.doNothing().when(OffHeapRegionEntryHelper.class);
    OffHeapRegionEntryHelper.setValue(re, Token.REMOVED_PHASE2);

    OffHeapRegionEntryHelper.releaseEntry(re);

    verify(spy, times(1)).setPendingAsync(Boolean.FALSE);

    PowerMockito.verifyStatic();
    OffHeapRegionEntryHelper.setValue(re, Token.REMOVED_PHASE2);
  }

  @Test
  public void doWithOffHeapClearShouldSetTheThreadLocalToTrue() {
    // verify that threadlocal is not set
    assertThat(OffHeapRegionEntryHelper.doesClearNeedToCheckForOffHeap()).isFalse();

    OffHeapRegionEntryHelper.doWithOffHeapClear(new Runnable() {
      @Override
      public void run() {
        // verify that threadlocal is set when offheap is cleared
        assertThat(OffHeapRegionEntryHelper.doesClearNeedToCheckForOffHeap()).isTrue();
      }
    });

    // verify that threadlocal is reset after offheap is cleared
    assertThat(OffHeapRegionEntryHelper.doesClearNeedToCheckForOffHeap()).isFalse();
  }
}
