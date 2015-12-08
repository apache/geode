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

package com.gemstone.gemfire.internal.cache.offheap;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.internal.cache.BytesAndBitsForCompactor;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.RegionEntryContext;
import com.gemstone.gemfire.internal.cache.persistence.BytesAndBits;
import com.gemstone.gemfire.internal.offheap.DataAsAddress;

import com.gemstone.gemfire.internal.offheap.OffHeapRegionEntryHelper;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

@Category(UnitTest.class)
public class DataAsAddressJUnitTest extends AbstractStoredObjectTestBase {

    @Test
    public void shouldReturnCorrectEncodingAddress() {

        DataAsAddress address1 = new DataAsAddress(10001L);
        assertNotNull(address1);
        assertEquals("Encoding address should be:", 10001, address1.getEncodedAddress());

        DataAsAddress address2 = new DataAsAddress(10002L);
        assertNotNull(address2);
        assertEquals("Returning always 10001 expected 10002", 10002, address2.getEncodedAddress());
    }

    @Test
    public void twoAddressesShouldBeEqualIfEncodingAddressIsSame() {
        DataAsAddress address1 = new DataAsAddress(10001L);
        DataAsAddress address2 = new DataAsAddress(10001L);

        assertEquals("Two addresses are equal if encoding address is same", true, address1.equals(address2));
    }

    @Test
    public void twoAddressesShouldNotBeEqualIfEncodingAddressIsNotSame() {
        DataAsAddress address1 = new DataAsAddress(10001L);
        DataAsAddress address2 = new DataAsAddress(10002L);

        assertEquals("Two addresses are not equal if encoding address is not same", false, address1.equals(address2));
    }

    @Test
    public void twoAddressesAreNotEqualIfTheyAreNotTypeDataAsAddress() {
        DataAsAddress address1 = new DataAsAddress(10001L);
        Long address2 = new Long(10002L);

        assertEquals("Two addresses are not equal if encoding address is not same", false, address1.equals(address2));
    }

    @Test
    public void addressHashCodeShouldBe() {
        DataAsAddress address1 = new DataAsAddress(10001L);
        assertEquals("", 10001, address1.hashCode());
    }

    @Test
    public void getSizeInBytesAlwaysReturnsZero() {
        DataAsAddress address1 = new DataAsAddress(10001L);
        DataAsAddress address2 = new DataAsAddress(10002L);

        assertEquals("getSizeInBytes", 0, address1.getSizeInBytes());
        assertEquals("getSizeInBytes", 0, address2.getSizeInBytes());
    }

    @Test
    public void getValueSizeInBytesAlwaysReturnsZero() {
        DataAsAddress address1 = new DataAsAddress(10001L);
        DataAsAddress address2 = new DataAsAddress(10002L);

        assertEquals("getSizeInBytes", 0, address1.getValueSizeInBytes());
        assertEquals("getSizeInBytes", 0, address2.getValueSizeInBytes());
    }

    @Test
    public void retainShouldAlwaysBeTrue() {
        DataAsAddress address1 = new DataAsAddress(10001L);
        DataAsAddress address2 = new DataAsAddress(10002L);

        assertEquals("retain", true, address1.retain());
        assertEquals("retain", true, address2.retain());
    }

    @Test
    public void dataAsAddressShouldImplementReleaseToAdhereToStoredObject() {
        DataAsAddress address = new DataAsAddress(10001L);
        address.release();
    }

    @Test
    public void isCompressedShouldReturnTrueIfCompressed() {
        int regionEntryValue = 1234567;
        byte[] regionEntryValueAsBytes =  ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(regionEntryValue).array();

        //encode a non-serialized entry value to address and compress it - last argument true is to let that it is compressed
        long encodedAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(regionEntryValueAsBytes, false, true);
        DataAsAddress offheapAddress = new DataAsAddress(encodedAddress);

        assertEquals("Should return true as it is compressed", true, offheapAddress.isCompressed());
    }

    @Test
    public void isCompressedShouldReturnFalseIfNotCompressed() {
        int regionEntryValue = 123456789;
        byte[] regionEntryValueAsBytes =  ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(regionEntryValue).array();

        //encode a non-serialized entry value to address and compress it - last argument true is to let that it is compressed
        long encodedAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(regionEntryValueAsBytes, false, false);
        DataAsAddress offheapAddress = new DataAsAddress(encodedAddress);

        assertEquals("Should return false as it is compressed", false, offheapAddress.isCompressed());
    }

    @Test
    public void isSerializedShouldReturnTrueIfSeriazlied() {
        Integer regionEntryValue = 123456789;
        byte[] serializedRegionEntryValue = EntryEventImpl.serialize(regionEntryValue);

        //encode a serialized entry value to address and compress it - second argument is to let that it is serialized
        long encodedAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(serializedRegionEntryValue, true, false);
        DataAsAddress offheapAddress = new DataAsAddress(encodedAddress);

        assertEquals("Should return true as it is serialized", true, offheapAddress.isSerialized());
    }

    @Test
    public void isSerializedShouldReturnFalseIfNotSeriazlied() {
        Integer regionEntryValue = 123456789;
        byte[] serializedRegionEntryValue = EntryEventImpl.serialize(regionEntryValue);

        //encode a serialized entry value to address and compress it - second argument is to let that it is serialized
        long encodedAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(serializedRegionEntryValue, false, false);
        DataAsAddress offheapAddress = new DataAsAddress(encodedAddress);

        assertEquals("Should return false as it is serialized", false, offheapAddress.isSerialized());
    }

    @Test
    public void getDecompressedBytesShouldReturnDecompressedBytesIfCompressed() {
        int regionEntryValue = 123456789;
        byte[] regionEntryValueAsBytes =  ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(regionEntryValue).array();

        //encode a non-serialized entry value to address and compress it - last argument true is to compress it
        long encodedAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(regionEntryValueAsBytes, false, true);
        DataAsAddress offheapAddress = new DataAsAddress(encodedAddress);

        RegionEntryContext regionContext = mock(RegionEntryContext.class);
        CachePerfStats cacheStats = mock(CachePerfStats.class);
        Compressor compressor = mock(Compressor.class);

        long startTime = 10000L;

        //mock required things
        when(regionContext.getCompressor()).thenReturn(compressor);
        when(compressor.decompress(new byte[]{7, 91, -51, 21})).thenReturn(new byte[]{7, 91, -51, 21});
        when(regionContext.getCachePerfStats()).thenReturn(cacheStats);
        when(cacheStats.startDecompression()).thenReturn(startTime);

        //invoke the thing
        byte[] bytes = offheapAddress.getDecompressedBytes(regionContext);

        //verify the thing happened
        verify(cacheStats, atLeastOnce()).startDecompression();
        verify(compressor, times(1)).decompress(new byte[]{7, 91, -51, 21});
        verify(cacheStats, atLeastOnce()).endDecompression(startTime);

        assertArrayEquals(regionEntryValueAsBytes, bytes);
    }

    @Test
    public void getDecompressedBytesShouldNotTryToDecompressIfNotDecompressed() {
        int regionEntryValue = 123456789;
        byte[] regionEntryValueAsBytes =  ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(regionEntryValue).array();

        //encode a non-serialized entry value to address and don`t compress it - last argument false is not to compress it
        long encodedAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(regionEntryValueAsBytes, false, false);

        DataAsAddress offheapAddress = new DataAsAddress(encodedAddress);

        //mock the thing
        RegionEntryContext regionContext = mock(RegionEntryContext.class);
        Compressor compressor = mock(Compressor.class);
        when(regionContext.getCompressor()).thenReturn(compressor);

        //invoke the thing
        byte[] actual = offheapAddress.getDecompressedBytes(regionContext);

        //verify the thing happened
        verify(regionContext, never()).getCompressor();
        assertArrayEquals(regionEntryValueAsBytes, actual);
    }

    @Test
    public void getRawBytesShouldReturnAByteArray() {
        int regionEntryValue = 123456789;
        byte[] regionEntryValueAsBytes =  ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(regionEntryValue).array();

        //encode a non-serialized entry value to address
        long encodedAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(regionEntryValueAsBytes, false, false);

        DataAsAddress offheapAddress = new DataAsAddress(encodedAddress);
        byte[] actual = offheapAddress.getRawBytes();

        assertArrayEquals(regionEntryValueAsBytes, actual);

    }

    @Test
    public void getSerializedValueShouldReturnASerializedByteArray() {
        Integer regionEntryValue = 123456789;
        byte[] serializedRegionEntryValue = EntryEventImpl.serialize(regionEntryValue);

        //encode a serialized entry value to address
        long encodedAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(serializedRegionEntryValue, true, false);

        DataAsAddress offheapAddress = new DataAsAddress(encodedAddress);

        byte[] actual = offheapAddress.getSerializedValue();

        assertArrayEquals(serializedRegionEntryValue, actual);
    }

    @Test
    public void getDeserializedObjectShouldReturnADeserializedObject() {
        Integer regionEntryValue = 123456789;
        byte[] serializedRegionEntryValue = EntryEventImpl.serialize(regionEntryValue);

        //encode a serialized entry value to address
        long encodedAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(serializedRegionEntryValue, true, false);

        DataAsAddress offheapAddress = new DataAsAddress(encodedAddress);

        Integer actualRegionEntryValue = (Integer) offheapAddress.getDeserializedValue(null, null);

        assertEquals(regionEntryValue, actualRegionEntryValue);
    }

    @Test
    public void getDeserializedObjectShouldReturnAByteArrayAsIsIfNotSerialized() {
        int regionEntryValue = 123456789;
        byte[] regionEntryValueAsBytes =  ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(regionEntryValue).array();

        //encode a non-serialized entry value to address
        long encodedAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(regionEntryValueAsBytes, false, false);

        DataAsAddress offheapAddress = new DataAsAddress(encodedAddress);

        byte[] deserializeValue = (byte[]) offheapAddress.getDeserializedValue(null, null);
        int actualRegionEntryValue = ByteBuffer.wrap(deserializeValue).getInt();

        assertEquals(regionEntryValue, actualRegionEntryValue);
    }

    @Test
    public void fillSerializedValueShouldFillWrapperWithSerializedValueIfValueIsSerialized() {
        Integer regionEntryValue = 123456789;
        byte[] serializedRegionEntryValue = EntryEventImpl.serialize(regionEntryValue);

        //encode a serialized entry value to address
        long encodedAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(serializedRegionEntryValue, true, false);

        DataAsAddress offheapAddress = new DataAsAddress(encodedAddress);

        //mock the things
        BytesAndBitsForCompactor wrapper = mock(BytesAndBitsForCompactor.class);

        byte userBits = 1;
        offheapAddress.fillSerializedValue(wrapper, userBits);

        verify(wrapper, times(1)).setData(serializedRegionEntryValue, userBits, serializedRegionEntryValue.length, true);
    }

    @Test
    public void fillSerializedValueShouldFillWrapperWithDeserializedValueIfValueIsNotSerialized() {
        int regionEntryValue = 1234567;
        byte[] regionEntryValueAsBytes =  ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(regionEntryValue).array();

        //encode a serialized entry value to address
        long encodedAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(regionEntryValueAsBytes, false, false);

        DataAsAddress offheapAddress = new DataAsAddress(encodedAddress);

        //mock the things
        BytesAndBitsForCompactor wrapper = mock(BytesAndBitsForCompactor.class);

        byte userBits = 1;
        offheapAddress.fillSerializedValue(wrapper, userBits);

        verify(wrapper, times(1)).setData(regionEntryValueAsBytes, userBits, regionEntryValueAsBytes.length, true);
    }
}
