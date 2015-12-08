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

import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.offheap.DataAsAddress;
import com.gemstone.gemfire.internal.offheap.OffHeapRegionEntryHelper;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class AbstractStoredObjectTestBase {

    @Test
    public void getValueAsDeserializedHeapObjectShouldReturnDeserializedValueIfValueIsSerialized() {
        Integer regionEntryValue = 123456789;
        byte[] serializedRegionEntryValue = EntryEventImpl.serialize(regionEntryValue);

        //encode a serialized entry value to address
        long encodedAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(serializedRegionEntryValue, true, false);

        DataAsAddress offheapAddress = new DataAsAddress(encodedAddress);

        Integer actualRegionEntryValue = (Integer) offheapAddress.getValueAsDeserializedHeapObject();

        assertEquals(regionEntryValue, actualRegionEntryValue);
    }

    @Test
    public void getValueAsDeserializedHeapObjectShouldReturnValueAsIsIfNotSerialized() {
        int regionEntryValue = 123456789;
        byte[] regionEntryValueAsBytes =  ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(regionEntryValue).array();

        //encode a non-serialized entry value to address
        long encodedAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(regionEntryValueAsBytes, false, false);

        DataAsAddress offheapAddress = new DataAsAddress(encodedAddress);

        byte[] deserializedValue = (byte[]) offheapAddress.getValueAsDeserializedHeapObject();
        int actualRegionEntryValue = ByteBuffer.wrap(deserializedValue).getInt();

        assertEquals(regionEntryValue, actualRegionEntryValue);
    }

    @Test
    public void getValueAsHeapByteArrayShouldReturnSerializedByteArrayIfValueIsSerialized() {
        Integer regionEntryValue = 123456789;
        byte[] serializedRegionEntryValue = EntryEventImpl.serialize(regionEntryValue);

        //encode a serialized entry value to address
        long encodedAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(serializedRegionEntryValue, true, false);

        DataAsAddress offheapAddress = new DataAsAddress(encodedAddress);

        byte[] actualSerializedRegionEntryValue = (byte[]) offheapAddress.getValueAsHeapByteArray();

        assertArrayEquals(serializedRegionEntryValue, actualSerializedRegionEntryValue);
    }

    @Test
    public void getValueAsHeapByteArrayShouldReturnDeserializedByteArrayIfValueIsNotSerialized() {
        int regionEntryValue = 123456789;
        byte[] regionEntryValueAsBytes =  ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(regionEntryValue).array();

        //encode a non-serialized entry value to address
        long encodedAddress = OffHeapRegionEntryHelper.encodeDataAsAddress(regionEntryValueAsBytes, false, false);

        DataAsAddress offheapAddress = new DataAsAddress(encodedAddress);

        byte[] deserializedValue = (byte[]) offheapAddress.getValueAsHeapByteArray();
        int actualRegionEntryValue = ByteBuffer.wrap(deserializedValue).getInt();

        assertEquals(regionEntryValue, actualRegionEntryValue);
    }
}
