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

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BytesAndBitsForCompactor;
import org.apache.geode.internal.cache.EntryBits;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryContext;

/**
 * Used to represent stored objects that can be stored in the address field. The RegionEntry for an
 * off-heap region uses a primitive long to store the off-heap address of the entry's value. If the
 * value can be encoded as a long (i.e. its serialized representation will fit in the 8 bytes of a
 * long without looking like an actual off-heap address) then these tiny values on an off-heap
 * regions are actually stored on the heap in the primitive long field. When these values are
 * "objectified" they will be an instance of this class. Instances of this class have a very short
 * lifetime.
 */
public class TinyStoredObject extends AbstractStoredObject {
  private final long address;

  public TinyStoredObject(long addr) {
    address = addr;
  }

  @Override
  public long getAddress() {
    return address;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o instanceof TinyStoredObject) {
      return getAddress() == ((TinyStoredObject) o).getAddress();
    }
    return false;
  }

  @Override
  public int hashCode() {
    long value = getAddress();
    return (int) (value ^ (value >>> 32));
  }

  @Override
  public int getSizeInBytes() {
    return 0;
  }

  public byte[] getDecompressedBytes(RegionEntryContext r) {
    if (isCompressed()) {
      byte[] bytes = OffHeapRegionEntryHelper.decodeAddressToRawBytes(getAddress());
      long time = r.getCachePerfStats().startDecompression();
      bytes = r.getCompressor().decompress(bytes);
      r.getCachePerfStats().endDecompression(time);
      return bytes;
    }
    return getRawBytes();
  }

  /**
   * If we contain a byte[] return it. Otherwise return the serialize bytes in us in a byte array.
   */
  public byte[] getRawBytes() {
    return OffHeapRegionEntryHelper.decodeUncompressedAddressToBytes(getAddress());
  }

  @Override
  public byte[] getSerializedValue() {
    byte[] value = getRawBytes();
    if (!isSerialized()) {
      value = EntryEventImpl.serialize(value);
    }
    return value;
  }

  @Override
  public Object getDeserializedValue(Region r, RegionEntry re) {
    return OffHeapRegionEntryHelper.decodeAddressToObject(address);
  }

  @Override
  public void fillSerializedValue(BytesAndBitsForCompactor wrapper, byte userBits) {
    byte[] value;
    if (isSerialized()) {
      value = getSerializedValue();
      userBits = EntryBits.setSerialized(userBits, true);
    } else {
      value = (byte[]) getDeserializedForReading();
    }
    wrapper.setData(value, userBits, value.length, true);
  }

  @Override
  public int getValueSizeInBytes() {
    return 0;
  }

  @Override
  public boolean isSerialized() {
    return OffHeapRegionEntryHelper.isSerialized(address);
  }

  @Override
  public boolean isCompressed() {
    return OffHeapRegionEntryHelper.isCompressed(address);
  }

  @Override
  public void release() {
    // nothing needed
  }

  @Override
  public boolean retain() {
    return true;
  }

  @Override
  public int getRefCount() {
    return -1;
  }

  @Override
  public int getSize() {
    return Long.BYTES;
  }

  @Override
  public int getDataSize() {
    return OffHeapRegionEntryHelper.decodeAddressToDataSize(address);
  }

  @Override
  public byte readDataByte(int offset) {
    return getRawBytes()[offset];
  }

  @Override
  public void writeDataByte(int offset, byte value) {
    throw new UnsupportedOperationException(
        "ObjectStoredAsAddress does not support modifying the data bytes");
  }

  @Override
  public void readDataBytes(int offset, byte[] bytes) {
    readDataBytes(offset, bytes, 0, bytes.length);
  }

  @Override
  public void writeDataBytes(int offset, byte[] bytes) {
    writeDataBytes(offset, bytes, 0, bytes.length);
  }

  @Override
  public void readDataBytes(int offset, byte[] bytes, int bytesOffset, int size) {
    byte[] src = getRawBytes();
    int dstIdx = bytesOffset;
    for (int i = offset; i < offset + size; i++) {
      bytes[dstIdx++] = src[i];
    }
  }

  @Override
  public void writeDataBytes(int offset, byte[] bytes, int bytesOffset, int size) {
    throw new UnsupportedOperationException(
        "ObjectStoredAsAddress does not support modifying the data bytes");
  }

  @Override
  public ByteBuffer createDirectByteBuffer() {
    return null;
  }

  @Override
  public boolean hasRefCount() {
    return false;
  }

  @Override
  public boolean checkDataEquals(StoredObject so) {
    return equals(so);
  }

  @Override
  public boolean checkDataEquals(byte[] serializedObj) {
    byte[] myBytes = getSerializedValue();
    return Arrays.equals(myBytes, serializedObj);
  }

  @Override
  public long getAddressForReadingData(int offset, int size) {
    throw new UnsupportedOperationException(
        "ObjectStoredAsAddress does not support reading at an address");
  }

  @Override
  public StoredObject slice(int offset, int limit) {
    throw new UnsupportedOperationException("ObjectStoredAsAddress does not support slice");
  }
}
