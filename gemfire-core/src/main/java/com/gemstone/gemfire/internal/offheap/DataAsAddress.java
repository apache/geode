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

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.BytesAndBitsForCompactor;
import com.gemstone.gemfire.internal.cache.EntryBits;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.RegionEntryContext;

/**
 * Used to represent offheap addresses whose
 * value encodes actual data instead a memory
 * location.
 * Instances of this class have a very short lifetime.
 */
public class DataAsAddress extends AbstractStoredObject {
  private final long address;
  
  public DataAsAddress(long addr) {
    this.address = addr;
  }
  
  public long getEncodedAddress() {
    return this.address;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof DataAsAddress) {
      return getEncodedAddress() == ((DataAsAddress) o).getEncodedAddress();
    }
    return false;
  }
  
  @Override
  public int hashCode() {
    long value = getEncodedAddress();
    return (int)(value ^ (value >>> 32));
  }

  @Override
  public int getSizeInBytes() {
    return 0;
  }

  public byte[] getDecompressedBytes(RegionEntryContext r) {
    return OffHeapRegionEntryHelper.encodedAddressToBytes(this.address, true, r);
  }

  /**
   * If we contain a byte[] return it.
   * Otherwise return the serialize bytes in us in a byte array.
   */
  public byte[] getRawBytes() {
    return OffHeapRegionEntryHelper.encodedAddressToRawBytes(this.address);
  }
  
  @Override
  public byte[] getSerializedValue() {
    return OffHeapRegionEntryHelper.encodedAddressToBytes(this.address);
  }

  @Override
  public Object getDeserializedValue(Region r, RegionEntry re) {
    return OffHeapRegionEntryHelper.encodedAddressToObject(this.address);
  }

  @Override
  public void fillSerializedValue(BytesAndBitsForCompactor wrapper,
      byte userBits) {
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
    return OffHeapRegionEntryHelper.isSerialized(this.address);
  }

  @Override
  public boolean isCompressed() {
    return OffHeapRegionEntryHelper.isCompressed(this.address);
  }
  
  @Override
  public boolean retain() {
    // nothing needed
    return true;
  }
  @Override
  public void release() {
    // nothing needed
  }
}