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
package org.apache.geode.internal.cache;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.offheap.annotations.Unretained;

/**
 * Used to fetch a record's raw bytes and user bits. The actual data length in byte array may be
 * less than the size of the byte array itself. An integer field contains the valid length. This
 * class is used exclusively by the Oplog Compactor for rolling the entries. The reason for this
 * class is to reuse the underlying byte array for rolling multiple entries there by reducing the
 * garbage.
 *
 * @since GemFire 5.5
 */
public class BytesAndBitsForCompactor {
  /**
   * If offHeapData is set then ignore the "data" and "validLength" fields. The offHeapData field is
   * unretained so it can only be used while the RegionEntry is still synced. When done with the
   * offHeapData, null it out if you want to reuse the byte[] later.
   */
  private @Unretained StoredObject offHeapData;
  private byte[] data;
  private byte userBits = 0;
  // length of the data present in the byte array
  private int validLength;
  @Immutable
  private static final byte[] INIT_FOR_WRAPPER = new byte[0];
  // boolean indicating if the object can be reused.
  // Typically if the data stores the reference of a value byte [] directly
  // from the RegionEntry than this byte array cannot be reused for
  // storing another entry's data
  private boolean isReusable;

  public BytesAndBitsForCompactor() {
    data = INIT_FOR_WRAPPER;
    // this.userBits = userBits;
    validLength = INIT_FOR_WRAPPER.length;
    isReusable = true;
  }


  public StoredObject getOffHeapData() {
    return offHeapData;
  }

  public byte[] getBytes() {
    return data;
  }

  public byte getBits() {
    return userBits;
  }

  public int getValidLength() {
    return validLength;
  }

  public boolean isReusable() {
    return isReusable;
  }

  /**
   *
   * @param data byte array storing the data
   * @param userBits byte with appropriate bits set
   * @param validLength The number of bytes representing the data , starting from 0 as offset
   * @param isReusable true if this object is safe for reuse as a data holder
   */
  public void setData(byte[] data, byte userBits, int validLength, boolean isReusable) {
    this.data = data;
    this.userBits = userBits;
    this.validLength = validLength;
    this.isReusable = isReusable;
  }

  public void setOffHeapData(StoredObject so, byte userBits) {
    offHeapData = so;
    this.userBits = userBits;
  }
}
