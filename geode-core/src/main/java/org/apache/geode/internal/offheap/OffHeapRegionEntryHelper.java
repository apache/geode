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

import org.apache.geode.annotations.Immutable;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.entries.OffHeapRegionEntry;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;

/**
 * The class just has static methods that operate on instances of {@link OffHeapRegionEntry}. It
 * allows common code to be shared for all the classes we have that implement
 * {@link OffHeapRegionEntry}.
 *
 * @since Geode 1.0
 */
public class OffHeapRegionEntryHelper {

  @Immutable
  private static final OffHeapRegionEntryHelper INSTANCE = new OffHeapRegionEntryHelper(
      new OffHeapRegionEntryHelperInstance());

  private final OffHeapRegionEntryHelperInstance delegate;

  private OffHeapRegionEntryHelper(OffHeapRegionEntryHelperInstance delegate) {
    this.delegate = delegate;
  }

  private static OffHeapRegionEntryHelperInstance delegate() {
    return INSTANCE.delegate;
  }

  /**
   * This method may release the object stored at ohAddress if the result needs to be decompressed
   * and the decompress parameter is true. This decompressed result will be on the heap.
   *
   * @param ohAddress OFF_HEAP_ADDRESS
   * @param decompress true if off-heap value should be decompressed before returning
   * @param context used for decompression
   * @return OFF_HEAP_OBJECT (sometimes)
   */
  @Unretained
  @Retained
  public static Object addressToObject(@Released @Retained long ohAddress, boolean decompress,
      RegionEntryContext context) {
    return delegate().addressToObject(ohAddress, decompress, context);
  }

  public static int getSerializedLength(TinyStoredObject dataAsAddress) {
    return delegate().getSerializedLength(dataAsAddress);
  }

  /**
   * The address in 're' will be @Released.
   */
  public static void releaseEntry(@Released OffHeapRegionEntry re) {
    delegate().releaseEntry(re);
  }

  public static void releaseEntry(@Unretained OffHeapRegionEntry re,
      @Released StoredObject expectedValue) {
    delegate().releaseEntry(re, expectedValue);
  }

  /**
   * Returns 0 if the data could not be encoded as an address.
   */
  static long encodeDataAsAddress(byte[] v, boolean isSerialized, boolean isCompressed) {
    return delegate().encodeDataAsAddress(v, isSerialized, isCompressed);
  }

  static Object decodeAddressToObject(long ohAddress) {
    return delegate().decodeAddressToObject(ohAddress);
  }

  static int decodeAddressToDataSize(long addr) {
    return delegate().decodeAddressToDataSize(addr);
  }

  /**
   * Returns the bytes encoded in the given address. Note that compressed addresses are not
   * supported by this method.
   *
   * @throws AssertionError if the address has compressed data
   */
  static byte[] decodeUncompressedAddressToBytes(long addr) {
    return delegate().decodeUncompressedAddressToBytes(addr);
  }

  /**
   * Returns the "raw" bytes that have been encoded in the given address. Note that if address is
   * compressed then the raw bytes are the compressed bytes.
   */
  static byte[] decodeAddressToRawBytes(long addr) {
    return delegate().decodeAddressToRawBytes(addr);
  }

  /**
   * The previous value at the address in 're' will be @Released and then the address in 're' will
   * be set to the @Unretained address of 'v'.
   */
  public static void setValue(@Released OffHeapRegionEntry re, @Unretained Object v) {
    delegate().setValue(re, v);
  }

  public static Token getValueAsToken(@Unretained OffHeapRegionEntry re) {
    return delegate().getValueAsToken(re);
  }

  @Unretained
  public static Object _getValue(@Unretained OffHeapRegionEntry re) {
    return delegate()._getValue(re);
  }

  public static boolean isOffHeap(long addr) {
    return delegate().isOffHeap(addr);
  }

  /**
   * If the value stored at the location held in 're' is returned, then it will be Retained. If the
   * value returned is 're' decompressed into another off-heap location, then 're' will be
   * Unretained but the new, decompressed value will be Retained. Therefore, whichever is returned
   * (the value at the address in 're' or the decompressed value) it will have been Retained.
   *
   * @return possible OFF_HEAP_OBJECT (caller must release)
   */
  @Retained
  public static Object _getValueRetain(@Retained @Unretained OffHeapRegionEntry re,
      boolean decompress, RegionEntryContext context) {
    return delegate()._getValueRetain(re, decompress, context);
  }

  public static boolean isSerialized(long address) {
    return delegate().isSerialized(address);
  }

  public static boolean isCompressed(long address) {
    return delegate().isCompressed(address);
  }
}
