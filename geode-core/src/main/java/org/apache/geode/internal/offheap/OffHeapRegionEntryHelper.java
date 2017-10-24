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

import org.apache.geode.internal.DSCODE;
import org.apache.geode.internal.cache.CachedDeserializableFactory;
import org.apache.geode.internal.cache.entries.DiskEntry;
import org.apache.geode.internal.cache.DiskId;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.entries.OffHeapRegionEntry;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.Token;
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

  protected static final long NULL_ADDRESS = 0L << 1;
  protected static final long INVALID_ADDRESS = 1L << 1;
  protected static final long LOCAL_INVALID_ADDRESS = 2L << 1;
  protected static final long DESTROYED_ADDRESS = 3L << 1;
  protected static final long REMOVED_PHASE1_ADDRESS = 4L << 1;
  protected static final long REMOVED_PHASE2_ADDRESS = 5L << 1;
  protected static final long END_OF_STREAM_ADDRESS = 6L << 1;
  protected static final long NOT_AVAILABLE_ADDRESS = 7L << 1;
  protected static final long TOMBSTONE_ADDRESS = 8L << 1;
  public static final int MAX_LENGTH_FOR_DATA_AS_ADDRESS = 8;

  private static final Token[] addrToObj =
      new Token[] {null, Token.INVALID, Token.LOCAL_INVALID, Token.DESTROYED, Token.REMOVED_PHASE1,
          Token.REMOVED_PHASE2, Token.END_OF_STREAM, Token.NOT_AVAILABLE, Token.TOMBSTONE,};

  private static long objectToAddress(@Unretained Object v) {
    if (v instanceof StoredObject)
      return ((StoredObject) v).getAddress();
    if (v == null)
      return NULL_ADDRESS;
    if (v == Token.TOMBSTONE)
      return TOMBSTONE_ADDRESS;
    if (v == Token.INVALID)
      return INVALID_ADDRESS;
    if (v == Token.LOCAL_INVALID)
      return LOCAL_INVALID_ADDRESS;
    if (v == Token.DESTROYED)
      return DESTROYED_ADDRESS;
    if (v == Token.REMOVED_PHASE1)
      return REMOVED_PHASE1_ADDRESS;
    if (v == Token.REMOVED_PHASE2)
      return REMOVED_PHASE2_ADDRESS;
    if (v == Token.END_OF_STREAM)
      return END_OF_STREAM_ADDRESS;
    if (v == Token.NOT_AVAILABLE)
      return NOT_AVAILABLE_ADDRESS;
    throw new IllegalStateException("Can not convert " + v + " to an off heap address.");
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
    if (isOffHeap(ohAddress)) {
      @Unretained
      OffHeapStoredObject chunk = new OffHeapStoredObject(ohAddress);
      @Unretained
      Object result = chunk;
      if (decompress && chunk.isCompressed()) {
        try {
          // to fix bug 47982 need to:
          byte[] decompressedBytes = chunk.getDecompressedBytes(context);
          if (chunk.isSerialized()) {
            // return a VMCachedDeserializable with the decompressed serialized bytes since chunk is
            // serialized
            result = CachedDeserializableFactory.create(decompressedBytes);
          } else {
            // return a byte[] since chunk is not serialized
            result = decompressedBytes;
          }
        } finally {
          // decompress is only true when this method is called by _getValueRetain.
          // In that case the caller has already retained ohAddress because it thought
          // we would return it. But we have unwrapped it and are returning the decompressed
          // results.
          // So we need to release the chunk here.
          chunk.release();
        }
      }
      return result;
    } else if ((ohAddress & ENCODED_BIT) != 0) {
      TinyStoredObject daa = new TinyStoredObject(ohAddress);
      Object result = daa;
      if (decompress && daa.isCompressed()) {
        byte[] decompressedBytes = daa.getDecompressedBytes(context);
        if (daa.isSerialized()) {
          // return a VMCachedDeserializable with the decompressed serialized bytes since daa is
          // serialized
          result = CachedDeserializableFactory.create(decompressedBytes);
        } else {
          // return a byte[] since daa is not serialized
          result = decompressedBytes;
        }
      }
      return result;
    } else {
      return addrToObj[(int) ohAddress >> 1];
    }
  }

  public static int getSerializedLength(TinyStoredObject dataAsAddress) {
    final long ohAddress = dataAsAddress.getAddress();

    if ((ohAddress & ENCODED_BIT) != 0) {
      boolean isLong = (ohAddress & LONG_BIT) != 0;
      if (isLong) {
        return 9;
      } else {
        return (int) ((ohAddress & SIZE_MASK) >> SIZE_SHIFT);
      }
    } else {
      return 0;
    }
  }

  /*
   * This method is optimized for cases where if the caller wants to convert address to a Token
   * compared to addressToObject which would deserialize the value.
   */
  private static Token addressToToken(long ohAddress) {
    if (isOffHeap(ohAddress) || (ohAddress & ENCODED_BIT) != 0) {
      return Token.NOT_A_TOKEN;
    } else {
      return addrToObj[(int) ohAddress >> 1];
    }
  }

  private static void releaseAddress(@Released long ohAddress) {
    if (isOffHeap(ohAddress)) {
      OffHeapStoredObject.release(ohAddress);
    }
  }

  /**
   * The address in 're' will be @Released.
   */
  public static void releaseEntry(@Released OffHeapRegionEntry re) {
    if (re instanceof DiskEntry) {
      DiskId did = ((DiskEntry) re).getDiskId();
      if (did != null && did.isPendingAsync()) {
        synchronized (did) {
          // This may not be needed so remove this call if it causes problems.
          // We no longer need this entry to be written to disk so unschedule it
          // before we change its value to REMOVED_PHASE2.
          did.setPendingAsync(false);
          setValue(re, Token.REMOVED_PHASE2);
          return;
        }
      }
    }
    setValue(re, Token.REMOVED_PHASE2);
  }

  public static void releaseEntry(@Unretained OffHeapRegionEntry re,
      @Released StoredObject expectedValue) {
    long oldAddress = objectToAddress(expectedValue);
    final long newAddress = objectToAddress(Token.REMOVED_PHASE2);
    if (re.setAddress(oldAddress, newAddress) || re.getAddress() != newAddress) {
      releaseAddress(oldAddress);
    } /*
       * else { if (!calledSetValue || re.getAddress() != newAddress) { expectedValue.release(); } }
       */
  }

  /**
   * This bit is set to indicate that this address has data encoded in it.
   */
  private static long ENCODED_BIT = 1L;
  /**
   * This bit is set to indicate that the encoded data is serialized.
   */
  static long SERIALIZED_BIT = 2L;
  /**
   * This bit is set to indicate that the encoded data is compressed.
   */
  static long COMPRESSED_BIT = 4L;
  /**
   * This bit is set to indicate that the encoded data is a long whose value fits in 7 bytes.
   */
  private static long LONG_BIT = 8L;
  /**
   * size is in the range 0..7 so we only need 3 bits.
   */
  private static long SIZE_MASK = 0x70L;
  /**
   * number of bits to shift the size by.
   */
  private static int SIZE_SHIFT = 4;
  // the msb of this byte is currently unused

  /**
   * Returns 0 if the data could not be encoded as an address.
   */
  public static long encodeDataAsAddress(byte[] v, boolean isSerialized, boolean isCompressed) {
    if (v.length < MAX_LENGTH_FOR_DATA_AS_ADDRESS) {
      long result = 0L;
      for (int i = 0; i < v.length; i++) {
        result |= v[i] & 0x00ff;
        result <<= 8;
      }
      result |= (v.length << SIZE_SHIFT) | ENCODED_BIT;
      if (isSerialized) {
        result |= SERIALIZED_BIT;
      }
      if (isCompressed) {
        result |= COMPRESSED_BIT;
      }
      return result;
    } else if (isSerialized && !isCompressed) {
      // Check for some special types that take more than 7 bytes to serialize
      // but that might be able to be inlined with less than 8 bytes.
      if (v[0] == DSCODE.LONG) {
        // A long is currently always serialized as 8 bytes (9 if you include the dscode).
        // But many long values will actually be small enough for is to encode in 7 bytes.
        if ((v[1] == 0 && (v[2] & 0x80) == 0) || (v[1] == -1 && (v[2] & 0x80) != 0)) {
          // The long can be encoded as 7 bytes since the most signification byte
          // is simply an extension of the sign byte on the second most signification byte.
          long result = 0L;
          for (int i = 2; i < v.length; i++) {
            result |= v[i] & 0x00ff;
            result <<= 8;
          }
          result |= (7 << SIZE_SHIFT) | LONG_BIT | SERIALIZED_BIT | ENCODED_BIT;
          return result;
        }
      }
    }
    return 0L;
  }

  static Object decodeAddressToObject(long ohAddress) {
    byte[] bytes = decodeUncompressedAddressToBytes(ohAddress);

    boolean isSerialized = (ohAddress & SERIALIZED_BIT) != 0;
    if (isSerialized) {
      return EntryEventImpl.deserialize(bytes);
    } else {
      return bytes;
    }
  }

  static int decodeAddressToDataSize(long addr) {
    assert (addr & ENCODED_BIT) != 0;
    boolean isLong = (addr & LONG_BIT) != 0;
    if (isLong) {
      return 9;
    }
    return (int) ((addr & SIZE_MASK) >> SIZE_SHIFT);
  }

  /**
   * Returns the bytes encoded in the given address. Note that compressed addresses are not
   * supported by this method.
   * 
   * @throws UnsupportedOperationException if the address has compressed data
   */
  static byte[] decodeUncompressedAddressToBytes(long addr) {
    assert (addr & COMPRESSED_BIT) == 0 : "Did not expect encoded address to be compressed";
    return decodeAddressToRawBytes(addr);
  }

  /**
   * Returns the "raw" bytes that have been encoded in the given address. Note that if address is
   * compressed then the raw bytes are the compressed bytes.
   */
  static byte[] decodeAddressToRawBytes(long addr) {
    assert (addr & ENCODED_BIT) != 0;
    int size = (int) ((addr & SIZE_MASK) >> SIZE_SHIFT);
    boolean isLong = (addr & LONG_BIT) != 0;
    byte[] bytes;
    if (isLong) {
      bytes = new byte[9];
      bytes[0] = DSCODE.LONG;
      for (int i = 8; i >= 2; i--) {
        addr >>= 8;
        bytes[i] = (byte) (addr & 0x00ff);
      }
      if ((bytes[2] & 0x80) != 0) {
        bytes[1] = -1;
      } else {
        bytes[1] = 0;
      }
    } else {
      bytes = new byte[size];
      for (int i = size - 1; i >= 0; i--) {
        addr >>= 8;
        bytes[i] = (byte) (addr & 0x00ff);
      }
    }
    return bytes;
  }

  /**
   * The previous value at the address in 're' will be @Released and then the address in 're' will
   * be set to the @Unretained address of 'v'.
   */
  public static void setValue(@Released OffHeapRegionEntry re, @Unretained Object v) {
    // setValue is called when synced so I don't need to worry
    // about oldAddress being released by someone else.
    final long newAddress = objectToAddress(v);
    long oldAddress;
    do {
      oldAddress = re.getAddress();
    } while (!re.setAddress(oldAddress, newAddress));
    ReferenceCountHelper.setReferenceCountOwner(re);
    releaseAddress(oldAddress);
    ReferenceCountHelper.setReferenceCountOwner(null);
  }

  public static Token getValueAsToken(@Unretained OffHeapRegionEntry re) {
    return addressToToken(re.getAddress());
  }

  @Unretained
  public static Object _getValue(@Unretained OffHeapRegionEntry re) {
    return addressToObject(re.getAddress(), false, null); // no context needed so decompress is
                                                          // false
  }

  public static boolean isOffHeap(long addr) {
    if ((addr & ENCODED_BIT) != 0)
      return false;
    if (addr < 0)
      return true;
    addr >>= 1; // shift right 1 to convert to array index;
    return addr >= addrToObj.length;
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
    int retryCount = 0;
    @Retained
    long addr = re.getAddress();
    while (isOffHeap(addr)) {
      if (OffHeapStoredObject.retain(addr)) {
        @Unretained
        long addr2 = re.getAddress();
        if (addr != addr2) {
          retryCount = 0;
          OffHeapStoredObject.release(addr);
          // spin around and try again.
          addr = addr2;
        } else {
          return addressToObject(addr, decompress, context);
        }
      } else {
        // spin around and try again
        long addr2 = re.getAddress();
        retryCount++;
        if (retryCount > 100) {
          throw new IllegalStateException("retain failed addr=" + addr + " addr2=" + addr
              + " 100 times" + " history=" + ReferenceCountHelper.getFreeRefCountInfo(addr));
        }
        addr = addr2;
        // Since retain returned false our region entry should have a different
        // value in it. However the actual address could be the exact same one
        // because addr was released, then reallocated from the free list and set
        // back into this region entry. See bug 47782
      }
    }
    return addressToObject(addr, decompress, context);
  }



  public static boolean isSerialized(long address) {
    return (address & SERIALIZED_BIT) != 0;
  }

  public static boolean isCompressed(long address) {
    return (address & COMPRESSED_BIT) != 0;
  }

  private static final ThreadLocal<Object> clearNeedsToCheckForOffHeap = new ThreadLocal<Object>();

  public static boolean doesClearNeedToCheckForOffHeap() {
    return clearNeedsToCheckForOffHeap.get() != null;
  }

  public static void doWithOffHeapClear(Runnable r) {
    clearNeedsToCheckForOffHeap.set(Boolean.TRUE);
    try {
      r.run();
    } finally {
      clearNeedsToCheckForOffHeap.remove();
    }
  }
}
