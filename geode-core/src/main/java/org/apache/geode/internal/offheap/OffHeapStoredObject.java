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

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.DSCODE;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.BytesAndBitsForCompactor;
import org.apache.geode.internal.cache.EntryBits;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.offheap.annotations.Unretained;

/**
 * A class that stores a Java object in off-heap memory. See {@link AddressableMemoryManager} for
 * how off-heap memory can be allocated, accessed, modified, and freed. Currently the object stored
 * in this class is always an entry value of a Region. Note: this class has a natural ordering that
 * is inconsistent with equals. Instances of this class should have a short lifetime. We do not
 * store references to it in the cache. Instead the memoryAddress is stored in a primitive field in
 * the cache and if used it will then, if needed, create an instance of this class.
 */
public class OffHeapStoredObject extends AbstractStoredObject
    implements Comparable<OffHeapStoredObject>, MemoryBlock {
  /**
   * The memory address of the first byte of addressable memory that belongs to this object
   */
  private final long memoryAddress;

  /**
   * The useCount, chunkSize, dataSizeDelta, isSerialized, and isCompressed are all stored in
   * addressable memory in a HEADER. This saves heap memory by using off heap.
   */
  public static final int HEADER_SIZE = 4 + 4;
  /**
   * We need to smallest chunk to at least have enough room for a hdr and for an off heap ref (which
   * is a long).
   */
  public static final int MIN_CHUNK_SIZE = HEADER_SIZE + 8;
  /**
   * int field. The number of bytes in this chunk.
   */
  private static final int CHUNK_SIZE_OFFSET = 0;
  /**
   * Volatile int field The upper two bits are used for the isSerialized and isCompressed flags. The
   * next three bits are unused. The lower 3 bits of the most significant byte contains a magic
   * number to help us detect if we are changing the ref count of an object that has been released.
   * The next byte contains the dataSizeDelta. The number of bytes of logical data in this chunk.
   * Since the number of bytes of logical data is always <= chunkSize and since chunkSize never
   * changes, we have dataSize be a delta whose max value would be HUGE_MULTIPLE-1. The lower two
   * bytes contains the use count.
   */
  static final int REF_COUNT_OFFSET = 4;
  /**
   * The upper two bits are used for the isSerialized and isCompressed flags.
   */
  static final int IS_SERIALIZED_BIT = 0x80000000;
  static final int IS_COMPRESSED_BIT = 0x40000000;
  // UNUSED 0x38000000
  static final int MAGIC_MASK = 0x07000000;
  static final int MAGIC_NUMBER = 0x05000000;
  static final int DATA_SIZE_DELTA_MASK = 0x00ff0000;
  static final int DATA_SIZE_SHIFT = 16;
  static final int REF_COUNT_MASK = 0x0000ffff;
  static final int MAX_REF_COUNT = 0xFFFF;
  static final long FILL_PATTERN = 0x3c3c3c3c3c3c3c3cL;
  static final byte FILL_BYTE = 0x3c;

  protected OffHeapStoredObject(long memoryAddress, int chunkSize) {
    MemoryAllocatorImpl.validateAddressAndSize(memoryAddress, chunkSize);
    this.memoryAddress = memoryAddress;
    setSize(chunkSize);
    AddressableMemoryManager.writeIntVolatile(getAddress() + REF_COUNT_OFFSET, MAGIC_NUMBER);
  }

  public void readyForFree() {
    AddressableMemoryManager.writeIntVolatile(getAddress() + REF_COUNT_OFFSET, 0);
  }

  public void readyForAllocation() {
    if (!AddressableMemoryManager.writeIntVolatile(getAddress() + REF_COUNT_OFFSET, 0,
        MAGIC_NUMBER)) {
      throw new IllegalStateException("Expected 0 but found " + Integer
          .toHexString(AddressableMemoryManager.readIntVolatile(getAddress() + REF_COUNT_OFFSET)));
    }
  }

  /**
   * Should only be used by FakeChunk subclass
   */
  protected OffHeapStoredObject() {
    this.memoryAddress = 0L;
  }

  /**
   * Used to create a Chunk given an existing, already allocated, memoryAddress. The off heap header
   * has already been initialized.
   */
  protected OffHeapStoredObject(long memoryAddress) {
    MemoryAllocatorImpl.validateAddress(memoryAddress);
    this.memoryAddress = memoryAddress;
  }

  protected OffHeapStoredObject(OffHeapStoredObject chunk) {
    this.memoryAddress = chunk.memoryAddress;
  }

  @Override
  public void fillSerializedValue(BytesAndBitsForCompactor wrapper, byte userBits) {
    if (isSerialized()) {
      userBits = EntryBits.setSerialized(userBits, true);
    }
    wrapper.setOffHeapData(this, userBits);
  }

  String getShortClassName() {
    String cname = getClass().getName();
    return cname.substring(getClass().getPackage().getName().length() + 1);
  }

  @Override
  public boolean checkDataEquals(@Unretained StoredObject so) {
    if (this == so) {
      return true;
    }
    if (isSerialized() != so.isSerialized()) {
      return false;
    }
    int mySize = getValueSizeInBytes();
    if (mySize != so.getValueSizeInBytes()) {
      return false;
    }
    if (!(so instanceof OffHeapStoredObject)) {
      return false;
    }
    OffHeapStoredObject other = (OffHeapStoredObject) so;
    if (getAddress() == other.getAddress()) {
      return true;
    }
    // We want to be able to do this operation without copying any of the data into the heap.
    // Hopefully the jvm is smart enough to use our stack for this short lived array.
    final byte[] dataCache1 = new byte[1024];
    final byte[] dataCache2 = new byte[dataCache1.length];
    int i;
    // inc it twice since we are reading two different objects
    MemoryAllocatorImpl.getAllocator().getStats().incReads();
    MemoryAllocatorImpl.getAllocator().getStats().incReads();
    for (i = 0; i < mySize - (dataCache1.length - 1); i += dataCache1.length) {
      this.readDataBytes(i, dataCache1);
      other.readDataBytes(i, dataCache2);
      for (int j = 0; j < dataCache1.length; j++) {
        if (dataCache1[j] != dataCache2[j]) {
          return false;
        }
      }
    }
    int bytesToRead = mySize - i;
    if (bytesToRead > 0) {
      // need to do one more read which will be less than dataCache.length
      this.readDataBytes(i, dataCache1, 0, bytesToRead);
      other.readDataBytes(i, dataCache2, 0, bytesToRead);
      for (int j = 0; j < bytesToRead; j++) {
        if (dataCache1[j] != dataCache2[j]) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public boolean checkDataEquals(byte[] serializedObj) {
    // caller was responsible for checking isSerialized
    int mySize = getValueSizeInBytes();
    if (mySize != serializedObj.length) {
      return false;
    }
    // We want to be able to do this operation without copying any of the data into the heap.
    // Hopefully the jvm is smart enough to use our stack for this short lived array.
    final byte[] dataCache = new byte[1024];
    int idx = 0;
    int i;
    MemoryAllocatorImpl.getAllocator().getStats().incReads();
    for (i = 0; i < mySize - (dataCache.length - 1); i += dataCache.length) {
      this.readDataBytes(i, dataCache);
      for (int j = 0; j < dataCache.length; j++) {
        if (dataCache[j] != serializedObj[idx++]) {
          return false;
        }
      }
    }
    int bytesToRead = mySize - i;
    if (bytesToRead > 0) {
      // need to do one more read which will be less than dataCache.length
      this.readDataBytes(i, dataCache, 0, bytesToRead);
      for (int j = 0; j < bytesToRead; j++) {
        if (dataCache[j] != serializedObj[idx++]) {
          return false;
        }
      }
    }
    return true;
  }


  /**
   * Throw an exception if this chunk is not allocated
   */
  public void checkIsAllocated() {
    int originalBits =
        AddressableMemoryManager.readIntVolatile(this.memoryAddress + REF_COUNT_OFFSET);
    if ((originalBits & MAGIC_MASK) != MAGIC_NUMBER) {
      throw new IllegalStateException(
          "It looks like this off heap memory was already freed. rawBits="
              + Integer.toHexString(originalBits));
    }
  }

  public void incSize(int inc) {
    setSize(getSize() + inc);
  }

  protected void beforeReturningToAllocator() {

  }

  @Override
  public int getSize() {
    return getSize(this.memoryAddress);
  }

  public void setSize(int size) {
    setSize(this.memoryAddress, size);
  }

  @Override
  public long getAddress() {
    return this.memoryAddress;
  }

  @Override
  public int getDataSize() {
    return getDataSize(this.memoryAddress);
  }

  protected static int getDataSize(long memoryAdress) {
    int dataSizeDelta = AddressableMemoryManager.readInt(memoryAdress + REF_COUNT_OFFSET);
    dataSizeDelta &= DATA_SIZE_DELTA_MASK;
    dataSizeDelta >>= DATA_SIZE_SHIFT;
    return getSize(memoryAdress) - dataSizeDelta;
  }

  protected long getBaseDataAddress() {
    return this.memoryAddress + HEADER_SIZE;
  }

  protected int getBaseDataOffset() {
    return 0;
  }

  @Override
  @Unretained
  public ByteBuffer createDirectByteBuffer() {
    return AddressableMemoryManager.createDirectByteBuffer(getBaseDataAddress(), getDataSize());
  }

  @Override
  public void sendTo(DataOutput out) throws IOException {
    if (!this.isCompressed() && out instanceof HeapDataOutputStream) {
      ByteBuffer bb = createDirectByteBuffer();
      if (bb != null) {
        HeapDataOutputStream hdos = (HeapDataOutputStream) out;
        if (this.isSerialized()) {
          hdos.write(bb);
        } else {
          hdos.writeByte(DSCODE.BYTE_ARRAY.toByte());
          InternalDataSerializer.writeArrayLength(bb.remaining(), hdos);
          hdos.write(bb);
        }
        return;
      }
    }
    super.sendTo(out);
  }

  @Override
  public void sendAsByteArray(DataOutput out) throws IOException {
    if (!isCompressed() && out instanceof HeapDataOutputStream) {
      ByteBuffer bb = createDirectByteBuffer();
      if (bb != null) {
        HeapDataOutputStream hdos = (HeapDataOutputStream) out;
        InternalDataSerializer.writeArrayLength(bb.remaining(), hdos);
        hdos.write(bb);
        return;
      }
    }
    super.sendAsByteArray(out);
  }

  /**
   * Returns an address that can be used with AddressableMemoryManager to access this object's data.
   *
   * @param offset the offset from this chunk's first byte of the byte the returned address should
   *        point to. Must be >= 0.
   * @param size the number of bytes that will be read using the returned address. Assertion will
   *        use this to verify that all the memory accessed belongs to this chunk. Must be > 0.
   * @return a memory address that can be used to access this object's data
   */
  @Override
  public long getAddressForReadingData(int offset, int size) {
    assert offset >= 0 && offset + size <= getDataSize() : "Offset=" + offset + ",size=" + size
        + ",dataSize=" + getDataSize() + ", chunkSize=" + getSize()
        + ", but offset + size must be <= " + getDataSize();
    assert size > 0;
    long result = getBaseDataAddress() + offset;
    // validateAddressAndSizeWithinSlab(result, size);
    return result;
  }

  @Override
  public byte readDataByte(int offset) {
    assert offset < getDataSize();
    return AddressableMemoryManager.readByte(getBaseDataAddress() + offset);
  }

  @Override
  public void writeDataByte(int offset, byte value) {
    assert offset < getDataSize();
    AddressableMemoryManager.writeByte(getBaseDataAddress() + offset, value);
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
    assert offset + size <= getDataSize();
    AddressableMemoryManager.readBytes(getBaseDataAddress() + offset, bytes, bytesOffset, size);
  }

  @Override
  public void writeDataBytes(int offset, byte[] bytes, int bytesOffset, int size) {
    assert offset + size <= getDataSize();
    AddressableMemoryManager.writeBytes(getBaseDataAddress() + offset, bytes, bytesOffset, size);
  }

  @Override
  public void release() {
    release(this.memoryAddress);
  }

  @Override
  public int compareTo(OffHeapStoredObject o) {
    int result = Integer.signum(getSize() - o.getSize());
    if (result == 0) {
      // For the same sized chunks we really don't care about their order
      // but we need compareTo to only return 0 if the two chunks are identical
      result = Long.signum(getAddress() - o.getAddress());
    }
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof OffHeapStoredObject) {
      return getAddress() == ((OffHeapStoredObject) o).getAddress();
    }
    return false;
  }

  @Override
  public int hashCode() {
    long value = this.getAddress();
    return (int) (value ^ (value >>> 32));
  }

  public void setSerializedValue(byte[] value) {
    writeDataBytes(0, value);
  }

  public byte[] getDecompressedBytes(RegionEntryContext context) {
    byte[] result = getCompressedBytes();
    long time = context.getCachePerfStats().startDecompression();
    result = context.getCompressor().decompress(result);
    context.getCachePerfStats().endDecompression(time);
    return result;
  }

  /**
   * Returns the raw possibly compressed bytes of this chunk
   */
  public byte[] getCompressedBytes() {
    byte[] result = new byte[getDataSize()];
    readDataBytes(0, result);
    // debugLog("reading", true);
    MemoryAllocatorImpl.getAllocator().getStats().incReads();
    return result;
  }

  /**
   * This method should only be called on uncompressed objects
   *
   * @return byte array of the StoredObject value.
   */
  protected byte[] getRawBytes() {
    assert !isCompressed();
    return getCompressedBytes();
  }

  @Override
  public byte[] getSerializedValue() {
    byte[] result = getRawBytes();
    if (!isSerialized()) {
      // The object is a byte[]. So we need to make it look like a serialized byte[] in our result
      result = EntryEventImpl.serialize(result);
    }
    return result;
  }

  @Override
  public Object getDeserializedValue(Region r, RegionEntry re) {
    if (isSerialized()) {
      return EntryEventImpl.deserialize(getRawBytes());
    } else {
      return getRawBytes();
    }
  }

  /**
   * We want this to include memory overhead so use getSize() instead of getDataSize().
   */
  @Override
  public int getSizeInBytes() {
    // Calling getSize includes the off heap header size.
    // We do not add anything to this since the size of the reference belongs to the region entry
    // size
    // not the size of this object.
    return getSize();
  }

  @Override
  public int getValueSizeInBytes() {
    return getDataSize();
  }

  @Override
  public boolean isSerialized() {
    return (AddressableMemoryManager.readInt(this.memoryAddress + REF_COUNT_OFFSET)
        & IS_SERIALIZED_BIT) != 0;
  }

  @Override
  public boolean isCompressed() {
    return (AddressableMemoryManager.readInt(this.memoryAddress + REF_COUNT_OFFSET)
        & IS_COMPRESSED_BIT) != 0;
  }

  @Override
  public boolean retain() {
    return retain(this.memoryAddress);
  }

  @Override
  public int getRefCount() {
    return getRefCount(this.memoryAddress);
  }

  public static int getSize(long memAddr) {
    MemoryAllocatorImpl.validateAddress(memAddr);
    return AddressableMemoryManager.readInt(memAddr + CHUNK_SIZE_OFFSET);
  }

  public static void setSize(long memAddr, int size) {
    MemoryAllocatorImpl.validateAddressAndSize(memAddr, size);
    AddressableMemoryManager.writeInt(memAddr + CHUNK_SIZE_OFFSET, size);
  }

  public static long getNext(long memAddr) {
    MemoryAllocatorImpl.validateAddress(memAddr);
    return AddressableMemoryManager.readLong(memAddr + HEADER_SIZE);
  }

  public static void setNext(long memAddr, long next) {
    MemoryAllocatorImpl.validateAddress(memAddr);
    AddressableMemoryManager.writeLong(memAddr + HEADER_SIZE, next);
  }

  /**
   * Fills the chunk with a repeated byte fill pattern.
   *
   * @param baseAddress the starting address for a {@link OffHeapStoredObject}.
   */
  public static void fill(long baseAddress) {
    long startAddress = baseAddress + MIN_CHUNK_SIZE;
    int size = getSize(baseAddress) - MIN_CHUNK_SIZE;

    AddressableMemoryManager.fill(startAddress, size, FILL_BYTE);
  }

  /**
   * Validates that the fill pattern for this chunk has not been disturbed. This method assumes the
   * TINY_MULTIPLE is 8 bytes.
   *
   * @throws IllegalStateException when the pattern has been violated.
   */
  public void validateFill() {
    assert FreeListManager.TINY_MULTIPLE == 8;

    long startAddress = getAddress() + MIN_CHUNK_SIZE;
    int size = getSize() - MIN_CHUNK_SIZE;

    for (int i = 0; i < size; i += FreeListManager.TINY_MULTIPLE) {
      if (AddressableMemoryManager.readLong(startAddress + i) != FILL_PATTERN) {
        throw new IllegalStateException(
            "Fill pattern violated for chunk " + getAddress() + " with size " + getSize());
      }
    }
  }

  public void setSerialized(boolean isSerialized) {
    if (isSerialized) {
      int bits;
      int originalBits;
      do {
        originalBits =
            AddressableMemoryManager.readIntVolatile(this.memoryAddress + REF_COUNT_OFFSET);
        if ((originalBits & MAGIC_MASK) != MAGIC_NUMBER) {
          throw new IllegalStateException(
              "It looks like this off heap memory was already freed. rawBits="
                  + Integer.toHexString(originalBits));
        }
        bits = originalBits | IS_SERIALIZED_BIT;
      } while (!AddressableMemoryManager.writeIntVolatile(this.memoryAddress + REF_COUNT_OFFSET,
          originalBits, bits));
    }
  }

  public void setCompressed(boolean isCompressed) {
    if (isCompressed) {
      int bits;
      int originalBits;
      do {
        originalBits =
            AddressableMemoryManager.readIntVolatile(this.memoryAddress + REF_COUNT_OFFSET);
        if ((originalBits & MAGIC_MASK) != MAGIC_NUMBER) {
          throw new IllegalStateException(
              "It looks like this off heap memory was already freed. rawBits="
                  + Integer.toHexString(originalBits));
        }
        bits = originalBits | IS_COMPRESSED_BIT;
      } while (!AddressableMemoryManager.writeIntVolatile(this.memoryAddress + REF_COUNT_OFFSET,
          originalBits, bits));
    }
  }

  public void setDataSize(int dataSize) {
    assert dataSize <= getSize();
    int delta = getSize() - dataSize;
    assert delta <= (DATA_SIZE_DELTA_MASK >> DATA_SIZE_SHIFT);
    delta <<= DATA_SIZE_SHIFT;
    int bits;
    int originalBits;
    do {
      originalBits =
          AddressableMemoryManager.readIntVolatile(this.memoryAddress + REF_COUNT_OFFSET);
      if ((originalBits & MAGIC_MASK) != MAGIC_NUMBER) {
        throw new IllegalStateException(
            "It looks like this off heap memory was already freed. rawBits="
                + Integer.toHexString(originalBits));
      }
      bits = originalBits;
      bits &= ~DATA_SIZE_DELTA_MASK; // clear the old dataSizeDelta bits
      bits |= delta; // set the dataSizeDelta bits to the new delta value
    } while (!AddressableMemoryManager.writeIntVolatile(this.memoryAddress + REF_COUNT_OFFSET,
        originalBits, bits));
  }

  public void initializeUseCount() {
    int rawBits;
    do {
      rawBits = AddressableMemoryManager.readIntVolatile(this.memoryAddress + REF_COUNT_OFFSET);
      if ((rawBits & MAGIC_MASK) != MAGIC_NUMBER) {
        throw new IllegalStateException(
            "It looks like this off heap memory was already freed. rawBits="
                + Integer.toHexString(rawBits));
      }
      int uc = rawBits & REF_COUNT_MASK;
      if (uc != 0) {
        throw new IllegalStateException("Expected use count to be zero but it was: " + uc
            + " rawBits=0x" + Integer.toHexString(rawBits));
      }
    } while (!AddressableMemoryManager.writeIntVolatile(this.memoryAddress + REF_COUNT_OFFSET,
        rawBits, rawBits + 1));
  }

  public static int getRefCount(long memAddr) {
    return AddressableMemoryManager.readInt(memAddr + REF_COUNT_OFFSET) & REF_COUNT_MASK;
  }

  public static boolean retain(long memAddr) {
    MemoryAllocatorImpl.validateAddress(memAddr);
    int uc;
    int rawBits;
    int retryCount = 0;
    do {
      rawBits = AddressableMemoryManager.readIntVolatile(memAddr + REF_COUNT_OFFSET);
      if ((rawBits & MAGIC_MASK) != MAGIC_NUMBER) {
        // same as uc == 0
        // TODO MAGIC_NUMBER rethink its use and interaction with compactor fragments
        return false;
      }
      uc = rawBits & REF_COUNT_MASK;
      if (uc == MAX_REF_COUNT) {
        throw new IllegalStateException(
            "Maximum use count exceeded. rawBits=" + Integer.toHexString(rawBits));
      } else if (uc == 0) {
        return false;
      }
      retryCount++;
      if (retryCount > 1000) {
        throw new IllegalStateException("tried to write " + (rawBits + 1) + " to @"
            + Long.toHexString(memAddr) + " 1,000 times.");
      }
    } while (!AddressableMemoryManager.writeIntVolatile(memAddr + REF_COUNT_OFFSET, rawBits,
        rawBits + 1));
    // debugLog("use inced ref count " + (uc+1) + " @" + Long.toHexString(memAddr), true);
    if (ReferenceCountHelper.trackReferenceCounts()) {
      ReferenceCountHelper.refCountChanged(memAddr, false, uc + 1);
    }

    return true;
  }

  public static void release(final long memAddr) {
    release(memAddr, null);
  }

  static void release(final long memAddr, FreeListManager freeListManager) {
    MemoryAllocatorImpl.validateAddress(memAddr);
    int newCount;
    int rawBits;
    boolean returnToAllocator;
    do {
      returnToAllocator = false;
      rawBits = AddressableMemoryManager.readIntVolatile(memAddr + REF_COUNT_OFFSET);
      if ((rawBits & MAGIC_MASK) != MAGIC_NUMBER) {
        String msg = "It looks like off heap memory @" + Long.toHexString(memAddr)
            + " was already freed. rawBits=" + Integer.toHexString(rawBits) + " history="
            + ReferenceCountHelper.getFreeRefCountInfo(memAddr);
        // debugLog(msg, true);
        throw new IllegalStateException(msg);
      }
      int curCount = rawBits & REF_COUNT_MASK;
      if ((curCount) == 0) {
        // debugLog("too many frees @" + Long.toHexString(memAddr), true);
        throw new IllegalStateException(
            "Memory has already been freed." + " history=" + ReferenceCountHelper
                .getFreeRefCountInfo(memAddr) /* + System.identityHashCode(this) */);
      }
      if (curCount == 1) {
        newCount = 0; // clear the use count, bits, and the delta size since it will be freed.
        returnToAllocator = true;
      } else {
        newCount = rawBits - 1;
      }
    } while (!AddressableMemoryManager.writeIntVolatile(memAddr + REF_COUNT_OFFSET, rawBits,
        newCount));
    // debugLog("free deced ref count " + (newCount&USE_COUNT_MASK) + " @" +
    // Long.toHexString(memAddr), true);
    if (returnToAllocator) {
      if (ReferenceCountHelper.trackReferenceCounts()) {
        if (ReferenceCountHelper.trackFreedReferenceCounts()) {
          ReferenceCountHelper.refCountChanged(memAddr, true, newCount & REF_COUNT_MASK);
        }
        ReferenceCountHelper.freeRefCountInfo(memAddr);
      }
      if (freeListManager == null) {
        freeListManager = MemoryAllocatorImpl.getAllocator().getFreeListManager();
      }
      freeListManager.free(memAddr);
    } else {
      if (ReferenceCountHelper.trackReferenceCounts()) {
        ReferenceCountHelper.refCountChanged(memAddr, true, newCount & REF_COUNT_MASK);
      }
    }
  }

  @Override
  public String toString() {
    return super.toString() + ":<dataSize=" + getDataSize() + " refCount=" + getRefCount()
        + " addr=" + Long.toHexString(getAddress()) + ">";
  }

  @Override
  public State getState() {
    if (getRefCount() > 0) {
      return State.ALLOCATED;
    } else {
      return State.DEALLOCATED;
    }
  }

  @Override
  public MemoryBlock getNextBlock() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getBlockSize() {
    return getSize();
  }

  @Override
  public int getSlabId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getFreeListId() {
    return -1;
  }

  @Override
  public String getDataType() {
    return null;
  }

  @Override
  public Object getDataValue() {
    return null;
  }

  public StoredObject slice(int position, int limit) {
    return new OffHeapStoredObjectSlice(this, position, limit);
  }

  @Override
  public boolean hasRefCount() {
    return true;
  }
}
