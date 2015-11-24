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

import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.RegionEntryContext;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;

/**
   * Note: this class has a natural ordering that is inconsistent with equals.
   * Instances of this class should have a short lifetime. We do not store references
   * to it in the cache. Instead the memoryAddress is stored in a primitive field in
   * the cache and if used it will then, if needed, create an instance of this class.
   */
  public abstract class Chunk extends OffHeapCachedDeserializable implements Comparable<Chunk>, MemoryBlock {
    /**
     * The unsafe memory address of the first byte of this chunk
     */
    private final long memoryAddress;
    
    /**
     * The useCount, chunkSize, dataSizeDelta, isSerialized, and isCompressed
     * are all stored in off heap memory in a HEADER. This saves heap memory
     * by using off heap.
     */
    public final static int OFF_HEAP_HEADER_SIZE = 4 + 4;
    /**
     * We need to smallest chunk to at least have enough room for a hdr
     * and for an off heap ref (which is a long).
     */
    public final static int MIN_CHUNK_SIZE = OFF_HEAP_HEADER_SIZE + 8;
    /**
     * int field.
     * The number of bytes in this chunk.
     */
    private final static int CHUNK_SIZE_OFFSET = 0;
    /**
     * Volatile int field
     * The upper two bits are used for the isSerialized
     * and isCompressed flags.
     * The next three bits are used to encode the SRC_TYPE enum.
     * The lower 3 bits of the most significant byte contains a magic number to help us detect
     * if we are changing the ref count of an object that has been released.
     * The next byte contains the dataSizeDelta.
     * The number of bytes of logical data in this chunk.
     * Since the number of bytes of logical data is always <= chunkSize
     * and since chunkSize never changes, we have dataSize be
     * a delta whose max value would be HUGE_MULTIPLE-1.
     * The lower two bytes contains the use count.
     */
    private final static int REF_COUNT_OFFSET = 4;
    /**
     * The upper two bits are used for the isSerialized
     * and isCompressed flags.
     */
    private final static int IS_SERIALIZED_BIT =    0x80000000;
    private final static int IS_COMPRESSED_BIT =    0x40000000;
    private final static int SRC_TYPE_MASK = 0x38000000;
    private final static int SRC_TYPE_SHIFT = 16/*refCount*/+8/*dataSize*/+3/*magicSize*/;
    private final static int MAGIC_MASK = 0x07000000;
    private final static int MAGIC_NUMBER = 0x05000000;
    private final static int DATA_SIZE_DELTA_MASK = 0x00ff0000;
    private final static int DATA_SIZE_SHIFT = 16;
    private final static int REF_COUNT_MASK =       0x0000ffff;
    private final static int MAX_REF_COUNT = 0xFFFF;
    final static long FILL_PATTERN = 0x3c3c3c3c3c3c3c3cL;
    final static byte FILL_BYTE = 0x3c;
    
    // The 8 bits reserved for SRC_TYPE are basically no longer used.
    // So we could free up these 8 bits for some other use or we could
    // keep them for future extensions.
    // If we ever want to allocate other "types" into a chunk of off-heap
    // memory then the SRC_TYPE would be the way to go.
    // For example we may want to allocate the memory for the off-heap
    // RegionEntry in off-heap memory without it being of type GFE.
    // When it is of type GFE then it either needs to be the bytes
    // of a byte array or it needs to be a serialized java object.
    // For the RegionEntry we may want all the primitive fields of
    // the entry at certain offsets in the off-heap memory so we could
    // access them directly in native byte format (i.e. no serialization).
    // Note that for every SRC_TYPE we should have a ChunkType subclass.
    public final static int SRC_TYPE_UNUSED0 = 0 << SRC_TYPE_SHIFT;
    public final static int SRC_TYPE_UNUSED1 = 1 << SRC_TYPE_SHIFT;
    public final static int SRC_TYPE_UNUSED2 = 2 << SRC_TYPE_SHIFT;
    public final static int SRC_TYPE_UNUSED3 = 3 << SRC_TYPE_SHIFT;
    public final static int SRC_TYPE_GFE = 4 << SRC_TYPE_SHIFT;
    public final static int SRC_TYPE_UNUSED5 = 5 << SRC_TYPE_SHIFT;
    public final static int SRC_TYPE_UNUSED6 = 6 << SRC_TYPE_SHIFT;
    public final static int SRC_TYPE_UNUSED7 = 7 << SRC_TYPE_SHIFT;
    
    protected Chunk(long memoryAddress, int chunkSize, ChunkType chunkType) {
      SimpleMemoryAllocatorImpl.validateAddressAndSize(memoryAddress, chunkSize);
      this.memoryAddress = memoryAddress;
      setSize(chunkSize);
      UnsafeMemoryChunk.writeAbsoluteIntVolatile(getMemoryAddress()+REF_COUNT_OFFSET, MAGIC_NUMBER|chunkType.getSrcType());
    }
    public void readyForFree() {
      UnsafeMemoryChunk.writeAbsoluteIntVolatile(getMemoryAddress()+REF_COUNT_OFFSET, 0);
    }
    public void readyForAllocation(ChunkType chunkType) {
      if (!UnsafeMemoryChunk.writeAbsoluteIntVolatile(getMemoryAddress()+REF_COUNT_OFFSET, 0, MAGIC_NUMBER|chunkType.getSrcType())) {
        throw new IllegalStateException("Expected 0 but found " + Integer.toHexString(UnsafeMemoryChunk.readAbsoluteIntVolatile(getMemoryAddress()+REF_COUNT_OFFSET)));
      }
    }
    /**
     * Should only be used by FakeChunk subclass
     */
    protected Chunk() {
      this.memoryAddress = 0L;
    }
    
    /**
     * Used to create a Chunk given an existing, already allocated,
     * memoryAddress. The off heap header has already been initialized.
     */
    protected Chunk(long memoryAddress) {
      SimpleMemoryAllocatorImpl.validateAddress(memoryAddress);
      this.memoryAddress = memoryAddress;
    }
    
    protected Chunk(Chunk chunk) {
      this.memoryAddress = chunk.memoryAddress;
    }
    
    /**
     * Throw an exception if this chunk is not allocated
     */
    public void checkIsAllocated() {
      int originalBits = UnsafeMemoryChunk.readAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET);
      if ((originalBits&MAGIC_MASK) != MAGIC_NUMBER) {
        throw new IllegalStateException("It looks like this off heap memory was already freed. rawBits=" + Integer.toHexString(originalBits));
      }
    }
    
    public void incSize(int inc) {
      setSize(getSize()+inc);
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

    public long getMemoryAddress() {
      return this.memoryAddress;
    }
    
    public int getDataSize() {
      /*int dataSizeDelta = UnsafeMemoryChunk.readAbsoluteInt(this.memoryAddress+REF_COUNT_OFFSET);
      dataSizeDelta &= DATA_SIZE_DELTA_MASK;
      dataSizeDelta >>= DATA_SIZE_SHIFT;
      return getSize() - dataSizeDelta;*/
      return getDataSize(this.memoryAddress);
    }
    
    protected static int getDataSize(long memoryAdress) {
      int dataSizeDelta = UnsafeMemoryChunk.readAbsoluteInt(memoryAdress+REF_COUNT_OFFSET);
      dataSizeDelta &= DATA_SIZE_DELTA_MASK;
      dataSizeDelta >>= DATA_SIZE_SHIFT;
      return getSize(memoryAdress) - dataSizeDelta;
    }
    
    protected long getBaseDataAddress() {
      return this.memoryAddress+OFF_HEAP_HEADER_SIZE;
    }
    protected int getBaseDataOffset() {
      return 0;
    }
    
    /**
     * Creates and returns a direct ByteBuffer that contains the contents of this Chunk.
     * Note that the returned ByteBuffer has a reference to this chunk's
     * off-heap address so it can only be used while this Chunk is retained.
     * @return the created direct byte buffer or null if it could not be created.
     */
    @Unretained
    public ByteBuffer createDirectByteBuffer() {
      return basicCreateDirectByteBuffer(getBaseDataAddress(), getDataSize());
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
            hdos.writeByte(DSCODE.BYTE_ARRAY);
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
       
    private static volatile Class dbbClass = null;
    private static volatile Constructor dbbCtor = null;
    private static volatile boolean dbbCreateFailed = false;
    
    /**
     * @return the created direct byte buffer or null if it could not be created.
     */
    private static ByteBuffer basicCreateDirectByteBuffer(long baseDataAddress, int dataSize) {
      if (dbbCreateFailed) {
        return null;
      }
      Constructor ctor = dbbCtor;
      if (ctor == null) {
        Class c = dbbClass;
        if (c == null) {
          try {
            c = Class.forName("java.nio.DirectByteBuffer");
          } catch (ClassNotFoundException e) {
            //throw new IllegalStateException("Could not find java.nio.DirectByteBuffer", e);
            dbbCreateFailed = true;
            dbbAddressFailed = true;
            return null;
          }
          dbbClass = c;
        }
        try {
          ctor = c.getDeclaredConstructor(long.class, int.class);
        } catch (NoSuchMethodException | SecurityException e) {
          //throw new IllegalStateException("Could not get constructor DirectByteBuffer(long, int)", e);
          dbbClass = null;
          dbbCreateFailed = true;
          return null;
        }
        ctor.setAccessible(true);
        dbbCtor = ctor;
      }
      try {
        return (ByteBuffer)ctor.newInstance(baseDataAddress, dataSize);
      } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        //throw new IllegalStateException("Could not create an instance using DirectByteBuffer(long, int)", e);
        dbbClass = null;
        dbbCtor = null;
        dbbCreateFailed = true;
        return null;
      }
    }
    private static volatile Method dbbAddressMethod = null;
    private static volatile boolean dbbAddressFailed = false;
    
    /**
     * Returns the address of the Unsafe memory for the first byte of a direct ByteBuffer.
     * If the buffer is not direct or the address can not be obtained return 0.
     */
    public static long getDirectByteBufferAddress(ByteBuffer bb) {
      if (!bb.isDirect()) {
        return 0L;
      }
      if (dbbAddressFailed) {
        return 0L;
      }
      Method m = dbbAddressMethod;
      if (m == null) {
        Class c = dbbClass;
        if (c == null) {
          try {
            c = Class.forName("java.nio.DirectByteBuffer");
          } catch (ClassNotFoundException e) {
            //throw new IllegalStateException("Could not find java.nio.DirectByteBuffer", e);
            dbbCreateFailed = true;
            dbbAddressFailed = true;
            return 0L;
          }
          dbbClass = c;
        }
        try {
          m = c.getDeclaredMethod("address");
        } catch (NoSuchMethodException | SecurityException e) {
          //throw new IllegalStateException("Could not get method DirectByteBuffer.address()", e);
          dbbClass = null;
          dbbAddressFailed = true;
          return 0L;
        }
        m.setAccessible(true);
        dbbAddressMethod = m;
      }
      try {
        return (Long)m.invoke(bb);
      } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        //throw new IllegalStateException("Could not create an invoke DirectByteBuffer.address()", e);
        dbbClass = null;
        dbbAddressMethod = null;
        dbbAddressFailed = true;
        return 0L;
      }
    }
    /**
     * Returns an address that can be used with unsafe apis to access this chunks memory.
     * @param offset the offset from this chunk's first byte of the byte the returned address should point to. Must be >= 0.
     * @param size the number of bytes that will be read using the returned address. Assertion will use this to verify that all the memory accessed belongs to this chunk. Must be > 0.
     * @return a memory address that can be used with unsafe apis
     */
    public long getUnsafeAddress(int offset, int size) {
      assert offset >= 0 && offset + size <= getDataSize(): "Offset=" + offset + ",size=" + size + ",dataSize=" + getDataSize() + ", chunkSize=" + getSize() + ", but offset + size must be <= " + getDataSize();
      assert size > 0;
      long result = getBaseDataAddress() + offset;
      // validateAddressAndSizeWithinSlab(result, size);
      return result;
    }
    
    @Override
    public byte readByte(int offset) {
      assert offset < getDataSize();
      return UnsafeMemoryChunk.readAbsoluteByte(getBaseDataAddress() + offset);
    }

    @Override
    public void writeByte(int offset, byte value) {
      assert offset < getDataSize();
      UnsafeMemoryChunk.writeAbsoluteByte(getBaseDataAddress() + offset, value);
    }

    @Override
    public void readBytes(int offset, byte[] bytes) {
      readBytes(offset, bytes, 0, bytes.length);
    }

    @Override
    public void writeBytes(int offset, byte[] bytes) {
      writeBytes(offset, bytes, 0, bytes.length);
    }

    public long getAddressForReading(int offset, int size) {
      assert offset+size <= getDataSize();
      return getBaseDataAddress() + offset;
    }
    
    @Override
    public void readBytes(int offset, byte[] bytes, int bytesOffset, int size) {
      assert offset+size <= getDataSize();
      UnsafeMemoryChunk.readAbsoluteBytes(getBaseDataAddress() + offset, bytes, bytesOffset, size);
    }

    @Override
    public void writeBytes(int offset, byte[] bytes, int bytesOffset, int size) {
      assert offset+size <= getDataSize();
      SimpleMemoryAllocatorImpl.validateAddressAndSizeWithinSlab(getBaseDataAddress() + offset, size);
      UnsafeMemoryChunk.writeAbsoluteBytes(getBaseDataAddress() + offset, bytes, bytesOffset, size);
    }
    
    @Override
    public void release() {
      release(this.memoryAddress, true);
     }

    @Override
    public int compareTo(Chunk o) {
      int result = Integer.signum(getSize() - o.getSize());
      if (result == 0) {
        // For the same sized chunks we really don't care about their order
        // but we need compareTo to only return 0 if the two chunks are identical
        result = Long.signum(getMemoryAddress() - o.getMemoryAddress());
      }
      return result;
    }
    
    @Override
    public boolean equals(Object o) {
      if (o instanceof Chunk) {
        return getMemoryAddress() == ((Chunk) o).getMemoryAddress();
      }
      return false;
    }
    
    @Override
    public int hashCode() {
      long value = this.getMemoryAddress();
      return (int)(value ^ (value >>> 32));
    }

    // OffHeapCachedDeserializable methods 
    
    @Override
    public void setSerializedValue(byte[] value) {
      writeBytes(0, value);
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
      readBytes(0, result);
      //debugLog("reading", true);
      SimpleMemoryAllocatorImpl.getAllocator().getStats().incReads();
      return result;
    }
    protected byte[] getRawBytes() {
      byte[] result = getCompressedBytes();
      // TODO OFFHEAP: change the following to assert !isCompressed();
      if (isCompressed()) {
        throw new UnsupportedOperationException();
      }
      return result;
    }

    @Override
    public byte[] getSerializedValue() {
      byte [] result = getRawBytes();
      if (!isSerialized()) {
        // The object is a byte[]. So we need to make it look like a serialized byte[] in our result
        result = EntryEventImpl.serialize(result);
      }
      return result;
    }
    
    @Override
    public Object getDeserializedValue(Region r, RegionEntry re) {
      if (isSerialized()) {
        // TODO OFFHEAP: debug deserializeChunk
        return EntryEventImpl.deserialize(getRawBytes());
        //assert !isCompressed();
        //return EntryEventImpl.deserializeChunk(this);
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
      // We do not add anything to this since the size of the reference belongs to the region entry size
      // not the size of this object.
      return getSize();
    }

    @Override
    public int getValueSizeInBytes() {
      return getDataSize();
    }

    @Override
    public void copyBytes(int src, int dst, int size) {
      throw new UnsupportedOperationException("Implement if used");
//      assert src+size <= getDataSize();
//      assert dst+size < getDataSize();
//      getSlabs()[this.getSlabIdx()].copyBytes(getBaseDataAddress()+src, getBaseDataAddress()+dst, size);
    }

    @Override
    public boolean isSerialized() {
      return (UnsafeMemoryChunk.readAbsoluteInt(this.memoryAddress+REF_COUNT_OFFSET) & IS_SERIALIZED_BIT) != 0;
    }

    @Override
    public boolean isCompressed() {
      return (UnsafeMemoryChunk.readAbsoluteInt(this.memoryAddress+REF_COUNT_OFFSET) & IS_COMPRESSED_BIT) != 0;
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
      SimpleMemoryAllocatorImpl.validateAddress(memAddr);
      return UnsafeMemoryChunk.readAbsoluteInt(memAddr+CHUNK_SIZE_OFFSET);
    }
    public static void setSize(long memAddr, int size) {
      SimpleMemoryAllocatorImpl.validateAddressAndSize(memAddr, size);
      UnsafeMemoryChunk.writeAbsoluteInt(memAddr+CHUNK_SIZE_OFFSET, size);
    }
    public static long getNext(long memAddr) {
      SimpleMemoryAllocatorImpl.validateAddress(memAddr);
      return UnsafeMemoryChunk.readAbsoluteLong(memAddr+OFF_HEAP_HEADER_SIZE);
    }
    public static void setNext(long memAddr, long next) {
      SimpleMemoryAllocatorImpl.validateAddress(memAddr);
      UnsafeMemoryChunk.writeAbsoluteLong(memAddr+OFF_HEAP_HEADER_SIZE, next);
    }
    @Override
    public ChunkType getChunkType() {
      return SimpleMemoryAllocatorImpl.getAllocator().getChunkFactory().getChunkTypeForAddress(getMemoryAddress());
    }
    public static int getSrcTypeOrdinal(long memAddr) {
      return getSrcType(memAddr) >> SRC_TYPE_SHIFT;
    }
    public static int getSrcType(long memAddr) {
      return getSrcTypeFromRawBits(UnsafeMemoryChunk.readAbsoluteInt(memAddr+REF_COUNT_OFFSET));
    }
    public static int getSrcTypeFromRawBits(int rawBits) {
      return rawBits & SRC_TYPE_MASK;
    }
    public static int getSrcTypeOrdinalFromRawBits(int rawBits) {
      return getSrcTypeFromRawBits(rawBits) >> SRC_TYPE_SHIFT;
    }
    
    /**
     * Fills the chunk with a repeated byte fill pattern.
     * @param baseAddress the starting address for a {@link Chunk}.
     */
    public static void fill(long baseAddress) {
      long startAddress = baseAddress + MIN_CHUNK_SIZE;
      int size = getSize(baseAddress) - MIN_CHUNK_SIZE;
      
      UnsafeMemoryChunk.fill(startAddress, size, FILL_BYTE);
    }
    
    /**
     * Validates that the fill pattern for this chunk has not been disturbed.  This method
     * assumes the TINY_MULTIPLE is 8 bytes.
     * @throws IllegalStateException when the pattern has been violated.
     */
    public void validateFill() {
      assert SimpleMemoryAllocatorImpl.TINY_MULTIPLE == 8;
      
      long startAddress = getMemoryAddress() + MIN_CHUNK_SIZE;
      int size = getSize() - MIN_CHUNK_SIZE;
      
      for(int i = 0;i < size;i += SimpleMemoryAllocatorImpl.TINY_MULTIPLE) {
        if(UnsafeMemoryChunk.readAbsoluteLong(startAddress + i) != FILL_PATTERN) {
          throw new IllegalStateException("Fill pattern violated for chunk " + getMemoryAddress() + " with size " + getSize());
        }        
      }
    }

    public void setSerialized(boolean isSerialized) {
      if (isSerialized) {
        int bits;
        int originalBits;
        do {
          originalBits = UnsafeMemoryChunk.readAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET);
          if ((originalBits&MAGIC_MASK) != MAGIC_NUMBER) {
            throw new IllegalStateException("It looks like this off heap memory was already freed. rawBits=" + Integer.toHexString(originalBits));
          }
          bits = originalBits | IS_SERIALIZED_BIT;
        } while (!UnsafeMemoryChunk.writeAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET, originalBits, bits));
      }
    }
    public void setCompressed(boolean isCompressed) {
      if (isCompressed) {
        int bits;
        int originalBits;
        do {
          originalBits = UnsafeMemoryChunk.readAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET);
          if ((originalBits&MAGIC_MASK) != MAGIC_NUMBER) {
            throw new IllegalStateException("It looks like this off heap memory was already freed. rawBits=" + Integer.toHexString(originalBits));
          }
          bits = originalBits | IS_COMPRESSED_BIT;
        } while (!UnsafeMemoryChunk.writeAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET, originalBits, bits));
      }
    }
    public void setDataSize(int dataSize) { // KIRK
      assert dataSize <= getSize();
      int delta = getSize() - dataSize;
      assert delta <= (DATA_SIZE_DELTA_MASK >> DATA_SIZE_SHIFT);
      delta <<= DATA_SIZE_SHIFT;
      int bits;
      int originalBits;
      do {
        originalBits = UnsafeMemoryChunk.readAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET);
        if ((originalBits&MAGIC_MASK) != MAGIC_NUMBER) {
          throw new IllegalStateException("It looks like this off heap memory was already freed. rawBits=" + Integer.toHexString(originalBits));
        }
        bits = originalBits;
        bits &= ~DATA_SIZE_DELTA_MASK; // clear the old dataSizeDelta bits
        bits |= delta; // set the dataSizeDelta bits to the new delta value
      } while (!UnsafeMemoryChunk.writeAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET, originalBits, bits));
    }
    
    public void initializeUseCount() {
      int rawBits;
      do {
        rawBits = UnsafeMemoryChunk.readAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET);
        if ((rawBits&MAGIC_MASK) != MAGIC_NUMBER) {
          throw new IllegalStateException("It looks like this off heap memory was already freed. rawBits=" + Integer.toHexString(rawBits));
        }
        int uc = rawBits & REF_COUNT_MASK;
        if (uc != 0) {
          throw new IllegalStateException("Expected use count to be zero but it was: " + uc + " rawBits=0x" + Integer.toHexString(rawBits));
        }
      } while (!UnsafeMemoryChunk.writeAbsoluteIntVolatile(this.memoryAddress+REF_COUNT_OFFSET, rawBits, rawBits+1));
    }

    public static int getRefCount(long memAddr) {
      return UnsafeMemoryChunk.readAbsoluteInt(memAddr+REF_COUNT_OFFSET) & REF_COUNT_MASK;
    }

    public static boolean retain(long memAddr) {
      SimpleMemoryAllocatorImpl.validateAddress(memAddr);
      int uc;
      int rawBits;
      int retryCount = 0;
      do {
        rawBits = UnsafeMemoryChunk.readAbsoluteIntVolatile(memAddr+REF_COUNT_OFFSET);
        if ((rawBits&MAGIC_MASK) != MAGIC_NUMBER) {
          // same as uc == 0
          // TODO MAGIC_NUMBER rethink its use and interaction with compactor fragments
          return false;
        }
        uc = rawBits & REF_COUNT_MASK;
        if (uc == MAX_REF_COUNT) {
          throw new IllegalStateException("Maximum use count exceeded. rawBits=" + Integer.toHexString(rawBits));
        } else if (uc == 0) {
          return false;
        }
        retryCount++;
        if (retryCount > 1000) {
          throw new IllegalStateException("tried to write " + (rawBits+1) + " to @" + Long.toHexString(memAddr) + " 1,000 times.");
        }
      } while (!UnsafeMemoryChunk.writeAbsoluteIntVolatile(memAddr+REF_COUNT_OFFSET, rawBits, rawBits+1));
      //debugLog("use inced ref count " + (uc+1) + " @" + Long.toHexString(memAddr), true);
      if (ReferenceCountHelper.trackReferenceCounts()) {
        ReferenceCountHelper.refCountChanged(memAddr, false, uc+1);
      }

      return true;
    }
    public static void release(final long memAddr, boolean issueOnReturnCallback) {
      SimpleMemoryAllocatorImpl.validateAddress(memAddr);
      int newCount;
      int rawBits;
      boolean returnToAllocator;
      do {
        returnToAllocator = false;
        rawBits = UnsafeMemoryChunk.readAbsoluteIntVolatile(memAddr+REF_COUNT_OFFSET);
        if ((rawBits&MAGIC_MASK) != MAGIC_NUMBER) {
          String msg = "It looks like off heap memory @" + Long.toHexString(memAddr) + " was already freed. rawBits=" + Integer.toHexString(rawBits) + " history=" + ReferenceCountHelper.getFreeRefCountInfo(memAddr);
          //debugLog(msg, true);
          throw new IllegalStateException(msg);
        }
        int curCount = rawBits&REF_COUNT_MASK;
        if ((curCount) == 0) {
          //debugLog("too many frees @" + Long.toHexString(memAddr), true);
          throw new IllegalStateException("Memory has already been freed." + " history=" + ReferenceCountHelper.getFreeRefCountInfo(memAddr) /*+ System.identityHashCode(this)*/);
        }
        if (curCount == 1) {
          newCount = 0; // clear the use count, bits, and the delta size since it will be freed.
          returnToAllocator = true;
        } else {
          newCount = rawBits-1;
        }
      } while (!UnsafeMemoryChunk.writeAbsoluteIntVolatile(memAddr+REF_COUNT_OFFSET, rawBits, newCount));
      //debugLog("free deced ref count " + (newCount&USE_COUNT_MASK) + " @" + Long.toHexString(memAddr), true);
      if (returnToAllocator ) {
        /*
        if(issueOnReturnCallback) {
         final GemFireCacheImpl.StaticSystemCallbacks sysCb =
              GemFireCacheImpl.FactoryStatics.systemCallbacks;
          if(sysCb != null ) {
            ChunkType ct = SimpleMemoryAllocatorImpl.getAllocator().getChunkFactory().getChunkTypeForRawBits(rawBits);
            int dataSizeDelta = computeDataSizeDelta(rawBits);
            sysCb.beforeReturningOffHeapMemoryToAllocator(memAddr, ct, dataSizeDelta);
          }
        }
        */
       
        if (ReferenceCountHelper.trackReferenceCounts()) {
          if (ReferenceCountHelper.trackFreedReferenceCounts()) {
            ReferenceCountHelper.refCountChanged(memAddr, true, newCount&REF_COUNT_MASK);
          }
          ReferenceCountHelper.freeRefCountInfo(memAddr);
        }
        
        // Use fill pattern for free list data integrity check.
        if(SimpleMemoryAllocatorImpl.getAllocator().validateMemoryWithFill) {
          fill(memAddr);
        }
        
        SimpleMemoryAllocatorImpl.getAllocator().freeChunk(memAddr);
      } else {
        if (ReferenceCountHelper.trackReferenceCounts()) {
          ReferenceCountHelper.refCountChanged(memAddr, true, newCount&REF_COUNT_MASK);
        }
      }
    }
    
    private static int computeDataSizeDelta(int rawBits) {
      int dataSizeDelta = rawBits;
      dataSizeDelta &= DATA_SIZE_DELTA_MASK;
      dataSizeDelta >>= DATA_SIZE_SHIFT;
      return dataSizeDelta;
    }
    
    @Override
    public String toString() {
      return toStringForOffHeapByteSource();
      // This old impl is not safe because it calls getDeserializedForReading and we have code that call toString that does not inc the refcount.
      // Also if this Chunk is compressed we don't know how to decompress it.
      //return super.toString() + ":<dataSize=" + getDataSize() + " refCount=" + getRefCount() + " addr=" + getMemoryAddress() + " storedObject=" + getDeserializedForReading() + ">";
    }
    
    protected String toStringForOffHeapByteSource() {
      return super.toString() + ":<dataSize=" + getDataSize() + " refCount=" + getRefCount() + " addr=" + Long.toHexString(getMemoryAddress()) + ">";
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
    public Chunk slice(int position, int limit) {
      throw new UnsupportedOperationException();
    }
  }