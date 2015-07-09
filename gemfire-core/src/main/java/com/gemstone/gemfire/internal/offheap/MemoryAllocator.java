package com.gemstone.gemfire.internal.offheap;

import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.ChunkType;

/**
 * Basic contract for a heap that manages off heap memory. Any MemoryChunks allocated from a heap
 * are returned to that heap when freed.
 * 
 * @author darrel
 * @since 9.0
 */
public interface MemoryAllocator {
  /**
   * @param size the size in bytes of the chunk of memory to allocate
   * @param chunkType TODO
   * @return the allocated chunk of memory.
   * @throws IllegalStateException if the heap does not have enough memory to grant the request
   */
  public MemoryChunk allocate(int size, ChunkType chunkType);
  
  /**
   * Allocates off heap memory for the given data and returns a MemoryChunk
   * that is backed by this allocated memory and that contains the data.
   * @param data the bytes of the data to put in the allocated CachedDeserializable
   * @param isSerialized true if data contains a serialized object; false if it is an actual byte array.
   * @param isCompressed true if data is compressed; false if it is uncompressed.
   * @param chunkType TODO
   * @throws IllegalStateException if the heap does not have enough memory to grant the request
   */
  public StoredObject allocateAndInitialize(byte[] data, boolean isSerialized, boolean isCompressed, ChunkType chunkType);
  
  public long getFreeMemory();
  
  public long getUsedMemory();

  public long getTotalMemory();

  public OffHeapMemoryStats getStats();

  /**
   * This allocator will no longer be used so free up any system memory that belongs to it.
   */
  public void close();

  public MemoryInspector getMemoryInspector();
  
  public void addMemoryUsageListener(MemoryUsageListener listener);
  
  public void removeMemoryUsageListener(MemoryUsageListener listener);
}
