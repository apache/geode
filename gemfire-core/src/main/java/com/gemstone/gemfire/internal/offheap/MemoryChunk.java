package com.gemstone.gemfire.internal.offheap;

/**
 * Represents a chunk of allocated memory that is not on the heap.
 * This interface provides methods that let you read and write to the chunk.
 * 
 * @author darrel
 * @since 9.0
 */
public interface MemoryChunk extends Releasable {
  
  /**
   * Returns the size of this memory chunk in bytes.
   */
  public int getSize();
  
  public byte readByte(int offset);
  public void writeByte(int offset, byte value);
  
  public void readBytes(int offset, byte[] bytes);
  public void writeBytes(int offset, byte[] bytes);
  public void readBytes(int offset, byte[] bytes, int bytesOffset, int size);
  public void writeBytes(int offset, byte[] bytes, int bytesOffset, int size);
  
  /**
   * Read the bytes in this range [src..src+size]
   * and write them to the range that starts at dst.
   * The number of bytes copied is size.
   */
  public void copyBytes(int src, int dst, int size);
}
