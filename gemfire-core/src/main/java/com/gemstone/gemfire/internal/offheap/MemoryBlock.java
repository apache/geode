package com.gemstone.gemfire.internal.offheap;

import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.ChunkType;

/**
 * Basic size and usage information about an off-heap memory block under
 * inspection. For test validation only.
 * 
 * @author Kirk Lund
 * @since 9.0
 */
public interface MemoryBlock {

  public enum State {
    /** Unused fragment (not used and not in a free list) */
    UNUSED, 
    /** Allocated chunk currently in use */
    ALLOCATED, 
    /** Deallocated chunk currently in a free list */
    DEALLOCATED 
  }
  
  public State getState();
  
  /**
   * Returns the unsafe memory address of the first byte of this block.
   */
  public long getMemoryAddress();
  
  /**
   * Returns the size of this memory block in bytes.
   */
  public int getBlockSize();
  
  /**
   * Returns the next memory block immediately after this one.
   */
  public MemoryBlock getNextBlock();
  
  /**
   * Returns the identifier of which slab contains this block.
   */
  public int getSlabId();
  
  /**
   * Returns the identifier of which free list contains this block.
   */
  public int getFreeListId();
  
  public int getRefCount();
  public String getDataType();
  public ChunkType getChunkType();
  public boolean isSerialized();
  public boolean isCompressed();
  public Object getDataValue();
}
