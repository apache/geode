package com.gemstone.gemfire.internal.offheap;

/**
 * Adds a reference count to the basic MemoryChunk.
 * Also an Object can be stored in one of these.
 * To increment the count call {@link #retain()}.
 * To decrement the count call {@link #release()}.
 * 
 * @author darrel
 * @since 9.0
 */
public interface MemoryChunkWithRefCount extends MemoryChunk, StoredObject {

  /**
   * Returns the number of users of this memory.
   */
  public int getRefCount();
}
