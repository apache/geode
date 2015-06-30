package com.gemstone.gemfire.internal.offheap;

import com.gemstone.gemfire.OutOfOffHeapMemoryException;

/**
 * Listens to the MemoryAllocator for notification of OutOfOffHeapMemoryError.
 * 
 * The implementation created by OffHeapStorage for a real DistribytedSystem
 * connection causes the System and Cache to close in order to avoid data
 * inconsistency.
 * 
 * @author Kirk Lund
 * @since 9.0
 */
public interface OutOfOffHeapMemoryListener {

  /**
   * Notification that an OutOfOffHeapMemoryError has occurred.
   * 
   * @param cause the actual OutOfOffHeapMemoryError that was thrown
   */
  public void outOfOffHeapMemory(OutOfOffHeapMemoryException cause);
  
  /**
   * Close any resources used by this listener.
   */
  public void close();
}
