package com.gemstone.gemfire.internal.offheap;

import com.gemstone.gemfire.OutOfOffHeapMemoryException;

/**
 * Null implementation of NullOutOfOffHeapMemoryListener for testing.
 *  
 * @author Kirk Lund
 */
public class NullOutOfOffHeapMemoryListener implements OutOfOffHeapMemoryListener {
  @Override
  public void outOfOffHeapMemory(OutOfOffHeapMemoryException cause) {
  }
  @Override
  public void close() {
  }
}
