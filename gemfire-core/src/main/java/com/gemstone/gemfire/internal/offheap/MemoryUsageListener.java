package com.gemstone.gemfire.internal.offheap;

/**
 * Defines callback for notification when off-heap memory usage changes.
 * 
 * @author Kirk Lund
 * @since 9.0
 */
public interface MemoryUsageListener {
  public void updateMemoryUsed(long bytesUsed);
}
