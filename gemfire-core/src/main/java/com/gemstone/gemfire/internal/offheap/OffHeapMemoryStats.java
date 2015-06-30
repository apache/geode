package com.gemstone.gemfire.internal.offheap;

import com.gemstone.gemfire.Statistics;

/**
 * Statistics for off-heap memory storage.
 * 
 * @author Kirk Lund
 * @since 9.0
 */
public interface OffHeapMemoryStats {

  public void incFreeMemory(long value);
  public void incMaxMemory(long value);
  public void incUsedMemory(long value);
  public void incObjects(int value);
  public void incReads();
  public void setFragments(long value);
  public void setLargestFragment(int value);
  public long startCompaction();
  public void endCompaction(long start);
  public void setFragmentation(int value);
  
  public long getFreeMemory();
  public long getMaxMemory();
  public long getUsedMemory();
  public long getReads();
  public int getObjects();
  public int getCompactions();
  public long getFragments();
  public int getLargestFragment();
  public int getFragmentation();
  public long getCompactionTime();
  
  public Statistics getStats();
  public void close();
  public void initialize(OffHeapMemoryStats stats);
}
