package com.gemstone.gemfire.internal.offheap;

import java.util.List;

/**
 * Provides for inspection of meta-data for off-heap memory blocks.
 * 
 * @author Kirk Lund
 * @since 9.0
 */
public interface MemoryInspector {

  public void clearInspectionSnapshot();
  
  public void createInspectionSnapshot();

  public MemoryBlock getFirstBlock();
  
  public List<MemoryBlock> getAllBlocks();
  
  public List<MemoryBlock> getAllocatedBlocks();
  
  public List<MemoryBlock> getDeallocatedBlocks();
  
  public List<MemoryBlock> getUnusedBlocks();
  
  public MemoryBlock getBlockContaining(long memoryAddress);
  
  public MemoryBlock getBlockAfter(MemoryBlock block);
  
  public List<MemoryBlock> getOrphans();
}
