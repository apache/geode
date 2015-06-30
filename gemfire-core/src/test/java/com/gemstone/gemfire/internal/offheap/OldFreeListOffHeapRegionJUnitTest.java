package com.gemstone.gemfire.internal.offheap;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class OldFreeListOffHeapRegionJUnitTest extends OffHeapRegionBase {

  @Override
  protected String getOffHeapMemorySize() {
    return "20m";
  }
  
  @Override
  public void configureOffHeapStorage() {
    System.setProperty("gemfire.OFF_HEAP_SLAB_SIZE", "1m");
  }

  @Override
  public void unconfigureOffHeapStorage() {
    System.clearProperty("gemfire.OFF_HEAP_TOTAL_SIZE");
    System.clearProperty("gemfire.OFF_HEAP_SLAB_SIZE");
  }

  @Override
  public int perObjectOverhead() {
    return SimpleMemoryAllocatorImpl.Chunk.OFF_HEAP_HEADER_SIZE;
  }

}
