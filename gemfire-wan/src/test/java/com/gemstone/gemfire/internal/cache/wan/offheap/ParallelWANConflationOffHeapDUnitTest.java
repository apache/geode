package com.gemstone.gemfire.internal.cache.wan.offheap;

import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelWANConflationDUnitTest;

@SuppressWarnings("serial")
public class ParallelWANConflationOffHeapDUnitTest extends
    ParallelWANConflationDUnitTest {

  public ParallelWANConflationOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }

}
