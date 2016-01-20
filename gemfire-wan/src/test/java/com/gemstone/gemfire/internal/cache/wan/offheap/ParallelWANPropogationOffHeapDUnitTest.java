package com.gemstone.gemfire.internal.cache.wan.offheap;

import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelWANPropagationDUnitTest;

@SuppressWarnings("serial")
public class ParallelWANPropogationOffHeapDUnitTest extends
    ParallelWANPropagationDUnitTest {

  public ParallelWANPropogationOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }

}
