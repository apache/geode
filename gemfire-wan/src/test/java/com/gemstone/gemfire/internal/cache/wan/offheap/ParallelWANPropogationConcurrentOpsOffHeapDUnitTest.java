package com.gemstone.gemfire.internal.cache.wan.offheap;

import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelWANPropagationConcurrentOpsDUnitTest;

@SuppressWarnings("serial")
public class ParallelWANPropogationConcurrentOpsOffHeapDUnitTest extends
    ParallelWANPropagationConcurrentOpsDUnitTest {

  public ParallelWANPropogationConcurrentOpsOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }

}
