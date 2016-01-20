package com.gemstone.gemfire.internal.cache.wan.offheap;

import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderQueueOverflowDUnitTest;

@SuppressWarnings("serial")
public class ParallelGatewaySenderQueueOverflowOffHeapDUnitTest extends
    ParallelGatewaySenderQueueOverflowDUnitTest {

  public ParallelGatewaySenderQueueOverflowOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }

}
