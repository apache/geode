package com.gemstone.gemfire.internal.cache.wan.offheap;

import com.gemstone.gemfire.internal.cache.wan.concurrent.ConcurrentParallelGatewaySenderOperation_2_DUnitTest;

public class ConcurrentParallelGatewaySenderOperation_2_OffHeapDUnitTest
    extends ConcurrentParallelGatewaySenderOperation_2_DUnitTest {

  public ConcurrentParallelGatewaySenderOperation_2_OffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }
}
