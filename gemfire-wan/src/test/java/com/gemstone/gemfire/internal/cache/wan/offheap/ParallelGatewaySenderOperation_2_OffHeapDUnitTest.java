package com.gemstone.gemfire.internal.cache.wan.offheap;

import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderOperation_2_DUnitTest;

public class ParallelGatewaySenderOperation_2_OffHeapDUnitTest
    extends ParallelGatewaySenderOperation_2_DUnitTest{

  public ParallelGatewaySenderOperation_2_OffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }
}
