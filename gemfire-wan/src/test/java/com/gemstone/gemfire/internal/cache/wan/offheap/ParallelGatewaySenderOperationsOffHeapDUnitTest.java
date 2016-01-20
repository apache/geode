package com.gemstone.gemfire.internal.cache.wan.offheap;

import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderOperationsDUnitTest;

@SuppressWarnings("serial")
public class ParallelGatewaySenderOperationsOffHeapDUnitTest extends
    ParallelGatewaySenderOperationsDUnitTest {

  public ParallelGatewaySenderOperationsOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }

}
