package com.gemstone.gemfire.internal.cache.wan.offheap;

import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelWANPersistenceEnabledGatewaySenderDUnitTest;

@SuppressWarnings("serial")
public class ParallelWANPersistenceEnabledGatewaySenderOffHeapDUnitTest extends
    ParallelWANPersistenceEnabledGatewaySenderDUnitTest {

  public ParallelWANPersistenceEnabledGatewaySenderOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }

}
