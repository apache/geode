package com.gemstone.gemfire.internal.cache.wan.concurrent;

@SuppressWarnings("serial")
public class ConcurrentParallelGatewaySenderOffHeapDUnitTest extends
    ConcurrentParallelGatewaySenderDUnitTest {

  public ConcurrentParallelGatewaySenderOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }

}
