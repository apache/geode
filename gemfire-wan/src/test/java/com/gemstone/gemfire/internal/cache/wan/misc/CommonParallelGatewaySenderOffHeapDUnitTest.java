package com.gemstone.gemfire.internal.cache.wan.misc;

@SuppressWarnings("serial")
public class CommonParallelGatewaySenderOffHeapDUnitTest extends
    CommonParallelGatewaySenderDUnitTest {

  public CommonParallelGatewaySenderOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }

}
