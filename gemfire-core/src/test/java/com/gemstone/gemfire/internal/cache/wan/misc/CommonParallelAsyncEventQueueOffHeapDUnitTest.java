package com.gemstone.gemfire.internal.cache.wan.misc;

@SuppressWarnings("serial")
public class CommonParallelAsyncEventQueueOffHeapDUnitTest extends
    CommonParallelAsyncEventQueueDUnitTest {

  public CommonParallelAsyncEventQueueOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }

}
