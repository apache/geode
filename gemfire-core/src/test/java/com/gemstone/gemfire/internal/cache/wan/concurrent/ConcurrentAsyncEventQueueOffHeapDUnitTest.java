package com.gemstone.gemfire.internal.cache.wan.concurrent;

@SuppressWarnings("serial")
public class ConcurrentAsyncEventQueueOffHeapDUnitTest extends
    ConcurrentAsyncEventQueueDUnitTest {

  public ConcurrentAsyncEventQueueOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }

}
