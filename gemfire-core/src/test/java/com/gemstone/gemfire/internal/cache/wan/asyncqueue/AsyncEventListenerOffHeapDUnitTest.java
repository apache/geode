package com.gemstone.gemfire.internal.cache.wan.asyncqueue;


@SuppressWarnings("serial")
public class AsyncEventListenerOffHeapDUnitTest extends
    AsyncEventListenerDUnitTest {

  public AsyncEventListenerOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }

}
