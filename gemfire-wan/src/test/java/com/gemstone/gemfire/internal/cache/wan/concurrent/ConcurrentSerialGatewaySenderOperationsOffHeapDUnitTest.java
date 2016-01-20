package com.gemstone.gemfire.internal.cache.wan.concurrent;

@SuppressWarnings("serial")
public class ConcurrentSerialGatewaySenderOperationsOffHeapDUnitTest extends
    ConcurrentSerialGatewaySenderOperationsDUnitTest {

  public ConcurrentSerialGatewaySenderOperationsOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }

}
