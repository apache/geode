package com.gemstone.gemfire.internal.cache.wan.offheap;

import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderQueueDUnitTest;

@SuppressWarnings("serial")
public class SerialGatewaySenderQueueOffHeapDUnitTest extends
    SerialGatewaySenderQueueDUnitTest {

  public SerialGatewaySenderQueueOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }

}
