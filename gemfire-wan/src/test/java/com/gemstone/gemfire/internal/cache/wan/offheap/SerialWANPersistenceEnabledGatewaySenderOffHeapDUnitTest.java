package com.gemstone.gemfire.internal.cache.wan.offheap;

import com.gemstone.gemfire.internal.cache.wan.serial.SerialWANPersistenceEnabledGatewaySenderDUnitTest;

@SuppressWarnings("serial")
public class SerialWANPersistenceEnabledGatewaySenderOffHeapDUnitTest extends
    SerialWANPersistenceEnabledGatewaySenderDUnitTest {

  public SerialWANPersistenceEnabledGatewaySenderOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }

}
