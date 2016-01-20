package com.gemstone.gemfire.internal.cache.wan.offheap;

import com.gemstone.gemfire.internal.cache.wan.serial.SerialWANPropogationDUnitTest;

@SuppressWarnings("serial")
public class SerialWANPropogationOffHeapDUnitTest extends
    SerialWANPropogationDUnitTest {

  public SerialWANPropogationOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }

}
