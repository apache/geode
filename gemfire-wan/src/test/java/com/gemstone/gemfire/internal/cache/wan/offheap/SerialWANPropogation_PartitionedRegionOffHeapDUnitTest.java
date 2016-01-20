package com.gemstone.gemfire.internal.cache.wan.offheap;

import com.gemstone.gemfire.internal.cache.wan.serial.SerialWANPropogation_PartitionedRegionDUnitTest;

@SuppressWarnings("serial")
public class SerialWANPropogation_PartitionedRegionOffHeapDUnitTest extends
    SerialWANPropogation_PartitionedRegionDUnitTest {

  public SerialWANPropogation_PartitionedRegionOffHeapDUnitTest(String name) {
    super(name);
  }

  @Override
  public boolean isOffHeap() {
    return true;
  }

}
