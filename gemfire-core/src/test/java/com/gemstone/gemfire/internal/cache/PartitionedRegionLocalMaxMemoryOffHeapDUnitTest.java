package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;

import dunit.SerializableRunnable;

/**
 * Tests PartitionedRegion localMaxMemory with Off-Heap memory.
 * 
 * @author Kirk Lund
 * @since 9.0
 */
@SuppressWarnings({ "deprecation", "serial" })
public class PartitionedRegionLocalMaxMemoryOffHeapDUnitTest extends PartitionedRegionLocalMaxMemoryDUnitTest {

  public PartitionedRegionLocalMaxMemoryOffHeapDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void tearDown2() throws Exception {
    SerializableRunnable checkOrphans = new SerializableRunnable() {

      @Override
      public void run() {
        if(hasCache()) {
          OffHeapTestUtil.checkOrphans();
        }
      }
    };
    invokeInEveryVM(checkOrphans);
    try {
      checkOrphans.run();
    } finally {
      super.tearDown2();
    }
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    // test creates a bit more than 1m of off heap so we need to total off heap size to be >1m
    props.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "2m");
    return props;
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  protected RegionAttributes<?, ?> createRegionAttrsForPR(int red, int localMaxMem, long recoveryDelay, EvictionAttributes evictionAttrs) {
    RegionAttributes<?, ?> attrs = super.createRegionAttrsForPR(
        red, localMaxMem, recoveryDelay, evictionAttrs);
    AttributesFactory factory = new AttributesFactory(attrs);
    factory.setOffHeap(true);
    return factory.create();
  }
}
