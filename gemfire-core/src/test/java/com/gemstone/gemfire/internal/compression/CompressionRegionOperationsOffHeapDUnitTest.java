package com.gemstone.gemfire.internal.compression;

import java.util.Properties;

import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.OffHeapTestUtil;

import dunit.SerializableRunnable;

public class CompressionRegionOperationsOffHeapDUnitTest extends
    CompressionRegionOperationsDUnitTest {

  public CompressionRegionOperationsOffHeapDUnitTest(String name) {
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
    props.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "1m");
    return props;
  }

  @Override
  protected void createRegion() {
    Compressor compressor = null;
    try {
      compressor = SnappyCompressor.getDefaultInstance();;
    } catch (Throwable t) {
      // Not a supported OS
      return;
    }
    createCompressedRegionOnVm(getVM(TEST_VM), REGION_NAME, compressor, true);
  }

}
