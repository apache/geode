package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests PartitionedRegion DataStore currentAllocatedMemory operation.
 * 
 * @author Kirk Lund
 * @since 9.0
 */
@Category(IntegrationTest.class)
public class PRDataStoreMemoryOffHeapJUnitTest extends PRDataStoreMemoryJUnitTest {

  @Override
  protected Properties getDistributedSystemProperties() {
    Properties dsProps = super.getDistributedSystemProperties();
    dsProps.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "1m");
    return dsProps;
  }
  
  @SuppressWarnings({ "rawtypes", "deprecation" })
  @Override
  protected RegionFactory<?, ?> defineRegionFactory() {
    return new RegionFactory()
        .setPartitionAttributes(definePartitionAttributes())
        .setOffHeap(true);
  }
}
