package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceType;
import com.gemstone.gemfire.internal.cache.control.OffHeapMemoryMonitor;
import com.gemstone.gemfire.internal.cache.lru.HeapEvictor;

import dunit.SerializableRunnable;

public class PartitionedRegionOffHeapEvictionDUnitTest extends
    PartitionedRegionEvictionDUnitTest {
  
  public PartitionedRegionOffHeapEvictionDUnitTest(String name) {
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
    Properties properties = super.getDistributedSystemProperties();    
    properties.setProperty(DistributionConfig.OFF_HEAP_MEMORY_SIZE_NAME, "100m");    
    
    return properties;
  }
  
  @Override
  protected void setEvictionPercentage(float percentage) {
    getCache().getResourceManager().setEvictionOffHeapPercentage(percentage);    
  }

  @Override
  protected boolean isOffHeap() {
    return true;
  }

  @Override
  protected ResourceType getMemoryType() {
    return ResourceType.OFFHEAP_MEMORY;
  }

  @Override
  protected HeapEvictor getEvictor(Region region) {
    return ((GemFireCacheImpl)region.getRegionService()).getOffHeapEvictor();
  }
  
  protected void raiseFakeNotification() {
    ((GemFireCacheImpl) getCache()).getOffHeapEvictor().testAbortAfterLoopCount = 1;
    
    setEvictionPercentage(85);
    OffHeapMemoryMonitor ohmm = ((GemFireCacheImpl) getCache()).getResourceManager().getOffHeapMonitor();
    ohmm.stopMonitoring();

    ohmm.updateStateAndSendEvent(94371840);
  }
  
  protected void cleanUpAfterFakeNotification() {
    ((GemFireCacheImpl) getCache()).getOffHeapEvictor().testAbortAfterLoopCount = Integer.MAX_VALUE;
  }
}