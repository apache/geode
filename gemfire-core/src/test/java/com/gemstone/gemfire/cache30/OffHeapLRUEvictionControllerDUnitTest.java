/*=========================================================================
 * Copyright (c) 2010-2011 VMware, Inc. All rights reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. VMware products are covered by
 * one or more patents listed at http://www.vmware.com/go/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import java.util.Properties;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.OffHeapTestUtil;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceType;
import com.gemstone.gemfire.internal.cache.lru.HeapEvictor;

import dunit.SerializableRunnable;

/**
 * Tests the basic functionality of the lru eviction 
 * controller and its statistics using off-heap regions.
 * 
 * @author rholmes
 * @since 9.0
 */
public class OffHeapLRUEvictionControllerDUnitTest extends
    LRUEvictionControllerDUnitTest {

  public OffHeapLRUEvictionControllerDUnitTest(String name) {
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
  protected boolean isOffHeapEnabled() {
    return true;
  }
  
  @Override
  protected HeapEvictor getEvictor() {
    return ((GemFireCacheImpl) getCache()).getOffHeapEvictor();
  }
  
  @Override
  protected ResourceType getResourceType() {
    return ResourceType.OFFHEAP_MEMORY;
  }
}
