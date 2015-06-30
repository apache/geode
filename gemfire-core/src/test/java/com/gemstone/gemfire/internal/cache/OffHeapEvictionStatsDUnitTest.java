/*=========================================================================
 * Copyright (c) 2010-2011 VMware, Inc. All rights reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. VMware products are covered by
 * one or more patents listed at http://www.vmware.com/go/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Properties;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;

import dunit.SerializableRunnable;

/**
 * Performs eviction stat dunit tests for off-heap regions.
 * @author rholmes
 * @since 9.0
 */
public class OffHeapEvictionStatsDUnitTest extends EvictionStatsDUnitTest {

  public OffHeapEvictionStatsDUnitTest(String name) {
    super(name);
    // TODO Auto-generated constructor stub
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

  public void createCache() {
    try {
      Properties props = new Properties();
      DistributedSystem ds = getSystem(props);
      assertNotNull(ds);
      ds.disconnect();
      ds = getSystem(getDistributedSystemProperties());
      cache = CacheFactory.create(ds);
      assertNotNull(cache);
      getLogWriter().info("cache= " + cache);
      getLogWriter().info("cache closed= " + cache.isClosed());
      cache.getResourceManager().setEvictionOffHeapPercentage(20);
      getLogWriter().info("eviction= "+cache.getResourceManager().getEvictionOffHeapPercentage());
      getLogWriter().info("critical= "+cache.getResourceManager().getCriticalOffHeapPercentage());
    }
    catch (Exception e) {
      fail("Failed while creating the cache", e);
    }
  }

  @Override
  public boolean isOffHeapEnabled() {
    return true;
  }    
}
