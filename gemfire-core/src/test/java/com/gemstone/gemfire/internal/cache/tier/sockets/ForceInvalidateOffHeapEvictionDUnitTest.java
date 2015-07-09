/*=========================================================================
 * Copyright (c) 2002-2011 VMware, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. VMware products are covered by
 * more patents listed at http://www.vmware.com/go/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.Properties;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.OffHeapTestUtil;

import dunit.SerializableRunnable;

/**
 * Runs force invalidate eviction tests with off-heap regions.
 * @author rholmes
 * @since 9.0
 */
public class ForceInvalidateOffHeapEvictionDUnitTest extends
    ForceInvalidateEvictionDUnitTest {

  public ForceInvalidateOffHeapEvictionDUnitTest(String name) {
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
  public boolean isOffHeapEnabled() {
    return true;
  }
}
