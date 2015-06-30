/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import org.junit.Ignore;
import org.junit.Test;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;

public class DistributedAckOverflowRegionCCEDUnitTest extends
    DistributedAckRegionCCEDUnitTest {

  public DistributedAckOverflowRegionCCEDUnitTest(String name) {
    super(name);
  }

  @Override
  protected RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setConcurrencyChecksEnabled(true);
    factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(
        5, EvictionAction.OVERFLOW_TO_DISK));
    return factory.create();
  }
  
  @Override
  protected RegionAttributes getRegionAttributes(String type) {
    RegionAttributes ra = getCache().getRegionAttributes(type);
    if (ra == null) {
      throw new IllegalStateException("The region shortcut " + type
                                      + " has been removed.");
    }
    AttributesFactory factory = new AttributesFactory(ra);
    factory.setConcurrencyChecksEnabled(true);
    if(!ra.getDataPolicy().isEmpty()) {
      factory.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(
          5, EvictionAction.OVERFLOW_TO_DISK));
    }
    return factory.create();
  }

  @Override
  @Test
  @Ignore
  public void testClearWithConcurrentEvents() throws Exception {
    // TODO this test is disabled due to frequent failures.  See bug #
    // Remove this method from this class when the problem is fixed
//    super.testClearWithConcurrentEvents();
  }

  @Override
  @Test
  @Ignore
  public void testClearWithConcurrentEventsAsync() throws Exception {
    // TODO this test is disabled due to frequent failures.  See bug #
    // Remove this method from this class when the problem is fixed
//    super.testClearWithConcurrentEventsAsync();
  }
  
  
}
