/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.text.ParseException;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.internal.cache.FilterProfile;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.InterestType;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class FilterProfileJUnitTest {

  private static String regionName = "test";
  private static int numElem = 120;
  
  @Test
  public void testFilterProfile() throws Exception {
    Cache cache = CacheUtils.getCache();
    createLocalRegion();
    LocalRegion region = (LocalRegion) cache.getRegion(regionName);
    final FilterProfile filterProfile = new FilterProfile(region);
    filterProfile.registerClientInterest("clientId", ".*",
        InterestType.REGULAR_EXPRESSION, false);

    final FilterProfileTestHook hook = new FilterProfileTestHook();
    FilterProfile.testHook = hook;

    new Thread(new Runnable() {
      public void run() {
        while (hook.getCount() != 1) {

        }
        filterProfile.unregisterClientInterest("clientId", ".*",
            InterestType.REGULAR_EXPRESSION);

      }
    }).start();
    filterProfile.hasAllKeysInterestFor("clientId");
  }

  class FilterProfileTestHook implements FilterProfile.TestHook {

    CountDownLatch latch = new CountDownLatch(2);

    // On first time, we know the first thread will reduce count by one
    // this allows us to start the second thread, by checking the current count
    public void await() {
      try {
        latch.countDown();
        latch.await();
      } catch (Exception e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }

    public long getCount() {
      return latch.getCount();
    }

    public void release() {
      latch.countDown();
    }

  };

  /**
   * Helper Methods
   */
  
  private void createLocalRegion() throws ParseException {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.NORMAL);
    RegionAttributes regionAttributes = attributesFactory.create();
    Region region = cache.createRegion(regionName, regionAttributes);
  }
  
  
}
