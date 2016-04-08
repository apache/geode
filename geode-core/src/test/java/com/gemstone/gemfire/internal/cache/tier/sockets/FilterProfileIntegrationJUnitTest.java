/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.text.ParseException;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.junit.experimental.categories.Category;

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
public class FilterProfileIntegrationJUnitTest {

  private static String regionName = "test";
  private static int numElem = 120;
  
  @Test
  public void testFilterProfile() throws Exception {
    Cache cache = CacheUtils.getCache();
    try {
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
    } finally {
      cache.getDistributedSystem().disconnect();
      cache.close();
    }
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
