/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.tier.sockets;

import java.text.ParseException;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class FilterProfileIntegrationJUnitTest {

  private static final String regionName = "test";
  private static final int numElem = 120;

  @Test
  public void testFilterProfile() throws Exception {
    Cache cache = CacheUtils.getCache();
    try {
      createLocalRegion();
      LocalRegion region = (LocalRegion) cache.getRegion(regionName);
      final FilterProfile filterProfile = new FilterProfile(region);
      filterProfile.registerClientInterest("clientId", ".*", InterestType.REGULAR_EXPRESSION,
          false);

      final FilterProfileTestHook hook = new FilterProfileTestHook();
      FilterProfile.testHook = hook;

      new Thread(() -> {
        while (hook.getCount() != 1) {

        }
        filterProfile.unregisterClientInterest("clientId", ".*", InterestType.REGULAR_EXPRESSION);

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
    @Override
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

    @Override
    public void release() {
      latch.countDown();
    }

  }

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
