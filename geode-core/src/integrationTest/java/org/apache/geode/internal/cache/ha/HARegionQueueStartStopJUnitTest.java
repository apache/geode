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
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

@Category({ClientSubscriptionTest.class})
public class HARegionQueueStartStopJUnitTest {

  private InternalCache createCache() throws CacheExistsException, RegionExistsException {
    final Properties props = new Properties();
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    return (InternalCache) CacheFactory.create(DistributedSystem.connect(props));
  }

  private RegionQueue createHARegionQueue(String name, InternalCache cache)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException {
    RegionQueue regionqueue = HARegionQueue.getHARegionQueueInstance(name, cache,
        HARegionQueue.NON_BLOCKING_HA_QUEUE, false, disabledClock());
    return regionqueue;
  }

  @Test
  public void testStartStop() throws Exception {
    InternalCache cache = createCache();
    createHARegionQueue("test", cache);
    Assert.assertTrue(HARegionQueue.getDispatchedMessagesMapForTesting() != null);
    HARegionQueue.stopHAServices();

    try {
      HARegionQueue.getDispatchedMessagesMapForTesting();
      fail("Expected NullPointerException to occur but did not occur");
    } catch (NullPointerException ignored) {
    }

    HARegionQueue.startHAServices(cache);
    Assert.assertTrue(HARegionQueue.getDispatchedMessagesMapForTesting() != null);
    cache.close();

    try {
      HARegionQueue.getDispatchedMessagesMapForTesting();
      fail("Expected NullPointerException to occur but did not occur");
    } catch (NullPointerException ignored) {
    }

    createCache();

    try {
      HARegionQueue.getDispatchedMessagesMapForTesting();
      fail("Expected NullPointerException to occur but did not occur");
    } catch (NullPointerException ignored) {
    }
  }
}
