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

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.statistics.StatisticsClockFactory.disabledClock;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheExistsException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.GatewayException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.util.CacheListenerAdapter;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.test.junit.categories.ClientSubscriptionTest;

/**
 * Test verifies the properties of a HARegion which allows localPuts and localDestroys on a
 * MirroredRegion
 */
@Category({ClientSubscriptionTest.class})
public class HARegionJUnitTest {

  /**
   * cache
   */
  private Cache cache = null;

  /**
   * create the cache
   */
  @Before
  public void setUp() throws Exception {
    cache = createCache();
  }

  /**
   * close the cache in tear down
   */
  @After
  public void tearDown() throws Exception {
    cache.close();
  }

  /**
   * create the cache
   */
  private Cache createCache() throws TimeoutException, CacheWriterException, GatewayException,
      CacheExistsException, RegionExistsException {
    return new CacheFactory().set(MCAST_PORT, "0").create();
  }

  /**
   * create the HARegion
   */
  private Region createHARegion() throws TimeoutException, CacheWriterException, GatewayException,
      CacheExistsException, RegionExistsException, IOException, ClassNotFoundException {
    AttributesFactory factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setScope(Scope.DISTRIBUTED_ACK);
    ExpirationAttributes ea = new ExpirationAttributes(2000, ExpirationAction.LOCAL_INVALIDATE);
    factory.setStatisticsEnabled(true);
    factory.setCacheListener(new CacheListenerAdapter() {
      @Override
      public void afterInvalidate(EntryEvent event) {}
    });
    RegionAttributes ra = factory.create();
    Region region =
        HARegion.getInstance("HARegionJUnitTest_region", (GemFireCacheImpl) cache, null, ra,
            disabledClock());
    region.getAttributesMutator().setEntryTimeToLive(ea);
    return region;
  }

  /**
   * test no exception being thrown while creating an HARegion
   */
  @Test
  public void testRegionCreation() throws Exception {
    createHARegion();
  }

  /**
   * test no exception being thrown while put is being done on an HARegion
   */
  @Test
  public void testPut() throws Exception {
    Region region = createHARegion();
    region.put("key1", "value1");
    assertEquals(region.get("key1"), "value1");
  }

  /**
   * test no exception being thrown while doing a localDestroy on a HARegion
   */
  @Test
  public void testLocalDestroy() throws Exception {
    Region region = createHARegion();
    region.put("key1", "value1");
    region.localDestroy("key1");
    assertEquals(region.get("key1"), null);
  }

  /**
   * Test to verify event id exists when evict destroy happens.
   */
  @Test
  public void testEventIdSetForEvictDestroy() throws Exception {
    AttributesFactory factory = new AttributesFactory();

    factory.setCacheListener(new CacheListenerAdapter() {
      @Override
      public void afterDestroy(EntryEvent event) {
        assertTrue("eventId has not been set for " + event,
            ((EntryEventImpl) event).getEventId() != null);
      }
    });

    EvictionAttributes evAttr =
        EvictionAttributes.createLRUEntryAttributes(1, EvictionAction.LOCAL_DESTROY);
    factory.setEvictionAttributes(evAttr);

    RegionAttributes attrs = factory.createRegionAttributes();
    Region region = cache.createVMRegion("TEST_REGION", attrs);
    region.put("key1", "value1");
    region.put("key2", "value2");
  }
}
