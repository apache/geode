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
package com.gemstone.gemfire.internal.cache.ha;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheExistsException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.ExpirationAction;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.GatewayException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test verifies the properties of a HARegion which allows localPuts and
 * localDestroys on a MirroredRegion
 */
@Category(IntegrationTest.class)
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
  private Cache createCache() throws TimeoutException, CacheWriterException, GatewayException, CacheExistsException, RegionExistsException {
    return new CacheFactory().set(MCAST_PORT, "0").create();
  }

  /**
   * create the HARegion
   */
  private Region createHARegion() throws TimeoutException, CacheWriterException,  GatewayException, CacheExistsException, RegionExistsException, IOException, ClassNotFoundException {
    AttributesFactory factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setScope(Scope.DISTRIBUTED_ACK);
    ExpirationAttributes ea = new ExpirationAttributes(2000,
        ExpirationAction.LOCAL_INVALIDATE);
    factory.setStatisticsEnabled(true);
    ;
    factory.setCacheListener(new CacheListenerAdapter() {
      @Override
      public void afterInvalidate(EntryEvent event) {
      }
    });
    RegionAttributes ra = factory.create();
    Region region = HARegion.getInstance("HARegionJUnitTest_region", (GemFireCacheImpl)cache,
        null, ra);
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

    factory.setCacheListener(new CacheListenerAdapter(){
      public void afterDestroy(EntryEvent event){
        assertTrue("eventId has not been set for "+ event, ((EntryEventImpl)event).getEventId() != null);
      }
     });

    EvictionAttributes evAttr = EvictionAttributes.createLRUEntryAttributes(1,EvictionAction.LOCAL_DESTROY);
    factory.setEvictionAttributes(evAttr);

    RegionAttributes attrs = factory.createRegionAttributes();
    Region region = cache.createVMRegion("TEST_REGION", attrs);
    region.put("key1", "value1");
    region.put("key2", "value2");
  }
}
