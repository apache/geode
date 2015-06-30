/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.internal.cache.ha;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import junit.framework.Assert;

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
 * 
 * @author Mitul Bid
 *  
 */
@Category(IntegrationTest.class)
public class HARegionJUnitTest
{

  /**
   * create the cache
   */
  @Before
  public void setUp() throws Exception
  {
    cache = createCache();
  }

  /**
   * close the cache in tear down
   */
  @After
  public void tearDown() throws Exception
  {
    cache.close();
  }

  /**
   * cache
   */
  private Cache cache = null;

  /**
   * 
   * create the cache
   * 
   * @return @throws
   *         TimeoutException
   * @throws CacheWriterException
   * @throws CapacityControllerException
   * @throws GatewayException
   * @throws CacheExistsException
   * @throws RegionExistsException
   */
  private Cache createCache() throws TimeoutException, CacheWriterException,
       GatewayException, CacheExistsException,
      RegionExistsException
  {
    return new CacheFactory().set("mcast-port",  "0").create();
  }

  /**
   * create the HARegion
   * 
   * @return @throws
   *         TimeoutException
   * @throws CacheWriterException
   * @throws CapacityControllerException
   * @throws GatewayException
   * @throws CacheExistsException
   * @throws RegionExistsException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  private Region createHARegion() throws TimeoutException,
      CacheWriterException,  GatewayException,
      CacheExistsException, RegionExistsException, IOException,
      ClassNotFoundException
  {
    AttributesFactory factory = new AttributesFactory();
    factory.setDataPolicy(DataPolicy.REPLICATE);
    factory.setScope(Scope.DISTRIBUTED_ACK);
    ExpirationAttributes ea = new ExpirationAttributes(2000,
        ExpirationAction.LOCAL_INVALIDATE);
    factory.setStatisticsEnabled(true);
    ;
    factory.setCacheListener(new CacheListenerAdapter() {
      public void afterInvalidate(EntryEvent event)
      {
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
   *  
   */
  @Test
  public void testRegionCreation()
  {
    try {
      createHARegion();
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to " + e);
    }
  }

  /**
   * test no exception being thrown while put is being done on an HARegion
   *  
   */
  @Test
  public void testPut()
  {
    try {
      Region region = createHARegion();
      region.put("key1", "value1");
      Assert.assertEquals(region.get("key1"), "value1");
    }
    catch (Exception e) {
      fail("put failed due to " + e);
    }
  }

  /**
   * test no exception being thrown while doing a localDestroy on a HARegion
   *  
   */
  @Test
  public void testLocalDestroy()
  {
    try {
      Region region = createHARegion();
      region.put("key1", "value1");
      region.localDestroy("key1");
      Assert.assertEquals(region.get("key1"), null);
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("put failed due to " + e);
    }
  }
  /**
   * Test to verify event id exists when evict destroy happens.
   * 
   */
  @Test
  public void testEventIdSetForEvictDestroy()
  { 
    try{
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
    catch (Exception e) {      
    }
    
    
  }

}
