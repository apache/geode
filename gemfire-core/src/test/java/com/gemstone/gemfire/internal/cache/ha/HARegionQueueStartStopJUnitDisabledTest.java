/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.ha;

import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.Assert;

import junit.framework.TestCase;

/**
 * @author Mitul Bid
 *
 */
public class HARegionQueueStartStopJUnitDisabledTest extends TestCase
{

  /**
   * Creates the cache instance for the test
   * 
   * @return the cache instance
   * @throws CacheException -
   *           thrown if any exception occurs in cache creation
   */
  private Cache createCache() throws CacheException
  {
    return CacheFactory.create(DistributedSystem.connect(new Properties()));
  }

  /**
   * Creates HA region-queue object
   * 
   * @return HA region-queue object
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws CacheException
   * @throws InterruptedException
   */
  private RegionQueue createHARegionQueue(String name, Cache cache)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException
  {
    RegionQueue regionqueue =HARegionQueue.getHARegionQueueInstance(name, cache,HARegionQueue.NON_BLOCKING_HA_QUEUE, false);
    return regionqueue;
  }

  public void testStartStop()
  {
    try {
      boolean exceptionOccured = false;
      Cache cache = createCache();
      createHARegionQueue("test", cache);
      Assert
          .assertTrue(HARegionQueue.getDispatchedMessagesMapForTesting() != null);
      HARegionQueue.stopHAServices();
      try {
        HARegionQueue.getDispatchedMessagesMapForTesting();
      }
      catch (NullPointerException e) {
        exceptionOccured = true;
      }
      if (!exceptionOccured) {
        fail("Expected exception to occur but did not occur");
      }
      HARegionQueue.startHAServices((GemFireCacheImpl)cache);
      Assert
          .assertTrue(HARegionQueue.getDispatchedMessagesMapForTesting() != null);
      cache.close();
      try {
        HARegionQueue.getDispatchedMessagesMapForTesting();
      }
      catch (NullPointerException e) {
        exceptionOccured = true;
      }
      if (!exceptionOccured) {
        fail("Expected exception to occur but did not occur");
      }
      
      cache = createCache();

      try {
        HARegionQueue.getDispatchedMessagesMapForTesting();
      }
      catch (NullPointerException e) {
        exceptionOccured = true;
      }
      if (!exceptionOccured) {
        fail("Expected exception to occur but did not occur");
      }
      
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to " + e);
    }

  }
  
  
}
