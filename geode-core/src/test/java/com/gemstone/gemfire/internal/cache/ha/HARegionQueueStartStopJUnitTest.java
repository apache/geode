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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.ha;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 *
 */
@Category(IntegrationTest.class)
public class HARegionQueueStartStopJUnitTest
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
    final Properties props = new Properties();
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    return CacheFactory.create(DistributedSystem.connect(props));
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

  @Test
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
