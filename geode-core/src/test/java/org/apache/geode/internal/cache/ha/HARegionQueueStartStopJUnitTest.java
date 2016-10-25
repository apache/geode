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
package org.apache.geode.internal.cache.ha;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.RegionQueue;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Properties;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.fail;

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
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
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
