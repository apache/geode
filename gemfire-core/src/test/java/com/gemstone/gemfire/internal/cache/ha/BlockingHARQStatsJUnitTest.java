/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.ha;

import java.io.IOException;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test runs all tests of HARegionQueueStatsJUnitTest using
 * BlockingHARegionQueue instead of HARegionQueue
 * 
 * @author Dinesh Patel
 * 
 */
@Category(IntegrationTest.class)
public class BlockingHARQStatsJUnitTest extends HARegionQueueStatsJUnitTest
{

  /**
   * Creates a BlockingHARegionQueue object.
   * 
   * @param name -
   *          name of the underlying region for region-queue
   * @return the BlockingHARegionQueue instance
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws CacheException
   * @throws InterruptedException
   */
  protected HARegionQueue createHARegionQueue(String name)
      throws IOException, ClassNotFoundException, CacheException, InterruptedException
  {
    HARegionQueue regionqueue = HARegionQueue.getHARegionQueueInstance(name,
        cache, HARegionQueue.BLOCKING_HA_QUEUE, false);
    return regionqueue;
  }

  /**
   * Creates a BlockingHARegionQueue object.
   * 
   * @param name -
   *          name of the underlying region for region-queue
   * @param attrs -
   *          attributes for the BlockingHARegionQueue
   * @return the BlockingHARegionQueue instance
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws CacheException
   * @throws InterruptedException
   */
  protected HARegionQueue createHARegionQueue(String name,
      HARegionQueueAttributes attrs) throws IOException, ClassNotFoundException, CacheException, InterruptedException
  {
    HARegionQueue regionqueue = HARegionQueue.getHARegionQueueInstance(name,
        cache,attrs, HARegionQueue.BLOCKING_HA_QUEUE, false);
    return regionqueue;
  }

}
