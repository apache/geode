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

import java.io.IOException;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test runs all tests of HARegionQueueStatsJUnitTest using
 * BlockingHARegionQueue instead of HARegionQueue
 * 
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
