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
package com.gemstone.gemfire.cache.query.internal.index;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager.IndexUpdaterThread;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.logging.LoggingThreadGroup;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.MCAST_PORT;
import static org.junit.Assert.*;

/**
 * Test create a region (Replicated OR Partitioned) and sets index maintenance
 * Asynchronous so that {@link IndexManager} starts a new thread for index
 * maintenance when region is populated. This test verifies that after cache
 * close {@link IndexUpdaterThread} is shutdown for each region
 * (Replicated/Bucket).
 * 
 * 
 */
@Category(IntegrationTest.class)
public class AsyncIndexUpdaterThreadShutdownJUnitTest {

  String name = "PR_with_Async_Index";

  @Test
  public void testAsyncIndexUpdaterThreadShutdownForRR() {
    Cache cache = new CacheFactory().set(MCAST_PORT, "0").create();

    RegionFactory rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.setIndexMaintenanceSynchronous(false);
    Region localRegion = rf.create(name);
    
    assertNotNull("Region ref null", localRegion);
    
    try {
      cache.getQueryService().createIndex("idIndex", "ID", "/"+name);
    } catch (Exception e) {
      cache.close();
      e.printStackTrace();
      fail("Index creation failed");
    }
    
    for (int i=0; i<500; i++) {
      localRegion.put(i, new Portfolio(i));
    }
    
    GemFireCacheImpl internalCache = (GemFireCacheImpl)cache;
    // Don't disconnect distributed system yet to keep system thread groups alive.
    internalCache.close("Normal disconnect", null, false, false);

    // Get Asynchronous index updater thread group from Distributed System.
    ThreadGroup indexUpdaterThreadGroup = LoggingThreadGroup.getThreadGroup("QueryMonitor Thread Group");
    
    assertEquals(0, indexUpdaterThreadGroup.activeCount());
    
    internalCache.getSystem().disconnect();
    
  }

  @Test
  public void testAsyncIndexUpdaterThreadShutdownForPR() {
    Cache cache = new CacheFactory().set(MCAST_PORT, "0").create();

    RegionFactory rf = cache.createRegionFactory(RegionShortcut.PARTITION);
    rf.setIndexMaintenanceSynchronous(false);
    Region localRegion = rf.create(name);
    
    assertNotNull("Region ref null", localRegion);
    
    try {
      cache.getQueryService().createIndex("idIndex", "ID", "/"+name);
    } catch (Exception e) {
      cache.close();
      e.printStackTrace();
      fail("Index creation failed");
    }
    
    for (int i=0; i<500; i++) {
      localRegion.put(i, new Portfolio(i));
    }

    GemFireCacheImpl internalCache = (GemFireCacheImpl)cache;
    // Don't disconnect distributed system yet to keep system thread groups alive.
    internalCache.close("Normal disconnect", null, false, false);

    // Get Asynchronous index updater thread group from Distributed System.
    ThreadGroup indexUpdaterThreadGroup = LoggingThreadGroup.getThreadGroup("QueryMonitor Thread Group");
    
    assertEquals(0, indexUpdaterThreadGroup.activeCount());

    internalCache.getSystem().disconnect();
  }
}
