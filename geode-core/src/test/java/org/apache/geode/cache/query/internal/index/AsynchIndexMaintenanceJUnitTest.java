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
package org.apache.geode.cache.query.internal.index;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.WaitCriterion;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class AsynchIndexMaintenanceJUnitTest {

  private QueryService qs;

  private Region region;

  private boolean indexUsed = false;
  private volatile boolean exceptionOccured = false;

  private Set idSet ;

  private void init() throws Exception {
    idSet = new HashSet();
    CacheUtils.startCache();
    Cache cache = CacheUtils.getCache();
    region = CacheUtils.createRegion("portfolio", Portfolio.class, false);

    qs = cache.getQueryService();
  }

  @Before
  public void setUp() throws Exception {
    init();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  private int getIndexSize(Index ri) {
    if (ri instanceof RangeIndex){
      return ((RangeIndex)ri).valueToEntriesMap.size();
    } else {
      return ((CompactRangeIndex)ri).getIndexStorage().size();
    }    
  }

  @Test
  public void testIndexMaintenanceBasedOnThreshhold() throws Exception {
    System.getProperties().put(DistributionConfig.GEMFIRE_PREFIX + "AsynchIndexMaintenanceThreshold", "50");
    System.getProperties().put(DistributionConfig.GEMFIRE_PREFIX + "AsynchIndexMaintenanceInterval", "0");
    final Index ri = qs.createIndex("statusIndex",
        IndexType.FUNCTIONAL, "p.getID", "/portfolio p");
    for( int i=0; i< 49; ++i) {
      region.put(""+(i+1), new Portfolio(i+1));
      idSet.add((i+1) + "");
    }    
    //assertIndexDetailsEquals(0, getIndexSize(ri));
    region.put("50", new Portfolio(50));
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return (getIndexSize(ri) == 50);
      }
      public String description() {
        return "valueToEntriesMap never became 50";
      }
    };
    Wait.waitForCriterion(ev, 3000, 200, true);
  }
  
  @Test
  public void testIndexMaintenanceBasedOnTimeInterval() throws Exception {
    System.getProperties().put(DistributionConfig.GEMFIRE_PREFIX + "AsynchIndexMaintenanceThreshold", "-1");
    System.getProperties().put(DistributionConfig.GEMFIRE_PREFIX + "AsynchIndexMaintenanceInterval", "10000");
    final Index ri = (Index) qs.createIndex("statusIndex",
        IndexType.FUNCTIONAL, "p.getID", "/portfolio p");
    
    final int size = 5;
    for( int i=0; i<size ; ++i) {
      region.put(""+(i+1), new Portfolio(i+1));
      idSet.add((i+1) + "");
    }    

    //assertIndexDetailsEquals(0, getIndexSize(ri));

    WaitCriterion evSize = new WaitCriterion() {
      public boolean done() {
        return (getIndexSize(ri) == size);
      }
      public String description() {
        return "valueToEntriesMap never became size :" + size;
      }
    };

    Wait.waitForCriterion(evSize, 17 * 1000, 200, true);
    
    // clear region.
    region.clear();
    
    WaitCriterion evClear = new WaitCriterion() {
      public boolean done() {
        return (getIndexSize(ri) == 0);
      }
      public String description() {
        return "valueToEntriesMap never became size :" + 0;
      }
    };
    Wait.waitForCriterion(evClear, 17 * 1000, 200, true);
    
    // Add to region.
    for( int i=0; i<size ; ++i) {
      region.put(""+(i+1), new Portfolio(i+1));
      idSet.add((i+1) + "");
    }    
    //assertIndexDetailsEquals(0, getIndexSize(ri));
    Wait.waitForCriterion(evSize, 17 * 1000, 200, true);
  }
  
  @Test
  public void testIndexMaintenanceBasedOnThresholdAsZero() throws Exception {
    System.getProperties().put(DistributionConfig.GEMFIRE_PREFIX + "AsynchIndexMaintenanceThreshold", "0");
    System.getProperties().put(DistributionConfig.GEMFIRE_PREFIX + "AsynchIndexMaintenanceInterval", "60000");
    final Index ri = (Index) qs.createIndex("statusIndex",
        IndexType.FUNCTIONAL, "p.getID", "/portfolio p");
    for( int i=0; i<3 ; ++i) {
      region.put(""+(i+1), new Portfolio(i+1));
      idSet.add((i+1) + "");
    }  

    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return (getIndexSize(ri) == 3);
      }
      public String description() {
        return "valueToEntries map never became size 3";
      }
    };
    Wait.waitForCriterion(ev, 10 * 1000, 200, true);
  }
  
  @Test
  public void testNoIndexMaintenanceBasedOnNegativeThresholdAndZeroSleepTime() throws Exception {
    System.getProperties().put(DistributionConfig.GEMFIRE_PREFIX + "AsynchIndexMaintenanceThreshold", "-1");
    System.getProperties().put(DistributionConfig.GEMFIRE_PREFIX + "AsynchIndexMaintenanceInterval", "0");
    Index ri = (Index) qs.createIndex("statusIndex",
        IndexType.FUNCTIONAL, "p.getID", "/portfolio p");
    
    int size = this.getIndexSize(ri);
    
    for( int i=0; i<3 ; ++i) {
      region.put(""+(i+1), new Portfolio(i+1));
      idSet.add((i+1) + "");
    }    
    Thread.sleep(10000); // TODO: delete this sleep
  }
  
  @Test
  public void testConcurrentIndexMaintenanceForNoDeadlocks() throws Exception {
    System.getProperties().put(DistributionConfig.GEMFIRE_PREFIX + "AsynchIndexMaintenanceThreshold", "700");
    System.getProperties().put(DistributionConfig.GEMFIRE_PREFIX + "AsynchIndexMaintenanceInterval", "500");
    qs.createIndex("statusIndex",
        IndexType.FUNCTIONAL, "p.getID", "/portfolio p");
    final int TOTAL_THREADS = 25;
    final int NUM_UPDATES = 25;
    final CyclicBarrier barrier = new CyclicBarrier(TOTAL_THREADS);
    Thread threads[] = new Thread[TOTAL_THREADS];
    for (int i = 0; i < TOTAL_THREADS; ++i) {
      final int k = i;
      threads[i] = new Thread(new Runnable() {
        public void run() {
          try {
            barrier.await();
            for (int i = 0; i < NUM_UPDATES; ++i) {
              try {
                region.put("" + (k + 1), new Portfolio(k + 1));
                Thread.sleep(10);
              } catch (IllegalStateException ie) {
                // If Asynchronous index queue is full. Retry.
                if (ie.getMessage().contains("Queue full")) {
                  // retry
                  i--;
                  continue;
                }
                throw ie;
              }
            }
          }
          catch (Exception e) {
            CacheUtils.getLogger().error(e);
            exceptionOccured = true;
          }
        }
      });
    }
    for (int i = 0; i < TOTAL_THREADS; ++i) {
      threads[i].start();
    }
    try {
      for (int i = 0; i < TOTAL_THREADS; ++i) {
        ThreadUtils.join(threads[i], 30 * 1000);
      }
    }
    catch (Exception e) {
      CacheUtils.getLogger().error(e);
      exceptionOccured = true;
    }
    assertFalse(exceptionOccured);
  }  

}
