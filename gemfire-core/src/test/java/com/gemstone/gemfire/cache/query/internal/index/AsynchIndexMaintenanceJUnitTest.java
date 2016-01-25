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

import java.util.concurrent.CyclicBarrier;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.DistributedTestCase.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import junit.framework.TestCase;

/**
 * @author Asif
 *
 */
@Category(IntegrationTest.class)
public class AsynchIndexMaintenanceJUnitTest {
  private QueryService qs;

  protected Region region;

  protected boolean indexUsed = false;
  protected volatile boolean exceptionOccured = false; 

  private Set idSet ;

  private void init() {
    idSet = new HashSet();
    try {
      CacheUtils.startCache();
      Cache cache = CacheUtils.getCache();
      region = CacheUtils.createRegion("portfolio", Portfolio.class, false);      
      
      qs = cache.getQueryService();

    }
    catch (Exception e) {
      e.printStackTrace();
    }

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
    System.getProperties().put("gemfire.AsynchIndexMaintenanceThreshold", "50");
    System.getProperties().put("gemfire.AsynchIndexMaintenanceInterval", "0");
    final Index ri = qs.createIndex("statusIndex",
        IndexType.FUNCTIONAL, "p.getID", "/portfolio p");
    for( int i=0; i< 49; ++i) {
      region.put(""+(i+1), new Portfolio(i+1));
      idSet.add((i+1) + "");
    }    
    //assertEquals(0, getIndexSize(ri));
    region.put("50", new Portfolio(50));
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        return (getIndexSize(ri) == 50);
      }
      public String description() {
        return "valueToEntriesMap never became 50";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 3000, 200, true);
  }
  
  @Test
  public void testIndexMaintenanceBasedOnTimeInterval() throws Exception {
    System.getProperties().put("gemfire.AsynchIndexMaintenanceThreshold", "-1");
    System.getProperties().put("gemfire.AsynchIndexMaintenanceInterval", "10000");
    final Index ri = (Index) qs.createIndex("statusIndex",
        IndexType.FUNCTIONAL, "p.getID", "/portfolio p");
    
    final int size = 5;
    for( int i=0; i<size ; ++i) {
      region.put(""+(i+1), new Portfolio(i+1));
      idSet.add((i+1) + "");
    }    

    //assertEquals(0, getIndexSize(ri));

    WaitCriterion evSize = new WaitCriterion() {
      public boolean done() {
        return (getIndexSize(ri) == size);
      }
      public String description() {
        return "valueToEntriesMap never became size :" + size;
      }
    };

    DistributedTestCase.waitForCriterion(evSize, 17 * 1000, 200, true);
    
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
    DistributedTestCase.waitForCriterion(evClear, 17 * 1000, 200, true);
    
    // Add to region.
    for( int i=0; i<size ; ++i) {
      region.put(""+(i+1), new Portfolio(i+1));
      idSet.add((i+1) + "");
    }    
    //assertEquals(0, getIndexSize(ri));
    DistributedTestCase.waitForCriterion(evSize, 17 * 1000, 200, true);
  }
  
  @Test
  public void testIndexMaintenanceBasedOnThresholdAsZero() throws Exception {
    System.getProperties().put("gemfire.AsynchIndexMaintenanceThreshold", "0");
    System.getProperties().put("gemfire.AsynchIndexMaintenanceInterval", "60000");
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
    DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);
  }
  
  @Test
  public void testNoIndexMaintenanceBasedOnNegativeThresholdAndZeroSleepTime() throws Exception {
    System.getProperties().put("gemfire.AsynchIndexMaintenanceThreshold", "-1");
    System.getProperties().put("gemfire.AsynchIndexMaintenanceInterval", "0");
    Index ri = (Index) qs.createIndex("statusIndex",
        IndexType.FUNCTIONAL, "p.getID", "/portfolio p");
    
    int size = this.getIndexSize(ri);
    
    for( int i=0; i<3 ; ++i) {
      region.put(""+(i+1), new Portfolio(i+1));
      idSet.add((i+1) + "");
    }    
    Thread.sleep(10000);
    //assertEquals(0, this.getIndexSize(ri));    
        
  }
  
  @Test
  public void testConcurrentIndexMaintenanceForNoDeadlocks() throws Exception {
    System.getProperties().put("gemfire.AsynchIndexMaintenanceThreshold", "700");
    System.getProperties().put("gemfire.AsynchIndexMaintenanceInterval", "500");
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
        DistributedTestCase.join(threads[i], 30 * 1000, null);
      }
    }
    catch (Exception e) {
      CacheUtils.getLogger().error(e);
      exceptionOccured = true;
    }
    assertFalse(exceptionOccured);
  }  

}
