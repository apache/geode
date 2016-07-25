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

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.IndexTrackingQueryObserver;
import com.gemstone.gemfire.cache.query.internal.IndexTrackingQueryObserver.IndexInfo;
import com.gemstone.gemfire.cache.query.internal.QueryObserver;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.index.IndexTrackingQueryObserverDUnitTest.IndexTrackingTestHook;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 */
@Category(IntegrationTest.class)
public class IndexTrackingQueryObserverJUnitTest {
  static QueryService qs;
  static Region region;
  static Index keyIndex1;
  static IndexInfo regionMap;
  
  private static final String queryStr = "select * from /portfolio where ID > 0";
  public static final int NUM_BKTS = 20;
  public static final String INDEX_NAME = "keyIndex1"; 
    
  @Before
  public void setUp() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "Query.VERBOSE", "true");
    CacheUtils.startCache();
    QueryObserver observer = QueryObserverHolder.setInstance(new IndexTrackingQueryObserver());
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
    QueryObserverHolder.reset();
  }

  @Test
  public void testIndexInfoOnPartitionedRegion() throws Exception{
    //Query VERBOSE has to be true for the test
    assertEquals("true", System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "Query.VERBOSE"));
    
    //Create Partition Region
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    paf.setTotalNumBuckets(NUM_BKTS);
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME,
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof PartitionedIndex);

    Query query = qs.newQuery(queryStr);

    //Inject TestHook in QueryObserver before running query.
    IndexTrackingTestHook th = new IndexTrackingTestHook(region, NUM_BKTS);
    QueryObserver observer = QueryObserverHolder.getInstance();
    assertTrue(QueryObserverHolder.hasObserver());
    
    ((IndexTrackingQueryObserver)observer).setTestHook(th);
    
    SelectResults results = (SelectResults)query.execute();
    
    //The query should return all elements in region.
    assertEquals(region.size(), results.size());
    
        //Check results size of Map.
    regionMap = ((IndexTrackingTestHook)th).getRegionMap();
    Collection<Integer> rslts = regionMap.getResults().values();
    int totalResults = 0;
    for (Integer i : rslts){
      totalResults += i.intValue();
    }
    assertEquals(results.size(), totalResults);
    QueryObserverHolder.reset();    
  }

  @Test
  public void testIndexInfoOnLocalRegion() throws Exception{
    //Query VERBOSE has to be true for the test
    assertEquals("true", System.getProperty(DistributionConfig.GEMFIRE_PREFIX + "Query.VERBOSE"));
    
    //Create Partition Region
    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);

    region = CacheUtils.createRegion("portfolio", af.create(), false);
    if (region.size() == 0) {
      for (int i = 1; i <= 100; i++) {
        region.put(Integer.toString(i), new Portfolio(i, i));
      }
    }
    assertEquals(100, region.size());
    qs = CacheUtils.getQueryService();
    
    keyIndex1 = (IndexProtocol) qs.createIndex(INDEX_NAME,
        IndexType.FUNCTIONAL, "ID", "/portfolio ");
    
    assertTrue(keyIndex1 instanceof CompactRangeIndex);

    Query query = qs.newQuery(queryStr);

    //Inject TestHook in QueryObserver before running query.
    IndexTrackingTestHook th = new IndexTrackingTestHook(region, 0);
    QueryObserver observer = QueryObserverHolder.getInstance();
    assertTrue(QueryObserverHolder.hasObserver());
    
    ((IndexTrackingQueryObserver)observer).setTestHook(th);
    
    SelectResults results = (SelectResults)query.execute();
    
    //The query should return all elements in region.
    assertEquals(region.size(), results.size());
    
    regionMap = ((IndexTrackingTestHook)th).getRegionMap();
    Object rslts = regionMap.getResults().get(region.getFullPath());
    assertTrue(rslts instanceof Integer);
    
    assertEquals(results.size(), ((Integer)rslts).intValue());
  }

}
