/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.query.internal.index;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.IndexTrackingQueryObserver;
import org.apache.geode.cache.query.internal.IndexTrackingQueryObserver.IndexInfo;
import org.apache.geode.cache.query.internal.QueryObserver;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.test.junit.categories.OQLIndexTest;
import org.apache.geode.util.internal.GeodeGlossary;

@Category({OQLIndexTest.class})
public class IndexTrackingQueryObserverJUnitTest {
  static QueryService qs;
  static Region region;
  static Index keyIndex1;
  static IndexInfo regionMap;

  private static final String queryStr = "select * from " + SEPARATOR + "portfolio where ID > 0";
  public static final int NUM_BKTS = 20;
  public static final String INDEX_NAME = "keyIndex1";

  @Before
  public void setUp() throws Exception {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "Query.VERBOSE", "true");
    CacheUtils.startCache();
    QueryObserver observer = QueryObserverHolder.setInstance(new IndexTrackingQueryObserver());
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
    QueryObserverHolder.reset();
  }

  @Test
  public void testIndexInfoOnPartitionedRegion() throws Exception {
    // Query VERBOSE has to be true for the test
    assertEquals("true", System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "Query.VERBOSE"));

    // Create Partition Region
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

    keyIndex1 =
        qs.createIndex(INDEX_NAME, IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    assertTrue(keyIndex1 instanceof PartitionedIndex);

    Query query = qs.newQuery(queryStr);

    // Inject TestHook in QueryObserver before running query.
    IndexTrackingTestHook th = new IndexTrackingTestHook(region, NUM_BKTS);
    QueryObserver observer = QueryObserverHolder.getInstance();
    assertTrue(QueryObserverHolder.hasObserver());

    ((IndexTrackingQueryObserver) observer).setTestHook(th);

    SelectResults results = (SelectResults) query.execute();

    // The query should return all elements in region.
    assertEquals(region.size(), results.size());

    // Check results size of Map.
    regionMap = th.getRegionMap();
    Collection<Integer> rslts = regionMap.getResults().values();
    int totalResults = 0;
    for (Integer i : rslts) {
      totalResults += i;
    }
    assertEquals(results.size(), totalResults);
    QueryObserverHolder.reset();
  }

  @Test
  public void testIndexInfoOnLocalRegion() throws Exception {
    // Query VERBOSE has to be true for the test
    assertEquals("true", System.getProperty(GeodeGlossary.GEMFIRE_PREFIX + "Query.VERBOSE"));

    // Create Partition Region
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

    keyIndex1 =
        qs.createIndex(INDEX_NAME, IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolio ");

    assertTrue(keyIndex1 instanceof CompactRangeIndex);

    Query query = qs.newQuery(queryStr);

    // Inject TestHook in QueryObserver before running query.
    IndexTrackingTestHook th = new IndexTrackingTestHook(region, 0);
    QueryObserver observer = QueryObserverHolder.getInstance();
    assertTrue(QueryObserverHolder.hasObserver());

    ((IndexTrackingQueryObserver) observer).setTestHook(th);

    SelectResults results = (SelectResults) query.execute();

    // The query should return all elements in region.
    assertEquals(region.size(), results.size());

    regionMap = th.getRegionMap();
    Object rslts = regionMap.getResults().get(region.getFullPath());
    assertTrue(rslts instanceof Integer);

    assertEquals(results.size(), ((Integer) rslts).intValue());
  }

}
