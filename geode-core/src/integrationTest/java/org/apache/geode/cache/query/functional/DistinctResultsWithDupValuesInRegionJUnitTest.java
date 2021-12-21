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
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.util.internal.GeodeGlossary;

@Category({OQLQueryTest.class})
public class DistinctResultsWithDupValuesInRegionJUnitTest {

  private static final String regionName = "test";
  private final int numElem = 100;

  public DistinctResultsWithDupValuesInRegionJUnitTest() {}

  @Before
  public void setUp() throws Exception {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "Query.VERBOSE", "true");
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  private static final String[] queries = new String[] {
      "select DISTINCT * from " + SEPARATOR
          + "test p, p.positions.values pos where p.ID> 0 OR p.status = 'active' OR pos.secId = 'IBM' order by p.ID",
      "select DISTINCT * from " + SEPARATOR
          + "test p, p.positions.values pos where p.ID> 0 OR p.status = 'active' OR pos.secId = 'IBM'",
      "select DISTINCT * from " + SEPARATOR
          + "test p, p.positions.values pos where p.ID> 0 OR p.status = 'active' order by p.ID",
      "select DISTINCT * from " + SEPARATOR
          + "test p, p.positions.values pos where p.ID> 0 order by p.ID",
      "select DISTINCT p.ID, p.status, pos.secId from " + SEPARATOR
          + "test p, p.positions.values pos where p.ID> 0 OR p.status = 'active' OR pos.secId = 'IBM' order by p.ID",
      "select DISTINCT p.ID, p.status, pos.secId, pos.secType from " + SEPARATOR
          + "test p, p.positions.values pos where p.ID> 0 OR p.status = 'active' OR pos.secId = 'IBM' order by p.ID",};

  private static final String[] moreQueries = new String[] {
      "select DISTINCT p.ID, p.status from " + SEPARATOR
          + "test p, p.positions.values pos where p.ID> 0 OR p.status = 'active' order by p.ID",};

  /**
   * Test on Local Region data
   */
  @Test
  public void testQueriesOnLocalRegion() {
    Cache cache = CacheUtils.getCache();

    createLocalRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem * 2, cache.getRegion(regionName).size());

    QueryService queryService = cache.getQueryService();
    Query query1 = null;
    try {
      for (String queryStr : queries) {
        query1 = queryService.newQuery(queryStr);

        SelectResults result1 = (SelectResults) query1.execute();

        assertEquals(queryStr, numElem * 2, result1.size());
        verifyDistinctResults(result1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Query " + query1 + " Execution Failed!");
    }
    // Destroy current Region for other tests
    cache.getRegion(regionName).destroyRegion();
  }

  /**
   * Test on Replicated Region data
   */
  @Test
  public void testQueriesOnReplicatedRegion() {
    Cache cache = CacheUtils.getCache();

    createReplicatedRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem * 2, cache.getRegion(regionName).size());

    QueryService queryService = cache.getQueryService();
    Query query1 = null;
    try {
      for (String queryStr : queries) {
        query1 = queryService.newQuery(queryStr);

        SelectResults result1 = (SelectResults) query1.execute();

        assertEquals(queryStr, numElem * 2, result1.size());
        verifyDistinctResults(result1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Query " + query1 + " Execution Failed!");
    }

    // Destroy current Region for other tests
    cache.getRegion(regionName).destroyRegion();
  }

  /**
   * Test on Partitioned Region data
   */
  @Test
  public void testQueriesOnPartitionedRegion() {
    Cache cache = CacheUtils.getCache();

    createPartitionedRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem * 2, cache.getRegion(regionName).size());

    QueryService queryService = cache.getQueryService();
    Query query1 = null;
    try {
      for (String queryStr : queries) {
        query1 = queryService.newQuery(queryStr);

        SelectResults result1 = (SelectResults) query1.execute();

        assertEquals(queryStr, numElem * 2, result1.size());
        verifyDistinctResults(result1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Query " + query1 + " Execution Failed!");
    }

    // Destroy current Region for other tests
    cache.getRegion(regionName).destroyRegion();
  }

  /**
   * Test on Replicated Region data
   */
  @Test
  public void testQueriesOnReplicatedRegionWithSameProjAttr() {
    Cache cache = CacheUtils.getCache();

    createReplicatedRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem * 2, cache.getRegion(regionName).size());

    QueryService queryService = cache.getQueryService();
    Query query1 = null;
    try {
      for (String queryStr : moreQueries) {
        query1 = queryService.newQuery(queryStr);

        SelectResults result1 = (SelectResults) query1.execute();

        assertEquals(queryStr, numElem, result1.size());
        verifyDistinctResults(result1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Query " + query1 + " Execution Failed!");
    }

    // Destroy current Region for other tests
    cache.getRegion(regionName).destroyRegion();
  }

  /**
   * Test on Partitioned Region data
   */
  @Test
  public void testQueriesOnPartitionedRegionWithSameProjAttr() {
    Cache cache = CacheUtils.getCache();

    createPartitionedRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem * 2, cache.getRegion(regionName).size());

    QueryService queryService = cache.getQueryService();
    Query query1 = null;
    try {
      for (String queryStr : moreQueries) {
        query1 = queryService.newQuery(queryStr);

        SelectResults result1 = (SelectResults) query1.execute();

        assertEquals(queryStr, numElem, result1.size());
        verifyDistinctResults(result1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Query " + query1 + " Execution Failed!");
    }

    // Destroy current Region for other tests
    cache.getRegion(regionName).destroyRegion();
  }

  /**
   * Test on Replicated Region data
   */
  @Test
  public void testQueriesOnReplicatedRegionWithNullProjAttr() {
    Cache cache = CacheUtils.getCache();

    createLocalRegionWithNullValues();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem * 2, cache.getRegion(regionName).size());

    QueryService queryService = cache.getQueryService();
    Query query1 = null;
    try {
      for (String queryStr : moreQueries) {
        query1 = queryService.newQuery(queryStr);

        SelectResults result1 = (SelectResults) query1.execute();
        cache.getLogger().fine(result1.asList().toString());
        assertEquals(queryStr, numElem, result1.size());
        verifyDistinctResults(result1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Query " + query1 + " Execution Failed!");
    }

    // Destroy current Region for other tests
    cache.getRegion(regionName).destroyRegion();
  }

  /**
   * Test on Partitioned Region data
   */
  @Test
  public void testQueriesOnPartitionedRegionWithNullProjAttr() {
    Cache cache = CacheUtils.getCache();

    createPartitionedRegionWithNullValues();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem * 2, cache.getRegion(regionName).size());

    QueryService queryService = cache.getQueryService();
    Query query1 = null;
    try {
      for (String queryStr : moreQueries) {
        query1 = queryService.newQuery(queryStr);

        SelectResults result1 = (SelectResults) query1.execute();
        cache.getLogger().fine(result1.asList().toString());
        assertEquals(queryStr, numElem + 5 /* Check createPartitionedRegionWithNullValues() */,
            result1.size());
        verifyDistinctResults(result1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Query " + query1 + " Execution Failed!");
    }

    // Destroy current Region for other tests
    cache.getRegion(regionName).destroyRegion();
  }

  /**
   * Test on Local Region data
   */
  @Test
  public void testQueriesOnLocalRegionWithIndex() {
    Cache cache = CacheUtils.getCache();

    createLocalRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem * 2, cache.getRegion(regionName).size());

    QueryService queryService = cache.getQueryService();
    Query query1 = null;
    try {
      queryService.createIndex("idIndex", "p.ID", SEPARATOR + regionName + " p");
      for (String queryStr : queries) {
        query1 = queryService.newQuery(queryStr);

        SelectResults result1 = (SelectResults) query1.execute();

        assertEquals(queryStr, numElem * 2, result1.size());
        verifyDistinctResults(result1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Query " + query1 + " Execution Failed!");
    }
    // Destroy current Region for other tests
    cache.getRegion(regionName).destroyRegion();
  }

  /**
   * Test on Replicated Region data
   */
  @Test
  public void testQueriesOnReplicatedRegionWithIndex() {
    Cache cache = CacheUtils.getCache();

    createReplicatedRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem * 2, cache.getRegion(regionName).size());

    QueryService queryService = cache.getQueryService();
    Query query1 = null;
    try {
      queryService.createIndex("idIndex", "p.ID", SEPARATOR + regionName + " p");
      for (String queryStr : queries) {
        query1 = queryService.newQuery(queryStr);

        SelectResults result1 = (SelectResults) query1.execute();

        assertEquals(queryStr, numElem * 2, result1.size());
        verifyDistinctResults(result1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Query " + query1 + " Execution Failed!");
    }

    // Destroy current Region for other tests
    cache.getRegion(regionName).destroyRegion();
  }

  /**
   * Test on Partitioned Region data
   */
  @Test
  public void testQueriesOnPartitionedRegionWithIndex() {
    Cache cache = CacheUtils.getCache();

    createPartitionedRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem * 2, cache.getRegion(regionName).size());

    QueryService queryService = cache.getQueryService();
    Query query1 = null;
    try {
      queryService.createIndex("idIndex", "p.ID", SEPARATOR + regionName + " p");
      for (String queryStr : queries) {
        query1 = queryService.newQuery(queryStr);

        SelectResults result1 = (SelectResults) query1.execute();

        assertEquals(queryStr, numElem * 2, result1.size());
        verifyDistinctResults(result1);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Query " + query1 + " Execution Failed!");
    }

    // Destroy current Region for other tests
    cache.getRegion(regionName).destroyRegion();
  }

  private void verifyDistinctResults(SelectResults result1) {
    List results = result1.asList();
    int size = results.size();
    for (int i = 0; i < size; i++) {
      Object obj = results.remove(0);
      if (results.contains(obj)) {
        fail("Non-distinct values found in the resultset for object: " + obj);
      }
    }
  }

  private void createLocalRegion() {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.NORMAL);
    RegionAttributes regionAttributes = attributesFactory.create();
    Region region = cache.createRegion(regionName, regionAttributes);

    for (int i = 1; i <= numElem; i++) {
      Portfolio obj = new Portfolio(i);
      region.put(i, obj);
      region.put(i + numElem, obj);
      CacheUtils.log(obj);
    }
  }

  private void createPartitionedRegion() {
    Cache cache = CacheUtils.getCache();
    PartitionAttributesFactory prAttFactory = new PartitionAttributesFactory();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setPartitionAttributes(prAttFactory.create());
    RegionAttributes regionAttributes = attributesFactory.create();
    Region region = cache.createRegion(regionName, regionAttributes);

    for (int i = 1; i <= numElem; i++) {
      Portfolio obj = new Portfolio(i);
      region.put(i, obj);
      region.put(i + numElem, obj);
      CacheUtils.log(obj);
    }
  }

  private void createLocalRegionWithNullValues() {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.NORMAL);
    RegionAttributes regionAttributes = attributesFactory.create();
    Region region = cache.createRegion(regionName, regionAttributes);

    for (int i = 1; i <= numElem; i++) {
      Portfolio obj = new Portfolio(i);
      region.put(i, obj);
      if (i % (numElem / 5) == 0) {
        obj.status = null;
      }
      region.put(i + numElem, obj);
      CacheUtils.log(obj);
    }
  }

  private void createPartitionedRegionWithNullValues() {
    Cache cache = CacheUtils.getCache();
    PartitionAttributesFactory prAttFactory = new PartitionAttributesFactory();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setPartitionAttributes(prAttFactory.create());
    RegionAttributes regionAttributes = attributesFactory.create();
    Region region = cache.createRegion(regionName, regionAttributes);

    for (int i = 1; i <= numElem; i++) {
      Portfolio obj = new Portfolio(i);
      region.put(i, obj);
      if (i % (numElem / 5) == 0) {
        obj.status = null;
      }
      region.put(i + numElem, obj);
      CacheUtils.log(obj);
    }
  }

  private void createReplicatedRegion() {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes regionAttributes = attributesFactory.create();
    Region region = cache.createRegion(regionName, regionAttributes);

    for (int i = 1; i <= numElem; i++) {
      Portfolio obj = new Portfolio(i);
      region.put(i, obj);
      region.put(i + numElem, obj);
      CacheUtils.log(obj);
    }
  }
}
