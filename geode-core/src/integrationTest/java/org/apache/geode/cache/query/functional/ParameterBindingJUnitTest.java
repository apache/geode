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
/*
 * ParameterBindingJUnitTest.java JUnit based test
 *
 * Created on March 10, 2005, 2:42 PM
 */
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.MultithreadedTester;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class ParameterBindingJUnitTest {
  String regionName = "Portfolios";

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
  }

  private Region createRegion(String regionName) {
    return CacheUtils.createRegion(regionName, Portfolio.class);
  }

  private Index createIndex(String indexName, String indexedExpression, String regionPath)
      throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    return qs.createIndex(indexName, indexedExpression, regionPath);
  }

  private void populateRegion(Region region, int numEntries) {
    IntStream.range(0, numEntries).parallel().forEach(i -> {
      region.put("" + i, new Portfolio(i));
    });
  }

  private Region createAndPopulateRegion(String regionName, int numEntries) {
    Region region = createRegion(regionName);
    populateRegion(region, numEntries);
    return region;
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  private void validateQueryWithBindParameter(String queryString, Object[] bindParameters,
      int expectedSize) throws Exception {
    Query query = CacheUtils.getQueryService().newQuery(queryString);
    Object result = query.execute(bindParameters);
    assertEquals(expectedSize, ((Collection) result).size());
  }

  @Test
  public void testBindCollectionInFromClause() throws Exception {
    int numEntries = 4;
    Region region = createAndPopulateRegion(regionName, numEntries);
    Object[] params = new Object[] {region.values()};
    validateQueryWithBindParameter("SELECT DISTINCT * FROM $1 ", params, numEntries);
  }

  @Test
  public void testBindArrayInFromClause() throws Exception {
    int numEntries = 4;
    Region region = createAndPopulateRegion(regionName, numEntries);
    Object[] params = new Object[] {region.values().toArray()};
    validateQueryWithBindParameter("SELECT DISTINCT * FROM $1 ", params, numEntries);
  }

  @Test
  public void testBindMapInFromClause() throws Exception {
    int numEntries = 4;
    Region region = createAndPopulateRegion(regionName, numEntries);
    Map map = new HashMap();
    for (final Object o : region.entrySet()) {
      Region.Entry entry = (Region.Entry) o;
      map.put(entry.getKey(), entry.getValue());
    }
    Object[] params = new Object[] {map};
    validateQueryWithBindParameter("SELECT DISTINCT * FROM $1 ", params, numEntries);
  }

  @Test
  public void testBindRegionInFromClause() throws Exception {
    int numEntries = 4;
    Region region = createAndPopulateRegion(regionName, numEntries);
    Object[] params = new Object[] {region};
    validateQueryWithBindParameter("SELECT DISTINCT * FROM $1 ", params, numEntries);
  }

  @Test
  public void testStringBindValueAsMethodParameter() throws Exception {
    int numEntries = 4;
    Region region = createAndPopulateRegion(regionName, numEntries);
    Query query = CacheUtils.getQueryService()
        .newQuery("SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where status.equals($1)");
    Object[] params = new Object[] {"active"};
    validateQueryWithBindParameter(
        "SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where status.equals($1)",
        params, 2);
  }

  @Test
  public void testBindStringAsBindParameter() throws Exception {
    int numEntries = 4;
    Region region = createAndPopulateRegion(regionName, numEntries);
    Query query = CacheUtils.getQueryService()
        .newQuery("SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where status = $1");
    Object[] params = new Object[] {"active"};
    validateQueryWithBindParameter(
        "SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where status = $1", params,
        2);
  }

  @Test
  public void testBindInt() throws Exception {
    int numEntries = 4;
    Region region = createAndPopulateRegion(regionName, numEntries);
    Object[] params = new Object[] {1};
    validateQueryWithBindParameter(
        "SELECT DISTINCT * FROM " + SEPARATOR + "Portfolios where ID = $1", params, 1);
  }

  @Test
  public void testMultithreadedBindUsingSameQueryObject() throws Exception {
    int numObjects = 10000;
    Region region = createAndPopulateRegion("Portfolios", numObjects);
    createIndex("Status Index", "status", SEPARATOR + "Portfolios");
    final Query query =
        CacheUtils.getQueryService()
            .newQuery("SELECT * FROM " + SEPARATOR + "Portfolios where status like $1");
    final Object[] bindParam = new Object[] {"%a%"};
    Collection<Callable> callables = new ConcurrentLinkedQueue<>();
    IntStream.range(0, 1000).parallel().forEach(i -> {
      callables.add(() -> {
        return query.execute(bindParam);
      });
    });
    Collection<Object> results = MultithreadedTester.runMultithreaded(callables);
    results.forEach(result -> {
      assertTrue(result.getClass().getName() + " was not an expected result",
          result instanceof Collection);
      assertEquals(numObjects, ((Collection) result).size());
    });
  }
}
