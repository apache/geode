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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

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
public class SelectToDateJUnitTest {

  private static final String regionName = "test";
  private static final int numElem = 120;
  private static final String format = "MMddyyyyHHmmss";
  private static final String mayDate = "05202012100559";
  private static final int numMonthsBeforeMay = 4;
  private static final int numMonthsAfterMay = 7;
  private static final int numElementsExpectedPerMonth = numElem * 2 / 12;

  @Before
  public void setUp() throws Exception {
    System.setProperty(GeodeGlossary.GEMFIRE_PREFIX + "Query.VERBOSE", "true");
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  private static final String[] toDateQueries = new String[] {
      "select * from " + SEPARATOR + "test p where p.createDate = to_date('" + mayDate + "', '"
          + format + "')",
      "select * from " + SEPARATOR + "test p where p.createDate < to_date('" + mayDate + "', '"
          + format + "')",
      "select * from " + SEPARATOR + "test p where p.createDate > to_date('" + mayDate + "', '"
          + format + "')",
      "select * from " + SEPARATOR + "test p where p.createDate <= to_date('" + mayDate + "', '"
          + format + "')",
      "select * from " + SEPARATOR + "test p where p.createDate >= to_date('" + mayDate + "', '"
          + format + "')"};

  // the test will be validating against the May date, so expected values revolve around month of
  // May
  private static final int[] toDateExpectedResults =
      new int[] {numElementsExpectedPerMonth, numMonthsBeforeMay * numElementsExpectedPerMonth,
          numMonthsAfterMay * numElementsExpectedPerMonth,
          (numMonthsBeforeMay + 1) * numElementsExpectedPerMonth,
          (numMonthsAfterMay + 1) * numElementsExpectedPerMonth};

  private static final String[] projectionQueries = new String[] {
      "select p.createDate from " + SEPARATOR + "test p where p.createDate = to_date('" + mayDate
          + "', '" + format
          + "')",
      "select p.createDate from " + SEPARATOR + "test p where p.createDate < to_date('" + mayDate
          + "', '" + format
          + "')",
      "select p.createDate from " + SEPARATOR + "test p where p.createDate > to_date('" + mayDate
          + "', '" + format
          + "')",
      "select p.createDate from " + SEPARATOR + "test p where p.createDate <= to_date('" + mayDate
          + "', '" + format
          + "')",
      "select p.createDate from " + SEPARATOR + "test p where p.createDate >= to_date('" + mayDate
          + "', '" + format
          + "')",};

  private void executeQueryTest(Cache cache, String[] queries, int[] expectedResults) {
    CacheUtils.log("********Execute Query Test********");
    QueryService queryService = cache.getQueryService();
    Query query = null;
    String queryString = null;
    int numQueries = queries.length;
    try {
      for (int i = 0; i < numQueries; i++) {
        queryString = queries[0];
        query = queryService.newQuery(queries[0]);
        SelectResults result = (SelectResults) query.execute();
        assertEquals(queries[0], expectedResults[0], result.size());
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail("Query " + queryString + ":" + query + " Execution Failed!");
    }
    CacheUtils.log("********Completed Executing Query Test********");

    // Destroy current Region for other tests
    cache.getRegion(regionName).destroyRegion();
  }

  private void printoutResults(SelectResults results) {
    Iterator iterator = results.iterator();
    while (iterator.hasNext()) {
      Portfolio p = (Portfolio) iterator.next();
      CacheUtils.log("->" + p + ";" + p.createDate);
    }
  }

  /**
   * Test on Local Region data
   */
  @Test
  public void testQueriesOnLocalRegion() throws Exception {
    Cache cache = CacheUtils.getCache();
    createLocalRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem * 2, cache.getRegion(regionName).size());
    executeQueryTest(cache, toDateQueries, toDateExpectedResults);
  }

  /**
   * Test on Replicated Region data
   */
  @Test
  public void testQueriesOnReplicatedRegion() throws Exception {
    Cache cache = CacheUtils.getCache();
    createReplicatedRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem * 2, cache.getRegion(regionName).size());
    executeQueryTest(cache, toDateQueries, toDateExpectedResults);
  }

  /**
   * Test on Partitioned Region data
   */
  @Test
  public void testQueriesOnPartitionedRegion() throws Exception {
    Cache cache = CacheUtils.getCache();
    createPartitionedRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem * 2, cache.getRegion(regionName).size());
    executeQueryTest(cache, toDateQueries, toDateExpectedResults);
  }

  /**
   * Test on Replicated Region data
   */
  @Test
  public void testQueriesOnReplicatedRegionWithSameProjAttr() throws Exception {
    Cache cache = CacheUtils.getCache();
    createReplicatedRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem * 2, cache.getRegion(regionName).size());
    executeQueryTest(cache, projectionQueries, toDateExpectedResults);
  }

  /**
   * Test on Partitioned Region data
   */
  @Test
  public void testQueriesOnPartitionedRegionWithSameProjAttr() throws Exception {
    Cache cache = CacheUtils.getCache();
    createPartitionedRegion();
    assertNotNull(cache.getRegion(regionName));
    assertEquals(numElem * 2, cache.getRegion(regionName).size());
    executeQueryTest(cache, projectionQueries, toDateExpectedResults);
  }

  /******** Region Creation Helper Methods *********/
  /**
   * Each month will have exactly 20 entries with a matching date Code borrowed from shobhit's test
   *
   */
  private void createLocalRegion() throws ParseException {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.NORMAL);
    RegionAttributes regionAttributes = attributesFactory.create();
    Region region = cache.createRegion(regionName, regionAttributes);

    for (int i = 1; i <= numElem; i++) {
      putData(i, region);
    }
  }

  private void createReplicatedRegion() throws ParseException {
    Cache cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes regionAttributes = attributesFactory.create();
    Region region = cache.createRegion(regionName, regionAttributes);

    for (int i = 1; i <= numElem; i++) {
      putData(i, region);
    }
  }

  private void createPartitionedRegion() throws ParseException {
    Cache cache = CacheUtils.getCache();
    PartitionAttributesFactory prAttFactory = new PartitionAttributesFactory();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setPartitionAttributes(prAttFactory.create());
    RegionAttributes regionAttributes = attributesFactory.create();
    Region region = cache.createRegion(regionName, regionAttributes);

    for (int i = 1; i <= numElem; i++) {
      putData(i, region);
    }
  }

  // creates a portfolio object and puts it into the specified region
  private void putData(int id, Region region) throws ParseException {
    Portfolio obj = new Portfolio(id);
    obj.createDate = getCreateDate(id);
    region.put(id, obj);
    region.put(id + numElem, obj);
    CacheUtils.log("Added object " + obj.createDate);
  }

  // creates a date object
  private Date getCreateDate(int i) throws ParseException {
    int month = (i % 12) + 1;
    String format = "MMddyyyyHHmmss";
    String dateString;
    if (month < 10) {
      dateString = "0" + month + "202012100559";
    } else {
      dateString = month + "202012100559";
    }

    SimpleDateFormat sdf = new SimpleDateFormat(format);
    return sdf.parse(dateString);
  }

}
