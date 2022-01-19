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
package org.apache.geode.cache.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.data.Numbers;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.junit.categories.OQLQueryTest;

/**
 * A sample test class using the QueryTestUtils
 *
 *
 */
@Category({OQLQueryTest.class})
public class QueryTestUtilsJUnitTest {

  private static final long serialVersionUID = 1L;

  QueryTestUtils utils;

  @Before
  public void setUp() throws Exception {

    utils = QueryTestUtils.getInstance();
    utils.createCache(null);
    // create regions
    utils.createReplicateRegion("exampleRegion");
    utils.createReplicateRegion("numericRegion");
    // put entries in the region
    populatePortfolioValuesInRegion("exampleRegion", 10);
    populateNumericValuesInRegion("numericRegion", 10);
  }

  private void populatePortfolioValuesInRegion(String regionName, int size) {
    Region region = utils.getRegion(regionName);
    for (int i = 1; i <= size; i++) {
      region.put("KEY-" + i, new Portfolio(i));
    }
  }

  private void populateOffsetPortfolioValuesInRegion(String regionName, int size) {
    Region region = utils.getRegion(regionName);
    for (int i = 1; i <= size; i++) {
      region.put("KEY-" + i, new Portfolio(i + 1));
    }
  }

  private void populateNumericValuesInRegion(String regionName, int size) {
    Region region = utils.getRegion(regionName);
    for (int i = 1; i <= size; i++) {
      region.put("KEY-" + i, new Numbers(i));
    }
  }

  @Test
  public void testQueries() {
    utils.initializeQueryMap();
    String[] queries = {"1"}; // SELECT * FROM /exampleRegion WHERE status = 'active'
    int results = 0;
    try {
      for (Object result : utils.executeQueries(queries)) {
        if (result instanceof SelectResults) {
          Collection<?> collection = ((SelectResults<?>) result).asList();
          results = collection.size();
          assertEquals(5, results);
          for (Object e : collection) {
            if (e instanceof Portfolio) {
              assertEquals(true, ((Portfolio) e).isActive());
            }
          }
        }
      }
    } catch (Exception e) {
      fail("Query execution failed. : " + e);
    }
    // execute all the queries from the map
    // utils.executeAllQueries();
  }

  @Test
  public void testQueriesWithoutDistinct() throws Exception {
    populateOffsetPortfolioValuesInRegion("exampleRegion", 2);
    String[] queries = {"181"};
    int results = 0;
    for (Object result : utils.executeQueriesWithoutDistinct(queries)) {
      if (result instanceof SelectResults) {
        Collection<?> collection = ((SelectResults<?>) result).asList();
        results = collection.size();
        assertEquals(9, results);
        List expectedIds = new ArrayList(Arrays.asList(10, 9, 8, 7, 6, 5, 4, 3, 3));
        for (Object e : collection) {
          if (e instanceof Portfolio) {
            assertTrue(expectedIds.contains(((Portfolio) e).getID()));
            expectedIds.remove((Integer) ((Portfolio) e).getID());
          }
        }
      }
    }
  }

  @Test
  public void testQueriesWithDistinct() throws Exception {
    String[] queries = {"181"};
    int results = 0;
    int i = 7;
    for (Object result : utils.executeQueriesWithDistinct(queries)) {
      if (result instanceof SelectResults) {
        Collection<?> collection = ((SelectResults<?>) result).asList();
        results = collection.size();
        assertEquals(8, results);
        List expectedIds = new ArrayList(Arrays.asList(10, 9, 8, 7, 6, 5, 4, 3, 2));
        for (Object e : collection) {
          if (e instanceof Portfolio) {
            assertTrue(expectedIds.contains(((Portfolio) e).getID()));
            expectedIds.remove((Integer) ((Portfolio) e).getID());
          }
        }
      }
    }
  }

  @After
  public void tearDown() throws Exception {
    utils.closeCache();
  }


}
