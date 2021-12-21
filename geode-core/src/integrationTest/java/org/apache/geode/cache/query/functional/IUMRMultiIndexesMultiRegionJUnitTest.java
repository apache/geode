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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Address;
import org.apache.geode.cache.query.data.Employee;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.query.data.Quote;
import org.apache.geode.cache.query.data.Restricted;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IUMRMultiIndexesMultiRegionJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testMultiIteratorsMultiRegion1() throws Exception {
    Object[][] r = new Object[4][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    Region r2 = CacheUtils.createRegion("portfolio2", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r2.put(i + "", new Portfolio(i));
    }

    Set add1 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));

    Region r3 = CacheUtils.createRegion("employees", Employee.class);
    for (int i = 0; i < 4; i++) {
      r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
    }
    String[] queries = {
        // Test case No. IUMR021
        "SELECT DISTINCT * FROM " + SEPARATOR + "portfolio1 pf1, " + SEPARATOR + "portfolio2 pf2, "
            + SEPARATOR + "employees e WHERE pf1.status = 'active'",};
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      r[i][0] = q.execute();
    }
    // Create Indexes
    qs.createIndex("statusIndexPf1", IndexType.FUNCTIONAL, "status", SEPARATOR + "portfolio1");
    qs.createIndex("statusIndexPf2", IndexType.FUNCTIONAL, "status", SEPARATOR + "portfolio2");

    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][1] = q.execute();
      if (!observer.isIndexesUsed) {
        fail("Index is NOT uesd");
      }

      Iterator itr = observer.indexesUsed.iterator();
      while (itr.hasNext()) {
        String indexUsed = itr.next().toString();
        if (!(indexUsed).equals("statusIndexPf1")) {
          fail("<statusIndexPf1> was expected but found " + indexUsed);
        }
      }

      assertThat(observer.indexesUsed.size()).isGreaterThan(0);
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
  }

  @Test
  public void testMultiIteratorsMultiRegion2() throws Exception {
    Object[][] r = new Object[4][2];
    QueryService qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    Region r2 = CacheUtils.createRegion("portfolio2", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r2.put(i + "", new Portfolio(i));
    }

    Set add1 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));

    Region r3 = CacheUtils.createRegion("employees", Employee.class);
    for (int i = 0; i < 4; i++) {
      r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
    }
    String[] queries = {
        // Test case No. IUMR022
        // Both the Indexes Must get used. Presently only one Index is being used.
        "SELECT DISTINCT * FROM " + SEPARATOR + "portfolio1 pf1, " + SEPARATOR + "portfolio2 pf2, "
            + SEPARATOR + "employees e1 WHERE pf1.status = 'active' AND e1.empId < 10"};
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      r[i][0] = q.execute();
    }
    // Create Indexes & Execute the queries
    qs.createIndex("statusIndexPf1", IndexType.FUNCTIONAL, "pf1.status",
        SEPARATOR + "portfolio1 pf1");
    qs.createIndex("empIdIndex", IndexType.FUNCTIONAL, "e.empId", SEPARATOR + "employees e");

    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][1] = q.execute();
      if (!observer.isIndexesUsed) {
        fail("Index is NOT uesd");
      }
      int indxs = observer.indexesUsed.size();
      if (indxs != 2) {
        fail("Both the idexes are not getting used.Only " + indxs + " index is getting used");
      }

      Iterator itr = observer.indexesUsed.iterator();
      String temp;

      while (itr.hasNext()) {
        temp = itr.next().toString();

        if (temp.equals("statusIndexPf1")) {
          break;
        } else if (temp.equals("empIdIndex")) {
          break;
        } else {
          fail("indices used do not match with those which are expected to be used"
              + "<statusIndexPf1> and <empIdIndex> were expected but found " + itr.next());
        }
      }
    }
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
  }

  @Test
  public void testMultiIteratorsMultiRegion3() throws Exception {
    Object[][] r = new Object[9][2];
    QueryService qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    Region r2 = CacheUtils.createRegion("portfolio2", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r2.put(i + "", new Portfolio(i));
    }
    String[] queries = {
        // Test Case No. IUMR004
        "SELECT DISTINCT * FROM " + SEPARATOR + "portfolio1 pf1, " + SEPARATOR
            + "portfolio2 pf2, pf1.positions.values posit1,"
            + " pf2.positions.values posit2 WHERE posit1.secId='IBM' AND posit2.secId='IBM'",
        // Test Case No.IUMR023
        "SELECT DISTINCT * FROM " + SEPARATOR + "portfolio1 pf1," + SEPARATOR
            + "portfolio2 pf2, pf1.positions.values posit1,"
            + " pf2.positions.values posit2 WHERE posit1.secId='IBM' OR posit2.secId='IBM'",

        "SELECT DISTINCT * FROM " + SEPARATOR
            + "portfolio1 pf1, pf1.collectionHolderMap.values coll1,"
            + " pf1.positions.values posit1, " + SEPARATOR
            + "portfolio2 pf2, pf2.collectionHolderMap.values coll2, pf2.positions.values posit2 "
            + " WHERE posit1.secId='IBM' AND posit2.secId='IBM'",
        // Test Case No. IUMR005
        "SELECT DISTINCT * FROM " + SEPARATOR + "portfolio1 pf1," + SEPARATOR
            + "portfolio2 pf2, pf1.positions.values posit1, pf2.positions.values posit2,"
            + " pf1.collectionHolderMap.values coll1,pf2.collectionHolderMap.values coll2 "
            + " WHERE posit1.secId='IBM' OR posit2.secId='IBM'",
        // Test Case No. IUMR006
        "SELECT DISTINCT coll1 as collHldrMap1 , coll2 as CollHldrMap2 FROM " + SEPARATOR
            + "portfolio1 pf1, " + SEPARATOR
            + "portfolio2 pf2, pf1.positions.values posit1, pf2.positions.values posit2,"
            + " pf1.collectionHolderMap.values coll1,pf2.collectionHolderMap.values coll2 "
            + " WHERE posit1.secId='IBM' OR posit2.secId='IBM'",};
    // Execute Queries Without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      r[i][0] = q.execute();
    }
    // Create Indexes and Execute the Queries
    qs.createIndex("secIdIndexPf1", IndexType.FUNCTIONAL, "pos11.secId",
        SEPARATOR
            + "portfolio1 pf1, pf1.collectionHolderMap.values coll1, pf1.positions.values pos11");
    qs.createIndex("secIdIndexPf2", IndexType.FUNCTIONAL, "pos22.secId",
        SEPARATOR
            + "portfolio2 pf2, pf2.collectionHolderMap.values coll2, pf2.positions.values pos22");

    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][1] = q.execute();
      if (!observer.isIndexesUsed) {
        fail("Index is NOT uesd");
      }
      int indxs = observer.indexesUsed.size();
      if (indxs != 2) {
        fail("Both the idexes are not getting used.Only " + indxs + " index is getting used");
      }

      Iterator itr = observer.indexesUsed.iterator();
      String temp;

      while (itr.hasNext()) {
        temp = itr.next().toString();

        if (temp.equals("secIdIndexPf1")) {
          break;
        } else if (temp.equals("secIdIndexPf2")) {
          break;
        } else {
          fail("indices used do not match with those which are expected to be used"
              + "<secIdIndexPf1> and <secIdIndexPf2> were expected but found " + itr.next());
        }
      }
    }
    // Verify the Query Results
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
  }

  @Test
  public void testMultiIteratorsMultiRegion4() throws Exception {
    Object[][] r = new Object[4][2];
    QueryService qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    Region r2 = CacheUtils.createRegion("portfolio2", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r2.put(i + "", new Portfolio(i));
    }
    String[] queries = {
        // Test case No. IUMR024
        // Both the Indexes Must get used. Presently only one Index is being used.
        "SELECT DISTINCT * FROM " + SEPARATOR + "portfolio1 pf1, pf1.positions.values posit1, "
            + SEPARATOR
            + "portfolio2 pf2, pf2.positions.values posit2 WHERE pf2.status='active' AND posit1.secId='IBM'"};
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      r[i][0] = q.execute();
    }

    // Create Indexes
    qs.createIndex("secIdIndexPf1", IndexType.FUNCTIONAL, "pos11.secId",
        SEPARATOR + "portfolio1 pf1, pf1.positions.values pos11");
    qs.createIndex("statusIndexPf2", IndexType.FUNCTIONAL, "pf2.status",
        SEPARATOR + "portfolio2 pf2");

    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][1] = q.execute();
      if (!observer.isIndexesUsed) {
        fail("Index is NOT uesd");
      }
      int indxs = observer.indexesUsed.size();
      if (indxs != 2) {
        fail("Both the idexes are not getting used.Only " + indxs + " index is getting used");
      }

      Iterator itr = observer.indexesUsed.iterator();
      String temp;

      while (itr.hasNext()) {
        temp = itr.next().toString();

        if (temp.equals("secIdIndexPf1")) {
          break;
        } else if (temp.equals("statusIndexPf2")) {
          break;
        } else {
          fail("indices used do not match with those which are expected to be used"
              + "<statusIndexPf1> and <statusIndexPf2> were expected but found " + itr.next());
        }
      }
    }
    // Verify the Query Results
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
  }

  @Test
  public void testMultiIteratorsMultiRegion5() throws Exception {
    Object[][] r = new Object[4][2];
    QueryService qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    Region r2 = CacheUtils.createRegion("portfolio2", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r2.put(i + "", new Portfolio(i));
    }

    Set add1 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));

    Region r3 = CacheUtils.createRegion("employees", Employee.class);
    for (int i = 0; i < 4; i++) {
      r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
    }
    String[] queries = {
        // Test case IUMR025
        // Three of the indexes must get used.. Presently only one Index is being used.
        "SELECT DISTINCT * FROM " + SEPARATOR + "portfolio1 pf1, " + SEPARATOR + "portfolio2 pf2, "
            + SEPARATOR
            + "employees e1 WHERE pf1.status = 'active' AND pf2.status = 'active' AND e1.empId < 10"};
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][0] = q.execute();
    }
    // Create Indexes and Execute the Queries
    qs.createIndex("statusIndexPf1", IndexType.FUNCTIONAL, "pf1.status",
        SEPARATOR + "portfolio1 pf1");
    qs.createIndex("statusIndexPf2", IndexType.FUNCTIONAL, "pf2.status",
        SEPARATOR + "portfolio2 pf2");
    qs.createIndex("empIdIndex", IndexType.FUNCTIONAL, "empId", SEPARATOR + "employees");

    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][1] = q.execute();
      if (!observer.isIndexesUsed) {
        fail("Index is NOT uesd");
      }
      int indxs = observer.indexesUsed.size();
      if (indxs != 3) {
        fail("Three of the idexes are not getting used. Only " + indxs + " index is getting used");
      }

      Iterator itr = observer.indexesUsed.iterator();
      String temp;

      while (itr.hasNext()) {
        temp = itr.next().toString();

        if (temp.equals("statusIndexPf1")) {
          break;
        } else if (temp.equals("statusIndexPf2")) {
          break;
        } else if (temp.equals("empIdIndex")) {
          break;
        } else {
          fail("indices used do not match with those which are expected to be used"
              + "<statusIndexPf1>, <statusIndexPf2> and <empIdIndex> were expected but found "
              + itr.next());
        }
      }
    }
    // Verify the Query Results
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
  }

  @Test
  public void testMultiIteratorsMultiRegion6() throws Exception {
    Object[][] r = new Object[4][2];
    QueryService qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    Region r2 = CacheUtils.createRegion("portfolio2", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r2.put(i + "", new Portfolio(i));
    }
    String[] queries = {
        // Both the Indexes Must get used. Presently only one Index is being used.
        " SELECT DISTINCT * FROM " + SEPARATOR + "portfolio1 pf1, " + SEPARATOR
            + "portfolio2 pf2, pf1.positions.values posit1,"
            + " pf2.positions.values posit2 WHERE posit1.secId='IBM' AND posit2.secId='IBM' ",
        " SELECT DISTINCT * FROM " + SEPARATOR
            + "portfolio1 pf1, pf1.collectionHolderMap.values coll1,"
            + " pf1.positions.values posit1, " + SEPARATOR
            + "portfolio2 pf2, pf2.collectionHolderMap.values coll2, pf2.positions.values posit2 "
            + " WHERE posit1.secId='IBM' AND posit2.secId='IBM'",};
    // Execute Queries Without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      r[i][0] = q.execute();
    }
    // Create Indexes and Execute the Queries
    qs.createIndex("secIdIndexPf1", IndexType.FUNCTIONAL, "pos11.secId",
        SEPARATOR + "portfolio1 pf1, pf1.positions.values pos11");
    qs.createIndex("secIdIndexPf2", IndexType.FUNCTIONAL, "pos22.secId",
        SEPARATOR + "portfolio2 pf2, pf2.positions.values pos22");

    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][1] = q.execute();
      if (!observer.isIndexesUsed) {
        fail("Index is NOT uesd");
      }
      int indxs = observer.indexesUsed.size();
      if (indxs != 2) {
        fail("Both the idexes are not getting used.Only " + indxs + " index is getting used");
      }

      Iterator itr = observer.indexesUsed.iterator();
      String temp;

      while (itr.hasNext()) {
        temp = itr.next().toString();

        if (temp.equals("secIdIndexPf1")) {
          break;
        } else if (temp.equals("secIdIndexPf2")) {
          break;
        } else {
          fail("indices used do not match with those which are expected to be used"
              + "<secIdIndexPf1> and <secIdIndexPf2> were expected but found " + itr.next());
        }
      }
    }
    // Verify the Query Results
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
  }

  @Test
  public void testMultiIteratorsMultiRegion7() throws Exception {
    Object[][] r = new Object[4][2];
    QueryService qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    Region r2 = CacheUtils.createRegion("portfolio2", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r2.put(i + "", new Portfolio(i));
    }
    String[] queries = {
        // Task IUMR007
        "SELECT DISTINCT coll1 as collHldrMap1 , coll2 as CollHldrMap2 FROM " + SEPARATOR
            + "portfolio1 pf1, " + SEPARATOR
            + "portfolio2 pf2, pf1.positions.values posit1,pf2.positions.values posit2,"
            + "pf1.collectionHolderMap.values coll1, pf2.collectionHolderMap.values coll2 "
            + "WHERE posit1.secId='IBM' OR posit2.secId='IBM'",};
    // Execute Queries Without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      r[i][0] = q.execute();
    }
    // Create Indexes and Execute the Queries
    qs.createIndex("secIdIndexPf1", IndexType.FUNCTIONAL, "pos11.secId",
        SEPARATOR + "portfolio1 pf1, pf1.positions.values pos11");
    qs.createIndex("secIdIndexPf2", IndexType.FUNCTIONAL, "pos22.secId",
        SEPARATOR
            + "portfolio2 pf2, pf2.collectionHolderMap.values coll2, pf2.positions.values pos22");

    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][1] = q.execute();
      if (!observer.isIndexesUsed) {
        fail("Index is NOT uesd");
      }
      int indxs = observer.indexesUsed.size();
      if (indxs != 2) {
        fail("Both the idexes are not getting used.Only " + indxs + " index is getting used");
      }

      Iterator itr = observer.indexesUsed.iterator();
      String temp;

      while (itr.hasNext()) {
        temp = itr.next().toString();

        if (temp.equals("secIdIndexPf1")) {
          break;
        } else if (temp.equals("secIdIndexPf2")) {
          break;
        } else {
          fail("indices used do not match with those which are expected to be used"
              + "<secIdIndexPf1> and <secIdIndexPf2> were expected but found " + itr.next());
        }
      }
    }
    // Verify the Query Results
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
  }

  @Test
  public void testMultiIteratorsMultiRegion8() throws Exception {
    Object[][] r = new Object[4][2];
    QueryService qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    Region r2 = CacheUtils.createRegion("portfolio2", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r2.put(i + "", new Portfolio(i));
    }

    Set add1 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));

    Region r3 = CacheUtils.createRegion("employees", Employee.class);
    for (int i = 0; i < 4; i++) {
      r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
    }
    String[] queries = {
        "SELECT DISTINCT * FROM " + SEPARATOR + "portfolio1 pf1, pf1.positions.values posit1, "
            + SEPARATOR + "portfolio2 pf2, " + SEPARATOR + "employees e WHERE posit1.secId='IBM'"};
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      r[i][0] = q.execute();
    }
    // Create Indexes
    qs.createIndex("statusIndexPf1", IndexType.FUNCTIONAL, "status", SEPARATOR + "portfolio1");
    qs.createIndex("secIdIndexPf1", IndexType.FUNCTIONAL, "posit1.secId",
        SEPARATOR + "portfolio1 pf1, pf1.positions.values posit1");

    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][1] = q.execute();
      if (!observer.isIndexesUsed) {
        fail("Index is NOT uesd");
      }

      Iterator itr = observer.indexesUsed.iterator();
      assertEquals("secIdIndexPf1", itr.next().toString());

      assertThat(observer.indexesUsed.size()).isGreaterThan(0);
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
  }

  @Test
  public void testBasicCompositeIndexUsage() throws Exception {
    try {
      IndexManager.TEST_RANGEINDEX_ONLY = true;

      QueryService qs = CacheUtils.getQueryService();
      Position.resetCounter();
      // Create Regions
      Region r1 = CacheUtils.createRegion("portfolio", Portfolio.class);
      for (int i = 0; i < 1000; i++) {
        r1.put(i + "", new Portfolio(i));
      }
      Set add1 = new HashSet();
      add1.add(new Address("411045", "Baner"));
      add1.add(new Address("411001", "DholePatilRd"));

      Region r2 = CacheUtils.createRegion("employee", Employee.class);
      for (int i = 0; i < 1000; i++) {
        r2.put(i + "", new Employee("empName", (20 + i), i /* empId */, "Mr.", (5000 + i), add1));
      }

      String[][] queriesWithResCount = {
          // Test case No. IUMR021
          {"SELECT DISTINCT * FROM " + SEPARATOR + "portfolio pf, " + SEPARATOR
              + "employee emp WHERE pf.ID = emp.empId",
              1000 + ""},
          {"SELECT * FROM " + SEPARATOR + "portfolio pf, " + SEPARATOR
              + "employee emp WHERE pf.ID = emp.empId", "" + 1000},
          {"SELECT pf.status, emp.empId, pf.getType() FROM " + SEPARATOR + "portfolio pf, "
              + SEPARATOR + "employee emp WHERE pf.ID = emp.empId",
              "" + 1000},
          /*
           * Following query returns more (999001) than expected (1000) results as pf.ID > 0
           * conditions is evaluated first for all Portfolio and Employee objects (999 * 1000) and
           * then other condition with AND is executed for pf.ID = 0 and pf.status = ''active' and
           * pf.ID = emp.ID. So total results are 999001.
           *
           * For expected results correct parenthesis must be used as in next query.
           *
           */
          {"SELECT pf.status, emp.empId, pf.getType() FROM " + SEPARATOR + "portfolio pf, "
              + SEPARATOR
              + "employee emp WHERE pf.ID = emp.empId AND pf.status='active' OR pf.ID > 0",
              "" + 999001},
          {"SELECT * FROM " + SEPARATOR + "portfolio pf, " + SEPARATOR
              + "employee emp WHERE pf.ID = emp.empId AND (pf.status='active' OR pf.ID > 499)",
              "" + 750},
          {"SELECT pf.status, emp.empId, pf.getType() FROM " + SEPARATOR + "portfolio pf, "
              + SEPARATOR
              + "employee emp WHERE pf.ID = emp.empId AND (pf.status='active' OR pf.ID > 499)",
              "" + 750},};

      String[] queries = new String[queriesWithResCount.length];
      Object[][] r = new Object[queries.length][2];

      // Execute Queries without Indexes
      for (int i = 0; i < queries.length; i++) {
        queries[i] = queriesWithResCount[i][0];
        Query q = CacheUtils.getQueryService().newQuery(queries[i]);
        r[i][0] = q.execute();
        assertTrue(r[i][0] instanceof SelectResults);
        assertEquals(Integer.parseInt(queriesWithResCount[i][1]), ((SelectResults) r[i][0]).size());
      }
      // Create Indexes
      qs.createIndex("idIndexPf", IndexType.FUNCTIONAL, "ID", SEPARATOR + "portfolio");
      qs.createIndex("statusIndexPf", IndexType.FUNCTIONAL, "status", SEPARATOR + "portfolio");
      qs.createIndex("empIdIndexPf2", IndexType.FUNCTIONAL, "empId", SEPARATOR + "employee");

      // Execute Queries with Indexes
      for (int i = 0; i < queries.length; i++) {
        Query q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        assertTrue(r[i][0] instanceof SelectResults);
        assertEquals(Integer.parseInt(queriesWithResCount[i][1]), ((SelectResults) r[i][0]).size());
        if (!observer.isIndexesUsed && i != 3 /* For join query without parenthesis */) {
          fail("Index is NOT used for query" + queries[i]);
        }

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          String temp = itr.next().toString();
          if (!(temp.equals("idIndexPf") || temp.equals("empIdIndexPf2")
              || temp.equals("statusIndexPf"))) {
            fail("<idIndexPf> or <empIdIndexPf2>    was expected but found " + temp);
          }
        }

        if (i != 3 /* For join query without parenthesis */) {
          int indxs = observer.indexesUsed.size();
          assertTrue("Indexes used is not of size >= 2", indxs >= 2);
        }
      }
      StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
      ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
    } finally {
      IndexManager.TEST_RANGEINDEX_ONLY = false;
    }
  }

  @Test
  public void testBasicCompositeIndexUsageWithOneIndexExpansionAndTruncation() throws Exception {
    try {
      IndexManager.TEST_RANGEINDEX_ONLY = true;

      Object[][] r = new Object[1][2];
      QueryService qs = CacheUtils.getQueryService();
      Position.resetCounter();
      // Create Regions
      Region r1 = CacheUtils.createRegion("portfolio", Portfolio.class);
      for (int i = 0; i < 1000; i++) {
        r1.put(i + "", new Portfolio(i));
      }
      Set add1 = new HashSet();
      add1.add(new Address("411045", "Baner"));
      add1.add(new Address("411001", "DholePatilRd"));

      Region r2 = CacheUtils.createRegion("employee", Employee.class);
      for (int i = 0; i < 1000; i++) {
        r2.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
      }

      String[] queries = {
          // Test case No. IUMR021
          "SELECT DISTINCT * FROM " + SEPARATOR + "portfolio pf, pf.positions pos, " + SEPARATOR
              + "employee emp WHERE pf.iD = emp.empId",};
      // Execute Queries without Indexes
      for (int i = 0; i < queries.length; i++) {
        Query q = CacheUtils.getQueryService().newQuery(queries[i]);
        r[i][0] = q.execute();
      }
      // Create Indexes
      qs.createIndex("idIndexPf", IndexType.FUNCTIONAL, "iD",
          SEPARATOR + "portfolio pf , pf.collectionHolderMap");
      qs.createIndex("empIdIndexPf2", IndexType.FUNCTIONAL, "empId", SEPARATOR + "employee");

      // Execute Queries with Indexes
      for (int i = 0; i < queries.length; i++) {
        Query q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          String temp = itr.next().toString();
          if (!(temp.equals("idIndexPf") || temp.equals("empIdIndexPf2"))) {
            fail("<idIndexPf> or <empIdIndexPf2>    was expected but found " + temp);
          }
        }

        int indxs = observer.indexesUsed.size();
        assertTrue("Indexes used is not of size = 2", indxs == 2);
      }
      StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
      ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
    } finally {
      IndexManager.TEST_RANGEINDEX_ONLY = false;
    }
  }

  @Test
  public void testBasicCompositeIndexUsageWithMultipleIndexes() throws Exception {
    try {
      IndexManager.TEST_RANGEINDEX_ONLY = true;
      Object[][] r = new Object[1][2];
      QueryService qs = CacheUtils.getQueryService();
      Position.resetCounter();
      // Create Regions
      Region r1 = CacheUtils.createRegion("portfolio", Portfolio.class);
      for (int i = 0; i < 1000; i++) {
        r1.put(i + "", new Portfolio(i));
      }
      Set add1 = new HashSet();
      add1.add(new Address("411045", "Baner"));
      add1.add(new Address("411001", "DholePatilRd"));

      Region r2 = CacheUtils.createRegion("employee", Employee.class);
      for (int i = 0; i < 1000; i++) {
        r2.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
      }

      String[] queries = {
          // Test case No. IUMR021
          "SELECT DISTINCT * FROM " + SEPARATOR + "portfolio pf, pf.positions pos, " + SEPARATOR
              + "employee emp WHERE pf.iD = emp.empId and pf.status='active' and emp.age > 900",};
      // Execute Queries without Indexes
      for (int i = 0; i < queries.length; i++) {
        Query q = CacheUtils.getQueryService().newQuery(queries[i]);
        r[i][0] = q.execute();
      }
      // Create Indexes
      qs.createIndex("idIndexPf", IndexType.FUNCTIONAL, "iD",
          SEPARATOR + "portfolio pf , pf.collectionHolderMap");
      qs.createIndex("empIdIndexPf2", IndexType.FUNCTIONAL, "empId", SEPARATOR + "employee");
      qs.createIndex("ageIndexemp", IndexType.FUNCTIONAL, "age", SEPARATOR + "employee emp ");
      // Execute Queries with Indexes
      for (int i = 0; i < queries.length; i++) {
        Query q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          String temp = itr.next().toString();
          if (!(temp.equals("ageIndexemp") || temp.equals("idIndexPf")
              || temp.equals("empIdIndexPf2") || temp.equals("statusIndexPf2"))) {
            fail("<idIndexPf> or <empIdIndexPf2>    was expected but found " + temp);
          }
        }

        int indxs = observer.indexesUsed.size();
        assertTrue("Indexes used is not of size = 3", indxs == 3);
      }
      StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
      ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
    } finally {
      IndexManager.TEST_RANGEINDEX_ONLY = false;
    }
  }

  @Test
  public void testAssertionBug() throws Exception {
    try {
      IndexManager.TEST_RANGEINDEX_ONLY = true;
      Region region1 = CacheUtils.createRegion("Quotes1", Quote.class);
      Region region2 = CacheUtils.createRegion("Quotes2", Quote.class);
      Region region3 = CacheUtils.createRegion("Restricted1", Restricted.class);
      for (int i = 0; i < 10; i++) {
        region1.put(i, new Quote(i));
        region2.put(i, new Quote(i));
        region3.put(i, new Restricted(i));
      }
      QueryService qs = CacheUtils.getQueryService();
      ////////// creating indexes on region Quotes1
      qs.createIndex("Quotes1Region-quoteIdStrIndex", IndexType.PRIMARY_KEY, "q.quoteIdStr",
          SEPARATOR + "Quotes1 q");

      qs.createIndex("Quotes1Region-quoteTypeIndex", IndexType.FUNCTIONAL, "q.quoteType",
          SEPARATOR + "Quotes1 q, q.restrict r");

      qs.createIndex("Quotes1Region-dealerPortfolioIndex", IndexType.FUNCTIONAL,
          "q.dealerPortfolio", SEPARATOR + "Quotes1 q, q.restrict r");

      qs.createIndex("Quotes1Region-channelNameIndex", IndexType.FUNCTIONAL, "q.channelName",
          SEPARATOR + "Quotes1 q, q.restrict r");

      qs.createIndex("Quotes1Region-priceTypeIndex", IndexType.FUNCTIONAL, "q.priceType",
          SEPARATOR + "Quotes1 q, q.restrict r");

      qs.createIndex("Quotes1Region-lowerQtyIndex", IndexType.FUNCTIONAL, "q.lowerQty",
          SEPARATOR + "Quotes1 q, q.restrict r");
      qs.createIndex("Quotes1Region-upperQtyIndex", IndexType.FUNCTIONAL, "q.upperQty",
          SEPARATOR + "Quotes1 q, q.restrict r");
      qs.createIndex("Quotes1Restricted-quoteTypeIndex", IndexType.FUNCTIONAL, "r.quoteType",
          SEPARATOR + "Quotes1 q, q.restrict r");

      qs.createIndex("Quotes1Restricted-minQtyIndex", IndexType.FUNCTIONAL, "r.minQty",
          SEPARATOR + "Quotes1 q, q.restrict r");
      qs.createIndex("Quotes1Restricted-maxQtyIndex", IndexType.FUNCTIONAL, "r.maxQty",
          SEPARATOR + "Quotes1 q, q.restrict r");

      ////////// creating indexes on region Quotes2
      qs.createIndex("Quotes2Region-quoteIdStrIndex", IndexType.PRIMARY_KEY, "q.quoteIdStr",
          SEPARATOR + "Quotes2 q");

      qs.createIndex("Quotes2Region-quoteTypeIndex", IndexType.FUNCTIONAL, "q.quoteType",
          SEPARATOR + "Quotes2 q, q.restrict r");

      qs.createIndex("Quotes2Region-dealerPortfolioIndex", IndexType.FUNCTIONAL,
          "q.dealerPortfolio", SEPARATOR + "Quotes2 q, q.restrict r");

      qs.createIndex("Quotes2Region-channelNameIndex", IndexType.FUNCTIONAL, "q.channelName",
          SEPARATOR + "Quotes2 q, q.restrict r");

      qs.createIndex("Quotes2Region-priceTypeIndex", IndexType.FUNCTIONAL, "q.priceType",
          SEPARATOR + "Quotes2 q, q.restrict r");

      qs.createIndex("Quotes2Region-lowerQtyIndex", IndexType.FUNCTIONAL, "q.lowerQty",
          SEPARATOR + "Quotes2 q, q.restrict r");
      qs.createIndex("Quotes2Region-upperQtyIndex", IndexType.FUNCTIONAL, "q.upperQty",
          SEPARATOR + "Quotes2 q, q.restrict r");
      qs.createIndex("Quotes2Restricted-quoteTypeIndex", IndexType.FUNCTIONAL, "r.quoteType",
          SEPARATOR + "Quotes2 q, q.restrict r");

      qs.createIndex("Quotes2Restricted-minQtyIndex", IndexType.FUNCTIONAL, "r.minQty",
          SEPARATOR + "Quotes2 q, q.restrict r");
      qs.createIndex("Quotes2Restricted-maxQtyIndex", IndexType.FUNCTIONAL, "r.maxQty",
          SEPARATOR + "Quotes2 q, q.restrict r");

      ////////// creating indexes on region Restricted1

      qs.createIndex("RestrictedRegion-quoteTypeIndex", IndexType.FUNCTIONAL, "r.quoteType",
          SEPARATOR + "Restricted1 r");
      qs.createIndex("RestrictedRegion-minQtyIndex", IndexType.FUNCTIONAL, "r.minQty",
          SEPARATOR + "Restricted1 r");
      qs.createIndex("RestrictedRegion-maxQtyIndex-1", IndexType.FUNCTIONAL, "r.maxQty",
          SEPARATOR + "Restricted1 r");
      Query q = qs.newQuery(
          "SELECT DISTINCT  q.cusip, q.quoteType, q.dealerPortfolio, q.channelName, q.dealerCode, q.priceType, q.price, q.lowerQty, q.upperQty, q.ytm, r.minQty, r.maxQty, r.incQty FROM "
              + SEPARATOR + "Quotes1 q, " + SEPARATOR
              + "Restricted1 r WHERE q.cusip = r.cusip AND q.quoteType = r.quoteType");
      q.execute();
    } finally {
      IndexManager.TEST_RANGEINDEX_ONLY = false;
    }
  }

  @Test
  public void testBasicCompositeIndexUsageInAllGroupJunction() throws Exception {
    try {
      IndexManager.TEST_RANGEINDEX_ONLY = true;

      Object[][] r = new Object[1][2];
      QueryService qs = CacheUtils.getQueryService();
      Position.resetCounter();
      // Create Regions
      Region r1 = CacheUtils.createRegion("portfolio", Portfolio.class);
      for (int i = 0; i < 100; i++) {
        r1.put(i + "", new Portfolio(i));
      }

      Region r3 = CacheUtils.createRegion("portfolio3", Portfolio.class);
      for (int i = 0; i < 10; i++) {
        r3.put(i + "", new Portfolio(i));
      }
      Set add1 = new HashSet();
      add1.add(new Address("411045", "Baner"));
      add1.add(new Address("411001", "DholePatilRd"));

      Region r2 = CacheUtils.createRegion("employee", Employee.class);
      for (int i = 0; i < 100; i++) {
        r2.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
      }

      String[] queries = {
          // Test case No. IUMR021
          "SELECT DISTINCT * FROM " + SEPARATOR + "portfolio pf, pf.positions pos, " + SEPARATOR
              + "portfolio3 pf3, " + SEPARATOR
              + "employee emp WHERE pf.iD = emp.empId and pf.status='active' and emp.age > 50 and pf3.status='active'",};
      // Execute Queries without Indexes
      for (int i = 0; i < queries.length; i++) {
        Query q = CacheUtils.getQueryService().newQuery(queries[i]);
        r[i][0] = q.execute();
      }
      // Create Indexes
      qs.createIndex("idIndexPf", IndexType.FUNCTIONAL, "iD",
          SEPARATOR + "portfolio pf , pf.collectionHolderMap");
      qs.createIndex("empIdIndexPf2", IndexType.FUNCTIONAL, "empId", SEPARATOR + "employee");
      qs.createIndex("statusIndexPf3", IndexType.FUNCTIONAL, "status",
          SEPARATOR + "portfolio3 pf3 ");
      qs.createIndex("ageIndexemp", IndexType.FUNCTIONAL, "age", SEPARATOR + "employee emp ");
      // Execute Queries with Indexes
      for (int i = 0; i < queries.length; i++) {
        Query q = CacheUtils.getQueryService().newQuery(queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          String temp = itr.next().toString();
          if (!(temp.equals("ageIndexemp") || temp.equals("idIndexPf")
              || temp.equals("empIdIndexPf2") || temp.equals("statusIndexPf3"))) {
            fail("<idIndexPf> or <empIdIndexPf2>    was expected but found " + temp);
          }
        }

        int indxs = observer.indexesUsed.size();
        assertTrue("Indexes used is not of size = 4 but of size = " + indxs, indxs == 4);
      }
      StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
      ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
    } finally {
      IndexManager.TEST_RANGEINDEX_ONLY = false;
    }
  }

  private static class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;
    ArrayList indexesUsed = new ArrayList();
    String indexName;

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexName = index.getName();
      indexesUsed.add(index.getName());
    }

    @Override
    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }
  }
}
