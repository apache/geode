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
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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
import org.apache.geode.cache.query.data.Address;
import org.apache.geode.cache.query.data.Employee;
import org.apache.geode.cache.query.data.PhoneNo;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Street;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IUMRShuffleIteratorsJUnitTest {

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    Region r1 = CacheUtils.createRegion("portfolios", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r1.put(i + "", new Portfolio(i));
    }
    Set add1 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));

    Region r2 = CacheUtils.createRegion("employees", Employee.class);
    for (int i = 0; i < 4; i++) {
      r2.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
    }

    Region r3 = CacheUtils.createRegion("address", Address.class);
    Set ph = new HashSet();
    Set str = new HashSet();
    ph.add(new PhoneNo(111, 222, 333, 444));
    str.add(new Street("DPRoad", "lane5"));
    str.add(new Street("DPStreet1", "lane5"));
    for (int i = 0; i < 4; i++) {
      r3.put(i, new Address("411001", "Pune", str, ph));
    }
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testQueryWithNOIndexes1() throws Exception {
    CacheUtils.getQueryService();

    String[] queries =
        {"select distinct * from " + SEPARATOR + "portfolios p, " + SEPARATOR + "employees e",
            "Select distinct * from " + SEPARATOR + "portfolios pf," + SEPARATOR
                + "employees e  where pf.status='active'"};

    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      q.execute();
      if (observer.isIndexesUsed) {
        fail("Index is used");
      }
    }
  }

  @Test
  public void testQueryWithIndexOnFirstReg2() throws Exception {
    Object[][] r = new Object[9][2];
    QueryService qs = CacheUtils.getQueryService();

    String[] queries =
        {"Select distinct * from " + SEPARATOR + "portfolios pf," + SEPARATOR
            + "employees e  where pf.status='active'"};

    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      r[i][0] = q.execute();
    }
    // Create Index and Execute the Queries
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", SEPARATOR + "portfolios");
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][1] = q.execute();
      if (!observer.isIndexesUsed) {
        fail("Index is NOT used");
      }

      Iterator itr = observer.indexesUsed.iterator();
      assertEquals("statusIndex", itr.next().toString());
    }
    // Verifying the query results
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
  }

  @Test
  public void testQueryWithIndexOnSecondReg3() throws Exception {
    Object[][] r = new Object[9][2];
    QueryService qs = CacheUtils.getQueryService();
    String[] queries = {
        // Test Case No. IUMR001
        "Select distinct * from " + SEPARATOR + "portfolios pf, " + SEPARATOR
            + "employees e  where e.name ='empName'",};
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      r[i][0] = q.execute();
    }
    // Create Index andExecute the Queries
    qs.createIndex("nameIndex", IndexType.FUNCTIONAL, "e.name", SEPARATOR + "employees e");
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][1] = q.execute();

      if (!observer.isIndexesUsed) {
        fail("Index is NOT used");
        // Test fails here. Index on second region is not getting used.
      }
      Iterator itr = observer.indexesUsed.iterator();
      assertEquals("nameIndex", itr.next().toString());
    }
    // Verifying the query results
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
  }

  @Test
  public void testQueryWithIndexOnBothReg4() throws Exception {
    Object[][] r = new Object[9][2];
    QueryService qs = CacheUtils.getQueryService();

    String[] queries = {
        // Test Case No. IUMR002
        "Select distinct * from " + SEPARATOR + "portfolios pf, " + SEPARATOR
            + "employees e  where e.name ='empName' and pf.status='active'",};

    // Execute Query without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      r[i][0] = q.execute();
    }

    // Create Indexes and Execute Queries
    qs.createIndex("nameIndex", IndexType.FUNCTIONAL, "e.name", SEPARATOR + "employees e");
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "pf.status", SEPARATOR + "portfolios pf");

    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][1] = q.execute();
      if (!observer.isIndexesUsed) {
        fail("Index is NOT used");
      }
      int indxs = observer.indexesUsed.size();
      if (indxs != 2) {
        fail("FAILED: Both The Indexes should be used. Presently only " + indxs
            + " Index(es) is used");
      }

      Iterator itr = observer.indexesUsed.iterator();
      String temp;

      while (itr.hasNext()) {
        temp = itr.next().toString();

        if (temp.equals("nameIndex")) {
          break;
        } else if (temp.equals("statusIndex")) {
          break;
        } else {
          fail("indices used do not match with those which are expected to be used"
              + "<nameIndex> and <statusIndex> were expected but found " + itr.next());
        }
      }
    }
    // Verifying the query results
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
  }

  @Test
  public void testWithThreeRegions5() throws Exception {
    Object[][] r = new Object[5][2];
    QueryService qs = CacheUtils.getQueryService();

    String[] queries = {
        // Scenario A: If the Order of the Regions in Query are changed Index is not being used.
        "select distinct * from " + SEPARATOR + "portfolios p, " + SEPARATOR + "employees e, "
            + SEPARATOR + "address a, a.street s where s.street ='DPStreet1'",
        "select distinct * from " + SEPARATOR + "address a, " + SEPARATOR + "portfolios p, "
            + SEPARATOR + "employees e, a.street s  where s.street ='DPStreet1'",};

    // Execute queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      r[i][0] = q.execute();
    }
    // Create Indexes and Execute the queries
    qs.createIndex("nameIndex", IndexType.FUNCTIONAL, "e.name", SEPARATOR + "employees e");
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "p.status", SEPARATOR + "portfolios p");
    qs.createIndex("streetIndex", IndexType.FUNCTIONAL, "s.street",
        SEPARATOR + "address a, a.street s");
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][1] = q.execute();
      if (!observer.isIndexesUsed) {
        fail("Index is NOT used");
      }
      Iterator itr = observer.indexesUsed.iterator();
      assertEquals("streetIndex", itr.next().toString());
    }
    // Verifying the query results
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
  }

  @Test
  public void testShuffleIterators6() throws Exception {
    Object[][] r = new Object[5][2];
    QueryService qs = CacheUtils.getQueryService();

    String[] queries = {
        "select distinct * from " + SEPARATOR
            + "address itr1,itr1.phoneNo itr2,itr1.street itr3 where itr2.mobile>333",
        "select distinct * from " + SEPARATOR
            + "address itr1,itr1.street itr2,itr1.phoneNo itr3 where itr3.mobile>333",};

    // Execute the query without index
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      r[i][0] = q.execute();
    }
    // Create index and Execute the query
    qs.createIndex("mobileIndex", IndexType.FUNCTIONAL, "itr2.mobile",
        SEPARATOR + "address itr1,itr1.phoneNo itr2,itr1.street itr3");
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][1] = q.execute();
      if (!observer.isIndexesUsed) {
        fail("Index is NOT used");
      }

      Iterator itr = observer.indexesUsed.iterator();
      assertEquals("mobileIndex", itr.next().toString());
    }
    // Verifying the query results
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
  }

  @Test
  public void testWithThreeRegions7() throws Exception {
    Object[][] r = new Object[5][2];
    QueryService qs = CacheUtils.getQueryService();

    String[] queries = {
        // Only Index on the first region in the Query is used
        "select distinct * from " + SEPARATOR + "address a, " + SEPARATOR + "portfolios p, "
            + SEPARATOR
            + "employees e, a.street s where p.status='active' and s.street ='DPStreet1'",};
    // Execute queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      r[i][0] = q.execute();
    }
    // Create Indexes and Execute the queries
    qs.createIndex("nameIndex", IndexType.FUNCTIONAL, "e.name", SEPARATOR + "employees e");
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "p.status", SEPARATOR + "portfolios p");
    qs.createIndex("streetIndex", IndexType.FUNCTIONAL, "s.street",
        SEPARATOR + "address a, a.street s");
    for (int i = 0; i < queries.length; i++) {
      Query q = CacheUtils.getQueryService().newQuery(queries[i]);
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      r[i][1] = q.execute();
      if (!observer.isIndexesUsed) {
        fail("Index is NOT used");
      }

      Iterator itr = observer.indexesUsed.iterator();
      String temp;

      while (itr.hasNext()) {
        temp = itr.next().toString();

        if (temp.equals("streetIndex")) {
          break;
        } else if (temp.equals("statusIndex")) {
          break;
        } else {
          fail("indices used do not match with those which are expected to be used"
              + "<streetIndex> and <statusIndex> were expected but found " + itr.next());
        }
      }
    }
    // Verifying the query results
    StructSetOrResultsSet ssORrs = new StructSetOrResultsSet();
    ssORrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, queries);
  }

  private static class QueryObserverImpl extends QueryObserverAdapter {

    private final List indexesUsed = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean isIndexesUsed = false;
    private volatile String indexName;

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
