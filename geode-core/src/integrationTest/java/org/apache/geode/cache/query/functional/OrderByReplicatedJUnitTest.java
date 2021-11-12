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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexExistsException;
import org.apache.geode.cache.query.IndexInvalidException;
import org.apache.geode.cache.query.IndexNameConflictException;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.Struct;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class OrderByReplicatedJUnitTest extends OrderByTestImplementation {

  @Override
  public Region createRegion(String regionName, Class valueConstraint) {
    Region r1 = CacheUtils.createRegion(regionName, valueConstraint);
    return r1;

  }

  @Override
  public Index createIndex(String indexName, IndexType indexType, String indexedExpression,
      String fromClause) throws IndexInvalidException, IndexNameConflictException,
      IndexExistsException, RegionNotFoundException, UnsupportedOperationException {
    return CacheUtils.getQueryService().createIndex(indexName, indexType, indexedExpression,
        fromClause);
  }

  @Override
  public Index createIndex(String indexName, String indexedExpression, String regionPath)
      throws IndexInvalidException, IndexNameConflictException, IndexExistsException,
      RegionNotFoundException, UnsupportedOperationException {
    return CacheUtils.getQueryService().createIndex(indexName, indexedExpression, regionPath);
  }



  @Override
  public boolean assertIndexUsedOnQueryNode() {
    return true;
  }


  @Test
  public void testOrderByWithNullValues() throws Exception {
    // IN ORDER BY NULL values are treated as smallest. E.g For an ascending
    // order by field
    // its null values are reported first and then the values in ascending
    // order.
    String queries[] = getQueriesForOrderByWithNullValues();

    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();

    // Create Regions
    final int size = 9;
    final int numNullValues = 3;
    Region r1 = createRegion("portfolio1", Portfolio.class);
    for (int i = 1; i <= size; i++) {
      Portfolio pf = new Portfolio(i);
      // Add numNullValues null values.
      if (i <= numNullValues) {
        pf.pkid = null;
        pf.status = "a" + i;
      }
      r1.put(i + "", pf);
    }

    Query q = null;
    SelectResults results = null;
    List list = null;
    String str = "";
    try {
      // Query 0 - null values are first in the order.
      str = queries[0];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      r[0][0] = results;
      list = results.asList();
      for (int i = 1; i <= size; i++) {
        Portfolio p = (Portfolio) list.get((i - 1));
        if (i <= numNullValues) {
          assertNull("Expected null value for pkid, p: " + p, p.pkid);
        } else {
          assertNotNull("Expected not null value for pkid", p.pkid);
          if (!p.pkid.equals("" + i)) {
            fail(" Value of pkid is not in expected order.");
          }
        }
      }

      // Query 1 - null values are first in the order.
      str = queries[1];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (int i = 1; i <= size; i++) {
        Portfolio p = (Portfolio) list.get((i - 1));
        if (i <= numNullValues) {
          assertNull("Expected null value for pkid", p.pkid);
        } else {
          assertNotNull("Expected not null value for pkid", p.pkid);
          if (!p.pkid.equals("" + i)) {
            fail(" Value of pkid is not in expected order.");
          }
        }
      }

      // Query 2 - null values are last in the order.
      str = queries[2];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (int i = 1; i <= size; i++) {
        Portfolio p = (Portfolio) list.get((i - 1));
        if (i > (size - numNullValues)) {
          assertNull("Expected null value for pkid", p.pkid);
        } else {
          assertNotNull("Expected not null value for pkid", p.pkid);
          if (!p.pkid.equals("" + (size - (i - 1)))) {
            fail(" Value of pkid is not in expected order.");
          }
        }
      }

      // Query 3 - 1 distinct null value with pkid.
      str = queries[3];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (int i = 1; i <= list.size(); i++) {
        String pkid = (String) list.get((i - 1));
        if (i == 1) {
          assertNull("Expected null value for pkid", pkid);
        } else {
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + (numNullValues + (i - 1)))) {
            fail(" Value of pkid is not in expected order.");
          }
        }
      }

      // Query 4 - 1 distinct null value with pkid.
      str = queries[4];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (int i = 1; i <= list.size(); i++) {
        String pkid = (String) list.get((i - 1));
        if (i == 1) {
          assertNull("Expected null value for pkid", pkid);
        } else {
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + (numNullValues + (i - 1)))) {
            fail(" Value of pkid is not in expected order.");
          }
        }
      }

      // Query 5 - 1 distinct null value with pkid at the end.
      str = queries[5];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (int i = 1; i <= list.size(); i++) {
        String pkid = (String) list.get((i - 1));
        if (i == (list.size())) {
          assertNull("Expected null value for pkid", pkid);
        } else {
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + (size - (i - 1)))) {
            fail(" Value of pkid is not in expected order.");
          }
        }
      }

      // Query 6 - ID field values should be in the same order.
      str = queries[6];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (int i = 1; i <= size; i++) {
        int id = ((Integer) list.get((i - 1))).intValue();
        // ID should be one of 1, 2, 3 because of distinct
        if (i <= numNullValues) {
          if (!(id == 1 || id == 2 || id == 3)) {
            fail(" Value of ID is not as expected " + id);
          }
        } else {
          if (id != i) {
            fail(" Value of ID is not as expected " + id);
          }
        }
      }

      // Query 7 - ID field values should be in the same order.
      str = queries[7];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (int i = 1; i <= list.size(); i++) {
        int id = ((Integer) list.get((i - 1))).intValue();
        if (id != (numNullValues + i)) {
          fail(" Value of ID is not as expected, " + id);
        }
      }

      // Query 8 - ID, pkid field values should be in the same order.
      str = queries[8];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (int i = 1; i <= size; i++) {
        Struct vals = (Struct) list.get((i - 1));
        int id = ((Integer) vals.get("ID")).intValue();
        String pkid = (String) vals.get("pkid");

        // ID should be one of 1, 2, 3 because of distinct
        if (i <= numNullValues) {
          if (!(id == 1 || id == 2 || id == 3)) {
            fail(" Value of ID is not as expected " + id);
          }
          assertNull("Expected null value for pkid", pkid);
        } else {
          if (id != i) {
            fail(" Value of ID is not as expected " + id);
          }
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + i)) {
            fail(" Value of pkid is not in expected order.");
          }
        }
      }

      // Query 9 - ID, pkid field values should be in the same order.
      str = queries[9];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();

      for (int i = 1; i <= list.size(); i++) {
        Struct vals = (Struct) list.get((i - 1));
        int id = ((Integer) vals.get("ID")).intValue();
        String pkid = (String) vals.get("pkid");

        if (i <= numNullValues) {
          assertNull("Expected null value for pkid, " + pkid, pkid);
          if (!(id == 1 || id == 2 || id == 3)) {
            fail(" Value of ID is not as expected " + id);
          }
        } else {
          if (!pkid.equals("" + i)) {
            fail(" Value of pkid is not as expected, " + pkid);
          }
          if (id != i) {
            fail(" Value of ID is not as expected, " + id);
          }
        }
      }

      // Query 10 - ID asc, pkid field values should be in the same order.
      str = queries[10];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();

      for (int i = 1; i <= list.size(); i++) {
        Struct vals = (Struct) list.get((i - 1));
        int id = ((Integer) vals.get("ID")).intValue();
        String pkid = (String) vals.get("pkid");

        if (i <= numNullValues) {
          assertNull("Expected null value for pkid, " + pkid, pkid);
          if (id != i) {
            fail(" Value of ID is not as expected, it is: " + id + " expected :" + i);
          }
        } else {
          if (!pkid.equals("" + i)) {
            fail(" Value of pkid is not as expected, " + pkid);
          }
          if (id != i) {
            fail(" Value of ID is not as expected, " + id);
          }
        }
      }

      // Query 11 - ID desc, pkid field values should be in the same order.
      str = queries[11];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();

      for (int i = 1; i <= list.size(); i++) {
        Struct vals = (Struct) list.get((i - 1));
        int id = ((Integer) vals.get("ID")).intValue();
        String pkid = (String) vals.get("pkid");

        if (i <= numNullValues) {
          assertNull("Expected null value for pkid, " + pkid, pkid);
          if (id != (numNullValues - (i - 1))) {
            fail(" Value of ID is not as expected " + id);
          }
        } else {
          if (!pkid.equals("" + i)) {
            fail(" Value of pkid is not as expected, " + pkid);
          }
          if (id != i) {
            fail(" Value of ID is not as expected, " + id);
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }
  }

  @Override
  public String[] getQueriesForOrderByWithNullValues() {
    // IN ORDER BY NULL values are treated as smallest. E.g For an ascending
    // order by field
    // its null values are reported first and then the values in ascending
    // order.
    String queries[] = {"SELECT  distinct * FROM " + SEPARATOR + "portfolio1 pf1 order by pkid", // 0
                                                                                                 // null
        // values are
        // first in the
        // order.
        "SELECT  distinct * FROM " + SEPARATOR + "portfolio1 pf1  order by pkid asc", // 1 same
        // as
        // above.
        "SELECT  distinct * FROM " + SEPARATOR + "portfolio1 order by pkid desc", // 2 null
        // values are
        // last in the
        // order.
        "SELECT  distinct pkid FROM " + SEPARATOR + "portfolio1 pf1 order by pkid", // 3 null
        // values
        // are first
        // in the
        // order.
        "SELECT  distinct pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid != 'XXXX' order by pkid asc", // 4
        "SELECT  distinct pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid != 'XXXX' order by pkid desc", // 5
        // null
        // values
        // are
        // last
        // in
        // the
        // order.

        "SELECT  distinct ID FROM " + SEPARATOR + "portfolio1 pf1 where ID < 1000 order by pkid", // 6
        "SELECT  distinct ID FROM " + SEPARATOR + "portfolio1 pf1 where ID > 3 order by pkid", // 7
        "SELECT  distinct ID, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID < 1000 order by pkid", // 8
        "SELECT  distinct ID, pkid FROM " + SEPARATOR + "portfolio1 pf1 where ID > 0 order by pkid", // 9
        "SELECT  distinct ID, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 0 order by pkid, ID asc", // 10
        "SELECT  distinct ID, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 0 order by pkid, ID desc",// 11
    };
    return queries;
  }

  @Test
  public void testLimitApplicationOnPrimaryKeyIndex() throws Exception {

    String queries[] = {
        // The PK index should be used but limit should not be applied as order by
        // cannot be applied while data is fetched
        // from index
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pf1.ID != $1 limit 10",};

    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 200; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute(new Object[] {new Integer(10)});
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    this.createIndex("PKIDIndexPf1", IndexType.PRIMARY_KEY, "ID", SEPARATOR + "portfolio1");
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute(new Object[] {"10"});
        int indexLimit = queries[i].indexOf("limit");
        int limit = -1;
        boolean limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        boolean orderByQuery = queries[i].indexOf("order by") != -1;
        SelectResults rcw = (SelectResults) r[i][1];
        if (orderByQuery) {
          assertEquals("Ordered", rcw.getCollectionType().getSimpleClassName());
        }
        if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        int indexDistinct = queries[i].indexOf("distinct");
        boolean distinctQuery = indexDistinct != -1;

        if (limitQuery) {
          if (orderByQuery) {
            assertFalse(observer.limitAppliedAtIndex);
          } else {
            assertTrue(observer.limitAppliedAtIndex);
          }
        } else {
          assertFalse(observer.limitAppliedAtIndex);
        }

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          String indexUsed = itr.next().toString();
          if (!(indexUsed).equals("PKIDIndexPf1")) {
            fail("<PKIDIndexPf1> was expected but found " + indexUsed);
          }
          // assertIndexDetailsEquals("statusIndexPf1",itr.next().toString());
        }

        int indxs = observer.indexesUsed.size();

        System.out.println("**************************************************Indexes Used :::::: "
            + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }

    // Result set verification
    Collection coll1 = null;
    Collection coll2 = null;
    Iterator itert1 = null;
    Iterator itert2 = null;
    ObjectType type1, type2;
    type1 = ((SelectResults) r[0][0]).getCollectionType().getElementType();
    type2 = ((SelectResults) r[0][1]).getCollectionType().getElementType();
    if ((type1.getClass().getName()).equals(type2.getClass().getName())) {
      CacheUtils.log("Both SelectResults are of the same Type i.e.--> "
          + ((SelectResults) r[0][0]).getCollectionType().getElementType());
    } else {
      CacheUtils
          .log("Classes are : " + type1.getClass().getName() + " " + type2.getClass().getName());
      fail("FAILED:Select result Type is different in both the cases." + "; failed query="
          + queries[0]);
    }
    if (((SelectResults) r[0][0]).size() == ((SelectResults) r[0][1]).size()) {
      CacheUtils.log(
          "Both SelectResults are of Same Size i.e.  Size= " + ((SelectResults) r[0][1]).size());
    } else {
      fail("FAILED:SelectResults size is different in both the cases. Size1="
          + ((SelectResults) r[0][0]).size() + " Size2 = " + ((SelectResults) r[0][1]).size()
          + "; failed query=" + queries[0]);
    }
    coll2 = (((SelectResults) r[0][1]).asSet());
    coll1 = (((SelectResults) r[0][0]).asSet());

    itert1 = coll1.iterator();
    itert2 = coll2.iterator();
    while (itert1.hasNext()) {
      Object[] values1 = ((Struct) itert1.next()).getFieldValues();
      Object[] values2 = ((Struct) itert2.next()).getFieldValues();
      assertEquals(values1.length, values2.length);
      assertTrue((((Integer) values1[0]).intValue() != 10));
      assertTrue((((Integer) values2[0]).intValue() != 10));
    }

  }

  @Test
  public void testLimitAndOrderByApplicationOnPrimaryKeyIndexQuery() throws Exception {
    String queries[] = {
        // The PK index should be used but limit should not be applied as order
        // by cannot be applied while data is fetched
        // from index
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pf1.ID != '10' order by ID desc limit 5 ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pf1.ID != $1 order by ID "

    };

    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute(new Object[] {new Integer(10)});
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    this.createIndex("PKIDIndexPf1", IndexType.PRIMARY_KEY, "ID", SEPARATOR + "portfolio1");
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute(new Object[] {"10"});
        int indexLimit = queries[i].indexOf("limit");
        int limit = -1;
        boolean limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        boolean orderByQuery = queries[i].indexOf("order by") != -1;
        SelectResults rcw = (SelectResults) r[i][1];
        if (orderByQuery) {
          assertEquals("Ordered", rcw.getCollectionType().getSimpleClassName());
        }
        if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        if (limitQuery) {
          if (orderByQuery) {
            assertFalse(observer.limitAppliedAtIndex);
          } else {
            assertTrue(observer.limitAppliedAtIndex);
          }
        } else {
          assertFalse(observer.limitAppliedAtIndex);
        }

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          String indexUsed = itr.next().toString();
          if (!(indexUsed).equals("PKIDIndexPf1")) {
            fail("<PKIDIndexPf1> was expected but found " + indexUsed);
          }
          // assertIndexDetailsEquals("statusIndexPf1",itr.next().toString());
        }

        int indxs = observer.indexesUsed.size();

        System.out.println("**************************************************Indexes Used :::::: "
            + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, true, queries);
  }

  @Test
  public void testOrderedResultsReplicatedRegion() throws Exception {
    String queries[] = {
        // Test case No. IUMR021

        "select distinct status as st from " + SEPARATOR
            + "portfolio1 where ID > 0 order by status",

        "select distinct p.status as st from " + SEPARATOR
            + "portfolio1 p where ID > 0 and status = 'inactive' order by p.status",

        "select distinct p.position1.secId as st from " + SEPARATOR
            + "portfolio1 p where p.ID > 0 and p.position1.secId != 'IBM' order by p.position1.secId",
        "select distinct  key.status as st from " + SEPARATOR
            + "portfolio1 key where key.ID > 5 order by key.status",
        "select distinct  key.status as st from " + SEPARATOR
            + "portfolio1 key where key.status = 'inactive' order by key.status desc, key.ID"

    };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes
    this.createIndex("i1", IndexType.FUNCTIONAL, "p.status", SEPARATOR + "portfolio1 p");
    this.createIndex("i2", IndexType.FUNCTIONAL, "p.ID", SEPARATOR + "portfolio1 p");
    this.createIndex("i3", IndexType.FUNCTIONAL, "p.position1.secId", SEPARATOR + "portfolio1 p");

    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length, true, queries);
    ssOrrs.compareExternallySortedQueriesWithOrderBy(queries, r);
  }


  @Override
  public String[] getQueriesForLimitNotAppliedIfOrderByNotUsingIndex() {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '12' and ID > 10 order by ID desc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID > 10 order by ID asc, pkid desc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '13'and  ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid <'9' and ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '15' and ID >= 10 and ID <= 20 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and pkid <='9' and ID >= 10 and ID <= 20 order by ID asc, pkid asc",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID != 10 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID != 10 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '17' and ID > 10 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '17' and ID > 10 order by ID asc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid < '7' and ID > 10 and ID < 20 order by ID asc, pkid asc limit 5 ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '18' and ID > 10 and ID < 20 order by ID desc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '2' and ID >= 10 and ID <= 20 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid != '17' and ID >= 10 and ID <= 20 order by ID asc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '0' and ID != 10 order by ID asc, pkid asc limit 10",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '3' and ID != 10 order by ID desc, pkid desc limit 10",

        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '12' and ID > 10 order by ID desc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID > 10 order by ID asc, pkid desc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '13'and  ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid <'9' and ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '15' and ID >= 10 and ID <= 20 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and pkid <='9' and ID >= 10 and ID <= 20 order by ID asc, pkid asc",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID != 10 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID != 10 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '17' and ID > 10 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '17' and ID > 10 order by ID asc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid < '7' and ID > 10 and ID < 20 order by ID asc, pkid asc limit 5 ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '18' and ID > 10 and ID < 20 order by ID desc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '2' and ID >= 10 and ID <= 20 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid != '17' and ID >= 10 and ID <= 20 order by ID asc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '0' and ID != 10 order by ID asc, pkid asc limit 10",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '3' and ID != 10 order by ID desc, pkid desc limit 10"

    };
    return queries;

  }

  @Override
  public String[] getQueriesForMultiColOrderByWithIndexResultWithProjection() {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid desc",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID asc , pkid desc",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID desc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID desc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID asc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc, pkid desc limit 5 ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID asc , pkid desc limit 10",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID desc, pkid desc limit 10",

        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid desc",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID asc , pkid desc",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID desc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID desc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 order by ID asc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc, pkid desc limit 5 ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID asc , pkid desc limit 10",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID != 10 order by ID desc, pkid desc limit 10",};
    return queries;
  }

  @Override
  public String[] getQueriesForMultiColOrderByWithMultiIndexResultProjection() {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '12' and ID > 10 order by ID desc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID > 10 order by ID asc, pkid desc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '13'and  ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid <'9' and ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '15' and ID >= 10 and ID <= 20 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and pkid <='9' and ID >= 10 and ID <= 20 order by ID asc, pkid asc",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID != 10 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID != 10 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '17' and ID > 10 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '17' and ID > 10 order by ID asc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid < '7' and ID > 10 and ID < 20 order by ID asc, pkid asc limit 5 ",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '18' and ID > 10 and ID < 20 order by ID desc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '2' and ID >= 10 and ID <= 20 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid != '17' and ID >= 10 and ID <= 20 order by ID asc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '0' and ID != 10 order by ID asc, pkid asc limit 10",
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '3' and ID != 10 order by ID desc, pkid desc limit 10",

        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '12' and ID > 10 order by ID desc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID > 10 order by ID asc, pkid desc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '13'and  ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid <'9' and ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '15' and ID >= 10 and ID <= 20 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and pkid <='9' and ID >= 10 and ID <= 20 order by ID asc, pkid asc",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID != 10 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID != 10 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '17' and ID > 10 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '17' and ID > 10 order by ID asc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid < '7' and ID > 10 and ID < 20 order by ID asc, pkid asc limit 5 ",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid = '18' and ID > 10 and ID < 20 order by ID desc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '1' and ID >= 10 and ID <= 20 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid != '17' and ID >= 10 and ID <= 20 order by ID asc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '0' and ID != 10 order by ID asc, pkid asc limit 10",
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pkid > '2' and ID != 10 order by ID desc, pkid desc limit 10"

    };
    return queries;
  }

}
