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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
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
import org.apache.geode.cache.query.dunit.TestObject;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public abstract class OrderByPartitionedJUnitTestBase extends OrderByTestImplementation {

  @Override
  public Region createRegion(String regionName, Class valueConstraint) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());
    af.setValueConstraint(valueConstraint);
    Region r1 = CacheUtils.createRegion(regionName, af.create(), false);
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


  // Asif: This test to me does not make sense as the bind parameter should be a
  // constant

  @Test
  public void testBug() throws Exception {
    // String queries[] =
    // {"SELECT DISTINCT * FROM /test WHERE id < $1 ORDER BY $2" };
    String queries[] = {"SELECT DISTINCT * FROM /test WHERE id < $1 ORDER BY id"};
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    AttributesFactory af = new AttributesFactory();
    // af.setPartitionAttributes(paf.create());
    Region r1 = CacheUtils.createRegion("test", af.create(), false);

    for (int i = 0; i < 100; i++) {
      r1.put("key-" + i, new TestObject(i, "ibm"));
    }
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        // r[i][0] = q.execute(new Object[]{new Integer(101),"id"});
        r[i][0] = q.execute(new Object[] {new Integer(101)});
        assertEquals(100, ((SelectResults) r[i][0]).size());
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }

  @Test
  public void testOrderedResultsPartitionedRegion_Bug43514_1() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "select distinct * from /portfolio1 p order by status, ID desc",
        "select distinct * from /portfolio1 p, p.positions.values val order by p.ID, val.secId desc",
        "select distinct p.status from /portfolio1 p order by p.status",
        "select distinct status, ID from /portfolio1 order by status, ID",
        "select distinct p.status, p.ID from /portfolio1 p order by p.status, p.ID",
        "select distinct key.ID from /portfolio1.keys key order by key.ID",
        "select distinct key.ID, key.status from /portfolio1.keys key order by key.status, key.ID",
        "select distinct key.ID, key.status from /portfolio1.keys key order by key.status desc, key.ID",
        "select distinct key.ID, key.status from /portfolio1.keys key order by key.status, key.ID desc",
        "select distinct p.status, p.ID from /portfolio1 p order by p.status asc, p.ID",
        "select distinct p.ID, p.status from /portfolio1 p order by p.ID desc, p.status asc",
        "select distinct p.ID from /portfolio1 p, p.positions.values order by p.ID",
        "select distinct p.ID, p.status from /portfolio1 p, p.positions.values order by p.status, p.ID",
        "select distinct pos.secId from /portfolio1 p, p.positions.values pos order by pos.secId",
        "select distinct p.ID, pos.secId from /portfolio1 p, p.positions.values pos order by pos.secId, p.ID",
        "select distinct p.iD from /portfolio1 p order by p.iD",
        "select distinct p.iD, p.status from /portfolio1 p order by p.iD",
        "select distinct iD, status from /portfolio1 order by iD",
        "select distinct p.getID() from /portfolio1 p order by p.getID()",
        "select distinct p.names[1] from /portfolio1 p order by p.names[1]",
        "select distinct p.position1.secId, p.ID from /portfolio1 p order by p.position1.secId desc, p.ID",
        "select distinct p.ID, p.position1.secId from /portfolio1 p order by p.position1.secId, p.ID",
        "select distinct e.key.ID from /portfolio1.entries e order by e.key.ID",
        "select distinct e.key.ID, e.value.status from /portfolio1.entries e order by e.key.ID",
        "select distinct e.key.ID, e.value.status from /portfolio1.entrySet e order by e.key.ID desc , e.value.status desc",
        "select distinct e.key, e.value from /portfolio1.entrySet e order by e.key.ID, e.value.status desc",
        "select distinct e.key from /portfolio1.entrySet e order by e.key.ID desc, e.key.pkid desc",
        "select distinct p.ID, pos.secId from /portfolio1 p, p.positions.values pos order by p.ID, pos.secId",
        "select distinct p.ID, pos.secId from /portfolio1 p, p.positions.values pos order by p.ID desc, pos.secId desc",
        "select distinct p.ID, pos.secId from /portfolio1 p, p.positions.values pos order by p.ID desc, pos.secId",

    };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());
    Region r1 = CacheUtils.createRegion("portfolio1", af.create(), false);

    for (int i = 0; i < 50; i++) {
      r1.put(new Portfolio(i), new Portfolio(i));
    }

    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        // CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes
    this.createIndex("i1", IndexType.FUNCTIONAL, "p.status", "/portfolio1 p");
    this.createIndex("i2", IndexType.FUNCTIONAL, "p.ID", "/portfolio1 p");
    this.createIndex("i3", IndexType.FUNCTIONAL, "p.position1.secId", "/portfolio1 p");
    this.createIndex("i4", IndexType.FUNCTIONAL, "key.ID", "/portfolio1.keys key");
    this.createIndex("i5", IndexType.FUNCTIONAL, "key.status", "/portfolio1.keys key");
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
  }

  @Test
  public void testOrderedResultsPartitionedRegion_Bug43514_2() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "select distinct status as st from /portfolio1 where ID > 0 order by status",
        "select distinct p.status as st from /portfolio1 p where ID > 0 and status = 'inactive' order by p.status",
        "select distinct p.position1.secId as st from /portfolio1 p where p.ID > 0 and p.position1.secId != 'IBM' order by p.position1.secId",
        "select distinct  key.status as st from /portfolio1 key where key.ID > 5 order by key.status",
        "select distinct key.ID,key.status as st from /portfolio1 key where key.status = 'inactive' order by key.status desc, key.ID",
        "select distinct  status, ID from /portfolio1 order by status",
        "select distinct  p.status, p.ID from /portfolio1 p order by p.status",
        "select distinct p.position1.secId, p.ID from /portfolio1 p order by p.position1.secId",
        "select distinct p.status, p.ID from /portfolio1 p order by p.status asc, p.ID",

        "select distinct p.ID from /portfolio1 p, p.positions.values order by p.ID",

        "select distinct * from /portfolio1 p, p.positions.values order by p.ID",
        "select distinct p.iD, p.status from /portfolio1 p order by p.iD",
        "select distinct iD, status from /portfolio1 order by iD",
        "select distinct * from /portfolio1 p order by p.getID()",
        "select distinct * from /portfolio1 p order by p.getP1().secId",
        "select distinct  p.position1.secId  as st from /portfolio1 p order by p.position1.secId",

        "select distinct p, pos from /portfolio1 p, p.positions.values pos order by p.ID",
        "select distinct p, pos from /portfolio1 p, p.positions.values pos order by pos.secId",
        "select distinct status from /portfolio1 where ID > 0 order by status",
        "select distinct p.status as st from /portfolio1 p where ID > 0 and status = 'inactive' order by p.status",
        "select distinct p.position1.secId as st from /portfolio1 p where p.ID > 0 and p.position1.secId != 'IBM' order by p.position1.secId"

    };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());
    Region r1 = CacheUtils.createRegion("portfolio1", af.create(), false);

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
    this.createIndex("i1", IndexType.FUNCTIONAL, "p.status", "/portfolio1 p");
    this.createIndex("i2", IndexType.FUNCTIONAL, "p.ID", "/portfolio1 p");
    this.createIndex("i3", IndexType.FUNCTIONAL, "p.position1.secId", "/portfolio1 p");

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
  }


  @Test
  public void testOrderByWithNullValues() throws Exception {
    // IN ORDER BY NULL values are treated as smallest. E.g For an ascending order by field
    // its null values are reported first and then the values in ascending order.
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
        int id = (Integer) ((Struct) list.get((i - 1))).getFieldValues()[0];
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



      // Query 7 - ID, pkid field values should be in the same order.
      str = queries[7];
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

      // Query 8 - ID asc, pkid field values should be in the same order.
      str = queries[8];
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

      // Query 9 - ID desc, pkid field values should be in the same order.
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

  public String[] getQueriesForOrderByWithNullValues() {
    // IN ORDER BY NULL values are treated as smallest. E.g For an ascending
    // order by field
    // its null values are reported first and then the values in ascending
    // order.
    String queries[] = {"SELECT  distinct * FROM /portfolio1 pf1 order by pkid", // 0 null
                                                                                 // values are
                                                                                 // first in the
                                                                                 // order.
        "SELECT  distinct * FROM /portfolio1 pf1  order by pkid asc", // 1 same
                                                                      // as
                                                                      // above.
        "SELECT  distinct * FROM /portfolio1 order by pkid desc", // 2 null
                                                                  // values are
                                                                  // last in the
                                                                  // order.
        "SELECT  distinct pkid FROM /portfolio1 pf1 order by pkid", // 3 null
                                                                    // values
                                                                    // are first
                                                                    // in the
                                                                    // order.
        "SELECT  distinct pkid FROM /portfolio1 pf1 where pkid != 'XXXX' order by pkid asc", // 4
        "SELECT  distinct pkid FROM /portfolio1 pf1 where pkid != 'XXXX' order by pkid desc", // 5
                                                                                              // null
                                                                                              // values
                                                                                              // are
                                                                                              // last
                                                                                              // in
                                                                                              // the
                                                                                              // order.

        "SELECT  distinct ID, pkid FROM /portfolio1 pf1 where ID < 1000 order by pkid", // 6
        "SELECT  distinct ID, pkid FROM /portfolio1 pf1 where ID > 0 order by pkid", // 7
        "SELECT  distinct ID, pkid FROM /portfolio1 pf1 where ID > 0 order by pkid, ID asc", // 8
        "SELECT  distinct ID, pkid FROM /portfolio1 pf1 where ID > 0 order by pkid, ID desc",// 9
    };
    return queries;
  }

  public String[] getQueriesForLimitNotAppliedIfOrderByNotUsingIndex() {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid = '12' and ID > 10 order by ID desc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '1' and ID > 10 order by ID asc, pkid desc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid = '13'and  ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid <'9' and ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid = '15' and ID >= 10 and ID <= 20 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '1' and pkid <='9' and ID >= 10 and ID <= 20 order by ID asc, pkid asc",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '1' and ID != 10 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '1' and ID != 10 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid = '17' and ID > 10 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '17' and ID > 10 order by ID asc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid < '7' and ID > 10 and ID < 20 order by ID asc, pkid asc limit 5 ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid = '18' and ID > 10 and ID < 20 order by ID desc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '1' and ID >= 10 and ID <= 20 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid != '17' and ID >= 10 and ID <= 20 order by ID asc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '0' and ID != 10 order by ID asc, pkid asc limit 10",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '1' and ID != 10 order by ID desc, pkid desc limit 10",


    };
    return queries;

  }

  public String[] getQueriesForMultiColOrderByWithIndexResultWithProjection() {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 10 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 10 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid desc",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID != 10 order by ID asc , pkid desc",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID != 10 order by ID desc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 10 order by ID desc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 10 order by ID asc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc, pkid desc limit 5 ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID != 10 order by ID asc , pkid desc limit 10",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID != 10 order by ID desc, pkid desc limit 10",};
    return queries;
  }

  public String[] getQueriesForMultiColOrderByWithMultiIndexResultProjection() {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid = '12' and ID > 10 order by ID desc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '1' and ID > 10 order by ID asc, pkid desc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid = '13'and  ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid <'9' and ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid = '15' and ID >= 10 and ID <= 20 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '1' and pkid <='9' and ID >= 10 and ID <= 20 order by ID asc, pkid asc",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '1' and ID != 10 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '1' and ID != 10 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid = '17' and ID > 10 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '17' and ID > 10 order by ID asc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid < '7' and ID > 10 and ID < 20 order by ID asc, pkid asc limit 5 ",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid = '18' and ID > 10 and ID < 20 order by ID desc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '1' and ID >= 10 and ID <= 20 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid != '17' and ID >= 10 and ID <= 20 order by ID asc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '0' and ID != 10 order by ID asc, pkid asc limit 10",
        "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '1' and ID != 10 order by ID desc, pkid desc limit 10",


    };
    return queries;
  }
}
