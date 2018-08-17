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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;

public abstract class NonDistinctOrderByTestImplementation {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }



  public abstract Region createRegion(String regionName, Class valueConstraint);

  public abstract Index createIndex(String indexName, IndexType indexType, String indexedExpression,
      String fromClause) throws IndexInvalidException, IndexNameConflictException,
      IndexExistsException, RegionNotFoundException, UnsupportedOperationException;

  public abstract Index createIndex(String indexName, String indexedExpression, String regionPath)
      throws IndexInvalidException, IndexNameConflictException, IndexExistsException,
      RegionNotFoundException, UnsupportedOperationException;

  public abstract boolean assertIndexUsedOnQueryNode();

  @Test
  public void testOrderByWithIndexResultDefaultProjection() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT   * FROM /portfolio1 pf1 where ID > 10 order by ID desc ",
        "SELECT   * FROM /portfolio1 pf1 where ID > 10 order by ID asc ",
        "SELECT   * FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc ",
        "SELECT   * FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc ",
        "SELECT   * FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc ",
        "SELECT   * FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc",
        "SELECT   * FROM /portfolio1 pf1 where ID != 10 order by ID asc ",
        "SELECT   * FROM /portfolio1 pf1 where ID != 10 order by ID desc ",
        "SELECT   * FROM /portfolio1 pf1 where ID > 10 order by ID desc limit 5",
        "SELECT   * FROM /portfolio1 pf1 where ID > 10 order by ID asc limit 5",
        "SELECT   * FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc limit 5 ",
        "SELECT   * FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc limit 5",
        "SELECT   * FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc limit 5",
        "SELECT   * FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc limit 5",
        "SELECT   * FROM /portfolio1 pf1 where ID != 10 order by ID asc limit 10",
        "SELECT   * FROM /portfolio1 pf1 where ID != 10 order by ID desc limit 10",
        "SELECT   *  FROM /portfolio1 pf1 where ID > 0 order by ID desc, pkid desc "};

    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = this.createRegion("portfolio1", Portfolio.class);

    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      r1.put("" + i, pf);
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

    this.createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", "/portfolio1");

    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        SelectResults rcw = (SelectResults) r[i][1];
        int indexLimit = queries[i].indexOf("limit");
        int limit = -1;
        boolean limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        assertTrue("Result size is " + rcw.size() + " and limit is " + limit,
            !limitQuery || rcw.size() <= limit);

        if (!rcw.getCollectionType().isOrdered()) {
          fail("The collection type=" + rcw.getCollectionType().getSimpleClassName()
              + "  is not ordered");
        }
        if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          if (!(itr.next().toString()).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found " + itr.next().toString());
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
    // Compare each of the query results with queries fired without order by ,
    // but without order by
    ssOrrs.compareExternallySortedQueriesWithOrderBy(queries, r);
  }

  @Test
  public void testOrderByWithIndexResultWithProjection() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT   shortID, description FROM /portfolio1 pf1 where ID > 10 order by shortID desc ",
        "SELECT   shortID, description FROM /portfolio1 pf1 where ID > 10 order by shortID asc ",
        "SELECT   shortID, description FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by shortID asc ",
        "SELECT   shortID, description FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by shortID desc ",
        "SELECT  shortID, description FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by shortID desc ",
        "SELECT  shortID, description FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by shortID asc",
        "SELECT  shortID, description FROM /portfolio1 pf1 where ID != 10 order by shortID asc ",
        "SELECT  shortID, description FROM /portfolio1 pf1 where ID != 10 order by shortID desc ",
        "SELECT  shortID, description FROM /portfolio1 pf1 where ID > 10 order by shortID desc limit 5",
        "SELECT  shortID, description FROM /portfolio1 pf1 where ID > 10 order by shortID asc limit 5",
        "SELECT  shortID, description FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by shortID asc limit 5 ",
        "SELECT  shortID, description FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by shortID desc limit 5",
        "SELECT  shortID, description FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by shortID desc limit 5",
        "SELECT  shortID, description FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by shortID asc limit 5",
        "SELECT  shortID, description FROM /portfolio1 pf1 where ID != 10 order by shortID asc limit 10",
        "SELECT  shortID, description FROM /portfolio1 pf1 where ID != 10 order by shortID desc limit 10",

    };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = this.createRegion("portfolio1", Portfolio.class);

    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      r1.put("" + i, pf);
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

    this.createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", "/portfolio1");

    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        SelectResults rcw = (SelectResults) r[i][1];
        int indexLimit = queries[i].indexOf("limit");
        int limit = -1;
        boolean limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        assertTrue(!limitQuery || rcw.size() <= limit);
        // assertIndexDetailsEquals("Set",rcw.getCollectionType().getSimpleClassName());
        if (!rcw.getCollectionType().isOrdered()) {
          fail("The collection type=" + rcw.getCollectionType().getSimpleClassName()
              + "  is not ordered");
        }

        if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          if (!(itr.next().toString()).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found " + itr.next().toString());
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
    ssOrrs.compareExternallySortedQueriesWithOrderBy(queries, r);
  }

  @Test
  public void testMultiColOrderByWithIndexResultDefaultProjection() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT   * FROM /portfolio1 pf1 where ID > 10 order by ID desc, pkid asc ",
        "SELECT   * FROM /portfolio1 pf1 where ID > 10 order by ID asc, pkid desc ",
        "SELECT   * FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT   * FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT   * FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid desc ",
        "SELECT   * FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid asc",
        "SELECT   * FROM /portfolio1 pf1 where ID != 10 order by ID asc, pkid asc ",
        "SELECT   * FROM /portfolio1 pf1 where ID != 10 order by ID desc, pkid desc ",
        "SELECT   * FROM /portfolio1 pf1 where ID > 10 order by ID desc, pkid asc limit 5",
        "SELECT   * FROM /portfolio1 pf1 where ID > 10 order by ID asc, pkid desc limit 5",
        "SELECT   * FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc, pkid asc limit 5 ",
        "SELECT   * FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc, pkid desc limit 5",
        "SELECT   * FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid asc limit 5",
        "SELECT   * FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid desc limit 5",
        "SELECT   * FROM /portfolio1 pf1 where ID != 10 order by ID asc, pkid asc limit 10",
        "SELECT   * FROM /portfolio1 pf1 where ID != 10 order by ID desc, pkid desc limit 10",

    };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions
    Region r1 = this.createRegion("portfolio1", Portfolio.class);

    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      r1.put("" + i, pf);
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

    this.createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", "/portfolio1");

    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        SelectResults rcw = (SelectResults) r[i][1];
        int indexLimit = queries[i].indexOf("limit");
        int limit = -1;
        boolean limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        assertTrue(!limitQuery || rcw.size() <= limit);
        assertTrue(rcw.getCollectionType().isOrdered());
        if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          if (!(itr.next().toString()).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found " + itr.next().toString());
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
    ssOrrs.compareExternallySortedQueriesWithOrderBy(queries, r);
  }

  @Test
  public void testMultiColOrderByWithIndexResultWithProjection() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 10 order by ID desc, pkid desc ",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 10 order by ID asc, pkid asc ",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid asc ",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid desc",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where ID != 10 order by ID asc , pkid desc",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where ID != 10 order by ID desc, pkid asc ",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 10 order by ID desc, pkid desc limit 5",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 10 order by ID asc, pkid asc limit 5",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc, pkid desc limit 5 ",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc, pkid asc limit 5",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid desc limit 5",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid asc limit 5",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where ID != 10 order by ID asc , pkid desc limit 10",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where ID != 10 order by ID desc, pkid desc limit 10",

    };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = this.createRegion("portfolio1", Portfolio.class);

    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      r1.put("" + i, pf);
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

    this.createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", "/portfolio1");

    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        SelectResults rcw = (SelectResults) r[i][1];
        int indexLimit = queries[i].indexOf("limit");
        int limit = -1;
        boolean limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        assertTrue(!limitQuery || rcw.size() <= limit);
        assertTrue(rcw.getCollectionType().isOrdered());
        if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          if (!(itr.next().toString()).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found " + itr.next().toString());
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
    ssOrrs.compareExternallySortedQueriesWithOrderBy(queries, r);
  }

  @Test
  public void testMultiColOrderByWithMultiIndexResultDefaultProjection() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT   * FROM /portfolio1 pf1 where pkid = '12' and ID > 10 order by ID desc, pkid asc ",
        "SELECT   * FROM /portfolio1 pf1 where pkid > '1' and ID > 10 order by ID asc, pkid desc ",
        "SELECT   * FROM /portfolio1 pf1 where pkid = '13'and  ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT   * FROM /portfolio1 pf1 where pkid <'9' and ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT   * FROM /portfolio1 pf1 where pkid = '15' and ID >= 10 and ID <= 20 order by ID desc, pkid desc ",
        "SELECT   * FROM /portfolio1 pf1 where pkid > '1' and pkid <='9' and ID >= 10 and ID <= 20 order by ID asc, pkid asc",
        "SELECT   * FROM /portfolio1 pf1 where pkid > '0' and ID != 10 order by ID asc, pkid asc ",
        "SELECT   * FROM /portfolio1 pf1 where pkid > '1' and ID != 10 order by ID desc, pkid desc ",
        "SELECT   * FROM /portfolio1 pf1 where pkid = '17' and ID > 10 order by ID desc, pkid asc limit 5",
        "SELECT   * FROM /portfolio1 pf1 where pkid > '17' and ID > 10 order by ID asc, pkid desc limit 5",
        "SELECT   * FROM /portfolio1 pf1 where pkid < '7' and ID > 10 and ID < 20 order by ID asc, pkid asc limit 5 ",
        "SELECT   * FROM /portfolio1 pf1 where pkid = '18' and ID > 10 and ID < 20 order by ID desc, pkid desc limit 5",
        "SELECT   * FROM /portfolio1 pf1 where pkid > '0' and ID >= 10 and ID <= 20 order by ID desc, pkid asc limit 5",
        "SELECT   * FROM /portfolio1 pf1 where pkid != '17' and ID >= 10 and ID <= 20 order by ID asc, pkid desc limit 5",
        "SELECT   * FROM /portfolio1 pf1 where pkid > '0' and ID != 10 order by ID asc, pkid asc limit 10",
        "SELECT   * FROM /portfolio1 pf1 where pkid > '9' and ID != 10 order by ID desc, pkid desc limit 10",

    };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = this.createRegion("portfolio1", Portfolio.class);

    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      r1.put("" + i, pf);
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

    this.createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", "/portfolio1");
    this.createIndex("PKIDIndexPf1", IndexType.FUNCTIONAL, "pkid", "/portfolio1");
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();

        SelectResults rcw = (SelectResults) r[i][1];

        int indexLimit = queries[i].indexOf("limit");
        int limit = -1;
        boolean limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        assertTrue(!limitQuery || rcw.size() <= limit);
        assertTrue(rcw.getCollectionType().isOrdered());
        if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          String indexUsed = itr.next().toString();
          if (!(indexUsed).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found " + indexUsed);
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
    ssOrrs.compareExternallySortedQueriesWithOrderBy(queries, r);
  }

  @Test
  public void testMultiColOrderByWithMultiIndexResultProjection() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid = '12' and ID > 10 order by ID desc, pkid asc ",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '1' and ID > 10 order by ID asc, pkid desc ",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid = '13'and  ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid <'9' and ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid = '15' and ID >= 10 and ID <= 20 order by ID desc, pkid desc ",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '1' and pkid <='9' and ID >= 10 and ID <= 20 order by ID asc, pkid asc",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '1' and ID != 10 order by ID asc, pkid asc ",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '1' and ID != 10 order by ID desc, pkid desc ",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid = '17' and ID > 10 order by ID desc, pkid asc limit 5",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '17' and ID > 10 order by ID asc, pkid desc limit 5",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid < '7' and ID > 10 and ID < 20 order by ID asc, pkid asc limit 5 ",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid = '18' and ID > 10 and ID < 20 order by ID desc, pkid desc limit 5",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '2' and ID >= 10 and ID <= 20 order by ID desc, pkid asc limit 5",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid != '17' and ID >= 10 and ID <= 20 order by ID asc, pkid desc limit 5",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '0' and ID != 10 order by ID asc, pkid asc limit 10",
        "SELECT   ID, description, createTime, pkid FROM /portfolio1 pf1 where pkid > '9' and ID != 10 order by ID desc, pkid desc limit 10",


    };

    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = this.createRegion("portfolio1", Portfolio.class);

    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      r1.put("" + i, pf);
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

    this.createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", "/portfolio1");
    this.createIndex("PKIDIndexPf1", IndexType.FUNCTIONAL, "pkid", "/portfolio1");
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        int indexLimit = queries[i].indexOf("limit");
        int limit = -1;
        boolean limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        SelectResults rcw = (SelectResults) r[i][1];
        assertTrue(rcw.getCollectionType().isOrdered());
        if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        assertTrue(!limitQuery || rcw.size() <= limit);

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          String indexUsed = itr.next().toString();
          if (!(indexUsed).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found " + indexUsed);
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
    ssOrrs.compareExternallySortedQueriesWithOrderBy(queries, r);
  }

  @Test
  public void testLimitNotAppliedIfOrderByNotUsingIndex() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT    description, pkid FROM /portfolio1 pf1 where pkid = '12' and ID > 10  order by pkid asc ",
        "SELECT    description, pkid FROM /portfolio1 pf1 where pkid > '1' and ID > 10 order by  pkid desc ",
        "SELECT    description, pkid FROM /portfolio1 pf1 where pkid = '13'and  ID > 10 and ID < 20 order by  pkid asc ",
        "SELECT    description, pkid FROM /portfolio1 pf1 where pkid <'9' and ID > 10 and ID < 20 order by  pkid desc",
        "SELECT    description, pkid FROM /portfolio1 pf1 where pkid = '15' and ID >= 10 and ID <= 20 order by  pkid desc ",
        "SELECT    description, pkid FROM /portfolio1 pf1 where pkid > '1' and pkid <='9' and ID >= 10 and ID <= 20 order by  pkid asc",
        "SELECT   description, pkid FROM /portfolio1 pf1 where pkid > '1' and ID != 10 order by  pkid asc ",
        "SELECT   description, pkid FROM /portfolio1 pf1 where pkid > '1' and ID != 10 order by  pkid desc ",
        "SELECT  description, pkid FROM /portfolio1 pf1 where pkid = '17' and ID > 10 order by  pkid asc limit 5",
        "SELECT    description,  pkid FROM /portfolio1 pf1 where pkid > '17' and ID > 10 order by  pkid desc limit 5",
        "SELECT    description,  pkid FROM /portfolio1 pf1 where pkid < '7' and ID > 10 and ID < 20 order by  pkid asc limit 5 ",
        "SELECT   description, pkid FROM /portfolio1 pf1 where pkid = '18' and ID > 10 and ID < 20 order by  pkid desc limit 5",
        "SELECT    description,  pkid FROM /portfolio1 pf1 where pkid > '1' and ID >= 10 and ID <= 20 order by pkid asc limit 5",
        "SELECT   description,  pkid FROM /portfolio1 pf1 where pkid != '17' and ID >= 10 and ID <= 20 order by pkid desc limit 5",
        "SELECT   description,  pkid FROM /portfolio1 pf1 where pkid > '0' and ID != 10 order by  pkid asc limit 10",
        "SELECT   description, createTime, pkid FROM /portfolio1 pf1 where pkid > '9' and ID != 10 order by  pkid desc limit 10",

    };

    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = this.createRegion("portfolio1", Portfolio.class);

    for (int i = 1; i < 200; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      r1.put("" + i, pf);
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

    this.createIndex("PKIDIndexPf1", IndexType.FUNCTIONAL, "pkid", "/portfolio1");
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        int indexLimit = queries[i].indexOf("limit");
        int limit = -1;
        boolean limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        SelectResults rcw = (SelectResults) r[i][1];
        assertTrue(rcw.getCollectionType().isOrdered());
        if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        // assertTrue(!limitQuery || !observer.limitAppliedAtIndex);

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
    ssOrrs.compareExternallySortedQueriesWithOrderBy(queries, r);
  }


  @Test
  public void testOrderByWithNullValues() throws Exception {
    // IN ORDER BY NULL values are treated as smallest. E.g For an ascending
    // order by field
    // its null values are reported first and then the values in ascending
    // order.
    String queries[] = {"SELECT   * FROM /portfolio1 pf1 order by pkid", // 0 null
                                                                         // values are
                                                                         // first in the
                                                                         // order.
        "SELECT   * FROM /portfolio1 pf1  order by pkid asc", // 1 same
                                                              // as
                                                              // above.
        "SELECT   * FROM /portfolio1 order by pkid desc", // 2 null
                                                          // values are
                                                          // last in the
                                                          // order.
        "SELECT   pkid FROM /portfolio1 pf1 order by pkid", // 3 null
                                                            // values
                                                            // are first
                                                            // in the
                                                            // order.
        "SELECT   pkid FROM /portfolio1 pf1 where pkid != 'XXXX' order by pkid asc", // 4
        "SELECT   pkid FROM /portfolio1 pf1 where pkid != 'XXXX' order by pkid desc", // 5
                                                                                      // null
                                                                                      // values
                                                                                      // are
                                                                                      // last
                                                                                      // in
                                                                                      // the
                                                                                      // order.

        "SELECT   ID, pkid FROM /portfolio1 pf1 where ID < 1000 order by pkid", // 6
        "SELECT   ID, pkid FROM /portfolio1 pf1 where ID > 3 order by pkid", // 7
        "SELECT   ID, pkid FROM /portfolio1 pf1 where ID < 1000 order by pkid", // 8
        "SELECT   ID, pkid FROM /portfolio1 pf1 where ID > 0 order by pkid", // 9
        "SELECT   ID, pkid FROM /portfolio1 pf1 where ID > 0 order by pkid, ID asc", // 10
        "SELECT   ID, pkid FROM /portfolio1 pf1 where ID > 0 order by pkid, ID desc", // 11
    };

    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();

    // Create Regions
    final int size = 9;
    final int numNullValues = 3;
    Region r1 = this.createRegion("portfolio1", Portfolio.class);
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

      // Query 3 - 3 null value with pkid.
      str = queries[3];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (int i = 1; i <= list.size(); i++) {
        String pkid = (String) list.get((i - 1));
        if (i <= numNullValues) {
          assertNull("Expected null value for pkid", pkid);
        } else {
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + i)) {
            fail(" Value of pkid is not in expected order.");
          }
        }
      }

      // Query 4 - 3 null value with pkid.
      str = queries[4];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (int i = 1; i <= list.size(); i++) {
        String pkid = (String) list.get((i - 1));
        if (i <= 3) {
          assertNull("Expected null value for pkid", pkid);
        } else {
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + i)) {
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
        if (i > (list.size() - numNullValues)) {
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

      // Query 7 - ID field values should be in the same order.
      str = queries[7];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();
      list = results.asList();
      for (int i = 1; i <= list.size(); i++) {
        int id = (Integer) ((Struct) list.get((i - 1))).getFieldValues()[0];
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

  @Test
  public void testOrderByWithNullValuesUseIndex() throws Exception {
    // IN ORDER BY NULL values are treated as smallest. E.g For an ascending
    // order by field
    // its null values are reported first and then the values in ascending
    // order.
    String queries[] = {"SELECT   * FROM /portfolio1 pf1 where ID > 0 order by pkid", // 0
                                                                                      // null
                                                                                      // values
                                                                                      // are
                                                                                      // first
                                                                                      // in
                                                                                      // the
                                                                                      // order.
        "SELECT   * FROM /portfolio1 pf1 where ID > 0 order by pkid asc", // 1
                                                                          // same
                                                                          // as
                                                                          // above.
        "SELECT   * FROM /portfolio1 where ID > 0 order by pkid desc", // 2
                                                                       // null
                                                                       // values
                                                                       // are
                                                                       // last
                                                                       // in
                                                                       // the
                                                                       // order.
        "SELECT   pkid FROM /portfolio1 pf1 where ID > 0 order by pkid", // 3
                                                                         // null
                                                                         // values
                                                                         // are
                                                                         // first
                                                                         // in
                                                                         // the
                                                                         // order.
        "SELECT   pkid FROM /portfolio1 pf1 where ID > 0 order by pkid asc", // 4
        "SELECT   pkid FROM /portfolio1 pf1 where ID > 0 order by pkid desc", // 5
                                                                              // null
                                                                              // values
                                                                              // are
                                                                              // last
                                                                              // in
                                                                              // the
                                                                              // order.
        "SELECT   ID, pkid FROM /portfolio1 pf1 where ID < 1000 order by pkid", // 6
        "SELECT   ID, pkid FROM /portfolio1 pf1 where ID > 3 order by pkid", // 7
        "SELECT   ID, pkid FROM /portfolio1 pf1 where ID < 1000 order by pkid", // 8
        "SELECT   ID, pkid FROM /portfolio1 pf1 where ID > 0 order by pkid", // 9
    };

    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();

    // Create Regions
    final int size = 9;
    final int numNullValues = 3;
    Region r1 = this.createRegion("portfolio1", Portfolio.class);
    for (int i = 1; i <= size; i++) {
      Portfolio pf = new Portfolio(i);
      // Add numNullValues null values.
      if (i <= numNullValues) {
        pf.pkid = null;
        pf.status = "a" + i;
      }
      r1.put(i + "", pf);
    }

    // Create Indexes
    this.createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", "/portfolio1");
    this.createIndex("PKIDIndexPf1", IndexType.FUNCTIONAL, "pkid", "/portfolio1");

    Query q = null;
    SelectResults results = null;
    List list = null;
    String str = "";
    try {
      // Query 0 - null values are first in the order.
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);

      str = queries[0];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();

      if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
        fail("Index is NOT uesd");
      }

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
        if (i <= numNullValues) {
          assertNull("Expected null value for pkid", pkid);
        } else {
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + i)) {
            fail(" Value of pkid is not in expected order.");
          }
        }
      }

      // Query 4 - 1 distinct null value with pkid.
      observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);

      str = queries[4];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults) q.execute();

      if (assertIndexUsedOnQueryNode() && !observer.isIndexesUsed) {
        fail("Index is NOT uesd");
      }

      list = results.asList();
      for (int i = 1; i <= list.size(); i++) {
        String pkid = (String) list.get((i - 1));
        if (i <= numNullValues) {
          assertNull("Expected null value for pkid", pkid);
        } else {
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + i)) {
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
        if (i > list.size() - numNullValues) {
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
        Struct strct = (Struct) list.get(i - 1);
        int id = ((Integer) strct.getFieldValues()[0]).intValue();
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
        Struct strct = (Struct) list.get(i - 1);
        int id = ((Integer) strct.getFieldValues()[0]).intValue();
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
    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }

  }

  @Test
  public void testOrderByForUndefined() throws Exception {
    String queries[] = {"SELECT  position1.secId FROM /test ORDER BY position1.secId", // 0
        "SELECT  position1.secId FROM /test ORDER BY position1.secId desc", // 1
        "SELECT  position1.secId FROM /test where ID > 0  ORDER BY position1.secId", // 2
        "SELECT  position1.secId FROM /test where ID > 0  ORDER BY position1.secId desc", // 3
        "SELECT  position1.secId, ID FROM /test ORDER BY position1.secId, ID", // 4
        "SELECT  position1.secId, ID FROM /test ORDER BY position1.secId desc, ID",// 5
    };
    Region r1 = this.createRegion("test", Portfolio.class);
    for (int i = 0; i < 10; i++) {
      Portfolio pf = new Portfolio(i);
      if (i % 2 == 0) {
        pf.position1 = null;
      }
      r1.put(i + "", pf);
    }
    QueryService qs = CacheUtils.getQueryService();
    SelectResults[][] sr = new SelectResults[queries.length][2];
    Object[] srArr = null;
    for (int i = 0; i < queries.length; i++) {
      try {
        sr[i][0] = (SelectResults) qs.newQuery(queries[i]).execute();
        srArr = sr[i][0].toArray();
        if (i == 0) {
          assertEquals("First result should be undefined for query " + queries[i],
              QueryService.UNDEFINED, srArr[0]);
        } else if (i == 1) {
          assertEquals("Last result should be undefined for query " + queries[i],
              QueryService.UNDEFINED, srArr[srArr.length - 1]);
        } else if (i == 2) {
          assertEquals("First result should be undefined for query " + queries[i],
              QueryService.UNDEFINED, srArr[0]);
        } else if (i == 3) {
          assertEquals("Last result should be undefined for query " + queries[i],
              QueryService.UNDEFINED, srArr[srArr.length - 1]);
        } else if (i == 4) {
          for (int j = 0; j < srArr.length / 2; j++) {
            assertEquals("Undefined should  have been returned for query " + queries[i],
                QueryService.UNDEFINED, ((Struct) srArr[j]).getFieldValues()[0]);
          }
        } else if (i == 5) {
          for (int j = srArr.length - 1; j > srArr.length / 2; j--) {
            assertEquals("Undefined should  have been returned for query " + queries[i],
                QueryService.UNDEFINED, ((Struct) srArr[j]).getFieldValues()[0]);
          }
        }
      } catch (Exception e) {
        fail("Query execution failed for: " + queries[i] + " : " + e);
      }
    }

    this.createIndex("secIndex", "position1.secId", r1.getFullPath());
    this.createIndex("IDIndex", "ID", r1.getFullPath());

    for (int i = 0; i < queries.length; i++) {
      try {
        sr[i][1] = (SelectResults) qs.newQuery(queries[i]).execute();
        srArr = sr[i][1].toArray();
        if (i == 0) {
          assertEquals("First result should be undefined for query " + queries[i],
              QueryService.UNDEFINED, srArr[0]);
        } else if (i == 1) {
          assertEquals("Last result should be undefined for query " + queries[i],
              QueryService.UNDEFINED, srArr[srArr.length - 1]);
        } else if (i == 2) {
          assertEquals("First result should be undefined for query " + queries[i],
              QueryService.UNDEFINED, srArr[0]);
        } else if (i == 3) {
          assertEquals("Last result should be undefined for query " + queries[i],
              QueryService.UNDEFINED, srArr[srArr.length - 1]);
        } else if (i == 4) {
          for (int j = 0; j < srArr.length / 2; j++) {
            assertEquals("Undefined should  have been returned for query " + queries[i],
                QueryService.UNDEFINED, ((Struct) srArr[j]).getFieldValues()[0]);
          }
        } else if (i == 5) {
          for (int j = srArr.length - 1; j > srArr.length / 2; j--) {
            assertEquals("Undefined should  have been returned for query " + queries[i],
                QueryService.UNDEFINED, ((Struct) srArr[j]).getFieldValues()[0]);
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail("Query execution failed for: " + queries[i] + " : " + e);
      }
    }

    CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
  }



  class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;

    ArrayList indexesUsed = new ArrayList();

    String indexName;
    boolean limitAppliedAtIndex = false;

    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexName = index.getName();
      indexesUsed.add(index.getName());
    }

    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }

    public void limitAppliedAtIndexLevel(Index index, int limit, Collection indexResult) {
      this.limitAppliedAtIndex = true;
    }

  }


}
