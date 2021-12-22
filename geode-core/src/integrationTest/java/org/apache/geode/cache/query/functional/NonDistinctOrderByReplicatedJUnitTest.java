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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

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
public class NonDistinctOrderByReplicatedJUnitTest extends NonDistinctOrderByTestImplementation {

  @Override
  public boolean assertIndexUsedOnQueryNode() {
    return true;
  }

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

  @Test
  public void testLimitAndOrderByApplicationOnPrimaryKeyIndexQuery() throws Exception {
    String[] queries = {
        // The PK index should be used but limit should not be applied as order
        // by cannot be applied while data is fetched
        // from index
        "SELECT   ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pf1.ID != '10' order by ID desc limit 5 ",
        "SELECT   ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pf1.ID != $1 order by ID "

    };

    Object[][] r = new Object[queries.length][2];
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
        r[i][0] = q.execute(10);
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    qs.createIndex("PKIDIndexPf1", IndexType.PRIMARY_KEY, "ID", SEPARATOR + "portfolio1");
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute("10");
        int indexLimit = queries[i].indexOf("limit");
        int limit = -1;
        boolean limitQuery = indexLimit != -1;
        if (limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit + 5).trim());
        }
        boolean orderByQuery = queries[i].indexOf("order by") != -1;
        SelectResults rcw = (SelectResults) r[i][1];
        if (orderByQuery) {
          assertTrue(rcw.getCollectionType().isOrdered());
        }
        if (!observer.isIndexesUsed) {
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

        for (final Object o : observer.indexesUsed) {
          String indexUsed = o.toString();
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
  public void testLimitApplicationOnPrimaryKeyIndex() throws Exception {

    String[] queries = {
        // The PK index should be used but limit should not be applied as order by
        // cannot be applied while data is fetched
        // from index
        "SELECT   ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where pf1.ID != $1 limit 10",};

    Object[][] r = new Object[queries.length][2];
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
        r[i][0] = q.execute(10);
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    qs.createIndex("PKIDIndexPf1", IndexType.PRIMARY_KEY, "ID", SEPARATOR + "portfolio1");
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute("10");
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
        if (!observer.isIndexesUsed) {
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

        for (final Object o : observer.indexesUsed) {
          String indexUsed = o.toString();
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
      assertTrue(((Integer) values1[0] != 10));
      assertTrue(((Integer) values2[0] != 10));
    }

  }

  @Test
  public void testNonDistinctOrderbyResultSetForReplicatedRegion() throws Exception {
    final int numElements = 200;
    CacheUtils.getCache();
    Region region = createRegion("portfolios", Portfolio.class);
    Short[] expectedArray = new Short[numElements - 1];
    for (int i = 1; i < numElements; ++i) {
      Portfolio pf = new Portfolio(i);
      pf.shortID = (short) ((short) i / 5);
      region.put("" + i, pf);
      expectedArray[i - 1] = pf.shortID;
    }
    Arrays.sort(expectedArray, (o1, o2) -> o1 - o2);

    String query = "select pf.shortID from " + SEPARATOR + "portfolios pf order by pf.shortID";
    QueryService qs = CacheUtils.getQueryService();

    SelectResults sr = (SelectResults) qs.newQuery(query).execute();
    Object[] results = sr.toArray();
    assertTrue(Arrays.equals(expectedArray, results));
  }


  @Test
  public void testOrderedResultsReplicatedRegion() throws Exception {
    String[] queries = {

        "select  status as st from " + SEPARATOR + "portfolio1 where ID > 0 order by status",

        "select  p.status as st from " + SEPARATOR
            + "portfolio1 p where ID > 0 and status = 'inactive' order by p.status",
        "select distinct p.position1.secId as st from " + SEPARATOR
            + "portfolio1 p where p.ID > 0 and p.position1.secId != 'IBM' order by p.position1.secId",
        "select distinct  key.status as st from " + SEPARATOR
            + "portfolio1 key where key.ID > 5 order by key.status",
        "select distinct  key.status as st from " + SEPARATOR
            + "portfolio1 key where key.status = 'inactive' order by key.status desc, key.ID"

    };
    Object[][] r = new Object[queries.length][2];
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
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes
    qs.createIndex("i1", IndexType.FUNCTIONAL, "p.status", SEPARATOR + "portfolio1 p");
    qs.createIndex("i2", IndexType.FUNCTIONAL, "p.ID", SEPARATOR + "portfolio1 p");
    qs.createIndex("i3", IndexType.FUNCTIONAL, "p.position1.secId", SEPARATOR + "portfolio1 p");

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
}
