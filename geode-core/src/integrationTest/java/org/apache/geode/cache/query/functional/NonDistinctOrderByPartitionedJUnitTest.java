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
import static org.junit.Assert.fail;

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
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class NonDistinctOrderByPartitionedJUnitTest extends NonDistinctOrderByTestImplementation {

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

  @Override
  public Region createRegion(String regionName, Class valueConstraint) {
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());
    af.setValueConstraint(valueConstraint);
    Region r1 = CacheUtils.createRegion(regionName, af.create(), false);
    return r1;

  }

  @Test
  public void testOrderedResultsPartitionedRegion_Bug43514_1() throws Exception {
    String[] queries = {
        // Test case No. IUMR021
        "select  * from " + SEPARATOR + "portfolio1 p order by status, ID desc",
        "select  * from " + SEPARATOR
            + "portfolio1 p, p.positions.values val order by p.ID, val.secId desc",
        "select  p.status from " + SEPARATOR + "portfolio1 p order by p.status",
        "select  status, ID from " + SEPARATOR + "portfolio1 order by status, ID",
        "select  p.status, p.ID from " + SEPARATOR + "portfolio1 p order by p.status, p.ID",
        "select  key.ID from " + SEPARATOR + "portfolio1.keys key order by key.ID",
        "select  key.ID, key.status from " + SEPARATOR
            + "portfolio1.keys key order by key.status, key.ID",
        "select  key.ID, key.status from " + SEPARATOR
            + "portfolio1.keys key order by key.status desc, key.ID",
        "select  key.ID, key.status from " + SEPARATOR
            + "portfolio1.keys key order by key.status, key.ID desc",
        "select  p.status, p.ID from " + SEPARATOR + "portfolio1 p order by p.status asc, p.ID",
        "select  p.ID, p.status from " + SEPARATOR
            + "portfolio1 p order by p.ID desc, p.status asc",
        "select  p.ID from " + SEPARATOR + "portfolio1 p, p.positions.values order by p.ID",
        "select  p.ID, p.status from " + SEPARATOR
            + "portfolio1 p, p.positions.values order by p.status, p.ID",
        "select  pos.secId from " + SEPARATOR
            + "portfolio1 p, p.positions.values pos order by pos.secId",
        "select  p.ID, pos.secId from " + SEPARATOR
            + "portfolio1 p, p.positions.values pos order by pos.secId, p.ID",
        "select  p.iD from " + SEPARATOR + "portfolio1 p order by p.iD",
        "select  p.iD, p.status from " + SEPARATOR + "portfolio1 p order by p.iD",
        "select  iD, status from " + SEPARATOR + "portfolio1 order by iD",
        "select  p.getID() from " + SEPARATOR + "portfolio1 p order by p.getID()",
        "select  p.names[1] from " + SEPARATOR + "portfolio1 p order by p.names[1]",
        "select  p.position1.secId, p.ID from " + SEPARATOR
            + "portfolio1 p order by p.position1.secId desc, p.ID",
        "select  p.ID, p.position1.secId from " + SEPARATOR
            + "portfolio1 p order by p.position1.secId, p.ID",
        "select  e.key.ID from " + SEPARATOR + "portfolio1.entries e order by e.key.ID",
        "select  e.key.ID, e.value.status from " + SEPARATOR
            + "portfolio1.entries e order by e.key.ID",
        "select  e.key.ID, e.value.status from " + SEPARATOR
            + "portfolio1.entrySet e order by e.key.ID desc , e.value.status desc",
        "select  e.key, e.value from " + SEPARATOR
            + "portfolio1.entrySet e order by e.key.ID, e.value.status desc",
        "select  e.key from " + SEPARATOR
            + "portfolio1.entrySet e order by e.key.ID desc, e.key.pkid desc",
        "select  p.ID, pos.secId from " + SEPARATOR
            + "portfolio1 p, p.positions.values pos order by p.ID, pos.secId",
        "select  p.ID, pos.secId from " + SEPARATOR
            + "portfolio1 p, p.positions.values pos order by p.ID desc, pos.secId desc",
        "select  p.ID, pos.secId from " + SEPARATOR
            + "portfolio1 p, p.positions.values pos order by p.ID desc, pos.secId",

    };
    Object[][] r = new Object[queries.length][2];
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
    qs.createIndex("i1", IndexType.FUNCTIONAL, "p.status", SEPARATOR + "portfolio1 p");
    qs.createIndex("i2", IndexType.FUNCTIONAL, "p.ID", SEPARATOR + "portfolio1 p");
    qs.createIndex("i3", IndexType.FUNCTIONAL, "p.position1.secId", SEPARATOR + "portfolio1 p");
    qs.createIndex("i4", IndexType.FUNCTIONAL, "key.ID", SEPARATOR + "portfolio1.keys key");
    qs.createIndex("i5", IndexType.FUNCTIONAL, "key.status", SEPARATOR + "portfolio1.keys key");
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

  @Test
  public void testOrderedResultsPartitionedRegion_Bug43514_2() throws Exception {
    String[] queries = {
        // Test case No. IUMR021
        "select  status as st from " + SEPARATOR + "portfolio1 where ID > 0 order by status",
        "select  p.status as st from " + SEPARATOR
            + "portfolio1 p where ID > 0 and status = 'inactive' order by p.status",
        "select  p.position1.secId as st from " + SEPARATOR
            + "portfolio1 p where p.ID > 0 and p.position1.secId != 'IBM' order by p.position1.secId",
        "select   key.status as st from " + SEPARATOR
            + "portfolio1 key where key.ID > 5 order by key.status",
        "select  key.ID,key.status as st from " + SEPARATOR
            + "portfolio1 key where key.status = 'inactive' order by key.status desc, key.ID",
        "select   status, ID from " + SEPARATOR + "portfolio1 order by status",
        "select   p.status, p.ID from " + SEPARATOR + "portfolio1 p order by p.status",
        "select  p.position1.secId, p.ID from " + SEPARATOR
            + "portfolio1 p order by p.position1.secId",
        "select  p.status, p.ID from " + SEPARATOR + "portfolio1 p order by p.status asc, p.ID",

        "select  p.ID from " + SEPARATOR + "portfolio1 p, p.positions.values order by p.ID",

        "select  * from " + SEPARATOR + "portfolio1 p, p.positions.values order by p.ID",
        "select  p.iD, p.status from " + SEPARATOR + "portfolio1 p order by p.iD",
        "select  iD, status from " + SEPARATOR + "portfolio1 order by iD",
        "select  * from " + SEPARATOR + "portfolio1 p order by p.getID()",
        "select  * from " + SEPARATOR + "portfolio1 p order by p.getP1().secId",
        "select   p.position1.secId  as st from " + SEPARATOR
            + "portfolio1 p order by p.position1.secId",

        "select  p, pos from " + SEPARATOR + "portfolio1 p, p.positions.values pos order by p.ID",
        "select  p, pos from " + SEPARATOR
            + "portfolio1 p, p.positions.values pos order by pos.secId",
        "select  status from " + SEPARATOR + "portfolio1 where ID > 0 order by status",
        "select  p.status as st from " + SEPARATOR
            + "portfolio1 p where ID > 0 and status = 'inactive' order by p.status",
        "select  p.position1.secId as st from " + SEPARATOR
            + "portfolio1 p where p.ID > 0 and p.position1.secId != 'IBM' order by p.position1.secId"

    };
    Object[][] r = new Object[queries.length][2];
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
