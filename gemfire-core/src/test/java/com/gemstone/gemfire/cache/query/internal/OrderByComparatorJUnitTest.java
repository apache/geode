/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.query.internal;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class OrderByComparatorJUnitTest {

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();

  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testOrderByComparatorUnmapped() throws Exception {

    String queries[] = {

    "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 0 order by ID desc, pkid desc ", };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 10; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
        ResultsCollectionPdxDeserializerWrapper wrapper = (ResultsCollectionPdxDeserializerWrapper) r[i][0];
        ResultsCollectionWrapper rcw = (ResultsCollectionWrapper) wrapper.results;
        Field baseField = rcw.getClass().getDeclaredField("base");
        baseField.setAccessible(true);
        Collection base = (Collection) baseField.get(rcw);
        assertTrue(base instanceof SortedStructSet);
        SortedStructSet sss = (SortedStructSet) base;
        assertTrue(sss.comparator() instanceof OrderByComparatorUnmapped);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }

  @Test
  public void testOrderByComparatorMapped() throws Exception {

    String queries[] = {

    "SELECT  distinct ID, description, createTime, pkid FROM /portfolio1 pf1 where ID > 0 order by ID desc, pkid desc ", };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 10; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
        ResultsCollectionPdxDeserializerWrapper wrapper = (ResultsCollectionPdxDeserializerWrapper) r[i][0];
        ResultsCollectionWrapper rcw = (ResultsCollectionWrapper) wrapper.results;
        Field baseField = rcw.getClass().getDeclaredField("base");
        baseField.setAccessible(true);
        Collection base = (Collection) baseField.get(rcw);
        assertTrue(base instanceof SortedStructSet);
        SortedStructSet sss = (SortedStructSet) base;
        assertFalse(sss.comparator() instanceof OrderByComparatorUnmapped);
        assertTrue(sss.comparator() instanceof OrderByComparator);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }

  @Test
  public void testUnsupportedOrderByForPR() throws Exception {

    String unsupportedQueries[] = { "select distinct p.status from /portfolio1 p order by p.status, p.ID",

    };
    Object r[][] = new Object[unsupportedQueries.length][2];
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

    for (int i = 0; i < unsupportedQueries.length; i++) {
      Query q = null;

      CacheUtils.getLogger().info("Executing query: " + unsupportedQueries[i]);
      q = CacheUtils.getQueryService().newQuery(unsupportedQueries[i]);
      try {
        r[i][0] = q.execute();
        fail("The query should have thrown exception");
      } catch (QueryInvalidException qe) {
        // ok
      }
    }
  }

  @Test
  public void testSupportedOrderByForRR() throws Exception {

    String unsupportedQueries[] = { "select distinct p.status from /portfolio1 p order by p.status, p.ID",

    };
    Object r[][] = new Object[unsupportedQueries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 50; i++) {
      r1.put(new Portfolio(i), new Portfolio(i));
    }

    for (int i = 0; i < unsupportedQueries.length; i++) {
      Query q = null;

      CacheUtils.getLogger().info("Executing query: " + unsupportedQueries[i]);
      q = CacheUtils.getQueryService().newQuery(unsupportedQueries[i]);
      try {
        r[i][0] = q.execute();

      } catch (QueryInvalidException qe) {
        qe.printStackTrace();
        fail(qe.toString());
      }
    }
  }

}
