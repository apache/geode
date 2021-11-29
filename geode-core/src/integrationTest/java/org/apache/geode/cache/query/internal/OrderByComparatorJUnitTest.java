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
package org.apache.geode.cache.query.internal;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvalidException;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.query.internal.types.StructTypeImpl;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
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
        "SELECT  distinct ID, description, createTime FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 0 order by ID desc, pkid desc ",};
    Object r[][] = new Object[queries.length][2];
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
        ResultsCollectionPdxDeserializerWrapper wrapper =
            (ResultsCollectionPdxDeserializerWrapper) r[i][0];
        ResultsCollectionWrapper rcw = (ResultsCollectionWrapper) wrapper.results;
        Field baseField = rcw.getClass().getDeclaredField("base");
        baseField.setAccessible(true);
        Collection base = (Collection) baseField.get(rcw);
        assertTrue(base instanceof SortedStructSet);
        SortedStructSet sss = (SortedStructSet) base;
        assertTrue(sss.comparator() instanceof OrderByComparatorMapped);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }

  @Test
  public void testOrderByComparatorMapped() throws Exception {
    String queries[] = {
        "SELECT  distinct ID, description, createTime, pkid FROM " + SEPARATOR
            + "portfolio1 pf1 where ID > 0 order by ID desc, pkid desc ",};
    Object r[][] = new Object[queries.length][2];
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
        ResultsCollectionPdxDeserializerWrapper wrapper =
            (ResultsCollectionPdxDeserializerWrapper) r[i][0];
        ResultsCollectionWrapper rcw = (ResultsCollectionWrapper) wrapper.results;
        Field baseField = rcw.getClass().getDeclaredField("base");
        baseField.setAccessible(true);
        Collection base = (Collection) baseField.get(rcw);
        assertTrue(base instanceof SortedStructSet);
        SortedStructSet sss = (SortedStructSet) base;
        assertFalse(sss.comparator() instanceof OrderByComparatorMapped);
        assertTrue(sss.comparator() instanceof OrderByComparator);
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
  }

  @Test
  public void testUnsupportedOrderByForPR() throws Exception {
    String unsupportedQueries[] =
        {"select distinct p.status from " + SEPARATOR + "portfolio1 p order by p.status, p.ID"};
    Object r[][] = new Object[unsupportedQueries.length][2];
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
      Query q;
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
    String unsupportedQueries[] =
        {"select distinct p.status from " + SEPARATOR + "portfolio1 p order by p.status, p.ID"};
    Object r[][] = new Object[unsupportedQueries.length][2];
    Position.resetCounter();

    // Create Regions
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 50; i++) {
      r1.put(new Portfolio(i), new Portfolio(i));
    }

    for (int i = 0; i < unsupportedQueries.length; i++) {
      Query q;
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

  // The following tests cover edge cases in OrderByComparator.
  @Test
  public void testCompareTwoNulls() throws Exception {
    assertThat(createComparator().compare(null, null)).isEqualTo(0);
  }

  @Test
  public void testCompareTwoObjectArrays() throws Exception {
    String[] arrString1 = {"elephants"};
    String[] arrString2 = {"elephant"};
    assertThat(createComparator().compare(arrString1, arrString2)).isEqualTo(1);
  }

  @Test
  public void testCompareThrowsClassCastException() throws Exception {
    String testString = "elephant";
    int testInt = 159;
    assertThatThrownBy(() -> createComparator().compare(testString, testInt))
        .isInstanceOf(ClassCastException.class);
  }

  private OrderByComparator createComparator() throws Exception {
    StructTypeImpl objType = new StructTypeImpl();
    ExecutionContext context = mock(ExecutionContext.class);
    when(context.getObserver()).thenReturn(new QueryObserverAdapter());
    return new OrderByComparator(null, objType, context);
  }
}
