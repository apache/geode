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

//
// DistinctAndNonDistinctQueryJUnitTest.java
//
package org.apache.geode.cache.query.functional;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class DistinctAndNonDistinctQueryJUnitTest {

  static List data =
      Arrays.asList("abcd", "bcdd", "cde", "de", "abcd", "bcdd", "cde", "de");

  public DistinctAndNonDistinctQueryJUnitTest() {}

  @Test
  public void testDistinct() throws Exception {
    String queryString = "select distinct * from $1";
    Query q = CacheUtils.getQueryService().newQuery(queryString);
    SelectResults results = (SelectResults) q.execute(new Object[] {data});
    assertEquals(4, results.size());
    for (Object element : data) {
      assertTrue(results.contains(element));
      assertEquals(1, results.occurrences(element));
    }
    CacheUtils.closeCache();
  }

  @Test
  public void testNonDistinct() throws Exception {
    String queryString = "select * from $1";
    Query q = CacheUtils.getQueryService().newQuery(queryString);
    SelectResults results = (SelectResults) q.execute(new Object[] {data});
    assertEquals(8, results.size());
    for (Object element : data) {
      assertTrue(results.contains(element));
      assertEquals(2, results.occurrences(element));
    }

    queryString = "select ALL * from $1";
    q = CacheUtils.getQueryService().newQuery(queryString);
    results = (SelectResults) q.execute(new Object[] {data});
    assertEquals(8, results.size());
    for (Object element : data) {
      assertTrue(results.contains(element));
      assertEquals(2, results.occurrences(element));
    }
    CacheUtils.closeCache();
  }

  @Test
  public void testDistinctNonDistinctWithIndexes() throws Exception {
    CacheUtils.startCache();
    Region rgn =
        CacheUtils.createRegion("testDistinctNonDistinctWithIndexes", String.class, Scope.LOCAL);
    QueryService qs = CacheUtils.getQueryService();
    qs.createIndex("length", IndexType.FUNCTIONAL, "length",
        SEPARATOR + "testDistinctNonDistinctWithIndexes");

    List filtered = new ArrayList();
    int i = 0;
    for (final Object datum : data) {
      String s = (String) datum;
      if (s.length() <= 3) {
        rgn.put(new Integer(i++), s);
        filtered.add(s);
      }
    }

    String queryString =
        "select distinct * from " + SEPARATOR + "testDistinctNonDistinctWithIndexes s "
            + " where 3 >= s.length";
    Query q = CacheUtils.getQueryService().newQuery(queryString);
    SelectResults results = (SelectResults) q.execute();
    assertEquals(2, results.size());
    for (Object element : filtered) {
      assertTrue(results.contains(element));
      assertEquals(1, results.occurrences(element));
    }

    queryString =
        "select distinct * from " + SEPARATOR + "testDistinctNonDistinctWithIndexes "
            + "where 3 >= length";
    q = CacheUtils.getQueryService().newQuery(queryString);
    results = (SelectResults) q.execute();
    assertEquals(2, results.size());
    for (Object element : filtered) {
      assertTrue(results.contains(element));
      assertEquals(1, results.occurrences(element));
    }

    queryString =
        "select * from " + SEPARATOR + "testDistinctNonDistinctWithIndexes " + "where 3 >= length";
    q = CacheUtils.getQueryService().newQuery(queryString);
    results = (SelectResults) q.execute(new Object[] {data});
    assertEquals(4, results.size());
    for (Object element : filtered) {
      assertTrue(results.contains(element));
      assertEquals(2, results.occurrences(element));
    }

    queryString = "select ALL * from " + SEPARATOR + "testDistinctNonDistinctWithIndexes "
        + "where 3 >= length";
    q = CacheUtils.getQueryService().newQuery(queryString);
    results = (SelectResults) q.execute(new Object[] {data});
    assertEquals(4, results.size());
    for (Object element : filtered) {
      assertTrue(results.contains(element));
      assertEquals(2, results.occurrences(element));
    }

    CacheUtils.closeCache();
  }
}
