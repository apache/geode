/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*=========================================================================
 * Copyright (c) 2007-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
//
//  DistinctAndNonDistinctQueryJUnitTest.java
//
package com.gemstone.gemfire.cache.query.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class DistinctAndNonDistinctQueryJUnitTest {

  static List data = Arrays.asList(new String[] {
    "abcd", "bcdd", "cde", "de", "abcd", "bcdd", "cde", "de" });

  public DistinctAndNonDistinctQueryJUnitTest() {
  }
  
  @Test
  public void testDistinct() throws Exception {
    String queryString = "select distinct * from $1";
    Query q = CacheUtils.getQueryService().newQuery(queryString);
    SelectResults results = (SelectResults)q.execute(new Object[] { data });
    assertEquals(4, results.size());
    for (Iterator itr = data.iterator(); itr.hasNext(); ) {
      Object element = itr.next();
      assertTrue(results.contains(element));
      assertEquals(1, results.occurrences(element));
    }
    CacheUtils.closeCache();
  }
  
  @Test
  public void testNonDistinct() throws Exception {
    String queryString = "select * from $1";
    Query q = CacheUtils.getQueryService().newQuery(queryString);
    SelectResults results = (SelectResults)q.execute(new Object[] { data });
    assertEquals(8, results.size());
    for (Iterator itr = data.iterator(); itr.hasNext(); ) {
      Object element = itr.next();
      assertTrue(results.contains(element));
      assertEquals(2, results.occurrences(element));
    }

    queryString = "select ALL * from $1";
    q = CacheUtils.getQueryService().newQuery(queryString);
    results = (SelectResults)q.execute(new Object[] { data });
    assertEquals(8, results.size());
    for (Iterator itr = data.iterator(); itr.hasNext(); ) {
      Object element = itr.next();
      assertTrue(results.contains(element));
      assertEquals(2, results.occurrences(element));
    }
    CacheUtils.closeCache();
  }
  
  @Test
  public void testDistinctNonDistinctWithIndexes() throws Exception {
    CacheUtils.startCache();
    Region rgn = CacheUtils.createRegion("testDistinctNonDistinctWithIndexes",
                                    String.class, Scope.LOCAL);
    QueryService qs = CacheUtils.getQueryService();
    qs.createIndex("length", IndexType.FUNCTIONAL, "length", "/testDistinctNonDistinctWithIndexes");
    
    List filtered = new ArrayList();
    int i = 0;
    for (Iterator itr = data.iterator(); itr.hasNext(); ) {
      String s = (String)itr.next();
      if (s.length() <= 3) {
        rgn.put(new Integer(i++), s);
        filtered.add(s);
      }
    }
    
    String queryString = "select distinct * from /testDistinctNonDistinctWithIndexes s "
      + " where 3 >= s.length";
    Query q = CacheUtils.getQueryService().newQuery(queryString);
    SelectResults results = (SelectResults)q.execute();
    assertEquals(2, results.size());
    for (Iterator itr = filtered.iterator(); itr.hasNext(); ) {
      Object element = itr.next();
      assertTrue(results.contains(element));
      assertEquals(1, results.occurrences(element));
    }
    
    queryString = "select distinct * from /testDistinctNonDistinctWithIndexes "
      + "where 3 >= length";
    q = CacheUtils.getQueryService().newQuery(queryString);
    results = (SelectResults)q.execute();
    assertEquals(2, results.size());
    for (Iterator itr = filtered.iterator(); itr.hasNext(); ) {
      Object element = itr.next();
      assertTrue(results.contains(element));
      assertEquals(1, results.occurrences(element));
    }
    
    queryString = "select * from /testDistinctNonDistinctWithIndexes "
      + "where 3 >= length";
    q = CacheUtils.getQueryService().newQuery(queryString);
    results = (SelectResults)q.execute(new Object[] { data });
    assertEquals(4, results.size());
    for (Iterator itr = filtered.iterator(); itr.hasNext(); ) {
      Object element = itr.next();
      assertTrue(results.contains(element));
      assertEquals(2, results.occurrences(element));
    }
    
    queryString = "select ALL * from /testDistinctNonDistinctWithIndexes "
      + "where 3 >= length";
    q = CacheUtils.getQueryService().newQuery(queryString);
    results = (SelectResults)q.execute(new Object[] { data });
    assertEquals(4, results.size());
    for (Iterator itr = filtered.iterator(); itr.hasNext(); ) {
      Object element = itr.next();
      assertTrue(results.contains(element));
      assertEquals(2, results.occurrences(element));
    }
    
    CacheUtils.closeCache();
  } 
}
