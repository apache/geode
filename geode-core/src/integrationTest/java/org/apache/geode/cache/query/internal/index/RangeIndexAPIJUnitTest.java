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
package org.apache.geode.cache.query.internal.index;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.CompiledIteratorDef;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.QCompiler;
import org.apache.geode.cache.query.internal.QueryExecutionContext;
import org.apache.geode.cache.query.internal.RuntimeIterator;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class RangeIndexAPIJUnitTest {

  private Region region = null;

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    IndexManager.ENABLE_UPDATE_IN_PROGRESS_INDEX_CALCULATION = false;
    region = CacheUtils.createRegion("portfolios", Portfolio.class);
    for (int i = 0; i < 12; i++) {
      // CacheUtils.log(new Portfolio(i));
      // make status of 10 and 11 as null
      Portfolio pf = new Portfolio(i);
      if (i == 10 || i == 11) {
        pf.status = null;
      }
      region.put(i, pf);
    }

  }

  @After
  public void tearDown() throws Exception {
    IndexManager.ENABLE_UPDATE_IN_PROGRESS_INDEX_CALCULATION = true;
    CacheUtils.closeCache();
  }

  @Test
  public void testQueryMethod_1() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    AbstractIndex i1 =
        (AbstractIndex) qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolios");
    AbstractIndex i2 = (AbstractIndex) qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status",
        SEPARATOR + "portfolios");
    AbstractIndex i3 = (AbstractIndex) qs.createIndex("status.toString()", IndexType.FUNCTIONAL,
        "status.toString", SEPARATOR + "portfolios");

    Set results = new HashSet();
    DefaultQuery q =
        new DefaultQuery("select * from " + SEPARATOR + "portfolios", CacheUtils.getCache(), false);
    q.setRemoteQuery(false);
    ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache(), q);
    bindIterators(context, SEPARATOR + "portfolios");
    i1.query(1, OQLLexerTokenTypes.TOK_EQ, results, context);
    assertEquals(1, results.size());
    assertTrue(results.iterator().next() == region.get(1));
    results.clear();
    i1.query(1, OQLLexerTokenTypes.TOK_GT, results, context);
    assertEquals(10, results.size());
    for (int i = 2; i < 12; ++i) {
      assertTrue(results.contains(region.get(i)));
    }
    results.clear();
    i2.query("active", OQLLexerTokenTypes.TOK_NE, results, context);
    assertEquals(7, results.size());
    for (int i = 1; i < 12;) {
      assertTrue(results.contains(region.get(i)));
      if (i >= 9) {
        ++i;
      } else {
        i += 2;
      }
    }

    results.clear();
    i3.query(QueryService.UNDEFINED, OQLLexerTokenTypes.TOK_EQ, results, context);
    assertEquals(2, results.size());
    assertTrue(results.contains(region.get(11)));
    assertTrue(results.contains(region.get(10)));

  }

  /**
   * The keysToRemove set will never contain null or UNDEFINED as those conditions never form part
   * of RangeJunctionCondnEvaluator. Such null or undefined conditions are treated as separate
   * filter operands. This test checks the query method of Index which takes a set of keys which
   * need to be removed from the set
   */
  @Test
  public void testQueryMethod_2() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    AbstractIndex i1 =
        (AbstractIndex) qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolios");
    AbstractIndex i2 = (AbstractIndex) qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status",
        SEPARATOR + "portfolios");
    AbstractIndex i3 = (AbstractIndex) qs.createIndex("status.toString()", IndexType.FUNCTIONAL,
        "status.toString", SEPARATOR + "portfolios");

    Set results = new HashSet();
    DefaultQuery q = new DefaultQuery("select * from " + SEPARATOR + "portfolios  ",
        CacheUtils.getCache(), false);
    q.setRemoteQuery(false);
    ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache(), q);
    bindIterators(context, SEPARATOR + "portfolios");
    Set keysToRemove = new HashSet();
    i1.query(1, OQLLexerTokenTypes.TOK_EQ, results, context);
    assertEquals(1, results.size());
    assertTrue(results.iterator().next() == region.get(1));
    results.clear();
    keysToRemove.clear();
    keysToRemove.add(1);
    try {
      i1.query(1, OQLLexerTokenTypes.TOK_EQ, results, keysToRemove, context);
      fail(
          "A condition having an  equal will be identified at RangeJunction level itself, so this type of condition should throw error in RangeIndex where along with an equal there happens not equal conditions");
    } catch (AssertionError error) {
      // pass
    }
    keysToRemove.clear();
    results.clear();
    keysToRemove.add(9);
    i1.query(1, OQLLexerTokenTypes.TOK_GT, results, keysToRemove, context);
    assertEquals(9, results.size());
    for (int i = 2; i < 12;) {
      if (i != 9) {
        assertTrue(results.contains(region.get(i)));
      }
      ++i;
    }

    keysToRemove.clear();
    results.clear();
    keysToRemove.add(1);
    keysToRemove.add(10);
    i1.query(1, OQLLexerTokenTypes.TOK_GE, results, keysToRemove, context);
    assertEquals(9, results.size());
    for (int i = 2; i < 12;) {
      if (i != 10) {
        assertTrue(results.contains(region.get(i)));
      }
      ++i;
    }

    keysToRemove.clear();
    results.clear();
    keysToRemove.add(8);
    keysToRemove.add(11);
    i1.query(11, OQLLexerTokenTypes.TOK_LT, results, keysToRemove, context);
    assertEquals(10, results.size());
    for (int i = 0; i < 11;) {
      if (i != 8) {
        assertTrue(results.contains(region.get(i)));
      }
      ++i;
    }

    keysToRemove.clear();
    results.clear();
    keysToRemove.add(8);
    keysToRemove.add(11);
    i1.query(11, OQLLexerTokenTypes.TOK_LE, results, keysToRemove, context);
    assertEquals(10, results.size());
    for (int i = 0; i < 11;) {
      if (i != 8) {
        assertTrue(results.contains(region.get(i)));
      }
      ++i;
    }

    keysToRemove.clear();
    results.clear();
    keysToRemove.add(1);
    keysToRemove.add(10);
    i1.query(1, OQLLexerTokenTypes.TOK_GT, results, keysToRemove, context);
    assertEquals(9, results.size());
    for (int i = 2; i < 12;) {
      if (i != 10) {
        assertTrue(results.contains(region.get(i)));
      }
      ++i;
    }
  }

  /**
   * Tests the query method of RangeIndex with takes a bound defined ( lower as well as upper) & may
   * contain NotEqaul Keys
   */
  @Test
  public void testQueryMethod_3() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    AbstractIndex i1 =
        (AbstractIndex) qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolios");
    AbstractIndex i2 = (AbstractIndex) qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status",
        SEPARATOR + "portfolios");
    AbstractIndex i3 = (AbstractIndex) qs.createIndex("status.toString()", IndexType.FUNCTIONAL,
        "status.toString", SEPARATOR + "portfolios");

    Set results = new HashSet();
    DefaultQuery q =
        new DefaultQuery("select * from " + SEPARATOR + "portfolios", CacheUtils.getCache(), false);
    q.setRemoteQuery(false);
    ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache(), q);
    bindIterators(context, SEPARATOR + "portfolios");
    Set keysToRemove = new HashSet();
    i1.query(5, OQLLexerTokenTypes.TOK_GT, 10, OQLLexerTokenTypes.TOK_LT,
        results, null, context);
    assertEquals(4, results.size());
    for (int i = 6; i < 10;) {
      assertTrue(results.contains(region.get(i)));
      ++i;
    }

    results.clear();
    keysToRemove.clear();
    keysToRemove.add(10);
    keysToRemove.add(9);
    i1.query(5, OQLLexerTokenTypes.TOK_GT, 10, OQLLexerTokenTypes.TOK_LT,
        results, keysToRemove, context);
    assertEquals(3, results.size());
    for (int i = 6; i < 9;) {
      assertTrue(results.contains(region.get(i)));
      ++i;
    }

    results.clear();
    keysToRemove.clear();
    keysToRemove.add(10);
    i1.query(5, OQLLexerTokenTypes.TOK_GT, 10, OQLLexerTokenTypes.TOK_LE,
        results, keysToRemove, context);
    assertEquals(4, results.size());
    for (int i = 6; i < 10;) {
      assertTrue(results.contains(region.get(i)));
      ++i;
    }
    results.clear();
    keysToRemove.clear();
    i1.query(5, OQLLexerTokenTypes.TOK_GT, 10, OQLLexerTokenTypes.TOK_LE,
        results, null, context);
    assertEquals(5, results.size());
    for (int i = 6; i < 11;) {
      assertTrue(results.contains(region.get(i)));
      ++i;
    }

    results.clear();
    keysToRemove.clear();
    i1.query(5, OQLLexerTokenTypes.TOK_GE, 10, OQLLexerTokenTypes.TOK_LE,
        results, null, context);
    assertEquals(6, results.size());
    for (int i = 5; i < 11;) {
      assertTrue(results.contains(region.get(i)));
      ++i;
    }

    results.clear();
    keysToRemove.clear();
    keysToRemove.add(5);
    i1.query(5, OQLLexerTokenTypes.TOK_GE, 10, OQLLexerTokenTypes.TOK_LE,
        results, keysToRemove, context);
    assertEquals(5, results.size());
    for (int i = 6; i < 11;) {
      assertTrue(results.contains(region.get(i)));
      ++i;
    }

    results.clear();
    keysToRemove.clear();
    keysToRemove.add(5);
    keysToRemove.add(10);
    keysToRemove.add(7);
    i1.query(5, OQLLexerTokenTypes.TOK_GE, 10, OQLLexerTokenTypes.TOK_LE,
        results, keysToRemove, context);
    assertEquals(3, results.size());
    assertTrue(results.contains(region.get(6)));
    assertTrue(results.contains(region.get(8)));
    assertTrue(results.contains(region.get(9)));
  }

  /**
   * Tests the query method of RangeIndex which just contains not equal keys. That is if the where
   * clause looks like a != 7 and a != 8 & a!=9
   */
  @Test
  public void testQueryMethod_4() throws Exception {
    QueryService qs;
    qs = CacheUtils.getQueryService();
    AbstractIndex i1 =
        (AbstractIndex) qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID",
            SEPARATOR + "portfolios");
    AbstractIndex i2 = (AbstractIndex) qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status",
        SEPARATOR + "portfolios");
    AbstractIndex i3 = (AbstractIndex) qs.createIndex("status.toString()", IndexType.FUNCTIONAL,
        "status.toString", SEPARATOR + "portfolios");
    Set results = new HashSet();
    DefaultQuery q =
        new DefaultQuery("select * from " + SEPARATOR + "portfolios", CacheUtils.getCache(), false);
    q.setRemoteQuery(false);
    ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache(), q);
    bindIterators(context, SEPARATOR + "portfolios");

    Set keysToRemove = new HashSet();
    keysToRemove.add(5);
    i1.query(results, keysToRemove, context);
    assertEquals(11, results.size());
    for (int i = 0; i < 12;) {
      if (i != 5) {
        assertTrue(results.contains(region.get(i)));
      }
      ++i;
    }

    results.clear();
    keysToRemove.clear();
    keysToRemove.add(5);
    keysToRemove.add(8);
    i1.query(results, keysToRemove, context);
    assertEquals(10, results.size());
    for (int i = 0; i < 12;) {
      if (i != 5 && i != 8) {
        assertTrue(results.contains(region.get(i)));
      }
      ++i;
    }

    results.clear();
    keysToRemove.clear();
    keysToRemove.add("active");
    keysToRemove.add("inactive");
    i2.query(results, keysToRemove, context);
    assertEquals(2, results.size());
    for (int i = 10; i < 12;) {
      assertTrue(results.contains(region.get(i)));
      ++i;
    }
  }

  @Test
  public void testQueryRVMapMultipleEntries() throws Exception {
    Cache cache = CacheUtils.getCache();
    QueryService queryService = CacheUtils.getCache().getQueryService();
    CacheUtils.createRegion("TEST_REGION", null);
    queryService.createIndex("tr.nested.id.index", "nested.id",
        SEPARATOR + "TEST_REGION, nested IN nested_values");
    Query queryInSet = queryService.newQuery(
        "SELECT DISTINCT tr.id FROM " + SEPARATOR
            + "TEST_REGION tr, tr.nested_values nested WHERE nested.id IN SET ('1')");
    Query queryEquals = queryService.newQuery(
        "SELECT DISTINCT tr.id FROM " + SEPARATOR
            + "TEST_REGION tr, nested IN nested_values WHERE nested.id='1'");

    Object[] nested =
        new Object[] {cache.createPdxInstanceFactory("nested_1").writeString("id", "1").create(),
            cache.createPdxInstanceFactory("nested_2").writeString("id", "1").create(),
            cache.createPdxInstanceFactory("nested_3").writeString("id", "1").create(),
            cache.createPdxInstanceFactory("nested_4").writeString("id", "4").create()};

    PdxInstance record = cache.createPdxInstanceFactory("root").writeString("id", "2")
        .writeObjectArray("nested_values", nested).create();

    cache.getRegion("TEST_REGION").put("100", record);
    Index index = cache.getQueryService().getIndex(cache.getRegion("TEST_REGION"),
        "mdf.testRegion.nested.id");
    SelectResults queryResults = (SelectResults) queryEquals.execute();
    SelectResults inSetQueryResults = (SelectResults) queryInSet.execute();
    assertEquals(queryResults.size(), inSetQueryResults.size());
    assertTrue(inSetQueryResults.size() > 0);
  }

  private void bindIterators(ExecutionContext context, String string)
      throws TypeMismatchException, NameResolutionException {
    QCompiler compiler = new QCompiler();
    List compilerItrDefs = compiler.compileFromClause(string);
    context.newScope(0);
    for (Object itrDef : compilerItrDefs) {
      CompiledIteratorDef iterDef = (CompiledIteratorDef) itrDef;
      RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
      context.bindIterator(rIter);
    }
  }

}
