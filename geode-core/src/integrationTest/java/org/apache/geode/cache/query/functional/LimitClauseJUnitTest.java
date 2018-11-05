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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.data.Position;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.QueryObserver;
import org.apache.geode.cache.query.internal.QueryObserverAdapter;
import org.apache.geode.cache.query.internal.QueryObserverHolder;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.types.StructType;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class LimitClauseJUnitTest {

  Region region;

  QueryService qs;

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    region = CacheUtils.createRegion("portfolios", Portfolio.class);
    // Add 10 unique objects
    Position.cnt = 0;
    for (int i = 1; i < 11; ++i) {
      Object p = new Portfolio(i);
      GemFireCacheImpl.getInstance().getLogger().fine(p.toString());
      region.put(Integer.toString(i), p);
    }
    // Add 5 pairs of same Object starting from 11 to 20
    /*
     * for(int i=11; i< 21 ;) { region.put( Integer.toString(i), new Portfolio(i)); region.put(
     * Integer.toString(i+1), new Portfolio(i)); i +=2; }
     */
    qs = CacheUtils.getQueryService();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
    IndexManager indexManager = ((LocalRegion) region).getIndexManager();
    if (indexManager != null)
      indexManager.destroy();
  }


  @Test
  public void testLikeWithLimitWithParameter() throws Exception {
    String queryString =
        "SELECT DISTINCT entry FROM $1 entry WHERE entry.key like $2 ORDER BY entry.key LIMIT $3 ";
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i < 100; i++) {
      region.put("p" + i, new Portfolio(i));
    }

    Object[] params = new Object[3];
    params[0] = region.entrySet();
    params[1] = "p%";
    params[2] = 5;

    SelectResults results = (SelectResults) qs.newQuery(queryString).execute(params);
    assertEquals(5, results.size());
  }

  @Test
  public void testDistinctLimitWithParameter() throws Exception {
    String queryString = "SELECT DISTINCT entry FROM $1 entry LIMIT $2 ";
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i < 100; i++) {
      region.put(i, new Portfolio(i));
    }

    Object[] params = new Object[2];
    params[0] = region;
    params[1] = 5;

    SelectResults results = (SelectResults) qs.newQuery(queryString).execute(params);
    assertEquals(5, results.size());
  }

  @Test
  public void testLimitWithParameter() throws Exception {
    String queryString = "SELECT * from /portfolios1 LIMIT $1 ";
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i < 100; i++) {
      region.put(i, new Portfolio(i));
    }

    int limit = 5;
    Object[] params = new Object[1];
    params[0] = limit;

    SelectResults results = (SelectResults) qs.newQuery(queryString).execute(params);
    assertEquals(limit, results.size());

    limit = 1;
    params[0] = limit;
    results = (SelectResults) qs.newQuery(queryString).execute(params);
    assertEquals(limit, results.size());

    limit = 0;
    params[0] = limit;
    results = (SelectResults) qs.newQuery(queryString).execute(params);
    assertEquals(limit, results.size());

    limit = 10;
    params[0] = limit;
    results = (SelectResults) qs.newQuery(queryString).execute(params);
    assertEquals(limit, results.size());
  }

  @Test
  public void testLimitWithParameterNotSet() throws Exception {
    String queryString = "SELECT * from /portfolios1 LIMIT $1 ";
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i < 100; i++) {
      region.put(i, new Portfolio(i));
    }

    Object[] params = new Object[1];
    SelectResults results = (SelectResults) qs.newQuery(queryString).execute(params);
    assertEquals(region.size(), results.size());
  }

  @Test
  public void testLimitWithNullParameterObject() throws Exception {
    String queryString = "SELECT * from /portfolios1 LIMIT $1 ";
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i < 100; i++) {
      region.put(i, new Portfolio(i));
    }
    try {
      SelectResults results = (SelectResults) qs.newQuery(queryString).execute();
    } catch (IllegalArgumentException e) {
      // we expect an illegal argument exception
      assertTrue(true);
    }
  }

  // Test to see if bind argument limit is applied correctly for indexed and non indexed
  // queries.
  @Test
  public void testLimitWithParameterForNonAndIndexedQuery() throws Exception {
    String queryString = "SELECT * from /portfolios1 WHERE shortID = $1 LIMIT $2 ";
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i < 100; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = Integer.valueOf(i % 10).shortValue();
      region.put(i, p);
    }

    Object[] params = new Object[2];
    params[0] = 5;
    int[] limits = {1, 5, 0, 10};
    for (int i = 0; i < limits.length; i++) {
      params[1] = limits[i];

      SelectResults results = helpTestIndexForQuery(queryString, "shortID", "/portfolios1", params);
      assertEquals(limits[i], results.size());
      // clear out indexes for next query.
      qs.removeIndexes();
    }

  }

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for ResultBag wrapped by a SelectResults
   *
   * Tests ResultBag behaviour
   */
  @Test
  public void testLimitDistinctIterEvaluatedQueryForResultBag() {
    try {
      Query query;
      SelectResults result;
      String queryString = "SELECT DISTINCT * FROM /portfolios pf WHERE pf.ID > 0 limit 5";
      query = qs.newQuery(queryString);
      final int[] num = new int[1];
      num[0] = 0;
      QueryObserver old = QueryObserverHolder.setInstance(new QueryObserverAdapter() {
        public void afterIterationEvaluation(Object result) {
          num[0] += 1;
        }
      });
      result = (SelectResults) query.execute();
      assertEquals(5, num[0]);
      assertTrue(result instanceof SelectResults);
      assertEquals(5, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(5, wrapper.asSet().size());
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for ResultBag wrapped by a SelectResults . This test contains
   * projection attributes. Since the attribute is unique every time, the limit will be satisfied
   * with first 5 iterations
   *
   * Tests ResultBag behaviour
   *
   */
  @Test
  public void testLimitDistinctIterEvaluatedQueryForResultBagWithProjectionAttribute() {
    try {
      Query query;
      SelectResults result;
      String queryString = "SELECT DISTINCT pf.ID FROM /portfolios pf WHERE pf.ID > 0 limit 5";
      query = qs.newQuery(queryString);
      final int[] num = new int[1];
      num[0] = 0;
      QueryObserver old = QueryObserverHolder.setInstance(new QueryObserverAdapter() {
        public void afterIterationEvaluation(Object result) {
          num[0] += 1;
        }
      });
      result = (SelectResults) query.execute();
      assertEquals(5, num[0]);
      assertTrue(result instanceof SelectResults);
      assertEquals(5, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(5, wrapper.asSet().size());
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for ResultBag wrapped by a SelectResults if the iteration included
   * duplicate elements. If the distinct clause is present then duplicate elements even if
   * satisfying the where clause should not be considered as part of the resultset as distinct will
   * eliminate them
   *
   * Tests ResultBag behaviour
   */
  @Test
  public void testLimitDistinctIterEvaluatedQueryWithDuplicatesInIterationForResultBag() {
    try {
      Region region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
      // Add 5 pairs of same Object starting from 11 to 20
      for (int i = 11; i < 21;) {
        region1.put(Integer.toString(i), new Portfolio(i));
        region1.put(Integer.toString(i + 1), new Portfolio(i));
        i += 2;
      }
      Query query;
      SelectResults result;
      final int[] num = new int[1];
      final int[] numRepeat = new int[1];
      numRepeat[0] = 0;
      final Set data = new HashSet();
      num[0] = 0;
      // In the worst possible case all the unique values come in
      // consecutive order & hence only 5 iterations will yield the
      // result
      QueryObserver old = QueryObserverHolder.setInstance(new QueryObserverAdapter() {
        public void afterIterationEvaluation(Object result) {
          num[0] += 1;
        }

        public void beforeIterationEvaluation(CompiledValue ritr, Object currObject) {
          if (data.contains(currObject)) {
            numRepeat[0] += 1;
          } else {
            data.add(currObject);
          }
        }

      });
      String queryString = "SELECT DISTINCT * FROM /portfolios1  pf WHERE pf.ID > 10 limit 5";
      query = qs.newQuery(queryString);
      result = (SelectResults) query.execute();
      assertEquals((5 + numRepeat[0]), num[0]);
      assertTrue(result instanceof SelectResults);
      assertEquals(5, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(5, wrapper.asSet().size());

    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for ResultBag wrapped by a SelectResults if the iteration included
   * duplicate elements. If the distinct clause is present then duplicate elements even if
   * satisfying the where clause should not be considered as part as distinct will eliminate them.
   * This test validates the above behaviour if projection sttribute is present and the projection
   * attribute may be duplicate
   *
   * Tests ResultBag behaviour
   */
  @Test
  public void testLimitDistinctIterEvaluatedQueryWithDuplicatesInIterationWithProjectionAttributeForResultBag() {
    try {
      Region region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
      // Add 5 pairs of same Object starting from 11 to 20
      for (int i = 11; i < 21;) {
        region1.put(Integer.toString(i), new Portfolio(i));
        region1.put(Integer.toString(i + 1), new Portfolio(i));
        i += 2;
      }
      Query query;
      SelectResults result;
      final int[] num = new int[1];
      num[0] = 0;
      final int[] numRepeat = new int[1];
      numRepeat[0] = 0;
      final Set data = new HashSet();
      QueryObserver old = QueryObserverHolder.setInstance(new QueryObserverAdapter() {
        public void afterIterationEvaluation(Object result) {
          num[0] += 1;
        }

        public void beforeIterationEvaluation(CompiledValue ritr, Object currObject) {
          if (data.contains(currObject)) {
            numRepeat[0] += 1;
          } else {
            data.add(currObject);
          }
        }
      });
      String queryString = "SELECT DISTINCT pf.ID FROM /portfolios1  pf WHERE pf.ID > 10 limit 5";
      query = qs.newQuery(queryString);
      result = (SelectResults) query.execute();
      assertEquals((5 + numRepeat[0]), num[0]);
      assertTrue(result instanceof SelectResults);
      assertEquals(5, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(5, wrapper.asSet().size());

    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for StructBag wrapped by a SelectResults . This test contains
   * projection attributes. Since the attribute is unique every time, the limit will be satisfied
   * with first 5 iterations
   *
   * Tests StructBag behaviour
   *
   */
  @Test
  public void testLimitDistinctIterEvaluatedQueryForStructBagWithProjectionAttribute() {
    try {
      Query query;
      SelectResults result;
      query = qs.newQuery(
          "SELECT DISTINCT pf.ID, pf.createTime FROM /portfolios pf WHERE pf.ID > 0 limit 5");
      final int[] num = new int[1];
      num[0] = 0;
      QueryObserver old = QueryObserverHolder.setInstance(new QueryObserverAdapter() {
        public void afterIterationEvaluation(Object result) {
          num[0] += 1;
        }
      });
      result = (SelectResults) query.execute();
      assertEquals(5, num[0]);
      assertTrue(result instanceof SelectResults);
      assertEquals(5, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(5, wrapper.asSet().size());
      assertTrue(wrapper.getCollectionType().getElementType() instanceof StructType);
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for StructBag wrapped by a SelectResults
   *
   * Tests StructBag behaviour
   */
  @Test
  public void testLimitDistinctIterEvaluatedQueryForStructBag() {
    try {
      Query query;
      SelectResults result;
      String queryString =
          "SELECT DISTINCT * FROM /portfolios pf, pf.positions.values WHERE pf.ID > 0 limit 5";
      query = qs.newQuery(queryString);
      final int[] num = new int[1];
      num[0] = 0;
      QueryObserver old = QueryObserverHolder.setInstance(new QueryObserverAdapter() {
        public void afterIterationEvaluation(Object result) {
          num[0] += 1;
        }
      });
      result = (SelectResults) query.execute();
      assertEquals(5, num[0]);
      assertTrue(result instanceof SelectResults);
      assertEquals(5, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(5, wrapper.asSet().size());
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for StructBag wrapped by a SelectResults
   *
   * Tests StructBag behaviour
   */
  @Ignore
  @Test
  public void testLimitQueryForStructBagWithRangeIndex() {
    try {
      Query query;
      SelectResults result;
      String queryString =
          "SELECT * FROM /portfolios pf, pf.positions.values pos WHERE pf.ID > 1  AND pos.secId = 'GOOG' limit 1";
      query = qs.newQuery(queryString);

      MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
      QueryObserver old = QueryObserverHolder.setInstance(observer);

      Index index = qs.createIndex("idIndex", "pf.ID", "/portfolios pf");
      Index posindex =
          qs.createIndex("posIndex", "pos.secId", "/portfolios pf, pf.positions.values pos");
      assertNotNull(index);
      assertNotNull(posindex);
      result = (SelectResults) query.execute();
      assertEquals(1, result.size());
      assertFalse(observer.limitAppliedAtIndex);
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for StructBag wrapped by a SelectResults if the iteration included
   * duplicate elements. If the distinct clause is present then duplicate elements even if
   * satisfying the where clause should not be considered as part as distinct will eliminate them.
   * This test validates the above behaviour if projection attribute is present and the projection
   * attribute may be duplicate
   *
   * Tests StructBag behaviour
   */
  @Test
  public void testLimitDistinctIterEvaluatedQueryWithDuplicatesInIterationWithProjectionAttributeForStructBag() {
    try {
      Region region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
      // Add 5 pairs of same Object starting from 11 to 20
      for (int i = 11; i < 21;) {
        region1.put(Integer.toString(i), new Portfolio(i));
        region1.put(Integer.toString(i + 1), new Portfolio(i));
        i += 2;
      }
      Query query;
      SelectResults result;
      final int[] num = new int[1];
      num[0] = 0;
      final int[] numRepeat = new int[1];
      numRepeat[0] = 0;
      final Set data = new HashSet();

      QueryObserver old = QueryObserverHolder.setInstance(new QueryObserverAdapter() {
        public void afterIterationEvaluation(Object result) {
          num[0] += 1;
        }

        public void beforeIterationEvaluation(CompiledValue ritr, Object currObject) {
          if (data.contains(currObject)) {
            numRepeat[0] += 1;
          } else {
            data.add(currObject);
          }
        }
      });
      String queryString =
          "SELECT DISTINCT pf.ID , pf.createTime FROM /portfolios1  pf WHERE pf.ID > 10 limit 5";
      query = qs.newQuery(queryString);
      result = (SelectResults) query.execute();
      assertEquals((5 + numRepeat[0]), num[0]);
      assertTrue(result instanceof SelectResults);
      assertEquals(5, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(5, wrapper.asSet().size());

    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for StructBag wrapped by a SelectResults if the iteration included
   * duplicate elements. If the distinct clause is present then duplicate elements even if
   * satisfying the where clause should not be considered as part of the resultset as distinct will
   * eliminate them
   *
   * Tests StructBag behaviour
   */
  @Test
  public void testLimitDistinctIterEvaluatedQueryWithDuplicatesInIterationForStructBag() {
    try {
      Region region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
      // Add 5 pairs of same Object starting from 11 to 20
      for (int i = 11; i < 21;) {
        region1.put(Integer.toString(i), new Portfolio(i));
        region1.put(Integer.toString(i + 1), new Portfolio(i));
        i += 2;
      }
      Query query;
      SelectResults result;
      final int[] num = new int[1];
      num[0] = 0;
      final int[] numRepeat = new int[1];
      numRepeat[0] = 0;
      final Set data = new HashSet();
      QueryObserver old = QueryObserverHolder.setInstance(new QueryObserverAdapter() {
        public void afterIterationEvaluation(Object result) {
          num[0] += 1;
        }

        public void beforeIterationEvaluation(CompiledValue ritr, Object currObject) {
          if (data.contains(currObject)) {
            numRepeat[0] += 1;
          } else if (currObject instanceof Portfolio) {
            data.add(currObject);
          }
        }
      });
      String queryString =
          "SELECT DISTINCT * FROM /portfolios1  pf, pf.collectionHolderMap.keySet  WHERE pf.ID > 10 limit 20";
      query = qs.newQuery(queryString);
      result = (SelectResults) query.execute();
      assertEquals((20 + 4 * numRepeat[0]), num[0]);
      assertTrue(result instanceof SelectResults);
      assertEquals(20, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(20, wrapper.asSet().size());

    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  // Asif:Test the index results behaviour also

  // Shobhit: Testing the Limit behavior now with indexes for all above test cases.

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for ResultBag wrapped by a SelectResults
   *
   * Tests ResultBag behaviour
   */
  @Test
  public void testLimitDistinctIterEvaluatedQueryForResultBagWithIndex() {
    try {
      Query query;
      SelectResults result;
      String queryString = "SELECT DISTINCT * FROM /portfolios pf WHERE pf.ID > 0 limit 5";
      query = qs.newQuery(queryString);
      MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
      QueryObserver old = QueryObserverHolder.setInstance(observer);
      Index index = qs.createIndex("idIndex", "pf.ID", "/portfolios pf");
      assertNotNull(index);
      result = (SelectResults) query.execute();
      assertTrue(result instanceof SelectResults);
      assertEquals(5, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(5, wrapper.asSet().size());
      assertTrue(observer.limitAppliedAtIndex);
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for ResultBag wrapped by a SelectResults . This test contains
   * projection attributes. Since the attribute is unique every time, the limit will be satisfied
   * with first 5 iterations
   *
   * Tests ResultBag behaviour
   *
   */
  @Test
  public void testLimitDistinctIterEvaluatedQueryForResultBagWithProjectionAttributeWithIndex() {
    try {
      Query query;
      SelectResults result;
      String queryString = "SELECT DISTINCT pf.ID FROM /portfolios pf WHERE pf.ID > 0 limit 5";
      query = qs.newQuery(queryString);

      MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
      QueryObserver old = QueryObserverHolder.setInstance(observer);
      Index index = qs.createIndex("idIndex", "pf.ID", "/portfolios pf");
      assertNotNull(index);

      result = (SelectResults) query.execute();
      assertTrue(result instanceof SelectResults);
      assertEquals(5, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(5, wrapper.asSet().size());
      assertTrue(observer.limitAppliedAtIndex);
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for ResultBag wrapped by a SelectResults if the iteration included
   * duplicate elements. If the distinct clause is present then duplicate elements even if
   * satisfying the where clause should not be considered as part of the resultset as distinct will
   * eliminate them
   *
   * Tests ResultBag behaviour
   */
  @Test
  public void testLimitDistinctIterEvaluatedQueryWithDuplicatesInIterationForResultBagWithIndex() {
    try {
      Region region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
      // Add 5 pairs of same Object starting from 11 to 20
      for (int i = 11; i < 21;) {
        region1.put(Integer.toString(i), new Portfolio(i));
        region1.put(Integer.toString(i + 1), new Portfolio(i));
        i += 2;
      }
      Query query;
      SelectResults result;

      MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
      QueryObserver old = QueryObserverHolder.setInstance(observer);
      Index index = qs.createIndex("idIndex", "pf.ID", "/portfolios1 pf");
      assertNotNull(index);

      String queryString = "SELECT DISTINCT * FROM /portfolios1  pf WHERE pf.ID > 10 limit 5";
      query = qs.newQuery(queryString);
      result = (SelectResults) query.execute();
      assertTrue(result instanceof SelectResults);
      assertEquals(5, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(5, wrapper.asSet().size());
      assertTrue(observer.limitAppliedAtIndex);
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for ResultBag wrapped by a SelectResults if the iteration included
   * duplicate elements. If the distinct clause is present then duplicate elements even if
   * satisfying the where clause should not be considered as part as distinct will eliminate them.
   * This test validates the above behaviour if projection sttribute is present and the projection
   * attribute may be duplicate
   *
   * Tests ResultBag behaviour
   */
  @Test
  public void testLimitDistinctIterEvaluatedQueryWithDuplicatesInIterationWithProjectionAttributeForResultBagWithIndex() {
    try {
      Region region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
      // Add 5 pairs of same Object starting from 11 to 20
      for (int i = 11; i < 21;) {
        region1.put(Integer.toString(i), new Portfolio(i));
        region1.put(Integer.toString(i + 1), new Portfolio(i));
        i += 2;
      }
      Query query;
      SelectResults result;

      MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
      QueryObserver old = QueryObserverHolder.setInstance(observer);

      Index index = qs.createIndex("idIndex", "pf.ID", "/portfolios1 pf");
      assertNotNull(index);
      String queryString = "SELECT DISTINCT pf.ID FROM /portfolios1  pf WHERE pf.ID > 10 limit 5";
      query = qs.newQuery(queryString);
      result = (SelectResults) query.execute();
      assertTrue(result instanceof SelectResults);
      assertEquals(5, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(5, wrapper.asSet().size());
      assertTrue(observer.limitAppliedAtIndex);
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for StructBag wrapped by a SelectResults . This test contains
   * projection attributes. Since the attribute is unique every time, the limit will be satisfied
   * with first 5 iterations
   *
   * Tests StructBag behaviour
   *
   */
  @Test
  public void testLimitDistinctIterEvaluatedQueryForStructBagWithProjectionAttributeWithIndex() {
    try {
      Query query;
      SelectResults result;
      query = qs.newQuery(
          "SELECT DISTINCT pf.ID, pf.createTime FROM /portfolios pf WHERE pf.ID > 0 limit 5");

      MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
      QueryObserver old = QueryObserverHolder.setInstance(observer);

      Index index = qs.createIndex("idIndex", "pf.ID", "/portfolios pf");
      assertNotNull(index);
      result = (SelectResults) query.execute();
      assertTrue(result instanceof SelectResults);
      assertEquals(5, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(5, wrapper.asSet().size());
      assertTrue(wrapper.getCollectionType().getElementType() instanceof StructType);
      assertTrue(observer.limitAppliedAtIndex);
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for StructBag wrapped by a SelectResults
   *
   * Tests StructBag behaviour
   */
  @Test
  public void testLimitDistinctIterEvaluatedQueryForStructBagWithIndex() {
    try {
      Query query;
      SelectResults result;
      String queryString =
          "SELECT DISTINCT * FROM /portfolios pf, pf.positions.values WHERE pf.ID > 0 limit 5";
      query = qs.newQuery(queryString);

      MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
      QueryObserver old = QueryObserverHolder.setInstance(observer);

      Index index = qs.createIndex("idIndex", "pf.ID", "/portfolios pf, pf.positions.values");
      assertNotNull(index);
      result = (SelectResults) query.execute();
      assertTrue(result instanceof SelectResults);
      assertEquals(5, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(5, wrapper.asSet().size());
      // currently this is false because we disabled limit application at the range index level
      assertFalse(observer.limitAppliedAtIndex);
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for StructBag wrapped by a SelectResults if the iteration included
   * duplicate elements. If the distinct clause is present then duplicate elements even if
   * satisfying the where clause should not be considered as part as distinct will eliminate them.
   * This test validates the above behaviour if projection attribute is present and the projection
   * attribute may be duplicate
   *
   * Tests StructBag behaviour
   */
  @Test
  public void testLimitDistinctIterEvaluatedQueryWithDuplicatesInIterationWithProjectionAttributeForStructBagWithIndex() {
    try {
      Region region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
      // Add 5 pairs of same Object starting from 11 to 20
      for (int i = 11; i < 21;) {
        region1.put(Integer.toString(i), new Portfolio(i));
        region1.put(Integer.toString(i + 1), new Portfolio(i));
        i += 2;
      }
      Query query;
      SelectResults result;

      MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
      QueryObserver old = QueryObserverHolder.setInstance(observer);
      Index index = qs.createIndex("idIndex", "pf.ID", "/portfolios1  pf");
      assertNotNull(index);
      String queryString =
          "SELECT DISTINCT pf.ID , pf.createTime FROM /portfolios1  pf WHERE pf.ID > 10 limit 5";
      query = qs.newQuery(queryString);
      result = (SelectResults) query.execute();
      assertTrue(result instanceof SelectResults);
      assertEquals(5, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(5, wrapper.asSet().size());
      assertTrue(observer.limitAppliedAtIndex);
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  /**
   * Tests the limit functionality for Iter evaluated query with distinct clause This tests the
   * basic limit functionality for StructBag wrapped by a SelectResults if the iteration included
   * duplicate elements. If the distinct clause is present then duplicate elements even if
   * satisfying the where clause should not be considered as part of the resultset as distinct will
   * eliminate them
   *
   * Tests StructBag behaviour
   */
  @Test
  public void testLimitDistinctIterEvaluatedQueryWithDuplicatesInIterationForStructBagWithIndex() {
    try {
      Region region1 = CacheUtils.createRegion("portfolios1", Portfolio.class);
      // Add 5 pairs of same Object starting from 11 to 20
      for (int i = 11; i < 21;) {
        region1.put(Integer.toString(i), new Portfolio(i));
        region1.put(Integer.toString(i + 1), new Portfolio(i));
        i += 2;
      }
      Query query;
      SelectResults result;

      MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
      QueryObserver old = QueryObserverHolder.setInstance(observer);

      Index index =
          qs.createIndex("idIndex", "pf.ID", "/portfolios1  pf, pf.collectionHolderMap.keySet");
      assertNotNull(index);

      String queryString =
          "SELECT DISTINCT * FROM /portfolios1  pf, pf.collectionHolderMap.keySet  WHERE pf.ID > 10 limit 20";
      query = qs.newQuery(queryString);
      result = (SelectResults) query.execute();
      assertTrue(result instanceof SelectResults);
      assertEquals(20, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(20, wrapper.asSet().size());
      // currently this is false because we disabled limit application at the range index level
      assertFalse(observer.limitAppliedAtIndex);
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  /**
   * Tests the limit functionality for query which has three conditions with AND operator and two
   * conditions can be evaluated using index but not the 3rd one.
   *
   * This tests the limit application on intermediate results from Index which should not be true in
   * this test.
   *
   */
  @Test
  public void testLimitDistinctQueryWithTwoCondButOneIndex() {
    try {
      Query query;
      SelectResults result;
      Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
      for (int i = 1; i < 100; i++) {
        Portfolio p = new Portfolio(i);
        if (i < 50)
          p.status = "active";
        region.put(Integer.toString(i), p);
      }

      // Create Index on ID
      Index index = qs.createIndex("idIndex", "ID", "/portfolios1");
      assertNotNull(index);
      String queryString =
          "select DISTINCT * from /portfolios1 where status ='inactive' AND (ID > 0 AND ID < 100) limit 10";
      query = qs.newQuery(queryString);
      result = (SelectResults) query.execute();
      assertTrue(result instanceof SelectResults);
      assertEquals(10, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(10, wrapper.asSet().size());
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  @Test
  public void testLimitDistinctQueryWithTwoCondWithTwoIndex() {
    try {
      Query query;
      SelectResults result;
      Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
      for (int i = 1; i < 100; i++) {
        Portfolio p = new Portfolio(i);
        if (i < 50)
          p.status = "active";
        region.put(Integer.toString(i), p);
      }

      // Create Index on ID
      Index index = qs.createIndex("idIndex", "ID", "/portfolios1");
      assertNotNull(index);
      index = qs.createIndex("statusIndex", "status", "/portfolios1");
      assertNotNull(index);
      MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
      QueryObserver old = QueryObserverHolder.setInstance(observer);

      String queryString =
          "select DISTINCT * from /portfolios1 where status ='inactive' AND (ID > 0 AND ID < 100) limit 10";
      query = qs.newQuery(queryString);
      result = (SelectResults) query.execute();
      assertTrue(result instanceof SelectResults);
      assertEquals(10, result.size());
      SelectResults wrapper = (SelectResults) result;
      assertEquals(10, wrapper.asSet().size());
      assertFalse(observer.limitAppliedAtIndex && observer.indexName.equals("idIndex"));
      assertTrue(observer.limitAppliedAtIndex && observer.indexName.equals("statusIndex"));
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  @Test
  public void testLimitNonDistinctQueryWithTwoCondButOneIndex() {
    try {
      Query query;
      SelectResults result;
      Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
      for (int i = 1; i < 100; i++) {
        Portfolio p = new Portfolio(i);
        if (i < 50)
          p.status = "active";
        region.put(Integer.toString(i), p);
      }

      MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
      QueryObserver old = QueryObserverHolder.setInstance(observer);

      // Create Index on ID
      Index index = qs.createIndex("idIndex", "ID", "/portfolios1");
      assertNotNull(index);
      String[] queryString = new String[] {
          "select * from /portfolios1 where status ='inactive' AND (ID > 0 AND ID < 100) limit 10",
          "select * from /portfolios1 where (status > 'inactiva' AND status < 'xyz') AND (ID > 0 AND ID < 100) limit 10",
          "select * from /portfolios1 where (status > 'inactiva' AND status < 'xyz') AND (ID > 0 AND ID < 100) AND (\"type\"='type1' OR \"type\"='type2') limit 10",};
      for (String qstr : queryString) {
        query = qs.newQuery(qstr);
        result = (SelectResults) query.execute();
        assertEquals(10, result.size());
        assertFalse(observer.limitAppliedAtIndex);
      }
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  @Test
  public void testLimitNonDistinctQueryWithTwoCondTwoIndex() {
    try {
      Query query;
      SelectResults result;
      Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
      for (int i = 1; i < 100; i++) {
        Portfolio p = new Portfolio(i);
        if (i < 50)
          p.status = "active";
        region.put(Integer.toString(i), p);
      }

      MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
      QueryObserver old = QueryObserverHolder.setInstance(observer);

      // Create Index on ID
      Index index = qs.createIndex("idIndex", "ID", "/portfolios1");
      assertNotNull(index);
      index = qs.createIndex("statusIndex", "status", "/portfolios1");
      assertNotNull(index);
      String[] queryString = new String[] {
          "select * from /portfolios1 where status ='inactive' AND (ID > 0 AND ID < 100) limit 10",};
      for (String qstr : queryString) {
        query = qs.newQuery(qstr);
        result = (SelectResults) query.execute();
        assertEquals(10, result.size());
        assertTrue(observer.limitAppliedAtIndex && observer.indexName.equals("statusIndex"));
      }
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  @Test
  public void testLimitNonDistinctQueryWithTwoRangeCondTwoIndex() {
    try {
      Query query;
      SelectResults result;
      Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
      for (int i = 1; i < 100; i++) {
        Portfolio p = new Portfolio(i);
        if (i < 50)
          p.status = "active";
        region.put(Integer.toString(i), p);
      }

      MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
      QueryObserver old = QueryObserverHolder.setInstance(observer);

      // Create Index on ID
      Index index = qs.createIndex("idIndex", "ID", "/portfolios1");
      assertNotNull(index);
      index = qs.createIndex("statusIndex", "status", "/portfolios1");
      assertNotNull(index);
      String[] queryString = new String[] {
          "select * from /portfolios1 where (status > 'inactiva' AND status < 'xyz') AND (ID > 70 AND ID < 100) limit 10",
          "select * from /portfolios1 where (status > 'inactiva' AND status < 'xyz') AND (ID > 60 AND ID < 100) AND (\"type\"='type1' OR \"type\"='type2') limit 10",};
      for (String qstr : queryString) {
        query = qs.newQuery(qstr);
        result = (SelectResults) query.execute();
        assertEquals(10, result.size());
        assertFalse(observer.limitAppliedAtIndex);
      }
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  @Test
  public void testLimitDistinctQueryWithDuplicateValues() {
    try {
      Query query;
      SelectResults result;
      Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
      for (int i = 1; i < 10; i++) {
        Portfolio p = new Portfolio(i);
        if (i == 2)
          p = new Portfolio(1);
        region.put(Integer.toString(i), p);
      }

      // Create Index on ID
      Index index = qs.createIndex("idIndex", "ID", "/portfolios1");
      assertNotNull(index);
      String queryString =
          "select DISTINCT * from /portfolios1 where status ='inactive' AND ID > 0 limit 2";
      query = qs.newQuery(queryString);
      result = (SelectResults) query.execute();
      assertEquals(2, result.size());
    } catch (Exception e) {
      CacheUtils.getLogger().error(e);
      fail(e.toString());
    } finally {
      QueryObserverHolder.setInstance(new QueryObserverAdapter());
    }
  }

  @Test
  public void testNotApplyingLimitAtIndexLevelForMultiIndexAndClauseUsage() throws Exception {
    // try {
    Query query;
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 15; i > 0; i--) {
      Portfolio p = new Portfolio(i);
      // CacheUtils.log(p);
      p.positions.clear();
      p.positions.put("IBM", new Position("IBM", i));
      region.put("KEY" + i, p);
    }

    String queryString =
        "<trace>SELECT * FROM /portfolios1 P, P.positions.values POS WHERE P.ID > 5 AND POS.secId = 'IBM' LIMIT 5";
    query = qs.newQuery(queryString);
    SelectResults resultsNoIndex = (SelectResults) query.execute();

    // Create Index on ID and secId
    Index secIndex =
        qs.createIndex("secIdIndex", "pos.secId", "/portfolios1 p, p.positions.values pos");
    Index idIndex = qs.createIndex("idIndex", IndexType.FUNCTIONAL, "P.ID", "/portfolios1 P");

    assertNotNull(secIndex);
    assertNotNull(idIndex);
    SelectResults resultsWithIndex = (SelectResults) query.execute();

    assertEquals(resultsNoIndex.size(), resultsWithIndex.size());
  }


  @Test
  public void testNotLimitAtIndexLevelForMultiSingleIndexAndClauseUsage() throws Exception {
    // try {
    Query query;
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 15; i > 0; i--) {
      Portfolio p = new Portfolio(i);
      // CacheUtils.log(p);
      p.positions.clear();
      p.positions.put("IBM", new Position("IBM", i));
      region.put("KEY" + i, p);
    }

    String queryString =
        "<trace>SELECT * FROM /portfolios1 P, P.positions.values POS WHERE P.ID > 4 and P.ID < 11 AND P.ID != 8 LIMIT 5";
    query = qs.newQuery(queryString);
    SelectResults resultsNoIndex = (SelectResults) query.execute();

    // Create Index on ID and secId
    Index secIndex =
        qs.createIndex("secIdIndex", "pos.secId", "/portfolios1 p, p.positions.values pos");
    Index idIndex = qs.createIndex("idIndex", IndexType.FUNCTIONAL, "P.ID",
        "/portfolios1 P, P.positions.values pos");

    // assertNotNull(secIndex);
    assertNotNull(idIndex);
    SelectResults resultsWithIndex = (SelectResults) query.execute();

    assertEquals(resultsNoIndex.size(), resultsWithIndex.size());
  }


  @Test
  public void testNotLimitAtIndexLevelForMultiSingleIndexOrClauseUsage() throws Exception {
    // try {
    Query query;
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 15; i > 0; i--) {
      Portfolio p = new Portfolio(i);
      // CacheUtils.log(p);
      p.positions.clear();
      p.positions.put("IBM", new Position("IBM", i));
      region.put("KEY" + i, p);
    }

    String queryString =
        "<trace>SELECT * FROM /portfolios1 P, P.positions.values POS WHERE P.ID < 4 OR P.ID > 11 AND P.ID != 13 LIMIT 5";
    query = qs.newQuery(queryString);
    SelectResults resultsNoIndex = (SelectResults) query.execute();

    // Create Index on ID and secId
    Index secIndex =
        qs.createIndex("secIdIndex", "pos.secId", "/portfolios1 p, p.positions.values pos");
    Index idIndex = qs.createIndex("idIndex", IndexType.FUNCTIONAL, "P.ID",
        "/portfolios1 P, P.positions.values pos");

    // assertNotNull(secIndex);
    assertNotNull(idIndex);
    SelectResults resultsWithIndex = (SelectResults) query.execute();

    assertEquals(resultsNoIndex.size(), resultsWithIndex.size());
  }

  @Test
  public void testLimitJunctionOnCompactRangeIndexedFieldWithAndClauseOnNonIndexedField()
      throws Exception {
    Query query;
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i <= 15; i++) {
      Portfolio p = new Portfolio(i);
      p.positions.clear();
      p.positions.put("IBM", new Position("IBM", i));
      region.put("KEY" + i, p);
    }

    for (int i = 16; i < 21; i++) {
      Portfolio p = new Portfolio(i);
      p.positions.clear();
      p.positions.put("VMW", new Position("VMW", i));
      region.put("KEY" + i, p);
    }

    MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
    QueryObserver old = QueryObserverHolder.setInstance(observer);

    // Create Index on ID
    Index idIndex = qs.createIndex("idIndex", IndexType.FUNCTIONAL, "P.ID", "/portfolios1 P");

    String queryString =
        "SELECT * FROM /portfolios1 P, P.positions.values POS WHERE P.ID > 9 AND P.ID < 21 AND POS.secId = 'VMW' LIMIT 5";
    query = qs.newQuery(queryString);

    assertNotNull(idIndex);
    SelectResults resultsWithIndex = (SelectResults) query.execute();
    assertFalse(observer.limitAppliedAtIndex);
    assertEquals(5, resultsWithIndex.size());
  }

  @Test
  public void testLimitJunctionOnRangeIndexedFieldWithAndClauseOnNonIndexedField()
      throws Exception {
    Query query;
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i <= 15; i++) {
      Portfolio p = new Portfolio(i);
      p.positions.clear();
      p.positions.put("IBM", new Position("IBM", i));
      region.put("KEY" + i, p);
    }

    for (int i = 16; i < 21; i++) {
      Portfolio p = new Portfolio(i);
      p.positions.clear();
      p.positions.put("VMW", new Position("VMW", i));
      region.put("KEY" + i, p);
    }

    MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
    QueryObserver old = QueryObserverHolder.setInstance(observer);

    // Create Index on ID
    Index idIndex = qs.createIndex("idIndex", IndexType.FUNCTIONAL, "P.ID",
        "/portfolios1 P, P.positions.values POS");

    String queryString =
        "SELECT * FROM /portfolios1 P, P.positions.values POS WHERE P.ID > 9 AND P.ID < 21 AND POS.secId = 'VMW' LIMIT 5";
    query = qs.newQuery(queryString);

    assertNotNull(idIndex);
    SelectResults resultsWithIndex = (SelectResults) query.execute();
    assertFalse(observer.limitAppliedAtIndex);
    assertEquals(5, resultsWithIndex.size());
  }

  // This is one where we are no longer applying index due to multiple index usage but old code
  // would. should take a look and see how/ or why old code can
  @Test
  public void testLimitJunctionOnRangeIndexedFieldWithAndClauseCompactRangeIndexedField()
      throws Exception {
    Query query;
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i <= 15; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = 1;
      p.positions.clear();
      p.positions.put("IBM", new Position("IBM", i));
      region.put("KEY" + i, p);
    }

    for (int i = 21; i < 100; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = 2;
      p.positions.clear();
      p.positions.put("VMW", new Position("VMW", i));
      region.put("KEY" + i, p);
    }

    for (int i = 16; i < 21; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = 2;
      p.positions.clear();
      p.positions.put("VMW", new Position("VMW", i));
      region.put("KEY" + i, p);
    }
    // MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
    // QueryObserver old = QueryObserverHolder.setInstance(observer);

    // Create Index on ID
    Index idIndex = qs.createIndex("idIndex", IndexType.FUNCTIONAL, "P.ID",
        "/portfolios1 P, P.positions.values POS");
    Index shortIdIndex =
        qs.createIndex("shortIdIndex", IndexType.FUNCTIONAL, "P.shortID", "/portfolios1 P");

    String queryString =
        "<trace>SELECT * FROM /portfolios1 P WHERE P.ID > 9 AND P.ID < 21 AND P.shortID = 2 LIMIT 5";
    query = qs.newQuery(queryString);

    assertNotNull(idIndex);
    SelectResults resultsWithIndex = (SelectResults) query.execute();
    // assertFalse(observer.limitAppliedAtIndex);
    assertEquals(5, resultsWithIndex.size());
  }

  @Test
  public void testLimitJunctionOnRangeIndexedFieldWithAndClauseRangeIndexedField()
      throws Exception {
    Query query;
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i <= 15; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = 1;
      p.positions.clear();
      p.positions.put("IBM", new Position("IBM", i));
      region.put("KEY" + i, p);
    }

    for (int i = 21; i < 100; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = 2;
      p.positions.clear();
      p.positions.put("VMW", new Position("VMW", i));
      region.put("KEY" + i, p);
    }

    for (int i = 16; i < 21; i++) {
      Portfolio p = new Portfolio(i);
      p.shortID = 2;
      p.positions.clear();
      p.positions.put("VMW", new Position("VMW", i));
      region.put("KEY" + i, p);
    }

    MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
    QueryObserver old = QueryObserverHolder.setInstance(observer);

    // Create Index on ID
    Index idIndex = qs.createIndex("idIndex", IndexType.FUNCTIONAL, "P.ID",
        "/portfolios1 P, P.positions.values POS");
    Index shortIdIndex = qs.createIndex("shortIdIndex", IndexType.FUNCTIONAL, "P.shortID",
        "/portfolios1 P, P.positions.values POS");

    String queryString =
        "SELECT * FROM /portfolios1 P WHERE P.ID > 9 AND P.ID < 21 AND P.shortID = 2 LIMIT 5";
    query = qs.newQuery(queryString);

    assertNotNull(idIndex);
    SelectResults resultsWithIndex = (SelectResults) query.execute();
    // assertFalse(observer.limitAppliedAtIndex);
    assertEquals(5, resultsWithIndex.size());
  }

  @Test
  public void testLimitOnEqualsCompactRangeIndexedFieldWithAndClauseNonIndexedField()
      throws Exception {
    Query query;
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i <= 15; i++) {
      Portfolio p = new Portfolio(10);
      p.shortID = 1;
      p.positions.clear();
      p.positions.put("IBM", new Position("IBM", i));
      region.put("KEY" + i, p);
    }

    for (int i = 16; i < 21; i++) {
      Portfolio p = new Portfolio(10);
      p.shortID = 2;
      p.positions.clear();
      p.positions.put("VMW", new Position("VMW", i));
      region.put("KEY" + i, p);
    }

    MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
    QueryObserver old = QueryObserverHolder.setInstance(observer);

    // Create Index on ID
    Index idIndex = qs.createIndex("idIndex", "P.ID", "/portfolios1 P");

    String queryString = "SELECT * FROM /portfolios1 P WHERE P.ID = 10 AND P.shortID = 2 LIMIT 5";
    query = qs.newQuery(queryString);

    assertNotNull(idIndex);
    SelectResults resultsWithIndex = (SelectResults) query.execute();
    assertTrue(observer.limitAppliedAtIndex);
    assertEquals(5, resultsWithIndex.size());
  }


  // This is one where the old code could apply limit but we do not. Should investigate... index
  // being used is on ShortId
  @Test
  public void testLimitOnEqualsCompactRangeIndexedFieldWithAndClauseCompactRangeIndexedField()
      throws Exception {
    Query query;
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i <= 15; i++) {
      Portfolio p = new Portfolio(10);
      p.shortID = 1;
      p.positions.clear();
      p.positions.put("IBM", new Position("IBM", i));
      region.put("KEY" + i, p);
    }

    for (int i = 16; i < 20; i++) {
      Portfolio p = new Portfolio(10);
      p.shortID = 2;
      p.positions.clear();
      p.positions.put("VMW", new Position("VMW", i));
      region.put("KEY" + i, p);
    }

    for (int i = 20; i < 24; i++) {
      Portfolio p = new Portfolio(11);
      p.shortID = 2;
      p.positions.clear();
      p.positions.put("VMW", new Position("VMW", i));
      region.put("KEY" + i, p);
    }
    for (int i = 24; i < 28; i++) {
      Portfolio p = new Portfolio(10);
      p.shortID = (short) (i % 3);
      p.positions.clear();
      p.positions.put("VMW", new Position("VMW", i));
      region.put("KEY" + i, p);
    }
    for (int i = 100; i < 200; i++) {
      Portfolio p = new Portfolio(10);
      p.shortID = 0;
      p.positions.clear();
      p.positions.put("VMW", new Position("VMW", i));
      region.put("KEY" + i, p);
    }
    // MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
    // QueryObserver old = QueryObserverHolder.setInstance(observer);

    // Create Index on ID
    Index idIndex = qs.createIndex("idIndex", "P.ID", "/portfolios1 P");
    Index shortIdIndex = qs.createIndex("shortIdIndex", "P.shortID", "/portfolios1 P");

    String queryString =
        "<trace>SELECT * FROM /portfolios1 P WHERE P.ID = 10 AND P.shortID = 2 LIMIT 5";
    query = qs.newQuery(queryString);

    assertNotNull(idIndex);
    SelectResults resultsWithIndex = (SelectResults) query.execute();
    // assertFalse(observer.limitAppliedAtIndex);
    assertEquals(5, resultsWithIndex.size());
  }

  // This is one where limit is applied at index for old code but we do not apply
  @Test
  public void testLimitOnEqualsRangeIndexedFieldWithAndClauseCompactRangeIndexedField()
      throws Exception {
    Query query;
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i <= 15; i++) {
      Portfolio p = new Portfolio(10);
      p.shortID = 1;
      p.positions.clear();
      p.positions.put("IBM", new Position("IBM", i));
      region.put("KEY" + i, p);
    }

    for (int i = 16; i < 21; i++) {
      Portfolio p = new Portfolio(10);
      p.shortID = 2;
      p.positions.clear();
      p.positions.put("VMW", new Position("VMW", i));
      region.put("KEY" + i, p);
    }

    MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
    QueryObserver old = QueryObserverHolder.setInstance(observer);

    // Create Index on ID
    Index idIndex = qs.createIndex("idIndex", "P.ID", "/portfolios1 P, P.positions.values POS");
    Index shortIdIndex = qs.createIndex("shortIdIndex", "P.shortID", "/portfolios1 P");

    String queryString = "SELECT * FROM /portfolios1 P WHERE P.ID = 10 AND P.shortID = 2 LIMIT 5";
    query = qs.newQuery(queryString);

    assertNotNull(idIndex);
    SelectResults resultsWithIndex = (SelectResults) query.execute();
    // assertFalse(observer.limitAppliedAtIndex);
    assertEquals(5, resultsWithIndex.size());
  }


  // This is one where we do not apply limit at index but old code does
  @Test
  public void testLimitOnJunctionWithCompactRangeIndexedFieldWithAndClauseJunctionCompactRangeIndexedField()
      throws Exception {
    Query query;
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i <= 15; i++) {
      Portfolio p = new Portfolio(10);
      p.shortID = 1;
      p.positions.clear();
      p.positions.put("IBM", new Position("IBM", i));
      region.put("KEY" + i, p);
    }

    for (int i = 16; i < 21; i++) {
      Portfolio p = new Portfolio(10);
      p.shortID = 2;
      p.positions.clear();
      p.positions.put("VMW", new Position("VMW", i));
      region.put("KEY" + i, p);
    }

    MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
    QueryObserver old = QueryObserverHolder.setInstance(observer);

    // Create Index on ID
    Index idIndex = qs.createIndex("idIndex", "P.ID", "/portfolios1 P");
    Index shortIdIndex = qs.createIndex("shortIdIndex", "P.shortID", "/portfolios1 P");

    String queryString =
        "SELECT * FROM /portfolios1 P WHERE P.ID > 9 AND P.ID < 20 AND P.shortID > 1 AND P.shortID < 3 LIMIT 5";
    query = qs.newQuery(queryString);

    assertNotNull(idIndex);
    SelectResults resultsWithIndex = (SelectResults) query.execute();
    // assertFalse(observer.limitAppliedAtIndex);
    assertEquals(5, resultsWithIndex.size());
  }

  @Test
  public void testLimitOnJunctionWithCompactRangeIndexedFieldWithAndClauseJunctionNonIndexedField()
      throws Exception {
    Query query;
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i <= 15; i++) {
      Portfolio p = new Portfolio(10);
      p.shortID = 1;
      p.positions.clear();
      p.positions.put("IBM", new Position("IBM", i));
      region.put("KEY" + i, p);
    }

    for (int i = 16; i < 21; i++) {
      Portfolio p = new Portfolio(10);
      p.shortID = 2;
      p.positions.clear();
      p.positions.put("VMW", new Position("VMW", i));
      region.put("KEY" + i, p);
    }

    MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
    QueryObserver old = QueryObserverHolder.setInstance(observer);

    // Create Index on ID
    Index idIndex = qs.createIndex("idIndex", "P.ID", "/portfolios1 P");

    String queryString =
        "SELECT * FROM /portfolios1 P WHERE P.ID > 9 AND P.ID < 20 AND P.shortID > 1 AND P.shortID < 3 LIMIT 5";
    query = qs.newQuery(queryString);

    assertNotNull(idIndex);
    SelectResults resultsWithIndex = (SelectResults) query.execute();
    assertFalse(observer.limitAppliedAtIndex);
    assertEquals(5, resultsWithIndex.size());
  }

  @Test
  public void testLimitOnCompactRangeIndexedFieldWithAndClauseJunctionNonIndexedField()
      throws Exception {
    Query query;
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i <= 15; i++) {
      Portfolio p = new Portfolio(10);
      p.shortID = 1;
      p.positions.clear();
      p.positions.put("IBM", new Position("IBM", i));
      region.put("KEY" + i, p);
    }

    for (int i = 16; i < 21; i++) {
      Portfolio p = new Portfolio(10);
      p.shortID = 2;
      p.positions.clear();
      p.positions.put("VMW", new Position("VMW", i));
      region.put("KEY" + i, p);
    }

    MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
    QueryObserver old = QueryObserverHolder.setInstance(observer);

    // Create Index on ID
    Index idIndex = qs.createIndex("idIndex", "P.ID", "/portfolios1 P");

    String queryString =
        "SELECT * FROM /portfolios1 P WHERE P.ID = 10 AND P.shortID > 1 AND P.shortID < 3 LIMIT 5";
    query = qs.newQuery(queryString);

    assertNotNull(idIndex);
    SelectResults resultsWithIndex = (SelectResults) query.execute();
    assertTrue(observer.limitAppliedAtIndex);
    assertEquals(5, resultsWithIndex.size());
  }


  @Test
  public void testLimitOnJunctionWithCompactRangeIndexedFieldWithAndOnCompactRangeIndexedField()
      throws Exception {
    Query query;
    SelectResults result;
    Region region = CacheUtils.createRegion("portfolios1", Portfolio.class);
    for (int i = 0; i <= 15; i++) {
      Portfolio p = new Portfolio(10);
      p.shortID = 1;
      p.positions.clear();
      p.positions.put("IBM", new Position("IBM", i));
      region.put("KEY" + i, p);
    }

    for (int i = 16; i < 21; i++) {
      Portfolio p = new Portfolio(10);
      p.shortID = 2;
      p.positions.clear();
      p.positions.put("VMW", new Position("VMW", i));
      region.put("KEY" + i, p);
    }

    MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
    QueryObserver old = QueryObserverHolder.setInstance(observer);

    // Create Index on ID
    Index idIndex = qs.createIndex("idIndex", "P.ID", "/portfolios1 P");
    Index shortIdIndex = qs.createIndex("shortIdIndex", "P.shortID", "/portfolios1 P");

    String queryString =
        "SELECT * FROM /portfolios1 P WHERE P.ID > 9 AND P.ID < 20 AND P.shortID = 2 LIMIT 5";
    query = qs.newQuery(queryString);

    assertNotNull(idIndex);
    SelectResults resultsWithIndex = (SelectResults) query.execute();
    // assertFalse(observer.limitAppliedAtIndex);
    assertEquals(5, resultsWithIndex.size());
  }

  /*
   * helper method to test against a compact range index
   *
   *
   */
  private SelectResults helpTestIndexForQuery(String query, String indexedExpression,
      String regionPath, Object[] params) throws Exception {
    QueryService qs = CacheUtils.getQueryService();
    MyQueryObserverAdapter observer = new MyQueryObserverAdapter();
    QueryObserverHolder.setInstance(observer);
    SelectResults nonIndexedResults = (SelectResults) qs.newQuery(query).execute(params);
    assertFalse(observer.indexUsed);

    qs.createIndex("newIndex", indexedExpression, regionPath);
    SelectResults indexedResults = (SelectResults) qs.newQuery(query).execute(params);
    assertEquals(nonIndexedResults.size(), indexedResults.size());
    assertTrue(observer.indexUsed);
    return indexedResults;
  }

  class MyQueryObserverAdapter extends QueryObserverAdapter {
    public boolean limitAppliedAtIndex = false;
    public String indexName;
    public boolean indexUsed = false;

    public void limitAppliedAtIndexLevel(Index index, int limit, Collection indexResult) {
      this.limitAppliedAtIndex = true;
      this.indexName = index.getName();
    }

    public void afterIndexLookup(Collection results) {
      if (results != null) {
        indexUsed = true;
      }
    }
  };
}
