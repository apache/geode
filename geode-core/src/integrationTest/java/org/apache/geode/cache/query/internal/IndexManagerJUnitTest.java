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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.index.IndexData;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.internal.index.IndexUtils;
import org.apache.geode.test.junit.categories.OQLIndexTest;

@Category({OQLIndexTest.class})
public class IndexManagerJUnitTest {

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      region.put("" + i, new Portfolio(i));
    }
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  /**
   * This test is for a fix for #47475 We added a way to determine whether to reevaluate an entry
   * for query execution The method is to keep track of the delta and current time in a single long
   * value The value is then used by the query to determine if a region entry needs to be
   * reevaluated based on subtracting the value with the query execution time. This provides a delta
   * + some false positive time If the delta + last modified time of the region entry is > query
   * start time, we can assume that it needs to be reevaluated
   */
  @Test
  public void testSafeQueryTime() throws Exception {
    IndexManager.resetIndexBufferTime();
    // fake entry update at LMT of 0 and actual time of 10
    // safe query time set in index manager is going to be 20
    IndexManager.setIndexBufferTime(0, 10);

    // fake query start at actual time of 9, 10, 11 and using the fake LMT of 0
    assertTrue(IndexManager.needsRecalculation(9, 0));
    assertTrue(IndexManager.needsRecalculation(10, 0));
    assertFalse(IndexManager.needsRecalculation(11, 0));

    assertFalse(IndexManager.needsRecalculation(9, -3)); // old enough updates shouldn't trigger a
                                                         // recalc
    assertTrue(IndexManager.needsRecalculation(9, -2)); // older updates but falls within the delta
                                                        // (false positive)

    assertTrue(IndexManager.needsRecalculation(10, 5));
    // This should eval to true only because of false positives.
    assertTrue(IndexManager.needsRecalculation(11, 5));

    // Now let's assume a new update has occurred, this update delta and time combo still is not
    // larger
    IndexManager.setIndexBufferTime(0, 9);
    IndexManager.setIndexBufferTime(1, 10);

    // Now let's assume a new update has occurred where the time is larger (enough to roll off the
    // large delta)
    // but the delta is smaller
    IndexManager.setIndexBufferTime(30, 30);

    // Now that we have a small delta, let's see if a query that was "stuck" would reevaluate
    // appropriately
    assertTrue(IndexManager.needsRecalculation(9, 0));
  }

  // Let's test for negative delta's or a system that is slower than others in the cluster
  @Test
  public void testSafeQueryTimeForASlowNode() throws Exception {
    IndexManager.resetIndexBufferTime();
    // fake entry update at LMT of 0 and actual time of 10
    // safe query time set in index manager is going to be -10
    IndexManager.setIndexBufferTime(210, 200);

    assertFalse(IndexManager.needsRecalculation(200, 190));
    assertFalse(IndexManager.needsRecalculation(200, 200));
    assertTrue(IndexManager.needsRecalculation(200, 210));

    assertTrue(IndexManager.needsRecalculation(200, 220));
    assertTrue(IndexManager.needsRecalculation(200, 221));

    // now lets say an entry updates with no delta
    IndexManager.setIndexBufferTime(210, 210);

    assertTrue(IndexManager.needsRecalculation(200, 190));
    assertTrue(IndexManager.needsRecalculation(200, 200));
    assertTrue(IndexManager.needsRecalculation(200, 210));

    assertTrue(IndexManager.needsRecalculation(200, 220));
    assertTrue(IndexManager.needsRecalculation(200, 221));

    assertTrue(IndexManager.needsRecalculation(210, 211));
    assertFalse(IndexManager.needsRecalculation(212, 210));
  }

  @Test
  public void testBestIndexPick() throws Exception {
    QueryService qs;

    qs = CacheUtils.getQueryService();
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/portfolios, positions");
    QCompiler compiler = new QCompiler();
    List list = compiler.compileFromClause("/portfolios pf");
    ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
    context.newScope(context.associateScopeID());

    Iterator iter = list.iterator();
    while (iter.hasNext()) {
      CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
      context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
      RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
      context.bindIterator(rIter);
      context.addToIndependentRuntimeItrMap(iterDef);
    }
    CompiledPath cp = new CompiledPath(new CompiledID("pf"), "status");

    // TASK ICM1
    String[] defintions = {"/portfolios", "index_iter1.positions"};
    IndexData id = IndexUtils.findIndex("/portfolios", defintions, cp, "*", CacheUtils.getCache(),
        true, context);
    Assert.assertEquals(id.getMatchLevel(), 0);
    Assert.assertEquals(id.getMapping()[0], 1);
    Assert.assertEquals(id.getMapping()[1], 2);
    String[] defintions1 = {"/portfolios"};
    IndexData id1 = IndexUtils.findIndex("/portfolios", defintions1, cp, "*", CacheUtils.getCache(),
        true, context);
    Assert.assertEquals(id1.getMatchLevel(), -1);
    Assert.assertEquals(id1.getMapping()[0], 1);
    String[] defintions2 = {"/portfolios", "index_iter1.positions", "index_iter1.coll1"};
    IndexData id2 = IndexUtils.findIndex("/portfolios", defintions2, cp, "*", CacheUtils.getCache(),
        true, context);
    Assert.assertEquals(id2.getMatchLevel(), 1);
    Assert.assertEquals(id2.getMapping()[0], 1);
    Assert.assertEquals(id2.getMapping()[1], 2);
    Assert.assertEquals(id2.getMapping()[2], 0);
  }

}
