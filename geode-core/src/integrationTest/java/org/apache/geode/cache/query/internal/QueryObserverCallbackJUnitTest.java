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

import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Address;
import org.apache.geode.cache.query.data.Employee;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class QueryObserverCallbackJUnitTest {

  private Region region;
  private QueryService qs;

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
    region = CacheUtils.createRegion("portfolio", Portfolio.class);
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));

    qs = CacheUtils.getQueryService();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
    IndexManager indexManager = ((LocalRegion) region).getIndexManager();
    if (indexManager != null) {
      indexManager.destroy();
    }
  }

  @Test
  public void testBeforeAndAfterCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND()
      throws Exception {
    Region r3 = CacheUtils.createRegion("employees", Employee.class);
    Set add1 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));
    for (int i = 0; i < 4; i++) {
      r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
    }
    Region r4 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r4.put(i + "", new Portfolio(i));
    }
    Query query = qs.newQuery(
        "select distinct * from /portfolio p, p.positions,/employees e, /portfolio1 p1 where p.ID = 1 and p1.ID = 2 and e.empId = 1");
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/portfolio");
    qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
    qs.createIndex("idIndex1", IndexType.FUNCTIONAL, "ID", "/portfolio1");
    qs.createIndex("empidIndex", IndexType.FUNCTIONAL, "empId", "/employees");
    MyQueryObserverImpl inst = new MyQueryObserverImpl();
    QueryObserverHolder.setInstance(inst);
    query.execute();
    assertTrue("beforeCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND callbak not received",
        inst.bfrCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND);
    assertTrue("afterCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND callbak not received",
        inst.aftCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND);
  }

  @Test
  public void testBeforeAndAfterCartesianOfCompositeGroupJunctionsInAnAllGroupJunctionOfType_AND()
      throws Exception {
    Region r3 = CacheUtils.createRegion("employees", Employee.class);
    Set add1 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));
    for (int i = 0; i < 4; i++) {
      r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
    }
    Region r4 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r4.put(i + "", new Portfolio(i));
    }
    Query query = qs.newQuery(
        "select distinct * from /portfolio p, p.positions,/employees e, /portfolio1 p1 where p.ID =p1.ID   and e.empId = 1 and p1.status = 'active' and p.status='active' ");
    qs.createIndex("statusIndex1", IndexType.FUNCTIONAL, "status", "/portfolio");
    qs.createIndex("statusIndex2", IndexType.FUNCTIONAL, "status", "/portfolio1");
    qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
    qs.createIndex("idIndex1", IndexType.FUNCTIONAL, "ID", "/portfolio1");
    qs.createIndex("empidIndex", IndexType.FUNCTIONAL, "empId", "/employees");
    MyQueryObserverImpl inst = new MyQueryObserverImpl();
    QueryObserverHolder.setInstance(inst);
    query.execute();
    assertTrue(
        "beforeCartesianOfCompositeGroupJunctionsInAnAllGroupJunctionOfType_AND callbak not received",
        inst.bfrCartesianOfGroupJunctionsInCompositeGroupJunctionOfType_AND);
    assertTrue(
        "afterCartesianOfCompositeGroupJunctionsInAnAllGroupJunctionOfType_AND callbak not received",
        inst.aftCartesianOfGroupJunctionsInCompositeGroupJunctionOfType_AND);
  }

  @Test
  public void testBeforeAndAfterCutDownAndExpansionOfSingleIndexResult() throws Exception {
    Query query = qs.newQuery("select distinct * from /portfolio p, p.positions where p.ID = 1  ");
    qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
    MyQueryObserverImpl inst = new MyQueryObserverImpl();
    QueryObserverHolder.setInstance(inst);
    query.execute();
    assertTrue("beforeCutDownAndExpansionOfSingleIndexResult callbak not received",
        inst.bfrCutDownAndExpansionOfSingleIndexResult);
    assertTrue("afterCutDownAndExpansionOfSingleIndexResult callbak not received",
        inst.aftCutDownAndExpansionOfSingleIndexResult);
  }

  @Test
  public void testBeforeAndAfterMergeJoinOfDoubleIndexResults() throws Exception {
    Region r3 = CacheUtils.createRegion("employees", Employee.class);
    Set add1 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));
    for (int i = 0; i < 4; i++) {
      r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
    }

    Query query = qs.newQuery(
        "select distinct * from /portfolio p, p.positions,/employees e where p.ID =  e.empId  ");
    qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
    qs.createIndex("empidIndex", IndexType.FUNCTIONAL, "empId", "/employees");
    MyQueryObserverImpl inst = new MyQueryObserverImpl();
    QueryObserverHolder.setInstance(inst);
    query.execute();
    assertTrue("beforeMergeJoinOfDoubleIndexResults callbak not received",
        inst.bfrMergeJoinOfDoubleIndexResults);
    assertTrue("afterMergeJoinOfDoubleIndexResults callbak not received",
        inst.aftMergeJoinOfDoubleIndexResults);
  }

  @Test
  public void testBeforeAndAfterIterJoinOfSingleIndexResults() throws Exception {
    Region r3 = CacheUtils.createRegion("employees", Employee.class);
    Set add1 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));
    for (int i = 0; i < 4; i++) {
      r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
    }
    Region r4 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r4.put(i + "", new Portfolio(i));
    }
    Query query = qs.newQuery(
        "select distinct * from /portfolio p, p.positions,/employees e, /portfolio1 p1 where p.ID =p1.ID   and e.empId = p1.ID ");

    qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
    qs.createIndex("idIndex1", IndexType.FUNCTIONAL, "ID", "/portfolio1");
    qs.createIndex("empidIndex", IndexType.FUNCTIONAL, "empId", "/employees");
    MyQueryObserverImpl inst = new MyQueryObserverImpl();
    QueryObserverHolder.setInstance(inst);
    query.execute();
    assertTrue("beforeIterJoinOfSingleIndexResults callbak not received",
        inst.bfrIterJoinOfSingleIndexResults);
    assertTrue("afterIterJoinOfSingleIndexResults callbak not received",
        inst.aftIterJoinOfSingleIndexResults);
    assertTrue("Validate callback of Indexes", inst.dbIndx[2] == inst.usedIndx);
    assertTrue("Validate callback of Indexes",
        inst.unusedIndx == inst.dbIndx[0] || inst.unusedIndx == inst.dbIndx[1]);
  }

  @Test
  public void testBeforeRangeJunctionDoubleConditionLookup() throws Exception {
    Region r3 = CacheUtils.createRegion("employees", Employee.class);
    Set add1 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));
    for (int i = 0; i < 4; i++) {
      r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
    }
    Region r4 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      r4.put(i + "", new Portfolio(i));
    }
    Query query =
        qs.newQuery("select distinct * from /portfolio p where p.ID > 1   and  p.ID < 3 ");

    qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
    qs.createIndex("idIndex1", IndexType.FUNCTIONAL, "ID", "/portfolio1");
    qs.createIndex("empidIndex", IndexType.FUNCTIONAL, "empId", "/employees");
    MyQueryObserverImpl inst = new MyQueryObserverImpl();
    QueryObserverHolder.setInstance(inst);
    query.execute();
    assertTrue("beforeIndexLookup For RangeJunction.DoubleCondnRangeJunctionEvaluator not invoked",
        inst.beforeRangeJunctionDoubleCondnIndexLookup);
  }

  private static class MyQueryObserverImpl implements QueryObserver {

    private boolean aftCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND = false;
    private boolean bfrCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND = false;
    private boolean bfrCartesianOfGroupJunctionsInCompositeGroupJunctionOfType_AND = false;
    private boolean aftCartesianOfGroupJunctionsInCompositeGroupJunctionOfType_AND = false;
    private boolean bfrCutDownAndExpansionOfSingleIndexResult = false;
    private boolean aftCutDownAndExpansionOfSingleIndexResult = false;
    private boolean bfrMergeJoinOfDoubleIndexResults = false;
    private boolean aftMergeJoinOfDoubleIndexResults = false;
    private boolean bfrIterJoinOfSingleIndexResults = false;
    private boolean aftIterJoinOfSingleIndexResults = false;
    private boolean beforeRangeJunctionDoubleCondnIndexLookup = false;
    private Index usedIndx = null;
    private Index unusedIndx = null;
    private int j = 0;
    private Index[] dbIndx = new Index[3];

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {
      dbIndx[j++] = index;
      if (j == 3)
        j = 0;
    }

    @Override
    public void startQuery(Query query) {
      // nothing
    }

    @Override
    public void beforeQueryEvaluation(CompiledValue expression, ExecutionContext context) {
      // nothing
    }

    @Override
    public void startIteration(Collection collection, CompiledValue whereClause) {
      // nothing
    }

    @Override
    public void beforeIterationEvaluation(CompiledValue executer, Object currentObject) {
      // nothing
    }

    @Override
    public void afterIterationEvaluation(Object result) {
      // nothing
    }

    @Override
    public void endIteration(SelectResults results) {
      // nothing
    }

    @Override
    public void afterIndexLookup(Collection results) {
      // nothing
    }

    @Override
    public void afterQueryEvaluation(Object result) {
      // nothing
    }

    @Override
    public void endQuery() {
      // nothing
    }

    @Override
    public void beforeRerunningIndexCreationQuery() {
      // nothing
    }

    @Override
    public void beforeCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND(
        Collection[] grpResults) {
      bfrCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND = true;
    }

    @Override
    public void afterCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND() {
      aftCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND = true;
    }

    @Override
    public void beforeCartesianOfGroupJunctionsInCompositeGroupJunctionOfType_AND(
        Collection[] grpResults) {
      bfrCartesianOfGroupJunctionsInCompositeGroupJunctionOfType_AND = true;
    }

    @Override
    public void afterCartesianOfGroupJunctionsInCompositeGroupJunctionOfType_AND() {
      aftCartesianOfGroupJunctionsInCompositeGroupJunctionOfType_AND = true;
    }

    @Override
    public void beforeCutDownAndExpansionOfSingleIndexResult(Index index,
        Collection initialResult) {
      bfrCutDownAndExpansionOfSingleIndexResult = true;
    }

    @Override
    public void afterCutDownAndExpansionOfSingleIndexResult(Collection finalResult) {
      aftCutDownAndExpansionOfSingleIndexResult = true;
    }

    @Override
    public void beforeMergeJoinOfDoubleIndexResults(Index index1, Index index2,
        Collection initialResult) {
      bfrMergeJoinOfDoubleIndexResults = true;
    }

    @Override
    public void afterMergeJoinOfDoubleIndexResults(Collection finalResult) {
      aftMergeJoinOfDoubleIndexResults = true;
    }

    @Override
    public void beforeIterJoinOfSingleIndexResults(Index usedIndex, Index unusedIndex) {
      bfrIterJoinOfSingleIndexResults = true;
      usedIndx = usedIndex;
      unusedIndx = unusedIndex;
    }

    @Override
    public void afterIterJoinOfSingleIndexResults(Collection finalResult) {
      aftIterJoinOfSingleIndexResults = true;
    }

    @Override
    public void beforeIndexLookup(Index index, int lowerBoundOperator, Object lowerBoundKey,
        int upperBoundOperator, Object upperBoundKey, Set NotEqualKeys) {
      beforeRangeJunctionDoubleCondnIndexLookup = true;
    }

    @Override
    public void beforeApplyingProjectionOnFilterEvaluatedResults(
        Object preProjectionAppliedResult) {
      // nothing
    }

    @Override
    public void invokedQueryUtilsIntersection(SelectResults sr1, SelectResults sr2) {
      // nothing
    }

    @Override
    public void invokedQueryUtilsUnion(SelectResults sr1, SelectResults sr2) {
      // nothing
    }

    @Override
    public void limitAppliedAtIndexLevel(Index index, int limit, Collection indexResult) {
      // nothing
    }

    @Override
    public void orderByColumnsEqual() {
      // nothing
    }
  }
}
