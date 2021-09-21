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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Address;
import org.apache.geode.cache.query.data.Employee;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category(OQLQueryTest.class)
public class QueryObserverCallbacksTest {
  private QueryService queryService;
  private MyQueryObserverImpl myQueryObserver;

  @Rule
  public ServerStarterRule serverStarterRule = new ServerStarterRule().withAutoStart();

  @Before
  public void setUp() throws Exception {
    myQueryObserver = spy(new MyQueryObserverImpl());
    queryService = serverStarterRule.getCache().getQueryService();
    QueryObserverHolder.setInstance(myQueryObserver);

    Region<String, Object> portfolio1 =
        createRegionWithValueConstraint("portfolio", Portfolio.class);
    portfolio1.put("0", new Portfolio(0));
    portfolio1.put("1", new Portfolio(1));
    portfolio1.put("2", new Portfolio(2));
    portfolio1.put("3", new Portfolio(3));

    Region<String, Object> portfolio2 =
        createRegionWithValueConstraint("portfolio1", Portfolio.class);
    portfolio2.put("0", new Portfolio(0));
    portfolio2.put("1", new Portfolio(1));
    portfolio2.put("2", new Portfolio(2));
    portfolio2.put("3", new Portfolio(3));

    Region<String, Object> employees = createRegionWithValueConstraint("employees", Employee.class);
    Set<Address> add1 = new HashSet<>();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));
    employees.put("0", new Employee("empName", (20), 0, "Mr.", (5000), add1));
    employees.put("1", new Employee("empName", (20 + 1), 1, "Mr.", (5000 + 1), add1));
    employees.put("2", new Employee("empName", (20 + 2), 2, "Mr.", (5000 + 2), add1));
    employees.put("3", new Employee("empName", (20 + 3), 3, "Mr.", (5000 + 3), add1));
  }

  @After
  public void tearDown() {
    QueryObserverHolder.reset();
    IndexManager.TEST_RANGEINDEX_ONLY = false;
  }

  @SuppressWarnings("unchecked")
  private Region<String, Object> createRegionWithValueConstraint(String regionName,
      Class<?> valueType) {
    RegionFactory regionFactory = serverStarterRule.getCache().createRegionFactory();
    regionFactory.setKeyConstraint(String.class);
    regionFactory.setValueConstraint(valueType);

    return regionFactory.create(regionName);
  }

  @Test
  public void testBeforeAndAfterCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND()
      throws Exception {
    Query query = queryService.newQuery(
        "select distinct * from " + SEPARATOR + "portfolio p, p.positions," + SEPARATOR
            + "employees e, " + SEPARATOR
            + "portfolio1 p1 where p.ID = 1 and p1.ID = 2 and e.empId = 1");
    queryService.createIndex("statusIndex", "status", SEPARATOR + "portfolio");
    queryService.createIndex("idIndex", "ID", SEPARATOR + "portfolio");
    queryService.createIndex("idIndex1", "ID", SEPARATOR + "portfolio1");
    queryService.createIndex("empidIndex", "empId", SEPARATOR + "employees");

    query.execute();
    verify(myQueryObserver, times(1))
        .beforeCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND(any());
    verify(myQueryObserver, times(1))
        .afterCartesianOfGroupJunctionsInAnAllGroupJunctionOfType_AND();
  }

  @Test
  public void testBeforeAndAfterCartesianOfCompositeGroupJunctionsInAnAllGroupJunctionOfType_AND()
      throws Exception {
    Query query = queryService.newQuery(
        "select distinct * from " + SEPARATOR + "portfolio p, p.positions," + SEPARATOR
            + "employees e, " + SEPARATOR
            + "portfolio1 p1 where p.ID =p1.ID   and e.empId = 1 and p1.status = 'active' and p.status='active' ");
    queryService.createIndex("statusIndex1", "status", SEPARATOR + "portfolio");
    queryService.createIndex("statusIndex2", "status", SEPARATOR + "portfolio1");
    queryService.createIndex("idIndex", "ID", SEPARATOR + "portfolio");
    queryService.createIndex("idIndex1", "ID", SEPARATOR + "portfolio1");
    queryService.createIndex("empidIndex", "empId", SEPARATOR + "employees");

    query.execute();
    verify(myQueryObserver, times(1))
        .beforeCartesianOfGroupJunctionsInCompositeGroupJunctionOfType_AND(any());
    verify(myQueryObserver, times(1))
        .afterCartesianOfGroupJunctionsInCompositeGroupJunctionOfType_AND();
  }

  @Test
  public void testBeforeAndAfterCutDownAndExpansionOfSingleIndexResult() throws Exception {
    Query query =
        queryService.newQuery(
            "select distinct * from " + SEPARATOR + "portfolio p, p.positions where p.ID = 1  ");
    queryService.createIndex("idIndex", "ID", SEPARATOR + "portfolio");

    query.execute();
    verify(myQueryObserver, times(1)).beforeCutDownAndExpansionOfSingleIndexResult(any(), any());
    verify(myQueryObserver, times(1)).afterCutDownAndExpansionOfSingleIndexResult(any());
  }

  @Test
  public void testBeforeAndAfterMergeJoinOfDoubleIndexResults() throws Exception {
    Query query = queryService.newQuery(
        "select distinct * from " + SEPARATOR + "portfolio p, p.positions," + SEPARATOR
            + "employees e where p.ID =  e.empId  ");
    queryService.createIndex("idIndex", "ID", SEPARATOR + "portfolio");
    queryService.createIndex("empidIndex", "empId", SEPARATOR + "employees");

    query.execute();
    verify(myQueryObserver, times(1)).beforeMergeJoinOfDoubleIndexResults(any(), any(), any());
    verify(myQueryObserver, times(1)).afterMergeJoinOfDoubleIndexResults(any());
  }

  @Test
  public void testBeforeAndAfterIterJoinOfSingleIndexResults() throws Exception {
    Query query = queryService.newQuery(
        "select distinct * from " + SEPARATOR + "portfolio p, p.positions," + SEPARATOR
            + "employees e, " + SEPARATOR
            + "portfolio1 p1 where p.ID =p1.ID   and e.empId = p1.ID ");
    queryService.createIndex("idIndex", "ID", SEPARATOR + "portfolio");
    queryService.createIndex("idIndex1", "ID", SEPARATOR + "portfolio1");
    queryService.createIndex("empidIndex", "empId", SEPARATOR + "employees");

    query.execute();
    verify(myQueryObserver, times(1)).beforeIterJoinOfSingleIndexResults(any(), any());
    verify(myQueryObserver, times(1)).afterIterJoinOfSingleIndexResults(any());
    assertThat(myQueryObserver.dbIndx[2] == myQueryObserver.usedIndx)
        .as("Validate callback of Indexes").isTrue();
    assertThat(myQueryObserver.unusedIndx == myQueryObserver.dbIndx[0]
        || myQueryObserver.unusedIndx == myQueryObserver.dbIndx[1])
            .as("Validate callback of Indexes").isTrue();
  }

  @Test
  public void testBeforeRangeJunctionDoubleConditionLookup() throws Exception {
    Query query = queryService
        .newQuery(
            "select distinct * from " + SEPARATOR + "portfolio p where p.ID > 1   and  p.ID < 3 ");
    queryService.createIndex("idIndex", "ID", SEPARATOR + "portfolio");
    queryService.createIndex("idIndex1", "ID", SEPARATOR + "portfolio1");
    queryService.createIndex("empidIndex", "empId", SEPARATOR + "employees");

    query.execute();
    verify(myQueryObserver, times(1)).beforeIndexLookup(any(), anyInt(), any(), anyInt(), any(),
        any());
  }

  @Test
  public void beforeAggregationsAndGroupByShouldBeCalledForAggregateFunctions() throws Exception {
    List<String> queries = Arrays.asList(
        "SELECT MIN(pf.ID) FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, MIN(pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",
        "SELECT pf.status, MIN(pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",

        "SELECT MAX(pf.ID) FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, MAX(pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",

        "SELECT AVG(pf.ID) FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, AVG(pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",
        "SELECT pf.status, AVG(DISTINCT pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",

        "SELECT SUM(pf.ID) FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, SUM(pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",
        "SELECT pf.status, SUM(DISTINCT pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",

        "SELECT COUNT(pf.ID) FROM " + SEPARATOR + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, COUNT(pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",
        "SELECT pf.status, COUNT(DISTINCT pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",

        "SELECT MIN(pf.ID), MAX(pf.ID), AVG(pf.ID), SUM(pf.ID), COUNT(pf.ID) FROM " + SEPARATOR
            + "portfolio pf WHERE pf.ID > 0",
        "SELECT pf.status, MIN(pf.ID), MAX(pf.ID), AVG(pf.ID), SUM(pf.ID), COUNT(pf.ID) FROM "
            + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status",
        "SELECT pf.status, MIN(pf.ID), MAX(pf.ID), AVG(DISTINCT pf.ID), SUM(DISTINCT pf.ID), COUNT(DISTINCT pf.ID) FROM "
            + SEPARATOR + "portfolio pf WHERE pf.ID > 0 GROUP BY pf.status");

    MyQueryObserverImpl myQueryObserver = spy(new MyQueryObserverImpl());
    QueryObserverHolder.setInstance(myQueryObserver);
    for (String queryString : queries) {
      Query query = queryService.newQuery(queryString);
      query.execute();
    }

    verify(myQueryObserver, times(queries.size())).beforeAggregationsAndGroupBy(any());
  }

  @Test
  public void testBeforeAndAfterIterationEvaluateNoWhere() throws Exception {
    Query query = queryService.newQuery(
        "select count(*) from " + SEPARATOR + "portfolio p");

    query.execute();
    verify(myQueryObserver, never()).beforeIterationEvaluation(any(), any());
    verify(myQueryObserver, never()).afterIterationEvaluation(any());
  }

  @Test
  public void testBeforeAndAfterIterationEvaluateWithoutIndex() throws Exception {
    Query query = queryService.newQuery(
        "select count(*) from " + SEPARATOR + "portfolio p where p.isActive = true ");

    query.execute();
    verify(myQueryObserver, times(4)).beforeIterationEvaluation(any(), any());
    verify(myQueryObserver, times(4)).afterIterationEvaluation(any());
  }

  @Test
  public void testBeforeAndAfterIterationEvaluateWithCompactRangeIndex() throws Exception {
    Query query = queryService.newQuery(
        "select count(*) from " + SEPARATOR + "portfolio p where p.isActive = true ");
    queryService.createIndex("isActiveIndex", "isActive", SEPARATOR + "portfolio");

    query.execute();
    verify(myQueryObserver, times(2)).beforeIterationEvaluation(any(), any());
    verify(myQueryObserver, times(2)).afterIterationEvaluation(any());
    assertThat(myQueryObserver.dbIndx[2] == myQueryObserver.usedIndx)
        .as("Validate callback of Indexes").isTrue();
    assertThat(myQueryObserver.unusedIndx == myQueryObserver.dbIndx[0]
        || myQueryObserver.unusedIndx == myQueryObserver.dbIndx[1])
            .as("Validate callback of Indexes").isTrue();
  }

  @Test
  public void testBeforeAndAfterIterationEvaluateWithRangeIndex() throws Exception {
    IndexManager.TEST_RANGEINDEX_ONLY = true;
    Query query = queryService.newQuery(
        "select count(*) from " + SEPARATOR + "portfolio p where p.description = 'XXXX' ");
    queryService.createIndex("descriptionIndex", "description", SEPARATOR + "portfolio");

    query.execute();
    verify(myQueryObserver, times(2)).beforeIterationEvaluation(any(), any());
    verify(myQueryObserver, times(2)).afterIterationEvaluation(any());
    assertThat(myQueryObserver.dbIndx[2] == myQueryObserver.usedIndx)
        .as("Validate callback of Indexes").isTrue();
    assertThat(myQueryObserver.unusedIndx == myQueryObserver.dbIndx[0]
        || myQueryObserver.unusedIndx == myQueryObserver.dbIndx[1])
            .as("Validate callback of Indexes").isTrue();
  }

  private static class MyQueryObserverImpl extends QueryObserverAdapter {
    private int j = 0;
    private Index usedIndx = null;
    private Index unusedIndx = null;
    private Index[] dbIndx = new Index[3];

    @Override
    public void beforeIndexLookup(Index index, int oper, Object key) {
      dbIndx[j++] = index;
      if (j == 3) {
        j = 0;
      }
    }

    @Override
    public void beforeIterJoinOfSingleIndexResults(Index usedIndex, Index unusedIndex) {
      usedIndx = usedIndex;
      unusedIndx = unusedIndex;
    }
  }
}
