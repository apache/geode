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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.data.Address;
import org.apache.geode.cache.query.data.Employee;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.types.ObjectTypeImpl;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class QueryUtilsJUnitTest {

  private Region region;

  @Before
  public void setUp() throws Exception {
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testObtainTheBottomMostCompiledValue() throws Exception {
    QCompiler compiler = new QCompiler();
    CompiledRegion cr = new CompiledRegion(SEPARATOR + "portfolio");
    CompiledID cid = new CompiledID("id");
    CompiledPath cp1 = new CompiledPath(new CompiledPath(cid, "path1"), "path2");
    CompiledPath cp2 = new CompiledPath(new CompiledPath(cr, "path1"), "path2");
    assertEquals(QueryUtils.obtainTheBottomMostCompiledValue(cr), cr);
    assertEquals(QueryUtils.obtainTheBottomMostCompiledValue(cid), cid);
    assertEquals(QueryUtils.obtainTheBottomMostCompiledValue(cp1), cid);
    assertEquals(QueryUtils.obtainTheBottomMostCompiledValue(cp2), cr);
  }

  @Test
  public void testCutDownAndExpandIndexResultsWithNoCutDownAndTwoFinalIters() throws Exception {
    region = CacheUtils.createRegion("portfolio", Portfolio.class);
    Portfolio[] po =
        new Portfolio[] {new Portfolio(0), new Portfolio(1), new Portfolio(2), new Portfolio(3)};
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));
    // compileFromClause returns a List<CompiledIteratorDef>
    QCompiler compiler = new QCompiler();
    List list = compiler.compileFromClause(SEPARATOR + "portfolio p, p.positions");
    ExecutionContext context = new ExecutionContext(null, CacheUtils.getCache());
    context.newScope(context.associateScopeID());
    RuntimeIterator[] indexToItrMappping = new RuntimeIterator[1];
    RuntimeIterator expand = null;
    boolean set = false;

    Iterator iter = list.iterator();
    while (iter.hasNext()) {
      CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
      context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
      RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
      if (!set) {
        set = true;
        indexToItrMappping[0] = rIter;
      } else {
        expand = rIter;
      }
      context.bindIterator(rIter);
      context.addToIndependentRuntimeItrMap(iterDef);
    }
    List finalList = new ArrayList();
    finalList.add(indexToItrMappping[0]);
    finalList.add(expand);
    ResultsSet indexResult = new ResultsSet(new ObjectTypeImpl(Portfolio.class));
    for (final Portfolio portfolio : po) {
      indexResult.add(portfolio);
    }
    List expandList = new LinkedList();
    expandList.add(expand);
    List dataList = new ArrayList();
    dataList.add(indexResult);
    dataList.add(indexToItrMappping);
    dataList.add(expandList);
    dataList.add(finalList);
    dataList.add(context);
    dataList.add(new ArrayList());
    SelectResults results = QueryUtils.testCutDownAndExpandIndexResults(dataList);
    assertTrue("Resultset obtained not of type StructBag", results instanceof StructBag);
    StructBag st = (StructBag) results;
    assertTrue("StructBag not of size expected as 8", st.size() == 8);
  }

  @Test
  public void testCutDownAndExpandIndexResultsWithNoCutDownAndThreeFinalIters() throws Exception {
    region = CacheUtils.createRegion("portfolio", Portfolio.class);
    Portfolio[] po =
        new Portfolio[] {new Portfolio(0), new Portfolio(1), new Portfolio(2), new Portfolio(3)};
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));

    Region r3 = CacheUtils.createRegion("employees", Employee.class);
    Set add1 = new HashSet();
    add1.add(new Address("411045", "Baner"));
    add1.add(new Address("411001", "DholePatilRd"));
    for (int i = 0; i < 4; i++) {
      r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
    }
    // compileFromClause returns a List<CompiledIteratorDef>
    QCompiler compiler = new QCompiler();
    List list = compiler
        .compileFromClause(SEPARATOR + "portfolio p, p.positions, " + SEPARATOR + "employees e");
    ExecutionContext context = new ExecutionContext(null, CacheUtils.getCache());
    context.newScope(context.associateScopeID());
    RuntimeIterator[] indexToItrMappping = new RuntimeIterator[1];
    RuntimeIterator[] expand = new RuntimeIterator[2];
    boolean set = false;
    int j = 0;

    Iterator iter = list.iterator();
    while (iter.hasNext()) {
      CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
      context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
      RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
      if (!set) {
        set = true;
        indexToItrMappping[0] = rIter;
      } else {
        expand[j++] = rIter;
      }
      context.bindIterator(rIter);
      context.addToIndependentRuntimeItrMap(iterDef);
    }
    List finalList = new ArrayList();
    finalList.add(indexToItrMappping[0]);

    ResultsSet indexResult = new ResultsSet(new ObjectTypeImpl(Portfolio.class));
    for (final Portfolio portfolio : po) {
      indexResult.add(portfolio);
    }
    List expandList = new LinkedList();
    expandList.add(expand[0]);
    expandList.add(expand[1]);
    finalList.addAll(expandList);
    List dataList = new ArrayList();
    dataList.add(indexResult);
    dataList.add(indexToItrMappping);
    dataList.add(expandList);
    dataList.add(finalList);
    dataList.add(context);
    dataList.add(new ArrayList());
    SelectResults results = QueryUtils.testCutDownAndExpandIndexResults(dataList);

    assertTrue("Resultset obtained not of type structbag", results instanceof StructBag);
    StructBag st = (StructBag) results;
    assertTrue("StructSet not of size expected as 32", st.size() == 32);
  }
}
