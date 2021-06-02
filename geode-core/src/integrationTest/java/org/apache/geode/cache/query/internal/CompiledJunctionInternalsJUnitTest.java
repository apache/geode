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
/*
 * Created on Nov 15, 2005
 *
 * TODO To change the template for this generated file go to Window - Preferences - Java - Code
 * Style - Code Templates
 */
package org.apache.geode.cache.query.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.IndexType;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Address;
import org.apache.geode.cache.query.data.Employee;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.query.internal.parse.OQLLexerTokenTypes;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class CompiledJunctionInternalsJUnitTest {

  Region region;
  QueryService qs;

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

  /*************************************************
   * AND JUNCTION TESTS. RangeJunction related tests when part of GroupJunction,
   * CompositeGroupJunction or AllGroupJunction
   **************************************************/

  // Case 1: When a Single GroupJunction contains multiple
  // RangeJunctionEvaluators for AND
  @Test
  public void testMultipleRangeJunctionsInAGroupJunction() {
    QCompiler compiler = new QCompiler();
    List list = compiler.compileFromClause("/portfolio p, p.positions");
    ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
    context.newScope(context.associateScopeID());
    try {
      qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/portfolio");
      qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
      Iterator iter = list.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.bindIterator(rIter);
        context.addToIndependentRuntimeItrMap(iterDef);
      }

      // case id > 7 and id < 10 and status > abc and status < xyz
      CompiledComparison cv[] = new CompiledComparison[4];

      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "status"),
          new CompiledLiteral(new String("abc")), OQLLexerTokenTypes.TOK_GT);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(10)), OQLLexerTokenTypes.TOK_LT);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "status"),
          new CompiledLiteral("xyz"), OQLLexerTokenTypes.TOK_LT);
      CompiledJunction cj = new CompiledJunction(cv, OQLLexerTokenTypes.LITERAL_and);
      context.addDependencies(new CompiledID("dummy"), cj.computeDependencies(context));
      cj.getPlanInfo(context);
      OrganizedOperands oo = cj.testOrganizedOperands(context);
      assertNotNull("The Organized operand object is null", oo);
      assertTrue("Filter Operand  did not happen to be an instance of GroupJunction",
          oo.filterOperand instanceof GroupJunction);
      assertNull("Iterate Operand  not coming out to be null", oo.iterateOperand);
      assertTrue("The Conditions in GroupJunction not equal to 2",
          ((GroupJunction) oo.filterOperand).getOperands().size() == 2);
      GroupJunction gj = (GroupJunction) oo.filterOperand;
      assertTrue(gj.getOperands().get(0) instanceof RangeJunction);
      assertTrue(gj.getOperands().get(1) instanceof RangeJunction);
      RangeJunction r1 = (RangeJunction) gj.getOperands().get(0);
      RangeJunction r2 = (RangeJunction) gj.getOperands().get(1);
      assertEquals(2, r1._operands.length);
      assertEquals(2, r2._operands.length);
      OrganizedOperands oo2 = r1.organizeOperands(context);
      assertTrue(RangeJunction.isInstanceOfDoubleCondnRangeJunctionEvaluator(oo2.filterOperand));
      oo2 = r2.organizeOperands(context);
      assertTrue(RangeJunction.isInstanceOfDoubleCondnRangeJunctionEvaluator(oo2.filterOperand));

    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed sue to Exception = " + e);
    }
  }


  // Case 2: When a Single GroupJunction contains multiple
  // RangeJunctions and a sinlge indexable compiledcomparison for AND
  @Test
  public void testMultipleRangeJunctionsAndAnIndexableConditionInAGroupJunction() {
    QCompiler compiler = new QCompiler();
    List list = compiler.compileFromClause("/portfolio p, p.positions");
    ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
    context.newScope(context.associateScopeID());
    try {
      qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/portfolio");
      qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
      qs.createIndex("createTime", IndexType.FUNCTIONAL, "createTime", "/portfolio");
      Iterator iter = list.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.bindIterator(rIter);
        context.addToIndependentRuntimeItrMap(iterDef);
      }

      // case id > 7 and id < 10 and status > abc and status < xyz and
      // createTime >7
      CompiledComparison cv[] = new CompiledComparison[5];

      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "status"),
          new CompiledLiteral(new String("abc")), OQLLexerTokenTypes.TOK_GT);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(10)), OQLLexerTokenTypes.TOK_LT);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "status"),
          new CompiledLiteral("xyz"), OQLLexerTokenTypes.TOK_LT);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "createTime"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      CompiledJunction cj = new CompiledJunction(cv, OQLLexerTokenTypes.LITERAL_and);
      context.addDependencies(new CompiledID("dummy"), cj.computeDependencies(context));
      cj.getPlanInfo(context);
      OrganizedOperands oo = cj.testOrganizedOperands(context);
      assertNotNull("The Organized operand object is null", oo);
      assertTrue("Filter Operand  did not happen to be an instance of GroupJunction",
          oo.filterOperand instanceof GroupJunction);
      assertNull("Iterate Operand  not coming out to be null", oo.iterateOperand);
      assertTrue("The Conditions in GroupJunction not equal to 3",
          ((GroupJunction) oo.filterOperand).getOperands().size() == 3);
      GroupJunction gj = (GroupJunction) oo.filterOperand;
      CompiledValue ops[] = gj._operands;
      int index = -1;
      int one = -1;
      int two = -1;
      if (ops[0] == cv[4]) {
        index = 0;
        one = 1;
        two = 2;
      } else if (ops[1] == cv[4]) {
        index = 1;
        one = 0;
        two = 2;
      } else if (ops[2] == cv[4]) {
        index = 2;
        one = 0;
        two = 1;
      } else {
        fail("Missing single condition");
      }
      assertTrue(ops[index] == cv[4]);
      assertTrue(ops[one] instanceof RangeJunction);
      assertTrue(ops[two] instanceof RangeJunction);

      RangeJunction r1 = (RangeJunction) gj._operands[one];
      RangeJunction r2 = (RangeJunction) gj._operands[two];
      assertEquals(2, r1._operands.length);
      assertEquals(2, r2._operands.length);
      OrganizedOperands oo2 = r1.organizeOperands(context);
      assertTrue(RangeJunction.isInstanceOfDoubleCondnRangeJunctionEvaluator(oo2.filterOperand));
      oo2 = r2.organizeOperands(context);
      assertTrue(RangeJunction.isInstanceOfDoubleCondnRangeJunctionEvaluator(oo2.filterOperand));

    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed sue to Exception = " + e);
    }
  }

  // Case 3: A Composite GroupJunction containing just a single RangeJunction
  // FOR AND clause. In such case a CompisteGroupJunction should contain a
  // GroupJunction and the GroupJunction should contain a CompiledJunction as
  // iter operand
  // portfolio.id= employee.id and p.id > 3 and p.id <10 and p.status =active
  // and p.type=abc
  // and emp.age = 1
  @Test
  public void testSingleRangeJunctionInACompositeGroupJunction() {
    try {
      Region r3 = CacheUtils.createRegion("employees", Employee.class);
      Set add1 = new HashSet();
      add1.add(new Address("411045", "Baner"));
      add1.add(new Address("411001", "DholePatilRd"));
      for (int i = 0; i < 4; i++) {
        r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
      }
      // compileFromClause returns a List<CompiledIteratorDef>
      QCompiler compiler = new QCompiler();
      List list = compiler.compileFromClause("/portfolio p, p.positions,/employees e");
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      context.newScope(context.associateScopeID());

      // qs.createIndex("statusIndex",
      // IndexType.FUNCTIONAL,"status","/portfolio");
      qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
      qs.createIndex("empid", IndexType.FUNCTIONAL, "empId", "/employees");
      Iterator iter = list.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.bindIterator(rIter);
        context.addToIndependentRuntimeItrMap(iterDef);
      }
      CompiledComparison cv[] = new CompiledComparison[6];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "status"),
          new CompiledLiteral(new String("active")), OQLLexerTokenTypes.TOK_EQ);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "type"),
          new CompiledLiteral(new String("type1")), OQLLexerTokenTypes.TOK_EQ);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("e"), "age"),
          new CompiledLiteral(new Integer(1)), OQLLexerTokenTypes.TOK_EQ);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("e"), "empId"),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_EQ);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(3)), OQLLexerTokenTypes.TOK_GT);
      cv[5] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(10)), OQLLexerTokenTypes.TOK_LT);
      CompiledJunction cj = new CompiledJunction(cv, OQLLexerTokenTypes.LITERAL_and);
      context.addDependencies(new CompiledID("dummy"), cj.computeDependencies(context));
      cj.getPlanInfo(context);
      OrganizedOperands oo = cj.testOrganizedOperands(context);
      assertNotNull("OO is null", oo);
      assertTrue("Filter operand of OO not CompositeGroupJunction",
          oo.filterOperand instanceof CompositeGroupJunction);
      CompositeGroupJunction cgj = (CompositeGroupJunction) oo.filterOperand;
      List filterableCC = cgj.getFilterableCCList();
      assertTrue("No. of filterable CC  not equal to 1", filterableCC.size() == 1);
      assertNotNull("1 RangeJunction inside CompositeGroupJunction should have existed",
          cgj.getGroupJunctionList());
      assertTrue(" Complete expansion flag should have been true", cgj.getExpansionFlag());
      assertNotNull("Iter operands list in CGJ should  be not null", cgj.getIterOperands());
      List ietrOps = cgj.getIterOperands();
      assertTrue(ietrOps.contains(cv[2]));
      assertTrue(cgj.getGroupJunctionList().size() == 1);
      assertTrue(cgj.getGroupJunctionList().get(0) instanceof RangeJunction);
      RangeJunction rg = (RangeJunction) cgj.getGroupJunctionList().get(0);
      OrganizedOperands oo1 = rg.organizeOperands(context);
      assertTrue(((CompiledJunction) oo1.iterateOperand).getOperands().contains(cv[0]));
      assertTrue(((CompiledJunction) oo1.iterateOperand).getOperands().contains(cv[1]));
      assertTrue(RangeJunction.isInstanceOfDoubleCondnRangeJunctionEvaluator(oo1.filterOperand));
      assertTrue(RangeJunction.getDoubleCondnEvaluatorGreaterKey(oo1.filterOperand)
          .equals(new Integer(3)));
      assertTrue(
          RangeJunction.getDoubleCondnEvaluatorLESSKey(oo1.filterOperand).equals(new Integer(10)));
      /*
       * assertTrue( " The size of itr operands should be 3 but size happens to be = " +
       * cgj.getIterOperands().size(), cgj.getIterOperands().size() == 3);
       */
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed sue to Exception = " + e);
    }
  }

  /**
   * Tests that the organized operands work correctly whether it is multiple GroupJunctions as part
   * of AllGroupJunction or a RangeJunction and GroupJunction. Since where previously it used to be
   * only Group can now be Group or Range , the source code needs to ensure that it does not type
   * cast wrongly a RangeJunction into a GroupJunction. Instead the source code should only type
   * cast it to AbstractGroupOrRangeJunction Case p.status = active and p.type = type1 and emp.id
   * >10 and p.id > 10 and p.id > 15
   */
  @Test
  public void testMultipleIndependentRegionsWithOnlyOneRegionHavingFilterEvaluatableCondition() {
    // Here there will be a single RangeJunction with the iter evaluatable
    // conditions of the other
    // independent iterator becoming the part of this single RangeJunction
    try {
      Region r3 = CacheUtils.createRegion("employees", Employee.class);
      Set add1 = new HashSet();
      add1.add(new Address("411045", "Baner"));
      add1.add(new Address("411001", "DholePatilRd"));
      for (int i = 0; i < 4; i++) {
        r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
      }
      // compileFromClause returns a List<CompiledIteratorDef>
      QCompiler compiler = new QCompiler();
      List list = compiler.compileFromClause("/portfolio p, p.positions,/employees e");
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      context.newScope(context.associateScopeID());
      qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio p");
      Iterator iter = list.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.bindIterator(rIter);
        context.addToIndependentRuntimeItrMap(iterDef);
      }
      CompiledComparison cv[] = new CompiledComparison[5];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "status"),
          new CompiledLiteral(new String("active")), OQLLexerTokenTypes.TOK_EQ);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "type"),
          new CompiledLiteral(new String("type1")), OQLLexerTokenTypes.TOK_EQ);

      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("e"), "empId"),
          new CompiledLiteral(new Integer(10)), OQLLexerTokenTypes.TOK_GT);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(10)), OQLLexerTokenTypes.TOK_GT);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(15)), OQLLexerTokenTypes.TOK_GT);
      CompiledJunction cj = new CompiledJunction(cv, OQLLexerTokenTypes.LITERAL_and);
      context.addDependencies(new CompiledID("dummy"), cj.computeDependencies(context));
      cj.getPlanInfo(context);
      OrganizedOperands oo = cj.testOrganizedOperands(context);
      assertNotNull("OO is null", oo);
      assertTrue("Filter operand of OO not a RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      OrganizedOperands oo1 = rj.organizeOperands(context);
      assertTrue(((CompiledJunction) oo1.iterateOperand).getOperands().contains(cv[0]));
      assertTrue(((CompiledJunction) oo1.iterateOperand).getOperands().contains(cv[1]));
      assertTrue(((CompiledJunction) oo1.iterateOperand).getOperands().contains(cv[2]));
      assertTrue(oo1.filterOperand == cv[4]);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed sue to Exception = " + e);
    }
  }

  /**
   * Tests that the organized operands work correctly whether it is multiple GroupJunctions as part
   * of AllGroupJunction or a RangeJunction and GroupJunction. Since where previously it used to be
   * only Group can now be Group or Range , the source code needs to ensure that it does not type
   * cast wrongly a RangeJunction into a GroupJunction. Instead the source code should only type
   * cast it to AbstractGroupOrRangeJunction Case p.status = active and p.type = type1 and emp.id
   * >10 and p.id > 10 and p.id > 15
   */
  @Test
  public void testMultipleIndependentRegionsWithRangeJunction() {

    try {
      Region r3 = CacheUtils.createRegion("employees", Employee.class);
      Set add1 = new HashSet();
      add1.add(new Address("411045", "Baner"));
      add1.add(new Address("411001", "DholePatilRd"));
      for (int i = 0; i < 4; i++) {
        r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
      }
      // compileFromClause returns a List<CompiledIteratorDef>
      QCompiler compiler = new QCompiler();
      List list = compiler.compileFromClause("/portfolio p, p.positions,/employees e");
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      context.newScope(context.associateScopeID());

      // qs.createIndex("statusIndex",
      // IndexType.FUNCTIONAL,"status","/portfolio");
      qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio p");
      qs.createIndex("empid", IndexType.FUNCTIONAL, "empId", "/employees e");
      Iterator iter = list.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.bindIterator(rIter);
        context.addToIndependentRuntimeItrMap(iterDef);
      }
      CompiledComparison cv[] = new CompiledComparison[5];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "status"),
          new CompiledLiteral(new String("active")), OQLLexerTokenTypes.TOK_EQ);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "type"),
          new CompiledLiteral(new String("type1")), OQLLexerTokenTypes.TOK_EQ);

      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("e"), "empId"),
          new CompiledLiteral(new Integer(10)), OQLLexerTokenTypes.TOK_GT);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(10)), OQLLexerTokenTypes.TOK_GT);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(15)), OQLLexerTokenTypes.TOK_GT);
      CompiledJunction cj = new CompiledJunction(cv, OQLLexerTokenTypes.LITERAL_and);
      context.addDependencies(new CompiledID("dummy"), cj.computeDependencies(context));
      cj.getPlanInfo(context);
      OrganizedOperands oo = cj.testOrganizedOperands(context);
      assertNotNull("OO is null", oo);
      assertTrue("Filter operand of OO not AllGroupJunction",
          oo.filterOperand instanceof AllGroupJunction);
      AllGroupJunction agj = (AllGroupJunction) oo.filterOperand;
      List filterableCC = agj.getGroupOperands();
      assertTrue(agj.getIterOperands().isEmpty());
      assertTrue(agj.getGroupOperands().size() == 2);
      assertTrue(agj.getGroupOperands().get(0) instanceof GroupJunction
          || agj.getGroupOperands().get(1) instanceof GroupJunction);
      assertTrue(agj.getGroupOperands().get(0) instanceof RangeJunction
          || agj.getGroupOperands().get(1) instanceof RangeJunction);
      RangeJunction rj = null;
      if (agj.getGroupOperands().get(0) instanceof RangeJunction) {
        rj = (RangeJunction) agj.getGroupOperands().get(0);
      } else {
        rj = (RangeJunction) agj.getGroupOperands().get(1);
      }

      OrganizedOperands oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[4]);
      assertNotNull(oo1.iterateOperand);
      assertTrue(((CompiledJunction) oo1.iterateOperand).getOperands().contains(cv[0]));
      assertTrue(((CompiledJunction) oo1.iterateOperand).getOperands().contains(cv[1]));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed sue to Exception = " + e);
    }

  }

  /** ********************************************************************** */
  // Asif: Since this is a single region & part fropm Group Junction
  // no other filter is there , we shoudl get a SingleGroupJucntion as
  // filter operand of CompiledJunction & not iter operand in Compiled
  // Juncion
  @Test
  public void testOrganizedOperandsSingleGroupJunctionSingleFilter() {
    // compileFromClause returns a List<CompiledIteratorDef>
    QCompiler compiler = new QCompiler();
    List list = compiler.compileFromClause("/portfolio p, p.positions");
    ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
    context.newScope(context.associateScopeID());
    try {
      qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/portfolio");
      qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
      Iterator iter = list.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.bindIterator(rIter);
        context.addToIndependentRuntimeItrMap(iterDef);
      }
      CompiledComparison cv[] = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_EQ);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "status"),
          new CompiledLiteral(new String("active")), OQLLexerTokenTypes.TOK_EQ);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "type"),
          new CompiledLiteral(new String("type1")), OQLLexerTokenTypes.TOK_EQ);
      CompiledJunction cj = new CompiledJunction(cv, OQLLexerTokenTypes.LITERAL_and);
      context.addDependencies(new CompiledID("dummy"), cj.computeDependencies(context));
      cj.getPlanInfo(context);
      OrganizedOperands oo = cj.testOrganizedOperands(context);
      assertNotNull("The Organized operand object is null", oo);
      assertTrue("Filter Operand  did not happen to be an instance of GroupJunction",
          oo.filterOperand instanceof GroupJunction);
      assertNull("Iterate Operand  not coming out to be null", oo.iterateOperand);
      assertTrue("The Conditions in GroupJunction not equal to 3",
          ((GroupJunction) oo.filterOperand).getOperands().size() == 3);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed sue to Exception = " + e);
    }
  }

  // Asif: Since this is a single region & apart from GroupJunction
  // there exists another filter , We shoudl get two Filter operands
  // One as GroupJunction & the other one the other Filter operand.
  // The un indexed condition should become part of iter operand
  // of CompiledJunction
  @Test
  public void testOrganizedOperandsSingleGroupJunctionMultipleFilter() {
    // compileFromClause returns a List<CompiledIteratorDef>
    QCompiler compiler = new QCompiler();
    List list = compiler.compileFromClause("/portfolio p, p.positions");
    ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
    context.newScope(context.associateScopeID());
    try {
      qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/portfolio");
      qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
      Iterator iter = list.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.bindIterator(rIter);
        context.addToIndependentRuntimeItrMap(iterDef);
      }
      CompiledComparison cv[] = new CompiledComparison[4];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_EQ);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "status"),
          new CompiledLiteral(new String("active")), OQLLexerTokenTypes.TOK_EQ);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "type"),
          new CompiledLiteral(new String("type1")), OQLLexerTokenTypes.TOK_EQ);
      cv[3] = new CompiledComparison(new CompiledLiteral(Boolean.TRUE),
          new CompiledLiteral(Boolean.TRUE), OQLLexerTokenTypes.TOK_EQ);
      CompiledJunction cj = new CompiledJunction(cv, OQLLexerTokenTypes.LITERAL_and);
      context.addDependencies(new CompiledID("dummy"), cj.computeDependencies(context));
      cj.getPlanInfo(context);
      OrganizedOperands oo = cj.testOrganizedOperands(context);
      assertNotNull("OrganizedOperand object is null", oo);
      assertTrue("Filter Openad of OrganizedOperand is not of type CompiledJunction",
          oo.filterOperand instanceof CompiledJunction);
      CompiledJunction temp = (CompiledJunction) oo.filterOperand;
      List operands = temp.getOperands();
      assertTrue("The number of Filter operands not coming to be 2", operands.size() == 2);
      assertTrue(
          "The independent operand shoudl be the first operand of the set of Filter Operands",
          operands.get(0) == cv[3]);
      assertTrue(operands.get(1) instanceof GroupJunction);
      List temp1 = ((GroupJunction) operands.get(1)).getOperands();
      assertTrue("The number & nature of CompiledComparison of Group Junction not as expected",
          temp1.get(0) == cv[0]);
      assertTrue("The number & nature of CompiledComparison of Group Junction not as expected",
          temp1.get(1) == cv[1]);
      assertNotNull("The iter operand of OO is null", oo.iterateOperand);
      assertTrue("IterOperand of OO not as expectd", oo.iterateOperand == cv[2]);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed sue to Exception = " + e);
    }
  }

  // Asif: There are two regions in query but since the conditions in where
  // clause
  // pertain to single region , we still will have a GroupJunction as a filter
  // operand.
  // Since there is only One Filter condition , we should have iter operand
  // as null;
  @Test
  public void testOrganizedOperandsGroupJunctionSingleFilterDoubleRegion() {
    try {
      Region r3 = CacheUtils.createRegion("employees", Employee.class);
      Set add1 = new HashSet();
      add1.add(new Address("411045", "Baner"));
      add1.add(new Address("411001", "DholePatilRd"));
      for (int i = 0; i < 4; i++) {
        r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
      }
      // compileFromClause returns a List<CompiledIteratorDef>
      QCompiler compiler = new QCompiler();
      List list = compiler.compileFromClause("/portfolio p, p.positions,/employees e");
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      context.newScope(context.associateScopeID());

      qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/portfolio");
      qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
      Iterator iter = list.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.bindIterator(rIter);
        context.addToIndependentRuntimeItrMap(iterDef);
      }
      CompiledComparison cv[] = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_EQ);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "status"),
          new CompiledLiteral(new String("active")), OQLLexerTokenTypes.TOK_EQ);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "type"),
          new CompiledLiteral(new String("type1")), OQLLexerTokenTypes.TOK_EQ);
      CompiledJunction cj = new CompiledJunction(cv, OQLLexerTokenTypes.LITERAL_and);
      context.addDependencies(new CompiledID("dummy"), cj.computeDependencies(context));
      cj.getPlanInfo(context);
      OrganizedOperands oo = cj.testOrganizedOperands(context);
      assertNotNull("OO turning out to be null", oo);
      assertTrue("Filter operand of OO not a GroupJunction",
          oo.filterOperand instanceof GroupJunction);
      GroupJunction temp = (GroupJunction) oo.filterOperand;
      List operands = temp.getOperands();
      assertTrue("No. of conditions in Group Junction not as expected of size 3",
          operands.size() == 3);
      assertNull("The iter operand of OO should have bee null", oo.iterateOperand);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to Exception = " + e);
    }
  }

  // Asif: There are two regions in query with a equi join condition. The other
  // conditions do not have index
  // on them. In such case we should have single CompositeGroupJunction . The
  // filterable cc list
  // size should be 1.
  @Test
  public void testOrganizedOperandsSingleCompositeGroupJunctionSingleFilterDoubleRegionWithNoGroupJunction() {
    try {
      Region r3 = CacheUtils.createRegion("employees", Employee.class);
      Set add1 = new HashSet();
      add1.add(new Address("411045", "Baner"));
      add1.add(new Address("411001", "DholePatilRd"));
      for (int i = 0; i < 4; i++) {
        r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
      }
      // compileFromClause returns a List<CompiledIteratorDef>
      QCompiler compiler = new QCompiler();
      List list = compiler.compileFromClause("/portfolio p, p.positions,/employees e");
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      context.newScope(context.associateScopeID());

      // qs.createIndex("statusIndex",
      // IndexType.FUNCTIONAL,"status","/portfolio");
      qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
      qs.createIndex("empid", IndexType.FUNCTIONAL, "empId", "/employees");
      Iterator iter = list.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.bindIterator(rIter);
        context.addToIndependentRuntimeItrMap(iterDef);
      }
      CompiledComparison cv[] = new CompiledComparison[4];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "status"),
          new CompiledLiteral(new String("active")), OQLLexerTokenTypes.TOK_EQ);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "type"),
          new CompiledLiteral(new String("type1")), OQLLexerTokenTypes.TOK_EQ);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("e"), "age"),
          new CompiledLiteral(new Integer(1)), OQLLexerTokenTypes.TOK_EQ);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("e"), "empId"),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_EQ);
      CompiledJunction cj = new CompiledJunction(cv, OQLLexerTokenTypes.LITERAL_and);
      context.addDependencies(new CompiledID("dummy"), cj.computeDependencies(context));
      cj.getPlanInfo(context);
      OrganizedOperands oo = cj.testOrganizedOperands(context);
      assertNotNull("OO is null", oo);
      assertTrue("Filter operand of OO not CompositeGroupJunction",
          oo.filterOperand instanceof CompositeGroupJunction);
      CompositeGroupJunction cgj = (CompositeGroupJunction) oo.filterOperand;
      List filterableCC = cgj.getFilterableCCList();
      assertTrue("No. of filterable CC  not equal to 1", filterableCC.size() == 1);
      assertNull("No GroupJunction inside CompositeGroupJunction should have existed",
          cgj.getGroupJunctionList());
      assertTrue(" Complete expansion flag should have been true", cgj.getExpansionFlag());
      assertNotNull("Iter operands list in CGJ should not be null", cgj.getIterOperands());
      assertTrue(" The size of itr operands should be 3 but size happens to be = "
          + cgj.getIterOperands().size(), cgj.getIterOperands().size() == 3);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed sue to Exception = " + e);
    }
  }

  // Asif: There are two regions in query with the conditions in where clause
  // for both the regions ,but as only conditions belonging to only
  // one group is filter evaluatable , we will have a GroupJunction as a filter
  // operand.
  // Since there are two filter operands, the iter operands belonging to the
  // other Group ( which
  // is not filter evaluatable , should get added to the iter operands of
  // CompiledJunction
  // So the iter operand of CompiledJunction should
  // have one value ( which originally would have gone to GroupJunction)
  @Test
  public void testOrganizedOperandsGroupJunctionDoubleFilterDoubleRegion() {
    try {
      Region r3 = CacheUtils.createRegion("employees", Employee.class);
      Set add1 = new HashSet();
      add1.add(new Address("411045", "Baner"));
      add1.add(new Address("411001", "DholePatilRd"));
      for (int i = 0; i < 4; i++) {
        r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
      }
      // compileFromClause returns a List<CompiledIteratorDef>
      QCompiler compiler = new QCompiler();
      List list = compiler.compileFromClause("/portfolio p, p.positions,/employees e");
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      context.newScope(context.associateScopeID());

      qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/portfolio");
      qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
      Iterator iter = list.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.bindIterator(rIter);
        context.addToIndependentRuntimeItrMap(iterDef);
      }
      CompiledComparison cv[] = new CompiledComparison[5];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_EQ);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "status"),
          new CompiledLiteral(new String("active")), OQLLexerTokenTypes.TOK_EQ);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "type"),
          new CompiledLiteral(new String("type1")), OQLLexerTokenTypes.TOK_EQ);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("e"), "age"),
          new CompiledLiteral(new Integer(1)), OQLLexerTokenTypes.TOK_EQ);
      cv[4] = new CompiledComparison(new CompiledLiteral(Boolean.TRUE),
          new CompiledLiteral(Boolean.TRUE), OQLLexerTokenTypes.TOK_EQ);
      CompiledJunction cj = new CompiledJunction(cv, OQLLexerTokenTypes.LITERAL_and);
      context.addDependencies(new CompiledID("dummy"), cj.computeDependencies(context));
      cj.getPlanInfo(context);
      OrganizedOperands oo = cj.testOrganizedOperands(context);
      assertNotNull("OO is null", oo);
      assertTrue("Filter Operand of OO is not CompiledJunction",
          oo.filterOperand instanceof CompiledJunction);
      CompiledJunction cj1 = ((CompiledJunction) oo.filterOperand);
      assertTrue("Filter opreands of OO not of size 2", cj1.getOperands().size() == 2);
      assertTrue("Indpenednet operand is not the first operand of OO filter",
          cj1.getOperands().get(0) == cv[4]);
      assertTrue("Second Operand of OO Filter not a GroupJunction ",
          cj1.getOperands().get(1) instanceof GroupJunction);
      GroupJunction gj = ((GroupJunction) cj1.getOperands().get(1));
      List opsGJ = gj.getOperands();
      // List its = agj.getIterOperands();
      assertTrue("Operands in GroupJunction not of size 3", opsGJ.size() == 3);
      assertTrue("Complete expansion flag of GroupJunction should be true", gj.getExpansionFlag());
      assertNotNull("Iter operand of OO is null", oo.iterateOperand);
      assertTrue("IterOperand of OO not the condition which was expected",
          oo.iterateOperand == cv[3]);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed sue to Exception = " + e);
    }
  }

  // Asif: There are two regions in query with the conditions in where clause
  // for both the regions such that both the GroupJunctions are filter
  // evaluatable
  // So we should get an AllGroupJunction as a result of organizeOperands of
  // CompiledJunction
  @Test
  public void testOrganizedOperandsAllGroupJunctionWithTwoGroupJunctions() {
    try {
      Region r3 = CacheUtils.createRegion("employees", Employee.class);
      Set add1 = new HashSet();
      add1.add(new Address("411045", "Baner"));
      add1.add(new Address("411001", "DholePatilRd"));
      for (int i = 0; i < 4; i++) {
        r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
      }
      // compileFromClause returns a List<CompiledIteratorDef>
      QCompiler compiler = new QCompiler();
      List list = compiler.compileFromClause("/portfolio p, p.positions,/employees e");
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      context.newScope(context.associateScopeID());

      qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/portfolio");
      qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
      qs.createIndex("empidIndex", IndexType.FUNCTIONAL, "empId", "/employees");
      Iterator iter = list.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.bindIterator(rIter);
        context.addToIndependentRuntimeItrMap(iterDef);
      }
      CompiledComparison cv[] = new CompiledComparison[5];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_EQ);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "status"),
          new CompiledLiteral(new String("active")), OQLLexerTokenTypes.TOK_EQ);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "type"),
          new CompiledLiteral(new String("type1")), OQLLexerTokenTypes.TOK_EQ);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("e"), "age"),
          new CompiledLiteral(new Integer(1)), OQLLexerTokenTypes.TOK_EQ);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("e"), "empId"),
          new CompiledLiteral(new Integer(1)), OQLLexerTokenTypes.TOK_EQ);
      CompiledJunction cj = new CompiledJunction(cv, OQLLexerTokenTypes.LITERAL_and);
      context.addDependencies(new CompiledID("dummy"), cj.computeDependencies(context));
      cj.getPlanInfo(context);
      OrganizedOperands oo = cj.testOrganizedOperands(context);
      assertNotNull("OO is null", oo);
      assertTrue("Filter Operand of OO is not AllGroupJunction",
          oo.filterOperand instanceof AllGroupJunction);
      AllGroupJunction agj = ((AllGroupJunction) oo.filterOperand);
      assertTrue("No of GroupJunctions should be2", agj.getGroupOperands().size() == 2);
      assertNull("Iter operand of OO is not null", oo.iterateOperand);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed sue to Exception = " + e);
    }
  }

  // Asif: There are 3 regions in the query. Two regionsare tied by a join which
  // will create
  // a CompositeGroupJunction . One region has an independent condition & so it
  // will
  // form a GroupJunction . Sinc ethis GroupJunction is outside
  // CompositeGroupJunction
  // we will have an AllGroupJunction. Within the CompositeGroupJunction , we
  // will have one
  // GroupJunction. One Filterable CC & no iter operand. Howver th eiet operand
  // of
  // allGroupJunction will be null & will contain one operand
  @Test
  public void testOrganizedOperandsAllGroupJunctionWithOneGroupJunctionAndOneCompositeGroupJunction() {
    try {
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
      // compileFromClause returns a List<CompiledIteratorDef>
      QCompiler compiler = new QCompiler();
      List list =
          compiler.compileFromClause("/portfolio p, p.positions,/employees e, /portfolio1 p1");
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      context.newScope(context.associateScopeID());

      qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status", "/portfolio");
      qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
      qs.createIndex("idIndex1", IndexType.FUNCTIONAL, "ID", "/portfolio1");
      qs.createIndex("empidIndex", IndexType.FUNCTIONAL, "empId", "/employees");
      Iterator iter = list.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.bindIterator(rIter);
        context.addToIndependentRuntimeItrMap(iterDef);
      }
      CompiledComparison cv[] = new CompiledComparison[5];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "status"),
          new CompiledLiteral(new String("active")), OQLLexerTokenTypes.TOK_EQ);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "type"),
          new CompiledLiteral(new String("type1")), OQLLexerTokenTypes.TOK_EQ);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("e"), "age"),
          new CompiledLiteral(new Integer(1)), OQLLexerTokenTypes.TOK_EQ);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("e"), "empId"),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_EQ);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p1"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_EQ);
      CompiledJunction cj = new CompiledJunction(cv, OQLLexerTokenTypes.LITERAL_and);
      context.addDependencies(new CompiledID("dummy"), cj.computeDependencies(context));
      cj.getPlanInfo(context);
      OrganizedOperands oo = cj.testOrganizedOperands(context);
      assertNotNull("OO is null", oo);
      assertTrue("Filter Operand of OO is not AllGroupJunction",
          oo.filterOperand instanceof AllGroupJunction);
      AllGroupJunction agj = ((AllGroupJunction) oo.filterOperand);
      assertTrue("No of GroupJunctions should be 2", agj.getGroupOperands().size() == 2);
      assertNotNull("The Iter operand should be not null", agj.getIterOperands());
      assertTrue("The Iter operand should be of size = 1", agj.getIterOperands().size() == 1);
      assertTrue("The Iter operand not matching the correct compiled comaprison",
          agj.getIterOperands().get(0) == cv[2]);
      // The List below should have two elements . One a GroupJunction & another
      // a CompositeGroupJunction
      List groupOps = agj.getGroupOperands();
      Iterator itr = groupOps.iterator();
      CompositeGroupJunction cgj = null;
      GroupJunction gj = null;
      while (itr.hasNext()) {
        Object temp = itr.next();
        if (temp instanceof CompositeGroupJunction) {
          cgj = (CompositeGroupJunction) temp;
        } else if (temp instanceof GroupJunction) {
          gj = (GroupJunction) temp;
        } else {
          fail(" The junction list in AllGroupJunction is not correct");
        }
      }
      assertTrue(" The CGJ formed is incorrect",
          cgj.getGroupJunctionList() != null && cgj.getGroupJunctionList().size() == 1
              && cgj.getIterOperands() == null && cgj.getFilterableCCList().get(0) == cv[3]
              && !cgj.getExpansionFlag());
      assertTrue(" The GJ inside CGJ is incorrect",
          ((GroupJunction) cgj.getGroupJunctionList().get(0)).getOperands().size() == 2
              && !((GroupJunction) cgj.getGroupJunctionList().get(0)).getExpansionFlag());
      assertTrue(" The GJ of AllGroupJunction is incorrect", !gj.getExpansionFlag()
          && gj.getOperands().size() == 1 && gj.getOperands().get(0) == cv[4]);
      assertNull("Iter operand of OO is not null", oo.iterateOperand);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed sue to Exception = " + e);
    }
  }

  // Asif: There are two regions in query with a equi join condition. The other
  // conditions do not have index
  // on them. In such case we should have single CompositeGroupJunction . The
  // filterable cc list
  // size should be 1.
  @Test
  public void testOrganizedOperandsSingleCompositeGroupJunctionSingleFilterFourRegionsWithNoGroupJunction() {
    try {
      Region r3 = CacheUtils.createRegion("employees", Employee.class);
      Set add1 = new HashSet();
      add1.add(new Address("411045", "Baner"));
      add1.add(new Address("411001", "DholePatilRd"));
      for (int i = 0; i < 4; i++) {
        r3.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
      }
      Region r2 = CacheUtils.createRegion("employees1", Employee.class);
      Set add2 = new HashSet();
      add2.add(new Address("411045", "Baner"));
      add2.add(new Address("411001", "DholePatilRd"));
      for (int i = 0; i < 4; i++) {
        r2.put(i + "", new Employee("empName", (20 + i), i, "Mr.", (5000 + i), add1));
      }
      Region r4 = CacheUtils.createRegion("portfolio1", Portfolio.class);
      for (int i = 0; i < 4; i++) {
        r4.put(i + "", new Portfolio(i));
      }
      // compileFromClause returns a List<CompiledIteratorDef>
      QCompiler compiler = new QCompiler();
      List list = compiler.compileFromClause(
          "/portfolio p, p.positions,/employees e, /employees1 e1, /portfolio p1");
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      context.newScope(context.associateScopeID());

      // qs.createIndex("statusIndex",
      // IndexType.FUNCTIONAL,"status","/portfolio");
      qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
      qs.createIndex("empid", IndexType.FUNCTIONAL, "empId", "/employees");
      qs.createIndex("empid1", IndexType.FUNCTIONAL, "empId", "/employees1");
      qs.createIndex("idIndex1", IndexType.FUNCTIONAL, "ID", "/portfolio1");
      Iterator iter = list.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.bindIterator(rIter);
        context.addToIndependentRuntimeItrMap(iterDef);
      }
      CompiledComparison cv[] = new CompiledComparison[3];
      /*
       * cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "status"), new
       * CompiledLiteral(new String("active")), OQLLexerTokenTypes.TOK_EQ); cv[1] = new
       * CompiledComparison(new CompiledPath(new CompiledID("p"), "type"), new CompiledLiteral(new
       * String("type1")), OQLLexerTokenTypes.TOK_EQ); cv[2] = new CompiledComparison(new
       * CompiledPath(new CompiledID("e"), "age"), new CompiledLiteral(new Integer(1)),
       * OQLLexerTokenTypes.TOK_EQ);
       */
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("e"), "empId"),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_EQ);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("e1"), "empId"),
          new CompiledPath(new CompiledID("p1"), "ID"), OQLLexerTokenTypes.TOK_EQ);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("e1"), "empId"),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_EQ);
      CompiledJunction cj = new CompiledJunction(cv, OQLLexerTokenTypes.LITERAL_and);
      context.addDependencies(new CompiledID("dummy"), cj.computeDependencies(context));
      cj.getPlanInfo(context);
      OrganizedOperands oo = cj.testOrganizedOperands(context);
      assertNotNull("OO is null", oo);
      assertTrue("Filter operand of OO not CompositeGroupJunction",
          oo.filterOperand instanceof CompositeGroupJunction);
      CompositeGroupJunction cgj = (CompositeGroupJunction) oo.filterOperand;
      List filterableCC = cgj.getFilterableCCList();
      assertTrue("No. of filterable CC  not equal to 3", filterableCC.size() == 3);
      assertNull("No GroupJunction inside CompositeGroupJunction should have existed",
          cgj.getGroupJunctionList());
      assertTrue(" Complete expansion flag should have been true", cgj.getExpansionFlag());
      /*
       * assertNotNull("Iter operands list in CGJ should not be null", cgj .getIterOperands());
       * assertTrue( " The size of itr operands should be 3 but size happens to be = " +
       * cgj.getIterOperands().size(), cgj.getIterOperands().size() == 3);
       */
    } catch (Exception e) {
      e.printStackTrace();
      fail("Test failed sue to Exception = " + e);
    }
  }


  // Tests for plain creation of RangeJunction ( single or within a Group or CompositeGroup or
  // AllGroup )
  // which is agnostic of whether the RangeJunction is of type AND or OR
  //////////////////////////////////////////////////////////////////////////////////////
  /**
   * Tests the creation of a single RangeJunction if the CompiledJunction only contains same index
   * condition without iter operand for AND
   */
  @Test
  public void testOrganizedOperandsSingleRangeJunctionCreationWithNoIterOperandForAND() {
    LogWriter logger = CacheUtils.getLogger();
    try {
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      CompiledComparison cv[] = null;
      cv = new CompiledComparison[12];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_EQ);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[5] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      cv[6] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_EQ);
      cv[7] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_LE);
      cv[8] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      cv[9] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[10] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[11] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      OrganizedOperands oo = this
          .oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv, context);
      assertNotNull("OrganizedOperand object is null", oo);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      assertEquals(cv.length, rj.getOperands().size());
    } catch (Exception e) {
      logger.error(e);
      fail(e.toString());
    }
  }

  /**
   * Tests the creation of a single RangeJunction if the CompiledJunction only contains same index
   * condition and an iter operand for AND
   */
  @Test
  public void testOrganizedOperandsSingleRangeJunctionCreationWithIterOperandForAND() {
    LogWriter logger = CacheUtils.getCache().getLogger();
    try {
      CompiledComparison cv[] = null;
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      cv = new CompiledComparison[13];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_EQ);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[5] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      cv[6] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_EQ);
      cv[7] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_LE);
      cv[8] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      cv[9] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[10] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[11] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      cv[12] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "createTime"),
          new CompiledLiteral(new Long(7)), OQLLexerTokenTypes.TOK_NE);

      OrganizedOperands oo = this
          .oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv, context);
      assertNotNull("OrganizedOperand object is null", oo);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      assertEquals(cv.length, rj.getOperands().size());
    } catch (Exception e) {
      logger.error(e);
      fail(e.toString());
    }
  }

  /**
   * Tests the creation of a single RangeJunction if the CompiledJunction only contains same index
   * condition without iter operand for OR
   */
  @Ignore
  @Test
  public void testOrganizedOperandsSingleRangeJunctionCreationWithNoIterOperandForOR() {
    LogWriter logger = CacheUtils.getLogger();
    try {
      CompiledComparison cv[] = null;
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      cv = new CompiledComparison[12];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_EQ);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[5] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      cv[6] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_EQ);
      cv[7] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_LE);
      cv[8] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      cv[9] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[10] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[11] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      OrganizedOperands oo = this
          .oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_or, cv, context);
      assertNotNull("OrganizedOperand object is null", oo);
      assertTrue("Filter Openad of OrganizedOperand is not of type CompiledJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      assertEquals(cv.length, rj.getOperands().size());
    } catch (Exception e) {
      logger.error(e);
      fail(e.toString());
    }
  }
  //////////////////////////////////////////////////////////////////////////////

  /// ***************************BEGIN*********************************************//
  // Tests the RangeJunction's organizeOperands for the correct creation of Evaluator.
  // These tests are dependent on whether the RangeJunction is of type AND or OR
  // All the tests below are for AND
  /**
   * Tests the functionality of organizedOperands function of a RangeJunction which is an AND with
   * no IterOperand. If the RangeJunction boils down to a single condition which can be evalauted as
   * a CompiledComparison then the filter operand will be a CompiledComparison
   *
   */
  @Test
  public void testOrganizedOperandsOfSingleRangeJunctionWithNoIterOperandForAND_1() {
    LogWriter logger = CacheUtils.getCache().getLogger();
    try {
      CompiledComparison cv[] = null;
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_LT);

      OrganizedOperands oo = this
          .oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv, context);
      assertNotNull("OrganizedOperand object is null", oo);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      assertEquals(cv.length, rj.getOperands().size());
      OrganizedOperands oo1 = rj.organizeOperands(context);
      assertNotNull(oo1);
      assertNull(oo1.iterateOperand);
      assertNotNull(oo1.filterOperand);
      assertEquals(oo1.filterOperand, cv[2]);

    } catch (Exception e) {
      logger.error(e);
      fail(e.toString());
    }
  }

  /**
   * Tests the functionality of organizedOperands function of a RangeJunction which is an AND with
   * no IterOperand but where the operator needs reflection. as the condition is Key > Path ( which
   * means Path less than Key) If the RangeJunction boils down toa single condition which can be
   * evalauted as a CompiledComparison then the filter operand will be a CompiledComparison
   *
   */
  @Test
  public void testOrganizedOperandsOfSingleRangeJunctionWithNoIterOperandForAND_2() {
    LogWriter logger = CacheUtils.getCache().getLogger();
    try {
      CompiledComparison cv[] = null;
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledLiteral(new Integer(2)),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_GT);

      OrganizedOperands oo = this
          .oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv, context);
      assertNotNull("OrganizedOperand object is null", oo);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      assertEquals(cv.length, rj.getOperands().size());
      OrganizedOperands oo1 = rj.organizeOperands(context);
      assertNotNull(oo1);
      assertNull(oo1.iterateOperand);
      assertNotNull(oo1.filterOperand);
      assertEquals(oo1.filterOperand, cv[2]);

    } catch (Exception e) {
      logger.error(e);
      fail(e.toString());
    }
  }



  /**
   * Tests the functionality of organizedOperands function of a RangeJunction which is an AND with a
   * IterOperand but where the operator needs reflection. as the condition is Key > Path ( which
   * means Path less than Key) The RangeJunction boils down to a single condition which can be
   * evalauted as a CompiledComparison so the filter operand will be a CompiledComparison. Thus the
   * organizdeOperand's filter operand will be a CompiledComparison while its Iteroperand will be a
   * CompiledComparison
   *
   */
  @Test
  public void testOrganizedOperandsOfSingleRangeJunctionWithIterOperandForAND_1() {
    LogWriter logger = CacheUtils.getCache().getLogger();
    try {
      CompiledComparison cv[] = null;
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      cv = new CompiledComparison[4];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledLiteral(new Integer(2)),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_GT);
      cv[3] = new CompiledComparison(new CompiledLiteral(new Integer(2)),
          new CompiledPath(new CompiledID("p"), "createTime"), OQLLexerTokenTypes.TOK_GT);
      OrganizedOperands oo = this
          .oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv, context);
      assertNotNull("OrganizedOperand object is null", oo);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      assertEquals(cv.length, rj.getOperands().size());
      OrganizedOperands oo1 = rj.organizeOperands(context);
      assertNotNull(oo1);
      assertEquals(cv[3], oo1.iterateOperand);
      assertEquals(oo1.filterOperand, cv[2]);

    } catch (Exception e) {
      logger.error(e);
      fail(e.toString());
    }
  }


  /**
   * Tests the functionality of organizedOperands function of a RangeJunction which is an AND with
   * two IterOperands but where the operator needs reflection. as the condition is Key > Path (
   * which means Path less than Key) The RangeJunction boils down to a single condition which can be
   * evalauted as a CompiledComparison so the filter operand will be a CompiledComparison. Thus the
   * organizdeOperand's filter operand will be a CompiledComparison while its Iteroperand will be a
   * CompiledJunction
   *
   */
  @Test
  public void testOrganizedOperandsOfSingleRangeJunctionWithTwoIterOperandsForAND_2() {
    LogWriter logger = CacheUtils.getCache().getLogger();
    try {
      CompiledComparison cv[] = null;
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      cv = new CompiledComparison[5];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledLiteral(new Integer(2)),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_GT);
      cv[3] = new CompiledComparison(new CompiledLiteral(new Integer(2)),
          new CompiledPath(new CompiledID("p"), "createTime"), OQLLexerTokenTypes.TOK_GT);
      cv[4] = new CompiledComparison(new CompiledLiteral(new String("xyz")),
          new CompiledPath(new CompiledID("p"), "getPk"), OQLLexerTokenTypes.TOK_GT);
      OrganizedOperands oo = this
          .oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv, context);
      assertNotNull("OrganizedOperand object is null", oo);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      assertEquals(cv.length, rj.getOperands().size());
      OrganizedOperands oo1 = rj.organizeOperands(context);
      assertNotNull(oo1);
      assertTrue(oo1.iterateOperand instanceof CompiledJunction);
      assertTrue(oo1.iterateOperand.getChildren().size() == 2);
      assertTrue(oo1.iterateOperand.getChildren().get(0) == cv[3]);
      assertTrue(oo1.iterateOperand.getChildren().get(1) == cv[4]);
      assertEquals(oo1.filterOperand, cv[2]);

    } catch (Exception e) {
      logger.error(e);
      fail(e.toString());
    }
  }

  /**
   * Tests the functionality of organizedOperands function of a RangeJunction which is an AND with
   * two IterOperands but where the operator needs reflection. as the condition is Key > Path (
   * which means Path less than Key) The RangeJunction boils down to a condition and a NOT EQUAL
   * which will be evalauted as a SingleCondnEvaluator so the filter operand will be a
   * SingleCondnEvaluator. Its Iter operand will be a CompiledJunction
   *
   */
  @Test
  public void testOrganizedOperandsOfSingleRangeJunctionWithTwoIterOperandsForAND_3() {
    LogWriter logger = CacheUtils.getCache().getLogger();
    try {
      CompiledComparison cv[] = null;
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      // case1: a <7 and a<=5 and 2>a and 2> createTime and "xyz" > pid
      // and 100 != a and 200 !=a and 1 != a
      cv = new CompiledComparison[8];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledLiteral(new Integer(2)),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_GT);
      cv[3] = new CompiledComparison(new CompiledLiteral(new Integer(2)),
          new CompiledPath(new CompiledID("p"), "createTime"), OQLLexerTokenTypes.TOK_GT);
      cv[4] = new CompiledComparison(new CompiledLiteral(new String("xyz")),
          new CompiledPath(new CompiledID("p"), "getPk"), OQLLexerTokenTypes.TOK_GT);
      cv[5] = new CompiledComparison(new CompiledLiteral(new Integer(100)),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_NE);
      cv[6] = new CompiledComparison(new CompiledLiteral(new Integer(200)),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_NE);

      cv[7] = new CompiledComparison(new CompiledLiteral(new Integer(1)),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_NE);

      OrganizedOperands oo = this
          .oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv, context);
      assertNotNull("OrganizedOperand object is null", oo);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      assertEquals(cv.length, rj.getOperands().size());
      OrganizedOperands oo1 = rj.organizeOperands(context);
      assertNotNull(oo1);
      assertTrue(oo1.iterateOperand instanceof CompiledJunction);
      assertTrue(oo1.iterateOperand.getChildren().size() == 2);
      assertTrue(oo1.iterateOperand.getChildren().get(0) == cv[3]);
      assertTrue(oo1.iterateOperand.getChildren().get(1) == cv[4]);
      assertTrue(RangeJunction.isInstanceOfSingleCondnEvaluator(oo1.filterOperand));
      Set keysToRemove = RangeJunction.getKeysToBeRemoved(oo1.filterOperand);
      assertEquals(1, keysToRemove.size());
      assertTrue(keysToRemove.contains(new Integer(1)));

    } catch (Exception e) {
      logger.error(e);
      fail(e.toString());
    }
  }

  /**
   * Tests the functionality of organizedOperands function of a RangeJunction which is an AND with
   * two IterOperands but where the operator needs reflection. as the condition is Key <= Path (
   * which means Path less than Key) The RangeJunction boils down to a condition and a NOT EQUAL
   * which will be evalauted as a SingleCondnEvaluator so the filter operand will be a
   * SingleCondnEvaluator. Its Iter operand will be a CompiledJunction. This checks for the
   * SingleCondnEvaluator with > operator
   *
   */
  @Test
  public void testOrganizedOperandsOfSingleRangeJunctionWithTwoIterOperandsForAND_4() {
    LogWriter logger = CacheUtils.getCache().getLogger();
    try {
      CompiledComparison cv[] = null;
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      cv = new CompiledComparison[7];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_GT);
      cv[2] = new CompiledComparison(new CompiledLiteral(new Integer(2)),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_LT);
      cv[3] = new CompiledComparison(new CompiledLiteral(new Integer(2)),
          new CompiledPath(new CompiledID("p"), "createTime"), OQLLexerTokenTypes.TOK_GT);
      cv[4] = new CompiledComparison(new CompiledLiteral(new String("xyz")),
          new CompiledPath(new CompiledID("p"), "getPk"), OQLLexerTokenTypes.TOK_GT);
      cv[5] = new CompiledComparison(new CompiledLiteral(new Integer(100)),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_NE);
      cv[6] = new CompiledComparison(new CompiledLiteral(new Integer(200)),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_NE);

      OrganizedOperands oo = this
          .oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv, context);
      assertNotNull("OrganizedOperand object is null", oo);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      assertEquals(cv.length, rj.getOperands().size());
      OrganizedOperands oo1 = rj.organizeOperands(context);
      assertNotNull(oo1);
      assertTrue(oo1.iterateOperand instanceof CompiledJunction);
      assertTrue(oo1.iterateOperand.getChildren().size() == 2);
      assertTrue(oo1.iterateOperand.getChildren().get(0) == cv[3]);
      assertTrue(oo1.iterateOperand.getChildren().get(1) == cv[4]);
      assertTrue(RangeJunction.isInstanceOfSingleCondnEvaluator(oo1.filterOperand));
      Set keysToRemove = RangeJunction.getKeysToBeRemoved(oo1.filterOperand);
      assertEquals(2, keysToRemove.size());
      assertTrue(keysToRemove.contains(new Integer(100)));
      assertTrue(keysToRemove.contains(new Integer(200)));

    } catch (Exception e) {
      logger.error(e);
      fail(e.toString());
    }
  }

  /**
   * Tests the functionality of organizedOperands function of a RangeJunction for various
   * combinations of GREATER THAN and Not equal conditions etc which results in a
   * SingleCondnEvaluator or CompiledComparison for a AND junction. It checks the correctness of the
   * operator & the evaluated key
   *
   */
  @Test
  public void testOrganizedOperandsSingleCondnEvalMultipleGreaterThanInEqualities_AND() {
    LogWriter logger = CacheUtils.getCache().getLogger();
    try {
      CompiledComparison cv[] = null;
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      /** **********For ALL greater or greater than combinations********* */
      // Case 1 : a >7 and a >=4 and a > 5 and a > 7
      cv = new CompiledComparison[4];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_GE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_GT);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);

      OrganizedOperands oo = this
          .oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv, context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      OrganizedOperands oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[0] || oo1.filterOperand == cv[3]);
      // Case2: a>=8 and a >4 and a>=6 and a>=8
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_GE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(6)), OQLLexerTokenTypes.TOK_GE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_GE);

      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[0] || oo1.filterOperand == cv[3]);
      cv = new CompiledComparison[5];
      // Case 3 : 7 < a and a >=4 and a > 5 and a >= 7 and a != 15
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_GE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_GT);
      cv[3] = new CompiledComparison(new CompiledLiteral(new Integer(7)),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_LT);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(15)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(RangeJunction.isInstanceOfSingleCondnEvaluator(oo1.filterOperand));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_GT);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(7)));
      assertTrue(RangeJunction.getIndex(oo1.filterOperand).getName().equals("idIndex"));
      assertTrue(RangeJunction.getKeysToBeRemoved(oo1.filterOperand).size() == 1 && RangeJunction
          .getKeysToBeRemoved(oo1.filterOperand).iterator().next().equals(new Integer(15)));
      // Case 4 : 7 < a and a >=7
      cv = new CompiledComparison[2];
      cv[0] = new CompiledComparison(new CompiledLiteral(new Integer(7)),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_LT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[0]);
      // Case 5 : a > 7 and a >=7
      cv = new CompiledComparison[2];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[0]);

      // Case 6 : a >= 7 and a >7
      cv = new CompiledComparison[2];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[1]);

      // Case 7 : a >= 8 and a >7
      cv = new CompiledComparison[2];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[0]);

      // Case 8 : a >=8 and a > =7
      cv = new CompiledComparison[2];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[0]);

      // Case 9 : a >=7 and a > =8
      cv = new CompiledComparison[2];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_GE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[1]);

      // Case 10 : 7 < a and a >=7 and a != 13
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledLiteral(new Integer(7)),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_LT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(13)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(7)));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_GT);

      // Case 11 : a > 7 and a >=7 and a != 13
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(13)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(7)));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_GT);

      // Case 12 : a >= 7 and a >7 and a != 13
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(13)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(7)));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_GT);

      // Case 13 : a >= 8 and a >7 and a != 13
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(13)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(8)));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_GE);

      // Case 14 : a >=8 and a > =7 and a !=13
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(13)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(8)));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_GE);

      // Case 15 : a >=7 and a > =8 and a != 13
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_GE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(13)), OQLLexerTokenTypes.TOK_NE);

      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(8)));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_GE);
      // Case 15 : a >=7 and a > =8 and a != 8
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_GE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(8)));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_GE);
      assertTrue(RangeJunction.getKeysToBeRemoved((oo1.filterOperand)).iterator().next()
          .equals(new Integer(8)));

      // Case 16 : a >=7 and a > =8 and a != 7
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_GE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[1]);

      // Case 17 : a >=7 and a > =8 and a != 7 and a!=4 and a != 3
      cv = new CompiledComparison[5];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_GE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_NE);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(3)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[1]);
      // Case 18 : a >=7 and a > =8 and a != 7 and a!=4 and a != 3 and a != 20
      // and a != 8
      cv = new CompiledComparison[7];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_GE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_NE);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(3)), OQLLexerTokenTypes.TOK_NE);
      cv[5] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(20)), OQLLexerTokenTypes.TOK_NE);
      cv[6] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_NE);

      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(8)));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_GE);
      Iterator itr = RangeJunction.getKeysToBeRemoved((oo1.filterOperand)).iterator();
      Object temp;
      assertTrue((temp = itr.next()).equals(new Integer(8)) || temp.equals(new Integer(20)));
      assertTrue((temp = itr.next()).equals(new Integer(8)) || temp.equals(new Integer(20)));
      assertFalse(itr.hasNext());

      // //////////////////////////////////////////////////////////////
    } catch (Exception e) {
      logger.error(e);
      fail(e.toString());
    }
  }

  /**
   * Tests the functionality of organizedOperands function of a RangeJunction for various
   * combinations of LESS THAN and Not equal conditions etc which results in a SingleCondnEvaluator
   * or CompiledComparison for a AND junction. It checks the correctness of the operator & the
   * evaluated key
   *
   */
  @Test
  public void testOrganizedOperandsSingleCondnEvalMultipleLessThanInEqualities_AND() {
    LogWriter logger = CacheUtils.getCache().getLogger();
    try {
      CompiledComparison cv[] = null;
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      /**
       * ******************For all LESS THAN OR LESS THAN EQUAL To ********************
       */
      // Case 1 : a < 7 and a <=4 and a < 5 and a <=4
      cv = new CompiledComparison[4];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_LT);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_LE);

      OrganizedOperands oo = this
          .oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv, context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      OrganizedOperands oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[1] || oo1.filterOperand == cv[3]);
      // Case2: a<=8 and a < 12 and a <=10 and a<=8
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_LE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(12)), OQLLexerTokenTypes.TOK_LT);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(10)), OQLLexerTokenTypes.TOK_LT);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_LT);

      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[0] || oo1.filterOperand == cv[3]);
      cv = new CompiledComparison[5];
      // Case 3 : 3 >= a and a <=4 and a < 5 and a <= 3 and a != 1
      cv[0] = new CompiledComparison(new CompiledLiteral(new Integer(3)),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_LT);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(3)), OQLLexerTokenTypes.TOK_LE);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(1)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(RangeJunction.isInstanceOfSingleCondnEvaluator(oo1.filterOperand));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_LE);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(3)));
      assertTrue(RangeJunction.getIndex(oo1.filterOperand).getName().equals("idIndex"));
      assertTrue(RangeJunction.getKeysToBeRemoved(oo1.filterOperand).size() == 1 && RangeJunction
          .getKeysToBeRemoved(oo1.filterOperand).iterator().next().equals(new Integer(1)));
      // Case 4 : 3 > a and a <=2
      cv = new CompiledComparison[2];
      cv[0] = new CompiledComparison(new CompiledLiteral(new Integer(3)),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_GT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_LE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[1]);
      // Case 5 : a < 7 and a <=7
      cv = new CompiledComparison[2];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[0]);

      // Case 6 : a <= 7 and a <7
      cv = new CompiledComparison[2];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[1]);

      // Case 7 : a <= 8 and a <9
      cv = new CompiledComparison[2];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_LE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(9)), OQLLexerTokenTypes.TOK_LT);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[0]);

      // Case 8 : a <=8 and a <=10
      cv = new CompiledComparison[2];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_LE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(10)), OQLLexerTokenTypes.TOK_LE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[0]);

      // Case 9 : a <=6 and a <= 5
      cv = new CompiledComparison[2];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(6)), OQLLexerTokenTypes.TOK_LE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_LE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[1]);

      // Case 10 : 7 > a and a <=7 and a != 2
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledLiteral(new Integer(7)),
          new CompiledPath(new CompiledID("p"), "ID"), OQLLexerTokenTypes.TOK_GT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(7)));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_LT);
      assertTrue(RangeJunction.getKeysToBeRemoved(oo1.filterOperand).iterator().next()
          .equals(new Integer(2)));
      // Case 11 : a < 7 and a <=7 and a != 1
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(1)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(7)));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_LT);
      assertTrue(RangeJunction.getKeysToBeRemoved(oo1.filterOperand).iterator().next()
          .equals(new Integer(1)));
      // Case 12 : a <= 7 and a <7 and a != 1
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(1)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(7)));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_LT);
      assertTrue(RangeJunction.getKeysToBeRemoved(oo1.filterOperand).iterator().next()
          .equals(new Integer(1)));
      // Case 13 : a <= 8 and a <9 and a !=1
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_LE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(9)), OQLLexerTokenTypes.TOK_LT);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(1)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(8)));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_LE);
      assertTrue(RangeJunction.getKeysToBeRemoved(oo1.filterOperand).iterator().next()
          .equals(new Integer(1)));

      // Case 14 : a <=8 and a <=9 and a !=1
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_LE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(9)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(1)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(8)));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_LE);
      assertTrue(RangeJunction.getKeysToBeRemoved(oo1.filterOperand).iterator().next()
          .equals(new Integer(1)));
      // Case 15 : a <=7 and a <=6 and a != 1
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(6)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(1)), OQLLexerTokenTypes.TOK_NE);

      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(6)));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_LE);
      assertTrue(RangeJunction.getKeysToBeRemoved(oo1.filterOperand).iterator().next()
          .equals(new Integer(1)));
      // Case 15 : a <=7 and a <= 6and a != 6
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(6)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(6)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(6)));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_LE);
      assertTrue(RangeJunction.getKeysToBeRemoved((oo1.filterOperand)).iterator().next()
          .equals(new Integer(6)));

      // Case 16 : a <=7 and a <=6 and a != 7
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(6)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[1]);

      // Case 17 : a <=7 and a <=6 and a != 7 and a!=10 and a != 8
      cv = new CompiledComparison[5];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(6)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(10)), OQLLexerTokenTypes.TOK_NE);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[1]);
      // Case 18 : a <=7 and a <=6 and a != 7 and a!=8 and a !=9 and a != 2 and
      // a != 6
      cv = new CompiledComparison[7];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(6)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_NE);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(9)), OQLLexerTokenTypes.TOK_NE);
      cv[5] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_NE);
      cv[6] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(6)), OQLLexerTokenTypes.TOK_NE);

      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorKey(oo1.filterOperand).equals(new Integer(6)));
      assertTrue(RangeJunction
          .getSingleCondnEvaluatorOperator(oo1.filterOperand) == OQLLexerTokenTypes.TOK_LE);
      Iterator itr = RangeJunction.getKeysToBeRemoved((oo1.filterOperand)).iterator();
      Object temp;
      assertTrue((temp = itr.next()).equals(new Integer(2)) || temp.equals(new Integer(6)));
      assertTrue((temp = itr.next()).equals(new Integer(2)) || temp.equals(new Integer(6)));
      assertFalse(itr.hasNext());
      // //////////////////////////////////////////////////////////////
    } catch (Exception e) {
      logger.error(e);
      fail(e.toString());
    }
  }

  /**
   * Tests the correct creation of NotEqualConditionEvaluator For AND junction
   *
   */
  @Test
  public void testNotEqualConditionEvaluator_AND() {
    LogWriter logger = CacheUtils.getLogger();
    try {
      CompiledComparison cv[] = null;
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      /**
       * ******************For all LESS THAN OR LESS THAN EQUAL To ********************
       */
      // Case 1 : a != 7 and a !=4 and a != 5 and a != 5
      cv = new CompiledComparison[4];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_NE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_NE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_NE);

      OrganizedOperands oo = this
          .oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv, context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      OrganizedOperands oo1 = rj.organizeOperands(context);
      assertTrue(RangeJunction.isInstanceOfNotEqualConditionEvaluator(oo1.filterOperand));
      Set keysRemove = RangeJunction.getKeysToBeRemoved(oo1.filterOperand);
      assertTrue(keysRemove.size() == 3);
      assertTrue(keysRemove.contains(new Integer(5)));
      assertTrue(keysRemove.contains(new Integer(7)));
      assertTrue(keysRemove.contains(new Integer(4)));
      // Case 2 : a != 7 and a != null and a != undefined
      CompiledValue[] cv1 = new CompiledValue[3];
      cv1[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      cv1[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(null), OQLLexerTokenTypes.TOK_NE);
      cv1[2] = new CompiledUndefined(new CompiledPath(new CompiledID("p"), "ID"), false);

      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv1,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand instanceof GroupJunction);
      CompiledValue[] ops = ((GroupJunction) oo1.filterOperand)._operands;
      // assertTrue(cv1[0] == ops[0] || cv1[0] == ops[1] || cv1[0] == ops[2]);
      if (RangeJunction.isInstanceOfNotEqualConditionEvaluator(ops[0])) {
        assertTrue(
            RangeJunction.getKeysToBeRemoved(ops[0]).iterator().next().equals(new Integer(7)));
      } else if (RangeJunction.isInstanceOfNotEqualConditionEvaluator(ops[1])) {
        assertTrue(
            RangeJunction.getKeysToBeRemoved(ops[1]).iterator().next().equals(new Integer(7)));
      } else if (RangeJunction.isInstanceOfNotEqualConditionEvaluator(ops[2])) {
        assertTrue(
            RangeJunction.getKeysToBeRemoved(ops[2]).iterator().next().equals(new Integer(7)));
      } else {
        fail("NotEqualConditionEvaluator not found");
      }
      assertTrue(cv1[1] == ops[0] || cv1[1] == ops[1] || cv1[1] == ops[2]);
      assertTrue(cv1[2] == ops[0] || cv1[2] == ops[1] || cv1[2] == ops[2]);
    } catch (Exception e) {
      logger.error(e.toString());
      fail(e.toString());
    }
  }

  /**
   * If possible , in case where RangeJunction contains something like a != 7 and a != undefined & a
   * != null, since undefined & null are not groupable , the a != 7 is also evaluated individually.
   * In such case, it makes sense not to create an Object of
   * rangeJunction.NotEqualConditionEvaluator for evaluating a !=7 , which however is the case now.
   * May be at some point, we should either try to club null & undefined as a part of
   * RangeJunctionEvaluator or we should not create an object of NotEqualConditionEvaluator.
   *
   */
  @Ignore
  @Test
  public void testNotEqualCoupledWithUndefinedAndNotNull() {
    LogWriter logger = CacheUtils.getLogger();
    try {
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      // Case 2 : a != 7 and a != null and a != undefined
      CompiledValue[] cv1 = new CompiledValue[3];
      cv1[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      cv1[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(null), OQLLexerTokenTypes.TOK_NE);
      cv1[2] = new CompiledUndefined(new CompiledPath(new CompiledID("p"), "ID"), false);

      OrganizedOperands oo = this.oganizedOperandsSingleRangeJunctionCreation(
          OQLLexerTokenTypes.LITERAL_and, cv1, context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      OrganizedOperands oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand instanceof GroupJunction);
      CompiledValue[] ops = ((GroupJunction) oo1.filterOperand)._operands;
      assertTrue(cv1[0] == ops[0] || cv1[0] == ops[1] || cv1[0] == ops[2]);
      assertTrue(cv1[1] == ops[0] || cv1[1] == ops[1] || cv1[1] == ops[2]);
      assertTrue(cv1[2] == ops[0] || cv1[2] == ops[1] || cv1[2] == ops[2]);
    } catch (Exception e) {
      logger.error(e.toString());
      fail(e.toString());
    }
  }

  /**
   * Test presence of equal condition in a AND RangeJunction An equal condition if accompanied by
   * any other condition in a RangeJunction ( except not null) should always return the equal
   * condition or a boolean false indicating empty resultset
   */
  @Test
  public void testEqualConditionInRangeJunction_AND() {
    LogWriter logger = CacheUtils.getLogger();
    try {
      CompiledComparison cv[] = null;
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      /**
       * ******************For all LESS THAN OR LESS THAN EQUAL To ********************
       */
      // Case 1 : a = 7 and a !=4 and a != 5 and a != 8
      cv = new CompiledComparison[4];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_EQ);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_NE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_NE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_NE);

      OrganizedOperands oo = this
          .oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv, context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      OrganizedOperands oo1 = rj.organizeOperands(context);
      assertEquals(oo1.filterOperand, cv[0]);
      // Case 2 : a > 7 and a !=4 and a != 5 and a = 8
      cv = new CompiledComparison[4];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_NE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_NE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_EQ);

      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertEquals(oo1.filterOperand, cv[3]);

      // Case3 : a < 7 and a !=4 and a =8 and a != 5
      cv = new CompiledComparison[4];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_LT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_NE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_EQ);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_NE);

      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand instanceof CompiledLiteral);
      assertFalse(
          ((Boolean) ((CompiledLiteral) oo1.filterOperand).evaluate(context)).booleanValue());

      // Case4 : a > 7 and a !=4 and a !=8 and a = 14
      cv = new CompiledComparison[4];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_NE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_NE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(14)), OQLLexerTokenTypes.TOK_EQ);

      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[3]);

      // Case5 : a <= 14 and a !=4 and a !=8 and a = 14
      cv = new CompiledComparison[4];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(14)), OQLLexerTokenTypes.TOK_LE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_NE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_NE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(14)), OQLLexerTokenTypes.TOK_EQ);

      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[3]);

      // Case6 : a >= 14 and a !=4 and a !=8 and a = 14

      cv = new CompiledComparison[4];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(14)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_NE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_NE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(14)), OQLLexerTokenTypes.TOK_EQ);

      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[3]);

      // Case7 : a >= 14 and a !=4 and a =9 and a = 14

      cv = new CompiledComparison[4];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(14)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_NE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(9)), OQLLexerTokenTypes.TOK_EQ);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(14)), OQLLexerTokenTypes.TOK_EQ);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand instanceof CompiledLiteral);
      assertFalse(
          ((Boolean) ((CompiledLiteral) oo1.filterOperand).evaluate(context)).booleanValue());

      // Case8 : a > 7 and a !=4 and a !=8 and a = 14 and a <18
      cv = new CompiledComparison[5];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_NE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_NE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(14)), OQLLexerTokenTypes.TOK_EQ);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(18)), OQLLexerTokenTypes.TOK_LT);

      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand == cv[3]);

      // case9:a > 7 and a !=4 and a !=8 and a = 14 and a <14
      cv = new CompiledComparison[5];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(14)), OQLLexerTokenTypes.TOK_GT);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(4)), OQLLexerTokenTypes.TOK_NE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(9)), OQLLexerTokenTypes.TOK_EQ);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(14)), OQLLexerTokenTypes.TOK_EQ);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(14)), OQLLexerTokenTypes.TOK_LT);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand instanceof CompiledLiteral);
      assertFalse(
          ((Boolean) ((CompiledLiteral) oo1.filterOperand).evaluate(context)).booleanValue());

    } catch (Exception e) {
      logger.error(e.toString());
    }
  }

  @Test
  public void testOrganizeOpsOfRangeJunctionForNonRangeEvaluatableOperand() {

    LogWriter logger = CacheUtils.getLogger();
    try {
      CompiledValue cv[] = null;
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      // Case 1 : a != null and a != null and a != undefined
      cv = new CompiledValue[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(null), OQLLexerTokenTypes.TOK_NE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(null), OQLLexerTokenTypes.TOK_NE);
      cv[2] = new CompiledUndefined(new CompiledPath(new CompiledID("p"), "ID"), false);
      OrganizedOperands oo = this
          .oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv, context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      OrganizedOperands oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand instanceof GroupJunction);
      CompiledValue[] ops = ((GroupJunction) oo1.filterOperand)._operands;
      assertTrue(ops[0] == cv[0] || ops[0] == cv[1] || ops[0] == cv[2]);
      assertTrue(ops[1] == cv[0] || ops[1] == cv[1] || ops[1] == cv[2]);
      assertTrue(ops[2] == cv[0] || ops[2] == cv[1] || ops[2] == cv[2]);
    } catch (Exception e) {
      logger.error(e);
      fail(e.toString());
    }

  }

  /**
   * Tests the genuine range condition evaluator ( having a upper and lower bound) formed from a
   * RangeJunction of type AND
   */
  @Test
  public void testDoubleCondnRangeJunctionEvaluator_AND() {
    LogWriter logger = CacheUtils.getLogger();
    try {
      CompiledComparison cv[] = null;
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      // Case 1 : a >= 7 and a <=10
      cv = new CompiledComparison[2];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(10)), OQLLexerTokenTypes.TOK_LE);
      OrganizedOperands oo = this
          .oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv, context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      OrganizedOperands oo1 = rj.organizeOperands(context);
      assertTrue(RangeJunction.isInstanceOfDoubleCondnRangeJunctionEvaluator((oo1.filterOperand)));
      assertTrue(RangeJunction.getDoubleCondnEvaluatorGreaterKey((oo1.filterOperand))
          .equals(new Integer(7)));
      assertTrue(RangeJunction.getDoubleCondnEvaluatorOperatorOfGreaterType(
          (oo1.filterOperand)) == OQLLexerTokenTypes.TOK_GE);
      assertTrue(RangeJunction.getDoubleCondnEvaluatorLESSKey((oo1.filterOperand))
          .equals(new Integer(10)));
      assertTrue(RangeJunction.getDoubleCondnEvaluatorOperatorOfLessType(
          (oo1.filterOperand)) == OQLLexerTokenTypes.TOK_LE);
      assertTrue(RangeJunction.getKeysToBeRemoved((oo1.filterOperand)) == null);

      // Case2 : a >= 7 and a <=10 a >=5 and a <=11 and a != 7
      cv = new CompiledComparison[5];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(10)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_GE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(11)), OQLLexerTokenTypes.TOK_LE);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(RangeJunction.isInstanceOfDoubleCondnRangeJunctionEvaluator((oo1.filterOperand)));
      assertTrue(RangeJunction.getDoubleCondnEvaluatorGreaterKey((oo1.filterOperand))
          .equals(new Integer(7)));
      assertTrue(RangeJunction.getDoubleCondnEvaluatorOperatorOfGreaterType(
          (oo1.filterOperand)) == OQLLexerTokenTypes.TOK_GE);
      assertTrue(RangeJunction.getDoubleCondnEvaluatorLESSKey((oo1.filterOperand))
          .equals(new Integer(10)));
      assertTrue(RangeJunction.getDoubleCondnEvaluatorOperatorOfLessType(
          (oo1.filterOperand)) == OQLLexerTokenTypes.TOK_LE);
      Set keysToRemove = RangeJunction.getKeysToBeRemoved((oo1.filterOperand));
      assertTrue(keysToRemove.size() == 1);
      assertTrue(keysToRemove.iterator().next().equals(new Integer(7)));

      // Case3 : a >= 7 and a <=6 and a >=8 and a <=5 and a != 7
      cv = new CompiledComparison[5];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(6)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_GE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_LE);
      cv[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand instanceof CompiledLiteral);
      assertFalse(
          ((Boolean) ((CompiledLiteral) oo1.filterOperand).evaluate(context)).booleanValue());

      // Case4 : a >= 7 and a <=6 and a >=8 and a <=5
      cv = new CompiledComparison[4];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(6)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_GE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_LE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand instanceof CompiledLiteral);
      assertFalse(
          ((Boolean) ((CompiledLiteral) oo1.filterOperand).evaluate(context)).booleanValue());

      // Case5 : a >= 1 and a <=6 and a !=8
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(1)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(6)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_NE);

      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(RangeJunction.isInstanceOfDoubleCondnRangeJunctionEvaluator((oo1.filterOperand)));
      assertTrue(RangeJunction.getDoubleCondnEvaluatorGreaterKey((oo1.filterOperand))
          .equals(new Integer(1)));
      assertTrue(RangeJunction.getDoubleCondnEvaluatorOperatorOfGreaterType(
          (oo1.filterOperand)) == OQLLexerTokenTypes.TOK_GE);
      assertTrue(
          RangeJunction.getDoubleCondnEvaluatorLESSKey((oo1.filterOperand)).equals(new Integer(6)));
      assertTrue(RangeJunction.getDoubleCondnEvaluatorOperatorOfLessType(
          (oo1.filterOperand)) == OQLLexerTokenTypes.TOK_LE);
      keysToRemove = RangeJunction.getKeysToBeRemoved((oo1.filterOperand));
      assertTrue(keysToRemove == null);

      // Case6 : a >= 1 and a <=6 and a !=8 and a!=2
      cv = new CompiledComparison[4];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(1)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(6)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_NE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(2)), OQLLexerTokenTypes.TOK_NE);

      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(RangeJunction.isInstanceOfDoubleCondnRangeJunctionEvaluator((oo1.filterOperand)));
      assertTrue(RangeJunction.getDoubleCondnEvaluatorGreaterKey((oo1.filterOperand))
          .equals(new Integer(1)));
      assertTrue(RangeJunction.getDoubleCondnEvaluatorOperatorOfGreaterType(
          (oo1.filterOperand)) == OQLLexerTokenTypes.TOK_GE);
      assertTrue(
          RangeJunction.getDoubleCondnEvaluatorLESSKey((oo1.filterOperand)).equals(new Integer(6)));
      assertTrue(RangeJunction.getDoubleCondnEvaluatorOperatorOfLessType(
          (oo1.filterOperand)) == OQLLexerTokenTypes.TOK_LE);
      keysToRemove = RangeJunction.getKeysToBeRemoved((oo1.filterOperand));
      assertTrue(keysToRemove.size() == 1);
      assertTrue(keysToRemove.iterator().next().equals(new Integer(2)));

      // Case7 : a >= 1 and a <=6 and a !=6 and a!=0
      cv = new CompiledComparison[4];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(1)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(6)), OQLLexerTokenTypes.TOK_LE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(6)), OQLLexerTokenTypes.TOK_NE);
      cv[3] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(0)), OQLLexerTokenTypes.TOK_NE);

      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(RangeJunction.isInstanceOfDoubleCondnRangeJunctionEvaluator((oo1.filterOperand)));
      assertTrue(RangeJunction.getDoubleCondnEvaluatorGreaterKey((oo1.filterOperand))
          .equals(new Integer(1)));
      assertTrue(RangeJunction.getDoubleCondnEvaluatorOperatorOfGreaterType(
          (oo1.filterOperand)) == OQLLexerTokenTypes.TOK_GE);
      assertTrue(
          RangeJunction.getDoubleCondnEvaluatorLESSKey((oo1.filterOperand)).equals(new Integer(6)));
      assertTrue(RangeJunction.getDoubleCondnEvaluatorOperatorOfLessType(
          (oo1.filterOperand)) == OQLLexerTokenTypes.TOK_LE);
      keysToRemove = RangeJunction.getKeysToBeRemoved((oo1.filterOperand));
      assertTrue(keysToRemove.size() == 1);
      assertTrue(keysToRemove.iterator().next().equals(new Integer(6)));

    } catch (Exception e) {
      logger.error(e);
      fail(e.toString());
    }
  }

  /**
   * Tests if the presence of UNDEFINED operand is treated as a separate eval operand & not made a
   * part of SingleCondn or DoubleCondn evaluator
   */
  @Test
  public void testNullorNotNullorUndefinedBehaviour() {
    LogWriter logger = CacheUtils.getLogger();
    try {
      CompiledComparison cv[] = null;
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
      this.bindIteratorsAndCreateIndex(context);
      // Case 1 : a >= 7 and a != null
      cv = new CompiledComparison[2];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(null), OQLLexerTokenTypes.TOK_EQ);
      OrganizedOperands oo = this
          .oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv, context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      RangeJunction rj = (RangeJunction) oo.filterOperand;
      OrganizedOperands oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand instanceof GroupJunction);
      CompiledValue[] operands = ((GroupJunction) oo1.filterOperand)._operands;
      assertEquals(2, operands.length);
      assertTrue(operands[0] == cv[0] || operands[0] == cv[1]);
      assertTrue(operands[1] == cv[0] || operands[1] == cv[1]);

      // Case 2 : a >= 7 and a != null and a!=5
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(null), OQLLexerTokenTypes.TOK_NE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(5)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand instanceof GroupJunction);
      operands = ((GroupJunction) oo1.filterOperand)._operands;
      assertEquals(2, operands.length);
      assertTrue(cv[1] == operands[0] || cv[1] == operands[1]);
      assertTrue(cv[0] == operands[0] || cv[0] == operands[1]);
      // Case 3 : a >= 7 and a != null and a!=7
      cv = new CompiledComparison[3];
      cv[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(null), OQLLexerTokenTypes.TOK_NE);
      cv[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand instanceof GroupJunction);
      operands = ((GroupJunction) oo1.filterOperand)._operands;
      assertEquals(2, operands.length);
      assertTrue(cv[1] == operands[0] || cv[1] == operands[1]);
      assertTrue(RangeJunction.isInstanceOfSingleCondnEvaluator(operands[0])
          || RangeJunction.isInstanceOfSingleCondnEvaluator(operands[1]));
      int x = -1;
      if (RangeJunction.isInstanceOfSingleCondnEvaluator(operands[0])) {
        x = 0;
      } else if (RangeJunction.isInstanceOfSingleCondnEvaluator(operands[1])) {
        x = 1;
      }

      Object key = RangeJunction.getSingleCondnEvaluatorKey(operands[x]);
      assertTrue(key.equals(new Integer(7)));
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorOperator(operands[x]) == OQLLexerTokenTypes.TOK_GE);
      assertTrue(RangeJunction.getKeysToBeRemoved(operands[x]).size() == 1);
      assertTrue(
          RangeJunction.getKeysToBeRemoved(operands[x]).iterator().next().equals(new Integer(7)));

      // Case 4 : a >= 7 and a == null and a!=7 and a = undefined
      CompiledValue[] cv1 = new CompiledValue[4];
      cv1[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv1[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(null), OQLLexerTokenTypes.TOK_EQ);
      cv1[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_NE);
      cv1[3] = new CompiledUndefined(new CompiledPath(new CompiledID("p"), "ID"), false);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv1,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand instanceof GroupJunction);
      assertEquals(((GroupJunction) oo1.filterOperand)._operands.length, 3);
      CompiledValue[] ops = ((GroupJunction) oo1.filterOperand)._operands;
      assertTrue(ops[0] == cv1[1] || ops[1] == cv1[1] || ops[2] == cv1[1]);
      assertTrue(ops[0] == cv1[3] || ops[1] == cv1[3] || ops[2] == cv1[3]);
      assertTrue(RangeJunction.isInstanceOfSingleCondnEvaluator(ops[0])
          || RangeJunction.isInstanceOfSingleCondnEvaluator(ops[1])
          || RangeJunction.isInstanceOfSingleCondnEvaluator(ops[2]));
      x = -1;
      if (RangeJunction.isInstanceOfSingleCondnEvaluator(ops[0])) {
        x = 0;
      } else if (RangeJunction.isInstanceOfSingleCondnEvaluator(ops[1])) {
        x = 1;
      } else if (RangeJunction.isInstanceOfSingleCondnEvaluator(ops[2])) {
        x = 2;
      }
      key = RangeJunction.getSingleCondnEvaluatorKey(ops[x]);
      assertTrue(key.equals(new Integer(7)));
      assertTrue(
          RangeJunction.getSingleCondnEvaluatorOperator(ops[x]) == OQLLexerTokenTypes.TOK_GE);
      assertTrue(RangeJunction.getKeysToBeRemoved(ops[x]).size() == 1);
      assertTrue(RangeJunction.getKeysToBeRemoved(ops[x]).iterator().next().equals(new Integer(7)));

      // Case 5 : a >= 7 and a == null and a == 6 and a = undefined and
      // createTime = 6
      cv1 = new CompiledValue[5];
      cv1[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv1[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(null), OQLLexerTokenTypes.TOK_EQ);
      cv1[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(6)), OQLLexerTokenTypes.TOK_EQ);
      cv1[3] = new CompiledUndefined(new CompiledPath(new CompiledID("p"), "ID"), false);
      cv1[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "createTime"),
          new CompiledLiteral(new Long(6)), OQLLexerTokenTypes.TOK_EQ);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv1,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand instanceof CompiledLiteral);
      assertFalse(
          ((Boolean) ((CompiledLiteral) oo1.filterOperand).evaluate(context)).booleanValue());
      assertNull(oo1.iterateOperand);

      // Case 6 : a >= 7 and a == null and a == 8 and a = undefined and
      // createTime = 6
      cv1 = new CompiledValue[5];
      cv1[0] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(7)), OQLLexerTokenTypes.TOK_GE);
      cv1[1] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(null), OQLLexerTokenTypes.TOK_EQ);
      cv1[2] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "ID"),
          new CompiledLiteral(new Integer(8)), OQLLexerTokenTypes.TOK_EQ);
      cv1[3] = new CompiledUndefined(new CompiledPath(new CompiledID("p"), "ID"), false);
      cv1[4] = new CompiledComparison(new CompiledPath(new CompiledID("p"), "createTime"),
          new CompiledLiteral(new Long(6)), OQLLexerTokenTypes.TOK_EQ);
      oo = this.oganizedOperandsSingleRangeJunctionCreation(OQLLexerTokenTypes.LITERAL_and, cv1,
          context);
      assertTrue("Filter Openad of OrganizedOperand is not of type RangeJunction",
          oo.filterOperand instanceof RangeJunction);
      rj = (RangeJunction) oo.filterOperand;
      oo1 = rj.organizeOperands(context);
      assertTrue(oo1.filterOperand instanceof GroupJunction);
      operands = ((GroupJunction) oo1.filterOperand)._operands;
      assertEquals(operands.length, 3);
      assertTrue(cv1[1] == operands[0] || cv1[1] == operands[1] || cv1[1] == operands[2]);
      assertTrue(cv1[2] == operands[0] || cv1[2] == operands[1] || cv1[2] == operands[2]);
      assertTrue(cv1[3] == operands[0] || cv1[3] == operands[1] || cv1[3] == operands[2]);
      assertTrue(oo1.iterateOperand == cv1[4]);

    } catch (Exception e) {
      logger.error(e.toString());
      fail(e.toString());
    }
  }

  private void bindIteratorsAndCreateIndex(ExecutionContext context) throws Exception {
    QCompiler compiler = new QCompiler();
    List list = compiler.compileFromClause("/portfolio p, p.positions");
    context.newScope(context.associateScopeID());
    qs.createIndex("idIndex", IndexType.FUNCTIONAL, "ID", "/portfolio");
    Iterator iter = list.iterator();
    while (iter.hasNext()) {
      CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
      context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
      RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
      context.bindIterator(rIter);
      context.addToIndependentRuntimeItrMap(iterDef);
    }
  }

  /**
   * Tests the creation of a single RangeJunction if the CompiledJunction only contains same index
   * condition with or without iter operand
   */
  private OrganizedOperands oganizedOperandsSingleRangeJunctionCreation(int junctionType,
      CompiledValue[] operandsForCJ, ExecutionContext context) {
    LogWriter logger = CacheUtils.getCache().getLogger();
    OrganizedOperands oo = null;
    try {
      CompiledJunction cj = new CompiledJunction(operandsForCJ, junctionType);
      context.addDependencies(new CompiledID("dummy"), cj.computeDependencies(context));
      cj.getPlanInfo(context);
      oo = cj.testOrganizedOperands(context);
      return oo;
    } catch (Exception e) {
      logger.error(e);
      fail(e.toString());
    }
    return oo;
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
    IndexManager indexManager = ((LocalRegion) region).getIndexManager();
    if (indexManager != null) {
      indexManager.destroy();
    }
  }
}
