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
 * IndexTest.java JUnit based test
 *
 * Created on March 9, 2005, 3:30 PM
 */
package org.apache.geode.cache.query.internal;

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.CacheUtils;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.data.Portfolio;
import org.apache.geode.test.junit.categories.OQLQueryTest;

@Category({OQLQueryTest.class})
public class QueryFromClauseCanonicalizationJUnitTest {
  Region region = null;

  QueryService qs = null;

  static String[] queries = {
      "SELECT DISTINCT ID, value.secId FROM " + SEPARATOR
          + "pos, getPositions where status = 'active' and ID = 0",
      "SELECT DISTINCT ID, value.secId FROM " + SEPARATOR
          + "pos, positions where status = 'active' and ID = 0",
      "SELECT DISTINCT ID, value.secId FROM " + SEPARATOR
          + "pos, getPositions() where status = 'active' and ID = 0",
      "SELECT DISTINCT ID, p.value.secId FROM " + SEPARATOR
          + "pos, getPositions('true') p where status = 'active' and ID = 0",
      "SELECT DISTINCT * FROM " + SEPARATOR
          + "pos as a, a.collectionHolderMap['0'].arr as b where a.status = 'active' and a.ID = 0",
      /*
       * "SELECT DISTINCT * FROM /pos as a, a.positions[a.collectionHolderMap['0'][1]] as b where a.status = 'active' and a.ID = 0"
       * ,
       */

  };

  @Before
  public void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    region = CacheUtils.createRegion("pos", Portfolio.class);
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));

    qs = CacheUtils.getQueryService();
  }

  @After
  public void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testCanonicalizedFromClause() throws Throwable {

    boolean overallTestFailed = false;
    Query q = null;
    QueryObserverImpl observer = null;
    for (int j = 0; j < queries.length; j++) {

      try {
        q = qs.newQuery(queries[j]);
        observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        q.execute();
      } catch (Exception e) {
        System.err.println(
            "QueryFromClauseCanonicalizationJUnitTest::testCanonicalizedFromClause.Exception in running query number="
                + j + "  Exception=" + e);
        e.printStackTrace();
        overallTestFailed = true;
        continue;
      }

      switch (j) {
        case 0:
        case 1:
        case 2:
          if (observer.clauses.get(0).toString().equals(SEPARATOR + "pos")
              && observer.clauses.get(1).toString().equals("iter1.positions")) {
            assertTrue(true);
          } else {
            overallTestFailed = true;
            System.err.println(
                "QueryFromClauseCanonicalizationJUnitTest::testCanonicalizedFromClause.Failure in query number="
                    + j);
          }
          break;
        case 3:
          if (observer.clauses.get(0).toString().equals(SEPARATOR + "pos")
              && observer.clauses.get(1).toString().equals("iter1.getPositions('true')")) {
            assertTrue(true);
          } else {
            overallTestFailed = true;
            System.err.println(
                "QueryFromClauseCanonicalizationJUnitTest::testCanonicalizedFromClause.Failure in query number="
                    + j);
          }
          break;
        case 5:
          if (observer.clauses.get(0).toString().equals(SEPARATOR + "pos")
              && observer.clauses.get(1)
                  .toString().equals("iter1.positions[iter1.collectionHolderMap[][]]")) {
            assertTrue(true);
          } else {
            overallTestFailed = true;
            System.err.println(
                "QueryFromClauseCanonicalizationJUnitTest::testCanonicalizedFromClause.Failure in query number="
                    + j);
          }
          break;
        case 4:
          if (observer.clauses.get(0).toString().equals(SEPARATOR + "pos")
              && observer.clauses.get(1).toString().equals("iter1.collectionHolderMap['0'].arr")) {
            assertTrue(true);
          } else {
            overallTestFailed = true;
            System.err.println(
                "QueryFromClauseCanonicalizationJUnitTest::testCanonicalizedFromClause.Failure in query number="
                    + j);
          }
          break;

      }

    }
    if (overallTestFailed) {
      Assert.fail();
    }

  }

  @Test
  public void testCanonicalizationOfMethod() throws Exception {
    QCompiler compiler = new QCompiler();
    List list = compiler.compileFromClause(SEPARATOR + "pos pf");
    ExecutionContext context =
        new ExecutionContext(new Object[] {"bindkey"}, CacheUtils.getCache());
    context.newScope(context.associateScopeID());

    Iterator iter = list.iterator();
    while (iter.hasNext()) {
      CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
      context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
      RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
      context.bindIterator(rIter);
      context.addToIndependentRuntimeItrMap(iterDef);
    }
    CompiledPath cp = new CompiledPath(new CompiledID("pf"), "positions");
    CompiledLiteral cl = new CompiledLiteral("key1");
    List args = new ArrayList();
    args.add(cl);
    CompiledOperation cop = new CompiledOperation(cp, "get", args);
    StringBuilder sbuff = new StringBuilder();
    cop.generateCanonicalizedExpression(sbuff, context);
    assertEquals(sbuff.toString(), "iter1.positions.get('key1')");

    // cp = new CompiledPath(new CompiledID("pf"), "positions");
    // CompiledBindArgument cb = new CompiledBindArgument(1);
    // args = new ArrayList();
    // args.add(cb);
    // cop = new CompiledOperation(cp, "get", args);
    //// context.setBindArguments(new Object[]{"bindkey"});
    // sbuff = new StringBuilder();
    // cop.generateCanonicalizedExpression(sbuff, context);
    // assertIndexDetailsEquals(sbuff.toString(),"iter1.positions.get('bindkey')");
    //

    // cp = new CompiledPath(new CompiledID("pf"), "getPositions()");
    // cb = new CompiledBindArgument(1);
    // args = new ArrayList();
    // args.add(cb);
    // cop = new CompiledOperation(cp, "get", args);
    // sbuff = new StringBuilder();
    // cop.generateCanonicalizedExpression(sbuff, context);
    // assertIndexDetailsEquals(sbuff.toString(),"iter1.positions().get('bindkey')");
    //
    //
    // cp = new CompiledPath(new CompiledID("pf"), "getPositions");
    // cb = new CompiledBindArgument(1);
    // args = new ArrayList();
    // args.add(cb);
    // cop = new CompiledOperation(cp, "get", args);
    // sbuff = new StringBuilder();
    // cop.generateCanonicalizedExpression(sbuff, context);
    // assertIndexDetailsEquals(sbuff.toString(),"iter1.positions.get('bindkey')");

    cp = new CompiledPath(new CompiledID("pf"), "getPositions");
    CompiledPath cp1 = new CompiledPath(new CompiledID("pf"), "pkid");
    args = new ArrayList();
    args.add(cp1);
    cop = new CompiledOperation(cp, "get", args);
    sbuff = new StringBuilder();
    cop.generateCanonicalizedExpression(sbuff, context);
    assertEquals(sbuff.toString(), "iter1.positions.get(iter1.pkid)");


    cp = new CompiledPath(new CompiledID("pf"), "getPositions");
    cp1 = new CompiledPath(new CompiledID("pf"), "pkid");
    CompiledIndexOperation ciop = new CompiledIndexOperation(cp, cp1);
    sbuff = new StringBuilder();
    ciop.generateCanonicalizedExpression(sbuff, context);
    assertEquals(sbuff.toString(), "iter1.positions[iter1.pkid]");
  }


  class QueryObserverImpl extends QueryObserverAdapter {
    public List clauses = new ArrayList();

    @Override
    public void beforeIterationEvaluation(CompiledValue executer, Object currentObject) {
      clauses.add(((RuntimeIterator) executer).getDefinition());

    }
  }
}
