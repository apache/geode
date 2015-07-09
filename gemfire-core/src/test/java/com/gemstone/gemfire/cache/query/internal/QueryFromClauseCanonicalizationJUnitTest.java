/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * IndexTest.java JUnit based test
 * 
 * Created on March 9, 2005, 3:30 PM
 */
package com.gemstone.gemfire.cache.query.internal;

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

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * 
 * @author Asif
 */
@Category(IntegrationTest.class)
public class QueryFromClauseCanonicalizationJUnitTest
{

  /*
   * ========================================================================
   * Copyright (C) GemStone Systems, Inc. 2000-2004. All Rights Reserved.
   * 
   * ========================================================================
   */
  Region region = null;

  QueryService qs = null;

  static String queries[] = {
      "SELECT DISTINCT ID, value.secId FROM /pos, getPositions where status = 'active' and ID = 0",
      "SELECT DISTINCT ID, value.secId FROM /pos, positions where status = 'active' and ID = 0",
      "SELECT DISTINCT ID, value.secId FROM /pos, getPositions() where status = 'active' and ID = 0",
      "SELECT DISTINCT ID, p.value.secId FROM /pos, getPositions('true') p where status = 'active' and ID = 0",
      "SELECT DISTINCT * FROM /pos as a, a.collectionHolderMap['0'].arr as b where a.status = 'active' and a.ID = 0",
     /* "SELECT DISTINCT * FROM /pos as a, a.positions[a.collectionHolderMap['0'][1]] as b where a.status = 'active' and a.ID = 0",*/

  };

  @Before
  public void setUp() throws java.lang.Exception
  {
    CacheUtils.startCache();
    region = CacheUtils.createRegion("pos", Portfolio.class);
    region.put("0", new Portfolio(0));
    region.put("1", new Portfolio(1));
    region.put("2", new Portfolio(2));
    region.put("3", new Portfolio(3));

    qs = CacheUtils.getQueryService();
  }

  @After
  public void tearDown() throws java.lang.Exception
  {
    CacheUtils.closeCache();
  }

  @Test
  public void testCanonicalizedFromClause() throws Throwable
  {  

    boolean overallTestFailed = false;
    Query q = null;
    QueryObserverImpl observer = null;
    for (int j = 0; j < queries.length; j++) {

      try {
        q = qs.newQuery(queries[j]);
        observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        q.execute();
      }
      catch (Exception e) {
        System.err
            .println("QueryFromClauseCanonicalizationJUnitTest::testCanonicalizedFromClause.Exception in running query number="
                + j + "  Exception=" + e);
        e.printStackTrace();
        overallTestFailed = true;
        continue;
      }

      switch (j) {
      case 0:
      case 1:
      case 2:
        if (observer.clauses.get(0).toString().equals("/pos")
            && observer.clauses.get(1).toString().equals("iter1.positions")) {
          assertTrue(true);
        }
        else {
          overallTestFailed = true;
          System.err
              .println("QueryFromClauseCanonicalizationJUnitTest::testCanonicalizedFromClause.Failure in query number="
                  + j);
        }
        break;
      case 3:
        if (observer.clauses.get(0).toString().equals("/pos")
            && observer.clauses.get(1).toString().equals(
                "iter1.getPositions('true')")) {
          assertTrue(true);
        }
        else {
          overallTestFailed = true;
          System.err
              .println("QueryFromClauseCanonicalizationJUnitTest::testCanonicalizedFromClause.Failure in query number="
                  + j);
        }
        break;
      case 5:
        if (observer.clauses.get(0).toString().equals("/pos")
            && observer.clauses.get(1).toString().equals(
                "iter1.positions[iter1.collectionHolderMap[][]]")) {
          assertTrue(true);
        }
        else {
          overallTestFailed = true;
          System.err
              .println("QueryFromClauseCanonicalizationJUnitTest::testCanonicalizedFromClause.Failure in query number="
                  + j);
        }
        break;
      case 4:
        if (observer.clauses.get(0).toString().equals("/pos")
            && observer.clauses.get(1).toString().equals(
                "iter1.collectionHolderMap['0'].arr")) {
          assertTrue(true);
        }
        else {
          overallTestFailed = true;
          System.err
              .println("QueryFromClauseCanonicalizationJUnitTest::testCanonicalizedFromClause.Failure in query number="
                  + j);
        }
        break;

      }

    }
    if (overallTestFailed)
      Assert.fail();

  }

  @Test
  public void testCanonicalizationOfMethod() throws Exception
  {
    QCompiler compiler = new QCompiler();
    List list = compiler.compileFromClause("/pos pf");
    ExecutionContext context = new ExecutionContext(new Object[]{"bindkey"}, CacheUtils.getCache());
    context.newScope(context.assosciateScopeID());

    Iterator iter = list.iterator();
    while (iter.hasNext()) {
      CompiledIteratorDef iterDef = (CompiledIteratorDef)iter.next();
      context.addDependencies(new CompiledID("dummy"), iterDef
          .computeDependencies(context));
      RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
      context.bindIterator(rIter);
      context.addToIndependentRuntimeItrMap(iterDef);
    }
    CompiledPath cp = new CompiledPath(new CompiledID("pf"), "positions");
    CompiledLiteral cl = new CompiledLiteral("key1");
    List args = new ArrayList();
    args.add(cl);
    CompiledOperation cop = new CompiledOperation(cp, "get", args);
    StringBuffer sbuff = new StringBuffer();
    cop.generateCanonicalizedExpression(sbuff, context);
    assertEquals(sbuff.toString(),"iter1.positions.get('key1')");
    
//    cp = new CompiledPath(new CompiledID("pf"), "positions");
//    CompiledBindArgument cb = new CompiledBindArgument(1);
//    args = new ArrayList();
//    args.add(cb);
//    cop = new CompiledOperation(cp, "get", args);
////    context.setBindArguments(new Object[]{"bindkey"});
//    sbuff = new StringBuffer();
//    cop.generateCanonicalizedExpression(sbuff, context);
//    assertEquals(sbuff.toString(),"iter1.positions.get('bindkey')");
//    
    
//    cp = new CompiledPath(new CompiledID("pf"), "getPositions()");
//    cb = new CompiledBindArgument(1);
//    args = new ArrayList();
//    args.add(cb);
//    cop = new CompiledOperation(cp, "get", args);
//    sbuff = new StringBuffer();
//    cop.generateCanonicalizedExpression(sbuff, context);
//    assertEquals(sbuff.toString(),"iter1.positions().get('bindkey')");
//    
//    
//    cp = new CompiledPath(new CompiledID("pf"), "getPositions");
//    cb = new CompiledBindArgument(1);
//    args = new ArrayList();
//    args.add(cb);
//    cop = new CompiledOperation(cp, "get", args);
//    sbuff = new StringBuffer();
//    cop.generateCanonicalizedExpression(sbuff, context);
//    assertEquals(sbuff.toString(),"iter1.positions.get('bindkey')");
    
    cp = new CompiledPath(new CompiledID("pf"), "getPositions");
    CompiledPath cp1 = new CompiledPath(new CompiledID("pf"),"pkid");
    args = new ArrayList();
    args.add(cp1);
    cop = new CompiledOperation(cp, "get", args);
    sbuff = new StringBuffer();
    cop.generateCanonicalizedExpression(sbuff, context);
    assertEquals(sbuff.toString(),"iter1.positions.get(iter1.pkid)");
    
    
    cp = new CompiledPath(new CompiledID("pf"), "getPositions");
    cp1 = new CompiledPath(new CompiledID("pf"),"pkid");    
    CompiledIndexOperation ciop = new CompiledIndexOperation(cp,cp1);
    sbuff = new StringBuffer();
    ciop.generateCanonicalizedExpression(sbuff, context);
    assertEquals(sbuff.toString(),"iter1.positions[iter1.pkid]");
  } 
  
  
  class QueryObserverImpl extends QueryObserverAdapter
  {
    public List clauses = new ArrayList();

    public void beforeIterationEvaluation(CompiledValue executer,
        Object currentObject)
    {
      clauses.add(((RuntimeIterator)executer).getDefinition());

    }
  }
}