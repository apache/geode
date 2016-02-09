/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Created on Oct 13, 2005
 *
 * 
 */
package com.gemstone.gemfire.cache.query.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * @author Asif
 * 
 *  
 */
@Category(IntegrationTest.class)
public class ExecutionContextJUnitTest {
  boolean failure = false;
  String exceptionStr ="";

  @Before
  public void setUp() throws Exception {
    this.failure = false;
    CacheUtils.startCache();
  }

  @After
  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  @Test
  public void testFunctionaAddToIndependentRuntimeItrMapWithoutIndex() {
    CacheUtils.createRegion("portfolio", Portfolio.class);
    // compileFromClause returns a List<CompiledIteratorDef>
    QCompiler compiler = new QCompiler();
    List list = compiler.compileFromClause("/portfolio p, p.positions");
    ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
    context.newScope(context.assosciateScopeID());
    try {
      Iterator iter = list.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        context.addDependencies(new CompiledID("dummy"), iterDef
            .computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.bindIterator(rIter);
        context.addToIndependentRuntimeItrMap(iterDef);
        assertTrue(
            " The index_interanal_id is not set as per expectation of iter'n'",
            rIter.getIndexInternalID().equals(rIter.getInternalId()));
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to Exception = " + e);
    }
  }

  @Test
  public void testFunctionaAddToIndependentRuntimeItrMapWithIndex() {
    try {
      CacheUtils.createRegion("portfolio", Portfolio.class);
      DefaultQueryService qs = new DefaultQueryService((InternalCache) CacheUtils.getCache());
      qs.createIndex("myindex", IndexType.FUNCTIONAL, "pf.id",
          "/portfolio pf, pf.positions pos");
      // compileFromClause returns a List<CompiledIteratorDef>
      QCompiler compiler = new QCompiler();
      List list = compiler.compileFromClause("/portfolio p, p.positions");
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils
          .getCache());
      context.newScope(context.assosciateScopeID());
      Iterator iter = list.iterator();
      int i = 0;
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        ++i;
        context.addDependencies(new CompiledID("dummy"), iterDef
            .computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.addToIndependentRuntimeItrMap(iterDef);
        context.bindIterator(rIter);
        assertTrue(
            " The index_interanal_id is not set as per expectation of index_iter'n'",
            rIter.getIndexInternalID().equals("index_iter" + i));
      }
    }
    catch (Exception e) {
      fail("Test failed sue to Exception = " + e);
    }
  }

  @Test
  public void testObtainingRegionPath() {
    try {
      CacheUtils.createRegion("portfolio", Portfolio.class);
      DefaultQueryService qs = new DefaultQueryService((InternalCache) CacheUtils.getCache());
      qs.createIndex("myindex", IndexType.FUNCTIONAL, "pf.id",
          "/portfolio pf, pf.positions pos");
      // compileFromClause returns a List<CompiledIteratorDef>
      QCompiler compiler = new QCompiler();
      List list = compiler.compileFromClause("/portfolio p, p.positions");
      ExecutionContext context = new QueryExecutionContext(null, CacheUtils
          .getCache());
      context.newScope(context.assosciateScopeID());
      Iterator iter = list.iterator();
      int i = 0;
      CompiledIteratorDef iterDef = null;
      while (iter.hasNext()) {
        iterDef = (CompiledIteratorDef) iter.next();
        ++i;
        context.addDependencies(new CompiledID("dummy"), iterDef
            .computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        context.addToIndependentRuntimeItrMap(iterDef);
        context.bindIterator(rIter);
        assertTrue(
            " The index_interanal_id is not set as per expectation of index_iter'n'",
            rIter.getIndexInternalID().equals("index_iter" + i));
      }
      Set temp = new HashSet();
      context.computeUtlimateDependencies(iterDef, temp);
      String regionPath = context
          .getRegionPathForIndependentRuntimeIterator((RuntimeIterator) temp
              .iterator().next());
      if (!(regionPath != null && regionPath.equals("/portfolio"))) {
        fail(" Region path is either null or not equal to /portfolio. The regionpath obtained = "
            + regionPath);
      }
      System.out
          .println(" ***********The Region Path obatined = " + regionPath);
    }
    catch (Exception e) {
      fail("Test failed sue to Exception = " + e);
    }
  }

  @Test
  public void testCurrScopeDpndntItrsBasedOnSingleIndpndntItr() {
    CacheUtils.createRegion("portfolio", Portfolio.class);
    CacheUtils.createRegion("dummy", null);
    // compileFromClause returns a List<CompiledIteratorDef>
    QCompiler compiler = new QCompiler();
    List list = compiler
        .compileFromClause("/portfolio p, p.positions, p.addreses addrs, addrs.collection1 coll1, /dummy d1, d1.collection2 d2");
    RuntimeIterator indItr = null;
    ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
    context.newScope(context.assosciateScopeID());
    int i = 0;
    List checkList = new ArrayList();
    try {
      Iterator iter = list.iterator();
      while (iter.hasNext()) {
        CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
        context.addDependencies(new CompiledID("test"), iterDef
            .computeDependencies(context));
        RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
        if (i == 0) {
          indItr = rIter;
          checkList.add(rIter);
        }
        else {
          if (i < 4) {
            checkList.add(rIter);
          }
        }
        ++i;
        context.bindIterator(rIter);
        context.addToIndependentRuntimeItrMap(iterDef);
        assertTrue(
            " The index_interanal_id is not set as per expectation of iter'n'",
            rIter.getIndexInternalID().equals(rIter.getInternalId()));
      }
      List list1 = context
          .getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(indItr);
      if (list1.size() != 4) {
        fail("The dependency set returned incorrect resul with size ="
            + list1.size());
      }
      for (int j = 0; j < 4; ++j) {
        assertEquals(list1.get(j), checkList.get(j));
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to Exception = " + e);
    }
  }
  
  @Test
  public void testScopeIndex1() {
    CacheUtils.createRegion("portfolios", Portfolio.class);
    CacheUtils.createRegion("positions", Position.class);
    // compileFromClause returns a List<CompiledIteratorDef>
    String qry = "select distinct p.pf, ELEMENT(select distinct pf1 from /portfolios pf1 where pf1.getID = p.pf.getID )  from (select distinct pf, pos from /portfolios pf, pf.positions.values pos) p, (select distinct * from /positions rtPos where rtPos.secId = p.pos.secId) as y " +
                "where ( select distinct pf2 from /portfolios pf2 ).size() <> 0 ";
    ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
    QCompiler compiler = new QCompiler();
    CompiledValue query =compiler.compileQuery(qry);
    helperComputeDependencyPhase(context,query);   
    Set runtimeItrs = context.getDependencySet(query, true);
    Iterator itr = runtimeItrs.iterator();
    while(itr.hasNext()) {
      RuntimeIterator rItr = (RuntimeIterator)itr.next();
      if(rItr.getName().equals("p")) {
        assertTrue( "The scopeID of outer iterator is not 1",rItr.getScopeID() ==1 );
      }else if(rItr.getName().equals("pf")) {
        assertTrue( "The scopeID of first inner  level iterator is not 2",rItr.getScopeID() ==2 );
      }else if(rItr.getName().equals("pos")) {
        assertTrue( "The scopeID of first inner  level iterator is not 2",rItr.getScopeID() ==2 );
      }else if(rItr.getName().equals("rtPos")) {
        assertTrue( "The scopeID of second inner level iterator is not 3",rItr.getScopeID() ==3 );
      }else if(rItr.getName().equals("y")) {
        assertTrue( "The scopeID of outer level iterator is not 1",rItr.getScopeID() ==1 );
      }else if(rItr.getName().equals("pf1")) {
        assertTrue( "The scopeID of inner level iterator is not 5",rItr.getScopeID() ==5 );
      }else if(rItr.getName().equals("pf2")) {
        assertTrue( "The scopeID of inner level iterator is not 4",rItr.getScopeID() ==4 );
      }else {
        fail ("No such iterator with name = "+ rItr.getName() + "should be available");
            
      }
    }
    helperEvaluateQuery(context, query);
    runtimeItrs = context.getDependencySet(query, true);
    itr = runtimeItrs.iterator();
    while(itr.hasNext()) {
      RuntimeIterator rItr = (RuntimeIterator)itr.next();
      if(rItr.getName().equals("p")) {
        assertTrue( "The scopeID of outer iterator is not 1",rItr.getScopeID() ==1 );
      }else if(rItr.getName().equals("pf")) {
        assertTrue( "The scopeID of first inner  level iterator is not 2",rItr.getScopeID() ==2 );
      }else if(rItr.getName().equals("pos")) {
        assertTrue( "The scopeID of first inner  level iterator is not 2",rItr.getScopeID() ==2 );
      }else if(rItr.getName().equals("rtPos")) {
        assertTrue( "The scopeID of second inner level iterator is not 3",rItr.getScopeID() ==3 );
      }else if(rItr.getName().equals("y")) {
        assertTrue( "The scopeID of outer level iterator is not 1",rItr.getScopeID() ==1 );
      }else if(rItr.getName().equals("pf1")) {
        assertTrue( "The scopeID of inner level iterator is not 5",rItr.getScopeID() ==5 );
      }else if(rItr.getName().equals("pf2")) {
        assertTrue( "The scopeID of inner level iterator is not 4",rItr.getScopeID() ==4 );
      }else {
        fail ("No such iterator with name = "+ rItr.getName() + "should be available");
            
      }
    }  
    
  } 
  
  @Test
  public void testMultiThreadedScopeIndex() {
    CacheUtils.createRegion("portfolios", Portfolio.class);
    CacheUtils.createRegion("positions", Position.class);
    // compileFromClause returns a List<CompiledIteratorDef>
    String qry = "select distinct p.pf from (select distinct pf, pos from /portfolios pf, pf.positions.values pos) p, (select distinct * from /positions rtPos where rtPos.secId = p.pos.secId) as y";
    final int TOATL_THREADS  = 80;
    final CountDownLatch latch = new CountDownLatch(TOATL_THREADS);
    
    QCompiler compiler = new QCompiler();
    final CompiledValue query =compiler.compileQuery(qry);
    Runnable runnable = new Runnable() {
      public void run() {
        try {
          latch.countDown();
          latch.await();
        ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
        helperComputeDependencyPhase(context,query);   
        Set runtimeItrs = context.getDependencySet(query, true);
        Iterator itr = runtimeItrs.iterator();
        while(itr.hasNext()) {
          RuntimeIterator rItr = (RuntimeIterator)itr.next();
          if(rItr.getName().equals("p")) {
            assertTrue( "The scopeID of outer iterator is not 1",rItr.getScopeID() ==1 );
          }else if(rItr.getName().equals("pf")) {
            assertTrue( "The scopeID of first inner  level iterator is not 2",rItr.getScopeID() ==2 );
          }else if(rItr.getName().equals("pos")) {
            assertTrue( "The scopeID of first inner  level iterator is not 2",rItr.getScopeID() ==2 );
          }else if(rItr.getName().equals("rtPos")) {
            assertTrue( "The scopeID of second inner level iterator is not 3",rItr.getScopeID() ==3 );
          }else if(rItr.getName().equals("y")) {
            assertTrue( "The scopeID of outer level iterator is not 1",rItr.getScopeID() ==1 );
          }else {
            fail ("No such iterator with name = "+ rItr.getName() + "should be available");
                
          }
          Thread.yield();
        }
        helperEvaluateQuery(context, query);
        runtimeItrs = context.getDependencySet(query, true);
        itr = runtimeItrs.iterator();
        while(itr.hasNext()) {
          RuntimeIterator rItr = (RuntimeIterator)itr.next();
          if(rItr.getName().equals("p")) {
            assertTrue( "The scopeID of outer iterator is not 1",rItr.getScopeID() ==1 );
          }else if(rItr.getName().equals("pf")) {
            assertTrue( "The scopeID of first inner  level iterator is not 2",rItr.getScopeID() ==2 );
          }else if(rItr.getName().equals("pos")) {
            assertTrue( "The scopeID of first inner  level iterator is not 2",rItr.getScopeID() ==2 );
          }else if(rItr.getName().equals("rtPos")) {
            assertTrue( "The scopeID of second inner level iterator is not 3",rItr.getScopeID() ==3 );
          }else if(rItr.getName().equals("y")) {
            assertTrue( "The scopeID of outer level iterator is not 1",rItr.getScopeID() ==1 );
          }else {
            fail ("No such iterator with name = "+ rItr.getName() + "should be available");
                
          }
        }    
        }
        catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        }
        catch(Throwable th) {
          exceptionStr = th.toString();
          failure = true;
        }
      }
    };
    
    Thread th[] = new Thread[TOATL_THREADS];
    for (int i =0; i < th.length ;++i) {
      th[i] = new Thread( runnable);
    }
    
    for (int i =0; i < th.length ;++i) {
      th[i].start();
    }
    
    for (int i =0; i < th.length ;++i) {
      try {
        ThreadUtils.join(th[i], 30 * 1000);
      }catch(Exception e) {
        fail(e.toString());
      }
    }
    if(failure) {
      fail(exceptionStr);
    }
  }
  
  protected void helperComputeDependencyPhase(ExecutionContext context, CompiledValue query) {   
    try {
      query.computeDependencies(context);
      
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to Exception = " + e);
    }
  }
  
  protected SelectResults helperEvaluateQuery(ExecutionContext context, CompiledValue query) {
    SelectResults rs = null; 
    try {
      rs= (SelectResults)query.evaluate(context);
      
      
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("Test failed due to Exception = " + e);
    }
    return rs;
  }
}
