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
 * Created on Oct 13, 2005
 *
 *
 */
package org.apache.geode.cache.query.internal;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.NameResolutionException;
import org.apache.geode.cache.query.TypeMismatchException;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category(OQLQueryTest.class)
public class ExecutionContextIntegrationTest {

  @Rule
  public ServerStarterRule server = new ServerStarterRule()
      .withRegion(RegionShortcut.REPLICATE, "portfolio")
      .withAutoStart();

  private void assertIteratorScope(Iterator itr) {
    while (itr.hasNext()) {
      RuntimeIterator rItr = (RuntimeIterator) itr.next();
      switch (rItr.getName()) {
        case "p":
          assertThat(rItr.getScopeID())
              .as("The scopeID of outer iterator is not 1").isEqualTo(1);
          break;
        case "pf":
        case "pos":
          assertThat(rItr.getScopeID())
              .as("The scopeID of first inner level iterator is not 2").isEqualTo(2);
          break;
        case "rtPos":
          assertThat(rItr.getScopeID())
              .as("The scopeID of second inner level iterator is not 3").isEqualTo(3);
          break;
        case "y":
          assertThat(rItr.getScopeID())
              .as("The scopeID of outer level iterator is not 1").isEqualTo(1);
          break;
        case "pf1":
          assertThat(rItr.getScopeID())
              .as("The scopeID of inner level iterator is not 5").isEqualTo(5);
          break;
        case "pf2":
          assertThat(rItr.getScopeID())
              .as("The scopeID of inner level iterator is not 4").isEqualTo(4);
          break;
        default:
          throw new RuntimeException(
              "No such iterator with name = " + rItr.getName() + " should be available");
      }
    }
  }

  private void assertIteratorScopeMultiThreaded(Iterator itr) {
    RuntimeIterator rItr = (RuntimeIterator) itr.next();
    switch (rItr.getName()) {
      case "p":
        assertThat(rItr.getScopeID())
            .as("The scopeID of outer iterator is not 1").isEqualTo(1);
        break;
      case "pf":
      case "pos":
        assertThat(rItr.getScopeID())
            .as("The scopeID of first inner level iterator is not 2").isEqualTo(2);
        break;
      case "rtPos":
        assertThat(rItr.getScopeID())
            .as("The scopeID of second inner level iterator is not 3").isEqualTo(3);
        break;
      case "y":
        assertThat(rItr.getScopeID())
            .as("The scopeID of outer level iterator is not 1").isEqualTo(1);
        break;
      default:
        throw new RuntimeException(
            "No such iterator with name = " + rItr.getName() + " should be available");
    }
  }

  private int computeEvaluateAndAssertIterator(ExecutionContext context, int i,
      CompiledIteratorDef iterDef) throws TypeMismatchException, NameResolutionException {
    ++i;
    @SuppressWarnings("unchecked")
    Set<RuntimeIterator> dependencies = iterDef.computeDependencies(context);
    context.addDependencies(new CompiledID("dummy"), dependencies);
    RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
    context.addToIndependentRuntimeItrMap(iterDef);
    context.bindIterator(rIter);

    assertThat(rIter.getIndexInternalID())
        .as("The index_internal_id is not set as per expectation of index_iter'n'")
        .isEqualTo("index_iter" + i);

    return i;
  }

  @Test
  public void constructorShouldUseConfiguredMethodAuthorizer() {
    ExecutionContext unsecuredContext = new QueryExecutionContext(null, server.getCache());
    MethodInvocationAuthorizer noOpAuthorizer = unsecuredContext.getMethodInvocationAuthorizer();

    // No security, no-op authorizer.
    assertThat(noOpAuthorizer).isNotNull();
    assertThat(noOpAuthorizer.getClass().getCanonicalName())
        .startsWith("org.apache.geode.cache.query.internal.DefaultQueryService$$Lambda$");

    server.stopMember();

    // Security Enabled -> RestrictedMethodAuthorizer
    server.withProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName())
        .withProperty("security-username", "cluster")
        .withProperty("security-password", "cluster")
        .startServer();
    ExecutionContext securedContext = new QueryExecutionContext(null, server.getCache());
    MethodInvocationAuthorizer authorizer = securedContext.getMethodInvocationAuthorizer();
    assertThat(authorizer).isNotNull();
    assertThat(authorizer).isInstanceOf(RestrictedMethodAuthorizer.class);
  }

  @Test
  public void testFunctionalAddToIndependentRuntimeItrMapWithoutIndex() throws Exception {
    QCompiler compiler = new QCompiler();
    List list = compiler.compileFromClause("/portfolio p, p.positions");
    ExecutionContext context = new QueryExecutionContext(null, server.getCache());
    context.newScope(context.associateScopeID());

    for (Object o : list) {
      CompiledIteratorDef iterDef = (CompiledIteratorDef) o;
      @SuppressWarnings("unchecked")
      Set<RuntimeIterator> dependencies = iterDef.computeDependencies(context);
      context.addDependencies(new CompiledID("dummy"), dependencies);
      RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
      context.bindIterator(rIter);
      context.addToIndependentRuntimeItrMap(iterDef);

      assertThat(rIter.getInternalId())
          .as(" The index_internal_id is not set as per expectation of iter'n'")
          .isEqualTo(rIter.getIndexInternalID());
    }
  }

  @Test
  public void testFunctionalAddToIndependentRuntimeItrMapWithIndex() throws Exception {
    DefaultQueryService qs = new DefaultQueryService(server.getCache());
    qs.createIndex("myindex", "pf.id", "/portfolio pf, pf.positions pos");
    // compileFromClause returns a List<CompiledIteratorDef>
    QCompiler compiler = new QCompiler();
    List list = compiler.compileFromClause("/portfolio p, p.positions");
    ExecutionContext context = new QueryExecutionContext(null, server.getCache());
    context.newScope(context.associateScopeID());
    Iterator iter = list.iterator();
    int i = 0;
    while (iter.hasNext()) {
      CompiledIteratorDef iterDef = (CompiledIteratorDef) iter.next();
      i = computeEvaluateAndAssertIterator(context, i, iterDef);
    }
  }

  @Test
  public void testObtainingRegionPath() throws Exception {
    DefaultQueryService qs = new DefaultQueryService(server.getCache());
    qs.createIndex("myindex", "pf.id", "/portfolio pf, pf.positions pos");
    // compileFromClause returns a List<CompiledIteratorDef>
    QCompiler compiler = new QCompiler();
    List list = compiler.compileFromClause("/portfolio p, p.positions");
    ExecutionContext context = new QueryExecutionContext(null, server.getCache());
    context.newScope(context.associateScopeID());
    Iterator iter = list.iterator();
    int i = 0;
    CompiledIteratorDef iterDef = null;

    while (iter.hasNext()) {
      iterDef = (CompiledIteratorDef) iter.next();
      i = computeEvaluateAndAssertIterator(context, i, iterDef);
    }

    Set<RuntimeIterator> temp = new HashSet<>();
    context.computeUltimateDependencies(iterDef, temp);
    String regionPath = context.getRegionPathForIndependentRuntimeIterator(temp.iterator().next());

    assertThat((regionPath != null && regionPath.equals("/portfolio")))
        .as("Region path " + regionPath + " is either null or not equal to /portfolio.")
        .isTrue();
  }

  @Test
  public void testCurrScopeDpndntItrsBasedOnSingleIndpndntItr() throws Exception {
    server.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("dummy");
    // compileFromClause returns a List<CompiledIteratorDef>
    QCompiler compiler = new QCompiler();
    List list = compiler.compileFromClause(
        "/portfolio p, p.positions, p.addreses addrs, addrs.collection1 coll1, /dummy d1, d1.collection2 d2");
    RuntimeIterator indItr = null;
    ExecutionContext context = new QueryExecutionContext(null, server.getCache());
    context.newScope(context.associateScopeID());
    int i = 0;
    List<RuntimeIterator> checkList = new ArrayList<>();

    for (Object o : list) {
      CompiledIteratorDef iterDef = (CompiledIteratorDef) o;
      @SuppressWarnings("unchecked")
      Set<RuntimeIterator> dependencies = iterDef.computeDependencies(context);
      context.addDependencies(new CompiledID("test"), dependencies);
      RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
      if (i == 0) {
        indItr = rIter;
        checkList.add(rIter);
      } else {
        if (i < 4) {
          checkList.add(rIter);
        }
      }
      ++i;
      context.bindIterator(rIter);
      context.addToIndependentRuntimeItrMap(iterDef);

      assertThat(rIter.getIndexInternalID())
          .as("The index_internal_id is not set as per expectation of iter'n'")
          .isEqualTo(rIter.getInternalId());
    }

    List list1 = context.getCurrScopeDpndntItrsBasedOnSingleIndpndntItr(indItr);
    assertThat(list1.size())
        .as("The dependency set returned incorrect result with size =" + list1.size())
        .isEqualTo(4);

    assertThat(list1).isEqualTo(checkList);
  }

  @Test
  public void testScopeIndex1() throws Exception {
    server.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("positions");
    // compileFromClause returns a List<CompiledIteratorDef>
    String qry =
        "select distinct p.pf, ELEMENT(select distinct pf1 from /portfolio pf1 where pf1.getID = p.pf.getID )  from (select distinct pf, pos from /portfolio pf, pf.positions.values pos) p, (select distinct * from /positions rtPos where rtPos.secId = p.pos.secId) as y "
            + "where ( select distinct pf2 from /portfolio pf2 ).size() <> 0 ";
    ExecutionContext context = new QueryExecutionContext(null, server.getCache());
    QCompiler compiler = new QCompiler();
    CompiledValue query = compiler.compileQuery(qry);
    query.computeDependencies(context);
    Set runtimeItrs = context.getDependencySet(query, true);
    Iterator itr = runtimeItrs.iterator();

    assertIteratorScope(itr);
    query.evaluate(context);
    runtimeItrs = context.getDependencySet(query, true);
    itr = runtimeItrs.iterator();
    assertIteratorScope(itr);
  }

  @Test
  public void testMultiThreadedScopeIndex() {
    server.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("positions");
    // compileFromClause returns a List<CompiledIteratorDef>
    String qry =
        "select distinct p.pf from (select distinct pf, pos from /portfolio pf, pf.positions.values pos) p, (select distinct * from /positions rtPos where rtPos.secId = p.pos.secId) as y";
    final int TOTAL_THREADS = 80;
    QCompiler compiler = new QCompiler();
    final CompiledValue query = compiler.compileQuery(qry);
    final CountDownLatch latch = new CountDownLatch(TOTAL_THREADS);

    Runnable runnable = () -> {
      try {
        latch.countDown();
        latch.await();
        ExecutionContext context = new QueryExecutionContext(null, server.getCache());
        query.computeDependencies(context);
        Set runtimeItrs = context.getDependencySet(query, true);
        Iterator itr = runtimeItrs.iterator();

        while (itr.hasNext()) {
          assertIteratorScopeMultiThreaded(itr);
          Thread.yield();
        }

        query.evaluate(context);
        runtimeItrs = context.getDependencySet(query, true);
        itr = runtimeItrs.iterator();
        while (itr.hasNext()) {
          assertIteratorScopeMultiThreaded(itr);
        }
      } catch (VirtualMachineError e) {
        SystemFailure.initiateFailure(e);
        throw e;
      } catch (Throwable th) {
        throw new RuntimeException(th);
      }
    };

    Thread[] th = new Thread[TOTAL_THREADS];
    for (int i = 0; i < th.length; ++i) {
      th[i] = new Thread(runnable);
    }

    for (Thread thread : th) {
      thread.start();
    }

    for (Thread thread : th) {
      ThreadUtils.join(thread, 30 * 1000);
    }
  }
}
