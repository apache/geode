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

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.security.query.data.QueryTestObject;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@RunWith(JUnitParamsRunner.class)
@Category({OQLQueryTest.class, SecurityTest.class})
public class DefaultQuerySecurityIntegrationTest {
  private InternalCache spiedCache;

  @Rule
  public TestName testName = new TestName();

  @Rule
  public ServerStarterRule server = new ServerStarterRule()
      .withProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName())
      .withProperty("security-username", "cluster")
      .withProperty("security-password", "cluster")
      .withAutoStart();

  private void createAndPopulateRegion(String name, RegionShortcut shortcut, int entries) {
    Cache cache = server.getCache();
    Region<Integer, QueryTestObject> region =
        cache.<Integer, QueryTestObject>createRegionFactory(shortcut).create(name);
    IntStream.range(0, entries)
        .forEach(id -> region.put(id, new CustomQueryTestObject(id, "id_" + id)));
    await().untilAsserted(() -> assertThat(region.size()).isEqualTo(entries));
  }

  @Before
  public void setUp() {
    spiedCache = spy(server.getCache());
    InternalQueryService spyQueryService = spy(new DefaultQueryService(server.getCache()));
    doReturn(new SpyAuthorizer()).when(spyQueryService).getMethodInvocationAuthorizer();
    when(spiedCache.getQueryService()).thenReturn(spyQueryService);
  }

  @After
  public void tearDown() {
    SpyAuthorizer.authorizations.set(0);
  }

  private void executeQueryAndAssertThatCachedAuthorizationsWereUsed(String queryString,
      int methodInvocations, int executions, int entries) {
    // Every new query has a new context and, thus, authorization should be executed again.
    IntStream.range(1, executions).forEach(counter -> {
      try {
        DefaultQuery query = spy(new DefaultQuery(queryString, spiedCache, false));

        // Make sure the PR Executor uses our spy cache.
        doAnswer(answer -> {
          QueryExecutor executor = (QueryExecutor) answer.callRealMethod();
          if (executor instanceof PartitionedRegion) {
            PartitionedRegion prExecutor = spy((PartitionedRegion) executor);
            when(prExecutor.getCache()).thenReturn(spiedCache);
            return prExecutor;
          }

          return executor;
        }).when(query).checkQueryOnPR(any());

        SelectResults result = (SelectResults) query.execute();
        assertThat(result.size()).isEqualTo(entries);

        assertThat(SpyAuthorizer.authorizations.get()).isEqualTo(methodInvocations * counter);
      } catch (Exception exception) {
        throw new RuntimeException(exception);
      }
    });
  }

  @Test
  @Parameters({"LOCAL", "REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0}")
  public void queryWithSingleProjectionFieldShouldUseCachingForImplicitMethodAuthorizations(
      RegionShortcut regionShortcut) {
    int entries = 100;
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut, entries);
    String queryString = "SELECT object.name FROM /" + regionName + " object";

    executeQueryAndAssertThatCachedAuthorizationsWereUsed(queryString, 1, 10, entries);
  }

  @Test
  @Parameters({"LOCAL", "REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0}")
  public void queryWithMultipleProjectionFieldsShouldUseCachingForImplicitMethodAuthorizations(
      RegionShortcut regionShortcut) {
    int entries = 200;
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut, entries);
    String queryString = "SELECT object.privateID, object.name FROM /" + regionName + " object";

    executeQueryAndAssertThatCachedAuthorizationsWereUsed(queryString, 2, 10, entries);
  }

  @Test
  @Parameters({"LOCAL", "REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0}")
  public void queryWithSingleProjectionMethodShouldUseCachingForExplicitMethodAuthorizations(
      RegionShortcut regionShortcut) {
    int entries = 100;
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut, entries);
    String queryString = "SELECT object.getName() FROM /" + regionName + " object";

    executeQueryAndAssertThatCachedAuthorizationsWereUsed(queryString, 1, 20, entries);
  }

  @Test
  @Parameters({"LOCAL", "REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0}")
  public void queryWithMultipleProjectionMethodsShouldUseCachingForExplicitMethodAuthorizations(
      RegionShortcut regionShortcut) {
    int entries = 200;
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut, entries);
    String queryString = "SELECT object.getId(), object.getName() FROM /" + regionName + " object";

    executeQueryAndAssertThatCachedAuthorizationsWereUsed(queryString, 2, 20, entries);
  }

  private static class SpyAuthorizer implements MethodInvocationAuthorizer {
    static final AtomicInteger authorizations = new AtomicInteger(0);

    SpyAuthorizer() {}

    @Override
    public boolean authorize(Method method, Object target) {
      authorizations.incrementAndGet();
      return true;
    }
  }

  @SuppressWarnings("unused")
  private static class CustomQueryTestObject extends QueryTestObject {
    private int privateID;

    public int privateID() {
      return privateID;
    }

    CustomQueryTestObject(int id, String name) {
      super(id, name);
      this.privateID = id;
    }
  }
}
