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
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

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
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@RunWith(GeodeParamsRunner.class)
@Category({OQLQueryTest.class, SecurityTest.class})
public class DefaultQuerySecurityIntegrationTest {
  private final int entries = 500;
  private final int executions = 20;
  private InternalCache spiedCache;

  @Rule
  public TestName testName = new TestName();

  @Rule
  public ServerStarterRule server = new ServerStarterRule()
      .withProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName())
      .withProperty("security-username", "cluster")
      .withProperty("security-password", "cluster")
      .withAutoStart();

  private void createAndPopulateRegion(String name, RegionShortcut shortcut) {
    Cache cache = server.getCache();
    Region<Integer, QueryTestObject> region =
        cache.<Integer, QueryTestObject>createRegionFactory(shortcut).create(name);
    IntStream.range(0, entries)
        .forEach(id -> region.put(id, new CustomQueryTestObject(id, "id_" + id)));
    await().untilAsserted(() -> assertThat(region.size()).isEqualTo(entries));
  }

  private void executeQueryAndAssertThatCachedAuthorizationsWereUsed(String queryString,
      int methodInvocations) {
    IntStream.range(1, executions).forEach(counter -> {
      try {
        DefaultQuery query = spy(new DefaultQuery(queryString, spiedCache, false));
        doAnswer(new SpyQueryExecutor(spiedCache)).when(query).checkQueryOnPR(any());

        SelectResults result = (SelectResults) query.execute();
        assertThat(result.size()).isEqualTo(entries);
        assertThat(SpyAuthorizer.authorizations.get()).isEqualTo(methodInvocations * counter);
      } catch (Exception exception) {
        throw new RuntimeException(exception);
      }
    });
  }

  private void executeQueryAndAssertThatAuthorizerWasInstantiatedExpectedAmountOfTimes(
      String queryString) {
    IntStream.range(1, executions).forEach(counter -> {
      try {
        DefaultQuery query = spy(new DefaultQuery(queryString, spiedCache, false));
        doAnswer(new SpyQueryExecutor(spiedCache)).when(query).checkQueryOnPR(any());

        SelectResults result = (SelectResults) query.execute();
        assertThat(result.size()).isEqualTo(entries);
        assertThat(SpyAuthorizer.instantiations.get()).isEqualTo(1);
      } catch (Exception exception) {
        throw new RuntimeException(exception);
      }
    });
  }

  @Before
  public void setUp() throws ClassNotFoundException {
    SpyAuthorizer.instantiations.set(0);
    SpyAuthorizer.authorizations.set(0);

    spiedCache = spy(server.getCache());
    QueryConfigurationService queryConfig = spiedCache.getService(QueryConfigurationService.class);
    queryConfig.updateMethodAuthorizer(spiedCache, false, SpyAuthorizer.class.getName(),
        Collections.emptySet());
  }

  @Test
  @Parameters({"LOCAL", "REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0}")
  public void queryWithSingleProjectionFieldShouldUseCachingForImplicitMethodAuthorizations(
      RegionShortcut regionShortcut) {
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut);
    String queryString = "SELECT object.name FROM " + SEPARATOR + regionName + " object";

    executeQueryAndAssertThatCachedAuthorizationsWereUsed(queryString, 1);
  }

  @Test
  @Parameters({"LOCAL", "REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0}")
  public void queryWithMultipleProjectionFieldsShouldUseCachingForImplicitMethodAuthorizations(
      RegionShortcut regionShortcut) {
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut);
    String queryString =
        "SELECT object.privateID, object.name FROM " + SEPARATOR + regionName + " object";

    executeQueryAndAssertThatCachedAuthorizationsWereUsed(queryString, 2);
  }

  @Test
  @Parameters({"LOCAL", "REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0}")
  public void queryWithSingleProjectionMethodShouldUseCachingForExplicitMethodAuthorizations(
      RegionShortcut regionShortcut) {
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut);
    String queryString = "SELECT object.getName() FROM " + SEPARATOR + regionName + " object";

    executeQueryAndAssertThatCachedAuthorizationsWereUsed(queryString, 1);
  }

  @Test
  @Parameters({"LOCAL", "REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0}")
  public void queryWithMultipleProjectionMethodsShouldUseCachingForExplicitMethodAuthorizations(
      RegionShortcut regionShortcut) {
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut);
    String queryString =
        "SELECT object.getId(), object.getName() FROM " + SEPARATOR + regionName + " object";

    executeQueryAndAssertThatCachedAuthorizationsWereUsed(queryString, 2);
  }

  @Test
  @Parameters({"LOCAL", "REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0}")
  public void queryWithSingleProjectionFieldShouldUseAuthorizerFromExecutionContext(
      RegionShortcut regionShortcut) {
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut);
    String queryString = "SELECT object.name FROM " + SEPARATOR + regionName + " object";

    executeQueryAndAssertThatAuthorizerWasInstantiatedExpectedAmountOfTimes(queryString);
  }

  @Test
  @Parameters({"LOCAL", "REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0}")
  public void queryWithMultipleProjectionFieldShouldUseAuthorizerFromExecutionContext(
      RegionShortcut regionShortcut) {
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut);
    String queryString =
        "SELECT object.privateID, object.name FROM " + SEPARATOR + regionName + " object";

    executeQueryAndAssertThatAuthorizerWasInstantiatedExpectedAmountOfTimes(queryString);
  }

  @Test
  @Parameters({"LOCAL", "REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0}")
  public void queryWithSingleProjectionMethodShouldUseAuthorizerFromExecutionContext(
      RegionShortcut regionShortcut) {
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut);
    String queryString = "SELECT object.getName() FROM " + SEPARATOR + regionName + " object";

    executeQueryAndAssertThatAuthorizerWasInstantiatedExpectedAmountOfTimes(queryString);
  }

  @Test
  @Parameters({"LOCAL", "REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0}")
  public void queryWithMultipleProjectionMethodsShouldUseAuthorizerFromExecutionContext(
      RegionShortcut regionShortcut) {
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut);
    String queryString =
        "SELECT object.getId(), object.getName() FROM " + SEPARATOR + regionName + " object";

    executeQueryAndAssertThatAuthorizerWasInstantiatedExpectedAmountOfTimes(queryString);
  }

  private static class SpyQueryExecutor implements Answer {
    private final InternalCache spyCache;

    SpyQueryExecutor(InternalCache spyCache) {
      this.spyCache = spyCache;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      QueryExecutor executor = (QueryExecutor) invocation.callRealMethod();

      if (executor instanceof PartitionedRegion) {
        PartitionedRegion prExecutor = spy((PartitionedRegion) executor);
        when(prExecutor.getCache()).thenReturn(spyCache);

        return prExecutor;
      }

      return executor;
    }
  }

  private static class SpyAuthorizer implements MethodInvocationAuthorizer {
    static final AtomicInteger instantiations = new AtomicInteger(0);
    static final AtomicInteger authorizations = new AtomicInteger(0);

    SpyAuthorizer() {
      instantiations.incrementAndGet();
    }

    @Override
    public boolean authorize(Method method, Object target) {
      authorizations.incrementAndGet();

      return true;
    }
  }

  @SuppressWarnings("unused")
  private static class CustomQueryTestObject extends QueryTestObject {
    private final int privateID;

    public int privateID() {
      return privateID;
    }

    CustomQueryTestObject(int id, String name) {
      super(id, name);
      privateID = id;
    }
  }
}
