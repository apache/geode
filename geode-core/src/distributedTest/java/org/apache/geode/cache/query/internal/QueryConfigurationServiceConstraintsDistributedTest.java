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
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.assertj.core.api.ThrowableAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@RunWith(JUnitParamsRunner.class)
@Category({OQLQueryTest.class, SecurityTest.class})
public class QueryConfigurationServiceConstraintsDistributedTest implements Serializable {
  private static final int ENTRIES = 500;
  private static final int PUT_KEY = ENTRIES + 1;
  private static final int CREATE_KEY = ENTRIES + 2;
  private static final int REMOVE_KEY = ENTRIES - 1;
  private static final int DESTROY_KEY = ENTRIES - 2;
  private static final int UPDATE_KEY = ENTRIES - 3;
  private static final int REPLACE_KEY = ENTRIES - 4;
  private static final int INVALIDATE_KEY = ENTRIES - 5;
  private static final String ID_INDEX_IDENTIFIER = "IdIndex";
  private static final String NAME_INDEX_IDENTIFIER = "NameIndex";
  private static final QueryObject TEST_VALUE = new QueryObject(999, "name_999");
  private File logFile;
  protected MemberVM server;
  protected ClientVM client;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @SuppressWarnings("unused")
  private Object[] getRegionTypeAndOperation() {
    return new Object[] {
        new Object[] {"LOCAL", "PUT"},
        new Object[] {"LOCAL", "CREATE"},
        new Object[] {"LOCAL", "REMOVE"},
        new Object[] {"LOCAL", "DESTROY"},
        new Object[] {"LOCAL", "UPDATE"},
        new Object[] {"LOCAL", "REPLACE"},
        new Object[] {"LOCAL", "INVALIDATE"},

        new Object[] {"REPLICATE", "PUT"},
        new Object[] {"REPLICATE", "CREATE"},
        new Object[] {"REPLICATE", "REMOVE"},
        new Object[] {"REPLICATE", "DESTROY"},
        new Object[] {"REPLICATE", "UPDATE"},
        new Object[] {"REPLICATE", "REPLACE"},
        new Object[] {"REPLICATE", "INVALIDATE"},

        new Object[] {"PARTITION", "PUT"},
        new Object[] {"PARTITION", "CREATE"},
        new Object[] {"PARTITION", "REMOVE"},
        new Object[] {"PARTITION", "DESTROY"},
        new Object[] {"PARTITION", "UPDATE"},
        new Object[] {"PARTITION", "REPLACE"},
        new Object[] {"PARTITION", "INVALIDATE"},
    };
  }

  @Before
  public void setUp() throws Exception {
    logFile = temporaryFolder.newFile(testName.getMethodName());

    server = cluster.startServerVM(1, cf -> cf
        .withProperty(SECURITY_MANAGER, SimpleSecurityManager.class.getName())
        .withProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.query.internal.*")
        .withProperty("security-username", "cluster").withProperty("security-password", "cluster")
        .withProperty("log-file", logFile.getAbsolutePath()));

    server.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();
      internalCache.getService(QueryConfigurationService.class).updateMethodAuthorizer(
          internalCache, false, TestMethodAuthorizer.class.getName(),
          Stream.of(QueryObject.GET_ID_METHOD, QueryObject.GET_NAME_METHOD)
              .collect(Collectors.toSet()));
    });

    client = cluster.startClientVM(2, ccf -> ccf
        .withCredential("data", "data")
        .withServerConnection(server.getPort())
        .withProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.query.internal.*"));
  }

  @After
  public void tearDown() {
    server.invoke(QueryObserverHolder::reset);
  }

  private void createAndPopulateRegion(String regionName, RegionShortcut shortcut) {
    server.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();
      Region<Integer, QueryObject> region =
          internalCache.<Integer, QueryObject>createRegionFactory(shortcut).create(regionName);
      IntStream.range(0, ENTRIES).forEach(id -> region.put(id, new QueryObject(id, "name_" + id)));
      await().untilAsserted(() -> assertThat(region.size()).isEqualTo(ENTRIES));
    });
  }

  private ThrowableAssert.ThrowingCallable getRegionOperation(Operation operation,
      Region<Integer, QueryObject> region) {
    switch (operation) {
      case PUT:
        return () -> region.put(PUT_KEY, TEST_VALUE);
      case CREATE:
        return () -> region.create(CREATE_KEY, TEST_VALUE);
      case REMOVE:
        return () -> region.remove(REMOVE_KEY);
      case DESTROY:
        return () -> region.destroy(DESTROY_KEY);
      case UPDATE:
        return () -> region.put(UPDATE_KEY, TEST_VALUE);
      case REPLACE:
        return () -> region.replace(REPLACE_KEY, TEST_VALUE);
      case INVALIDATE:
        return () -> region.invalidate(INVALIDATE_KEY);
      default:
        return () -> {
        };
    }
  }

  private void assertRegionOperationResult(Operation operation,
      Region<Integer, QueryObject> region) {
    // Assert operation result.
    switch (operation) {
      case PUT:
        assertThat(region.get(PUT_KEY)).isEqualTo(TEST_VALUE);
        break;
      case CREATE:
        assertThat(region.get(CREATE_KEY)).isEqualTo(TEST_VALUE);
        break;
      case REMOVE:
        assertThat(region.get(REMOVE_KEY)).isNull();
        break;
      case DESTROY:
        assertThat(region.get(DESTROY_KEY)).isNull();
        break;
      case UPDATE:
        assertThat(region.get(UPDATE_KEY)).isEqualTo(TEST_VALUE);
        break;
      case REPLACE:
        assertThat(region.get(REPLACE_KEY)).isEqualTo(TEST_VALUE);
        break;
      case INVALIDATE:
        assertThat(region.get(INVALIDATE_KEY)).isNull();
        break;
    }
  }

  private void executeOperationFromClient(String regionName, Operation operation) {
    client.invoke(() -> {
      assertThat(ClusterStartupRule.getClientCache()).isNotNull();
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<Integer, QueryObject> region =
          clientCache.<Integer, QueryObject>createClientRegionFactory(ClientRegionShortcut.PROXY)
              .create(regionName);

      ThrowableAssert.ThrowingCallable operationCallable =
          getRegionOperation(operation, region);
      assertThatCode(operationCallable).doesNotThrowAnyException();
    });
  }

  /**
   * The test registers a {@link QueryObserver} that changes the installed
   * {@link MethodInvocationAuthorizer} to the default one (denies everything) as soon as the query
   * starts, and makes sure the query succeeds (it is not affected by the authorizer change).
   * The query is executed from the client.
   */
  @Test
  @Parameters({"LOCAL", "REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0})")
  public void queriesInFlightExecutedByClientsShouldNotBeAffectedWhenMethodAuthorizerIsChanged(
      RegionShortcut regionShortcut) {
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut);
    String queryString =
        "<TRACE> SELECT object." + QueryObject.GET_NAME_METHOD + " FROM " + SEPARATOR + regionName
            + " object";

    // Set test query observer.
    server.invoke(() -> {
      TestQueryObserver queryObserver =
          new TestQueryObserver(RestrictedMethodAuthorizer.class.getName(), Collections.emptySet());
      QueryObserverHolder.setInstance(queryObserver);
    });

    // Execute query from the client.
    client.invoke(() -> {
      assertThat(ClusterStartupRule.getClientCache()).isNotNull();
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Query query = clientCache.getQueryService().newQuery(queryString);
      @SuppressWarnings("unchecked")
      SelectResults<QueryObject> result = (SelectResults<QueryObject>) query.execute();
      assertThat(result.size()).isEqualTo(ENTRIES);
    });

    // Execute query again, it should fail as the restricted authorizer has been installed.
    server.invoke(QueryObserverHolder::reset);
    client.invoke(() -> {
      assertThat(ClusterStartupRule.getClientCache()).isNotNull();
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Query newQuery = clientCache.getQueryService().newQuery(queryString);
      assertThatThrownBy(newQuery::execute)
          .isInstanceOf(ServerOperationException.class)
          .hasCauseInstanceOf(NotAuthorizedException.class)
          .hasStackTraceContaining(
              RestrictedMethodAuthorizer.UNAUTHORIZED_STRING + QueryObject.GET_NAME_METHOD);
    });
  }

  /**
   * The test registers a {@link QueryObserver} that changes the installed
   * {@link MethodInvocationAuthorizer} to the default one (denies everything) as soon as the query
   * starts, and makes sure the query succeeds (it is not affected by the authorizer change).
   * The query is executed from the server.
   */
  @Test
  @Parameters({"LOCAL", "REPLICATE", "PARTITION"})
  @TestCaseName("[{index}] {method}(RegionType:{0})")
  public void queriesInFlightExecutedByServersShouldNotBeAffectedWhenMethodAuthorizerIsChanged(
      RegionShortcut regionShortcut) {
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut);
    String queryString =
        "<TRACE> SELECT object." + QueryObject.GET_NAME_METHOD + " FROM " + SEPARATOR + regionName
            + " object";

    server.invoke(() -> {
      // Set test query observer.
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();
      TestQueryObserver queryObserver =
          new TestQueryObserver(RestrictedMethodAuthorizer.class.getName(), Collections.emptySet());
      QueryObserverHolder.setInstance(queryObserver);

      // Execute query.
      Query query = internalCache.getQueryService().newQuery(queryString);
      @SuppressWarnings("unchecked")
      SelectResults<QueryObject> result = (SelectResults<QueryObject>) query.execute();
      assertThat(queryObserver.invocations.get()).isEqualTo(1);
      assertThat(result.size()).isEqualTo(ENTRIES);

      // Execute query again, it should fail as the restricted authorizer has been installed.
      QueryObserverHolder.reset();
      Query newQuery = internalCache.getQueryService().newQuery(queryString);
      assertThatThrownBy(newQuery::execute)
          .isInstanceOf(NotAuthorizedException.class)
          .hasMessage(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING + QueryObject.GET_NAME_METHOD);
    });
  }

  /**
   * The test creates an index with a method invocation as part of the expression, changes
   * the {@link MethodInvocationAuthorizer} to a custom one that still allows the method that is
   * invoked by the index and executes a region operation from the client that would cause an
   * index mapping removal/addition.
   * The operation should succeed, the index should still be valid and no errors should be logged.
   */
  @Test
  @Parameters(method = "getRegionTypeAndOperation")
  @TestCaseName("[{index}] {method}(RegionType:{0};Operation:{1})")
  public void indexesShouldNotBeAffectedByMethodAuthorizerChangeAfterRegionOperationOnClientWhenIndexedExpressionContainsMethodsAllowedByTheNewAuthorizer(
      RegionShortcut regionShortcut, Operation operation) {
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut);

    server.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();

      // Index is valid.
      QueryService queryService = internalCache.getQueryService();
      queryService.createIndex(NAME_INDEX_IDENTIFIER, "e." + QueryObject.GET_NAME_METHOD,
          SEPARATOR + regionName + " e");
      Index index =
          queryService.getIndex(internalCache.getRegion(regionName), NAME_INDEX_IDENTIFIER);
      assertThat(index.isValid()).isTrue();

      // Change the authorizer (still allow 'getName' to be executed)
      internalCache.getService(QueryConfigurationService.class).updateMethodAuthorizer(
          internalCache, false, TestMethodAuthorizer.class.getName(),
          Stream.of(QueryObject.GET_NAME_METHOD).collect(Collectors.toSet()));
    });

    // Execute operation on client side.
    executeOperationFromClient(regionName, operation);

    // Assert that operation succeeded and that index is still valid.
    server.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();
      QueryService queryService = internalCache.getQueryService();
      Region<Integer, QueryObject> region = internalCache.getRegion(regionName);
      Index indexInvalid =
          queryService.getIndex(internalCache.getRegion(regionName), NAME_INDEX_IDENTIFIER);
      assertThat(indexInvalid.isValid()).isTrue();
      assertRegionOperationResult(operation, region);
    });

    // No errors logged on server side.
    LogFileAssert.assertThat(logFile)
        .doesNotContain(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING);
  }

  /**
   * The test creates an index with a method invocation as part of the expression, changes
   * the {@link MethodInvocationAuthorizer} to a custom one that still allows the methods part of
   * the index expression and executes a region operation from the server that would cause an
   * index mapping removal/addition.
   * The operation should succeed, the index should still be valid and no errors should be logged.
   */
  @Test
  @Parameters(method = "getRegionTypeAndOperation")
  @TestCaseName("[{index}] {method}(RegionType:{0};Operation:{1})")
  public void indexesShouldNotBeAffectedByMethodAuthorizerChangeAfterRegionOperationOnServerWhenIndexedExpressionContainsMethodsAllowedByTheNewAuthorizer(
      RegionShortcut regionShortcut, Operation operation) {
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut);

    server.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();
      Region<Integer, QueryObject> region = internalCache.getRegion(regionName);

      // Index is valid.
      QueryService queryService = internalCache.getQueryService();
      queryService.createIndex(NAME_INDEX_IDENTIFIER, "e." + QueryObject.GET_NAME_METHOD,
          SEPARATOR + regionName + " e");
      Index index =
          queryService.getIndex(internalCache.getRegion(regionName), NAME_INDEX_IDENTIFIER);
      assertThat(index.isValid()).isTrue();

      // Operation to invoke.
      ThrowableAssert.ThrowingCallable operationCallable =
          getRegionOperation(operation, region);

      // Change the authorizer (still allow 'getName' to be executed)
      internalCache.getService(QueryConfigurationService.class).updateMethodAuthorizer(
          internalCache, false, TestMethodAuthorizer.class.getName(),
          Stream.of(QueryObject.GET_NAME_METHOD).collect(Collectors.toSet()));

      // Execute operation, assert operation result, index is valid and no exceptions logged.
      assertThatCode(operationCallable).doesNotThrowAnyException();
      Index indexInvalid =
          queryService.getIndex(internalCache.getRegion(regionName), NAME_INDEX_IDENTIFIER);
      assertThat(indexInvalid.isValid()).isTrue();
      assertRegionOperationResult(operation, region);
    });

    // No errors logged on server side.
    LogFileAssert.assertThat(logFile)
        .doesNotContain(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING);
  }

  /**
   * The test creates an index with a method invocation as part of the expression, changes
   * the {@link MethodInvocationAuthorizer} to the default one (denies everything) and executes a
   * region operation from the client that would cause an index mapping removal/addition.
   * The operation should succeed, the index should be marked as invalid and the error should be
   * logged.
   */
  @Test
  @Parameters(method = "getRegionTypeAndOperation")
  @TestCaseName("[{index}] {method}(RegionType:{0};Operation:{1})")
  public void indexesShouldBeMarkedAsInvalidDuringMappingRemovalOrAdditionAfterRegionOperationOnClientWhenMethodAuthorizerIsChangedAndIndexExpressionContainsMethodsNotAllowedByTheNewAuthorizer(
      RegionShortcut regionShortcut, Operation operation) {
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut);

    server.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();

      // Index is valid.
      QueryService queryService = internalCache.getQueryService();
      queryService.createIndex(ID_INDEX_IDENTIFIER, "e." + QueryObject.GET_ID_METHOD,
          SEPARATOR + regionName + " e");
      Index index = queryService.getIndex(internalCache.getRegion(regionName), ID_INDEX_IDENTIFIER);
      assertThat(index.isValid()).isTrue();

      // Change the authorizer (deny everything not allowed by default).
      internalCache.getService(QueryConfigurationService.class).updateMethodAuthorizer(
          internalCache, false, RestrictedMethodAuthorizer.class.getName(), Collections.emptySet());
    });

    // Execute operation on client side.
    executeOperationFromClient(regionName, operation);

    // Assert that operation succeeded but index is marked as invalid.
    server.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();
      QueryService queryService = internalCache.getQueryService();
      Region<Integer, QueryObject> region = internalCache.getRegion(regionName);
      Index indexInvalid =
          queryService.getIndex(internalCache.getRegion(regionName), ID_INDEX_IDENTIFIER);
      assertThat(indexInvalid.isValid()).isFalse();
      assertRegionOperationResult(operation, region);
    });

    // Assert index modification failure was logged.
    LogFileAssert.assertThat(logFile)
        .contains(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING + QueryObject.GET_ID_METHOD);
  }


  /**
   * The test creates an index with a method invocation as part of the expression, changes
   * the {@link MethodInvocationAuthorizer} to the default one (denies everything) and executes a
   * region operation on the server that would cause an index mapping removal/addition.
   * The operation should succeed, the index should be marked as invalid and the error should be
   * logged.
   */
  @Test
  @Parameters(method = "getRegionTypeAndOperation")
  @TestCaseName("[{index}] {method}(RegionType:{0};Operation:{1})")
  public void indexesShouldBeMarkedAsInvalidDuringMappingRemovalOrAdditionAfterRegionOperationOnServerWhenMethodAuthorizerIsChangedAndIndexExpressionContainsMethodsNotAllowedByTheNewAuthorizer(
      RegionShortcut regionShortcut, Operation operation) {
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut);

    server.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();
      Region<Integer, QueryObject> region = internalCache.getRegion(regionName);

      // Index is valid.
      QueryService queryService = internalCache.getQueryService();
      queryService.createIndex(ID_INDEX_IDENTIFIER, "e." + QueryObject.GET_ID_METHOD,
          SEPARATOR + regionName + " e");
      Index index = queryService.getIndex(internalCache.getRegion(regionName), ID_INDEX_IDENTIFIER);
      assertThat(index.isValid()).isTrue();

      // Operation to invoke.
      ThrowableAssert.ThrowingCallable operationCallable =
          getRegionOperation(operation, region);

      // Change the authorizer (deny everything not allowed by default).
      internalCache.getService(QueryConfigurationService.class).updateMethodAuthorizer(
          internalCache, false, RestrictedMethodAuthorizer.class.getName(), Collections.emptySet());

      // Execute operation, should succeed but index modification should fail, marking it as
      // invalid.
      assertThatCode(operationCallable).doesNotThrowAnyException();
      Index indexInvalid =
          queryService.getIndex(internalCache.getRegion(regionName), ID_INDEX_IDENTIFIER);
      assertThat(indexInvalid.isValid()).isFalse();

      // Assert operation result.
      assertRegionOperationResult(operation, region);
    });

    // Assert index modification failure was logged.
    LogFileAssert.assertThat(logFile)
        .contains(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING + QueryObject.GET_ID_METHOD);
  }

  public enum Operation {
    PUT, CREATE, REMOVE, DESTROY, UPDATE, REPLACE, INVALIDATE
  }

  private static class QueryObject implements Serializable {
    static final String GET_ID_METHOD = "getId";
    static final String GET_NAME_METHOD = "getName";

    private final int id;
    private final String name;

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    QueryObject(int id, String name) {
      this.id = id;
      this.name = name;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      QueryObject that = (QueryObject) o;
      if (getId() != that.getId()) {
        return false;
      }

      return getName() != null ? getName().equals(that.getName()) : that.getName() == null;
    }

    @Override
    public int hashCode() {
      int result = getId();
      result = 31 * result + (getName() != null ? getName().hashCode() : 0);
      return result;
    }
  }

  public static class TestMethodAuthorizer implements MethodInvocationAuthorizer {
    private Set<String> authorizedMethods;
    private RestrictedMethodAuthorizer restrictedMethodAuthorizer;

    @Override
    public void initialize(Cache cache, Set<String> parameters) {
      this.authorizedMethods = parameters;
      this.restrictedMethodAuthorizer = new RestrictedMethodAuthorizer(cache);
    }

    @Override
    public boolean authorize(Method method, Object target) {
      if (restrictedMethodAuthorizer.authorize(method, target)) {
        return true;
      }

      return authorizedMethods.contains(method.getName());
    }
  }

  // Query Observer to change the authorizer right after the query execution starts.
  private static class TestQueryObserver extends QueryObserverAdapter implements Serializable {
    final AtomicInteger invocations = new AtomicInteger(0);
    private final String authorizerClassName;
    private final Set<String> authorizerParameters;

    TestQueryObserver(String authorizerClassName, Set<String> authorizerParameters) {
      this.authorizerClassName = authorizerClassName;
      this.authorizerParameters = authorizerParameters;
    }

    @Override
    public void startQuery(Query query) {
      invocations.incrementAndGet();

      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();
      internalCache.getService(QueryConfigurationService.class)
          .updateMethodAuthorizer(internalCache, false, authorizerClassName, authorizerParameters);
    }
  }
}
