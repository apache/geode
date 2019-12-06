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
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.assertj.core.api.ThrowableAssert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.security.query.TestCqListener;
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
  private static final int ENTRIES = 300;
  private static final int PUT_KEY = ENTRIES + 1;
  private static final int CREATE_KEY = ENTRIES + 2;
  private static final int REMOVE_KEY = ENTRIES - 1;
  private static final int DESTROY_KEY = ENTRIES - 2;
  private static final int UPDATE_KEY = ENTRIES - 3;
  private static final int REPLACE_KEY = ENTRIES - 4;
  private static final int INVALIDATE_KEY = ENTRIES - 5;
  private static final QueryObject TEST_VALUE = new QueryObject(999, "name_999");
  private File logFile;
  protected MemberVM server;
  protected ClientVM client;
  private static TestCqListener cqListener = null;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @SuppressWarnings("unused")
  private Object[] getRegionTypeOperationsAndCqExecutionType() {
    return new Object[] {
        new Object[] {"REPLICATE", "PUT", true},
        new Object[] {"REPLICATE", "PUT", false},
        new Object[] {"REPLICATE", "CREATE", true},
        new Object[] {"REPLICATE", "CREATE", false},
        new Object[] {"REPLICATE", "REMOVE", true},
        new Object[] {"REPLICATE", "REMOVE", false},
        new Object[] {"REPLICATE", "DESTROY", true},
        new Object[] {"REPLICATE", "DESTROY", false},
        new Object[] {"REPLICATE", "UPDATE", true},
        new Object[] {"REPLICATE", "UPDATE", false},
        new Object[] {"REPLICATE", "REPLACE", true},
        new Object[] {"REPLICATE", "REPLACE", false},
        new Object[] {"REPLICATE", "INVALIDATE", true},
        new Object[] {"REPLICATE", "INVALIDATE", false},

        new Object[] {"PARTITION", "PUT", true},
        new Object[] {"PARTITION", "PUT", false},
        new Object[] {"PARTITION", "CREATE", true},
        new Object[] {"PARTITION", "CREATE", false},
        new Object[] {"PARTITION", "REMOVE", true},
        new Object[] {"PARTITION", "REMOVE", false},
        new Object[] {"PARTITION", "DESTROY", true},
        new Object[] {"PARTITION", "DESTROY", false},
        new Object[] {"PARTITION", "UPDATE", true},
        new Object[] {"PARTITION", "UPDATE", false},
        new Object[] {"PARTITION", "REPLACE", true},
        new Object[] {"PARTITION", "REPLACE", false},
        new Object[] {"PARTITION", "INVALIDATE", true},
        new Object[] {"PARTITION", "INVALIDATE", false},
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
          internalCache, true, TestMethodAuthorizer.class.getName(),
          Stream.of(QueryObject.GET_ID_METHOD, QueryObject.GET_NAME_METHOD)
              .collect(Collectors.toSet()));
    });

    client = cluster.startClientVM(2, ccf -> ccf
        .withCredential("data", "data")
        .withPoolSubscription(true)
        .withServerConnection(server.getPort())
        .withProperty(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.query.internal.*"));
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

  private void executeOperationsAndAssertResults(String regionName, Operation operation) {
    // Execute operation that would cause the CQ to process the event.
    InternalCache internalCache = ClusterStartupRule.getCache();
    assertThat(internalCache).isNotNull();
    Region<Integer, QueryObject> region = internalCache.getRegion(regionName);

    // The initial operation should fail, every later one should fail as well.
    ThrowableAssert.ThrowingCallable operationCallable =
        getRegionOperation(operation, region);
    assertThatCode(operationCallable).doesNotThrowAnyException();

    // Execute all operations after the initial one.
    Arrays.stream(Operation.values())
        .filter(op -> !operation.equals(op))
        .forEach(op -> assertThatCode(getRegionOperation(op, region))
            .doesNotThrowAnyException());

    // Assert results from operations.
    assertThat(region.get(PUT_KEY)).isEqualTo(TEST_VALUE);
    assertThat(region.get(CREATE_KEY)).isEqualTo(TEST_VALUE);
    assertThat(region.get(REMOVE_KEY)).isNull();
    assertThat(region.get(DESTROY_KEY)).isNull();
    assertThat(region.get(UPDATE_KEY)).isEqualTo(TEST_VALUE);
    assertThat(region.get(REPLACE_KEY)).isEqualTo(TEST_VALUE);
    assertThat(region.get(INVALIDATE_KEY)).isNull();
  }

  private void createClientCq(String queryString, boolean executeWithInitialResults) {
    client.invoke(() -> {
      QueryConfigurationServiceConstraintsDistributedTest.cqListener = new TestCqListener();
      assertThat(ClusterStartupRule.getClientCache()).isNotNull();
      QueryService queryService = ClusterStartupRule.getClientCache().getQueryService();
      CqAttributesFactory cqAttributesFactory = new CqAttributesFactory();
      cqAttributesFactory
          .addCqListener(QueryConfigurationServiceConstraintsDistributedTest.cqListener);

      CqQuery cq = queryService.newCq(queryString, cqAttributesFactory.create());
      if (!executeWithInitialResults) {
        cq.execute();
      } else {
        assertThat(cq.executeWithInitialResults().size()).isEqualTo(ENTRIES);
      }
    });
  }

  /**
   * The test creates a CQ with a method invocation as part of the expression, changes
   * the {@link MethodInvocationAuthorizer} to a custom one that still allows the methods part of
   * the expression and executes all region operations that would fire the CQ.
   * The operations should succeed, the CQ should fire 'onEvent' and no errors should be logged.
   */
  @Test
  @Parameters(method = "getRegionTypeOperationsAndCqExecutionType")
  @TestCaseName("[{index}] {method}(RegionType:{0};Operation:{1},ExecuteWithInitialResults:{2})")
  public void cqsShouldSucceedDuringEventProcessingAfterRegionOperationWhenMethodAuthorizerIsChangedAndQueryContainsMethodsAllowedByTheNewAuthorizer(
      RegionShortcut regionShortcut, Operation operation, boolean executeWithInitialResults) {
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut);
    String queryString = "SELECT * FROM /" + regionName + " object WHERE object."
        + QueryObject.GET_ID_METHOD + " > -1";
    createClientCq(queryString, executeWithInitialResults);

    server.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();
      assertThat(internalCache.getCqService().getAllCqs().size()).isEqualTo(1);

      // Change the authorizer (still allow 'getId' to be executed)
      internalCache.getService(QueryConfigurationService.class).updateMethodAuthorizer(
          internalCache, true, TestMethodAuthorizer.class.getName(),
          Stream.of(QueryObject.GET_ID_METHOD).collect(Collectors.toSet()));

      // Execute operations that would cause the CQ to process the event.
      executeOperationsAndAssertResults(regionName, operation);
    });

    client.invoke(() -> {
      await().untilAsserted(() -> assertThat(
          QueryConfigurationServiceConstraintsDistributedTest.cqListener.getNumErrors())
              .isEqualTo(0));
      await().untilAsserted(() -> assertThat(
          QueryConfigurationServiceConstraintsDistributedTest.cqListener.getNumEvents())
              .isEqualTo(Operation.values().length));
    });

    // No errors logged on server side.
    LogFileAssert.assertThat(logFile)
        .doesNotContain(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING);
  }

  /**
   * The test creates a CQ with a method invocation as part of the expression, changes
   * the {@link MethodInvocationAuthorizer} to the default one (denies everything) and executes
   * all region operations that would fire the CQ.
   * The operations should succeed, the CQ should fire 'onError' and the issues should be logged.
   */
  @Test
  @Parameters(method = "getRegionTypeOperationsAndCqExecutionType")
  @TestCaseName("[{index}] {method}(RegionType:{0};Operation:{1},ExecuteWithInitialResults:{2})")
  public void cqsShouldFailDuringEventProcessingAfterRegionOperationWhenMethodAuthorizerIsChangedAndQueryContainsMethodsNotAllowedByTheNewAuthorizer(
      RegionShortcut regionShortcut, Operation operation, boolean executeWithInitialResults) {
    String regionName = testName.getMethodName();
    createAndPopulateRegion(regionName, regionShortcut);
    String queryString = "SELECT * FROM /" + regionName + " object WHERE object."
        + QueryObject.GET_ID_METHOD + " > -1";
    createClientCq(queryString, executeWithInitialResults);

    server.invoke(() -> {
      InternalCache internalCache = ClusterStartupRule.getCache();
      assertThat(internalCache).isNotNull();
      assertThat(internalCache.getCqService().getAllCqs().size()).isEqualTo(1);

      // Change the authorizer (deny everything not allowed by default).
      internalCache.getService(QueryConfigurationService.class).updateMethodAuthorizer(
          internalCache, true, RestrictedMethodAuthorizer.class.getName(), Collections.emptySet());

      // Execute operations that would cause the CQ to process the event.
      executeOperationsAndAssertResults(regionName, operation);
    });

    client.invoke(() -> {
      await().untilAsserted(() -> assertThat(
          QueryConfigurationServiceConstraintsDistributedTest.cqListener.getNumEvents())
              .isEqualTo(0));
      await().untilAsserted(() -> assertThat(
          QueryConfigurationServiceConstraintsDistributedTest.cqListener.getNumErrors())
              .isEqualTo(Operation.values().length));
    });

    // No errors logged on server side.
    LogFileAssert.assertThat(logFile).contains(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING);
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
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;
      QueryObject that = (QueryObject) o;
      if (getId() != that.getId())
        return false;

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
}
