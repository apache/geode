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

import static java.util.stream.Collectors.toSet;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPort;
import static org.apache.geode.security.templates.UserPasswordAuthInit.PASSWORD;
import static org.apache.geode.security.templates.UserPasswordAuthInit.USER_NAME;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getController;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import junitparams.JUnitParamsRunner;
import junitparams.NamedParameters;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.query.CqAttributesFactory;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqListener;
import org.apache.geode.cache.query.CqQuery;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.RegionNotFoundException;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.cache.query.security.RestrictedMethodAuthorizer;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.assertj.LogFileAssert;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.OQLQueryTest;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({OQLQueryTest.class, SecurityTest.class})
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class QueryConfigurationServiceConstraintsDistributedTest implements Serializable {

  private static final String GET_ID_METHOD = "getId";
  private static final String GET_NAME_METHOD = "getName";

  private static final int ENTRIES = 300;
  private static final int PUT_KEY = ENTRIES + 1;
  private static final int CREATE_KEY = ENTRIES + 2;
  private static final int REMOVE_KEY = ENTRIES - 1;
  private static final int DESTROY_KEY = ENTRIES - 2;
  private static final int UPDATE_KEY = ENTRIES - 3;
  private static final int REPLACE_KEY = ENTRIES - 4;
  private static final int INVALIDATE_KEY = ENTRIES - 5;
  private static final QueryObject TEST_VALUE = new QueryObject(999, "name_999");

  private static volatile CountingCqListener cqListener;
  private static volatile ServerLauncher serverLauncher;
  private static volatile ClientCache clientCache;

  private VM serverVM;
  private VM clientVM;

  private String regionName;
  private String queryString;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() {
    serverVM = getVM(0);
    clientVM = getController();

    regionName = testName.getMethodName();
    queryString = String.join(" ",
        "SELECT * FROM /" + regionName + " object",
        "WHERE object." + GET_ID_METHOD + " > -1");

    int serverPort = getRandomAvailableTCPPort();

    serverVM.invoke(() -> {
      serverLauncher = new ServerLauncher.Builder()
          .setMemberName("server")
          .setServerPort(serverPort)
          .setWorkingDirectory(temporaryFolder.newFolder("server").getAbsolutePath())
          .set(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.query.internal.*")
          .set(SECURITY_MANAGER, SimpleSecurityManager.class.getName())
          .set(USER_NAME, "cluster")
          .set(PASSWORD, "cluster")
          .build();

      serverLauncher.start();

      InternalCache internalCache = (InternalCache) serverLauncher.getCache();
      assertThat(internalCache).isNotNull();

      internalCache
          .getService(QueryConfigurationService.class)
          .updateMethodAuthorizer(internalCache, true, TestMethodAuthorizer.class.getName(),
              Stream.of(GET_ID_METHOD, GET_NAME_METHOD).collect(toSet()));
    });

    clientVM.invoke(() -> {
      clientCache = new ClientCacheFactory()
          .set(SERIALIZABLE_OBJECT_FILTER, "org.apache.geode.cache.query.internal.*")
          .set(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName())
          .set(USER_NAME, "data")
          .set(PASSWORD, "data")
          .addPoolServer("localhost", serverPort)
          .setPoolSubscriptionEnabled(true)
          .create();
    });
  }

  @NamedParameters("parameters")
  @SuppressWarnings("unused")
  private static String[] namedParameters() {
    return new String[] {
        "REPLICATE, PUT, true", "REPLICATE, PUT, false",
        "REPLICATE, CREATE, true", "REPLICATE, CREATE, false",
        "REPLICATE, REMOVE, true", "REPLICATE, REMOVE, false",
        "REPLICATE, DESTROY, true", "REPLICATE, DESTROY, false",
        "REPLICATE, UPDATE, true", "REPLICATE, UPDATE, false",
        "REPLICATE, REPLACE, true", "REPLICATE, REPLACE, false",
        "REPLICATE, INVALIDATE, true", "REPLICATE, INVALIDATE, false",
        "PARTITION, PUT, true", "REPLICATE, PUT, false",
        "PARTITION, CREATE, true", "REPLICATE, CREATE, false",
        "PARTITION, REMOVE, true", "REPLICATE, REMOVE, false",
        "PARTITION, DESTROY, true", "REPLICATE, DESTROY, false",
        "PARTITION, UPDATE, true", "REPLICATE, UPDATE, false",
        "PARTITION, REPLACE, true", "REPLICATE, REPLACE, false",
        "PARTITION, INVALIDATE, true", "REPLICATE, INVALIDATE, false"};
  }

  /**
   * The test creates a CQ with a method invocation as part of the expression, changes
   * the {@link MethodInvocationAuthorizer} to a custom one that still allows the methods part of
   * the expression and executes all region operations that would fire the CQ.
   * The operations should succeed, the CQ should fire 'onEvent' and no errors should be logged.
   */
  @Test
  @Parameters(named = "parameters")
  @TestCaseName("{method}(regionShortcut={0}, operation={1}, executeWithInitialResults={2})")
  public void cqsShouldSucceedDuringEventProcessingAfterRegionOperationWhenMethodAuthorizerIsChangedAndQueryContainsMethodsAllowedByTheNewAuthorizer(
      RegionShortcut regionShortcut, Operation operation, boolean executeWithInitialResults) {
    serverVM.invoke(() -> {
      createRegion(regionShortcut);
      populateRegion();
    });

    clientVM.invoke(() -> createClientCq(queryString, executeWithInitialResults));

    serverVM.invoke(() -> {
      InternalCache internalCache = (InternalCache) serverLauncher.getCache();
      assertThat(internalCache.getCqService().getAllCqs()).hasSize(1);

      // Make Sure Cq is running before continuing.
      internalCache.getCqService().getAllCqs()
          .forEach(cq -> await().untilAsserted(() -> assertThat(cq.isRunning()).isTrue()));

      // Change the authorizer (still allow 'getId' to be executed)
      internalCache.getService(QueryConfigurationService.class).updateMethodAuthorizer(
          internalCache, true, TestMethodAuthorizer.class.getName(),
          Stream.of(GET_ID_METHOD).collect(toSet()));

      // Execute operations that would cause the CQ to process the event.
      doOperations(operation);

      validateRegionValues();
    });

    clientVM.invoke(() -> {
      await().untilAsserted(
          () -> assertThat(cqListener.getEventCount()).isEqualTo(Operation.values().length));
      assertThat(cqListener.getErrorCount()).isEqualTo(0);
    });

    // No errors logged on server side.
    File logFile = new File(new File(temporaryFolder.getRoot(), "server"), "server.log");
    LogFileAssert
        .assertThat(logFile)
        .doesNotContain(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING);
  }

  /**
   * The test creates a CQ with a method invocation as part of the expression, changes
   * the {@link MethodInvocationAuthorizer} to the default one (denies everything) and executes
   * all region operations that would fire the CQ.
   * The operations should succeed, the CQ should fire 'onError' and the issues should be logged.
   */
  @Test
  @Parameters(named = "parameters")
  @TestCaseName("{method}(regionShortcut={0}, operation={1}, executeWithInitialResults={2})")
  public void cqsShouldFailDuringEventProcessingAfterRegionOperationWhenMethodAuthorizerIsChangedAndQueryContainsMethodsNotAllowedByTheNewAuthorizer(
      RegionShortcut regionShortcut, Operation operation, boolean executeWithInitialResults) {
    serverVM.invoke(() -> {
      createRegion(regionShortcut);
      populateRegion();
    });

    clientVM.invoke(() -> createClientCq(queryString, executeWithInitialResults));

    serverVM.invoke(() -> {
      InternalCache internalCache = (InternalCache) serverLauncher.getCache();
      assertThat(internalCache.getCqService().getAllCqs()).hasSize(1);

      // Make Sure Cq is running before continuing.
      internalCache.getCqService().getAllCqs()
          .forEach(cq -> await().untilAsserted(() -> assertThat(cq.isRunning()).isTrue()));

      // Change the authorizer (deny everything not allowed by default).
      internalCache.getService(QueryConfigurationService.class).updateMethodAuthorizer(
          internalCache, true, RestrictedMethodAuthorizer.class.getName(), Collections.emptySet());

      // Execute operations that would cause the CQ to process the event.
      doOperations(operation);

      validateRegionValues();
    });

    clientVM.invoke(() -> {
      await().untilAsserted(
          () -> assertThat(cqListener.getErrorCount()).isEqualTo(Operation.values().length));
      assertThat(cqListener.getEventCount()).isEqualTo(0);
    });

    // No errors logged on server side.
    File logFile = new File(new File(temporaryFolder.getRoot(), "server"), "server.log");
    LogFileAssert
        .assertThat(logFile)
        .contains(RestrictedMethodAuthorizer.UNAUTHORIZED_STRING);
  }

  private void createRegion(RegionShortcut shortcut) {
    serverLauncher.getCache()
        .<Integer, QueryObject>createRegionFactory(shortcut)
        .create(regionName);
  }

  private void populateRegion() {
    Region<Integer, QueryObject> region = serverLauncher.getCache().getRegion(regionName);

    IntStream
        .range(0, ENTRIES)
        .forEach(id -> region.put(id, new QueryObject(id, "name_" + id)));

    await().untilAsserted(() -> assertThat(region.size()).isEqualTo(ENTRIES));
  }

  private void doOperations(Operation operation) {
    Region<Integer, QueryObject> region = serverLauncher.getCache().getRegion(regionName);

    operation.operate(region);

    Arrays.stream(Operation.values())
        .filter(op -> !operation.equals(op))
        .forEach(op -> op.operate(region));
  }

  private void validateRegionValues() {
    Region<Integer, QueryObject> region = serverLauncher.getCache().getRegion(regionName);

    assertThat(region.get(PUT_KEY)).isEqualTo(TEST_VALUE);
    assertThat(region.get(CREATE_KEY)).isEqualTo(TEST_VALUE);
    assertThat(region.get(REMOVE_KEY)).isNull();
    assertThat(region.get(DESTROY_KEY)).isNull();
    assertThat(region.get(UPDATE_KEY)).isEqualTo(TEST_VALUE);
    assertThat(region.get(REPLACE_KEY)).isEqualTo(TEST_VALUE);
    assertThat(region.get(INVALIDATE_KEY)).isNull();
  }

  private void createClientCq(String queryString, boolean executeWithInitialResults)
      throws CqException, RegionNotFoundException {
    cqListener = new CountingCqListener();

    QueryService queryService = clientCache.getQueryService();
    CqAttributesFactory cqAttributesFactory = new CqAttributesFactory();
    cqAttributesFactory.addCqListener(cqListener);

    CqQuery cq = queryService.newCq(queryString, cqAttributesFactory.create());
    if (executeWithInitialResults) {
      assertThat(cq.executeWithInitialResults()).hasSize(ENTRIES);
    } else {
      cq.execute();
    }
  }

  private enum Operation {
    PUT(region -> region.put(PUT_KEY, TEST_VALUE)),
    CREATE(region -> region.create(CREATE_KEY, TEST_VALUE)),
    REMOVE(region -> region.remove(REMOVE_KEY)),
    DESTROY(region -> region.destroy(DESTROY_KEY)),
    UPDATE(region -> region.put(UPDATE_KEY, TEST_VALUE)),
    REPLACE(region -> region.replace(REPLACE_KEY, TEST_VALUE)),
    INVALIDATE(region -> region.invalidate(INVALIDATE_KEY));

    private final Consumer<Region<Integer, QueryObject>> regionConsumer;

    Operation(Consumer<Region<Integer, QueryObject>> regionConsumer) {
      this.regionConsumer = regionConsumer;
    }

    void operate(Region<Integer, QueryObject> region) {
      regionConsumer.accept(region);
    }
  }

  private static class QueryObject implements Serializable {

    private final int id;
    private final String name;

    public int getId() {
      return id;
    }

    public String getName() {
      return name;
    }

    private QueryObject(int id, String name) {
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
      authorizedMethods = parameters;
      restrictedMethodAuthorizer = new RestrictedMethodAuthorizer(cache);
    }

    @Override
    public boolean authorize(Method method, Object target) {
      if (restrictedMethodAuthorizer.authorize(method, target)) {
        return true;
      }

      return authorizedMethods.contains(method.getName());
    }
  }

  private static class CountingCqListener implements CqListener {

    private final AtomicInteger eventCount = new AtomicInteger();
    private final AtomicInteger errorCount = new AtomicInteger();

    private int getEventCount() {
      return eventCount.get();
    }

    private int getErrorCount() {
      return errorCount.get();
    }

    @Override
    public void onEvent(CqEvent aCqEvent) {
      eventCount.incrementAndGet();
    }

    @Override
    public void onError(CqEvent aCqEvent) {
      errorCount.incrementAndGet();
    }
  }
}
