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
package org.apache.geode.internal.cache.execute;

import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.client.ClientRegionShortcut.CACHING_PROXY;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import util.TestException;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.internal.ClientMetadataService;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.MemberVM;

@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class FunctionRetryDUnitTest implements Serializable {

  private static final int TOTAL_NUM_BUCKETS = 3;
  private static final int REDUNDANT_COPIES = 0;

  private static final String regionName = "FunctionRetryDUnitTest";

  private MemberVM locator;
  private MemberVM server1;
  private MemberVM server2;
  private MemberVM server3;
  private ClientVM client;

  private Logger logger = LogService.getLogger();

  private enum HAStatus {
    NOT_HA, HA
  }

  private enum ExecutionTarget {
    REGION,
    REGION_WITH_FILTER_1_KEY,
    REGION_WITH_FILTER_2_KEYS,
    SERVER,
    SERVERS,
    SERVER_REGION_SERVICE,
    SERVERS_REGION_SERVICE,
    MEMBER,
    MEMBERS
  }

  private enum FunctionIdentifierType {
    STRING, OBJECT_REFERENCE
  }

  private enum ClientMetadataStatus {
    CLIENT_HAS_METADATA, CLIENT_MISSING_METADATA
  }

  @ClassRule
  public static final ClusterStartupRule clusterStartupRule = new ClusterStartupRule(5);

  @Rule
  public DistributedRule distributedRule = new DistributedRule(5);

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    // System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT", "100");
    locator = clusterStartupRule.startLocatorVM(0);
    server1 = startServer(1);
    server2 = startServer(2);
    server3 = startServer(3);
  }

  @After
  public void tearDown() throws Exception {
    IgnoredException.removeAllExpectedExceptions();
  }

  @Test
  // TODO: 2 keys matching filter; redundancy 1; 2 retry attempts
  @Parameters({
      /*
       * haStatus | clientMetadataStatus | functionIdentifierType | retryAttempts | expectedCalls
       */
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | -1 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 0 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 2 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | -1 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | 0 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | 2 | 1",
      "NOT_HA | CLIENT_HAS_METADATA | OBJECT_REFERENCE | -1 | 1",
      "NOT_HA | CLIENT_HAS_METADATA | STRING | -1 | 1",

      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | -1 | 3", // 11
      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 0 | 1",
      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 2 | 3", // 3
      "HA | CLIENT_MISSING_METADATA | STRING | -1 | 3", // 7, 11
      "HA | CLIENT_MISSING_METADATA | STRING | 0 | 1",
      "HA | CLIENT_MISSING_METADATA | STRING | 2 | 3", // 3
      "HA | CLIENT_HAS_METADATA | OBJECT_REFERENCE | -1 | 3", // 9, 11
      "HA | CLIENT_HAS_METADATA | STRING | -1 | 3", // 7, 9
  })
  @TestCaseName("[{index}] {method}: {params}")
  public void testOnServer(final HAStatus haStatus,
      final ClientMetadataStatus clientMetadataStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectedCalls) throws Exception {
    testAny(haStatus,
        clientMetadataStatus,
        ExecutionTarget.SERVER,
        functionIdentifierType,
        retryAttempts,
        expectedCalls);
  }

  @Test
  // TODO: 2 keys matching filter; redundancy 1; 2 retry attempts
  @Parameters({
      /*
       * haStatus | clientMetadataStatus | functionIdentifierType | retryAttempts | expectedCalls
       */
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | -1 | 3",
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 0 | 3",
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 2 | 3",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | -1 | 3",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | 0 | 3",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | 2 | 3",
      "NOT_HA | CLIENT_HAS_METADATA | OBJECT_REFERENCE | -1 | 3",
      "NOT_HA | CLIENT_HAS_METADATA | STRING | -1 | 3",

      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | -1 | 3",
      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 0 | 3",
      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 2 | 3",
      "HA | CLIENT_MISSING_METADATA | STRING | -1 | 3",
      "HA | CLIENT_MISSING_METADATA | STRING | 0 | 3",
      "HA | CLIENT_MISSING_METADATA | STRING | 2 | 3",
      "HA | CLIENT_HAS_METADATA | OBJECT_REFERENCE | -1 | 3",
      "HA | CLIENT_HAS_METADATA | STRING | -1 | 3",
  })
  @TestCaseName("[{index}] {method}: {params}")
  public void testOnServers(final HAStatus haStatus,
      final ClientMetadataStatus clientMetadataStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectedCalls) throws Exception {
    testAny(haStatus,
        clientMetadataStatus,
        ExecutionTarget.SERVERS,
        functionIdentifierType,
        retryAttempts,
        expectedCalls);
  }

  @Test
  // TODO: 2 keys matching filter; redundancy 1; 2 retry attempts
  @Parameters({
      /*
       * haStatus | clientMetadataStatus | functionIdentifierType | retryAttempts | expectedCalls
       */
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | -1 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 0 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 2 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | -1 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | 0 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | 2 | 1",
      "NOT_HA | CLIENT_HAS_METADATA | OBJECT_REFERENCE | -1 | 1",
      "NOT_HA | CLIENT_HAS_METADATA | STRING | -1 | 1",

      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | -1 | 3", // 11
      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 0 | 1",
      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 2 | 3", // 3
      "HA | CLIENT_MISSING_METADATA | STRING | -1 | 3", // 7, 11
      "HA | CLIENT_MISSING_METADATA | STRING | 0 | 1",
      "HA | CLIENT_MISSING_METADATA | STRING | 2 | 3", // 3
      "HA | CLIENT_HAS_METADATA | OBJECT_REFERENCE | -1 | 3", // 9, 11
      "HA | CLIENT_HAS_METADATA | STRING | -1 | 3", // 7, 9
  })
  @TestCaseName("[{index}] {method}: {params}")
  public void testOnServerWithRegionService(final HAStatus haStatus,
      final ClientMetadataStatus clientMetadataStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectedCalls) throws Exception {
    testAny(haStatus,
        clientMetadataStatus,
        ExecutionTarget.SERVER_REGION_SERVICE,
        functionIdentifierType,
        retryAttempts,
        expectedCalls);
  }

  @Test
  // TODO: 2 keys matching filter; redundancy 1; 2 retry attempts
  @Parameters({
      /*
       * haStatus | clientMetadataStatus | functionIdentifierType | retryAttempts | expectedCalls
       */
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | -1 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 0 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 2 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | -1 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | 0 | 1",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | 2 | 1",
      "NOT_HA | CLIENT_HAS_METADATA | OBJECT_REFERENCE | -1 | 1",
      "NOT_HA | CLIENT_HAS_METADATA | STRING | -1 | 1",

      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | -1 | 3",
      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 0 | 1",
      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 2 | 3",
      "HA | CLIENT_MISSING_METADATA | STRING | -1 | 3",
      "HA | CLIENT_MISSING_METADATA | STRING | 0 | 1",
      "HA | CLIENT_MISSING_METADATA | STRING | 2 | 3",
      "HA | CLIENT_HAS_METADATA | OBJECT_REFERENCE | -1 | 3",
      "HA | CLIENT_HAS_METADATA | STRING | -1 | 3",
  })
  @TestCaseName("[{index}] {method}: {params}")
  public void testOnServersWithRegionService(final HAStatus haStatus,
      final ClientMetadataStatus clientMetadataStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectedCalls) throws Exception {
    testAny(haStatus,
        clientMetadataStatus,
        ExecutionTarget.SERVERS_REGION_SERVICE,
        functionIdentifierType,
        retryAttempts,
        expectedCalls);
  }


  @Test
  // TODO: 2 keys matching filter; redundancy 1; 2 retry attempts
  @Parameters({
      /*
       * haStatus | clientMetadataStatus | functionIdentifierType | retryAttempts | expectedCalls
       */
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | -1 | 3",
      "NOT_HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | 0 | 3",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | -1 | 3",
      "NOT_HA | CLIENT_MISSING_METADATA | STRING | 0 | 3",

      "NOT_HA | CLIENT_HAS_METADATA             | OBJECT_REFERENCE       | -1           | 3",
      "NOT_HA | CLIENT_HAS_METADATA             | OBJECT_REFERENCE       | 0            | 3",
      "NOT_HA | CLIENT_HAS_METADATA             | OBJECT_REFERENCE       | 2            | 3",
      "NOT_HA | CLIENT_HAS_METADATA             | STRING       | -1            | 3",
      "NOT_HA | CLIENT_HAS_METADATA             | STRING       | 0            | 3",
      "NOT_HA | CLIENT_HAS_METADATA             | STRING       | 2            | 3",

      "HA | CLIENT_MISSING_METADATA | OBJECT_REFERENCE | -1 | 9", // Infinite
      "HA | CLIENT_MISSING_METADATA | STRING | -1 | 9", // Infinite

      "HA | CLIENT_HAS_METADATA | OBJECT_REFERENCE | -1 | 9", // Infinite
      "HA | CLIENT_HAS_METADATA | OBJECT_REFERENCE | 0 | 3", // Infinite
      "HA | CLIENT_HAS_METADATA | OBJECT_REFERENCE | 2 | 9", // Infinite
      "HA | CLIENT_HAS_METADATA | STRING | -1 | 9", // Infinite
      "HA | CLIENT_HAS_METADATA | STRING | 0 | 3", // Infinite
      "HA | CLIENT_HAS_METADATA | STRING | 2 | 9", // Infinite
  })
  @TestCaseName("[{index}] {method}: {params}")
  public void testOnRegion(final HAStatus haStatus,
      final ClientMetadataStatus clientMetadataStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectedCalls) throws Exception {
    testAny(haStatus,
        clientMetadataStatus,
        ExecutionTarget.REGION,
        functionIdentifierType,
        retryAttempts,
        expectedCalls);
  }


  @Test
  // TODO: 2 keys matching filter; redundancy 1; 2 retry attempts
  @Parameters({
      /*
       * haStatus | clientMetadataStatus | functionIdentifierType | retryAttempts | expectedCalls
       */
//      "NOT_HA | CLIENT_MISSING_METADATA  | OBJECT_REFERENCE | -1 | 1",
//      "NOT_HA | CLIENT_MISSING_METADATA  | OBJECT_REFERENCE | 0 | 1",
//      "NOT_HA | CLIENT_MISSING_METADATA  | OBJECT_REFERENCE | 2 | 1",
//      "NOT_HA | CLIENT_MISSING_METADATA  | STRING | -1 | 1",
//      "NOT_HA | CLIENT_MISSING_METADATA  | STRING | 0 | 1",
//      "NOT_HA | CLIENT_MISSING_METADATA  | STRING | 2 | 1",
//      "HA | CLIENT_MISSING_METADATA  | OBJECT_REFERENCE | -1 | 3", // Infinite
//      "HA | CLIENT_MISSING_METADATA  | OBJECT_REFERENCE | 0 | 1",
//      "HA | CLIENT_MISSING_METADATA  | OBJECT_REFERENCE | 2 | 3",
//      "HA | CLIENT_MISSING_METADATA  | STRING | -1 | 3", // Infinite
//      "HA | CLIENT_MISSING_METADATA  | STRING | 0 | 1",
//      "HA | CLIENT_MISSING_METADATA  | STRING | 2 | 3",
//
//      "HA | CLIENT_HAS_METADATA  | OBJECT_REFERENCE | -1 | 3", // Infinite
//      "HA | CLIENT_HAS_METADATA  | OBJECT_REFERENCE | 0 | 1", // Infinite
//      "HA | CLIENT_HAS_METADATA  | OBJECT_REFERENCE | 2 | 3", // Infinite
//      "HA | CLIENT_HAS_METADATA  | STRING | -1 | 3", // Infinite

      "HA | CLIENT_HAS_METADATA  | STRING | 3 | 4",

  })
  @TestCaseName("[{index}] {method}: {params}")
  public void testOnRegionWithSingleKeyFilter(final HAStatus haStatus,
      final ClientMetadataStatus clientMetadataStatus,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectedCalls) throws Exception {
    testAny(haStatus,
        clientMetadataStatus,
        ExecutionTarget.REGION_WITH_FILTER_1_KEY,
        functionIdentifierType,
        retryAttempts,
        expectedCalls);
  }


  public void testAny(
      final HAStatus haStatus,
      final ClientMetadataStatus clientMetadataStatus,
      final ExecutionTarget executionTarget,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts,
      final int expectedCalls) throws Exception {

    if (retryAttempts == PoolFactory.DEFAULT_RETRY_ATTEMPTS) {
      client = startClient(4);
    } else {
      client = startClient(4, retryAttempts);
    }

    final TheFunction function = new TheFunction(haStatus);

    server1.invoke(() -> {
      createServerRegion();
      registerFunctionIfNeeded(functionIdentifierType, function);
    });
    server2.invoke(() -> {
      createServerRegion();
      registerFunctionIfNeeded(functionIdentifierType, function);
    });
    server3.invoke(() -> {
      createServerRegion();
      registerFunctionIfNeeded(functionIdentifierType, function);
    });

    client.invoke(() -> {
      createClientRegion();
      registerFunctionIfNeeded(functionIdentifierType, function);
      ClientMetadataService cms =
          ((InternalCache) clusterStartupRule.getClientCache()).getClientMetadataService();
      cms.setMetadataStable(false);

      final Region<Object, Object> region =
          clusterStartupRule.getClientCache().getRegion(regionName);

      for (int i = 0; i < 3 /* numberOfEntries */; i++) {
        region.put("k" + i, "v" + i);
      }
    });

    client.invoke(() -> {
      ClientMetadataService cms = ((InternalCache) clusterStartupRule.getClientCache())
          .getClientMetadataService();

      if (clientMetadataStatus.equals(clientMetadataStatus.CLIENT_HAS_METADATA)) {
        final Region<Object, Object> region =
            clusterStartupRule.getClientCache().getRegion(regionName);
        cms.scheduleGetPRMetaData((InternalRegion) region, true);
        GeodeAwaitility.await("Awaiting ClientMetadataService.isMetadataStable()")
            .untilAsserted(() -> assertThat(cms.isMetadataStable()).isTrue());
      } else {
        cms.setMetadataStable(false);
      }
    });

    // TODO: remove this once this test is passing (it's here for debugging)
    IgnoredException.addIgnoredException(FunctionException.class.getName());

    AsyncInvocation clientExecuteAsync = client.invokeAsync(() -> {

      assertThat(executionTarget).isNotNull();

      final Execution<Integer, Long, List<Long>> execution = getExecutionTarget(executionTarget);

      ResultCollector<Long, List<Long>> resultCollector = null;

      // TODO - Add expected exception.
      try {
        switch (functionIdentifierType) {
          case STRING:
            resultCollector = execution.execute(function.getId());
            break;
          case OBJECT_REFERENCE:
            resultCollector = execution.execute(function);
            break;
          default:
            throw new TestException("unknown FunctionIdentifierType: " + functionIdentifierType);
        }
      } catch (final FunctionException e) {
        logger.info("#### Got FunctionException ", e);
        assertThat(e.getCause()).isInstanceOf(ServerConnectivityException.class);
      } catch (ServerConnectivityException sce) {
        assertThat(executionTarget).isInstanceOfAny(ExecutionTarget.SERVER.getClass(),
            ExecutionTarget.SERVERS.getClass());
      }

      if (resultCollector != null) {
        try {
          resultCollector.getResult();
        } catch (Exception ex) {
          System.out.println("#### Exception while collecting the result: " + ex.getMessage());
        }
      }
    });

    System.out.println("#### Number of functions executed on all servers :"
        + getNumberOfFunctionCalls(function.getId()));
    assertThat(getNumberOfFunctionCalls(function.getId())).isEqualTo(expectedCalls);

    clientExecuteAsync.join();
  }

  private Execution getExecutionTarget(ExecutionTarget executionTarget) {
    assertThat(executionTarget).isNotNull();

    final Execution<Integer, Long, List<Long>> execution;

    switch (executionTarget) {
      case REGION:
        execution =
            FunctionService.onRegion(clusterStartupRule.getClientCache().getRegion(regionName))
                .setArguments(200);
        break;
      case REGION_WITH_FILTER_1_KEY:
        final HashSet<String> filter = new HashSet<String>(Arrays.asList("k0"));
        execution =
            FunctionService.onRegion(clusterStartupRule.getClientCache().getRegion(regionName))
                .setArguments(200).withFilter(filter);
        break;
      case SERVER:
        execution = FunctionService.onServer(clusterStartupRule.getClientCache().getDefaultPool())
            .setArguments(200);
        break;
      case SERVER_REGION_SERVICE:
        execution = FunctionService
            .onServer(clusterStartupRule.getClientCache().getRegion(regionName).getRegionService())
            .setArguments(200);
        break;
      case SERVERS:
        execution = FunctionService.onServers(clusterStartupRule.getClientCache().getDefaultPool())
            .setArguments(200);
        break;
      case SERVERS_REGION_SERVICE:
        execution = FunctionService
            .onServer(clusterStartupRule.getClientCache().getRegion(regionName).getRegionService())
            .setArguments(200);
        break;
      default:
        throw new TestException("unknown ExecutionTarget: " + executionTarget);
    }
    return execution;
  }

  private void registerFunctionIfNeeded(
      final FunctionIdentifierType functionIdentifierType,
      final TheFunction function) {
    switch (functionIdentifierType) {
      case STRING:
        FunctionService.registerFunction(function);
        break;
      case OBJECT_REFERENCE:
        // no-op: no need to pre-register the fn if we will invoke it by reference
        break;
      default:
        throw new TestException("unknown FunctionIdentifierType: " + functionIdentifierType);
    }
  }

  private int getNumberOfFunctionCalls(final String functionId) {
    return getNumberOfFunctionCalls(server1, functionId) +
        getNumberOfFunctionCalls(server2, functionId) +
        getNumberOfFunctionCalls(server3, functionId);
  }

  private int getNumberOfFunctionCalls(final MemberVM vm, final String functionId) {
    return vm.invoke(() -> {
      final int numExecutions;
      final FunctionStats functionStats = FunctionStats.getFunctionStats(functionId);
      if (functionStats == null) {
        numExecutions = 0;
      } else {
        try {
          GeodeAwaitility.await("Awaiting functionStats.getFunctionExecutionsRunning().isZero()")
              .atMost(30 * 4, TimeUnit.SECONDS)
              .untilAsserted(
                  () -> assertThat(functionStats.getFunctionExecutionsRunning()).isZero());
        } catch (final Exception e) {
          logger.info(
              "#### numExecutions after timeout: " + functionStats.getFunctionExecutionCalls());
          throw e;
        }

        numExecutions = functionStats.getFunctionExecutionCalls();
      }
      return numExecutions;
    });
  }

  private ClientVM startClient(final int vmIndex) throws Exception {
    return clusterStartupRule.startClientVM(
        vmIndex,
        cacheRule -> cacheRule
            .withCacheSetup(fnTimeOut -> System
                .setProperty(DistributionConfig.GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT", "100"))
            .withLocatorConnection(locator.getPort()));
  }

  private ClientVM startClient(final int vmIndex, final int retryAttempts) throws Exception {
    return clusterStartupRule.startClientVM(
        vmIndex,
        cacheRule -> cacheRule
            .withCacheSetup(fnTimeOut -> System
                .setProperty(DistributionConfig.GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT", "100"))
            .withLocatorConnection(locator.getPort())
            .withCacheSetup(cf -> cf.setPoolRetryAttempts(retryAttempts)));
  }

  private MemberVM startServer(final int vmIndex) {
    return clusterStartupRule.startServerVM(
        vmIndex,
        cacheRule -> cacheRule
            .withConnectionToLocator(locator.getPort())
            .withProperty(SERIALIZABLE_OBJECT_FILTER,
                "org.apache.geode.internal.cache.execute.FunctionRetryDUnitTest*"));
  }

  private void createServerRegion() {
    final PartitionAttributesFactory<String, String> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(REDUNDANT_COPIES);
    paf.setTotalNumBuckets(TOTAL_NUM_BUCKETS);
    clusterStartupRule.getCache().createRegionFactory(PARTITION)
        .setPartitionAttributes(paf.create())
        .create(regionName);
  }

  private void createClientRegion() {
    clusterStartupRule.getClientCache().createClientRegionFactory(CACHING_PROXY).create(regionName);
  }

  private static class TheFunction implements Function<Integer> {

    private final HAStatus haStatus;

    public TheFunction(final HAStatus haStatus) {
      this.haStatus = haStatus;
    }

    @Override
    public void execute(final FunctionContext<Integer> context) {
      LogService.getLogger().info("#### Function Executing on server...");
      final int thinkTimeMillis = context.getArguments();
      final long elapsed = getElapsed(() -> Thread.sleep(thinkTimeMillis));
      context.getResultSender().lastResult(elapsed);
    }

    @Override
    public boolean isHA() {
      return haStatus == HAStatus.HA;
    }

    @FunctionalInterface
    private interface Thunk {
      void apply() throws InterruptedException;
    }

    private long getElapsed(final Thunk thunk) {
      final long start = System.currentTimeMillis();
      try {
        thunk.apply();
      } catch (final InterruptedException e) {
        // no-op
      }
      final long end = System.currentTimeMillis();
      return end - start;
    }
  }

}
