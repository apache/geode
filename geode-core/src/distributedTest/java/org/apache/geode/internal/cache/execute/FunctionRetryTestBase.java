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

import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
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
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.execute.metrics.FunctionStats;
import org.apache.geode.internal.cache.execute.metrics.FunctionStatsManager;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.util.internal.GeodeGlossary;

public class FunctionRetryTestBase implements Serializable {

  @ClassRule
  public static final ClusterStartupRule clusterStartupRule = new ClusterStartupRule(5);

  private static final int TOTAL_NUM_BUCKETS = 3;
  private static final int REDUNDANT_COPIES = 0;
  private static final String regionName = "FunctionOnRegionRetryDUnitTest";

  @Rule
  public DistributedRule distributedRule = new DistributedRule(5);

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  protected enum HAStatus {
    NOT_HA, HA
  }

  protected enum ExecutionTarget {
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

  protected enum FunctionIdentifierType {
    STRING, OBJECT_REFERENCE
  }

  protected enum ClientMetadataStatus {
    CLIENT_HAS_METADATA, CLIENT_MISSING_METADATA
  }

  MemberVM locator;
  MemberVM server1;
  MemberVM server2;
  MemberVM server3;
  ClientVM client;
  Logger logger = LogService.getLogger();

  @Before
  public void setUp() throws Exception {
    locator = clusterStartupRule.startLocatorVM(0);
    server1 = startServer(1);
    server2 = startServer(2);
    server3 = startServer(3);
  }

  @After
  public void tearDown() throws Exception {
    IgnoredException.removeAllExpectedExceptions();
  }

  public Function testFunctionRetry(
      final HAStatus haStatus,
      final ClientMetadataStatus clientMetadataStatus,
      final ExecutionTarget executionTarget,
      final FunctionIdentifierType functionIdentifierType,
      final int retryAttempts) throws Exception {

    if (retryAttempts == PoolFactory.DEFAULT_RETRY_ATTEMPTS) {
      client = startClient(4);
    } else {
      client = startClient(4, retryAttempts);
    }

    TheFunction function = new TheFunction(haStatus);

    createServerRegionAndRegisterFunction(functionIdentifierType, function);

    createClientRegionAndInitializeMetaDataState();

    setClientMetaDataStatus(clientMetadataStatus);

    IgnoredException.addIgnoredException(FunctionException.class.getName());

    AsyncInvocation clientExecuteAsync = client.invokeAsync(() -> {

      assertThat(executionTarget).isNotNull();

      final Execution<Integer, Long, List<Long>> execution = getExecutionTarget(executionTarget);

      ((AbstractExecution) execution).removeFunctionAttributes(function.getId());
      ResultCollector<Long, List<Long>> resultCollector = null;

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
        assertThat(e.getCause()).isInstanceOf(ServerConnectivityException.class);
      } catch (ServerConnectivityException sce) {
        assertThat(executionTarget).isInstanceOfAny(ExecutionTarget.SERVER.getClass(),
            ExecutionTarget.SERVERS.getClass());
      }

      if (resultCollector != null) {
        try {
          resultCollector.getResult();
        } catch (Exception ex) {
          logger.info("Exception while collecting the result: " + ex.getMessage());
        }
      }
    });

    clientExecuteAsync.join();

    return function;

  }

  private void setClientMetaDataStatus(final ClientMetadataStatus clientMetadataStatus) {
    client.invoke(() -> {
      ClientMetadataService cms = ((InternalCache) ClusterStartupRule.getClientCache())
          .getClientMetadataService();

      if (clientMetadataStatus.equals(ClientMetadataStatus.CLIENT_HAS_METADATA)) {
        final Region<Object, Object> region =
            ClusterStartupRule.getClientCache().getRegion(regionName);
        cms.scheduleGetPRMetaData((InternalRegion) region, true);
        GeodeAwaitility.await("Awaiting ClientMetadataService.isMetadataStable()")
            .untilAsserted(() -> assertThat(cms.isMetadataStable()).isTrue());
      } else {
        cms.setMetadataStable(false);
      }
    });
  }

  private void createClientRegionAndInitializeMetaDataState() {
    client.invoke(() -> {
      createClientRegion();
      // registerFunctionIfNeeded(functionIdentifierType, function);
      ClientMetadataService cms =
          ((InternalCache) ClusterStartupRule.getClientCache()).getClientMetadataService();
      cms.setMetadataStable(false);

      final Region<Object, Object> region =
          ClusterStartupRule.getClientCache().getRegion(regionName);

      for (int i = 0; i < 3 /* numberOfEntries */; i++) {
        region.put("k" + i, "v" + i);
      }
    });
  }

  private void createServerRegionAndRegisterFunction(
      final FunctionIdentifierType functionIdentifierType,
      final TheFunction function) {
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
  }

  private Execution getExecutionTarget(ExecutionTarget executionTarget) {
    assertThat(executionTarget).isNotNull();

    final Execution<Integer, Long, List<Long>> execution;

    switch (executionTarget) {
      case REGION:
        execution =
            FunctionService.onRegion(ClusterStartupRule.getClientCache().getRegion(regionName))
                .setArguments(200);
        break;
      case REGION_WITH_FILTER_1_KEY:
        final HashSet<String> filter = new HashSet<>(Arrays.asList("k0"));
        execution =
            FunctionService.onRegion(ClusterStartupRule.getClientCache().getRegion(regionName))
                .setArguments(200).withFilter(filter);
        break;
      case SERVER:
        execution = FunctionService.onServer(ClusterStartupRule.getClientCache().getDefaultPool())
            .setArguments(200);
        break;
      case SERVER_REGION_SERVICE:
        execution = FunctionService
            .onServer(ClusterStartupRule.getClientCache().getRegion(regionName).getRegionService())
            .setArguments(200);
        break;
      case SERVERS:
        execution = FunctionService.onServers(ClusterStartupRule.getClientCache().getDefaultPool())
            .setArguments(200);
        break;
      case SERVERS_REGION_SERVICE:
        execution = FunctionService
            .onServers(ClusterStartupRule.getClientCache().getRegion(regionName).getRegionService())
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

  int getNumberOfFunctionCalls(final String functionId) {
    return getNumberOfFunctionCalls(server1, functionId) +
        getNumberOfFunctionCalls(server2, functionId) +
        getNumberOfFunctionCalls(server3, functionId);
  }

  private int getNumberOfFunctionCalls(final MemberVM vm, final String functionId) {
    return vm.invoke(() -> {
      final int numExecutions;
      final FunctionStats functionStats = FunctionStatsManager.getFunctionStats(functionId);
      if (functionStats == null) {
        numExecutions = 0;
      } else {
        try {
          GeodeAwaitility.await("Awaiting functionStats.getFunctionExecutionsRunning().isZero()")
              .atMost(30 * 4, TimeUnit.SECONDS)
              .untilAsserted(
                  () -> assertThat(functionStats.getFunctionExecutionsRunning()).isZero());
        } catch (final Exception e) {
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
                .setProperty(GeodeGlossary.GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT", "20"))
            .withLocatorConnection(locator.getPort()));
  }

  private ClientVM startClient(final int vmIndex, final int retryAttempts) throws Exception {
    return clusterStartupRule.startClientVM(
        vmIndex,
        cacheRule -> cacheRule
            .withCacheSetup(fnTimeOut -> System
                .setProperty(GeodeGlossary.GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT", "20"))
            .withLocatorConnection(locator.getPort())
            .withCacheSetup(cf -> cf.setPoolRetryAttempts(retryAttempts)));
  }

  private MemberVM startServer(final int vmIndex) {
    return clusterStartupRule.startServerVM(
        vmIndex,
        cacheRule -> cacheRule
            .withConnectionToLocator(locator.getPort())
            .withProperty(SERIALIZABLE_OBJECT_FILTER,
                "org.apache.geode.internal.cache.execute.FunctionRetryTestBase*"));
  }

  private void createServerRegion() {
    final PartitionAttributesFactory<String, String> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(REDUNDANT_COPIES);
    paf.setTotalNumBuckets(TOTAL_NUM_BUCKETS);
    ClusterStartupRule.getCache().createRegionFactory(PARTITION)
        .setPartitionAttributes(paf.create())
        .create(regionName);
  }

  private void createClientRegion() {
    ClusterStartupRule.getClientCache().createClientRegionFactory(CACHING_PROXY).create(regionName);
  }


  static class TheFunction implements Function<Integer> {
    private final boolean haStatus;

    public TheFunction(final HAStatus haStatus) {
      this.haStatus = (haStatus == HAStatus.HA);
    }

    @Override
    public void execute(final FunctionContext<Integer> context) {
      LogService.getLogger().info("Function Executing on server.");
      final int thinkTimeMillis = context.getArguments();
      try {
        Thread.sleep(thinkTimeMillis);
      } catch (Exception ignored) {

      }
      context.getResultSender().lastResult(thinkTimeMillis);
    }

    @Override
    public boolean isHA() {
      return haStatus;
    }
  }

}
