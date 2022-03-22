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
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category({WanTest.class})
@RunWith(GeodeParamsRunner.class)
public class GenericDUnitTest implements Serializable {

  private static final int MAX_THREADS = 2;

  private static final String regionName = "GenericDUnitTest";

  @ClassRule
  public static final ClusterStartupRule clusterStartupRule = new ClusterStartupRule(5);

  @Rule
  public DistributedRule distributedRule = new DistributedRule(6);

  MemberVM locator;
  MemberVM server1;
  MemberVM server2;
  MemberVM server3;
  MemberVM server4;
  ClientVM client;

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Before
  public void setUp() throws Exception {
    locator = clusterStartupRule.startLocatorVM(0);
    server1 = startServer(1, MAX_THREADS);
  }

  @After
  public void tearDown() throws Exception {
    IgnoredException.removeAllExpectedExceptions();
  }

  @Test
  @Parameters({"1", "2", "3", "4"})
  @TestCaseName("{method}(servers:{params})")
  public void testSuccessfulExecution(int numberOfServers) throws Exception {
    addIgnoredException("IOException while sending the result chunk to client");
    // addIgnoredException("Uncaught exception");

    if (numberOfServers > 1) {
      server2 = startServer(2, MAX_THREADS);
      if (numberOfServers > 2) {
        server3 = startServer(3, MAX_THREADS);
        if (numberOfServers > 3) {
          server4 = startServer(4, MAX_THREADS);
        }
      }
    }

    List<MemberVM> serversInA =
        Arrays.asList(server1, server2, server3, server4).subList(0, numberOfServers);

    // Set client connect-timeout to a very high value so that if there are no ServerConnection
    // threads available the test will time-out before the client times-out.
    int connectTimeout = (int) GeodeAwaitility.getTimeout().toMillis() * 2;

    client = startClient(5, 0, connectTimeout);

    Function function = new TestFunction();

    for (MemberVM memberVM : serversInA) {
      createServerRegionAndRegisterFunction(memberVM, 1, function);
    }

    client.invoke(() -> createClientRegion());

    int executions = (numberOfServers * MAX_THREADS) + 1;
    int functionTimeoutSecs = 2;
    for (int i = 0; i < executions; i++) {
      int inv = i;
      await().untilAsserted(() -> assertThatThrownBy(() -> client
          .invoke(() -> executeSlowFunctionOnRegionNoFilter(regionName, functionTimeoutSecs, inv)))
              .getCause().getCause().isInstanceOf(ServerConnectivityException.class));
    }
  }

  private Object executeSlowFunctionOnRegionNoFilter(String regionName, int functionTimeoutSecs,
      int invocation) {
    Function function = new TestFunction();
    FunctionService.registerFunction(function);
    final Region<Object, Object> region =
        ClusterStartupRule.getClientCache().getRegion(regionName);

    Execution execution = FunctionService.onRegion(region);

    ResultCollector resultCollector;
    int entries = 4;
    int waitBetweenEntriesMs = 5000;
    Object[] args = {invocation, entries, waitBetweenEntriesMs};
    resultCollector =
        execution.setArguments(args).execute(function.getId(), functionTimeoutSecs,
            TimeUnit.SECONDS);
    Object result = resultCollector.getResult();
    return result;
  }

  private void createServerRegion(int redundantCopies) {
    final PartitionAttributesFactory<String, String> paf = new PartitionAttributesFactory<>();
    paf.setRedundantCopies(redundantCopies);
    ClusterStartupRule.getCache().createRegionFactory(PARTITION)
        .setPartitionAttributes(paf.create())
        .create(regionName);
  }

  private void createServerRegionAndRegisterFunction(MemberVM server, int redundantCopies,
      final Function function) {
    server.invoke(() -> {
      createServerRegion(redundantCopies);
      FunctionService.registerFunction(function);
    });
  }

  private ClientVM startClient(final int vmIndex, final int retryAttempts, final int connectTimeout)
      throws Exception {
    return clusterStartupRule.startClientVM(
        vmIndex,
        cacheRule -> cacheRule
            .withLocatorConnection(locator.getPort())
            .withCacheSetup(cf -> cf.setPoolRetryAttempts(retryAttempts)
                .setPoolPRSingleHopEnabled(false)
                .setPoolSocketConnectTimeout(connectTimeout)));
  }

  private MemberVM startServer(final int vmIndex, int maxThreads) {
    return clusterStartupRule.startServerVM(
        vmIndex,
        cacheRule -> cacheRule
            .withProperty(SERIALIZABLE_OBJECT_FILTER,
                "org.apache.geode.internal.cache.execute.GenericDUnitTest*")
            .withMaxThreads(maxThreads)
            .withConnectionToLocator(locator.getPort()));
  }

  private void createClientRegion() {
    ClusterStartupRule.getClientCache().createClientRegionFactory(CACHING_PROXY).create(regionName);
  }

  static class TestFunction implements Function<Object[]>, Serializable {
    public TestFunction() {
      super();
    }

    @Override
    public void execute(FunctionContext<Object[]> context) {
      final Object[] args = context.getArguments();
      if (args.length < 3) {
        throw new IllegalStateException(
            "Arguments length does not match required length.");
      }
      Integer invocation = (Integer) args[0];
      Integer entries = (Integer) args[1];
      Integer waitBetweenEntriesMs = (Integer) args[2];
      for (int i = 0; i < entries; i++) {
        logger.info("toberal inv: {} before sleeping. i: {}", invocation, i, new Exception());
        try {
          Thread.sleep(waitBetweenEntriesMs);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        logger.info("toberal inv: {} before sending result. i: {}", invocation, i, new Exception());
        context.getResultSender().sendResult(i);
        logger.info("toberal inv: {} sent result. i: {}", invocation, i, new Exception());
      }
      logger.info("toberal inv: {} before returning last result. resultSender: {}", invocation,
          context.getResultSender());
      context.getResultSender().lastResult(entries);
      logger.info("toberal inv: {} after returning last result", invocation);
    }

    public static final String ID = TestFunction.class.getName();

    private static final Logger logger = LogService.getLogger();

    @Override
    public String getId() {
      return ID;
    }

    @Override
    public boolean hasResult() {
      return true;
    }

    @Override
    public boolean isHA() {
      return false;
    }

    @Override
    public boolean optimizeForWrite() {
      return false;
    }
  }
}
