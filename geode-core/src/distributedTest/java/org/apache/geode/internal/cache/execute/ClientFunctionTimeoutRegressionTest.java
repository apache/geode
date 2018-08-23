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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.apache.geode.test.dunit.DistributedTestUtils.getLocatorPort;
import static org.apache.geode.test.dunit.Invoke.invokeInEveryVM;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.internal.InternalClientCache;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheServer;
import org.apache.geode.internal.cache.tier.ServerSideHandshake;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.FunctionServiceTest;

/**
 * Server should terminate client function execution when it times out. Client sends
 * CLIENT_FUNCTION_TIMEOUT to server when default is overridden by client.
 *
 * <p>
 * TRAC #51193: The function execution connection on the server is never terminated even if the
 * gemfire.CLIENT_FUNCTION_TIMEOUT property is set
 */
@Category(FunctionServiceTest.class)
@RunWith(JUnitParamsRunner.class)
@SuppressWarnings("serial")
public class ClientFunctionTimeoutRegressionTest implements Serializable {

  private static final int TOTAL_NUM_BUCKETS = 4;
  private static final int REDUNDANT_COPIES = 1;

  private static InternalCache serverCache;
  private static InternalClientCache clientCache;

  private String regionName;

  private VM server;
  private VM client;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    server = getVM(0);
    client = getVM(1);

    regionName = getClass().getSimpleName();
  }

  @After
  public void tearDown() throws Exception {
    invokeInEveryVM(() -> {
      if (clientCache != null) {
        clientCache.close();
      }
      if (serverCache != null) {
        serverCache.close();
      }
      clientCache = null;
      serverCache = null;
    });
  }

  @Test
  @Parameters({"SERVER,REPLICATE,0", "SERVER,REPLICATE,6000", "REGION,REPLICATE,0",
      "REGION,REPLICATE,6000", "REGION,PARTITION,0", "REGION,PARTITION,6000"})
  public void executeFunctionUsesClientTimeoutOnServer(final ExecutionTarget executionTarget,
      final RegionType regionType, final int timeout) {
    int port = server.invoke(() -> createServerCache(regionType));
    client.invoke(() -> createClientCache(client.getHost().getHostName(), port, timeout));

    client.invoke(() -> executeFunctionToVerifyClientTimeoutOnServer(executionTarget, timeout));
  }

  private void createClientCache(final String hostName, final int port, final int timeout) {
    if (timeout > 0) {
      System.setProperty(GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT", String.valueOf(timeout));
    }

    Properties config = new Properties();
    config.setProperty(LOCATORS, "");
    config.setProperty(MCAST_PORT, "0");

    ClientCacheFactory clientCacheFactory = new ClientCacheFactory(config);
    clientCacheFactory.addPoolServer(hostName, port);

    clientCache = (InternalClientCache) clientCacheFactory.create();

    ClientRegionFactory<String, String> clientRegionFactory =
        clientCache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY);

    clientRegionFactory.create(regionName);
  }

  private int createServerCache(final RegionType regionType) throws IOException {
    assertThat(regionType).isNotNull();

    Properties config = new Properties();
    config.setProperty(LOCATORS, "localhost[" + getLocatorPort() + "]");
    config.setProperty(SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.internal.cache.execute.ClientFunctionTimeoutRegressionTest*");

    serverCache = (InternalCache) new CacheFactory(config).create();

    RegionFactory<String, String> regionFactory;

    if (regionType == RegionType.PARTITION) {
      PartitionAttributesFactory<String, String> paf = new PartitionAttributesFactory<>();
      paf.setRedundantCopies(REDUNDANT_COPIES);
      paf.setTotalNumBuckets(TOTAL_NUM_BUCKETS);

      regionFactory = serverCache.createRegionFactory(RegionShortcut.PARTITION);
      regionFactory.setPartitionAttributes(paf.create());

    } else {
      regionFactory = serverCache.createRegionFactory(RegionShortcut.REPLICATE);
    }

    regionFactory.create(regionName);

    CacheServer server = serverCache.addCacheServer();
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private void executeFunctionToVerifyClientTimeoutOnServer(
      final ExecutionTarget functionServiceTarget, final int timeout) {
    assertThat(functionServiceTarget).isNotNull();

    Function<Integer> function = new CheckClientReadTimeout();
    FunctionService.registerFunction(function);
    Execution<Integer, Boolean, List<Boolean>> execution = null;

    if (functionServiceTarget == ExecutionTarget.REGION) {
      execution =
          FunctionService.onRegion(clientCache.getRegion(regionName)).setArguments(timeout);
    } else {
      execution = FunctionService.onServer(clientCache.getDefaultPool()).setArguments(timeout);
    }

    ResultCollector<Boolean, List<Boolean>> resultCollector = execution.execute(function);

    String description = "Server did not read client_function_timeout from client.";
    assertThat(resultCollector.getResult().get(0)).as(description).isTrue();
  }

  private enum RegionType {
    PARTITION, REPLICATE
  }

  private enum ExecutionTarget {
    REGION, SERVER
  }

  /**
   * Input: client function timeout <br>
   * Output: true if server has client timeout equal to the input
   */
  private static class CheckClientReadTimeout implements Function<Integer> {

    public CheckClientReadTimeout() {
      // nothing
    }

    @Override
    public void execute(FunctionContext<Integer> context) {
      boolean timeoutMatches = false;
      int expected = context.getArguments();

      InternalCacheServer cacheServer =
          (InternalCacheServer) context.getCache().getCacheServers().get(0);
      AcceptorImpl acceptor = (AcceptorImpl) cacheServer.getAcceptor();
      ServerConnection[] scs = acceptor.getAllServerConnectionList();

      for (ServerConnection sc : scs) {
        ServerSideHandshake hs = sc.getHandshake();
        if (hs != null && expected == hs.getClientReadTimeout()) {
          timeoutMatches = true;
        }
      }

      context.getResultSender().lastResult(timeoutMatches);
    }

    @Override
    public String getId() {
      return CheckClientReadTimeout.class.getName();
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }
}
