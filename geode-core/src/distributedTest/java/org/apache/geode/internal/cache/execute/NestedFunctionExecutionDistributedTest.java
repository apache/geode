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

import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.cache.client.ClientRegionShortcut.PROXY;
import static org.apache.geode.distributed.internal.DistributionConfig.GEMFIRE_PREFIX;
import static org.apache.geode.distributed.internal.OperationExecutors.MAX_FE_THREADS;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.apache.geode.test.dunit.VM.getVM;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.CacheRule;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.FunctionServiceTest;
import org.apache.geode.test.junit.rules.ConcurrencyRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category(FunctionServiceTest.class)
@SuppressWarnings("serial")
public class NestedFunctionExecutionDistributedTest implements Serializable {

  private String uniqueName;
  private String regionName;
  private String hostName;

  private VM server;
  private VM client;

  public static final String CHILD_FUNCTION_EXECUTED = "childFunctionExecuted";

  @Rule
  public DistributedRule distributedRule = new DistributedRule();

  @Rule
  public CacheRule cacheRule = new CacheRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    server = getVM(0);
    client = getVM(1);

    uniqueName = getClass().getSimpleName() + "_" + testName.getMethodName();
    regionName = uniqueName + "_region";
    hostName = getHostName();

    int port = server.invoke(() -> createServerCache());

    client.invoke(() -> createClientCache(port, 30000));
  }

  private int createServerCache() throws IOException {
    Properties properties = new Properties();
    properties.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        "org.apache.geode.internal.cache.execute.NestedFunctionExecutionDistributedTest*");
    cacheRule.createCache(properties);
    CacheServer cacheServer = cacheRule.getCache().addCacheServer();
    cacheServer.setPort(0);
    // Make sure there are enough connections
    cacheServer
        .setMaxConnections(Math.max(CacheServer.DEFAULT_MAX_CONNECTIONS, MAX_FE_THREADS * 3));
    cacheServer.start();
    return cacheServer.getPort();
  }

  private void createClientCache(int port, int functionTimeout) {
    System.setProperty(GEMFIRE_PREFIX + "CLIENT_FUNCTION_TIMEOUT", String.valueOf(functionTimeout));
    clientCacheRule.createClientCache();
    PoolManager.createFactory().addServer(hostName, port).setRetryAttempts(0).create(uniqueName);
  }

  @Test
  public void testNestedFunctionExecutionOnReplicatedRegion() {
    // Create regions
    server.invoke(() -> createServerRegion(REPLICATE));
    client.invoke(() -> createClientRegion(PROXY));

    // Execute function
    client.invoke(() -> executeFunction(new ParentFunction(), MAX_FE_THREADS * 2));
  }

  private void createServerRegion(RegionShortcut shortcut) {
    cacheRule.getCache().createRegionFactory(shortcut).create(regionName);
  }

  private void createClientRegion(ClientRegionShortcut shortcut) {
    clientCacheRule.getClientCache().createClientRegionFactory(shortcut).create(regionName);
  }

  private void executeFunction(Function function, int numThreads) {
    ConcurrencyRule concurrencyRule = new ConcurrencyRule();
    try {
      for (int i = 0; i < numThreads; i++) {
        Callable<String> executeFunction = () -> {
          List<String> result =
              (List<String>) FunctionService
                  .onRegion(clientCacheRule.getClientCache().getRegion(regionName))
                  .execute(function)
                  .getResult();
          return result.get(0);
        };
        concurrencyRule.add(executeFunction).expectValue(CHILD_FUNCTION_EXECUTED);
      }
      concurrencyRule.executeInParallel();
    } finally {
      concurrencyRule.clear();
      concurrencyRule.stopThreadPool();
    }
  }

  private static class ParentFunction implements Function {

    private static final CyclicBarrier CHILD_FUNCTION_EXECUTION_BARRIER =
        new CyclicBarrier(MAX_FE_THREADS);

    @Override
    public void execute(FunctionContext context) {
      // Wait for MAX_FE_THREADS to be in use before continuing to execute the ChildFunction.
      try {
        CHILD_FUNCTION_EXECUTION_BARRIER.await();
      } catch (Exception e) {
        throw new FunctionException("Caught exception waiting for barrier: ", e);
      }
      List childFunctionResult =
          (List) FunctionService.onRegion(((RegionFunctionContext) context).getDataSet())
              .execute(new ChildFunction()).getResult();
      context.getResultSender().lastResult(childFunctionResult.get(0));
    }

    @Override
    public String getId() {
      return getClass().getName();
    }
  }

  private static class ChildFunction implements Function {

    @Override
    public void execute(FunctionContext context) {
      context.getResultSender().lastResult(CHILD_FUNCTION_EXECUTED);
    }

    @Override
    public String getId() {
      return getClass().getName();
    }
  }
}
