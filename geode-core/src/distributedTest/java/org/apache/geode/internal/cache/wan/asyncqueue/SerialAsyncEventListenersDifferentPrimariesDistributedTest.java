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
package org.apache.geode.internal.cache.wan.asyncqueue;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getHostName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import junitparams.Parameters;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import org.apache.geode.ForcedDisconnectException;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.asyncqueue.AsyncEvent;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.logging.internal.OSProcess;
import org.apache.geode.test.dunit.rules.ClientCacheRule;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.AEQTest;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;
import org.apache.geode.test.junit.runners.GeodeParamsRunner;

@Category(AEQTest.class)
@SuppressWarnings("serial")
@RunWith(GeodeParamsRunner.class)
public class SerialAsyncEventListenersDifferentPrimariesDistributedTest implements Serializable {

  private MemberVM locator;

  private MemberVM server1;

  private MemberVM server2;

  private MemberVM server3;

  @Rule
  public ClusterStartupRule clusterRule = new ClusterStartupRule();

  @Rule
  public ClientCacheRule clientCacheRule = new ClientCacheRule();

  @Rule
  public SerializableTestName testName = new SerializableTestName();

  @Rule
  public ExecutorServiceRule executorServiceRule = new ExecutorServiceRule();

  private boolean exceptionOccurred = false;

  private static final Random RANDOM = new Random();

  private static final int NUM_THREADS = 20;

  private static final long TIME_TO_RUN = 30000; // milliseconds

  private static final long TIME_TO_WAIT = 5; // minutes

  @After
  public void tearDown() {
    if (exceptionOccurred) {
      // An exception occurred. Since this could be a deadlock, dump the stacks and kill the
      // servers.
      addIgnoredException("Possible loss of quorum");
      addIgnoredException(ForcedDisconnectException.class);
      server1.invoke(() -> dumpStacks());
      server2.invoke(() -> dumpStacks());
      server3.invoke(() -> dumpStacks());
      clusterRule.crashVM(1);
      clusterRule.crashVM(2);
      clusterRule.crashVM(3);
    }
  }

  @Test
  @Parameters({"REPLICATE", "PARTITION", "PARTITION_REDUNDANT"})
  public void testMultithreadedFunctionExecutionsDoingRegionOperations(RegionShortcut shortcut)
      throws Exception {
    // This is a test for GEODE-6821.
    //
    // 3 servers each with:
    // - a function that performs a random region operation on the input region
    // - a replicated region on which the function is executed
    // - two regions each with a serial AEQ (the type of region varies between replicate, partition,
    // partition_redundant)
    //
    // 1 multi-threaded client that repeatedly executes the function for a specified length of time
    // with random region names and operations.
    //
    // This test will deadlock pretty much immediately if the shared P2P reader thread processes any
    // replication.

    // Start Locator
    locator = clusterRule.startLocatorVM(0);

    // Start servers
    int locatorPort = locator.getPort();
    server1 = clusterRule.startServerVM(1, s -> s.withConnectionToLocator(locatorPort));
    server2 = clusterRule.startServerVM(2, s -> s.withConnectionToLocator(locatorPort));
    server3 = clusterRule.startServerVM(3, s -> s.withConnectionToLocator(locatorPort));

    // Register Function in all servers
    Stream.of(server1, server2, server3).forEach(server -> server
        .invoke(() -> FunctionService.registerFunction(new RegionOperationsFunction())));

    // Create AEQ1 in all servers with server1 as primary
    String aeq1Id = testName.getMethodName() + "_1";
    Stream.of(server1, server2, server3).forEach(
        server -> server.invoke(() -> createAsyncEventQueue(aeq1Id, new TestAsyncEventListener())));

    // Create AEQ2 in all servers with server2 as primary
    String aeq2Id = testName.getMethodName() + "_2";
    Stream.of(server2, server1, server3).forEach(
        server -> server.invoke(() -> createAsyncEventQueue(aeq2Id, new TestAsyncEventListener())));

    // Create region the function is executed on in all servers
    String functionExecutionRegionName = testName.getMethodName() + "_functionExecutor";
    Stream.of(server1, server2, server3).forEach(
        server -> server.invoke(() -> createRegion(functionExecutionRegionName, REPLICATE)));

    // Create region attached to AEQ1 in all servers
    String aeg1RegionName = testName.getMethodName() + "_1";
    Stream.of(server1, server2, server3)
        .forEach(server -> server.invoke(() -> createRegion(aeg1RegionName, shortcut, aeq1Id)));

    // Create region attached to AEQ2 in all servers
    String aeg2RegionName = testName.getMethodName() + "_2";
    Stream.of(server1, server2, server3)
        .forEach(server -> server.invoke(() -> createRegion(aeg2RegionName, shortcut, aeq2Id)));

    // Create Client cache and proxy function region
    createClientCacheAndRegion(locatorPort, functionExecutionRegionName);

    // Launch threads to execute the Function from multiple threads in multiple members doing
    // multiple operations against multiple regions
    List<CompletableFuture<Void>> futures = launchTimedFunctionExecutionThreads(
        functionExecutionRegionName, aeg1RegionName, aeg2RegionName, NUM_THREADS, TIME_TO_RUN);

    // Wait for futures to complete. If they complete, the test is successful. If they timeout, the
    // test is unsuccessful.
    waitForFuturesToComplete(futures);
  }

  private void createAsyncEventQueue(String asyncEventQueueId,
      AsyncEventListener asyncEventListener) {
    ClusterStartupRule.getCache().createAsyncEventQueueFactory().setDispatcherThreads(1)
        .setParallel(false).create(asyncEventQueueId, asyncEventListener);
  }

  private void createRegion(String regionName, RegionShortcut shortcut) {
    createRegion(regionName, shortcut, null);
  }

  private void createRegion(String regionName, RegionShortcut shortcut, String asyncEventQueueId) {
    RegionFactory factory = ClusterStartupRule.getCache().createRegionFactory(shortcut);
    if (asyncEventQueueId != null) {
      factory.addAsyncEventQueueId(asyncEventQueueId);
    }
    factory.create(regionName);
  }

  private void createClientCacheAndRegion(int port, String regionName) {
    ClientCacheFactory clientCacheFactory =
        new ClientCacheFactory().addPoolLocator(getHostName(), port).setPoolRetryAttempts(0)
            .setPoolMinConnections(0);
    clientCacheRule.createClientCache(clientCacheFactory);
    clientCacheRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create(regionName);
  }

  private List<CompletableFuture<Void>> launchTimedFunctionExecutionThreads(
      String functionExecutionRegionName, String aeg1RegionName, String aeg2RegionName,
      int numThreads, long timeToRun) {
    String[] possibleOperationRegionNames = new String[] {aeg1RegionName, aeg2RegionName};
    String[] possibleOperations = new String[] {"put", "putAll", "destroy", "removeAll"};
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (int i = 0; i < numThreads; i++) {
      futures.add(executorServiceRule.runAsync(
          () -> executeFunctionTimed(RegionOperationsFunction.class.getSimpleName(),
              functionExecutionRegionName, possibleOperationRegionNames, possibleOperations,
              timeToRun)));
    }
    return futures;
  }

  private void executeFunctionTimed(String functionId, String regionName,
      String[] possibleOperationRegionNames, String[] possibleOperations, long timeToRun) {
    long endRun = System.currentTimeMillis() + timeToRun;
    while (System.currentTimeMillis() < endRun) {
      String operationRegionName =
          possibleOperationRegionNames[RANDOM.nextInt(possibleOperationRegionNames.length)];
      String operation = possibleOperations[RANDOM.nextInt(possibleOperations.length)];
      FunctionService.onRegion(clientCacheRule.getClientCache().getRegion(regionName))
          .setArguments(new String[] {operationRegionName, operation}).execute(functionId)
          .getResult();
    }
  }

  private void waitForFuturesToComplete(List<CompletableFuture<Void>> futures) throws Exception {
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()])).get(
          TIME_TO_WAIT,
          MINUTES);
    } catch (Exception e) {
      // An exception occurred. Set the boolean to cause the servers to be killed in tearDown.
      exceptionOccurred = true;
      throw e;
    }
  }

  private void dumpStacks() {
    OSProcess.printStacks(0);
  }

  public static class RegionOperationsFunction implements Function {

    private final Cache cache;

    private static final int NUM_ENTRIES = 10000;

    public RegionOperationsFunction() {
      cache = CacheFactory.getAnyInstance();
    }

    @Override
    public void execute(FunctionContext context) {
      String[] args = (String[]) context.getArguments();
      String regionName = args[0];
      String operation = args[1];
      Region region = cache.getRegion(regionName);
      switch (operation) {
        case "put":
          doPut(region);
          break;
        case "putAll":
          doPutAll(region);
          break;
        case "destroy":
          doDestroy(region);
          break;
        case "removeAll":
          doRemoveAll(region);
          break;
      }
      context.getResultSender().lastResult(true);
    }

    private void doPut(Region region) {
      region.put(RANDOM.nextInt(NUM_ENTRIES), "value");
    }

    private void doPutAll(Region region) {
      Map events = new HashMap();
      events.put(RANDOM.nextInt(NUM_ENTRIES), "value");
      events.put(RANDOM.nextInt(NUM_ENTRIES), "value");
      events.put(RANDOM.nextInt(NUM_ENTRIES), "value");
      events.put(RANDOM.nextInt(NUM_ENTRIES), "value");
      events.put(RANDOM.nextInt(NUM_ENTRIES), "value");
      region.putAll(events);
    }

    private void doDestroy(Region region) {
      try {
        region.destroy(RANDOM.nextInt(NUM_ENTRIES));
      } catch (EntryNotFoundException ignored) {
      }
    }

    private void doRemoveAll(Region region) {
      List keys = new ArrayList();
      keys.add(RANDOM.nextInt(NUM_ENTRIES));
      keys.add(RANDOM.nextInt(NUM_ENTRIES));
      keys.add(RANDOM.nextInt(NUM_ENTRIES));
      keys.add(RANDOM.nextInt(NUM_ENTRIES));
      keys.add(RANDOM.nextInt(NUM_ENTRIES));
      region.removeAll(keys);
    }

    @Override
    public String getId() {
      return getClass().getSimpleName();
    }
  }

  private static class TestAsyncEventListener implements AsyncEventListener {

    @Override
    public boolean processEvents(List<AsyncEvent> events) {
      return true;
    }
  }
}
