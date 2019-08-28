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
package org.apache.geode.metrics.functionexecutions;


import static java.io.File.pathSeparatorChar;
import static java.lang.Boolean.TRUE;
import static java.util.stream.Collectors.toList;
import static org.apache.geode.test.compiler.ClassBuilder.writeJarFromClasses;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.metrics.MetricsPublishingService;
import org.apache.geode.metrics.SimpleMetricsPublishingService;
import org.apache.geode.rules.ServiceJarRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

/**
 * Acceptance tests for function executions timer on cluster with two servers and one locator
 */
public class FunctionExecutionsTimerClusterExecutionTest {

  private int locatorPort;
  private ClientCache clientCache;
  private Pool server1Pool;
  private Pool server2Pool;
  private Pool multiServerPool;
  private Region<Object, Object> replicateRegion;
  private Region<Object, Object> partitionRegion;

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ServiceJarRule serviceJarRule = new ServiceJarRule();

  @Before
  public void setUp() throws IOException {
    int[] availablePorts = AvailablePortHelper.getRandomAvailableTCPPorts(3);

    locatorPort = availablePorts[0];
    int server1Port = availablePorts[1];
    int server2Port = availablePorts[2];

    Path serviceJarPath = serviceJarRule.createJarFor("metrics-publishing-service.jar",
        MetricsPublishingService.class, SimpleMetricsPublishingService.class);

    Path functionsJarPath = temporaryFolder.getRoot().toPath()
        .resolve("functions.jar").toAbsolutePath();
    writeJarFromClasses(functionsJarPath.toFile(),
        GetFunctionExecutionTimerValues.class, FunctionToTime.class, ExecutionsTimerValues.class);

    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=" + "locator",
        "--dir=" + temporaryFolder.newFolder("locator").getAbsolutePath(),
        "--port=" + locatorPort);

    String server1Name = "server1";
    String server2Name = "server2";
    String startServer1Command =
        startServerCommand(server1Name, server1Port, serviceJarPath, functionsJarPath);
    String startServer2Command =
        startServerCommand(server2Name, server2Port, serviceJarPath, functionsJarPath);

    String replicateRegionName = "ReplicateRegion";
    String createReplicateRegionCommand = String.join(" ",
        "create region",
        "--type=REPLICATE",
        "--name=" + replicateRegionName);

    String partitionRegionName = "PartitionRegion";
    String createPartitionRegionCommand = String.join(" ",
        "create region",
        "--type=PARTITION",
        "--groups=" + server1Name,
        "--name=" + partitionRegionName);

    gfshRule.execute(startLocatorCommand, startServer1Command, startServer2Command,
        createReplicateRegionCommand, createPartitionRegionCommand);

    clientCache = new ClientCacheFactory().create();

    server1Pool = PoolManager.createFactory()
        .addServer("localhost", server1Port)
        .create("server1Pool");

    server2Pool = PoolManager.createFactory()
        .addServer("localhost", server2Port)
        .create("server2Pool");

    multiServerPool = PoolManager.createFactory()
        .addServer("localhost", server1Port)
        .addServer("localhost", server2Port)
        .create("multiServerPool");

    replicateRegion = clientCache
        .createClientRegionFactory(ClientRegionShortcut.PROXY)
        .setPoolName(multiServerPool.getName())
        .create(replicateRegionName);

    partitionRegion = clientCache
        .createClientRegionFactory(ClientRegionShortcut.PROXY)
        .setPoolName(server1Pool.getName())
        .create(partitionRegionName);
  }

  @After
  public void tearDown() {
    partitionRegion.close();
    replicateRegion.close();
    multiServerPool.destroy();
    server2Pool.destroy();
    server1Pool.destroy();
    clientCache.close();

    String connectToLocatorCommand = "connect --locator=localhost[" + locatorPort + "]";
    String shutdownCommand = "shutdown --include-locators=true";
    gfshRule.execute(connectToLocatorCommand, shutdownCommand);
  }

  @Test
  public void timersRecordCountAndTotalTime_ifFunctionExecutedOnReplicateRegion() {
    FunctionToTime function = new FunctionToTime();
    Duration functionDuration = Duration.ofSeconds(1);
    executeFunctionOnReplicateRegion(function, functionDuration);

    List<ExecutionsTimerValues> values = getAllExecutionsTimerValues(function.getId());

    assertThat(getAggregateCount(values))
        .as("Number of function executions across all servers")
        .isEqualTo(1);

    assertThat(getAggregateTotalTime(values))
        .as("Total time of function executions across all servers")
        .isBetween((double) functionDuration.toNanos(), ((double) functionDuration.toNanos()) * 2);
  }

  @Test
  public void timersRecordCountAndTotalTime_ifFunctionExecutedOnReplicateRegionMultipleTimes() {
    FunctionToTime function = new FunctionToTime();
    Duration functionDuration = Duration.ofSeconds(1);
    int numberOfExecutions = 10;

    for (int i = 0; i < numberOfExecutions; i++) {
      executeFunctionOnReplicateRegion(function, functionDuration);
    }

    List<ExecutionsTimerValues> values = getAllExecutionsTimerValues(function.getId());

    double expectedMinimumTotalTime = ((double) functionDuration.toNanos()) * numberOfExecutions;
    double expectedMaximumTotalTime = expectedMinimumTotalTime * 2;

    assertThat(getAggregateCount(values))
        .as("Number of function executions across all servers")
        .isEqualTo(numberOfExecutions);

    assertThat(getAggregateTotalTime(values))
        .as("Total time of function executions across all servers")
        .isBetween(expectedMinimumTotalTime, expectedMaximumTotalTime);
  }

  @Test
  public void timersRecordCountAndTotalTime_ifFunctionExecutedOnPartitionRegionMultipleTimes() {
    FunctionToTime function = new FunctionToTime();
    Duration functionDuration = Duration.ofSeconds(1);
    int numberOfExecutions = 10;

    for (int i = 0; i < numberOfExecutions; i++) {
      executeFunctionOnPartitionRegion(function, functionDuration);
    }

    List<ExecutionsTimerValues> server1Values = getServer1ExecutionsTimerValues(function.getId());
    List<ExecutionsTimerValues> server2Values = getServer2ExecutionsTimerValues(function.getId());

    double expectedMinimumTotalTime = ((double) functionDuration.toNanos()) * numberOfExecutions;
    double expectedMaximumTotalTime = expectedMinimumTotalTime * 2;

    assertThat(getAggregateCount(server1Values))
        .as("Number of function executions on server 1")
        .isEqualTo(numberOfExecutions);

    assertThat(getAggregateTotalTime(server1Values))
        .as("Total time of function executions on server 1")
        .isBetween(expectedMinimumTotalTime, expectedMaximumTotalTime);

    assertThat(getAggregateCount(server2Values))
        .as("Number of function executions on server 2")
        .isEqualTo(0);

    assertThat(getAggregateTotalTime(server2Values))
        .as("Total time of function executions on server 2")
        .isEqualTo(0);
  }

  private String startServerCommand(String serverName, int serverPort, Path serviceJarPath,
      Path functionsJarPath)
      throws IOException {
    return String.join(" ",
        "start server",
        "--name=" + serverName,
        "--groups=" + serverName,
        "--dir=" + temporaryFolder.newFolder(serverName).getAbsolutePath(),
        "--server-port=" + serverPort,
        "--locators=localhost[" + locatorPort + "]",
        "--classpath=" + serviceJarPath + pathSeparatorChar + functionsJarPath);
  }

  private void executeFunctionOnReplicateRegion(Function<? super String[]> function,
      Duration duration) {
    executeFunctionOnRegion(function, duration, replicateRegion);
  }

  private void executeFunctionOnPartitionRegion(Function<? super String[]> function,
      Duration duration) {
    executeFunctionOnRegion(function, duration, partitionRegion);
  }

  private void executeFunctionOnRegion(Function<? super String[]> function, Duration duration,
      Region<?, ?> region) {
    @SuppressWarnings("unchecked")
    Execution<String[], Object, List<Object>> execution =
        (Execution<String[], Object, List<Object>>) FunctionService.onRegion(region);

    execution
        .setArguments(new String[] {String.valueOf(duration.toMillis()), TRUE.toString()})
        .execute(function)
        .getResult();
  }

  private List<ExecutionsTimerValues> getServer1ExecutionsTimerValues(String functionId) {
    return getExecutionsTimerValuesFromPool(functionId, server1Pool);
  }

  private List<ExecutionsTimerValues> getServer2ExecutionsTimerValues(String functionId) {
    return getExecutionsTimerValuesFromPool(functionId, server2Pool);
  }

  private List<ExecutionsTimerValues> getAllExecutionsTimerValues(String functionId) {
    return getExecutionsTimerValuesFromPool(functionId, multiServerPool);
  }

  private List<ExecutionsTimerValues> getExecutionsTimerValuesFromPool(String functionId,
      Pool pool) {
    @SuppressWarnings("unchecked")
    Execution<Void, List<ExecutionsTimerValues>, List<List<ExecutionsTimerValues>>> functionExecution =
        (Execution<Void, List<ExecutionsTimerValues>, List<List<ExecutionsTimerValues>>>) FunctionService
            .onServers(pool);

    List<List<ExecutionsTimerValues>> timerValuesForEachServer = functionExecution
        .execute(new GetFunctionExecutionTimerValues())
        .getResult();

    return timerValuesForEachServer.stream()
        .flatMap(List::stream)
        .filter(v -> v.functionId.equals(functionId))
        .collect(toList());
  }

  private static Long getAggregateCount(List<ExecutionsTimerValues> values) {
    return values.stream().map(x -> x.count).reduce(0L, Long::sum);
  }

  private static Double getAggregateTotalTime(List<ExecutionsTimerValues> values) {
    return values.stream().map(x -> x.totalTime).reduce(0.0, Double::sum);
  }
}
