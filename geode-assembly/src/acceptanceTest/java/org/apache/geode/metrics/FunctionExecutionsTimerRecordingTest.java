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
package org.apache.geode.metrics;


import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.stream.Collectors.toList;
import static org.apache.geode.test.compiler.ClassBuilder.writeJarFromClasses;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

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
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.rules.ServiceJarRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class FunctionExecutionsTimerRecordingTest {

  private int locatorPort;
  private ClientCache clientCache;
  private Pool server1Pool;
  private Pool multiServerPool;
  private Region<Object, Object> replicateRegion;

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

    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=" + "locator",
        "--dir=" + temporaryFolder.newFolder("locator").getAbsolutePath(),
        "--port=" + locatorPort);

    String startServer1Command = startServerCommand("server1", server1Port, serviceJarPath);
    String startServer2Command = startServerCommand("server2", server2Port, serviceJarPath);

    String replicateRegionName = "region";
    String createRegionCommand = "create region --type=REPLICATE --name=" + replicateRegionName;

    Path functionsJarPath = temporaryFolder.getRoot().toPath()
        .resolve("functions.jar").toAbsolutePath();
    writeJarFromClasses(functionsJarPath.toFile(),
        GetFunctionExecutionTimerValues.class, FunctionToTime.class, ExecutionsTimerValues.class);

    String deployFunctionsCommand = "deploy --jar=" + functionsJarPath;

    gfshRule.execute(startLocatorCommand, startServer1Command, startServer2Command,
        createRegionCommand, deployFunctionsCommand);

    clientCache = new ClientCacheFactory().create();

    server1Pool = PoolManager.createFactory()
        .addServer("localhost", server1Port)
        .create("server1");

    multiServerPool = PoolManager.createFactory()
        .addServer("localhost", server1Port)
        .addServer("localhost", server2Port)
        .create("multiServerPool");

    replicateRegion = clientCache
        .createClientRegionFactory(ClientRegionShortcut.PROXY)
        .setPoolName(multiServerPool.getName())
        .create(replicateRegionName);
  }

  @After
  public void tearDown() {
    replicateRegion.close();
    multiServerPool.destroy();
    server1Pool.destroy();
    clientCache.close();

    String connectToLocatorCommand = "connect --locator=localhost[" + locatorPort + "]";
    String shutdownCommand = "shutdown --include-locators=true";
    gfshRule.execute(connectToLocatorCommand, shutdownCommand);
  }

  @Test
  public void timerRecordsCountAndTotalTimeIfFunctionSucceeds() {
    Duration functionDuration = Duration.ofSeconds(1);
    executeFunctionThatSucceeds(FunctionToTime.ID, functionDuration);

    List<ExecutionsTimerValues> values = getTimerValuesFromServer1().stream()
        .filter(v -> v.functionId.equals(FunctionToTime.ID))
        .filter(v -> v.succeeded)
        .collect(toList());

    assertThat(values)
        .hasSize(1);

    assertThat(values.get(0).count)
        .as("Function execution count")
        .isEqualTo(1);

    assertThat(values.get(0).totalTime)
        .as("Function execution total time")
        .isGreaterThan(functionDuration.toNanos());
  }

  @Test
  public void timerRecordsCountAndTotalTimeIfFunctionThrows() {
    Duration functionDuration = Duration.ofSeconds(1);
    executeFunctionThatThrows(FunctionToTime.ID, functionDuration);

    List<ExecutionsTimerValues> values = getTimerValuesFromServer1().stream()
        .filter(v -> v.functionId.equals(FunctionToTime.ID))
        .filter(v -> !v.succeeded)
        .collect(toList());

    assertThat(values)
        .hasSize(1);

    assertThat(values.get(0).count)
        .as("Function execution count")
        .isEqualTo(1);

    assertThat(values.get(0).totalTime)
        .as("Function execution total time")
        .isGreaterThan(functionDuration.toNanos());
  }

  @Test
  public void replicateRegionExecutionIncrementsTimerOnOnlyOneServer() {
    Duration functionDuration = Duration.ofSeconds(1);
    executeFunctionOnReplicateRegion(FunctionToTime.ID, functionDuration);

    List<ExecutionsTimerValues> values = getTimerValuesFromAllServers().stream()
        .flatMap(List::stream)
        .filter(v -> v.functionId.equals(FunctionToTime.ID))
        .filter(v -> v.succeeded)
        .collect(toList());

    long totalCount = values.stream().map(x -> x.count).reduce(0L, Long::sum);
    double totalTime = values.stream().map(x -> x.totalTime).reduce(0.0, Double::sum);

    assertThat(values)
        .as("Execution timer values for each server")
        .hasSize(2);

    assertThat(totalCount)
        .as("Number of function executions across all servers")
        .isEqualTo(1);

    assertThat(totalTime)
        .as("Total time of function executions across all servers")
        .isBetween((double) functionDuration.toNanos(), ((double) functionDuration.toNanos()) * 2);
  }

  @Test
  public void mutlipleReplicateRegionExecutionsIncrementsTimers() {
    Duration functionDuration = Duration.ofSeconds(1);
    int numberOfExecutions = 10;

    for (int i = 0; i < numberOfExecutions; i++) {
      executeFunctionOnReplicateRegion(FunctionToTime.ID, functionDuration);
    }

    List<ExecutionsTimerValues> values = getTimerValuesFromAllServers().stream()
        .flatMap(List::stream)
        .filter(v -> v.functionId.equals(FunctionToTime.ID))
        .filter(v -> v.succeeded)
        .collect(toList());

    long totalCount = values.stream().map(x -> x.count).reduce(0L, Long::sum);
    double totalTime = values.stream().map(x -> x.totalTime).reduce(0.0, Double::sum);

    assertThat(values)
        .as("Execution timer values for each server")
        .hasSize(2);

    assertThat(totalCount)
        .as("Number of function executions across all servers")
        .isEqualTo(numberOfExecutions);

    double expectedMinimumTotalTime = ((double) functionDuration.toNanos()) * numberOfExecutions;
    double expectedMaximumTotalTime = expectedMinimumTotalTime * 2;

    assertThat(totalTime)
        .as("Total time of function executions across all servers")
        .isBetween(expectedMinimumTotalTime, expectedMaximumTotalTime);
  }

  private String startServerCommand(String serverName, int serverPort, Path serviceJarPath)
      throws IOException {
    return String.join(" ",
        "start server",
        "--name=" + serverName,
        "--groups=" + serverName,
        "--dir=" + temporaryFolder.newFolder(serverName).getAbsolutePath(),
        "--server-port=" + serverPort,
        "--locators=localhost[" + locatorPort + "]",
        "--classpath=" + serviceJarPath);
  }

  @SuppressWarnings("SameParameterValue")
  private void executeFunctionThatSucceeds(String functionId, Duration duration) {
    @SuppressWarnings("unchecked")
    Execution<String[], String, List<String>> execution =
        (Execution<String[], String, List<String>>) FunctionService.onServer(server1Pool);

    Throwable thrown = catchThrowable(() -> execution
        .setArguments(new String[] {String.valueOf(duration.toMillis()), TRUE.toString()})
        .execute(functionId)
        .getResult());

    assertThat(thrown)
        .as("Exception from function expected to succeed")
        .isNull();
  }

  @SuppressWarnings("SameParameterValue")
  private void executeFunctionThatThrows(String functionId, Duration duration) {
    @SuppressWarnings("unchecked")
    Execution<String[], String, List<String>> execution =
        (Execution<String[], String, List<String>>) FunctionService.onServer(server1Pool);

    Throwable thrown = catchThrowable(() -> execution
        .setArguments(new String[] {String.valueOf(duration.toMillis()), FALSE.toString()})
        .execute(functionId)
        .getResult());

    assertThat(thrown)
        .withFailMessage("Expected function to throw but it did not")
        .isNotNull();
  }

  @SuppressWarnings("SameParameterValue")
  private void executeFunctionOnReplicateRegion(String functionId, Duration duration) {
    @SuppressWarnings("unchecked")
    Execution<String[], String, List<String>> execution =
        (Execution<String[], String, List<String>>) FunctionService.onRegion(replicateRegion);

    Throwable thrown = catchThrowable(() -> execution
        .setArguments(new String[] {String.valueOf(duration.toMillis()), TRUE.toString()})
        .execute(functionId)
        .getResult());

    assertThat(thrown)
        .as("Exception from function expected to succeed")
        .isNull();
  }

  private List<ExecutionsTimerValues> getTimerValuesFromServer1() {
    List<List<ExecutionsTimerValues>> values = getTimerValuesFromPool(server1Pool);

    assertThat(values)
        .hasSize(1);

    return values.get(0);
  }

  private List<List<ExecutionsTimerValues>> getTimerValuesFromAllServers() {
    return getTimerValuesFromPool(multiServerPool);
  }

  private List<List<ExecutionsTimerValues>> getTimerValuesFromPool(Pool serverPool) {
    @SuppressWarnings("unchecked")
    Execution<Void, List<ExecutionsTimerValues>, List<List<ExecutionsTimerValues>>> functionExecution =
        (Execution<Void, List<ExecutionsTimerValues>, List<List<ExecutionsTimerValues>>>) FunctionService
            .onServers(serverPool);

    return functionExecution
        .execute(GetFunctionExecutionTimerValues.ID)
        .getResult();
  }

  public static class FunctionToTime implements Function<String[]> {
    private static final String ID = "FunctionToTime";

    @Override
    public void execute(FunctionContext<String[]> context) {
      String[] arguments = context.getArguments();
      long timeToSleep = Long.parseLong(arguments[0]);
      boolean successful = Boolean.parseBoolean(arguments[1]);

      try {
        Thread.sleep(timeToSleep);
      } catch (InterruptedException ignored) {
      }

      if (successful) {
        context.getResultSender().lastResult("OK");
      } else {
        throw new FunctionException("FAIL");
      }
    }

    @Override
    public String getId() {
      return ID;
    }
  }
}
