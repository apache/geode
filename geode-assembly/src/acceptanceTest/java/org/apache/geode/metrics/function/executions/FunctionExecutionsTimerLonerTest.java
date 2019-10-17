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
package org.apache.geode.metrics.function.executions;


import static java.io.File.pathSeparatorChar;
import static java.lang.Boolean.TRUE;
import static java.lang.String.valueOf;
import static java.util.stream.Collectors.toList;
import static org.apache.geode.test.compiler.ClassBuilder.writeJarFromClasses;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
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
 * Acceptance tests for function executions timer on a loner server with no locator
 */
public class FunctionExecutionsTimerLonerTest {

  private int serverPort;
  private int jmxRmiPort;
  private Path serverFolder;
  private Path serviceJarPath;
  private Path functionHelpersJarPath;
  private ClientCache clientCache;
  private Pool serverPool;
  private String connectCommand;
  private String startServerCommandWithStatsEnabled;
  private String startServerCommandWithStatsDisabled;
  private String startServerCommandWithTimeStatsDisabled;
  private String stopServerCommand;

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ServiceJarRule serviceJarRule = new ServiceJarRule();

  @Before
  public void setUp() throws IOException {
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    serverPort = ports[0];
    jmxRmiPort = ports[1];

    serverFolder = temporaryFolder.newFolder("server").toPath().toAbsolutePath();

    serviceJarPath = serviceJarRule.createJarFor("services.jar",
        MetricsPublishingService.class, SimpleMetricsPublishingService.class);

    functionHelpersJarPath =
        temporaryFolder.getRoot().toPath().resolve("function-helpers.jar").toAbsolutePath();
    writeJarFromClasses(functionHelpersJarPath.toFile(), FunctionToTimeWithResult.class,
        GetFunctionExecutionTimerValues.class, ExecutionsTimerValues.class);

    startServerCommandWithStatsEnabled = startServerCommand(false, true);
    startServerCommandWithStatsDisabled = startServerCommand(true, true);
    startServerCommandWithTimeStatsDisabled = startServerCommand(false, false);

    stopServerCommand = "stop server --dir=" + serverFolder;
    connectCommand = "connect --jmx-manager=localhost[" + jmxRmiPort + "]";
  }

  @After
  public void tearDown() {
    stopServer();
  }

  @Test
  public void timersNotRegisteredIfFunctionDeployedButNotExecuted() {
    startServerWithStatsEnabled();

    deployFunction(FunctionToTimeWithResult.class);

    Assertions.assertThat(getExecutionsTimerValuesFor(FunctionToTimeWithResult.ID))
        .as("Function executions timers on server")
        .isEmpty();
  }

  @Test
  public void timersRegisteredIfFunctionDeployedAndThenExecuted() {
    startServerWithStatsEnabled();
    deployFunction(FunctionToTimeWithResult.class);

    Duration functionDuration = Duration.ofSeconds(1);
    executeFunctionById(FunctionToTimeWithResult.ID, functionDuration);

    Assertions.assertThat(getExecutionsTimerValuesFor(FunctionToTimeWithResult.ID))
        .as("Function executions timers on server")
        .hasSize(2);
  }

  @Test
  public void timersRegisteredIfStatsDisabled() {
    startServerWithStatsDisabled();

    executeFunctionThatSucceeds(new FunctionToTimeWithResult(), Duration.ofMillis(1));

    Assertions.assertThat(getExecutionsTimerValuesFor(FunctionToTimeWithResult.ID))
        .as("Function executions timers on server")
        .hasSize(2);
  }

  @Test
  public void timersUnregisteredIfServerRestarts() {
    startServerWithStatsEnabled();
    executeFunctionThatSucceeds(new FunctionToTimeWithResult(), Duration.ofMillis(1));

    restartServer();

    Assertions.assertThat(getExecutionsTimerValuesFor(FunctionToTimeWithResult.ID))
        .as("Function executions timers on server")
        .isEmpty();
  }

  @Test
  public void successTimerRecordsCountAndTotalTimeIfFunctionSucceeds() {
    startServerWithStatsEnabled();
    FunctionToTimeWithResult function = new FunctionToTimeWithResult();
    Duration functionDuration = Duration.ofSeconds(1);
    executeFunctionThatSucceeds(function, functionDuration);

    ExecutionsTimerValues successTimerValues = getSuccessTimerValues(function.getId());

    assertThat(successTimerValues.count)
        .as("Successful function executions count")
        .isEqualTo(1);

    assertThat(successTimerValues.totalTime)
        .as("Successful function executions total time")
        .isGreaterThan(functionDuration.toNanos());
  }

  @Test
  public void successTimerRecordsCountAndTotalTimeIfTimeStatsDisabled() {
    startServerWithTimeStatsDisabled();
    FunctionToTimeWithResult function = new FunctionToTimeWithResult();
    Duration functionDuration = Duration.ofSeconds(1);
    executeFunctionThatSucceeds(function, functionDuration);

    ExecutionsTimerValues successTimerValues = getSuccessTimerValues(function.getId());

    assertThat(successTimerValues.count)
        .as("Successful function executions count")
        .isEqualTo(1);

    assertThat(successTimerValues.totalTime)
        .as("Successful function executions total time")
        .isGreaterThan(functionDuration.toNanos());
  }

  @Test
  public void failureTimerRecordsCountAndTotalTimeIfFunctionThrows() {
    startServerWithStatsEnabled();
    FunctionToTimeWithResult function = new FunctionToTimeWithResult();
    Duration functionDuration = Duration.ofSeconds(1);
    executeFunctionThatThrows(function, functionDuration);

    ExecutionsTimerValues failureTimerValues = getFailureTimerValues(function.getId());

    assertThat(failureTimerValues.count)
        .as("Failed function executions count")
        .isEqualTo(1);

    assertThat(failureTimerValues.totalTime)
        .as("Failed function executions total time")
        .isGreaterThan(functionDuration.toNanos());
  }

  private String startServerCommand(boolean statsDisabled, boolean enableTimeStatistics) {
    return String.join(" ",
        "start server",
        "--name=server",
        "--dir=" + serverFolder,
        "--server-port=" + serverPort,
        "--classpath=" + serviceJarPath + pathSeparatorChar + functionHelpersJarPath,
        "--enable-time-statistics=" + enableTimeStatistics,
        "--J=-Dgemfire.enable-cluster-config=true",
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Dgemfire.jmx-manager-port=" + jmxRmiPort,
        "--J=-Dgemfire.statsDisabled=" + statsDisabled);
  }

  private void createClientAndPool() {
    clientCache = new ClientCacheFactory()
        .addPoolServer("localhost", serverPort)
        .create();

    serverPool = PoolManager.createFactory()
        .addServer("localhost", serverPort)
        .create("server-pool");
  }

  private void closeClientAndPool() {
    serverPool.destroy();
    clientCache.close();
  }

  private void stopServer() {
    closeClientAndPool();
    gfshRule.execute(stopServerCommand);
  }

  private void startServerWithStatsEnabled() {
    startServer(startServerCommandWithStatsEnabled);
  }

  private void startServerWithStatsDisabled() {
    startServer(startServerCommandWithStatsDisabled);
  }

  private void startServerWithTimeStatsDisabled() {
    startServer(startServerCommandWithTimeStatsDisabled);
  }

  private void startServer(String command) {
    gfshRule.execute(command);
    createClientAndPool();
  }

  private void restartServer() {
    stopServer();
    startServerWithStatsEnabled();
  }

  @SuppressWarnings("SameParameterValue")
  private <T> void deployFunction(Class<? extends Function<T>> functionClass) {
    Path functionJarPath = temporaryFolder.getRoot().toPath()
        .resolve(functionClass.getSimpleName() + ".jar").toAbsolutePath();

    Throwable thrown =
        catchThrowable(() -> writeJarFromClasses(functionJarPath.toFile(), functionClass));

    assertThat(thrown)
        .as("Exception from writing function JAR")
        .isNull();

    String deployFunctionCommand = "deploy --jar=" + functionJarPath;
    gfshRule.execute(connectCommand, deployFunctionCommand);
  }

  private void executeFunctionThatSucceeds(Function<? super String[]> function, Duration duration) {
    Throwable thrown = catchThrowable(() -> executeFunction(function, duration, true));

    assertThat(thrown)
        .as("Exception from function expected to succeed")
        .isNull();
  }

  private void executeFunctionThatThrows(Function<? super String[]> function, Duration duration) {
    Throwable thrown = catchThrowable(() -> executeFunction(function, duration, false));

    assertThat(thrown)
        .withFailMessage("Expected function to throw but it did not")
        .isNotNull();
  }

  private void executeFunction(Function<? super String[]> function, Duration duration,
      boolean successful) {
    @SuppressWarnings("unchecked")
    Execution<String[], Object, List<Object>> execution =
        (Execution<String[], Object, List<Object>>) FunctionService.onServer(serverPool);

    execution
        .setArguments(new String[] {valueOf(duration.toMillis()), valueOf(successful)})
        .execute(function)
        .getResult();
  }

  @SuppressWarnings("SameParameterValue")
  private void executeFunctionById(String functionId, Duration duration) {
    @SuppressWarnings("unchecked")
    Execution<String[], Object, List<Object>> execution =
        (Execution<String[], Object, List<Object>>) FunctionService.onServer(serverPool);

    Throwable thrown = catchThrowable(() -> execution
        .setArguments(new String[] {valueOf(duration.toMillis()), TRUE.toString()})
        .execute(functionId)
        .getResult());

    assertThat(thrown)
        .as("Exception from function expected to succeed")
        .isNull();
  }

  private ExecutionsTimerValues getSuccessTimerValues(String functionId) {
    return getExecutionsTimerValues(functionId, true);
  }

  private ExecutionsTimerValues getFailureTimerValues(String functionId) {
    return getExecutionsTimerValues(functionId, false);
  }

  private ExecutionsTimerValues getExecutionsTimerValues(String functionId,
      boolean succeededTagValue) {
    List<ExecutionsTimerValues> executionsTimerValues = getExecutionsTimerValuesFor(functionId)
        .stream()
        .filter(v -> v.succeeded == succeededTagValue)
        .collect(toList());

    Assertions.assertThat(executionsTimerValues)
        .hasSize(1);

    return executionsTimerValues.get(0);
  }

  private List<ExecutionsTimerValues> getExecutionsTimerValuesFor(String functionId) {
    @SuppressWarnings("unchecked")
    Execution<Void, List<ExecutionsTimerValues>, List<List<ExecutionsTimerValues>>> functionExecution =
        (Execution<Void, List<ExecutionsTimerValues>, List<List<ExecutionsTimerValues>>>) FunctionService
            .onServer(serverPool);

    List<List<ExecutionsTimerValues>> results = functionExecution
        .execute(new GetFunctionExecutionTimerValues())
        .getResult();

    assertThat(results)
        .hasSize(1);

    return results.get(0).stream()
        .filter(v -> v.functionId.equals(functionId))
        .collect(toList());
  }
}
