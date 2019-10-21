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
import static java.util.stream.Collectors.toList;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.compiler.ClassBuilder.writeJarFromClasses;
import static org.assertj.core.api.Assertions.assertThat;

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

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.metrics.MetricsPublishingService;
import org.apache.geode.metrics.SimpleMetricsPublishingService;
import org.apache.geode.rules.ServiceJarRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

/**
 * Acceptance tests for function executions timer for functions with hasResult=false
 */
public class FunctionExecutionsTimerNoResultTest {

  private int locatorPort;
  private ClientCache clientCache;
  private Region<Object, Object> replicateRegion;
  private Region<Object, Object> partitionRegion;
  private FunctionToTimeWithoutResult functionWithNoResult;
  private Duration functionDuration;

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ServiceJarRule serviceJarRule = new ServiceJarRule();

  @Before
  public void setUp() throws IOException {
    int[] availablePorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    locatorPort = availablePorts[0];
    int serverPort = availablePorts[1];

    Path serviceJarPath = serviceJarRule.createJarFor("metrics-publishing-service.jar",
        MetricsPublishingService.class, SimpleMetricsPublishingService.class);

    Path functionsJarPath = temporaryFolder.getRoot().toPath()
        .resolve("functions.jar").toAbsolutePath();
    writeJarFromClasses(functionsJarPath.toFile(),
        GetFunctionExecutionTimerValues.class, FunctionToTimeWithoutResult.class,
        ExecutionsTimerValues.class);

    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=" + "locator",
        "--dir=" + temporaryFolder.newFolder("locator").getAbsolutePath(),
        "--port=" + locatorPort);

    String serverName = "server1";
    String startServerCommand =
        startServerCommand(serverName, serverPort, serviceJarPath, functionsJarPath);

    String replicateRegionName = "ReplicateRegion";
    String createReplicateRegionCommand = String.join(" ",
        "create region",
        "--type=REPLICATE",
        "--name=" + replicateRegionName);

    String partitionRegionName = "PartitionRegion";
    String createPartitionRegionCommand = String.join(" ",
        "create region",
        "--type=PARTITION",
        "--name=" + partitionRegionName);

    gfshRule.execute(startLocatorCommand, startServerCommand, createReplicateRegionCommand,
        createPartitionRegionCommand);

    clientCache = new ClientCacheFactory()
        .addPoolLocator("localhost", locatorPort)
        .create();

    replicateRegion = clientCache
        .createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create(replicateRegionName);

    partitionRegion = clientCache
        .createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create(partitionRegionName);

    functionWithNoResult = new FunctionToTimeWithoutResult();
    functionDuration = Duration.ofSeconds(1);
  }

  @After
  public void tearDown() {
    partitionRegion.close();
    replicateRegion.close();
    clientCache.close();

    String connectToLocatorCommand = "connect --locator=localhost[" + locatorPort + "]";
    String shutdownCommand = "shutdown --include-locators=true";
    gfshRule.execute(connectToLocatorCommand, shutdownCommand);
  }

  @Test
  public void successTimerRecordsCountAndTotalTime_ifFunctionSucceeds_whenExecutedOnReplicateRegion() {
    executeFunctionThatSucceeds(onRegion(replicateRegion));

    await().untilAsserted(() -> {
      ExecutionsTimerValues value = successTimerValue();

      assertThat(value.count)
          .as("Number of successful executions")
          .isEqualTo(1);

      assertThat(value.totalTime)
          .as("Total time of successful executions")
          .isGreaterThan(functionDuration.toNanos());
    });
  }

  @Test
  public void failureTimerRecordsCountAndTotalTime_ifFunctionThrows_whenExecutedOnReplicateRegion() {
    executeFunctionThatThrows(onRegion(replicateRegion));

    await().untilAsserted(() -> {
      ExecutionsTimerValues value = failureTimerValue();

      assertThat(value.count)
          .as("Number of failed executions")
          .isEqualTo(1);

      assertThat(value.totalTime)
          .as("Total time of failed executions")
          .isGreaterThan(functionDuration.toNanos());
    });
  }

  @Test
  public void successTimerRecordsCountAndTotalTime_ifFunctionSucceeds_whenExecutedOnPartitionRegion() {
    executeFunctionThatSucceeds(onRegion(partitionRegion));

    await().untilAsserted(() -> {
      ExecutionsTimerValues value = successTimerValue();

      assertThat(value.count)
          .as("Number of successful executions")
          .isEqualTo(1);

      assertThat(value.totalTime)
          .as("Total time of successful executions")
          .isGreaterThan(functionDuration.toNanos());
    });
  }

  @Test
  public void failureTimerRecordsCountAndTotalTime_ifFunctionThrows_whenExecutedOnPartitionRegion() {
    executeFunctionThatThrows(onRegion(partitionRegion));

    await().untilAsserted(() -> {
      ExecutionsTimerValues value = failureTimerValue();

      Assertions.assertThat(value.count)
          .as("Number of failed executions")
          .isEqualTo(1);

      Assertions.assertThat(value.totalTime)
          .as("Total time of failed executions")
          .isGreaterThan(functionDuration.toNanos());
    });
  }

  @Test
  public void successTimerRecordsCountAndTotalTime_ifFunctionSucceeds_whenExecutedOnServer() {
    executeFunctionThatSucceeds(onServer(clientCache));

    await().untilAsserted(() -> {
      ExecutionsTimerValues value = successTimerValue();

      assertThat(value.count)
          .as("Number of successful executions")
          .isEqualTo(1);

      assertThat(value.totalTime)
          .as("Total time of successful executions")
          .isGreaterThan(functionDuration.toNanos());
    });
  }

  @Test
  public void failureTimerRecordsCountAndTotalTime_ifFunctionThrows_whenExecutedOnServer() {
    executeFunctionThatThrows(onServer(clientCache));

    await().untilAsserted(() -> {
      ExecutionsTimerValues value = failureTimerValue();

      assertThat(value.count)
          .as("Number of failed executions")
          .isEqualTo(1);

      assertThat(value.totalTime)
          .as("Total time of failed executions")
          .isGreaterThan(functionDuration.toNanos());
    });
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

  private void executeFunctionThatSucceeds(Execution<? super String[], ?, ?> execution) {
    executeFunction(execution, true);
  }

  private void executeFunctionThatThrows(Execution<? super String[], ?, ?> execution) {
    executeFunction(execution, false);
  }

  private void executeFunction(Execution<? super String[], ?, ?> execution, boolean isSuccessful) {
    execution
        .setArguments(new String[] {
            String.valueOf(functionDuration.toMillis()),
            String.valueOf(isSuccessful)})
        .execute(functionWithNoResult);
  }

  private ExecutionsTimerValues successTimerValue() {
    return getExecutionsTimerValuesFromServer(true);
  }

  private ExecutionsTimerValues failureTimerValue() {
    return getExecutionsTimerValuesFromServer(false);
  }

  private ExecutionsTimerValues getExecutionsTimerValuesFromServer(boolean isSuccessful) {
    List<List<ExecutionsTimerValues>> timerValuesForEachServer =
        FunctionExecutionsTimerNoResultTest
            .<Void, List<ExecutionsTimerValues>>onServer(clientCache)
            .execute(new GetFunctionExecutionTimerValues())
            .getResult();

    List<ExecutionsTimerValues> values = timerValuesForEachServer.stream()
        .flatMap(List::stream)
        .filter(v -> v.functionId.equals(FunctionToTimeWithoutResult.ID))
        .filter(v -> v.succeeded == isSuccessful)
        .collect(toList());

    Assertions.assertThat(values)
        .hasSize(1);

    return values.get(0);
  }

  @SuppressWarnings("unchecked")
  private static <IN, OUT> Execution<IN, OUT, List<OUT>> onRegion(Region<?, ?> region) {
    return (Execution<IN, OUT, List<OUT>>) FunctionService.onRegion(region);
  }

  @SuppressWarnings("unchecked")
  private static <IN, OUT> Execution<IN, OUT, List<OUT>> onServer(RegionService regionService) {
    return (Execution<IN, OUT, List<OUT>>) FunctionService.onServer(regionService);
  }
}
