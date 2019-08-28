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


import static org.apache.geode.test.compiler.ClassBuilder.writeJarFromClasses;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

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
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.rules.ServiceJarRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class FunctionExecutionsTimerRegistrationTest {

  private int serverPort;
  private ClientCache clientCache;
  private Pool serverPool;
  private String connectCommand;
  private String startServerCommand;
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
    int jmxRmiPort = ports[1];

    Path serverFolder = temporaryFolder.newFolder("server").toPath().toAbsolutePath();

    Path serviceJarPath = serviceJarRule.createJarFor("services.jar",
        MetricsPublishingService.class, SimpleMetricsPublishingService.class);

    Path testHelpersJarPath =
        temporaryFolder.getRoot().toPath().resolve("test-helpers.jar").toAbsolutePath();
    writeJarFromClasses(testHelpersJarPath.toFile(),
        GetFunctionExecutionTimerValues.class, ExecutionsTimerValues.class);

    startServerCommand = String.join(" ",
        "start server",
        "--name=server",
        "--dir=" + serverFolder,
        "--server-port=" + serverPort,
        "--classpath=" + serviceJarPath,
        "--J=-Dgemfire.enable-cluster-config=true",
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Dgemfire.jmx-manager-port=" + jmxRmiPort);

    stopServerCommand = "stop server --dir=" + serverFolder;

    connectCommand = "connect --jmx-manager=localhost[" + jmxRmiPort + "]";
    String deployHelpersCommand = "deploy --jar=" + testHelpersJarPath;

    gfshRule.execute(startServerCommand, connectCommand, deployHelpersCommand);

    createClientAndPool();
  }

  @After
  public void tearDown() {
    closeClientAndPool();

    gfshRule.execute(stopServerCommand);
  }

  @Test
  public void noTimersRegistered_ifOnlyInternalFunctionsRegistered() {
    List<ExecutionsTimerValues> values = getTimerValues();

    assertThat(values).isEmpty();
  }

  @Test
  public void timersRegistered_ifFunctionDeployed() throws IOException {
    deployFunction(UserDeployedFunction.class);

    List<ExecutionsTimerValues> values = getTimerValues();

    assertThat(values)
        .hasSize(2)
        .allMatch(v -> v.functionId.equals(UserDeployedFunction.ID), "Has correct function ID")
        .allMatch(v -> v.count == 0L, "Has correct count")
        .allMatch(v -> v.totalTime == 0.0, "Has correct total time")
        .matches(v -> v.stream().anyMatch(t -> t.succeeded), "At least one succeeded timer")
        .matches(v -> v.stream().anyMatch(t -> !t.succeeded), "At least one failure timer");
  }

  @Test
  public void timersUnregistered_ifFunctionUndeployed() throws IOException {
    deployFunction(UserDeployedFunction.class);
    undeployFunctionAndRestartServer(UserDeployedFunction.class);

    List<ExecutionsTimerValues> values = getTimerValues();

    assertThat(values).isEmpty();
  }

  private void createClientAndPool() {
    clientCache = new ClientCacheFactory().addPoolServer("localhost", serverPort).create();
    serverPool = PoolManager.createFactory()
        .addServer("localhost", serverPort)
        .create("server-pool");
  }

  private void closeClientAndPool() {
    serverPool.destroy();
    clientCache.close();
  }

  @SuppressWarnings("SameParameterValue")
  private <T> void deployFunction(Class<? extends Function<T>> functionClass) throws IOException {
    Path functionJarPath = temporaryFolder.getRoot().toPath()
        .resolve(functionClass.getSimpleName() + ".jar").toAbsolutePath();
    writeJarFromClasses(functionJarPath.toFile(), functionClass);
    String deployFunctionCommand = "deploy --jar=" + functionJarPath;

    gfshRule.execute(connectCommand, deployFunctionCommand);
  }

  @SuppressWarnings("SameParameterValue")
  private <T> void undeployFunctionAndRestartServer(Class<? extends Function<T>> functionClass) {
    closeClientAndPool();

    String functionJarName = functionClass.getSimpleName() + ".jar";
    String undeployFunctionCommand = "undeploy --jar=" + functionJarName;

    gfshRule.execute(connectCommand, undeployFunctionCommand, stopServerCommand,
        startServerCommand);

    createClientAndPool();
  }

  private List<ExecutionsTimerValues> getTimerValues() {
    @SuppressWarnings("unchecked")
    Execution<Void, List<ExecutionsTimerValues>, List<List<ExecutionsTimerValues>>> functionExecution =
        (Execution<Void, List<ExecutionsTimerValues>, List<List<ExecutionsTimerValues>>>) FunctionService
            .onServer(serverPool);

    List<List<ExecutionsTimerValues>> results = functionExecution
        .execute(GetFunctionExecutionTimerValues.ID)
        .getResult();

    assertThat(results)
        .hasSize(1);

    return results.get(0);
  }

  public static class UserDeployedFunction implements Function<Void> {
    private static final String ID = "UserDeployedFunction";

    @Override
    public void execute(FunctionContext<Void> context) {
      // Nothing
    }

    @Override
    public String getId() {
      return ID;
    }

    @Override
    public boolean hasResult() {
      return false;
    }

    @Override
    public boolean isHA() {
      return false;
    }
  }

}
