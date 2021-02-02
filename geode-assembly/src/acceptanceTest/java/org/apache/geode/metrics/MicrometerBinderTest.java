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

import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.compiler.ClassBuilder.writeJarFromClasses;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
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
import org.apache.geode.rules.ServiceJarRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class MicrometerBinderTest {

  private Path serverFolder;
  private ClientCache clientCache;
  private Pool serverPool;
  private Execution<String, Boolean, List<Boolean>> functionExecution;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public ServiceJarRule serviceJarRule = new ServiceJarRule();

  @Before
  public void startServer() throws IOException {
    serverFolder = temporaryFolder.getRoot().toPath().toAbsolutePath();

    int[] ports = getRandomAvailableTCPPorts(2);

    int serverPort = ports[0];
    int jmxRmiPort = ports[1];

    Path serviceJarPath = serviceJarRule.createJarFor("metrics-publishing-service.jar",
        MetricsPublishingService.class, SimpleMetricsPublishingService.class);

    String startServerCommand = String.join(" ",
        "start server",
        "--name=server",
        "--dir=" + serverFolder,
        "--server-port=" + serverPort,
        "--classpath=" + serviceJarPath,
        "--http-service-port=0",
        "--J=-Dgemfire.enable-cluster-config=true",
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Dgemfire.jmx-manager-port=" + jmxRmiPort);

    gfshRule.execute(startServerCommand, "sleep --time=1");

    Path functionJarPath = serverFolder.resolve("function.jar").toAbsolutePath();
    writeJarFromClasses(functionJarPath.toFile(), CheckIfMeterExistsFunction.class);

    String connectCommand = "connect --jmx-manager=localhost[" + jmxRmiPort + "]";
    String deployCommand = "deploy --jar=" + functionJarPath.toAbsolutePath();

    gfshRule.execute(connectCommand, deployCommand);

    clientCache = new ClientCacheFactory().addPoolServer("localhost", serverPort).create();

    serverPool = PoolManager.createFactory()
        .addServer("localhost", serverPort)
        .create("server-pool");

    @SuppressWarnings("unchecked")
    Execution<String, Boolean, List<Boolean>> functionExecution =
        (Execution<String, Boolean, List<Boolean>>) FunctionService.onServer(serverPool);
    this.functionExecution = functionExecution;
  }

  @After
  public void stopServer() {
    clientCache.close();
    serverPool.destroy();

    String stopServerCommand = "stop server --dir=" + serverFolder;
    gfshRule.execute(stopServerCommand);
  }

  @Test
  public void jvmMemoryMetricsBinderExists() {
    String meterNameToCheck = "jvm.memory.used";

    List<Boolean> results = functionExecution
        .setArguments(meterNameToCheck)
        .execute(CheckIfMeterExistsFunction.ID)
        .getResult();

    assertThat(results)
        .as("Meter from %s binder should exist", JvmMemoryMetrics.class.getSimpleName())
        .containsOnly(true);
  }

  @Test
  public void jvmThreadMetricsBinderExists() {
    String meterNameToCheck = "jvm.threads.peak";

    List<Boolean> results = functionExecution
        .setArguments(meterNameToCheck)
        .execute(CheckIfMeterExistsFunction.ID)
        .getResult();

    assertThat(results)
        .as("Meter from %s binder should exist", JvmThreadMetrics.class.getSimpleName())
        .containsOnly(true);
  }

  @Test
  public void processorMetricsBinderExists() {
    String meterNameToCheck = "system.cpu.count";

    List<Boolean> results = functionExecution
        .setArguments(meterNameToCheck)
        .execute(CheckIfMeterExistsFunction.ID)
        .getResult();

    assertThat(results)
        .as("Meter from %s binder should exist", ProcessorMetrics.class.getSimpleName())
        .containsOnly(true);
  }

  @Test
  public void uptimeMetricsBinderExists() {
    String meterNameToCheck = "process.uptime";

    List<Boolean> results = functionExecution
        .setArguments(meterNameToCheck)
        .execute(CheckIfMeterExistsFunction.ID)
        .getResult();

    assertThat(results)
        .as("Meter from %s binder should exist", UptimeMetrics.class.getSimpleName())
        .containsOnly(true);
  }

  public static class CheckIfMeterExistsFunction implements Function<String> {
    private static final String ID = "CheckIfMeterExistsFunction";

    @Override
    public void execute(FunctionContext<String> context) {
      String meterName = context.getArguments();

      Meter meter = SimpleMetricsPublishingService.getRegistry()
          .find(meterName)
          .meter();

      boolean isMeterFound = meter != null;

      context.<Boolean>getResultSender().lastResult(isMeterFound);
    }

    @Override
    public String getId() {
      return ID;
    }
  }
}
