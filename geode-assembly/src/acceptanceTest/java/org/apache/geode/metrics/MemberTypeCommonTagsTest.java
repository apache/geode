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

import static java.io.File.pathSeparator;
import static org.apache.geode.cache.execute.FunctionService.onMember;
import static org.apache.geode.cache.execute.FunctionService.onServer;
import static org.apache.geode.test.compiler.ClassBuilder.writeJarFromClasses;
import static org.apache.geode.test.micrometer.MicrometerAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.rules.ServiceJarRule;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;

public class MemberTypeCommonTagsTest {
  private Path serverFolder;
  private Pool serverPool;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public GfshRule gfshRule = new GfshRule();

  @Rule
  public ServiceJarRule serviceJarRule = new ServiceJarRule();
  private ClientCache clientCache;
  private Path locatorFolder;
  private Cache cache;

  @Test
  public void theMemberTypeTag_forAnEmbeddedCache_isEmbeddedCache() {
    SimpleMeterRegistry simpleMeterRegistry = new SimpleMeterRegistry();

    try (Cache ignored = createEmbeddedCache(simpleMeterRegistry)) {
      Gauge gauge = simpleMeterRegistry.find("jvm.buffer.memory.used").gauge();
      assertThat(gauge).hasTag("member.type", "embedded-cache");
    }
  }

  @Test
  public void theMemberTypeTag_forAServer_isServer() throws IOException {
    startServer("");

    try {
      assertThat(memberTypeTag(onServer(serverPool))).isEqualTo("server");
    } finally {
      stopServer();
    }
  }

  @Test
  public void theMemberTypeTag_forAMemberServerWithAnEmbeddedLocator_isServerLocator()
      throws IOException {
    startAMemberServerWithAnEmbeddedLocator();

    try {
      assertThat(memberTypeTag(onServer(serverPool))).isEqualTo("server-locator");
    } finally {
      stopServer();
    }
  }

  @Test
  public void theMemberTypeTag_forALocator_isLocator() throws IOException {
    DistributedMember locator = startLocator();

    try {
      assertThat(memberTypeTag(onMember(locator))).isEqualTo("locator");
    } finally {
      stopLocator();
    }
  }

  private Cache createEmbeddedCache(SimpleMeterRegistry simpleMeterRegistry) {
    Properties properties = new Properties();
    CacheFactory cacheFactory = new CacheFactory(properties);
    return cacheFactory.addMeterSubregistry(simpleMeterRegistry).create();
  }

  private DistributedMember startLocator() throws IOException {
    // need a locator for a test
    locatorFolder = temporaryFolder.getRoot().toPath().toAbsolutePath();

    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(1);

    int locatorPort = ports[0];

    Path serviceJarPath = serviceJarRule.createJarFor("metrics-publishing-service.jar",
        MetricsPublishingService.class, SimpleMetricsPublishingService.class);

    Path functionJarPath = locatorFolder.resolve("function.jar").toAbsolutePath();
    writeJarFromClasses(functionJarPath.toFile(), GetMemberTypeTag.class);

    String startLocatorCommand = String.join(" ",
        "start locator",
        "--name=locator",
        "--dir=" + locatorFolder,
        "--port=" + locatorPort,
        "--classpath=" + serviceJarPath + pathSeparator + functionJarPath,
        "--bind-address=127.0.0.1",
        "--http-service-port=7075");

    gfshRule.execute(startLocatorCommand);

    Properties properties = new Properties();
    properties.setProperty(DistributionConfig.LOCATORS_NAME, "127.0.0.1[" + locatorPort + "]");
    CacheFactory cacheFactory = new CacheFactory(properties);
    cache = cacheFactory.create();

    return cache.getDistributedSystem().findDistributedMember("locator");
  }

  private void stopLocator() {
    cache.close();
    String stopLocatorCommand = "stop locator --dir=" + locatorFolder;
    gfshRule.execute(stopLocatorCommand);
  }

  private void startAMemberServerWithAnEmbeddedLocator() throws IOException {
    // need to start a server with the "start-locator" variable
    startServer("--J=-Dgemfire.start-locator=127.0.0.1[10335]");
  }

  private void startServer(String additionalParameters) throws IOException {
    serverFolder = temporaryFolder.getRoot().toPath().toAbsolutePath();

    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(2);

    int serverPort = ports[0];
    int jmxRmiPort = ports[1];

    Path serviceJarPath = serviceJarRule.createJarFor("metrics-publishing-service.jar",
        MetricsPublishingService.class, SimpleMetricsPublishingService.class);

    Path functionJarPath = serverFolder.resolve("function.jar").toAbsolutePath();
    writeJarFromClasses(functionJarPath.toFile(), GetMemberTypeTag.class);

    String startServerCommand = String.join(" ",
        "start server",
        "--name=server",
        "--dir=" + serverFolder,
        "--server-port=" + serverPort,
        "--classpath=" + serviceJarPath + pathSeparator + functionJarPath,
        "--J=-Dgemfire.enable-cluster-config=true",
        "--J=-Dgemfire.jmx-manager=true",
        "--J=-Dgemfire.jmx-manager-start=true",
        "--J=-Dgemfire.jmx-manager-port=" + jmxRmiPort,
        additionalParameters);

    gfshRule.execute(startServerCommand);
    clientCache = new ClientCacheFactory().addPoolServer("localhost", serverPort).create();

    serverPool = PoolManager.createFactory()
        .addServer("localhost", serverPort)
        .create("server-pool");
  }

  private void stopServer() {
    serverPool.destroy();
    clientCache.close();

    String stopServerCommand = "stop server --dir=" + serverFolder;
    gfshRule.execute(stopServerCommand);
  }

  private String memberTypeTag(Execution execution) {
    @SuppressWarnings("unchecked")
    List<String> results = (List<String>) execution
        .execute(new GetMemberTypeTag())
        .getResult();
    return results.get(0);
  }

  static class GetMemberTypeTag implements Function<String> {
    private static final String ID = "GetMemberTypeTag";

    @Override
    public void execute(FunctionContext<String> context) {
      String meterNameToCheck = "jvm.memory.used";

      Meter meter = SimpleMetricsPublishingService.getRegistry()
          .find(meterNameToCheck)
          .meter();

      String result = null;

      if (meter != null) {
        Map<String, String> tagsMap = meter.getId().getTags().stream()
            .collect(Collectors.toMap(Tag::getKey, Tag::getValue));
        result = tagsMap.get("member.type");
      }

      context.<String>getResultSender().lastResult(result);
    }

    @Override
    public String getId() {
      return ID;
    }
  }
}
