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
package org.apache.geode.test.junit.rules;

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.awaitility.Awaitility;

import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ServerLauncher;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.pdx.PdxSerializer;

/**
 * This is a rule to start up a server in your current VM. It's useful for your Integration Tests.
 *
 * <p>
 * This rules allows you to create/start a server using any @ConfigurationProperties, you can chain
 * the configuration of the rule like this: ServerStarterRule server = new ServerStarterRule()
 * .withProperty(key, value) .withName(name) .withProperties(properties) .withSecurityManager(class)
 * .withJmxManager() .withRestService() .withEmbeddedLocator() .withRegion(type, name) etc, etc. If
 * your rule calls withAutoStart(), the cache and server will be started before your test code.
 *
 * <p>
 * In your test code, you can use the rule to access the server's attributes, like the port
 * information, working dir, name, and the cache and cacheServer it creates.
 *
 * <p>
 * If you need a rule to start a server/locator in different VMs for Distributed tests, You should
 * use {@code LocatorServerStartupRule}.
 */
public class ServerStarterRule extends MemberStarterRule<ServerStarterRule> implements Server {
  private int embeddedLocatorPort = -1;
  private transient CacheServer server;
  private transient ServerLauncher serverLauncher;
  private transient InternalCache cache;
  private boolean pdxPersistent = false;
  private boolean pdxReadSerialized = false;
  private PdxSerializer pdxSerializer = null;
  private Map<String, RegionShortcut> regions = new HashMap<>();

  @Override
  public InternalCache getCache() {
    return cache;
  }

  @Override
  public CacheServer getServer() {
    return server;
  }

  @Override
  public int getEmbeddedLocatorPort() {
    return embeddedLocatorPort;
  }

  @Override
  protected void normalizeProperties() {
    super.normalizeProperties();
    if (httpPort < 0 && "true".equalsIgnoreCase(properties.getProperty(START_DEV_REST_API))) {
      withRestService();
    }
  }

  public ServerStarterRule withPDXPersistent() {
    pdxPersistent = true;
    return this;
  }

  public ServerStarterRule withPDXReadSerialized() {
    pdxReadSerialized = true;
    return this;
  }

  public ServerStarterRule withEmbeddedLocator() {
    embeddedLocatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    properties.setProperty("start-locator", "localhost[" + embeddedLocatorPort + "]");
    return this;
  }

  public ServerStarterRule withRestService(boolean useDefaultPort) {
    properties.setProperty(START_DEV_REST_API, "true");
    properties.setProperty(HTTP_SERVICE_BIND_ADDRESS, "localhost");

    if (!useDefaultPort) {
      httpPort = AvailablePortHelper.getRandomAvailableTCPPort();
      properties.setProperty(HTTP_SERVICE_PORT, httpPort + "");
    } else {
      httpPort = 0;
    }

    return this;
  }

  public ServerStarterRule withRestService() {
    return withRestService(false);
  }

  public ServerStarterRule withRegion(RegionShortcut type, String name) {
    this.autoStart = true;
    regions.put(name, type);
    return this;
  }

  /**
   * Convenience method to create a region with customized regionFactory
   *
   * @param regionFactoryConsumer a lamda that allows you to customize the regionFactory
   */
  public Region createRegion(RegionShortcut type, String name,
      Consumer<RegionFactory> regionFactoryConsumer) {
    RegionFactory regionFactory = getCache().createRegionFactory(type);
    regionFactoryConsumer.accept(regionFactory);

    return regionFactory.create(name);
  }

  /**
   * convenience method to create a partition region with customized regionFactory and a customized
   * PartitionAttributeFactory
   *
   * @param regionFactoryConsumer a lamda that allows you to customize the regionFactory
   * @param attributesFactoryConsumer a lamda that allows you to customize the
   *        partitionAttributeFactory
   */
  public Region createPartitionRegion(String name, Consumer<RegionFactory> regionFactoryConsumer,
      Consumer<PartitionAttributesFactory> attributesFactoryConsumer) {
    return createRegion(RegionShortcut.PARTITION, name, rf -> {
      regionFactoryConsumer.accept(rf);
      PartitionAttributesFactory attributeFactory = new PartitionAttributesFactory();
      attributesFactoryConsumer.accept(attributeFactory);
      rf.setPartitionAttributes(attributeFactory.create());
    });
  }

  public void startServer(Properties properties, int locatorPort) {
    withProperties(properties).withConnectionToLocator(locatorPort).startServer();
  }

  @Override
  public void before() {
    super.before();

    // MemberStarterRule deletes the member's workingDirectory (TemporaryFolder) within the after()
    // method but doesn't recreate it during before(), breaking the purpose of the TemporaryFolder
    // rule all together (it gets created only from the withWorkingDir() method, which is generally
    // called only once).
    try {
      if (temporaryFolder != null) {
        temporaryFolder.create();
      }
    } catch (IOException ioException) {
      // Should never happen.
      throw new RuntimeException(ioException);
    }

    if (autoStart) {
      startServer();

      regions.forEach((regionName, regionType) -> {
        RegionFactory rf = getCache().createRegionFactory(regionType);
        rf.create(regionName);
      });
    }
  }

  public void startServer() {
    ServerLauncher.Builder serverLauncherBuilder = new ServerLauncher.Builder();
    properties.forEach((key, value) -> serverLauncherBuilder.set((String) key, (String) value));
    serverLauncherBuilder.setServerPort(memberPort);
    serverLauncherBuilder.setPdxPersistent(pdxPersistent);
    serverLauncherBuilder.setPdxReadSerialized(pdxReadSerialized);

    // Set the name configured, or the default is none is configured.
    if (StringUtils.isNotBlank(this.name)) {
      serverLauncherBuilder.setMemberName(this.name);
    } else {
      this.name = serverLauncherBuilder.getMemberName();
    }

    serverLauncherBuilder.setDeletePidFileOnStop(true);
    if (pdxSerializer != null)
      serverLauncherBuilder.setPdxSerializer(pdxSerializer);

    // The ServerLauncher class doesn't provide a public way of logging only to console.
    serverLauncher = spy(serverLauncherBuilder.build());
    doAnswer((invocation) -> (!logFile) ? null : invocation.callRealMethod()).when(serverLauncher)
        .getLogFile();

    // Start server and wait until it comes online (max 60 seconds).
    serverLauncher.start();
    Awaitility.await().atMost(60, TimeUnit.SECONDS)
        .untilAsserted(() -> assertThat(serverLauncher.isRunning()).isTrue());
    cache = (InternalCache) serverLauncher.getCache();
    server = cache.getCacheServers().get(0);
    memberPort = server.getPort();
    DistributionConfig config =
        ((InternalDistributedSystem) cache.getDistributedSystem()).getConfig();
    jmxPort = config.getJmxManagerPort();
    httpPort = config.getHttpServicePort();
  }

  @Override
  public void stopMember() {
    if (serverLauncher != null) {
      try {
        serverLauncher.stop();
      } catch (Exception exception) {
        exception.printStackTrace(System.out);
      }
    }
  }

  @Override
  public void after() {
    super.after();

    // Some files are generated in the current dir if workingDir is not set, clean them up.
    if (getWorkingDir() == null) {
      Path serverLogFile = Paths.get(this.name + ".log");
      FileUtils.deleteQuietly(serverLogFile.toFile());
    }
  }
}
