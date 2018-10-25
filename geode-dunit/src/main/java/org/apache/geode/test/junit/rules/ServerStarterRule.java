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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
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
  private transient InternalCache cache;
  private transient CacheServer server;
  private int embeddedLocatorPort = -1;
  private boolean pdxPersistent = false;
  private PdxSerializer pdxSerializer = null;
  private boolean pdxReadSerialized = false;
  private boolean noCacheServer = false;

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
  public void before() {
    super.before();
    if (autoStart) {
      startServer();
      regions.forEach((regionName, regionType) -> {
        RegionFactory rf = getCache().createRegionFactory(regionType);
        rf.create(regionName);
      });
    }
  }

  @Override
  public void stopMember() {
    // make sure this cache is the one currently open. A server cache can be recreated due to
    // importing a new set of cluster configuration.
    cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      try {
        cache.close();
      } catch (Exception e) {
      } finally {
        cache = null;
      }
    }
    server = null;
  }

  public ServerStarterRule withPDXPersistent() {
    pdxPersistent = true;
    return this;
  }

  public ServerStarterRule withPDXReadSerialized() {
    pdxReadSerialized = true;
    return this;
  }

  public ServerStarterRule withPdxSerializer(PdxSerializer pdxSerializer) {
    this.pdxSerializer = pdxSerializer;
    return this;
  }

  /**
   * If your only needs a cache and does not need a server for clients to connect
   */
  public ServerStarterRule withNoCacheServer() {
    this.noCacheServer = true;
    return this;
  }

  public ServerStarterRule withEmbeddedLocator() {
    embeddedLocatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    properties.setProperty("start-locator", "localhost[" + embeddedLocatorPort + "]");
    return this;
  }

  public ServerStarterRule withRestService() {
    return withRestService(false);
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

  @Override
  protected void normalizeProperties() {
    super.normalizeProperties();
    if (httpPort < 0 && "true".equalsIgnoreCase(properties.getProperty(START_DEV_REST_API))) {
      withRestService();
    }
  }

  public ServerStarterRule withRegion(RegionShortcut type, String name) {
    this.autoStart = true;
    regions.put(name, type);
    return this;
  }

  /**
   * convenience method to create a region with customized regionFactory
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

  public void startServer() {
    CacheFactory cf = new CacheFactory(this.properties);
    cf.setPdxPersistent(pdxPersistent);
    cf.setPdxReadSerialized(pdxReadSerialized);
    if (pdxSerializer != null) {
      cf.setPdxSerializer(pdxSerializer);
    }
    cache = (InternalCache) cf.create();
    DistributionConfig config =
        ((InternalDistributedSystem) cache.getDistributedSystem()).getConfig();
    jmxPort = config.getJmxManagerPort();
    httpPort = config.getHttpServicePort();

    if (!noCacheServer) {
      server = cache.addCacheServer();
      // memberPort is by default zero, which translates to "randomly select an available port,"
      // which is why it is updated after this try block
      server.setPort(memberPort);
      try {
        server.start();
      } catch (IOException e) {
        throw new RuntimeException("unable to start server", e);
      }
      memberPort = server.getPort();
    }
  }

  @Override
  public int getEmbeddedLocatorPort() {
    return embeddedLocatorPort;
  }
}
