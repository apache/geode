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

import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.CacheServerHelper;
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
 * use {@code ClusterStartupRule}.
 */
public class ServerStarterRule extends MemberStarterRule<ServerStarterRule> implements Server {
  private final int availableLocatorPort;
  private transient InternalCache cache;
  private transient List<CacheServer> servers = new ArrayList<>();
  private int embeddedLocatorPort = -1;
  private boolean pdxPersistent = false;
  private boolean pdxPersistentUserSet = false;
  private PdxSerializer pdxSerializer = null;
  private boolean pdxReadSerialized = false;
  private boolean pdxReadSerializedUserSet = false;
  // By default we start one server per jvm
  private int serverCount = 1;

  public ServerStarterRule() {
    String workerID = System.getProperty("org.gradle.test.worker");
    Path wd = Paths.get(".").toAbsolutePath();
    String className = getClass().getSimpleName();
    availableLocatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
  }

  private Map<String, RegionShortcut> regions = new HashMap<>();

  @Override
  public InternalCache getCache() {
    return cache;
  }

  @Override
  public CacheServer getServer() {
    return servers.get(0);
  }

  public List<CacheServer> getServers() {
    return servers;
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
    for (CacheServer server : servers) {
      server.stop();
    }
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
    servers.clear();
  }

  public ServerStarterRule withPDXPersistent() {
    pdxPersistent = true;
    pdxPersistentUserSet = true;
    return this;
  }

  public ServerStarterRule withPDXReadSerialized() {
    pdxReadSerialized = true;
    pdxReadSerializedUserSet = true;
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
    this.serverCount = 0;
    return this;
  }

  public ServerStarterRule withServerCount(int serverCount) {
    this.serverCount = serverCount;
    return this;
  }

  public ServerStarterRule withEmbeddedLocator() {
    embeddedLocatorPort = availableLocatorPort;
    properties.setProperty("start-locator", "localhost[" + embeddedLocatorPort + "]");
    return this;
  }

  public ServerStarterRule withRestService() {
    return withRestService(false);
  }

  public ServerStarterRule withRestService(boolean useDefaultPort) {
    withHttpService(useDefaultPort);
    properties.setProperty(START_DEV_REST_API, "true");
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

  public void startServer(Properties properties, int locatorPort) {
    withProperties(properties).withConnectionToLocator(locatorPort).startServer();
  }

  public void startServer() {
    if (servers == null) {
      servers = new ArrayList<>();
    }
    CacheFactory cf = new CacheFactory(this.properties);
    if (pdxPersistentUserSet) {
      cf.setPdxPersistent(pdxPersistent);
    }
    if (pdxReadSerializedUserSet) {
      cf.setPdxReadSerialized(pdxReadSerialized);
    }
    if (pdxSerializer != null) {
      cf.setPdxSerializer(pdxSerializer);
    }
    cache = (InternalCache) cf.create();
    DistributionConfig config =
        ((InternalDistributedSystem) cache.getDistributedSystem()).getConfig();
    jmxPort = config.getJmxManagerPort();
    httpPort = config.getHttpServicePort();

    if (serverCount > 1 && memberPort != 0) {
      throw new IllegalStateException("can't specify a member port when you have multiple port");
    }

    for (int i = 0; i < serverCount; i++) {
      CacheServer server = cache.addCacheServer();
      if (i == 0) {
        CacheServerHelper.setIsDefaultServer(server);
      }
      // memberPort is by default zero, which translates to "randomly select an available port,"
      // which is why it is updated after this try block
      if (serverCount == 1) {
        server.setPort(memberPort);
      } else {
        server.setPort(0);
      }
      try {
        server.start();
      } catch (IOException e) {
        throw new RuntimeException("unable to start server", e);
      }
      // if this member has multiple cache servers, the memberPort will be the last server's port
      // started.
      memberPort = server.getPort();
      servers.add(server);
    }
  }

  @Override
  public int getEmbeddedLocatorPort() {
    return embeddedLocatorPort;
  }

  @Override
  public void waitTilFullyReconnected() {
    try {
      await().until(() -> {
        InternalDistributedSystem internalDistributedSystem =
            InternalDistributedSystem.getConnectedInstance();
        return internalDistributedSystem != null
            && internalDistributedSystem.getCache() != null
            && !internalDistributedSystem.getCache().getCacheServers().isEmpty();
      });

    } catch (Exception e) {
      // provide more information when condition is not satisfied after awaitility timeout
      InternalDistributedSystem ids = InternalDistributedSystem.getConnectedInstance();
      System.out.println("ds is: " + (ids != null ? "not null" : "null"));
      System.out.println("cache is: " + (ids.getCache() != null ? "not null" : "null"));
      System.out.println("has cache server: "
          + (!ids.getCache().getCacheServers().isEmpty()));
      throw e;
    }
    InternalDistributedSystem dm = InternalDistributedSystem.getConnectedInstance();
    cache = dm.getCache();
  }
}
