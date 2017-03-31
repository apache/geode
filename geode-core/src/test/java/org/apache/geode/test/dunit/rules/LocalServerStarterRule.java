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
package org.apache.geode.test.dunit.rules;

import static org.apache.geode.distributed.Locator.startLocatorAndDS;
import static org.junit.Assert.assertTrue;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

public class LocalServerStarterRule extends ExternalResource implements Serializable {
  private volatile transient Cache cache;
  private volatile transient CacheServer server;
  private final transient TemporaryFolder temporaryFolder;

  private final Properties properties;
  private final Map<String, RegionShortcut> regionsToCreate;

  private final AvailablePort.Keeper serverPort;
  private final AvailablePort.Keeper jmxPort;
  private final AvailablePort.Keeper httpPort;
  private final AvailablePort.Keeper tcpPort;

  LocalServerStarterRule(ServerStarterBuilder serverStarterBuilder) {
    this.properties = serverStarterBuilder.getProperties();
    if (serverStarterBuilder.hasAutomaticallyManagedWorkingDir()) {
      temporaryFolder = new TemporaryFolder();
    } else {
      temporaryFolder = null;
    }

    this.serverPort = serverStarterBuilder.getServerPort();
    this.jmxPort = serverStarterBuilder.getJmxPort();
    this.httpPort = serverStarterBuilder.getHttpPort();
    this.tcpPort = serverStarterBuilder.getTcpPort();

    this.regionsToCreate = serverStarterBuilder.getRegionsToCreate();
  }

  @Override
  public void before() throws Throwable {
    if (temporaryFolder != null) {
      temporaryFolder.create();
      this.properties.setProperty(ConfigurationProperties.DEPLOY_WORKING_DIR,
          temporaryFolder.getRoot().getAbsolutePath());
    }

    releasePortKeepers();
    CacheFactory cf = new CacheFactory(this.properties);

    cache = cf.create();
    server = cache.addCacheServer();
    server.setPort(this.serverPort.getPort());
    server.start();

    for (Map.Entry<String, RegionShortcut> region : regionsToCreate.entrySet()) {
      cache.createRegionFactory(region.getValue()).create(region.getKey());
    }
  }

  @Override
  public void after() {
    if (cache != null) {
      cache.close();
      cache = null;
    }
    if (server != null) {
      server.stop();
      server = null;
    }

    if (temporaryFolder != null) {
      temporaryFolder.delete();
    }

    releasePortKeepers();
  }

  private void releasePortKeepers() {
    Stream.of(serverPort, jmxPort, httpPort, tcpPort).filter(Objects::nonNull)
        .forEach(AvailablePort.Keeper::release);
  }

  public int getHttpPort() {
    String httpPort = properties.getProperty(ConfigurationProperties.HTTP_SERVICE_PORT);

    if (httpPort != null) {
      return (Integer.valueOf(httpPort));
    }

    if (properties.getProperty(ConfigurationProperties.START_DEV_REST_API) != null) {
      throw new IllegalStateException("No HTTP_SERVICE_PORT has been specified");
    } else {
      throw new IllegalStateException("Dev rest api not configured for this server");
    }
  }

  public Cache getCache() {
    return cache;
  }

  public CacheServer getServer() {
    return server;
  }

  public File getWorkingDir() {
    if (cache == null) {
      throw new IllegalStateException("Server not yet initialized");
    }
    return ((InternalDistributedSystem) cache.getDistributedSystem()).getConfig()
        .getDeployWorkingDir();
  }

  public String getHostname() {
    return "localhost";
  }


  public Integer getJmxPort() {
    String jmxPort = properties.getProperty(ConfigurationProperties.JMX_MANAGER_PORT);
    if (jmxPort == null) {
      return null;
    }

    return Integer.valueOf(jmxPort);
  }

  public int getServerPort() {
    return this.serverPort.getPort();
  }

}
