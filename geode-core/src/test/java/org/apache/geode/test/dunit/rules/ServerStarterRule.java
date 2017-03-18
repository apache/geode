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

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;

import java.io.File;
import java.io.IOException;
import java.util.Properties;


/**
 * This is a rule to start up a server in your current VM. It's useful for your Integration Tests.
 *
 * If you need a rule to start a server/locator in different VMs for Distributed tests, You should
 * use {@link LocatorServerStartupRule}.
 */
public class ServerStarterRule extends MemberStarterRule<ServerStarterRule> implements Server {

  private transient Cache cache;
  private transient CacheServer server;
  private int embeddedLocatorPort = -1;

  /**
   * Default constructor, if used, the rule will create a temporary folder as the server's working
   * dir, and will delete it when the test is done.
   */
  public ServerStarterRule() {}

  /**
   * if constructed this way, the rule won't be deleting the workingDir after the test is done. It's
   * the caller's responsibility to delete it.
   * 
   * @param workingDir: the working dir this server should be writing the artifacts to.
   */
  public ServerStarterRule(File workingDir) {
    super(workingDir);
  }

  public Cache getCache() {
    return cache;
  }

  public CacheServer getServer() {
    return server;
  }

  @Override
  void stopMember() {
    // make sure this cache is the one currently open. A server cache can be recreated due to
    // importing a new set of cluster configuration.
    cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      cache.close();
      cache = null;
    }
    if (server != null) {
      server.stop();
      server = null;
    }
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
    }
    return this;
  }

  public ServerStarterRule startServer() {
    return startServer(false);
  }

  public ServerStarterRule createRegion(RegionShortcut type, String name) {
    cache.createRegionFactory(type).create(name);
    return this;
  }

  public ServerStarterRule startServer(Properties properties, int locatorPort) {
    return withProperties(properties).withConnectionToLocator(locatorPort).startServer();
  }

  public ServerStarterRule startServer(boolean pdxPersistent) {
    normalizeProperties();
    CacheFactory cf = new CacheFactory(this.properties);
    cf.setPdxReadSerialized(pdxPersistent);
    cf.setPdxPersistent(pdxPersistent);
    cache = cf.create();
    DistributionConfig config =
        ((InternalDistributedSystem) cache.getDistributedSystem()).getConfig();
    server = cache.addCacheServer();
    server.setPort(0);
    try {
      server.start();
    } catch (IOException e) {
      throw new RuntimeException("unable to start server", e);
    }
    memberPort = server.getPort();
    jmxPort = config.getJmxManagerPort();
    httpPort = config.getHttpServicePort();
    return this;
  }

  public int getEmbeddedLocatorPort() {
    return embeddedLocatorPort;
  }

}
