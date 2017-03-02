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

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_FILE;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.GemFireCacheImpl;

import java.io.File;
import java.io.IOException;
import java.util.Properties;


/**
 * This is a rule to start up a server in your current VM. It's useful for your Integration Tests.
 *
 * You can create this rule either with a property or without a property. If created with a
 * property, The rule will automatically start the server for you with the properties given.
 *
 * If created without a property, the rule won't start the server until you specicially call one of
 * the startServer function.
 *
 * Either way, the rule will handle properly stopping the server for you.
 *
 * If you need a rule to start a server/locator in different VMs for Distributed tests, You should
 * use {@link LocatorServerStartupRule}.
 */
public class ServerStarterRule extends MemberStarterRule implements Server {

  private transient Cache cache;
  private transient CacheServer server;

  /**
   * Default constructor, if used, the rule will create a temporary folder as the server's working
   * dir, and will delete it when the test is done.
   */
  public ServerStarterRule() {}

  /**
   * if constructed this way, the rule won't be deleting the workingDir after the test is done. It's
   * up to the caller's responsibility to delete it.
   * 
   * @param workingDir: the working dir this server should be writing the artifacts to.
   */
  public ServerStarterRule(File workingDir) {
    this.workingDir = workingDir;
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

  public ServerStarterRule startServer() {
    return startServer(new Properties(), -1, false);
  }

  public ServerStarterRule startServer(int locatorPort) {
    return startServer(new Properties(), locatorPort, false);
  }

  public ServerStarterRule startServer(int locatorPort, boolean pdxPersistent) {
    return startServer(new Properties(), locatorPort, pdxPersistent);
  }

  public ServerStarterRule startServer(Properties properties) {
    return startServer(properties, -1, false);
  }

  public ServerStarterRule startServer(Properties properties, int locatorPort) {
    return startServer(properties, locatorPort, false);
  }

  public ServerStarterRule startServer(Properties properties, int locatorPort,
      boolean pdxPersistent) {
    if (properties == null) {
      properties = new Properties();
    }
    if (!properties.containsKey(NAME)) {
      properties.setProperty(NAME, "server");
    }
    name = properties.getProperty(NAME);
    if (!properties.containsKey(LOG_FILE)) {
      properties.setProperty(LOG_FILE, new File(name + ".log").getAbsolutePath().toString());
    }

    if (locatorPort > 0) {
      properties.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
    }
    if (!properties.containsKey(MCAST_PORT)) {
      properties.setProperty(MCAST_PORT, "0");
    }

    if (!properties.containsKey(LOCATORS)) {
      properties.setProperty(LOCATORS, "");
    }
    if (properties.containsKey(JMX_MANAGER_PORT)) {
      jmxPort = Integer.parseInt(properties.getProperty(JMX_MANAGER_PORT));
      if (jmxPort > 0) {
        if (!properties.containsKey(JMX_MANAGER))
          properties.put(JMX_MANAGER, "true");
        if (!properties.containsKey(JMX_MANAGER_START))
          properties.put(JMX_MANAGER_START, "true");
      }
    }
    CacheFactory cf = new CacheFactory(properties);
    cf.setPdxReadSerialized(pdxPersistent);
    cf.setPdxPersistent(pdxPersistent);
    cache = cf.create();
    server = cache.addCacheServer();
    server.setPort(0);
    try {
      server.start();
    } catch (IOException e) {
      throw new RuntimeException("unable to start server", e);
    }
    memberPort = server.getPort();
    return this;
  }
}
