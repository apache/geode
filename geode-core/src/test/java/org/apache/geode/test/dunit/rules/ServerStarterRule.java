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
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.NAME;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.junit.rules.ExternalResource;

import java.io.Serializable;
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
public class ServerStarterRule extends ExternalResource implements Serializable {

  public Cache cache;
  public CacheServer server;

  private Properties properties;

  /**
   * Default constructor, if used, the rule won't start the server for you, you will need to
   * manually start it. The rule will handle stop the server for you.
   */
  public ServerStarterRule() {}

  public ServerStarterRule(Properties properties) {
    this.properties = properties;
  }

  public void startServer() throws Exception {
    startServer(0, false);
  }

  public void startServer(int locatorPort) throws Exception {
    startServer(locatorPort, false);
  }

  public void startServer(int locatorPort, boolean pdxPersistent) throws Exception {
    startServer(properties, locatorPort, pdxPersistent);
  }

  public void startServer(Properties properties) throws Exception {
    startServer(properties, 0, false);
  }

  public void startServer(Properties properties, int locatorPort) throws Exception {
    startServer(properties, locatorPort, false);
  }

  public void startServer(Properties properties, int locatorPort, boolean pdxPersistent)
      throws Exception {
    if (properties == null) {
      properties = new Properties();
    }
    if (locatorPort > 0) {
      properties.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
    }
    if (!properties.containsKey(MCAST_PORT)) {
      properties.setProperty(MCAST_PORT, "0");
    }
    if (!properties.containsKey(NAME)) {
      properties.setProperty(NAME, this.getClass().getName());
    }
    if (!properties.containsKey(LOCATORS)) {
      properties.setProperty(LOCATORS, "");
    }
    if (properties.containsKey(JMX_MANAGER_PORT)) {
      int jmxPort = Integer.parseInt(properties.getProperty(JMX_MANAGER_PORT));
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
    server.start();
  }

  /**
   * if you use this class as a rule, the default startServer will be called in the before. You need
   * to make sure your properties to start the server with has the locator information it needs to
   * connect to, otherwise, this server won't connect to any locator
   */
  protected void before() throws Throwable {
    if (properties != null)
      startServer();
  }

  @Override
  public void after() {
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
}
