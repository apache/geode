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

package org.apache.geode.management.internal.security;

import static org.apache.geode.distributed.ConfigurationProperties.*;

import java.io.Serializable;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.rules.ExternalResource;

import org.apache.geode.cache.Cache;
import org.apache.geode.security.TestSecurityManager;
import org.apache.geode.test.dunit.rules.ServerStarterRule;

/**
 * this rule would help you start up a cache server with the given properties in the current VM
 */
public class CacheServerStartupRule extends ExternalResource implements Serializable {

  private ServerStarterRule serverStarter;

  public static CacheServerStartupRule withDefaultSecurityJson(int jmxManagerPort) {
    return new CacheServerStartupRule(jmxManagerPort,
        "org/apache/geode/management/internal/security/cacheServer.json");
  }

  public CacheServerStartupRule(int jmxManagerPort, String jsonFile) {
    Properties properties = new Properties();
    if (jmxManagerPort > 0) {
      properties.put(JMX_MANAGER_PORT, String.valueOf(jmxManagerPort));
    }
    if (jsonFile != null) {
      properties.put(SECURITY_MANAGER, TestSecurityManager.class.getName());
      properties.put(TestSecurityManager.SECURITY_JSON, jsonFile);
    }
    serverStarter = new ServerStarterRule(properties);
  }

  @Before
  public void before() throws Throwable {
    serverStarter.startServer();
    serverStarter.cache.createRegionFactory().create("region1");
  }

  @After
  public void after() {
    serverStarter.after();
  }

  public Cache getCache() {
    return serverStarter.cache;
  }

  public int getServerPort() {
    return serverStarter.server.getPort();
  }
}
