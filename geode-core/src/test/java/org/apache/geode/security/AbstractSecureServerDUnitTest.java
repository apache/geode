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

package org.apache.geode.security;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.rules.ServerStarterRule;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

public abstract class AbstractSecureServerDUnitTest extends JUnit4DistributedTestCase {

  protected static final String REGION_NAME = "AuthRegion";

  protected VM client1 = null;
  protected VM client2 = null;
  protected VM client3 = null;
  protected int serverPort;
  protected boolean pdxPersistent = false;

  // overwrite this in child classes
  public Properties getProperties() {
    return new Properties() {
      {
        setProperty(SECURITY_MANAGER, TestSecurityManager.class.getName());
        setProperty(TestSecurityManager.SECURITY_JSON,
            "org/apache/geode/management/internal/security/clientServer.json");
      }
    };
  }

  // overwrite this if you want a different set of initial data
  public Map<String, String> getData() {
    Map<String, String> data = new HashMap();
    for (int i = 0; i < 5; i++) {
      data.put("key" + i, "value" + i);
    }
    return data;
  }

  @Before
  public void before() throws Exception {
    ServerStarterRule serverStarter = new ServerStarterRule(getProperties());
    serverStarter.startServer(0, pdxPersistent);
    serverPort = serverStarter.server.getPort();
    Region region =
        serverStarter.cache.createRegionFactory(RegionShortcut.REPLICATE).create(REGION_NAME);
    for (Entry entry : getData().entrySet()) {
      region.put(entry.getKey(), entry.getValue());
    }

    IgnoredException.addIgnoredException("No longer connected to localhost");
    IgnoredException.addIgnoredException(AuthenticationFailedException.class.getName());
    final Host host = Host.getHost(0);
    this.client1 = host.getVM(1);
    this.client2 = host.getVM(2);
    this.client3 = host.getVM(3);
  }

  public static void assertNotAuthorized(ThrowingCallable shouldRaiseThrowable, String permString) {
    assertThatThrownBy(shouldRaiseThrowable).hasMessageContaining(permString);
  }

  public static Properties createClientProperties(String userName, String password) {
    Properties props = new Properties();
    props.setProperty(UserPasswordAuthInit.USER_NAME, userName);
    props.setProperty(UserPasswordAuthInit.PASSWORD, password);
    props.setProperty(LOG_LEVEL, "fine");
    props.setProperty(LOCATORS, "");
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName());
    return props;
  }

  public static ClientCache createClientCache(String username, String password, int serverPort) {
    ClientCache cache = new ClientCacheFactory(createClientProperties(username, password))
        .setPoolSubscriptionEnabled(true).addPoolServer("localhost", serverPort).create();

    cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
    return cache;
  }

}
