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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionService;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.security.templates.UserPasswordAuthInit;
import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

public class ClientCacheRule extends SerializableExternalResource {
  private ClientCache cache;
  private ClientCacheFactory cacheFactory;
  private List<Consumer<ClientCacheFactory>> cacheSetups;
  private Properties properties;
  private boolean autoCreate;

  public ClientCacheRule() {
    properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    cacheSetups = new ArrayList<>();
  }

  public ClientCacheRule withProperties(Properties properties) {
    this.properties.putAll(properties);
    return this;
  }

  public ClientCacheRule withCredential(String username, String password) {
    properties.setProperty(UserPasswordAuthInit.USER_NAME, username);
    properties.setProperty(UserPasswordAuthInit.PASSWORD, password);
    properties.setProperty(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName());
    return this;
  }

  public ClientCacheRule withProperty(String key, String value) {
    properties.put(key, value);
    return this;
  }

  public ClientCacheRule withCacheSetup(Consumer<ClientCacheFactory> setup) {
    cacheSetups.add(setup);
    return this;
  }

  public ClientCacheRule withPoolSubscription(boolean enabled) {
    withCacheSetup(cf -> cf.setPoolSubscriptionEnabled(enabled));
    return this;
  }

  public ClientCacheRule withServerConnection(int... serverPorts) {
    withCacheSetup(cf -> {
      for (int serverPort : serverPorts) {
        cf.addPoolServer("localhost", serverPort);
      }
    });
    return this;
  }

  public ClientCacheRule withLocatorConnection(int... serverPorts) {
    withCacheSetup(cf -> {
      for (int serverPort : serverPorts) {
        cf.addPoolLocator("localhost", serverPort);
      }
    });
    return this;
  }

  public ClientCacheRule withMultiUser(boolean enabled) {
    withCacheSetup(cf -> cf.setPoolMultiuserAuthentication(enabled));
    return this;
  }

  public ClientCacheRule withAutoCreate() {
    this.autoCreate = true;
    return this;
  }

  public ClientCache createCache() throws Exception {
    cacheFactory = new ClientCacheFactory(properties);
    cacheSetups.stream().forEach(setup -> setup.accept(cacheFactory));
    cache = cacheFactory.create();
    return cache;
  }

  public <K, V> Region<K, V> createProxyRegion(String regionPath) {
    ClientRegionFactory<K, V> regionFactory =
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
    return regionFactory.create(regionPath);
  }

  public RegionService createAuthenticatedView(String username, String password) {
    Properties properties = new Properties();
    properties.setProperty(UserPasswordAuthInit.USER_NAME, username);
    properties.setProperty(UserPasswordAuthInit.PASSWORD, password);
    properties.setProperty(SECURITY_CLIENT_AUTH_INIT, UserPasswordAuthInit.class.getName());
    return cache.createAuthenticatedView(properties);
  }

  @Override
  public void before() throws Exception {
    if (autoCreate) {
      createCache();
    }
  }

  @Override
  public void after() {
    try {
      if (cache != null) {
        cache.close();
      }
    } catch (Exception ignored) {
      // ignored
    }
    // this will clean up the SocketCreators created in this VM so that it won't contaminate
    // future tests
    SocketCreatorFactory.close();
  }

  public ClientCache getCache() {
    return cache;
  }
}
