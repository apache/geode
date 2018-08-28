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

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import java.util.Properties;
import java.util.function.Consumer;

import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

public class ClientCacheRule extends SerializableExternalResource {
  private ClientCache cache;
  private ClientCacheFactory cacheFactory;
  private Consumer<ClientCacheFactory> cacheSetup;
  private Properties properties;

  public ClientCacheRule() {
    properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
  }

  public ClientCacheRule withProperties(Properties properties) {
    this.properties.putAll(properties);
    return this;
  }

  public ClientCacheRule withProperty(String key, String value) {
    properties.put(key, value);
    return this;
  }

  public ClientCacheRule withCacheSetup(Consumer<ClientCacheFactory> setup) {
    cacheSetup = setup;
    return this;
  }

  @Override
  public void before() throws Exception {
    cacheFactory = new ClientCacheFactory(properties);
    cacheSetup.accept(cacheFactory);
    cache = cacheFactory.create();
  }

  @Override
  public void after() {
    if (cache != null) {
      cache.close();
    }
    // this will clean up the SocketCreators created in this VM so that it won't contaminate
    // future tests
    SocketCreatorFactory.close();
  }

  public ClientCache getCache() {
    return cache;
  }
}
