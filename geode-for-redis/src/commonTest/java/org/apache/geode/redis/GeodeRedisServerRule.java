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
 *
 */

package org.apache.geode.redis;

import static org.apache.geode.distributed.ConfigurationProperties.GEODE_FOR_REDIS_USERNAME;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.redis.internal.GeodeRedisServer;
import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

public class GeodeRedisServerRule extends SerializableExternalResource {
  private GemFireCache cache;
  private GeodeRedisServer server;
  private CacheFactory cacheFactory;
  private Boolean enableUnsupportedCommands = true;

  public GeodeRedisServerRule() {
    cacheFactory = new CacheFactory();
    cacheFactory.set(LOG_LEVEL, "warn");
    cacheFactory.set(MCAST_PORT, "0");
    cacheFactory.set(LOCATORS, "");
  }

  public void setEnableUnsupportedCommands(boolean allow) {
    this.server.setAllowUnsupportedCommands(allow);
  }

  @Override
  protected void before() {
    cache = cacheFactory.create();
    server = new GeodeRedisServer("localhost", 0, (InternalCache) cache);
    server.setAllowUnsupportedCommands(enableUnsupportedCommands);

    // Ensure that buckets are created up front
    try {
      server.getRegionProvider().getSlotAdvisor().getBucketSlots();
    } catch (InterruptedException ignored) {
    }
  }

  public GeodeRedisServerRule withProperty(String property, String value) {
    cacheFactory.set(property, value);
    return this;
  }

  @Override
  protected void after() {
    cache.close();
    server.shutdown();
  }

  public GeodeRedisServer getServer() {
    return server;
  }

  public GeodeRedisServerRule withUsername(String username) {
    cacheFactory.set(GEODE_FOR_REDIS_USERNAME, username);

    return this;
  }

  public int getPort() {
    return server.getPort();
  }
}
