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
package org.apache.geode.redis;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class RedisServerIntegrationTest {

  private Cache cache;
  private GeodeRedisServer redisServer;
  private int redisPort;

  @Before
  public void createCache() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    CacheFactory cacheFactory = new CacheFactory(props);
    cache = cacheFactory.create();
    redisPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    assert (cache.rootRegions().size() == 0);
  }

  @After
  public void teardown() {
    if (redisServer != null) {
      redisServer.shutdown();
      cache.close();
    }
  }

  @Test
  public void initializeRedisCreatesFourRegions() {
    redisServer = new GeodeRedisServer(redisPort);
    redisServer.start();
    assert cache.rootRegions().size() == 4 : cache.rootRegions().size();
    assert cache.getRegion(GeodeRedisServer.REDIS_META_DATA_REGION) != null;
  }

  @Test
  public void initializeRedisCreatesPartitionedRegionByDefault() {
    redisServer = new GeodeRedisServer(redisPort);
    redisServer.start();
    Region r = cache.getRegion(GeodeRedisServer.STRING_REGION);
    assert r.getAttributes().getDataPolicy() == DataPolicy.PARTITION : r.getAttributes()
        .getDataPolicy();
  }

  @Test
  public void initializeRedisCreatesRegionsUsingSystemProperty() {
    System.setProperty("gemfireredis.regiontype", "REPLICATE");
    redisServer = new GeodeRedisServer(redisPort);
    redisServer.start();
    Region r = cache.getRegion(GeodeRedisServer.STRING_REGION);
    assert r.getAttributes().getDataPolicy() == DataPolicy.REPLICATE : r.getAttributes()
        .getDataPolicy();
    System.setProperty("gemfireredis.regiontype", "");
  }
}
