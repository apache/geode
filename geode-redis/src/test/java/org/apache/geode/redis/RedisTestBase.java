/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.redis;

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.REDIS_PORT;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.net.SocketCreator;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import redis.clients.jedis.Jedis;

public class RedisTestBase {
  //  protected Jedis jedis;
  protected GemFireCacheImpl cache;
  protected static String redisPort;
  protected Random rand;
  protected String hostName;

  @BeforeClass
  public static void setupBeforeClass() {
    redisPort = String.valueOf(AvailablePortHelper.getRandomAvailableTCPPort());
  }

  protected Jedis defaultJedisInstance() {
    return new Jedis(hostName, Integer.parseInt(redisPort), 100000);
  }

  @Before
  public void setUp() throws IOException {
    hostName = SocketCreator.getLocalHost().getHostName();
    cache = (GemFireCacheImpl) createCacheInstance(getDefaultRedisCacheProperties());
    rand = new Random(System.nanoTime());
  }

  protected Properties getDefaultRedisCacheProperties() {
    Properties properties = new Properties();
    properties.setProperty(LOG_LEVEL, "config");
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(LOCATORS, "");
    properties.setProperty(REDIS_PORT, redisPort);
    return properties;
  }

  protected Cache createCacheInstance(Properties properties) {
    CacheFactory cacheFactory = new CacheFactory(properties);
    return cacheFactory.create();
  }

  @After
  public void tearDown() throws InterruptedException {
//    if (jedis != null) {
//      jedis.flushAll();
//      jedis.shutdown();
//    }
//    GeodeRedisService service = cache.getService(GeodeRedisService.class);
//    service.stop();
    if (cache != null) {
      cache.close();
    }
  }

  protected String randString() {
    int length = rand.nextInt(8) + 5;
    StringBuilder rString = new StringBuilder();
    for (int i = 0; i < length; i++) {
      rString.append((char) (rand.nextInt(57) + 65));
    }
    // return rString.toString();
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }

  protected void shutdownCache() {
    if (cache != null) {
      cache.close();
    }
  }
}
