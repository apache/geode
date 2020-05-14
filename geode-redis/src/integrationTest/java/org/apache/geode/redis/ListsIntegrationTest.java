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

import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class ListsIntegrationTest {
  static Jedis jedis;
  private static GeodeRedisServer server;
  private static GemFireCache cache;
  private static Random rand = new Random();
  private static int port = 6379;

  @BeforeClass
  public static void setUp() {
    CacheFactory cf = new CacheFactory();
    cf.set(LOG_LEVEL, "error");
    cf.set(MCAST_PORT, "0");
    cf.set(LOCATORS, "");
    cache = cf.create();
    port = AvailablePortHelper.getRandomAvailableTCPPort();
    server = new GeodeRedisServer("localhost", port);

    server.start();
    jedis = new Jedis("localhost", port, 10000000);
  }

  @Test
  public void testLindex() {
    int elements = 50;
    ArrayList<String> strings = new ArrayList<String>();
    String key = randString();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.rpush(key, stringArray);


    for (int i = 0; i < elements; i++) {
      String gemString = jedis.lindex(key, i);
      String s = strings.get(i);
      assertEquals(gemString, s);
    }
  }

  @Test
  public void testLPopRPush() {
    int elements = 50;
    ArrayList<String> strings = new ArrayList<String>();
    String key = randString();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.rpush(key, stringArray);

    for (int i = 0; i < elements; i++) {
      String gemString = jedis.lpop(key);
      String s = strings.get(i);
      assertEquals(s, gemString);
    }
  }

  @Test
  public void testRPopLPush() {
    int elements = 500;
    ArrayList<String> strings = new ArrayList<String>();
    String key = randString();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.lpush(key, stringArray);

    for (int i = 0; i < elements; i++) {
      String gemString = jedis.rpop(key);
      String s = strings.get(i);
      assertEquals(gemString, s);
    }

  }

  @Test
  public void testLRange() {
    int elements = 10;
    ArrayList<String> strings = new ArrayList<String>();
    String key = randString();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.rpush(key, stringArray);

    for (int i = 0; i < elements; i++) {
      List<String> range = jedis.lrange(key, 0, i);
      assertEquals(range, strings.subList(0, i + 1));
    }

    for (int i = 0; i < elements; i++) {
      List<String> range = jedis.lrange(key, i, -1);
      assertEquals(range, strings.subList(i, strings.size()));
    }
  }

  @Test
  public void testLTrim() {
    int elements = 5;
    ArrayList<String> strings = new ArrayList<String>();
    String key = randString();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.rpush(key, stringArray);
    // Take off last element one at a time
    for (int i = elements - 1; i >= 0; i--) {
      jedis.ltrim(key, 0, i);
      List<String> range = jedis.lrange(key, 0, -1);
      assertEquals(range, strings.subList(0, i + 1));
    }
    jedis.rpop(key);
    jedis.rpush(key, stringArray);
    // Take off first element one at a time
    for (int i = 1; i < elements; i++) {
      jedis.ltrim(key, 1, -1);
      List<String> range = jedis.lrange(key, 0, -1);
      List<String> expected = strings.subList(i, strings.size());
      assertEquals(range, expected);
    }
  }

  @Test
  public void testLRPushX() {
    String key = randString();
    String otherKey = "Other key";
    jedis.lpush(key, randString());
    assertTrue(jedis.lpushx(key, randString()) > 0);
    assertTrue(jedis.rpushx(key, randString()) > 0);

    assertTrue(jedis.lpushx(otherKey, randString()) == 0);
    assertTrue(jedis.rpushx(otherKey, randString()) == 0);

    jedis.del(key);

    long response = jedis.lpushx(key, randString());
    assertTrue("response:" + response, response == 0);
    response = jedis.lpushx(key, randString());
    assertTrue("response:" + response, jedis.rpushx(key, randString()) == 0);
  }

  @Test
  public void testLRem() {
    int elements = 5;
    ArrayList<String> strings = new ArrayList<String>();
    String key = randString();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.rpush(key, stringArray);

    for (int i = 0; i < elements; i++) {
      String remove = strings.remove(0);
      jedis.lrem(key, 0, remove);
      List<String> range = jedis.lrange(key, 0, -1);
      assertEquals(strings, range);
    }
  }

  @Test
  public void testLSet() {
    int elements = 10;
    ArrayList<String> strings = new ArrayList<String>();
    String key = randString();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.rpush(key, stringArray);

    for (int i = 0; i < elements; i++) {
      String s = randString();
      strings.set(i, s);
      jedis.lset(key, i, s);
      List<String> range = jedis.lrange(key, 0, -1);
      assertEquals(range, strings);
    }
  }

  private String randString() {
    int length = rand.nextInt(8) + 5;
    StringBuilder rString = new StringBuilder();
    for (int i = 0; i < length; i++)
      rString.append((char) (rand.nextInt(57) + 65));
    return Long.toHexString(Double.doubleToLongBits(Math.random()));
  }

  @After
  public void flushAll() {
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    cache.close();
    server.shutdown();
  }
}
