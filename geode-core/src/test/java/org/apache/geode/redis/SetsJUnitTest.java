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

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.junit.categories.IntegrationTest;

import org.apache.geode.redis.GeodeRedisServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class SetsJUnitTest {

  private static Jedis jedis;
  private static GeodeRedisServer server;
  private static GemFireCache cache;
  private static Random rand;
  private static int port = 6379;

  @BeforeClass
  public static void setUp() throws IOException {
    rand = new Random();
    CacheFactory cf = new CacheFactory();
    // cf.set("log-file", "redis.log");
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
  public void testSAddScard() {
    int elements = 10;
    Set<String> strings = new HashSet<String>();
    String key = randString();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    Long response = jedis.sadd(key, stringArray);
    assertEquals(response, new Long(strings.size()));

    assertEquals(jedis.scard(key), new Long(strings.size()));
  }

  @Test
  public void testSMembersIsMember() {
    int elements = 10;
    Set<String> strings = new HashSet<String>();
    String key = randString();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.sadd(key, stringArray);

    Set<String> returnedSet = jedis.smembers(key);

    assertEquals(returnedSet, new HashSet<String>(strings));

    for (String entry : strings) {
      boolean exists = jedis.sismember(key, entry);
      assertTrue(exists);
    }
  }

  @Test
  public void testSMove() {
    String source = randString();
    String dest = randString();
    String test = randString();
    int elements = 10;
    Set<String> strings = new HashSet<String>();
    for (int i = 0; i < elements; i++) {
      String elem = randString();
      strings.add(elem);
    }
    String[] stringArray = strings.toArray(new String[strings.size()]);
    jedis.sadd(source, stringArray);

    long i = 1;
    for (String entry : strings) {
      assertTrue(jedis.smove(source, dest, entry) == 1);
      assertTrue(jedis.sismember(dest, entry));
      assertTrue(jedis.scard(source) == strings.size() - i);
      assertTrue(jedis.scard(dest) == i);
      i++;
    }

    assertTrue(jedis.smove(test, dest, randString()) == 0);
  }

  @Test
  public void testSDiffAndStore() {
    int numSets = 3;
    int elements = 10;
    String[] keys = new String[numSets];
    ArrayList<Set<String>> sets = new ArrayList<Set<String>>();
    for (int j = 0; j < numSets; j++) {
      keys[j] = randString();
      Set<String> newSet = new HashSet<String>();
      for (int i = 0; i < elements; i++)
        newSet.add(randString());
      sets.add(newSet);
    }

    for (int i = 0; i < numSets; i++) {
      Set<String> s = sets.get(i);
      String[] stringArray = s.toArray(new String[s.size()]);
      jedis.sadd(keys[i], stringArray);
    }

    Set<String> result = sets.get(0);
    for (int i = 1; i < numSets; i++)
      result.removeAll(sets.get(i));

    assertEquals(result, jedis.sdiff(keys));

    String destination = randString();

    jedis.sdiffstore(destination, keys);

    Set<String> destResult = jedis.smembers(destination);

    assertEquals(result, destResult);

  }

  @Test
  public void testSUnionAndStore() {
    int numSets = 3;
    int elements = 10;
    String[] keys = new String[numSets];
    ArrayList<Set<String>> sets = new ArrayList<Set<String>>();
    for (int j = 0; j < numSets; j++) {
      keys[j] = randString();
      Set<String> newSet = new HashSet<String>();
      for (int i = 0; i < elements; i++)
        newSet.add(randString());
      sets.add(newSet);
    }

    for (int i = 0; i < numSets; i++) {
      Set<String> s = sets.get(i);
      String[] stringArray = s.toArray(new String[s.size()]);
      jedis.sadd(keys[i], stringArray);
    }

    Set<String> result = sets.get(0);
    for (int i = 1; i < numSets; i++)
      result.addAll(sets.get(i));

    assertEquals(result, jedis.sunion(keys));

    String destination = randString();

    jedis.sunionstore(destination, keys);

    Set<String> destResult = jedis.smembers(destination);

    assertEquals(result, destResult);

  }

  @Test
  public void testSInterAndStore() {
    int numSets = 3;
    int elements = 10;
    String[] keys = new String[numSets];
    ArrayList<Set<String>> sets = new ArrayList<Set<String>>();
    for (int j = 0; j < numSets; j++) {
      keys[j] = randString();
      Set<String> newSet = new HashSet<String>();
      for (int i = 0; i < elements; i++)
        newSet.add(randString());
      sets.add(newSet);
    }

    for (int i = 0; i < numSets; i++) {
      Set<String> s = sets.get(i);
      String[] stringArray = s.toArray(new String[s.size()]);
      jedis.sadd(keys[i], stringArray);
    }

    Set<String> result = sets.get(0);
    for (int i = 1; i < numSets; i++)
      result.retainAll(sets.get(i));

    assertEquals(result, jedis.sinter(keys));

    String destination = randString();

    jedis.sinterstore(destination, keys);

    Set<String> destResult = jedis.smembers(destination);

    assertEquals(result, destResult);

  }

  private String randString() {
    int length = rand.nextInt(8) + 5;
    StringBuilder rString = new StringBuilder();
    for (int i = 0; i < length; i++)
      rString.append((char) (rand.nextInt(57) + 65));
    return rString.toString();
    // return Long.toHexString(Double.doubleToLongBits(Math.random()));
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
