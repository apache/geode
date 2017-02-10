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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import redis.clients.jedis.Jedis;

@Category(IntegrationTest.class)
public class ListsJUnitTest extends RedisTestBase {
  @Test
  public void testLindex() {
    try (Jedis jedis = defaultJedisInstance()) {
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
  }

  @Test
  public void testLPopRPush() {
    try (Jedis jedis = defaultJedisInstance()) {
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
  }

  @Test
  public void testRPopLPush() {
    try (Jedis jedis = defaultJedisInstance()) {
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
  }

  @Test
  public void testLRange() {
    try (Jedis jedis = defaultJedisInstance()) {
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
  }

  @Test
  public void testLTrim() {
    try (Jedis jedis = defaultJedisInstance()) {
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
  }

  @Test
  public void testLRPushX() {
    try (Jedis jedis = defaultJedisInstance()) {
      String key = randString();
      String otherKey = "Other key";
      jedis.lpush(key, randString());
      assertTrue(jedis.lpushx(key, randString()) > 0);
      assertTrue(jedis.rpushx(key, randString()) > 0);

      assertTrue(jedis.lpushx(otherKey, randString()) == 0);
      assertTrue(jedis.rpushx(otherKey, randString()) == 0);

      jedis.del(key);

      assertTrue(jedis.lpushx(key, randString()) == 0);
      assertTrue(jedis.rpushx(key, randString()) == 0);
    }
  }

  @Test
  public void testLRem() {
    try (Jedis jedis = defaultJedisInstance()) {
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
  }

  @Test
  public void testLSet() {
    try (Jedis jedis = defaultJedisInstance()) {
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
  }

}
