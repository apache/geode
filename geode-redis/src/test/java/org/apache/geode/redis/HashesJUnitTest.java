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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.geode.test.junit.categories.FlakyTest;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import redis.clients.jedis.Jedis;

@Category(IntegrationTest.class)
public class HashesJUnitTest extends RedisTestBase {

  @Test
  public void testHMSetHSetHLen() {
    try (Jedis jedis = defaultJedisInstance()) {
      int num = 10;
      String key = randString();
      Map<String, String> hash = new HashMap<String, String>();
      for (int i = 0; i < num; i++) {
        hash.put(randString(), randString());
      }
      String response = jedis.hmset(key, hash);
      assertTrue(response.equals("OK"));
      assertEquals(new Long(hash.size()), jedis.hlen(key));

      key = randString();
      hash = new HashMap<String, String>();
      for (int i = 0; i < num; i++) {
        hash.put(randString(), randString());
      }
      Set<String> keys = hash.keySet();
      Long count = 1L;
      for (String field : keys) {
        Long res = jedis.hset(key, field, hash.get(field));
        assertTrue(res == 1L);
        assertEquals(count++, jedis.hlen(key));
      }
    }
  }

  @Test
  public void testHMGetHDelHGetAllHVals() {
    try (Jedis jedis = defaultJedisInstance()) {
      String key = randString();
      Map<String, String> hash = new HashMap<String, String>();
      for (int i = 0; i < 10; i++) {
        String m = randString();
        String f = randString();
        hash.put(m, f);
      }
      jedis.hmset(key, hash);
      Set<String> keys = hash.keySet();
      String[] keyArray = keys.toArray(new String[keys.size()]);
      List<String> retList = jedis.hmget(key, keyArray);

      for (int i = 0; i < keys.size(); i++) {
        assertEquals(retList.get(i), hash.get(keyArray[i]));
      }

      Map<String, String> retMap = jedis.hgetAll(key);

      assertEquals(retMap, hash);

      List<String> retVals = jedis.hvals(key);
      Set<String> retSet = new HashSet<String>(retVals);

      assertTrue(retSet.containsAll(hash.values()));

      jedis.hdel(key, keyArray);
      assertTrue(jedis.hlen(key) == 0);
    }
  }

  @Test
  public void testHkeys() {
    try (Jedis jedis = defaultJedisInstance()) {
      String key = randString();
      Map<String, String> hash = new HashMap<String, String>();
      for (int i = 0; i < 10; i++) {
        hash.put(randString(), randString());
      }
      String response = jedis.hmset(key, hash);

      Set<String> keys = hash.keySet();
      Set<String> retSet = jedis.hkeys(key);

      assertTrue(retSet.containsAll(keys));
    }
  }

  @Category(FlakyTest.class) // GEODE-1942
  @Test
  public void testHIncrBy() {
    try (Jedis jedis = defaultJedisInstance()) {
      String key = randString();
      String field = randString();

      Long incr = (long) rand.nextInt(50);
      if (incr == 0) {
        incr++;
      }

      long response1 = jedis.hincrBy(key, field, incr);
      assertTrue(response1 == incr);

      long response2 = jedis.hincrBy(randString(), randString(), incr);
      assertTrue(response2 == incr);

      long response3 = jedis.hincrBy(key, field, incr);
      assertTrue(response3 == 2 * incr);

      String field1 = randString();
      Exception ex = null;
      try {
        jedis.hincrBy(key, field1, Long.MAX_VALUE);
        jedis.hincrBy(key, field1, incr);
      } catch (Exception e) {
        ex = e;
      }

      assertNotNull(ex);
    }
  }
}
