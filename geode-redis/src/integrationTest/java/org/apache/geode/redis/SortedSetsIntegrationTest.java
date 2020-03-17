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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class SortedSetsIntegrationTest {
  static Jedis jedis;
  private static Random rand = new Random();
  private static GeodeRedisServer server;
  private static GemFireCache cache;
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
  public void testZAddZRange() {
    int numMembers = 10;
    String key = randString();
    Map<String, Double> scoreMembers = new HashMap<>();

    for (int i = 0; i < numMembers; i++)
      scoreMembers.put(randString(), rand.nextDouble());

    jedis.zadd(key, scoreMembers);
    int k = 0;
    for (String entry : scoreMembers.keySet())
      assertNotNull(jedis.zscore(key, entry));

    Set<Tuple> results = jedis.zrangeWithScores(key, 0, -1);
    Map<String, Double> resultMap = new HashMap<String, Double>();
    for (Tuple t : results) {
      resultMap.put(t.getElement(), t.getScore());
    }

    assertEquals(scoreMembers, resultMap);

    for (int i = 0; i < 10; i++) {
      int start;
      int stop;
      do {
        start = rand.nextInt(numMembers);
        stop = rand.nextInt(numMembers);
      } while (start > stop);
      results = jedis.zrangeWithScores(key, start, stop);
      List<Entry<String, Double>> resultList = new ArrayList<Entry<String, Double>>();
      for (Tuple t : results)
        resultList.add(new AbstractMap.SimpleEntry<String, Double>(t.getElement(), t.getScore()));
      List<Entry<String, Double>> list =
          new ArrayList<Entry<String, Double>>(scoreMembers.entrySet());
      Collections.sort(list, new EntryCmp());
      list = list.subList(start, stop + 1);
      assertEquals(list, resultList);
    }
  }

  @Test
  public void testZRevRange() {
    int numMembers = 10;
    String key = randString();

    Map<String, Double> scoreMembers = new HashMap<String, Double>();

    for (int i = 0; i < numMembers; i++)
      scoreMembers.put(randString(), rand.nextDouble());

    jedis.zadd(key, scoreMembers);

    Set<Tuple> results;

    for (int i = 0; i < 10; i++) {
      int start;
      int stop;
      do {
        start = rand.nextInt(numMembers);
        stop = rand.nextInt(numMembers);
      } while (start > stop);
      results = jedis.zrevrangeWithScores(key, start, stop);
      List<Entry<String, Double>> resultList = new ArrayList<Entry<String, Double>>();
      for (Tuple t : results)
        resultList.add(new AbstractMap.SimpleEntry<String, Double>(t.getElement(), t.getScore()));
      List<Entry<String, Double>> list =
          new ArrayList<Entry<String, Double>>(scoreMembers.entrySet());
      Collections.sort(list, new EntryRevCmp());
      list = list.subList(start, stop + 1);
      assertEquals(list, resultList);
    }
  }

  @Test
  public void testZCount() {
    int num = 10;
    int runs = 2;
    for (int i = 0; i < runs; i++) {
      Double min;
      Double max;
      do {
        min = rand.nextDouble();
        max = rand.nextDouble();
      } while (min > max);


      int count = 0;

      String key = randString();
      Map<String, Double> scoreMembers = new HashMap<String, Double>();

      for (int j = 0; j < num; j++) {
        Double nextDouble = rand.nextDouble();
        if (nextDouble >= min && nextDouble <= max)
          count++;
        scoreMembers.put(randString(), nextDouble);
      }

      jedis.zadd(key, scoreMembers);
      Long countResult = jedis.zcount(key, min, max);
      assertTrue(count == countResult);
    }

  }

  @Test
  public void testZIncrBy() {
    String key = randString();
    String member = randString();
    Double score = 0.0;
    for (int i = 0; i < 20; i++) {
      Double incr = rand.nextDouble();
      Double result = jedis.zincrby(key, incr, member);
      score += incr;
      assertEquals(score, result, 1.0 / 100000000.0);
    }


    jedis.zincrby(key, Double.MAX_VALUE, member);
    Double infResult = jedis.zincrby(key, Double.MAX_VALUE, member);


    assertEquals(infResult, Double.valueOf(Double.POSITIVE_INFINITY));
  }

  @Test
  public void testZRangeByScore() {
    Double min;
    Double max;
    for (int j = 0; j < 2; j++) {
      do {
        min = rand.nextDouble();
        max = rand.nextDouble();
      } while (min > max);
      int numMembers = 500;
      String key = randString();
      Map<String, Double> scoreMembers = new HashMap<String, Double>();
      List<Entry<String, Double>> expected = new ArrayList<Entry<String, Double>>();
      for (int i = 0; i < numMembers; i++) {
        String s = randString();
        Double d = rand.nextDouble();
        scoreMembers.put(s, d);
        if (d > min && d < max)
          expected.add(new AbstractMap.SimpleEntry<String, Double>(s, d));
      }
      jedis.zadd(key, scoreMembers);
      Set<Tuple> results = jedis.zrangeByScoreWithScores(key, min, max);
      List<Entry<String, Double>> resultList = new ArrayList<Entry<String, Double>>();
      for (Tuple t : results)
        resultList.add(new AbstractMap.SimpleEntry<String, Double>(t.getElement(), t.getScore()));
      Collections.sort(expected, new EntryCmp());

      assertEquals(expected, resultList);
      jedis.del(key);
    }
  }

  @Test
  public void testZRevRangeByScore() {
    Double min;
    Double max;
    for (int j = 0; j < 2; j++) {
      do {
        min = rand.nextDouble();
        max = rand.nextDouble();
      } while (min > max);
      int numMembers = 500;
      String key = randString();
      Map<String, Double> scoreMembers = new HashMap<String, Double>();
      List<Entry<String, Double>> expected = new ArrayList<Entry<String, Double>>();
      for (int i = 0; i < numMembers; i++) {
        String s = randString();
        Double d = rand.nextDouble();
        scoreMembers.put(s, d);
        if (d > min && d < max)
          expected.add(new AbstractMap.SimpleEntry<String, Double>(s, d));
      }
      jedis.zadd(key, scoreMembers);
      Set<Tuple> results = jedis.zrevrangeByScoreWithScores(key, max, min);
      List<Entry<String, Double>> resultList = new ArrayList<Entry<String, Double>>();
      for (Tuple t : results)
        resultList.add(new AbstractMap.SimpleEntry<String, Double>(t.getElement(), t.getScore()));
      Collections.sort(expected, new EntryRevCmp());

      assertEquals(expected, resultList);
      jedis.del(key);
    }
  }

  @Test
  public void testZRemZScore() {
    Double min;
    Double max;
    for (int j = 0; j < 2; j++) {
      do {
        min = rand.nextDouble();
        max = rand.nextDouble();
      } while (min > max);
      int numMembers = 5000;
      String key = randString();
      Map<String, Double> scoreMembers = new HashMap<String, Double>();
      List<Entry<String, Double>> expected = new ArrayList<Entry<String, Double>>();
      for (int i = 0; i < numMembers; i++) {
        String s = randString();
        Double d = rand.nextDouble();
        scoreMembers.put(s, d);
        if (d > min && d < max)
          expected.add(new AbstractMap.SimpleEntry<String, Double>(s, d));
      }
      jedis.zadd(key, scoreMembers);
      Collections.sort(expected, new EntryCmp());
      for (int i = expected.size(); i > 0; i--) {
        Entry<String, Double> remEntry = expected.remove(i - 1);
        String rem = remEntry.getKey();
        Double val = remEntry.getValue();
        assertEquals(val, jedis.zscore(key, rem));

        assertTrue(jedis.zrem(key, rem) == 1);
      }
      String s = randString();
      if (!expected.contains(s))
        assertTrue(jedis.zrem(key, s) == 0);
      jedis.del(key);
    }
  }

  @Test
  public void testZRank() {
    for (int j = 0; j < 2; j++) {
      int numMembers = 10;
      String key = randString();
      Map<String, Double> scoreMembers = new HashMap<String, Double>();
      List<Entry<String, Double>> expected = new ArrayList<Entry<String, Double>>();
      for (int i = 0; i < numMembers; i++) {
        String s = randString();
        Double d = rand.nextDouble();
        scoreMembers.put(s, d);
        expected.add(new AbstractMap.SimpleEntry<String, Double>(s, d));
      }
      Collections.sort(expected, new EntryCmp());
      jedis.zadd(key, scoreMembers);
      for (int i = 0; i < expected.size(); i++) {
        Entry<String, Double> en = expected.get(i);
        String field = en.getKey();
        Long rank = jedis.zrank(key, field);
        assertEquals(new Long(i), rank);
      }
      String field = randString();
      if (!expected.contains(field))
        assertNull(jedis.zrank(key, field));
      jedis.del(key);
    }
  }

  @Test
  public void testZRevRank() {
    for (int j = 0; j < 2; j++) {
      int numMembers = 10;
      String key = randString();
      Map<String, Double> scoreMembers = new HashMap<String, Double>();
      List<Entry<String, Double>> expected = new ArrayList<Entry<String, Double>>();
      for (int i = 0; i < numMembers; i++) {
        String s = randString();
        Double d = rand.nextDouble();
        scoreMembers.put(s, d);
        expected.add(new AbstractMap.SimpleEntry<String, Double>(s, d));
      }
      Collections.sort(expected, new EntryRevCmp());
      jedis.zadd(key, scoreMembers);
      for (int i = 0; i < expected.size(); i++) {
        Entry<String, Double> en = expected.get(i);
        String field = en.getKey();
        Long rank = jedis.zrevrank(key, field);
        assertEquals(new Long(i), rank);
      }
      String field = randString();
      if (!expected.contains(field))
        assertNull(jedis.zrank(key, field));
      jedis.del(key);
    }
  }

  private class EntryCmp implements Comparator<Entry<String, Double>> {

    @Override
    public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
      Double diff = o1.getValue() - o2.getValue();
      if (diff == 0)
        return o2.getKey().compareTo(o1.getKey());
      else
        return diff > 0 ? 1 : -1;
    }

  }

  private class EntryRevCmp implements Comparator<Entry<String, Double>> {

    @Override
    public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
      Double diff = o2.getValue() - o1.getValue();
      if (diff == 0)
        return o1.getKey().compareTo(o2.getKey());
      else
        return diff > 0 ? 1 : -1;
    }

  }

  @Test
  public void testZRemRangeByScore() {
    Double min;
    Double max;
    for (int j = 0; j < 3; j++) {
      do {
        min = rand.nextDouble();
        max = rand.nextDouble();
      } while (min > max);
      int numMembers = 10;
      String key = randString();
      Map<String, Double> scoreMembers = new HashMap<String, Double>();
      List<Entry<String, Double>> fullList = new ArrayList<Entry<String, Double>>();
      List<Entry<String, Double>> toRemoveList = new ArrayList<Entry<String, Double>>();
      for (int i = 0; i < numMembers; i++) {
        String s = randString();
        Double d = rand.nextDouble();
        scoreMembers.put(s, d);
        fullList.add(new AbstractMap.SimpleEntry<String, Double>(s, d));
        if (d > min && d < max)
          toRemoveList.add(new AbstractMap.SimpleEntry<String, Double>(s, d));
      }
      jedis.zadd(key, scoreMembers);
      Long numRemoved = jedis.zremrangeByScore(key, min, max);
      List<Entry<String, Double>> expectedList = new ArrayList<Entry<String, Double>>(fullList);
      expectedList.removeAll(toRemoveList);
      Collections.sort(expectedList, new EntryCmp());
      Set<Tuple> result = jedis.zrangeWithScores(key, 0, -1);
      List<Entry<String, Double>> resultList = new ArrayList<Entry<String, Double>>();
      for (Tuple t : result)
        resultList.add(new AbstractMap.SimpleEntry<String, Double>(t.getElement(), t.getScore()));
      assertEquals(expectedList, resultList);
      jedis.del(key);
    }
  }

  private String randString() {
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
