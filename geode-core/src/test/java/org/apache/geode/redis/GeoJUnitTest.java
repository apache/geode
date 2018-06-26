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
import org.apache.geode.cache.Region;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.StringWrapper;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.categories.RedisTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

@Category({IntegrationTest.class, RedisTest.class})
public class GeoJUnitTest {
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
  public void testGeoAddSingle() {
    Long l = jedis.geoadd("Sicily", 13.361389, 38.115556, "Palermo");
    assertTrue(l == 1L);

    Region<ByteArrayWrapper, StringWrapper> sicilyRegion = cache.getRegion("Sicily");
    assertNotNull("Expected region to be not NULL", sicilyRegion);

    // Check GeoHash
    assertEquals(sicilyRegion.get(new ByteArrayWrapper(new String("Palermo").getBytes())).toString(), "sqc8b49rnyte");
  }

  @Test
  public void testGeoAddMultiple() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    memberCoordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    memberCoordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    Long l = jedis.geoadd("Sicily", memberCoordinateMap);
    assertTrue(l == 2L);

    Region<ByteArrayWrapper, StringWrapper> sicilyRegion = cache.getRegion("Sicily");
    assertNotNull("Expected region to be not NULL", sicilyRegion);

    // Check GeoHash
    assertEquals(sicilyRegion.get(new ByteArrayWrapper(new String("Palermo").getBytes())).toString(), "sqc8b49rnyte");
    assertEquals(sicilyRegion.get(new ByteArrayWrapper(new String("Catania").getBytes())).toString(), "sqdtr74hyu5n");
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
