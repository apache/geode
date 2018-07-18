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
import static org.apache.geode.internal.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.StringWrapper;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
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
  public void testGeoAdd() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    memberCoordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    memberCoordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    Long l = jedis.geoadd("Sicily", memberCoordinateMap);
    assertTrue(l == 2L);

    Region<ByteArrayWrapper, StringWrapper> sicilyRegion = cache.getRegion("Sicily");
    assertNotNull("Expected region to be not NULL", sicilyRegion);

    // Check GeoHash
    assertEquals(
        sicilyRegion.get(new ByteArrayWrapper(new String("Palermo").getBytes())).toString(),
        "sqc8b49rnyte");
    assertEquals(
        sicilyRegion.get(new ByteArrayWrapper(new String("Catania").getBytes())).toString(),
        "sqdtr74hyu5n");
  }

  @Test
  public void testGeoAddBoundaries() {
    Exception ex = null;
    try {
      jedis.geoadd("Sicily", 13.36, 91.0, "Palermo");
    } catch (Exception e) {
      ex = e;
    }
    assertNotNull(ex);

    ex = null;
    try {
      jedis.geoadd("Sicily", -181, 38.12, "Palermo");
    } catch (Exception e) {
      ex = e;
    }
    assertNotNull(ex);
  }

  @Test
  public void testGeoHash() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    memberCoordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    memberCoordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    Long l = jedis.geoadd("Sicily", memberCoordinateMap);
    assertTrue(l == 2L);

    List<String> hashes = jedis.geohash("Sicily", "Palermo", "Catania", "Rome");

    assertEquals("sqc8b49rnyte", hashes.get(0));
    assertEquals("sqdtr74hyu5n", hashes.get(1));
    assertEquals(null, hashes.get(2));
  }

  @Test
  public void testGeoPos() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    memberCoordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    memberCoordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    Long l = jedis.geoadd("Sicily", memberCoordinateMap);
    assertTrue(l == 2L);

    List<GeoCoordinate> positions = jedis.geopos("Sicily", "Palermo", "Catania", "Rome");

    assertEquals(13.361389, positions.get(0).getLongitude(), 0.000001);
    assertEquals(38.115556, positions.get(0).getLatitude(), 0.000001);
    assertEquals(15.087269, positions.get(1).getLongitude(), 0.000001);
    assertEquals(37.502669, positions.get(1).getLatitude(), 0.000001);
    assertEquals(null, positions.get(2));
  }

  @Test
  public void testGeoDist() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    memberCoordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    memberCoordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    Long l = jedis.geoadd("Sicily", memberCoordinateMap);
    assertTrue(l == 2L);

    Double dist = jedis.geodist("Sicily", "Palermo", "Catania");
    assertEquals(166274.1516, dist, 5.0);

    dist = jedis.geodist("Sicily", "Palermo", "Catania", GeoUnit.KM);
    assertEquals(166.2742, dist, 0.005);

    dist = jedis.geodist("Sicily", "Palermo", "Catania", GeoUnit.M);
    assertEquals(166274.1516, dist, 5.0);

    dist = jedis.geodist("Sicily", "Palermo", "Catania", GeoUnit.MI);
    assertEquals(103.3182, dist, 0.003);

    dist = jedis.geodist("Sicily", "Palermo", "Catania", GeoUnit.FT);
    assertEquals(545520.0960, dist, 15.0);

    dist = jedis.geodist("Sicily", "Palermo", "Foo");
    assertNull(dist);
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
