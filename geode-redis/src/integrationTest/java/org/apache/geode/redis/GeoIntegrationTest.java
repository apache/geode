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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.GeoRadiusParam;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.junit.categories.RedisTest;

@Category({RedisTest.class})
public class GeoIntegrationTest {
  static Jedis jedis;
  private static GeodeRedisServer server;
  private static GemFireCache cache;
  private static int port = 6379;

  @BeforeClass
  public static void setUp() throws IOException {
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

  @After
  public void cleanup() {
    // TODO: GEODE-7909 Correct implementation of flushAll so it deletes Geo* keys properly and this
    // zrem is
    // not needed
    jedis.zrem("Sicily", "Palermo", "Catania");
    jedis.flushAll();
  }

  @AfterClass
  public static void tearDown() {
    jedis.close();
    cache.close();
    server.shutdown();
  }

  @Test
  public void testGeoAdd() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    memberCoordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    memberCoordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    Long l = jedis.geoadd("Sicily", memberCoordinateMap);
    assertTrue(l == 2L);
    // TODO: make sure GEOADD is tested sufficiently without needing to get the region/Geode
    // internals and properly implemented
    // Region<ByteArrayWrapper, ByteArrayWrapper> sicilyRegion = cache.getRegion("Sicily");
    // assertNotNull("Expected region to be not NULL", sicilyRegion);
    //
    // // Check GeoHash
    // String hash =
    // sicilyRegion.get(new ByteArrayWrapper(new String("Palermo").getBytes())).toString();
    // assertEquals("sqc8b49rnyte", hash);
    //
    // hash = sicilyRegion.get(new ByteArrayWrapper(new String("Catania").getBytes())).toString();
    // assertEquals("sqdtr74hyu5n", hash);
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

  @Test
  public void testGeoRadiusBasic() {
    // jedis.flushAll();
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    memberCoordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    memberCoordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    Long l = jedis.geoadd("Sicily", memberCoordinateMap);
    assertThat(l).isEqualTo(2L);

    List<GeoRadiusResponse> gr = jedis.georadius("Sicily", 15.0, 37.0, 100000, GeoUnit.M);
    assertEquals(1, gr.size());
    assertEquals("Catania", gr.get(0).getMemberByString());

    gr = jedis.georadius("Sicily", 15.0, 37.0, 200, GeoUnit.KM);
    assertEquals(2, gr.size());
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Catania")));
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Palermo")));
  }

  @Test
  public void testGeoRadiusWithDist() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    memberCoordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    memberCoordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    Long l = jedis.geoadd("Sicily", memberCoordinateMap);
    assertTrue(l == 2L);

    List<GeoRadiusResponse> gr = jedis.georadius("Sicily", 15.0, 37.0, 100, GeoUnit.KM,
        GeoRadiusParam.geoRadiusParam().withDist());
    assertEquals(1, gr.size());
    assertEquals("Catania", gr.get(0).getMemberByString());
    assertEquals(56.4413, gr.get(0).getDistance(), 0.0001);
  }

  @Test
  public void testGeoRadiusWithCoord() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    memberCoordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    memberCoordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    Long l = jedis.geoadd("Sicily", memberCoordinateMap);
    assertTrue(l == 2L);

    List<GeoRadiusResponse> gr = jedis.georadius("Sicily", 15.0, 37.0, 100, GeoUnit.KM,
        GeoRadiusParam.geoRadiusParam().withCoord());
    assertEquals(1, gr.size());
    assertEquals("Catania", gr.get(0).getMemberByString());
    assertEquals(15.087269, gr.get(0).getCoordinate().getLongitude(), 0.0001);
    assertEquals(37.502669, gr.get(0).getCoordinate().getLatitude(), 0.0001);
  }

  @Test
  public void testGeoRadiusCount() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    memberCoordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    memberCoordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    Long l = jedis.geoadd("Sicily", memberCoordinateMap);
    assertTrue(l == 2L);

    List<GeoRadiusResponse> gr = jedis.georadius("Sicily", 15.0, 37.0, 200,
        GeoUnit.KM, GeoRadiusParam.geoRadiusParam().count(1));
    assertEquals(1, gr.size());
  }

  @Test
  public void testGeoRadiusOrdered() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    memberCoordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    memberCoordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    Long l = jedis.geoadd("Sicily", memberCoordinateMap);
    assertTrue(l == 2L);

    List<GeoRadiusResponse> gr = jedis.georadius("Sicily", 15.0, 37.0, 200,
        GeoUnit.KM, GeoRadiusParam.geoRadiusParam().sortAscending());
    assertEquals(2, gr.size());
    assertEquals("Catania", gr.get(0).getMemberByString());
    assertEquals("Palermo", gr.get(1).getMemberByString());

    gr = jedis.georadius("Sicily", 15.0, 37.0, 200,
        GeoUnit.KM, GeoRadiusParam.geoRadiusParam().sortDescending());
    assertEquals(2, gr.size());
    assertEquals("Palermo", gr.get(0).getMemberByString());
    assertEquals("Catania", gr.get(1).getMemberByString());
  }

  @Test
  public void testGeoRadiusWithCoordDistCountAsc() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    memberCoordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    memberCoordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    Long l = jedis.geoadd("Sicily", memberCoordinateMap);
    assertTrue(l == 2L);

    List<GeoRadiusResponse> gr = jedis.georadius("Sicily", 15.0, 37.0, 200, GeoUnit.KM,
        GeoRadiusParam.geoRadiusParam().withCoord().withDist().count(1).sortAscending());
    assertEquals(1, gr.size());
    assertEquals("Catania", gr.get(0).getMemberByString());
    assertEquals(15.087269, gr.get(0).getCoordinate().getLongitude(), 0.0001);
    assertEquals(37.502669, gr.get(0).getCoordinate().getLatitude(), 0.0001);
    assertEquals(56.4413, gr.get(0).getDistance(), 0.0001);
  }

  @Test
  public void testGeoRadiusByMemberBasic() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    memberCoordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    memberCoordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    Long l = jedis.geoadd("Sicily", memberCoordinateMap);
    assertTrue(l == 2L);

    List<GeoRadiusResponse> gr = jedis.georadiusByMember("Sicily", "Catania", 250, GeoUnit.KM);
    assertEquals(1, gr.size());
    assertEquals("Palermo", gr.get(0).getMemberByString());
  }

  @Test
  public void testGeoRadiusByMemberWithDist() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    memberCoordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    memberCoordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    Long l = jedis.geoadd("Sicily", memberCoordinateMap);
    assertTrue(l == 2L);

    List<GeoRadiusResponse> gr = jedis.georadiusByMember("Sicily", "Catania", 250,
        GeoUnit.KM, GeoRadiusParam.geoRadiusParam().withDist());
    assertEquals(1, gr.size());
    assertEquals("Palermo", gr.get(0).getMemberByString());
    assertEquals(166.2742, gr.get(0).getDistance(), 0.0001);
  }

  @Test
  public void testGeoRadiusByMemberWithCoord() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    memberCoordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    memberCoordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    Long l = jedis.geoadd("Sicily", memberCoordinateMap);
    assertTrue(l == 2L);

    List<GeoRadiusResponse> gr = jedis.georadiusByMember("Sicily", "Catania", 250,
        GeoUnit.KM, GeoRadiusParam.geoRadiusParam().withCoord());
    assertEquals(1, gr.size());
    assertEquals("Palermo", gr.get(0).getMemberByString());
    assertEquals(13.361389, gr.get(0).getCoordinate().getLongitude(), 0.0001);
    assertEquals(38.115556, gr.get(0).getCoordinate().getLatitude(), 0.0001);
  }


  @Test
  public void testGeoRadiusByMemberFull() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    // 47.599246, -122.333826
    memberCoordinateMap.put("Galvanize", new GeoCoordinate(-122.333826, 47.599246));
    // 47.600249, -122.331166
    memberCoordinateMap.put("Flatstick", new GeoCoordinate(-122.331166, 47.600249));
    // 47.601120, -122.332063
    memberCoordinateMap.put("Fuel Sports", new GeoCoordinate(-122.332063, 47.601120));
    // 47.600657, -122.334362
    memberCoordinateMap.put("Central Saloon", new GeoCoordinate(-122.334362, 47.600657));
    // 47.598519, -122.334405
    memberCoordinateMap.put("Cowgirls", new GeoCoordinate(-122.334405, 47.598519));

    // 47.608336, -122.340746
    memberCoordinateMap.put("Jarrbar", new GeoCoordinate(-122.340746, 47.608336));
    // 47.612499, -122.336871
    memberCoordinateMap.put("Oliver's lounge", new GeoCoordinate(-122.336871, 47.612499));
    // 47.612622, -122.320288
    memberCoordinateMap.put("Garage", new GeoCoordinate(-122.320288, 47.612622));
    // 47.607362, -122.316517
    memberCoordinateMap.put("Ba Bar", new GeoCoordinate(-122.316517, 47.607362));

    // 47.615146, -122.322355
    memberCoordinateMap.put("Bill's", new GeoCoordinate(-122.322355, 47.615146));
    // 47.621821, -122.336747
    memberCoordinateMap.put("Brave Horse Tavern", new GeoCoordinate(-122.336747, 47.621821));

    // 47.616580, -122.200777
    memberCoordinateMap.put("Earl's", new GeoCoordinate(-122.200777, 47.616580));
    // 47.615165, -122.201317
    memberCoordinateMap.put("Wild Ginger", new GeoCoordinate(-122.201317, 47.615165));

    Long l = jedis.geoadd("Seattle", memberCoordinateMap);
    assertTrue(l == 13L);

    List<GeoRadiusResponse> gr = jedis.georadiusByMember("Seattle", "Galvanize", 0.2, GeoUnit.MI);
    assertEquals(4, gr.size());
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Flatstick")));
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Fuel Sports")));
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Central Saloon")));
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Cowgirls")));

    gr = jedis.georadiusByMember("Seattle", "Galvanize", 1.2, GeoUnit.MI);
    assertEquals(8, gr.size());
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Jarrbar")));
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Oliver's lounge")));
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Garage")));
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Ba Bar")));

    gr = jedis.georadiusByMember("Seattle", "Galvanize", 2.0, GeoUnit.MI);
    assertEquals(10, gr.size());
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Bill's")));
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Brave Horse Tavern")));

    gr = jedis.georadiusByMember("Seattle", "Galvanize", 10.0, GeoUnit.MI);
    assertEquals(12, gr.size());
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Earl's")));
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Wild Ginger")));
  }

  @Test
  public void testGeoRadiusByMemberNorth() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    // 66.084124, -18.592468
    memberCoordinateMap.put("Northern Iceland", new GeoCoordinate(-18.592468, 66.084124));
    // 64.203491, -51.726331
    memberCoordinateMap.put("Nuuk", new GeoCoordinate(-51.726331, 64.203491));
    // 55.861822, -4.237179
    memberCoordinateMap.put("Glasgow", new GeoCoordinate(-4.237179, 55.861822));
    // 51.585005, -0.159837
    memberCoordinateMap.put("London", new GeoCoordinate(-0.159837, 51.585005));
    // 59.933071, 10.769027
    memberCoordinateMap.put("Oslo", new GeoCoordinate(10.769027, 59.933071));

    Long l = jedis.geoadd("North", memberCoordinateMap);
    assertTrue(l == 5L);

    List<GeoRadiusResponse> gr = jedis.georadiusByMember("North", "Northern Iceland", 1590.0,
        GeoUnit.KM, GeoRadiusParam.geoRadiusParam().withDist());

    assertEquals(2, gr.size());
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Nuuk")));
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Glasgow")));

    gr = jedis.georadiusByMember("North", "Northern Iceland", 3000.0,
        GeoUnit.KM, GeoRadiusParam.geoRadiusParam().withDist());
    assertEquals(4, gr.size());
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Nuuk")));
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Glasgow")));
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("Oslo")));
    assertTrue(gr.stream().anyMatch(r -> r.getMemberByString().equals("London")));
  }

  @Test
  public void testGeoRadiusByMemberInvalid() {
    Map<String, GeoCoordinate> memberCoordinateMap = new HashMap<>();
    memberCoordinateMap.put("Palermo", new GeoCoordinate(13.361389, 38.115556));
    memberCoordinateMap.put("Catania", new GeoCoordinate(15.087269, 37.502669));
    Long l = jedis.geoadd("Sicily", memberCoordinateMap);
    assertTrue(l == 2L);

    Exception ex = null;
    try {
      jedis.georadiusByMember("Sicily", "Roma", 91.0, GeoUnit.KM);
    } catch (Exception e) {
      ex = e;
    }

    assertNotNull(ex);
    assertTrue(ex.getMessage().contains("could not decode requested zset member"));
  }
}
