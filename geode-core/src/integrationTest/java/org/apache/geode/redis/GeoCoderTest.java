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

import org.apache.geode.redis.internal.CoderException;
import org.apache.geode.redis.internal.GeoCoder;
import org.apache.geode.redis.internal.HashArea;
import org.apache.geode.redis.internal.HashNeighbors;
import org.apache.geode.redis.internal.org.apache.hadoop.fs.GeoCoord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class GeoCoderTest {

  @Test
  public void testGeoHash() throws CoderException {
    String hash = GeoCoder.geoHash(Double.toString(13.361389).getBytes(), Double.toString(38.115556).getBytes(), 60);
    assertEquals("sqc8b49rnyte", hash);
    hash = GeoCoder.geoHash(Double.toString(13.361389).getBytes(), Double.toString(38.115556).getBytes(), 52);
    assertEquals("sqc8b49rny0", hash);
    hash = GeoCoder.geoHash(Double.toString(15.087269).getBytes(), Double.toString(37.502669).getBytes(), 52);
    assertEquals("sqdtr74hyu0", hash);
    hash = GeoCoder.geoHash(Double.toString(13.361389).getBytes(), Double.toString(38.115556).getBytes(), 6);
    assertEquals("s0", hash);
  }

  @Test
  public void testGeoPos() throws CoderException {
    GeoCoord pos = GeoCoder.geoPos("sqc8b49rnyte");
    assertEquals(13.361389, pos.getLongitude(), 0.000001);
    assertEquals(38.115556, pos.getLatitude(), 0.000001);
  }

  @Test
  public void testEstimateStepsByRadius() {
    int steps;
    steps = GeoCoder.geohashEstimateStepsByRadius(2000000, 0, 26);
    assertEquals(3, steps);

    steps = GeoCoder.geohashEstimateStepsByRadius(2000000, 71, 26);
    assertEquals(2, steps);

    steps = GeoCoder.geohashEstimateStepsByRadius(2000000, 81, 26);
    assertEquals(1, steps);

    steps = GeoCoder.geohashEstimateStepsByRadius(100000, 15, 26);
    assertEquals(7, steps);
  }

  @Test
  public void testGetNeighbors() throws CoderException {
    HashNeighbors hn1 = GeoCoder.getNeighbors("sq");
    assertEquals("sq", hn1.center);
    assertEquals("sn", hn1.west);
    assertEquals("sw", hn1.east);
    assertEquals("sr", hn1.north);
    assertEquals("sm", hn1.south);
    assertEquals("sp", hn1.northwest);
    assertEquals("sx", hn1.northeast);
    assertEquals("sj", hn1.southwest);
    assertEquals("st", hn1.southeast);

    HashNeighbors hn2 = GeoCoder.getNeighbors("s");
    assertEquals("s", hn2.center);
    assertEquals("e", hn2.west);
    assertEquals("t", hn2.east);
    assertEquals("u", hn2.north);
    assertEquals("k", hn2.south);
    assertEquals("g", hn2.northwest);
    assertEquals("v", hn2.northeast);
    assertEquals("7", hn2.southwest);
    assertEquals("m", hn2.southeast);

    // Test boundary conditions at the poles
    HashNeighbors hn3 = GeoCoder.getNeighbors("c");
    assertEquals("c", hn3.center);
    assertEquals("b", hn3.west);
    assertEquals("f", hn3.east);
    assertEquals("c", hn3.north);
    assertEquals("9", hn3.south);
    assertEquals("b", hn3.northwest);
    assertEquals("f", hn3.northeast);
    assertEquals("8", hn3.southwest);
    assertEquals("d", hn3.southeast);

    HashNeighbors hn4 = GeoCoder.getNeighbors("1");
    assertEquals("1", hn4.center);
    assertEquals("0", hn4.west);
    assertEquals("4", hn4.east);
    assertEquals("3", hn4.north);
    assertEquals("1", hn4.south);
    assertEquals("2", hn4.northwest);
    assertEquals("6", hn4.northeast);
    assertEquals("0", hn4.southwest);
    assertEquals("4", hn4.southeast);

    // Test boundary conditions at international date line
    HashNeighbors hn5 = GeoCoder.getNeighbors("2");
    assertEquals("2", hn5.center);
    assertEquals("r", hn5.west);
    assertEquals("3", hn5.east);
    assertEquals("8", hn5.north);
    assertEquals("0", hn5.south);
    assertEquals("x", hn5.northwest);
    assertEquals("9", hn5.northeast);
    assertEquals("p", hn5.southwest);
    assertEquals("1", hn5.southeast);
  }

  @Test
  public void testGeoHashTile() {
    HashArea t;
    t = GeoCoder.geoHashTile("9");
    assertEquals(0, t.minlat, 0.0);
    assertEquals(-135.0, t.minlon, 0.0);
    assertEquals(45, t.maxlat, 0.0);
    assertEquals(-90, t.maxlon, 0.0);

    //34.40917, -119.70703, 34.45312, -119.66308
    t = GeoCoder.geoHashTile("9q4gu");
    assertEquals(34.40917, t.minlat, 0.00001);
    assertEquals(-119.70703, t.minlon, 0.00001);
    assertEquals(34.45312, t.maxlat, 0.00001);
    assertEquals(-119.66308, t.maxlon, 0.00001);

    //34.41926, -119.69849, 34.41930, -119.69844
    t = GeoCoder.geoHashTile("9q4gu1y4z");
    assertEquals(34.41926, t.minlat, 0.00001);
    assertEquals(-119.69849, t.minlon, 0.00001);
    assertEquals(34.41930, t.maxlat, 0.00001);
    assertEquals(-119.69844, t.maxlon, 0.00001);
  }
}
