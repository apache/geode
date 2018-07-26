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

import org.junit.Test;

import org.apache.geode.redis.internal.CoderException;
import org.apache.geode.redis.internal.GeoCoder;
import org.apache.geode.redis.internal.HashArea;
import org.apache.geode.redis.internal.HashNeighbors;
import org.apache.geode.redis.internal.org.apache.hadoop.fs.GeoCoord;

public class GeoCoderTest {

  @Test
  public void testGeoHash() throws CoderException {
    String hash = GeoCoder.geoHash(Double.toString(13.361389).getBytes(),
        Double.toString(38.115556).getBytes(), 60);
    assertEquals("sqc8b49rnyte", hash);
    hash = GeoCoder.geoHash(Double.toString(13.361389).getBytes(),
        Double.toString(38.115556).getBytes(), 52);
    assertEquals("sqc8b49rny3", hash);
    hash = GeoCoder.geoHash(Double.toString(15.087269).getBytes(),
        Double.toString(37.502669).getBytes(), 52);
    assertEquals("sqdtr74hyu0", hash);
    hash = GeoCoder.geoHash(Double.toString(13.361389).getBytes(),
        Double.toString(38.115556).getBytes(), 6);
    assertEquals("s1", hash);
  }

  @Test
  public void testGeoPos() throws CoderException {
    GeoCoord pos = GeoCoder.geoPos(GeoCoder.hashToBits("sqc8b49rnyte"));
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
    HashNeighbors hn1 = GeoCoder.getNeighbors(GeoCoder.hashToBits("sq"));
    assertEquals("sq", GeoCoder.bitsToHash(hn1.center.toCharArray()));
    assertEquals("sn", GeoCoder.bitsToHash(hn1.west.toCharArray()));
    assertEquals("sw", GeoCoder.bitsToHash(hn1.east.toCharArray()));
    assertEquals("sr", GeoCoder.bitsToHash(hn1.north.toCharArray()));
    assertEquals("sm", GeoCoder.bitsToHash(hn1.south.toCharArray()));
    assertEquals("sp", GeoCoder.bitsToHash(hn1.northwest.toCharArray()));
    assertEquals("sx", GeoCoder.bitsToHash(hn1.northeast.toCharArray()));
    assertEquals("sj", GeoCoder.bitsToHash(hn1.southwest.toCharArray()));
    assertEquals("st", GeoCoder.bitsToHash(hn1.southeast.toCharArray()));

    HashNeighbors hn2 = GeoCoder.getNeighbors(GeoCoder.hashToBits("s"));
    assertEquals("s", GeoCoder.bitsToHash(hn2.center.toCharArray()));
    assertEquals("e", GeoCoder.bitsToHash(hn2.west.toCharArray()));
    assertEquals("t", GeoCoder.bitsToHash(hn2.east.toCharArray()));
    assertEquals("u", GeoCoder.bitsToHash(hn2.north.toCharArray()));
    assertEquals("k", GeoCoder.bitsToHash(hn2.south.toCharArray()));
    assertEquals("g", GeoCoder.bitsToHash(hn2.northwest.toCharArray()));
    assertEquals("v", GeoCoder.bitsToHash(hn2.northeast.toCharArray()));
    assertEquals("7", GeoCoder.bitsToHash(hn2.southwest.toCharArray()));
    assertEquals("m", GeoCoder.bitsToHash(hn2.southeast.toCharArray()));

    // Test boundary conditions at the poles
    HashNeighbors hn3 = GeoCoder.getNeighbors(GeoCoder.hashToBits("c"));
    assertEquals("c", GeoCoder.bitsToHash(hn3.center.toCharArray()));
    assertEquals("b", GeoCoder.bitsToHash(hn3.west.toCharArray()));
    assertEquals("f", GeoCoder.bitsToHash(hn3.east.toCharArray()));
    assertEquals("c", GeoCoder.bitsToHash(hn3.north.toCharArray()));
    assertEquals("9", GeoCoder.bitsToHash(hn3.south.toCharArray()));
    assertEquals("b", GeoCoder.bitsToHash(hn3.northwest.toCharArray()));
    assertEquals("f", GeoCoder.bitsToHash(hn3.northeast.toCharArray()));
    assertEquals("8", GeoCoder.bitsToHash(hn3.southwest.toCharArray()));
    assertEquals("d", GeoCoder.bitsToHash(hn3.southeast.toCharArray()));

    HashNeighbors hn4 = GeoCoder.getNeighbors(GeoCoder.hashToBits("1"));
    assertEquals("1", GeoCoder.bitsToHash(hn4.center.toCharArray()));
    assertEquals("0", GeoCoder.bitsToHash(hn4.west.toCharArray()));
    assertEquals("4", GeoCoder.bitsToHash(hn4.east.toCharArray()));
    assertEquals("3", GeoCoder.bitsToHash(hn4.north.toCharArray()));
    assertEquals("1", GeoCoder.bitsToHash(hn4.south.toCharArray()));
    assertEquals("2", GeoCoder.bitsToHash(hn4.northwest.toCharArray()));
    assertEquals("6", GeoCoder.bitsToHash(hn4.northeast.toCharArray()));
    assertEquals("0", GeoCoder.bitsToHash(hn4.southwest.toCharArray()));
    assertEquals("4", GeoCoder.bitsToHash(hn4.southeast.toCharArray()));

    // Test boundary conditions at international date line
    HashNeighbors hn5 = GeoCoder.getNeighbors(GeoCoder.hashToBits("2"));
    assertEquals("2", GeoCoder.bitsToHash(hn5.center.toCharArray()));
    assertEquals("r", GeoCoder.bitsToHash(hn5.west.toCharArray()));
    assertEquals("3", GeoCoder.bitsToHash(hn5.east.toCharArray()));
    assertEquals("8", GeoCoder.bitsToHash(hn5.north.toCharArray()));
    assertEquals("0", GeoCoder.bitsToHash(hn5.south.toCharArray()));
    assertEquals("x", GeoCoder.bitsToHash(hn5.northwest.toCharArray()));
    assertEquals("9", GeoCoder.bitsToHash(hn5.northeast.toCharArray()));
    assertEquals("p", GeoCoder.bitsToHash(hn5.southwest.toCharArray()));
    assertEquals("1", GeoCoder.bitsToHash(hn5.southeast.toCharArray()));
  }

  @Test
  public void testGeoHashTile() {
    HashArea t;
    t = GeoCoder.geoHashTile(GeoCoder.hashToBits("9"));
    assertEquals(0, t.minlat, 0.0);
    assertEquals(-135.0, t.minlon, 0.0);
    assertEquals(45, t.maxlat, 0.0);
    assertEquals(-90, t.maxlon, 0.0);

    // 34.40917, -119.70703, 34.45312, -119.66308
    t = GeoCoder.geoHashTile(GeoCoder.hashToBits("9q4gu"));
    assertEquals(34.40917, t.minlat, 0.00001);
    assertEquals(-119.70703, t.minlon, 0.00001);
    assertEquals(34.45312, t.maxlat, 0.00001);
    assertEquals(-119.66308, t.maxlon, 0.00001);

    // 34.41926, -119.69849, 34.41930, -119.69844
    t = GeoCoder.geoHashTile(GeoCoder.hashToBits("9q4gu1y4z"));
    assertEquals(34.41926, t.minlat, 0.00001);
    assertEquals(-119.69849, t.minlon, 0.00001);
    assertEquals(34.41930, t.maxlat, 0.00001);
    assertEquals(-119.69844, t.maxlon, 0.00001);
  }
}
