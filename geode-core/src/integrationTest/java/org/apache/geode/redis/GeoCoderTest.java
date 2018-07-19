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
}
