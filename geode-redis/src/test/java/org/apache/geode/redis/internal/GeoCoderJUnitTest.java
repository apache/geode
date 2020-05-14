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
 *
 */
package org.apache.geode.redis.internal;

import static org.junit.Assert.assertEquals;

import com.github.davidmoten.geo.LatLong;
import org.junit.Test;

public class GeoCoderJUnitTest {
  @Test
  public void testGeoHash() {
    String hash = GeoCoder.geohash(Double.toString(13.361389).getBytes(),
        Double.toString(38.115556).getBytes());
    assertEquals("sqc8b49rnyte", hash);
  }

  @Test
  public void testGeoPos() {
    LatLong pos = GeoCoder.geoPos("sqc8b49rnyte");
    assertEquals(13.361389, pos.getLon(), 0.000001);
    assertEquals(38.115556, pos.getLat(), 0.000001);
  }
}
