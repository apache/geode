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
package org.apache.geode.redis.internal;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import com.github.davidmoten.geo.LatLong;

public class GeoRadiusResponseElement {
  private String name;
  private Optional<LatLong> coord;
  private Double distFromCenter;
  private boolean showDist;
  private Optional<String> hash;

  public String getName() {
    return name;
  }

  public Optional<LatLong> getCoord() {
    return coord;
  }

  public Double getDistFromCenter() {
    return distFromCenter;
  }

  public boolean isShowDist() {
    return showDist;
  }

  public Optional<String> getHash() {
    return hash;
  }

  public GeoRadiusResponseElement(String n, Optional<LatLong> c, Double d, boolean sh,
      Optional<String> h) {
    this.name = n;
    this.coord = c;
    this.distFromCenter = d;
    this.showDist = sh;
    this.hash = h;
  }

  public static void sortByDistanceAscending(List<GeoRadiusResponseElement> elements) {
    Collections.sort(elements, Comparator.comparing(GeoRadiusResponseElement::getDistFromCenter));
  }

  public static void sortByDistanceDescending(List<GeoRadiusResponseElement> elements) {
    Collections.sort(elements,
        Comparator.comparing((GeoRadiusResponseElement x) -> -1.0 * x.getDistFromCenter()));
  }
}
