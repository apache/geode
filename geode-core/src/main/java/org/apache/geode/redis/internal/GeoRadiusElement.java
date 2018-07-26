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

import org.apache.geode.redis.internal.org.apache.hadoop.fs.GeoCoord;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class GeoRadiusElement {
    private String name;
    private Optional<GeoCoord> coord;
    private Double distFromCenter;
    private boolean showDist;
    private Optional<String> hash;

    public String getName() {
        return name;
    }
    public Optional<GeoCoord> getCoord() {
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

    public GeoRadiusElement(String n, Optional<GeoCoord> c, Double d, boolean sh, Optional<String> h) {
        this.name = n;
        this.coord = c;
        this.distFromCenter = d;
        this.showDist = sh;
        this.hash = h;
    }

    public static void sortByDistanceAscending(List<GeoRadiusElement> elements) {
        Collections.sort(elements, Comparator.comparing(GeoRadiusElement::getDistFromCenter));
    }

    public static void sortByDistanceDescending(List<GeoRadiusElement> elements) {
        Collections.sort(elements, Comparator.comparing((GeoRadiusElement x) -> -1.0 * x.getDistFromCenter()));
    }
}
