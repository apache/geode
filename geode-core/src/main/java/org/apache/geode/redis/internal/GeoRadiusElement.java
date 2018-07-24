package org.apache.geode.redis.internal;

import org.apache.geode.redis.internal.org.apache.hadoop.fs.GeoCoord;

import java.util.Optional;

public class GeoRadiusElement {
    private String name;
    private Optional<GeoCoord> coord;
    private Optional<Double> distFromCenter;
    private Optional<String> hash;

    public String getName() {
        return name;
    }
    public Optional<GeoCoord> getCoord() {
        return coord;
    }

    public Optional<Double> getDistFromCenter() {
        return distFromCenter;
    }

    public Optional<String> getHash() {
        return hash;
    }

    public GeoRadiusElement(String n, Optional<GeoCoord> c, Optional<Double> d, Optional<String> h) {
        this.name = n;
        this.coord = c;
        this.distFromCenter = d;
        this.hash = h;
    }
}
