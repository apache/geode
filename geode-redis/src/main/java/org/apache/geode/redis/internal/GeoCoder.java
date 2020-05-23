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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.github.davidmoten.geo.GeoHash;
import com.github.davidmoten.geo.LatLong;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

public class GeoCoder {

  /**
   * Earth radius for distance calculations.
   */
  private static final double EARTH_RADIUS_IN_METERS = 6372797.560856;

  public static ByteBuf getBulkStringGeoCoordinateArrayResponse(ByteBufAllocator alloc,
      Collection<LatLong> items)
      throws CoderException {
    ByteBuf response = alloc.buffer();
    response.writeByte(Coder.ARRAY_ID);
    ByteBuf tmp = alloc.buffer();
    int size = 0;
    try {
      for (LatLong next : items) {
        if (next == null) {
          tmp.writeBytes(Coder.bNIL);
        } else {
          tmp.writeBytes(Coder.getArrayResponse(alloc,
              Arrays.asList(
                  Double.toString(next.getLon()),
                  Double.toString(next.getLat()))));
        }
        size++;
      }

      response.writeBytes(Coder.intToBytes(size));
      response.writeBytes(Coder.CRLFar);
      response.writeBytes(tmp);
    } finally {
      tmp.release();
    }

    return response;
  }

  public static ByteBuf geoRadiusResponse(ByteBufAllocator alloc,
      Collection<GeoRadiusResponseElement> list)
      throws CoderException {
    if (list.isEmpty()) {
      return Coder.getEmptyArrayResponse(alloc);
    }

    List<Object> responseElements = new ArrayList<>();
    for (GeoRadiusResponseElement element : list) {
      String name = element.getName();

      String distStr = "";
      if (element.isShowDist()) {
        distStr = element.getDistFromCenter().toString();
      }

      List<String> coord = new ArrayList<>();
      if (element.getCoord().isPresent()) {
        coord.add(Double.toString(element.getCoord().get().getLon()));
        coord.add(Double.toString(element.getCoord().get().getLat()));
      }

      String hash = "";
      if (element.getHash().isPresent()) {
        hash = element.getHash().get();
      }

      if (!Objects.equals(distStr, "") || !coord.isEmpty() || !Objects.equals(hash, "")) {
        List<Object> elementData = new ArrayList<>();
        elementData.add(name);
        if (!Objects.equals(distStr, "")) {
          elementData.add(distStr);
        }
        if (!coord.isEmpty()) {
          elementData.add(coord);
        }
        if (!Objects.equals(hash, "")) {
          elementData.add(hash);
        }
        responseElements.add(elementData);
      } else {
        responseElements.add(name);
      }
    }

    return Coder.getArrayResponse(alloc, responseElements);
  }

  /**
   * Converts geohash to lat/long.
   *
   * @param hash geohash as base32
   * @return a LatLong object containing the coordinates
   */
  public static LatLong geoPos(String hash) {
    return GeoHash.decodeHash(hash);
  }

  /**
   * Calculates distance between two points.
   *
   * @param hash1 geohash of first point
   * @param hash2 geohash of second point
   * @return distance in meters
   */
  public static double geoDist(String hash1, String hash2) {
    LatLong coord1 = geoPos(hash1);
    LatLong coord2 = geoPos(hash2);

    double lat1 = Math.toRadians(coord1.getLat());
    double long1 = Math.toRadians(coord1.getLon());
    double lat2 = Math.toRadians(coord2.getLat());
    double long2 = Math.toRadians(coord2.getLon());

    return dist(long1, lat1, long2, lat2);
  }

  /**
   * Calculates geohash given latitude and longitude as byte arrays encoding decimals.
   *
   * @param lon byte array encoding longitude as decimal
   * @param lat byte array encoding latitude as decimal
   * @return geohash as base32
   */
  public static String geohash(byte[] lon, byte[] lat) throws IllegalArgumentException {
    double longitude = Coder.bytesToDouble(lon);
    double latitude = Coder.bytesToDouble(lat);
    return GeoHash.encodeHash(latitude, longitude);
  }

  public static Set<String> geohashSearchAreas(double longitude, double latitude,
      double radiusMeters) {
    HashArea boundingBox = boundingBox(longitude, latitude, radiusMeters);
    int steps =
        Math.max(1, GeoHash.hashLengthToCoverBoundingBox(boundingBox.minlat, boundingBox.maxlon,
            boundingBox.maxlat, boundingBox.minlon));

    List<String> extra = new ArrayList<>();
    // Large distance boundary condition
    if (steps == 1) {
      extra.addAll(GeoHash.neighbours(GeoHash.encodeHash(latitude, longitude, steps)));
    }

    Set<String> areas = GeoHash.coverBoundingBox(boundingBox.maxlat, boundingBox.minlon,
        boundingBox.minlat, boundingBox.maxlon, steps).getHashes();
    if (!extra.isEmpty()) {
      extra.forEach(ex -> areas.add(ex));
    }

    return areas;
  }

  public static HashArea boundingBox(double longitude, double latitude,
      double radiusMeters) {
    double minlon = longitude - Math
        .toDegrees((radiusMeters / EARTH_RADIUS_IN_METERS) * Math.cos(Math.toRadians(latitude)));
    double maxlon = longitude + Math
        .toDegrees((radiusMeters / EARTH_RADIUS_IN_METERS) * Math.cos(Math.toRadians(latitude)));
    double minlat = latitude - Math.toDegrees(radiusMeters / EARTH_RADIUS_IN_METERS);
    double maxlat = latitude + Math.toDegrees(radiusMeters / EARTH_RADIUS_IN_METERS);

    return new HashArea(minlon, maxlon, minlat, maxlat);
  }

  public static double dist(double long1, double lat1, double long2, double lat2) {
    double hav =
        haversine(lat2 - lat1) + (Math.cos(lat1) * Math.cos(lat2) * haversine(long2 - long1));
    double distAngle = Math.acos(1 - (2 * hav));

    return EARTH_RADIUS_IN_METERS * distAngle;
  }

  public static double haversine(double rad) {
    return 0.5 * (1 - Math.cos(rad));
  }

  public static double parseUnitScale(String unit) throws IllegalArgumentException {
    switch (unit) {
      case "km":
        return 0.001;
      case "m":
        return 1.0;
      case "ft":
        return 3.28084;
      case "mi":
        return 0.000621371;
      default:
        throw new IllegalArgumentException();
    }
  }
}
