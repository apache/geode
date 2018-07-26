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

import com.sun.tools.javac.util.Pair;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class GeoCoder {
  /**
   * Length of generated geohash in bits.
   */
  public static final int LEN_GEOHASH = 60;

  /**
   * Earth radius for distance calculations.
   */
  private static final double EARTH_RADIUS_IN_METERS = 6372797.560856;
  public static final double LONG_MIN = -180.0;
  public static final double LONG_MAX = 180.0;
  public static final double LAT_MIN = -90.0;
  public static final double LAT_MAX = 90.0;
  public static final double MERCATOR_MAX = 20037726.37;


  public static ByteBuf getBulkStringGeoCoordinateArrayResponse(ByteBufAllocator alloc,
      Collection<GeoCoord> items) {
    Iterator<GeoCoord> it = items.iterator();
    ByteBuf response = alloc.buffer();
    response.writeByte(Coder.ARRAY_ID);
    ByteBuf tmp = alloc.buffer();
    int size = 0;
    while (it.hasNext()) {
      GeoCoord next = it.next();
      if (next == null) {
        tmp.writeBytes(Coder.bNIL);
      } else {
        tmp.writeBytes(Coder.getBulkStringArrayResponse(alloc,
            Arrays.asList(
                Double.toString(next.getLongitude()),
                Double.toString(next.getLatitude()))));
      }
      size++;
    }

    response.writeBytes(Coder.intToBytes(size));
    response.writeBytes(Coder.CRLFar);
    response.writeBytes(tmp);

    tmp.release();

    return response;
  }

  public static ByteBuf geoRadiusResponse(ByteBufAllocator alloc,
      Collection<GeoRadiusResponseElement> list) {
    if (list.isEmpty())
      return Coder.getEmptyArrayResponse(alloc);

    List<Object> responseElements = new ArrayList<>();
    for (GeoRadiusResponseElement element : list) {
      String name = element.getName();

      String distStr = "";
      if (element.isShowDist()) {
        distStr = element.getDistFromCenter().toString();
      }

      List<String> coord = new ArrayList<>();
      if (element.getCoord().isPresent()) {
        coord.add(Double.toString(element.getCoord().get().getLongitude()));
        coord.add(Double.toString(element.getCoord().get().getLatitude()));
      }

      String hash = "";
      if (element.getHash().isPresent()) {
        hash = element.getHash().get();
      }

      if (distStr != "" || !coord.isEmpty() || hash != "") {
        List<Object> elementData = new ArrayList<>();
        elementData.add(name);
        if (distStr != "")
          elementData.add(distStr);
        if (!coord.isEmpty())
          elementData.add(coord);
        if (hash != "")
          elementData.add(hash);

        responseElements.add(elementData);
      } else {
        responseElements.add(name);
      }
    }

    return Coder.getBulkStringArrayResponse(alloc, responseElements);
  }

  /**
   * Converts geohash to lat/long.
   *
   * @param hash geohash as base32
   * @return a GeoCoord object containing the coordinates
   */
  public static GeoCoord geoPos(char[] hash) {
    Pair<char[], char[]> hashBits = deinterleave(hash);

    return new GeoCoord(coord(hashBits.fst, LONG_MIN, LONG_MAX),
        coord(hashBits.snd, LAT_MIN, LAT_MAX));
  }

  /**
   * Calculates distance between two points.
   *
   * @param hash1 geohash of first point
   * @param hash2 geohash of second point
   * @return distance in meters
   */
  public static Double geoDist(char[] hash1, char[] hash2) {
    GeoCoord coord1 = geoPos(hash1);
    GeoCoord coord2 = geoPos(hash2);

    Double lat1 = Math.toRadians(coord1.getLatitude());
    Double long1 = Math.toRadians(coord1.getLongitude());
    Double lat2 = Math.toRadians(coord2.getLatitude());
    Double long2 = Math.toRadians(coord2.getLongitude());

    return dist(long1, lat1, long2, lat2);
  }

  /**
   * Calculates geohash given latitude and longitude as byte arrays encoding decimals.
   *
   * @param lon byte array encoding longitude as decimal
   * @param lat byte array encoding latitude as decimal
   * @return geohash as base32
   */
  public static String geoHash(byte[] lon, byte[] lat, int length) throws CoderException {
    return bitsToHash(geoHashBits(lon, lat, length));
  }

  public static char[] geoHashBits(byte[] lon, byte[] lat, int length) throws CoderException {
    Double longitude = Coder.bytesToDouble(lon);
    Double latitude = Coder.bytesToDouble(lat);

    int longLen, latLen;
    if (length % 2 == 0) {
      longLen = length / 2;
      latLen = length / 2;
    } else {
      longLen = (length + 1) / 2;
      latLen = (length - 1) / 2;
    }

    char[] longDigits = coordDigits(longitude, LONG_MIN, LONG_MAX, longLen);
    char[] latDigits = coordDigits(latitude, LAT_MIN, LAT_MAX, latLen);

    return interleave(longDigits, latDigits);
  }

  /**
   * Return a set of hashes (center + 8) that are able to cover a range query
   * for the specified position and radius.
   */
  public static HashNeighbors geoHashGetAreasByRadius(double longitude, double latitude,
      double radiusMeters) throws CoderException {
    HashArea boundingBox = geoHashBoundingBox(longitude, latitude, radiusMeters);
    int steps = geohashEstimateStepsByRadius(radiusMeters, latitude, LEN_GEOHASH / 2);
    char[] hash =
        geoHashBits(Double.toString(longitude).getBytes(), Double.toString(latitude).getBytes(),
            2 * steps);
    HashNeighbors neighbors = getNeighbors(hash);
    HashArea centerArea = geoHashTile(hash);
    HashArea northArea = geoHashTile(neighbors.north.toCharArray());
    HashArea southArea = geoHashTile(neighbors.south.toCharArray());
    HashArea eastArea = geoHashTile(neighbors.east.toCharArray());
    HashArea westArea = geoHashTile(neighbors.west.toCharArray());

    /*
     * Check if the step is enough at the limits of the covered area.
     * Sometimes when the search area is near an edge of the
     * area, the estimated step is not small enough, since one of the
     * north / south / west / east square is too near to the search area
     * to cover everything.
     */
    if ((dist(longitude, latitude, longitude, northArea.maxlat) < radiusMeters
        || dist(longitude, latitude, longitude, southArea.minlat) < radiusMeters
        || dist(longitude, latitude, eastArea.maxlon, latitude) < radiusMeters
        || dist(longitude, latitude, westArea.minlon, latitude) < radiusMeters) && steps > 1) {
      steps--;
      hash =
          geoHashBits(Double.toString(longitude).getBytes(), Double.toString(latitude).getBytes(),
              2 * steps);
      neighbors = getNeighbors(hash);
      centerArea = geoHashTile(hash);
    }

    /* Exclude the search areas that are useless. */
    if (steps >= 2) {
      if (centerArea.minlat < boundingBox.minlat) {
        neighbors.south = null;
        neighbors.southwest = null;
        neighbors.southeast = null;
      }

      if (centerArea.maxlat > boundingBox.maxlat) {
        neighbors.north = null;
        neighbors.northeast = null;
        neighbors.northwest = null;
      }

      if (centerArea.minlon < boundingBox.minlon) {
        neighbors.west = null;
        neighbors.southwest = null;
        neighbors.northwest = null;
      }

      if (centerArea.maxlon > boundingBox.maxlon) {
        neighbors.east = null;
        neighbors.southeast = null;
        neighbors.northeast = null;
      }
    }

    return neighbors;
  }

  /**
   * This function is used in order to estimate the step (bits precision)
   * of the 9 search area boxes during radius queries.
   */
  public static int geohashEstimateStepsByRadius(double rangeMeters, double lat, int maxStep) {
    if (rangeMeters == 0)
      return maxStep;
    int step = 1;
    while (rangeMeters < MERCATOR_MAX) {
      rangeMeters *= 2;
      step++;
    }
    step -= 2; /* Make sure range is included in most of the base cases. */

    /*
     * Wider range torwards the poles... Note: it is possible to do better
     * than this approximation by computing the distance between meridians
     * at this latitude, but this does the trick for now.
     */
    if (lat > 66 || lat < -66) {
      step--;
      if (lat > 80 || lat < -80)
        step--;
    }

    /* Frame to valid range. */
    if (step < 1)
      step = 1;
    if (step > maxStep)
      step = maxStep;
    return step;
  }

  /**
   * Return the bounding box of the search area centered at latitude,longitude
   * having a radius of radius_meter. bounds[0] - bounds[2] is the minimum
   * and maxium longitude, while bounds[1] - bounds[3] is the minimum and
   * maximum latitude.
   * This function does not behave correctly with very large radius values, for
   * instance for the coordinates 81.634948934258375 30.561509253718668 and a
   * radius of 7083 kilometers, it reports as bounding boxes:
   * min_lon 7.680495, min_lat -33.119473, max_lon 155.589402, max_lat 94.242491
   * However, for instance, a min_lon of 7.6804
   * 95 is not correct, because the
   * point -1.27579540014266968 61.33421815228281559 is at less than 7000
   * kilometers away.
   * Since this function is currently only used as an optimization, the
   * optimization is not used for very big radii, however the function
   * should be fixed.
   */
  public static HashArea geoHashBoundingBox(double longitude, double latitude,
      double radiusMeters) {
    double minlon = longitude - Math
        .toDegrees((radiusMeters / EARTH_RADIUS_IN_METERS) * Math.cos(Math.toRadians(latitude)));
    double maxlon = longitude + Math
        .toDegrees((radiusMeters / EARTH_RADIUS_IN_METERS) * Math.cos(Math.toRadians(latitude)));
    double minlat = latitude - Math.toDegrees(radiusMeters / EARTH_RADIUS_IN_METERS);
    double maxlat = latitude + Math.toDegrees(radiusMeters / EARTH_RADIUS_IN_METERS);

    return new HashArea(minlon, maxlon, minlat, maxlat);
  }

  public static Double dist(Double long1, Double lat1, Double long2, Double lat2) {
    Double hav =
        haversine(lat2 - lat1) + (Math.cos(lat1) * Math.cos(lat2) * haversine(long2 - long1));
    Double distAngle = Math.acos(1 - (2 * hav));

    return EARTH_RADIUS_IN_METERS * distAngle;
  }

  public static HashArea geoHashTile(char[] hash) {
    Pair<char[], char[]> coordBits = deinterleave(hash);
    Pair<Double, Double> lonRange = getRange(coordBits.fst, LONG_MIN, LONG_MAX);
    Pair<Double, Double> latRange = getRange(coordBits.snd, LAT_MIN, LAT_MAX);

    return new HashArea(lonRange.fst, lonRange.snd, latRange.fst, latRange.snd);
  }

  public static Pair<Double, Double> getRange(char[] coordDigits, Double min, Double max) {
    Double mid = (min + max) / 2;
    for (char d : coordDigits) {
      if (d == '1') {
        min = mid;
      } else {
        max = mid;
      }
      mid = (min + max) / 2;
    }

    return Pair.of(min, max);
  }

  public static HashNeighbors getNeighbors(char[] hash) throws CoderException {
    HashNeighbors hn = new HashNeighbors();
    hn.center = new String(hash);
    hn.west = new String(move(hash, -1, 0));
    hn.east = new String(move(hash, 1, 0));
    hn.north = new String(move(hash, 0, 1));
    hn.south = new String(move(hash, 0, -1));
    hn.northwest = new String(move(hash, -1, 1));
    hn.northeast = new String(move(hash, 1, 1));
    hn.southwest = new String(move(hash, -1, -1));
    hn.southeast = new String(move(hash, 1, -1));

    return hn;
  }

  private static char[] move(char[] hash, int dist_lon, int dist_lat) throws CoderException {
    Pair<char[], char[]> coordBits = deinterleave(hash);
    char[] lonbits = coordBits.fst;
    char[] latbits = coordBits.snd;

    char[] newLonBits = Integer
        .toBinaryString((Integer.parseInt(new String(lonbits), 2) + dist_lon))
        .toCharArray();
    char[] newLatBits = Integer
        .toBinaryString((Integer.parseInt(new String(latbits), 2) + dist_lat))
        .toCharArray();

    newLonBits = sliceOrPad(newLonBits, lonbits.length);

    if (newLatBits.length > latbits.length) {
      newLatBits = latbits;
    } else {
      newLatBits = sliceOrPad(newLatBits, latbits.length);
    }

    return interleave(newLonBits, newLatBits);
  }

  public static char[] hashToBits(String hash) {
    StringBuilder binStringBuilder = new StringBuilder();
    for (char digit : hash.toCharArray()) {
      int val = base32Val(digit);
      binStringBuilder.append(base32bin(val));
    }

    return binStringBuilder.toString().toCharArray();
  }

  private static Pair<char[], char[]> deinterleave(char[] binChars) {
    int longLen, latLen;
    if (binChars.length % 2 == 0) {
      longLen = binChars.length / 2;
      latLen = binChars.length / 2;
    } else {
      longLen = (binChars.length + 1) / 2;
      latLen = (binChars.length - 1) / 2;
    }

    char[] lonChars = new char[longLen];
    char[] latChars = new char[latLen];
    for (int i = 0; i < binChars.length; i += 2) {
      lonChars[i / 2] = binChars[i];

      if (i / 2 < latChars.length) {
        latChars[i / 2] = binChars[i + 1];
      }
    }

    return Pair.of(lonChars, latChars);
  }

  private static char[] interleave(char[] longDigits, char[] latDigits) throws CoderException {
    if (longDigits.length > latDigits.length + 1) {
      throw new CoderException();
    }

    char[] hashBin = new char[longDigits.length + latDigits.length];
    for (int i = 0; i < longDigits.length; i++) {
      hashBin[2 * i] = longDigits[i];

      if (i < latDigits.length) {
        hashBin[(2 * i) + 1] = latDigits[i];
      }
    }

    return hashBin;
  }

  public static String bitsToHash(char[] hashBin) {
    StringBuilder hashStrBuilder = new StringBuilder();
    StringBuilder digitBuilder = new StringBuilder();

    int e = 0;
    for (int d = 0; d < hashBin.length; d++) {
      digitBuilder.append(hashBin[d]);
      if (e == 4 || d == hashBin.length - 1) {
        hashStrBuilder.append(base32(Integer.parseInt(digitBuilder.toString(), 2)));
        digitBuilder = new StringBuilder();
        e = 0;
      } else {
        e++;
      }
    }

    return hashStrBuilder.toString();
  }

  public static double haversine(Double rad) {
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

  private static char base32(int x) {
    String base32str = "0123456789bcdefghjkmnpqrstuvwxyz";
    return base32str.charAt(x);
  }

  private static int base32Val(char d) {
    String base32str = "0123456789bcdefghjkmnpqrstuvwxyz";
    return base32str.indexOf(d);
  }

  private static String base32bin(int v) {
    if (v > 15) {
      return Integer.toBinaryString(v);
    }

    if (v > 7) {
      return "0" + Integer.toBinaryString(v);
    }

    if (v > 3) {
      return "00" + Integer.toBinaryString(v);
    }

    if (v > 1) {
      return "000" + Integer.toBinaryString(v);
    }

    return "0000" + Integer.toBinaryString(v);
  }

  private static char[] coordDigits(Double coordinate, Double min, Double max, int length)
      throws CoderException {
    if (coordinate > max || coordinate < min) {
      throw new CoderException();
    }

    Double coordOffset = (coordinate - min) / (max - min);
    Long coordOffsetL = (long) (coordOffset * (1 << length));
    char[] x = sliceOrPad(Long.toBinaryString(coordOffsetL).toCharArray(), length);

    return x;
  }

  private static double coord(char[] digits, Double min, Double max) {
    double coord = (double) Long.parseLong(new String(digits), 2);
    double scale = (double) (1 << digits.length);

    return min + ((coord / scale) * (max - min));
  }

  private static char[] sliceOrPad(char[] binChars, int length) {
    char[] newBinChars = new char[length];

    for (int i = 0, j = binChars.length - newBinChars.length; i < newBinChars.length; i++, j++) {
      if (j >= 0) {
        newBinChars[i] = binChars[j];
      } else {
        newBinChars[i] = '0';
      }
    }

    return newBinChars;
  }
}
