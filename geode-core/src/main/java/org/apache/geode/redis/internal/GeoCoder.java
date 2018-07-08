package org.apache.geode.redis.internal;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import org.apache.geode.redis.internal.org.apache.hadoop.fs.GeoCoord;

public class GeoCoder {
  /**
   * Length of generated geohash in bits.
   */
  private static final int LEN_GEOHASH = 60;

  /**
   * Earth radius for distance calculations.
   */
  private static final double EARTH_RADIUS_IN_METERS = 6372797.560856;


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

  /**
   * Converts geohash to lat/long.
   *
   * @param hash geohash as base32
   * @return a GeoCoord object containing the coordinates
   */
  public static GeoCoord geoPos(String hash) {
    StringBuilder binStringBuilder = new StringBuilder();
    for (char digit : hash.toCharArray()) {
      int val = base32Val(digit);
      binStringBuilder.append(base32bin(val));
    }

    char[] binChars = binStringBuilder.toString().toCharArray();
    char[] lonChars = new char[binChars.length / 2];
    char[] latChars = new char[binChars.length / 2];
    for (int i = 0; i < binChars.length; i += 2) {
      lonChars[i / 2] = binChars[i];
      latChars[i / 2] = binChars[i + 1];
    }

    return new GeoCoord(coord(lonChars, -180.0, 180.0), coord(latChars, -90.0, 90.0));
  }

  /**
   * Calculates distance between two points.
   *
   * @param hash1 geohash of first point
   * @param hash2 geohash of second point
   * @return distance in meters
   */
  public static Double geoDist(String hash1, String hash2) {
    GeoCoord coord1 = geoPos(hash1);
    GeoCoord coord2 = geoPos(hash2);

    Double lat1 = Math.toRadians(coord1.getLatitude());
    Double long1 = Math.toRadians(coord1.getLongitude());
    Double lat2 = Math.toRadians(coord2.getLatitude());
    Double long2 = Math.toRadians(coord2.getLongitude());

    Double hav =
        haversine(lat2 - lat1) + (Math.cos(lat1) * Math.cos(lat2) * haversine(long2 - long1));
    Double distAngle = Math.acos(1 - (2 * hav));

    return EARTH_RADIUS_IN_METERS * distAngle;
  }

  public static Double haversine(Double rad) {
    return 0.5 * (1 - Math.cos(rad));
  }

  /**
   * Calculates geohash given latitude and longitude as byte arrays encoding decimals.
   *
   * @param lon byte array encoding longitude as decimal
   * @param lat byte array encoding latitude as decimal
   * @return geohash as base32
   */
  public static String geoHash(byte[] lon, byte[] lat) {
    Double longitude = Coder.bytesToDouble(lon);
    Double latitude = Coder.bytesToDouble(lat);

    char[] longDigits = coordDigits(longitude, -180.0, 180.0);
    char[] latDigits = coordDigits(latitude, -90.0, 90.0);
    char[] hashBin = new char[LEN_GEOHASH];
    for (int c = 0; c < LEN_GEOHASH / 2; c++) {
      hashBin[2 * c] = longDigits[c];
      hashBin[(2 * c) + 1] = latDigits[c];
    }

    StringBuilder hashStrBuilder = new StringBuilder();
    StringBuilder digitBuilder = new StringBuilder();

    int e = 0;
    for (int d = 0; d < LEN_GEOHASH; d++) {
      digitBuilder.append(hashBin[d]);
      if (e == 4) {
        hashStrBuilder.append(base32(Integer.parseInt(digitBuilder.toString(), 2)));
        digitBuilder = new StringBuilder();
        e = 0;
      } else {
        e++;
      }
    }

    return hashStrBuilder.toString();
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

  private static char[] coordDigits(Double coordinate, Double min, Double max) {
    char[] bin = new char[LEN_GEOHASH / 2];
    for (int c = 0; c < bin.length; c++) {
      Double mid = (min + max) / 2;
      if (coordinate >= mid) {
        bin[c] = '1';
        min = mid;
      } else {
        bin[c] = '0';
        max = mid;
      }
    }

    return bin;
  }

  private static double coord(char[] digits, Double min, Double max) {
    Double mid = (min + max) / 2;
    for (char d : digits) {
      if (d == '1') {
        min = mid;
      } else {
        max = mid;
      }
      mid = (min + max) / 2;
    }

    return mid;
  }
}
