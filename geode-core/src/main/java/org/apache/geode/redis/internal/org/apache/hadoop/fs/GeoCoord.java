package org.apache.geode.redis.internal.org.apache.hadoop.fs;

public class GeoCoord {
  double longitude;
  double latitude;

  public GeoCoord(double lon, double lat) {
    this.longitude = lon;
    this.latitude = lat;
  }

  public double getLongitude() {
    return longitude;
  }

  public double getLatitude() {
    return latitude;
  }
}
