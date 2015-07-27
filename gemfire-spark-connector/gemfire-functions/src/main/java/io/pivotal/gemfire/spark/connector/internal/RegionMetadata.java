package io.pivotal.gemfire.spark.connector.internal;

import com.gemstone.gemfire.distributed.internal.ServerLocation;

import java.util.HashMap;
import java.util.HashSet;
import java.io.Serializable;

/**
 * This class contains all info required by GemFire RDD partitioner to create partitions.
 */
public class RegionMetadata implements Serializable {

  private String  regionPath;
  private boolean isPartitioned;
  private int     totalBuckets;
  private HashMap<ServerLocation, HashSet<Integer>> serverBucketMap;
  private String  keyTypeName;
  private String  valueTypeName;

  /**
   * Default constructor.
   * @param regionPath the full path of the given region
   * @param isPartitioned true for partitioned region, false otherwise
   * @param totalBuckets number of total buckets for partitioned region, ignored otherwise
   * @param serverBucketMap gemfire server (host:port pair) to bucket set map
   * @param keyTypeName region key class name
   * @param valueTypeName region value class name                    
   */
  public RegionMetadata(String regionPath, boolean isPartitioned, int totalBuckets, HashMap<ServerLocation, HashSet<Integer>> serverBucketMap,
                        String keyTypeName, String valueTypeName) {
    this.regionPath = regionPath;
    this.isPartitioned = isPartitioned;
    this.totalBuckets = totalBuckets;
    this.serverBucketMap = serverBucketMap;
    this.keyTypeName = keyTypeName;
    this.valueTypeName = valueTypeName;
  }

  public RegionMetadata(String regionPath, boolean isPartitioned, int totalBuckets, HashMap<ServerLocation, HashSet<Integer>> serverBucketMap) {
    this(regionPath, isPartitioned, totalBuckets, serverBucketMap, null, null);
  }

  public String getRegionPath() {
    return regionPath;
  }

  public boolean isPartitioned() {
    return isPartitioned;
  }

  public int getTotalBuckets() {
    return totalBuckets;
  }
  
  public HashMap<ServerLocation, HashSet<Integer>> getServerBucketMap() {
    return serverBucketMap;
  }

  public String getKeyTypeName() {
    return keyTypeName;
  }

  public String getValueTypeName() {
    return valueTypeName;
  }

  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("RegionMetadata(region=").append(regionPath)
       .append("(").append(keyTypeName).append(", ").append(valueTypeName).append(")")
       .append(", partitioned=").append(isPartitioned).append(", #buckets=").append(totalBuckets)
       .append(", map=").append(serverBucketMap).append(")");
    return buf.toString();
  }

}
