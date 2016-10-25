/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.spark.connector.internal;

import org.apache.geode.distributed.internal.ServerLocation;

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
   * @param serverBucketMap geode server (host:port pair) to bucket set map
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
