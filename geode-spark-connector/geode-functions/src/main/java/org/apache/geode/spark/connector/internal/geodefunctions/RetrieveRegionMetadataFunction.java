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
package org.apache.geode.spark.connector.internal.geodefunctions;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.cache.BucketServerLocation66;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.InternalRegionFunctionContext;
import org.apache.geode.spark.connector.internal.RegionMetadata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * This GemFire function retrieve region metadata
 */
public class RetrieveRegionMetadataFunction implements Function {

  public final static String ID = "geode-spark-retrieve-region-metadata";
  
  private static final RetrieveRegionMetadataFunction instance = new RetrieveRegionMetadataFunction();

  public RetrieveRegionMetadataFunction() {
  }

  public static RetrieveRegionMetadataFunction getInstance() {
    return instance;    
  }
  
  @Override
  public String getId() {
    return ID;
  }

  @Override
  public boolean optimizeForWrite() {
    return false;
  }

  @Override
  public boolean isHA() {
    return true;
  }

  @Override
  public boolean hasResult() {
    return true;
  }

  @Override
  public void execute(FunctionContext context) {
    LocalRegion region = (LocalRegion) ((InternalRegionFunctionContext) context).getDataSet();
    String regionPath = region.getFullPath();
    boolean isPartitioned = region.getDataPolicy().withPartitioning();
    String kTypeName = getTypeClassName(region.getAttributes().getKeyConstraint());
    String vTypeName = getTypeClassName(region.getAttributes().getValueConstraint());

    RegionMetadata metadata;
    if (! isPartitioned) {
      metadata = new RegionMetadata(regionPath, false, 0, null, kTypeName, vTypeName);
    } else {
      PartitionedRegion pregion = (PartitionedRegion) region;
      int totalBuckets = pregion.getAttributes().getPartitionAttributes().getTotalNumBuckets();
      Map<Integer, List<BucketServerLocation66>> bucketMap = pregion.getRegionAdvisor().getAllClientBucketProfiles();
      HashMap<ServerLocation, HashSet<Integer>> serverMap = bucketServerMap2ServerBucketSetMap(bucketMap);
      metadata = new RegionMetadata(regionPath, true, totalBuckets, serverMap, kTypeName, vTypeName);
    }
    
    ResultSender<RegionMetadata> sender = context.getResultSender();
    sender.lastResult(metadata);
  }
  
  private String getTypeClassName(Class clazz) {
    return clazz == null ? null : clazz.getCanonicalName();
  }
  
  /** convert bucket to server map to server to bucket set map */
  private HashMap<ServerLocation, HashSet<Integer>>
    bucketServerMap2ServerBucketSetMap(Map<Integer, List<BucketServerLocation66>> map) {
    HashMap<ServerLocation, HashSet<Integer>> serverBucketMap = new HashMap<>();
    for (Integer id : map.keySet()) {
      List<BucketServerLocation66> locations = map.get(id);
      for (BucketServerLocation66 location : locations) {
        ServerLocation server = new ServerLocation(location.getHostName(), location.getPort());
        if (location.isPrimary()) {
          HashSet<Integer> set = serverBucketMap.get(server);
          if (set == null) {
            set = new HashSet<>();
            serverBucketMap.put(server, set);
          }
          set.add(id);
          break;
        }
      }
    }
    return serverBucketMap;    
  }
}
