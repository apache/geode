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
package com.gemstone.gemfire.cache.client.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.internal.cache.BucketServerLocation66;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.cache.BucketServerLocation;
import com.gemstone.gemfire.internal.cache.FixedPartitionAttributesImpl;
import com.gemstone.gemfire.internal.cache.BucketServerLocation66;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Stores the information such as partition attributes and meta data details
 * 
 * 
 * @since 6.5
 * 
 */
public class ClientPartitionAdvisor {

  private final ConcurrentMap<Integer, List<BucketServerLocation66>> bucketServerLocationsMap 
  = new ConcurrentHashMap<Integer, List<BucketServerLocation66>>();

  private final int totalNumBuckets;

  private String serverGroup = "";
  
  private final String colocatedWith;

  private PartitionResolver partitionResolver = null;
  
  private Map<String, List<Integer>> fixedPAMap = null;

  private boolean fpaAttrsCompletes = false;

  @SuppressWarnings("unchecked")
  public ClientPartitionAdvisor(int totalNumBuckets, String colocatedWith,
      String partitionResolverName, Set<FixedPartitionAttributes> fpaSet) {

    this.totalNumBuckets = totalNumBuckets;
    this.colocatedWith = colocatedWith;
    try {
      if (partitionResolverName != null) {
        this.partitionResolver = (PartitionResolver)
            ClassPathLoader.getLatest().forName(partitionResolverName).newInstance();
      }
    }
    catch (Exception e) {
      e.printStackTrace();
      throw new InternalGemFireException(LocalizedStrings.ClientPartitionAdvisor_CANNOT_CREATE_AN_INSTANCE_OF_PARTITION_RESOLVER_0.toLocalizedString(partitionResolverName));
    }
    if (fpaSet != null) {
      fixedPAMap = new ConcurrentHashMap<String, List<Integer>>();
      int totalFPABuckets = 0;
      for (FixedPartitionAttributes fpa : fpaSet) {
        List attrList = new ArrayList();
        totalFPABuckets+=fpa.getNumBuckets();
        attrList.add(fpa.getNumBuckets());
        attrList.add(((FixedPartitionAttributesImpl)fpa).getStartingBucketID());
        fixedPAMap.put(fpa.getPartitionName(), attrList);
      }
      if(totalFPABuckets == this.totalNumBuckets) {
        this.fpaAttrsCompletes = true;
      }
    }
  }

  public ServerLocation adviseServerLocation(int bucketId) {
    if (this.bucketServerLocationsMap.containsKey(bucketId)) {
      List<BucketServerLocation66> locations = this.bucketServerLocationsMap
          .get(bucketId);
      List<BucketServerLocation66> locationsCopy = new ArrayList<BucketServerLocation66>(
          locations);
      // TODO: We need to consider Load Balancing Policy
      if (locationsCopy.isEmpty()) {
        return null;
      }
      if (locationsCopy.size() == 1) {
        return locationsCopy.get(0);
      }
      int index = new Random().nextInt(locationsCopy.size() - 1);
      return locationsCopy.get(index);
    }
    return null;
  }

  public ServerLocation adviseRandomServerLocation() {
    ArrayList<Integer> bucketList = new ArrayList<Integer>(
        this.bucketServerLocationsMap.keySet());

    if (bucketList.size() > 0) {
      Collections.shuffle(bucketList);
      List<BucketServerLocation66> locations = this.bucketServerLocationsMap
          .get(bucketList.get(0));

      if (locations != null) {
        List<BucketServerLocation66> serverList = new ArrayList<BucketServerLocation66>(
            locations);
        if (serverList.size() == 0)
          return null;
        return serverList.get(0);
      }
    }
    return null;
  }
  
  public List<BucketServerLocation66> adviseServerLocations(int bucketId) {
    if (this.bucketServerLocationsMap.containsKey(bucketId)) {
      List<BucketServerLocation66> locationsCopy = new ArrayList<BucketServerLocation66>(
          this.bucketServerLocationsMap.get(bucketId));
      return locationsCopy;
    }
    return null;
  }
  
  public ServerLocation advisePrimaryServerLocation(int bucketId) {
    if (this.bucketServerLocationsMap.containsKey(bucketId)) {
      List<BucketServerLocation66> locations = this.bucketServerLocationsMap
          .get(bucketId);
      List<BucketServerLocation66> locationsCopy = new ArrayList<BucketServerLocation66>(
          locations);
      for (BucketServerLocation66 loc : locationsCopy) {
        if (loc.isPrimary()) {
          return loc;
        }
      }
    }
    return null;
  }

  public void updateBucketServerLocations(int bucketId,
    List<BucketServerLocation66> bucketServerLocations, ClientMetadataService cms) {
    List<BucketServerLocation66> locationCopy = new ArrayList<BucketServerLocation66>();
    List<BucketServerLocation66> locations;
    
    boolean honourSeverGroup = cms.honourServerGroup();

    if (this.serverGroup.length() != 0 && honourSeverGroup) {
      for (BucketServerLocation66 s : bucketServerLocations) {
        String[] groups = s.getServerGroups();
        if (groups.length > 0) {
          for (String str : groups) {
            if (str.equals(this.serverGroup)) {
              locationCopy.add(s);
              break;
            }
          }
        } else {
          locationCopy.add(s);
        }
      }
      locations = Collections.unmodifiableList(locationCopy);
    } else {
      locations = Collections.unmodifiableList(bucketServerLocations);
    }
    
    this.bucketServerLocationsMap.put(bucketId, locations);
  }

  public void removeBucketServerLocation(ServerLocation serverLocation) {
    Iterator<Map.Entry<Integer, List<BucketServerLocation66>>> iter = this.bucketServerLocationsMap
        .entrySet().iterator();
    while (iter.hasNext()) {
      Map.Entry<Integer, List<BucketServerLocation66>> entry = iter.next();
      Integer key = entry.getKey();
      List<BucketServerLocation66> oldLocations = entry.getValue();
      List<BucketServerLocation66> newLocations = new ArrayList<BucketServerLocation66>(
          oldLocations);
      // if this serverLocation contains in the list the remove the
      // serverLocation and update the map with new List
      while (newLocations.remove(serverLocation)
          && !this.bucketServerLocationsMap.replace(key, oldLocations,
              newLocations)) {
        oldLocations = this.bucketServerLocationsMap.get(key);
        newLocations = new ArrayList<BucketServerLocation66>(oldLocations);
      }
    }
  }

  public Map<Integer, List<BucketServerLocation66>> getBucketServerLocationsMap_TEST_ONLY() {
    return this.bucketServerLocationsMap;
  }

  /**
   * This method returns total number of buckets for a PartitionedRegion.
   * 
   * @return total number of buckets for a PartitionedRegion.
   */
  
  public int getTotalNumBuckets() {
    return this.totalNumBuckets;
  }

  /**
   * @return the serverGroup
   */
  public String getServerGroup() {
    return this.serverGroup;
  }
   

  public void setServerGroup(String group) {
    this.serverGroup = group;
  }

  /**
   * Returns name of the colocated PartitionedRegion on CacheServer
   */
  public String getColocatedWith() {
    return this.colocatedWith;
  }

  /**
   * Returns the PartitionResolver set for custom partitioning
   * 
   * @return <code>PartitionResolver</code> for the PartitionedRegion
   */
  public PartitionResolver getPartitionResolver() {
    return this.partitionResolver;
  }

  public Set<String> getFixedPartitionNames() {
    return this.fixedPAMap.keySet();
  }

  public int assignFixedBucketId(Region region, String partition,
      Object resolveKey) {
    if (this.fixedPAMap.containsKey(partition)) {
      List<Integer> attList = this.fixedPAMap.get(partition);
      int hc = resolveKey.hashCode();
      int bucketId = Math.abs(hc % (attList.get(0)));
      int partitionBucketID = bucketId + attList.get(1);
      return partitionBucketID;
    }
    else {
      // We don't know as we might not have got the all FPAttributes
      // from the FPR, So don't throw the exception but send the request
      // to the server and update the FPA attributes
      // This exception should be thrown from the server as we will
      // not be sure of partition not available unless we contact the server.
      return -1;
    }
  }
  
  public Map<String, List<Integer>> getFixedPAMap(){
    return this.fixedPAMap;
  }
  
  public void updateFixedPAMap(Map<String, List<Integer>> map) {
    this.fixedPAMap.putAll(map);
  }

  public boolean isFPAAttrsComplete() {
    return this.fpaAttrsCompletes;
  }
}
