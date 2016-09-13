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
package com.gemstone.gemfire.management.internal.cli.domain;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;

/***
 * Data class which contains description of a region and provides the aggregated
 * view of the region Used by describe region command
 * 
 * 
 */
public class RegionDescription implements Serializable {

  private static final long  serialVersionUID               = 1L;
  private String name;
  private boolean isPartition;
  private boolean isPersistent;
  private boolean isReplicate;
  private boolean haslocalDataStorage;
  private boolean isLocal = false;
  private boolean isReplicatedProxy = false;;
  private boolean isAccessor = false;
  

  // Common Non Default Attributes
  private Map<String, String>                     cndRegionAttributes;
  private Map<String, String>                     cndPartitionAttributes;
  private Map<String, String>                     cndEvictionAttributes;

  private Map<String, RegionDescriptionPerMember> regionDescPerMemberMap = null;
  private Scope scope;
  private DataPolicy dataPolicy;
  
  public RegionDescription() {

  }
  
  public DataPolicy getDataPolicy() {
    return this.dataPolicy;
  }
  public Scope getScope() {
    return this.scope;
  }
  /**
   * Adds the RegionDescription per member to the aggregated view
   * 
   * @param regionDescPerMember
   * 
   */
  public boolean add(RegionDescriptionPerMember regionDescPerMember) {
    boolean isAdded = false;
    
    if (regionDescPerMemberMap == null) {
      regionDescPerMemberMap = new HashMap<String, RegionDescriptionPerMember>();
      regionDescPerMemberMap.put(regionDescPerMember.getHostingMember(), regionDescPerMember);
      this.scope = regionDescPerMember.getScope();
      this.dataPolicy = regionDescPerMember.getDataPolicy();
      this.name = regionDescPerMember.getName();
      isPartition = this.dataPolicy.withPartitioning();
      isPersistent = this.dataPolicy.withPersistence();
      isReplicate = this.dataPolicy.withReplication();
      haslocalDataStorage = this.dataPolicy.withStorage();
      isLocal = this.scope.isLocal();
      isAccessor = regionDescPerMember.isAccessor();
      //COPY
      this.cndRegionAttributes = new HashMap<String, String>();
      this.cndRegionAttributes.putAll(regionDescPerMember.getNonDefaultRegionAttributes());
      
      this.cndPartitionAttributes = new HashMap<String, String>();
      this.cndPartitionAttributes.putAll(regionDescPerMember.getNonDefaultPartitionAttributes());
      
      this.cndEvictionAttributes = new HashMap<String, String>();
      this.cndEvictionAttributes.putAll(regionDescPerMember.getNonDefaultEvictionAttributes());
      
      if (this.dataPolicy.equals(DataPolicy.EMPTY) && this.scope.equals(Scope.DISTRIBUTED_ACK)) {
        isReplicatedProxy = true;
      }
      
      //Don't have to show the scope for PR's
      
      
      isAdded = true;
    } else {
       if (this.scope.equals(regionDescPerMember.getScope()) 
           && this.name.equals(regionDescPerMember.getName()) 
           && this.dataPolicy.equals(regionDescPerMember.getDataPolicy())
           && this.isAccessor == regionDescPerMember.isAccessor()) {
         
         regionDescPerMemberMap.put(regionDescPerMember.getHostingMember(), regionDescPerMember);
         findCommon(cndRegionAttributes, regionDescPerMember.getNonDefaultRegionAttributes());
         findCommon(cndEvictionAttributes, regionDescPerMember.getNonDefaultEvictionAttributes());
         findCommon(cndPartitionAttributes, regionDescPerMember.getNonDefaultPartitionAttributes());
         
         isAdded = true;
       }
    }
    return isAdded;
  }
  
  private void findCommon(Map<String, String> commonNdMap, Map<String, String> incomingNdMap) {
    //First get the intersection of the both maps
    
    Set<String> commonNdKeySet = commonNdMap.keySet();
    Set<String> incomingNdKeySet = incomingNdMap.keySet();
    
    commonNdKeySet.retainAll(incomingNdKeySet);
    
    //Now compare the values
    //Take a copy of the set to avoid a CME
    Iterator <String> commonKeysIter = (new HashSet<String>(commonNdKeySet)).iterator();
    
    while (commonKeysIter.hasNext()) {
      String attribute = commonKeysIter.next();
      String commonNdValue = commonNdMap.get(attribute);
      String incomingNdValue = incomingNdMap.get(attribute);
      
      if (commonNdValue != null) {
        if (!commonNdValue.equals(incomingNdValue)) {
          //Remove it from the commonNdMa
          commonNdMap.remove(attribute);
        }
      } else {
        if (incomingNdValue != null) {
          commonNdMap.remove(attribute);
        }
      }
    }
  }
  
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RegionDescription) {
      RegionDescription regionDesc = (RegionDescription) obj;

      return this.getName().equals(regionDesc.getName())
          && this.scope.equals(regionDesc.getScope())
          && this.dataPolicy.equals(regionDesc.getDataPolicy());
    }
    return true;
  }
  
  public int hashCode() {
    return 42; // any arbitrary constant will do
    
  }
  
  public Set<String> getHostingMembers() {
    return regionDescPerMemberMap.keySet();
  }

  public String getName() {
    return this.name;
  }

  public boolean isPersistent() {
    return this.isPersistent;
  }

  public boolean isPartition() {
    return this.isPartition;
  }

  public boolean isReplicate() {
    return this.isReplicate;
  }
  
  public boolean hasLocalStorage() {
    return this.haslocalDataStorage;
  }
  
  public boolean isLocal() {
    return this.isLocal;
  }
  
  public boolean isReplicatedProxy() {
    return this.isReplicatedProxy;
  }
  
  public boolean isAccessor() {
    return this.isAccessor;
  }

  
  /***
   * Get
   * @return Map containing attribute name and its associated value
   */
  public Map<String, String> getCndRegionAttributes() {
    return this.cndRegionAttributes;
  }
  
  /***
   * Gets the common non-default Eviction Attributes
   * @return Map containing attribute name and its associated value
   */
  public Map<String, String> getCndEvictionAttributes() {
    return this.cndEvictionAttributes;
  }
  
  /***
   * Gets the common non-default PartitionAttributes
   * @return Map containing attribute name and its associated value
   */
  public Map<String, String> getCndPartitionAttributes() {
    return this.cndPartitionAttributes;
  }
  
  public Map<String, RegionDescriptionPerMember> getRegionDescriptionPerMemberMap(){
    return this.regionDescPerMemberMap;
  }
  
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
   
    return sb.toString();
  }

}
