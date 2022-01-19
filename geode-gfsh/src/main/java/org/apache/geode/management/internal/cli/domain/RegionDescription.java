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
package org.apache.geode.management.internal.cli.domain;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Scope;

/***
 * Data class which contains description of a region and provides the aggregated view of the region
 * Used by describe region command
 */
public class RegionDescription implements Serializable {

  private static final long serialVersionUID = 6461449275798378332L;
  private String name;
  private boolean isPartition;
  private boolean isPersistent;
  private boolean isReplicate;
  private boolean isLocal = false;
  private boolean isAccessor = false;

  // COPY
  // Common Non Default Attributes
  private final Map<String, String> cndRegionAttributes = new HashMap<>();
  private final Map<String, String> cndPartitionAttributes = new HashMap<>();
  private final Map<String, String> cndEvictionAttributes = new HashMap<>();

  private Map<String, RegionDescriptionPerMember> regionDescPerMemberMap = null;
  private Scope scope;
  private DataPolicy dataPolicy;

  public RegionDescription() {}

  public DataPolicy getDataPolicy() {
    return dataPolicy;
  }

  public Scope getScope() {
    return scope;
  }

  /**
   * Adds the RegionDescription per member to the aggregated view
   *
   * @return boolean describing if description was successfully added
   */
  public boolean add(RegionDescriptionPerMember regionDescPerMember) {
    boolean isAdded = false;

    if (regionDescPerMemberMap == null) {
      regionDescPerMemberMap = new HashMap<>();
      regionDescPerMemberMap.put(regionDescPerMember.getHostingMember(), regionDescPerMember);
      scope = regionDescPerMember.getScope();
      dataPolicy = regionDescPerMember.getDataPolicy();
      name = regionDescPerMember.getName();
      isPartition = dataPolicy.withPartitioning();
      isPersistent = dataPolicy.withPersistence();
      isReplicate = dataPolicy.withReplication();
      isLocal = scope.isLocal();
      isAccessor = regionDescPerMember.isAccessor();
      cndRegionAttributes.putAll(regionDescPerMember.getNonDefaultRegionAttributes());
      cndPartitionAttributes.putAll(regionDescPerMember.getNonDefaultPartitionAttributes());
      cndEvictionAttributes.putAll(regionDescPerMember.getNonDefaultEvictionAttributes());

      isAdded = true;
    } else if (scope.equals(regionDescPerMember.getScope())
        && name.equals(regionDescPerMember.getName())
        && dataPolicy.equals(regionDescPerMember.getDataPolicy())
        && isAccessor == regionDescPerMember.isAccessor()) {

      regionDescPerMemberMap.put(regionDescPerMember.getHostingMember(), regionDescPerMember);
      findCommon(cndRegionAttributes, regionDescPerMember.getNonDefaultRegionAttributes());
      findCommon(cndEvictionAttributes, regionDescPerMember.getNonDefaultEvictionAttributes());
      findCommon(cndPartitionAttributes, regionDescPerMember.getNonDefaultPartitionAttributes());

      isAdded = true;
    }
    return isAdded;
  }

  /**
   * Removes any key-value pairs from @commonValuesMap that do not agree with the respective
   * key-value pairs of @additionalValuesMap
   *
   * @param commonValuesMap Common values map, whose key set will be reduced.
   * @param additionalValuesMap Incoming values map, against which @commonValuesMap.
   */
  static void findCommon(Map<String, String> commonValuesMap,
      Map<String, String> additionalValuesMap) {

    Set<String> sharedKeySet = commonValuesMap.keySet();
    sharedKeySet.retainAll(additionalValuesMap.keySet());

    for (String sharedKey : new HashSet<>(sharedKeySet)) {
      String commonNdValue = commonValuesMap.get(sharedKey);
      String incomingNdValue = additionalValuesMap.get(sharedKey);
      if (commonNdValue != null && !commonNdValue.equals(incomingNdValue)
          || commonNdValue == null && incomingNdValue != null) {
        commonValuesMap.remove(sharedKey);
      }
    }
  }


  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RegionDescription) {
      RegionDescription regionDesc = (RegionDescription) obj;

      return getName().equals(regionDesc.getName()) && scope.equals(regionDesc.getScope())
          && dataPolicy.equals(regionDesc.getDataPolicy());
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
    return name;
  }

  public boolean isPersistent() {
    return isPersistent;
  }

  public boolean isPartition() {
    return isPartition;
  }

  public boolean isReplicate() {
    return isReplicate;
  }

  public boolean isLocal() {
    return isLocal;
  }

  public boolean isAccessor() {
    return isAccessor;
  }

  /***
   * Gets the common, non-default region attributes
   *
   * @return Map containing attribute name and its associated value
   */
  public Map<String, String> getCndRegionAttributes() {
    return cndRegionAttributes;
  }

  /***
   * Gets the common, non-default eviction attributes
   *
   * @return Map containing attribute name and its associated value
   */
  public Map<String, String> getCndEvictionAttributes() {
    return cndEvictionAttributes;
  }

  /***
   * Gets the common, non-default partition attributes
   *
   * @return Map containing attribute name and its associated value
   */
  public Map<String, String> getCndPartitionAttributes() {
    return cndPartitionAttributes;
  }

  public Map<String, RegionDescriptionPerMember> getRegionDescriptionPerMemberMap() {
    return regionDescPerMemberMap;
  }

  public String toString() {

    return "";
  }

  public boolean isEmpty() {
    return regionDescPerMemberMap == null || regionDescPerMemberMap.isEmpty();
  }
}
