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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.internal.cache.PartitionAttributesImpl;
import org.apache.geode.management.internal.cli.util.RegionAttributesDefault;
import org.apache.geode.management.internal.cli.util.RegionAttributesNames;

/***
 * Data class containing the PartitionAttributes for a region on a certain member
 *
 */
public class PartitionAttributesInfo implements Serializable {

  private static final long serialVersionUID = 1L;
  private int totalNumBuckets = 0;
  private int localMaxMemory = 0;
  private int redundantCopies = 0;
  private String colocatedWith = "";
  private long recoveryDelay = 0;
  private long startupRecoveryDelay = 0;
  private String partitionResolverName = "";
  private List<FixedPartitionAttributesInfo> fpaInfoList;
  private final Map<String, String> nonDefaultAttributes;

  public PartitionAttributesInfo(PartitionAttributes<?, ?> partitionAttributes) {
    totalNumBuckets = partitionAttributes.getTotalNumBuckets();
    localMaxMemory = partitionAttributes.getLocalMaxMemory();
    redundantCopies = partitionAttributes.getRedundantCopies();
    colocatedWith = partitionAttributes.getColocatedWith();
    recoveryDelay = partitionAttributes.getRecoveryDelay();
    startupRecoveryDelay = partitionAttributes.getStartupRecoveryDelay();
    PartitionResolver<?, ?> partitionResolver = partitionAttributes.getPartitionResolver();

    if (partitionResolver != null) {
      partitionResolverName = partitionResolver.getName();
    }

    List<FixedPartitionAttributes> fpaList = partitionAttributes.getFixedPartitionAttributes();

    if (fpaList != null) {
      Iterator<FixedPartitionAttributes> iters = fpaList.iterator();
      fpaInfoList = new ArrayList<FixedPartitionAttributesInfo>();

      while (iters.hasNext()) {
        FixedPartitionAttributes fpa = iters.next();
        FixedPartitionAttributesInfo fpaInfo = new FixedPartitionAttributesInfo(fpa);
        fpaInfoList.add(fpaInfo);
      }
    }

    nonDefaultAttributes = new HashMap<String, String>();
    if (totalNumBuckets != RegionAttributesDefault.TOTAL_NUM_BUCKETS) {
      nonDefaultAttributes.put(RegionAttributesNames.TOTAL_NUM_BUCKETS,
          Integer.toString(totalNumBuckets));
    }

    if (localMaxMemory != ((PartitionAttributesImpl) partitionAttributes)
        .getLocalMaxMemoryDefault()) {
      nonDefaultAttributes.put(RegionAttributesNames.LOCAL_MAX_MEMORY,
          Integer.toString(localMaxMemory));
    }


    if (redundantCopies != RegionAttributesDefault.REDUNDANT_COPIES) {
      nonDefaultAttributes.put(RegionAttributesNames.REDUNDANT_COPIES,
          Integer.toString(redundantCopies));
    }


    if (colocatedWith != null
        && !colocatedWith.equals(RegionAttributesDefault.COLOCATED_WITH)) {
      nonDefaultAttributes.put(RegionAttributesNames.COLOCATED_WITH, colocatedWith);
    }


    if (recoveryDelay != RegionAttributesDefault.RECOVERY_DELAY) {
      nonDefaultAttributes.put(RegionAttributesNames.RECOVERY_DELAY,
          Long.toString(recoveryDelay));
    }


    if (startupRecoveryDelay != RegionAttributesDefault.STARTUP_RECOVERY_DELAY) {
      nonDefaultAttributes.put(RegionAttributesNames.STARTUP_RECOVERY_DELAY,
          Long.toString(startupRecoveryDelay));
    }

    if (partitionResolverName != null
        && !partitionResolverName.equals(RegionAttributesDefault.PARTITION_RESOLVER)) {
      nonDefaultAttributes.put(RegionAttributesNames.PARTITION_RESOLVER,
          partitionResolverName);
    }
  }

  public int getTotalNumBuckets() {
    return totalNumBuckets;
  }

  public int getLocalMaxMemory() {
    return localMaxMemory;
  }

  public int getRedundantCopies() {
    return redundantCopies;
  }

  public String getColocatedWith() {
    return colocatedWith;
  }

  public long getRecoveryDelay() {
    return recoveryDelay;
  }

  public long getStartupRecoveryDelay() {
    return startupRecoveryDelay;
  }

  public String getPartitionResolverName() {
    return partitionResolverName;
  }

  public List<FixedPartitionAttributesInfo> getFixedPartitionAttributesInfo() {
    return fpaInfoList;
  }


  public boolean equals(Object obj) {
    if (obj instanceof PartitionAttributesInfo) {
      PartitionAttributesInfo paInfo = (PartitionAttributesInfo) obj;
      return StringUtils.equals(getColocatedWith(), paInfo.getColocatedWith())
          && getLocalMaxMemory() == paInfo.getLocalMaxMemory()
          && StringUtils.equals(getPartitionResolverName(), paInfo.getPartitionResolverName())
          && getRecoveryDelay() == paInfo.getRecoveryDelay()
          && getRedundantCopies() == paInfo.getRedundantCopies()
          && getStartupRecoveryDelay() == paInfo.getStartupRecoveryDelay()
          && getTotalNumBuckets() == paInfo.getTotalNumBuckets()
          && getFixedPartitionAttributesInfo().equals(paInfo.getFixedPartitionAttributesInfo());
    } else {
      return false;
    }
  }

  public int hashCode() {
    return 42; // any arbitrary constant will do

  }

  public Map<String, String> getNonDefaultAttributes() {
    return nonDefaultAttributes;
  }
}
