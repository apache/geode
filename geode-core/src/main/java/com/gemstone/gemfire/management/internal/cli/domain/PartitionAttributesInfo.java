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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang.StringUtils;

import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.internal.cache.PartitionAttributesImpl;
import com.gemstone.gemfire.management.internal.cli.util.RegionAttributesDefault;
import com.gemstone.gemfire.management.internal.cli.util.RegionAttributesNames;

/***
 * Data class containing the PartitionAttributes for a region on a certain member
 *
 */
public class PartitionAttributesInfo implements Serializable {

  /**
   * 
   */
  private static final long	serialVersionUID	= 1L;
  private int totalNumBuckets = 0;
  private int localMaxMemory = 0;
  private int redundantCopies = 0;
  private String colocatedWith = "";
  private long recoveryDelay = 0;
  private long startupRecoveryDelay = 0;
  private String partitionResolverName = "";
  private List<FixedPartitionAttributesInfo> fpaInfoList;
  private Map<String, String> nonDefaultAttributes;

  public PartitionAttributesInfo(PartitionAttributes<?, ?> partitionAttributes) {
    this.totalNumBuckets = partitionAttributes.getTotalNumBuckets();
    this.localMaxMemory = partitionAttributes.getLocalMaxMemory();
    this.redundantCopies = partitionAttributes.getRedundantCopies();
    this.colocatedWith = partitionAttributes.getColocatedWith();
    this.recoveryDelay = partitionAttributes.getRecoveryDelay();
    this.startupRecoveryDelay = partitionAttributes.getStartupRecoveryDelay();
    PartitionResolver<?, ?> partitionResolver = partitionAttributes.getPartitionResolver();

    if (partitionResolver != null) {
      partitionResolverName = partitionResolver.getName();
    }

    List<FixedPartitionAttributes> fpaList = partitionAttributes.getFixedPartitionAttributes();
    
    if (fpaList != null) {
      Iterator<FixedPartitionAttributes> iters = fpaList.iterator();
      fpaInfoList = new ArrayList<FixedPartitionAttributesInfo>();

      while (iters.hasNext()) {
        FixedPartitionAttributes fpa = (FixedPartitionAttributes)iters.next();
        FixedPartitionAttributesInfo fpaInfo = new FixedPartitionAttributesInfo(fpa);
        fpaInfoList.add(fpaInfo);
      }
    }
    
    nonDefaultAttributes = new HashMap<String, String>();
    if (this.totalNumBuckets != RegionAttributesDefault.TOTAL_NUM_BUCKETS) {
      nonDefaultAttributes.put(RegionAttributesNames.TOTAL_NUM_BUCKETS, Integer.toString(this.totalNumBuckets));
    }

    if (this.localMaxMemory != ((PartitionAttributesImpl) partitionAttributes).getLocalMaxMemoryDefault()) {
      nonDefaultAttributes.put(RegionAttributesNames.LOCAL_MAX_MEMORY, Integer.toString(this.localMaxMemory));
    }


    if (this.redundantCopies != RegionAttributesDefault.REDUNDANT_COPIES) {
      nonDefaultAttributes.put(RegionAttributesNames.REDUNDANT_COPIES, Integer.toString(this.redundantCopies));
    }


    if (this.colocatedWith != null && !this.colocatedWith.equals(RegionAttributesDefault.COLOCATED_WITH)) {
      nonDefaultAttributes.put(RegionAttributesNames.COLOCATED_WITH, this.colocatedWith);
    }


    if (this.recoveryDelay != RegionAttributesDefault.RECOVERY_DELAY) {
      nonDefaultAttributes.put(RegionAttributesNames.RECOVERY_DELAY, Long.toString(this.recoveryDelay));
    }


    if (this.startupRecoveryDelay != RegionAttributesDefault.STARTUP_RECOVERY_DELAY) {
      nonDefaultAttributes.put(RegionAttributesNames.STARTUP_RECOVERY_DELAY, Long.toString(this.startupRecoveryDelay));
    }

    if (this.partitionResolverName != null && !this.partitionResolverName.equals(RegionAttributesDefault.PARTITION_RESOLVER)) {
      nonDefaultAttributes.put(RegionAttributesNames.PARTITION_RESOLVER, this.partitionResolverName);
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


  public boolean equals (Object obj) {
    if (obj instanceof PartitionAttributesInfo) {
      PartitionAttributesInfo paInfo = (PartitionAttributesInfo) obj;
      return StringUtils.equals(this.getColocatedWith(), paInfo.getColocatedWith())
             && this.getLocalMaxMemory() == paInfo.getLocalMaxMemory()
             && StringUtils.equals(this.getPartitionResolverName(), paInfo.getPartitionResolverName())
             && this.getRecoveryDelay()  == paInfo.getRecoveryDelay()
             && this.getRedundantCopies() == paInfo.getRedundantCopies()
             && this.getStartupRecoveryDelay() == paInfo.getStartupRecoveryDelay()
             && this.getTotalNumBuckets()  == paInfo.getTotalNumBuckets()
             && this.getFixedPartitionAttributesInfo().equals(paInfo.getFixedPartitionAttributesInfo());
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
