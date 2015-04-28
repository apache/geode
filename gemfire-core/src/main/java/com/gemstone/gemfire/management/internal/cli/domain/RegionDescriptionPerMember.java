/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.domain;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;

public class RegionDescriptionPerMember implements Serializable {

  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  private int size = 0;
  private RegionAttributesInfo regionAttributesInfo;
  private String hostingMember;
  private String name;
  private boolean isAccessor = false;
  
  public RegionDescriptionPerMember(Region<?,?> region, String hostingMember) {
    this.regionAttributesInfo = new RegionAttributesInfo(region.getAttributes());
    this.hostingMember = hostingMember;
    this.size = region.size();
    this.name = region.getFullPath().substring(1);
    
    //For the replicated proxy
    if ((this.regionAttributesInfo.getDataPolicy().equals(DataPolicy.EMPTY)
        && this.regionAttributesInfo.getScope().equals(Scope.DISTRIBUTED_ACK))) {
      setAccessor(true);
    } 
    
    //For the partitioned proxy
    if (this.regionAttributesInfo.getPartitionAttributesInfo() != null && this.regionAttributesInfo.getPartitionAttributesInfo().getLocalMaxMemory() == 0) {
      setAccessor(true);
    }
  }

  public boolean equals(Object obj) {
    if (obj instanceof RegionDescriptionPerMember) {
      RegionDescriptionPerMember regionDesc = (RegionDescriptionPerMember) obj;

      return this.name.equals(regionDesc.getName())
          && this.getScope().equals(regionDesc.getScope())
          && this.getDataPolicy().equals(regionDesc.getDataPolicy())
          && this.isAccessor == regionDesc.isAccessor;
    }
    return true;
  }

  public int hashCode() {
    return 42; // any arbitrary constant will do
    
  }
  
  public String getHostingMember() {
    return hostingMember;
  }

  public int getSize() {
    return this.size;
  }

  public String getName() {
    return this.name;
  }
  
  public Scope getScope() {
    return this.regionAttributesInfo.getScope();
  }
  
  public DataPolicy getDataPolicy() {
    return this.regionAttributesInfo.getDataPolicy();
  }
  
  public Map<String, String> getNonDefaultRegionAttributes() {
    this.regionAttributesInfo.getNonDefaultAttributes().put("size", Integer.toString(this.size));
    return this.regionAttributesInfo.getNonDefaultAttributes();
  }
  
  public Map<String, String> getNonDefaultEvictionAttributes() {
    EvictionAttributesInfo eaInfo = regionAttributesInfo.getEvictionAttributesInfo();
    if (eaInfo != null) {
      return eaInfo.getNonDefaultAttributes();
    } else {
      return Collections.emptyMap();
    }
  }
  
  public Map<String, String> getNonDefaultPartitionAttributes() {
    PartitionAttributesInfo paInfo = regionAttributesInfo.getPartitionAttributesInfo();
    if (paInfo != null) {
      return paInfo.getNonDefaultAttributes();
    } else {
      return Collections.emptyMap();
    }
  }
  
  public List<FixedPartitionAttributesInfo> getFixedPartitionAttributes() {
   PartitionAttributesInfo paInfo = regionAttributesInfo.getPartitionAttributesInfo();
   List<FixedPartitionAttributesInfo> fpa = null;
   
   if (paInfo != null) {
     fpa = paInfo.getFixedPartitionAttributesInfo();
   }
   
   return fpa;
  }

  public boolean isAccessor() {
    return isAccessor;
  }

  public void setAccessor(boolean isAccessor) {
    this.isAccessor = isAccessor;
  }
}
