/*
 * ========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.util;

import java.util.LinkedList;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.management.ManagementException;
import com.gemstone.gemfire.management.ManagementService;

/**
 * Class to handle Region path. 
 * 
 * @author Abhishek Chaudhari
 * @since 7.0
 */
public class RegionPath {

  private final String regionPath;
  private String regionName;
  private String regionParentPath;

  public RegionPath(String pathName) {
    regionPath = pathName;
    String[] regions = pathName.split(Region.SEPARATOR);
    
    LinkedList<String> regionsNames = new LinkedList<String>();
    for (String region : regions) {
      if (!region.isEmpty()) {
        regionsNames.add(region);
      }
    }
    
    regionName = regionsNames.removeLast();
    StringBuilder parentPathBuilder = new StringBuilder();
    while (!regionsNames.isEmpty()) {
      parentPathBuilder.append(Region.SEPARATOR).append(regionsNames.removeFirst());
    }

    regionParentPath = parentPathBuilder.length() != 0 ? parentPathBuilder.toString() : null;
  }
  
  public String getName() {
    return regionName;
  }

  /**
   * @return the regionPath
   */
  public String getRegionPath() {
    return regionPath;
  }

  public String getParent() {
    return regionParentPath;
  }
  
  /**
   * @return Parent RegionPath of this RegionPath. null if this is a root region
   */
  public RegionPath getParentRegionPath() {
    if (regionParentPath == null) {
      return null;
    }
    return new RegionPath(getParent());
  }
  
  public boolean isRootRegion() {
    return regionParentPath == null;
  }

  public boolean existsInCache(Cache cache) {
    return cache != null && cache.getRegion(regionPath) != null;
  }

  public boolean existsInCluster(Cache cache) {
    boolean existsInCluster = false;

    if (cache != null) {
      ManagementService managementService = ManagementService.getExistingManagementService(cache);
      if (managementService.isManager()) {
        existsInCluster = managementService != null && managementService.getDistributedRegionMXBean(regionPath) != null;
      } else {
        throw new ManagementException("Not a cache from Manager member.");
      }
    }

    return existsInCluster;
  }
  
  @Override
  public String toString() {
    return "RegionPath [regionPath=" + regionPath + "]";
  }

  public static void main(String[] args) {
    RegionPath rp = new RegionPath("/region1/region11/region111/region1112");
    
    System.out.println("name :: "+rp.getName());
    System.out.println("regionpath :: "+rp.getRegionPath());
    System.out.println("parent :: "+rp.getParent());
    System.out.println("parent region path :: "+rp.getParentRegionPath());
    
    System.out.println("---------------------------------------------------");
    
    rp = new RegionPath("/region1");
    
    System.out.println("name :: "+rp.getName());
    System.out.println("regionpath :: "+rp.getRegionPath());
    System.out.println("parent :: "+rp.getParent());
    System.out.println("parent region path :: "+rp.getParentRegionPath());
  }
}
