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
package org.apache.geode.management.internal.cli.util;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.management.ManagementException;
import org.apache.geode.management.ManagementService;

/**
 * Class to handle Region path.
 *
 * @since GemFire 7.0
 */
public class RegionPath {

  private final String regionPath;
  private String regionName;
  private String regionParentPath;

  public RegionPath(String pathName) {
    regionPath = pathName;
    String[] regions = pathName.split(Region.SEPARATOR);

    LinkedList<String> regionsNames = new LinkedList<>();
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

  public boolean isRoot() {
    return regionParentPath == Region.SEPARATOR || regionParentPath == null;
  }

  public String[] getRegionsOnParentPath() {
    String[] regionsOnPath = getParent().split(Region.SEPARATOR);

    // Ignore preceding separator if there is one
    int start = regionsOnPath[0] == null || regionsOnPath[0].isEmpty() ? 1 : 0;

    List<String> regions = new ArrayList<>();
    for (int i = start; i < regionsOnPath.length; i++) {
      regions.add(regionsOnPath[i]);
    }

    return regions.toArray(new String[] {});
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
        existsInCluster = managementService != null
            && managementService.getDistributedRegionMXBean(regionPath) != null;
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

    System.out.println("name :: " + rp.getName());
    System.out.println("regionpath :: " + rp.getRegionPath());
    System.out.println("parent :: " + rp.getParent());
    System.out.println("parent region path :: " + rp.getParentRegionPath());

    System.out.println("---------------------------------------------------");

    rp = new RegionPath("/region1");

    System.out.println("name :: " + rp.getName());
    System.out.println("regionpath :: " + rp.getRegionPath());
    System.out.println("parent :: " + rp.getParent());
    System.out.println("parent region path :: " + rp.getParentRegionPath());
  }
}
