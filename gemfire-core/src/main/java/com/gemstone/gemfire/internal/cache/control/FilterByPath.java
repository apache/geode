/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.control;

import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;

/**
 * @author dsmith
 *
 */
public class FilterByPath implements RegionFilter {
  
  private final Set<String> included;
  private final Set<String> excluded;
  
  public FilterByPath(Set<String> included, Set<String> excluded) {
    super();
    if (included != null) {
      this.included = new HashSet<String>();
      for (String regionName : included) {
        this.included.add((!regionName.startsWith("/")) ? ("/" + regionName) : regionName);
      }
    }else{
      this.included = null;
    }
    if (excluded != null) {
      this.excluded = new HashSet<String>();
      for (String regionName : excluded) {
        this.excluded.add((!regionName.startsWith("/")) ? ("/" + regionName) : regionName);
      }
    }else{
      this.excluded = null;
    }
  }


  public boolean include(Region<?, ?> region) {
    String fullPath = region.getFullPath();
    if(included != null) {
      return included.contains(fullPath);
    }
    if(excluded != null) {
      return !excluded.contains(fullPath);
    }
    
    return true;
  }

}
