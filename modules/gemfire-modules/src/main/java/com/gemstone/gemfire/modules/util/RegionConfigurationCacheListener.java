/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.util;

import com.gemstone.gemfire.cache.*;

import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import java.util.Properties;

public class RegionConfigurationCacheListener extends CacheListenerAdapter<String,RegionConfiguration> implements Declarable {

  private Cache cache;
  
  public RegionConfigurationCacheListener() {
    this.cache = CacheFactory.getAnyInstance();
  }

  public void afterCreate(EntryEvent<String,RegionConfiguration> event) {
    RegionConfiguration configuration = event.getNewValue();
    if (this.cache.getLogger().fineEnabled()) {
      this.cache.getLogger().fine("RegionConfigurationCacheListener received afterCreate for region " + event.getKey());
    }
    // Create region
    // this is a replicate region, and many VMs can be doing create region
    // simultaneously, so ignore the RegionExistsException
    try {
      Region region = RegionHelper.createRegion(this.cache, configuration);
      if (this.cache.getLogger().fineEnabled()) {
        this.cache.getLogger().fine("RegionConfigurationCacheListener created region: " + region);
      }
    } catch (RegionExistsException exists) {
      // ignore
      this.cache.getLogger().fine("Region with configuration "+configuration+" existed");
    }
  }
  
  @Override
  public void afterUpdate(EntryEvent<String, RegionConfiguration> event) {
    // a region could have been destroyed and then
    // re-created, we want to create region again
    // on remote nodes
    afterCreate(event);
  }
  
  public void afterRegionCreate(RegionEvent<String,RegionConfiguration> event) {
    StringBuilder builder1=null, builder2=null;
    Region<String,RegionConfiguration> region = event.getRegion();
    if (this.cache.getLogger().fineEnabled()) {
      builder1 = new StringBuilder();
      int regionSize = region.size();
      if (regionSize > 0) {
        builder1
          .append("RegionConfigurationCacheListener region ")
          .append(region.getName())
          .append(" has been initialized with the following ")
          .append(regionSize)
          .append(" region configurations:\n");
        builder2 = new StringBuilder();
        builder2
          .append("RegionConfigurationCacheListener created the following ")
          .append(regionSize)
          .append(" regions:\n");
      } else {
        builder1
          .append("RegionConfigurationCacheListener region ")
          .append(region.getName())
          .append(" has been initialized with no region configurations");
      }
    }
    for (RegionConfiguration configuration : region.values()) {
      if (this.cache.getLogger().fineEnabled()) {
        builder1
          .append("\t")
          .append(configuration);
      }
      try {
        Region createRegion = RegionHelper.createRegion(this.cache, configuration);
        if (this.cache.getLogger().fineEnabled()) {
          builder2
            .append("\t")
            .append(createRegion);
        }
      } catch (RegionExistsException exists) {
        // could have been concurrently created by another function
        if (this.cache.getLogger().fineEnabled()) {
          builder2.append("\t").append(" region existed");
        }
      }
    
    }
    if (this.cache.getLogger().fineEnabled()) {
      this.cache.getLogger().fine(builder1.toString());
      if (builder2 != null) {
        this.cache.getLogger().fine(builder2.toString());
      }
    }
  }

  public void init(Properties p) {
  }
}
