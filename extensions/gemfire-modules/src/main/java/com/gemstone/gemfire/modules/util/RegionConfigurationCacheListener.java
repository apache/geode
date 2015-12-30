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
package com.gemstone.gemfire.modules.util;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

import java.util.Properties;

public class RegionConfigurationCacheListener extends CacheListenerAdapter<String, RegionConfiguration> implements Declarable {

  private Cache cache;

  public RegionConfigurationCacheListener() {
    this.cache = CacheFactory.getAnyInstance();
  }

  public void afterCreate(EntryEvent<String, RegionConfiguration> event) {
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
      this.cache.getLogger().fine("Region with configuration " + configuration + " existed");
    }
  }

  @Override
  public void afterUpdate(EntryEvent<String, RegionConfiguration> event) {
    // a region could have been destroyed and then
    // re-created, we want to create region again
    // on remote nodes
    afterCreate(event);
  }

  public void afterRegionCreate(RegionEvent<String, RegionConfiguration> event) {
    StringBuilder builder1 = null, builder2 = null;
    Region<String, RegionConfiguration> region = event.getRegion();
    if (this.cache.getLogger().fineEnabled()) {
      builder1 = new StringBuilder();
      int regionSize = region.size();
      if (regionSize > 0) {
        builder1.append("RegionConfigurationCacheListener region ")
            .append(region.getName())
            .append(" has been initialized with the following ")
            .append(regionSize)
            .append(" region configurations:\n");
        builder2 = new StringBuilder();
        builder2.append("RegionConfigurationCacheListener created the following ")
            .append(regionSize)
            .append(" regions:\n");
      } else {
        builder1.append("RegionConfigurationCacheListener region ")
            .append(region.getName())
            .append(" has been initialized with no region configurations");
      }
    }
    for (RegionConfiguration configuration : region.values()) {
      if (this.cache.getLogger().fineEnabled()) {
        builder1.append("\t").append(configuration);
      }
      try {
        Region createRegion = RegionHelper.createRegion(this.cache, configuration);
        if (this.cache.getLogger().fineEnabled()) {
          builder2.append("\t").append(createRegion);
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
