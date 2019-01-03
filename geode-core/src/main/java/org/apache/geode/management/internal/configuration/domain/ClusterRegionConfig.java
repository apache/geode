/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.management.internal.configuration.domain;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.ClusterCacheElement;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.internal.cache.PartitionAttributesImpl;

public class ClusterRegionConfig extends RegionConfig implements ClusterCacheElement {
  @Override
  public void addToLocatorConfig(CacheConfig cache) {
    cache.getRegions().add(this);
  }

  @Override
  public void updateInLocatorConfig(CacheConfig cache) {

  }

  @Override
  public void deleteFromLocatorConfig(CacheConfig cache) {

  }

  @Override
  public CacheElement getExistingInLocatorConfig(CacheConfig cache) {
    return null;
  }

  @Override
  public void createOnServer(Cache cache) throws Exception {
    String regionPath = getName();
    if (getRegionAttributes() == null) {
      this.regionAttributes = new RegionAttributesType();
    }

    RegionAttributesType regionAttributes = getRegionAttributes();
    switch (getRefid()) {
      case "PARTITION":
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        RegionAttributesType.PartitionAttributes partitionAttributes =
            new RegionAttributesType.PartitionAttributes();
        partitionAttributes.setRedundantCopies("1");
        regionAttributes.setPartitionAttributes(partitionAttributes);
        break;
      case "REPLICATE":
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.REPLICATE);
        break;
      default:
        break;
    }

    RegionFactory factory = cache.createRegionFactory();
    if (regionAttributes.getPartitionAttributes() != null) {
      factory.setPartitionAttributes(
          PartitionAttributesImpl.fromConfig(regionAttributes.getPartitionAttributes()));
    }

    factory
        .setDataPolicy(DataPolicy.fromString(regionAttributes.getDataPolicy().value().toUpperCase()
            .replace("-", "_")));
    factory.create(regionPath);
  }

  @Override
  public void updateOnServer(Cache cache) throws Exception {

  }

  @Override
  public void deleteFromServer(Cache cache) throws Exception {

  }

  @Override
  public CacheElement getExistingOnServer(Cache cache) {
    return null;
  }
}
