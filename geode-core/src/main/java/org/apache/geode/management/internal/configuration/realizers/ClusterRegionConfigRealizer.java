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

package org.apache.geode.management.internal.configuration.realizers;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.internal.cache.PartitionAttributesImpl;

public class ClusterRegionConfigRealizer extends ConfigurationRealizer {
  public ClusterRegionConfigRealizer(CacheElement config, Cache cache) {
    super(config, cache);
  }

  @Override
  public void create() {
    RegionConfig regionConfig = (RegionConfig) config;
    String regionPath = regionConfig.getName();
    if (regionConfig.getRegionAttributes() == null) {
      regionConfig.setRegionAttributes(new RegionAttributesType());
    }

    RegionAttributesType regionAttributes = regionConfig.getRegionAttributes();
    switch (regionConfig.getRefid()) {
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
}
