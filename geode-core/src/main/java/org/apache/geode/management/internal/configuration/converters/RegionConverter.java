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

package org.apache.geode.management.internal.configuration.converters;

import org.apache.geode.cache.configuration.EnumActionDestroyOverflow;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesScope;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.configuration.RegionType;
import org.apache.geode.management.configuration.Region;

public class RegionConverter extends ConfigurationConverter<Region, RegionConfig> {
  @Override
  protected Region fromNonNullXmlObject(RegionConfig xmlObject) {
    Region region = new Region();
    region.setName(xmlObject.getName());
    if (xmlObject.getType() == null) {
      // older gfsh would generate the region xml without the refid/type. we will not
      // support showing these regions in management rest api for now.
      region.setType(RegionType.UNSUPPORTED);
    } else {
      try {
        region.setType(RegionType.valueOf(xmlObject.getType()));
      } catch (IllegalArgumentException e) {
        // Management rest api will not support showing regions with "LOCAL*" types or user defined
        // refids
        region.setType(RegionType.UNSUPPORTED);
      }
    }
    RegionAttributesType regionAttributes = xmlObject.getRegionAttributes();

    if (regionAttributes != null) {
      region.setDiskStoreName(regionAttributes.getDiskStoreName());
      region.setKeyConstraint(regionAttributes.getKeyConstraint());
      region.setValueConstraint(regionAttributes.getValueConstraint());
    }
    return region;
  }

  @Override
  protected RegionConfig fromNonNullConfigObject(Region configObject) {
    RegionConfig region = new RegionConfig();
    region.setName(configObject.getName());
    region.setType(configObject.getType().name());


    RegionAttributesType attributesType = createRegionAttributesByType(configObject.getType().name());

    attributesType.setDiskStoreName(configObject.getDiskStoreName());
    attributesType.setKeyConstraint(configObject.getKeyConstraint());
    attributesType.setValueConstraint(configObject.getValueConstraint());
    region.setRegionAttributes(attributesType);
    return region;
  }

  public RegionAttributesType createRegionAttributesByType(String type) {
    RegionAttributesType regionAttributes = new RegionAttributesType();
    switch (type) {
      case "PARTITION": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        break;
      }
      case "REPLICATE": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.REPLICATE);
        regionAttributes.setScope(RegionAttributesScope.DISTRIBUTED_ACK);
        break;
      }
      case "PARTITION_REDUNDANT": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes.setRedundantCopy("1");
        break;
      }
      case "PARTITION_PERSISTENT": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION);
        break;
      }
      case "PARTITION_REDUNDANT_PERSISTENT": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION);
        regionAttributes.setRedundantCopy("1");
        break;
      }
      case "PARTITION_OVERFLOW": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes
            .setLruHeapPercentageEvictionAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
        break;
      }
      case "PARTITION_REDUNDANT_OVERFLOW": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes.setRedundantCopy("1");
        regionAttributes
            .setLruHeapPercentageEvictionAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
        break;
      }
      case "PARTITION_PERSISTENT_OVERFLOW": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION);
        regionAttributes
            .setLruHeapPercentageEvictionAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
        break;
      }
      case "PARTITION_REDUNDANT_PERSISTENT_OVERFLOW": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION);
        regionAttributes.setRedundantCopy("1");
        regionAttributes
            .setLruHeapPercentageEvictionAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
        break;
      }
      case "PARTITION_HEAP_LRU": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes
            .setLruHeapPercentageEvictionAction(EnumActionDestroyOverflow.LOCAL_DESTROY);
        break;

      }
      case "PARTITION_REDUNDANT_HEAP_LRU": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes.setRedundantCopy("1");
        regionAttributes
            .setLruHeapPercentageEvictionAction(EnumActionDestroyOverflow.LOCAL_DESTROY);
        break;
      }

      case "REPLICATE_PERSISTENT": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_REPLICATE);
        regionAttributes.setScope(RegionAttributesScope.DISTRIBUTED_ACK);
        break;
      }
      case "REPLICATE_OVERFLOW": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.REPLICATE);
        regionAttributes.setScope(RegionAttributesScope.DISTRIBUTED_ACK);
        regionAttributes
            .setLruHeapPercentageEvictionAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
        break;

      }
      case "REPLICATE_PERSISTENT_OVERFLOW": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_REPLICATE);
        regionAttributes.setScope(RegionAttributesScope.DISTRIBUTED_ACK);
        regionAttributes
            .setLruHeapPercentageEvictionAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
        break;
      }
      case "REPLICATE_HEAP_LRU": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PRELOADED);
        regionAttributes.setScope(RegionAttributesScope.DISTRIBUTED_ACK);
        regionAttributes.setInterestPolicy("all");
        regionAttributes
            .setLruHeapPercentageEvictionAction(EnumActionDestroyOverflow.LOCAL_DESTROY);
        break;
      }
      case "LOCAL": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.NORMAL);
        regionAttributes.setScope(RegionAttributesScope.LOCAL);
        break;
      }
      case "LOCAL_PERSISTENT": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_REPLICATE);
        regionAttributes.setScope(RegionAttributesScope.LOCAL);
        break;
      }
      case "LOCAL_HEAP_LRU": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.NORMAL);
        regionAttributes.setScope(RegionAttributesScope.LOCAL);
        regionAttributes
            .setLruHeapPercentageEvictionAction(EnumActionDestroyOverflow.LOCAL_DESTROY);
        break;
      }
      case "LOCAL_OVERFLOW": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.NORMAL);
        regionAttributes.setScope(RegionAttributesScope.LOCAL);
        regionAttributes
            .setLruHeapPercentageEvictionAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
        break;
      }
      case "LOCAL_PERSISTENT_OVERFLOW": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_REPLICATE);
        regionAttributes.setScope(RegionAttributesScope.LOCAL);
        regionAttributes
            .setLruHeapPercentageEvictionAction(EnumActionDestroyOverflow.OVERFLOW_TO_DISK);
        break;
      }
      case "PARTITION_PROXY": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes.setLocalMaxMemory("0");
        break;
      }
      case "PARTITION_PROXY_REDUNDANT": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes.setLocalMaxMemory("0");
        regionAttributes.setRedundantCopy("1");
        break;
      }
      case "REPLICATE_PROXY": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.EMPTY);
        regionAttributes.setScope(RegionAttributesScope.DISTRIBUTED_ACK);
        break;
      }
      default:
        throw new IllegalArgumentException("invalid type " + type);
    }

    return regionAttributes;
  }
}
