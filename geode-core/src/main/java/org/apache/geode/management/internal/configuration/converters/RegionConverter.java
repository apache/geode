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

import java.util.Optional;

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
    RegionAttributesType regionAttributes = xmlObject.getRegionAttributes();
    region.setType(getRegionType(xmlObject.getType(), regionAttributes));

    if (regionAttributes != null) {
      region.setDiskStoreName(regionAttributes.getDiskStoreName());
      region.setKeyConstraint(regionAttributes.getKeyConstraint());
      region.setValueConstraint(regionAttributes.getValueConstraint());
      RegionAttributesType.PartitionAttributes partitionAttributes =
          regionAttributes.getPartitionAttributes();
      if (partitionAttributes != null && partitionAttributes.getRedundantCopies() != null) {
        region.setRedundantCopies(Integer.parseInt(partitionAttributes.getRedundantCopies()));
      }
    }
    return region;
  }

  @Override
  protected RegionConfig fromNonNullConfigObject(Region configObject) {
    RegionConfig region = new RegionConfig();
    region.setName(configObject.getName());
    region.setType(configObject.getType().name());


    RegionAttributesType attributesType =
        createRegionAttributesByType(configObject.getType().name());

    attributesType.setDiskStoreName(configObject.getDiskStoreName());
    attributesType.setKeyConstraint(configObject.getKeyConstraint());
    attributesType.setValueConstraint(configObject.getValueConstraint());
    region.setRegionAttributes(attributesType);
    return region;
  }

  /**
   * Data policy to regionType is almost a 1-to-1 mapping, except in
   * the case of DataPolicy.PARTITION, we will need to see the local max memory
   * to determine if it's a PARTITION type or a PARTITION_PROXY type.
   *
   * we do our best to infer the type from the existing xml attributes. For data
   * policies not supported by management rest api (for example, NORMAL and RELOADED)
   * it will show as UNSUPPORTED
   */
  public RegionType getRegionType(String refid, RegionAttributesType regionAttributes) {
    if (refid != null) {
      try {
        return RegionType.valueOf(refid);
      }
      catch(Exception e){
        return RegionType.UNSUPPORTED;
      }
    }

    // if refid is null, we will try to determine the type based on the region attributes
    if (regionAttributes == null) {
      return RegionType.UNSUPPORTED;
    }
    RegionAttributesDataPolicy dataPolicy = regionAttributes.getDataPolicy();

    if (regionAttributes.getDataPolicy() == null) {
      return RegionType.UNSUPPORTED;
    }

    switch (dataPolicy) {
      case PARTITION: {
        RegionAttributesType.PartitionAttributes partitionAttributes =
            regionAttributes.getPartitionAttributes();
        if (partitionAttributes != null && partitionAttributes.getLocalMaxMemory().equals("0")) {
          return RegionType.PARTITION_PROXY;
        }
        return RegionType.PARTITION;
      }
      case PERSISTENT_PARTITION: {
        return RegionType.PARTITION_PERSISTENT;
      }
      case PERSISTENT_REPLICATE: {
        return RegionType.REPLICATE_PERSISTENT;
      }
      case REPLICATE: {
        return RegionType.REPLICATE;
      }
      case EMPTY: {
        return RegionType.REPLICATE_PROXY;
      }
    }
    return RegionType.UNSUPPORTED;
  }


  public RegionAttributesType createRegionAttributesByType(String type) {
    RegionAttributesType regionAttributes = new RegionAttributesType();
    switch (type) {
      // these are supported by the management rest api
      case "PARTITION": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        break;
      }
      case "PARTITION_PERSISTENT": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_PARTITION);
        break;
      }
      case "PARTITION_PROXY": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes.setLocalMaxMemory("0");
        break;
      }
      case "REPLICATE": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.REPLICATE);
        regionAttributes.setScope(RegionAttributesScope.DISTRIBUTED_ACK);
        break;
      }
      case "REPLICATE_PERSISTENT": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PERSISTENT_REPLICATE);
        regionAttributes.setScope(RegionAttributesScope.DISTRIBUTED_ACK);
        break;
      }
      case "REPLICATE_PROXY": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.EMPTY);
        regionAttributes.setScope(RegionAttributesScope.DISTRIBUTED_ACK);
        break;
      }
      // below can come from gfsh
      case "PARTITION_REDUNDANT": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes.setRedundantCopy("1");
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

      case "PARTITION_PROXY_REDUNDANT": {
        regionAttributes.setDataPolicy(RegionAttributesDataPolicy.PARTITION);
        regionAttributes.setLocalMaxMemory("0");
        regionAttributes.setRedundantCopy("1");
        break;
      }

      default:
        throw new IllegalArgumentException("invalid type " + type);
    }

    return regionAttributes;
  }
}
