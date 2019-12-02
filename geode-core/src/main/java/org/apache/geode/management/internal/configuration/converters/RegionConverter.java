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


import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.cache.configuration.EnumActionDestroyOverflow;
import org.apache.geode.cache.configuration.RegionAttributesDataPolicy;
import org.apache.geode.cache.configuration.RegionAttributesScope;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;

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
      Optional.ofNullable(regionAttributes.getPartitionAttributes())
          .flatMap(
              partitionAttributes -> Optional.ofNullable(partitionAttributes.getRedundantCopies()))
          .ifPresent(copies -> region.setRedundantCopies(Integer.parseInt(copies)));

      RegionAttributesType.ExpirationAttributesType entryIdleTime =
          regionAttributes.getEntryIdleTime();
      List<Region.Expiration> expirations = new ArrayList<>();
      if (entryIdleTime != null) {
        expirations.add(convertFrom(Region.ExpirationType.ENTRY_IDLE_TIME, entryIdleTime));
      }
      RegionAttributesType.ExpirationAttributesType entryTimeToLive =
          regionAttributes.getEntryTimeToLive();
      if (entryTimeToLive != null) {
        expirations.add(convertFrom(Region.ExpirationType.ENTRY_TIME_TO_LIVE, entryTimeToLive));
      }

      RegionAttributesType.ExpirationAttributesType regionIdleTime =
          regionAttributes.getRegionIdleTime();
      if (regionIdleTime != null) {
        expirations.add(convertFrom(Region.ExpirationType.LEGACY, regionIdleTime));
      }

      RegionAttributesType.ExpirationAttributesType regionTimeToLive =
          regionAttributes.getRegionTimeToLive();
      if (regionTimeToLive != null) {
        expirations.add(convertFrom(Region.ExpirationType.LEGACY, regionTimeToLive));
      }

      if (!expirations.isEmpty()) {
        region.setExpirations(expirations);
      }

      if (regionAttributes.getEvictionAttributes() != null) {
        RegionAttributesType.EvictionAttributes evictionAttributes =
            regionAttributes.getEvictionAttributes();
        if (evictionAttributes.getLruMemorySize() != null) {
          region.setEviction(convertFrom(evictionAttributes.getLruMemorySize()));
        }
        if (evictionAttributes.getLruEntryCount() != null) {
          region.setEviction(convertFrom(evictionAttributes.getLruEntryCount()));
        }
        if (evictionAttributes.getLruHeapPercentage() != null) {
          region.setEviction(convertFrom(evictionAttributes.getLruHeapPercentage()));
        }
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

    attributesType.setStatisticsEnabled(true);
    attributesType.setDiskStoreName(configObject.getDiskStoreName());
    attributesType.setKeyConstraint(configObject.getKeyConstraint());
    attributesType.setValueConstraint(configObject.getValueConstraint());

    if (configObject.getRedundantCopies() != null) {
      RegionAttributesType.PartitionAttributes partitionAttributes =
          new RegionAttributesType.PartitionAttributes();
      partitionAttributes.setRedundantCopies(configObject.getRedundantCopies().toString());
      attributesType.setPartitionAttributes(partitionAttributes);
    }

    List<Region.Expiration> expirations = configObject.getExpirations();
    if (expirations != null) {
      for (Region.Expiration expiration : expirations) {
        switch (expiration.getType()) {
          case ENTRY_IDLE_TIME:
            attributesType.setEntryIdleTime(convertFrom(expiration));
            break;
          case ENTRY_TIME_TO_LIVE:
            attributesType.setEntryTimeToLive(convertFrom(expiration));
            break;
        }
      }
    }

    if (configObject.getEviction() != null) {
      attributesType.setEvictionAttributes(convertFrom(configObject.getEviction()));
    }

    region.setRegionAttributes(attributesType);
    return region;
  }

  private RegionAttributesType.EvictionAttributes convertFrom(Region.Eviction eviction) {
    return RegionAttributesType.EvictionAttributes.generate(getEvictionActionString(eviction),
        eviction.getMemorySizeMb(), eviction.getEntryCount(), eviction.getObjectSizer());
  }

  private String getEvictionActionString(Region.Eviction eviction) {
    if (eviction.getAction() == null) {
      return "local-destroy";
    } else {
      switch (eviction.getAction()) {
        case LOCAL_DESTROY:
          return "local-destroy";
        case OVERFLOW_TO_DISK:
          return "overflow-to-disk";
        default:
          throw new IllegalStateException("Unhandled eviction action: " + eviction.getAction());
      }
    }
  }

  private Region.EvictionAction getEvictionAction(EnumActionDestroyOverflow evictionAction) {
    switch (evictionAction) {
      case LOCAL_DESTROY:
        return Region.EvictionAction.LOCAL_DESTROY;
      case OVERFLOW_TO_DISK:
        return Region.EvictionAction.OVERFLOW_TO_DISK;
      default:
        throw new IllegalStateException("Unhandled eviction action xml: " + evictionAction);
    }
  }

  Region.Eviction convertFrom(
      RegionAttributesType.EvictionAttributes.LruMemorySize evictionAttributes) {
    return new Region.Eviction(Region.EvictionType.MEMORY_SIZE,
        getEvictionAction(evictionAttributes.getAction()),
        Integer.parseInt(evictionAttributes.getMaximum()), evictionAttributes.getClassName());
  }

  Region.Eviction convertFrom(
      RegionAttributesType.EvictionAttributes.LruEntryCount evictionAttributes) {
    return new Region.Eviction(Region.EvictionType.ENTRY_COUNT,
        getEvictionAction(evictionAttributes.getAction()),
        Integer.parseInt(evictionAttributes.getMaximum()), null);
  }

  Region.Eviction convertFrom(
      RegionAttributesType.EvictionAttributes.LruHeapPercentage evictionAttributes) {
    return new Region.Eviction(Region.EvictionType.HEAP_PERCENTAGE,
        getEvictionAction(evictionAttributes.getAction()), null,
        evictionAttributes.getClassName());
  }

  RegionAttributesType.ExpirationAttributesType convertFrom(Region.Expiration expiration) {
    RegionAttributesType.ExpirationAttributesType xmlExpiration =
        new RegionAttributesType.ExpirationAttributesType();
    xmlExpiration.setTimeout(expiration.getTimeInSeconds().toString());
    // when action is null from the management api, the default action is DESTROY
    if (expiration.getAction() == null) {
      xmlExpiration.setAction(Region.ExpirationAction.DESTROY.name().toLowerCase());
    } else {
      xmlExpiration.setAction(expiration.getAction().name().toLowerCase());
    }
    return xmlExpiration;
  }

  Region.Expiration convertFrom(Region.ExpirationType type,
      RegionAttributesType.ExpirationAttributesType xmlExpiration) {
    Region.Expiration expiration = new Region.Expiration();
    expiration.setType(type);
    if (StringUtils.isBlank(xmlExpiration.getTimeout())) {
      expiration.setTimeInSeconds(0);
    } else {
      expiration.setTimeInSeconds(Integer.parseInt(xmlExpiration.getTimeout()));
    }

    // in the xml, the default expiration action is INVALIDATE
    if (StringUtils.isBlank(xmlExpiration.getAction())) {
      expiration.setAction(Region.ExpirationAction.INVALIDATE);
    } else {
      try {
        expiration.setAction(
            Region.ExpirationAction.valueOf(xmlExpiration.getAction().toUpperCase()));
      } catch (Exception e) {
        expiration.setAction(Region.ExpirationAction.LEGACY);
      }
    }
    return expiration;
  }

  /**
   * Data policy to regionType is almost a 1-to-1 mapping, except in
   * the case of DataPolicy.PARTITION, we will need to see the local max memory
   * to determine if it's a PARTITION type or a PARTITION_PROXY type.
   *
   * we do our best to infer the type from the existing xml attributes. For data
   * policies not supported by management rest api (for example, NORMAL and PRELOADED)
   * it will show as LEGACY
   */
  public RegionType getRegionType(String refid, RegionAttributesType regionAttributes) {
    if (refid != null) {
      try {
        return RegionType.valueOf(refid);
      } catch (Exception e) {
        // try to determine the region type based on the regionAttributes
      }
    }

    // if refid is null, we will try to determine the type based on the region attributes
    if (regionAttributes == null) {
      return RegionType.LEGACY;
    }
    RegionAttributesDataPolicy dataPolicy = regionAttributes.getDataPolicy();

    if (dataPolicy == null) {
      return RegionType.LEGACY;
    }

    switch (dataPolicy) {
      case PARTITION: {
        RegionAttributesType.PartitionAttributes partitionAttributes =
            regionAttributes.getPartitionAttributes();
        if (partitionAttributes != null && "0".equals(partitionAttributes.getLocalMaxMemory())) {
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
    return RegionType.LEGACY;
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
