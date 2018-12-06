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
package org.apache.geode.management.internal.cli.domain;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.configuration.ClassNameType;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.ExpirationAttributesType;
import org.apache.geode.cache.configuration.RegionAttributesScope;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.internal.cli.functions.RegionFunctionArgs;

public class RegionConfigFactory {
  public RegionConfig generate(RegionFunctionArgs args) {
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName(getLeafRegion(args.getRegionPath()));

    RegionAttributes<?, ?> regionAttributes = args.getRegionAttributes();
    if (args.getKeyConstraint() != null) {
      addAttribute(regionConfig, a -> a.setKeyConstraint(args.getKeyConstraint()));
    }

    if (args.getValueConstraint() != null) {
      addAttribute(regionConfig, a -> a.setValueConstraint(args.getValueConstraint()));
    }

    if (args.getStatisticsEnabled() != null) {
      addAttribute(regionConfig, a -> a.setStatisticsEnabled(args.getStatisticsEnabled()));
    } else if (regionAttributes != null) {
      addAttribute(regionConfig, a -> a.setStatisticsEnabled(regionAttributes
          .getStatisticsEnabled()));
    }

    if (args.getEntryExpirationIdleTime() != null) {
      RegionAttributesType.EntryIdleTime entryIdleTime = new RegionAttributesType.EntryIdleTime();
      entryIdleTime.setExpirationAttributes(
          args.getEntryExpirationIdleTime().getExpirationAttributes().toConfigType());
      addAttribute(regionConfig, a -> a.setEntryIdleTime(entryIdleTime));
    } else if (regionAttributes != null &&
        regionAttributes.getEntryIdleTimeout() != null &&
        !regionAttributes.getEntryIdleTimeout().isDefault()) {
      RegionAttributesType.EntryIdleTime entryIdleTime = new RegionAttributesType.EntryIdleTime();
      entryIdleTime.setExpirationAttributes(regionAttributes
          .getEntryIdleTimeout().toConfigType());
      addAttribute(regionConfig, a -> a.setEntryIdleTime(entryIdleTime));
    }

    if (args.getEntryIdleTimeCustomExpiry() != null) {
      Object maybeEntryIdleAttr = getAttribute(regionConfig, a -> a.getEntryIdleTime());
      RegionAttributesType.EntryIdleTime entryIdleTime =
          maybeEntryIdleAttr != null ? (RegionAttributesType.EntryIdleTime) maybeEntryIdleAttr
              : new RegionAttributesType.EntryIdleTime();

      ExpirationAttributesType expirationAttributes;
      if (entryIdleTime.getExpirationAttributes() == null) {
        expirationAttributes = new ExpirationAttributesType();
        expirationAttributes.setTimeout("0");
      } else {
        expirationAttributes = entryIdleTime.getExpirationAttributes();
      }

      DeclarableType customExpiry = new DeclarableType();
      customExpiry.setClassName(args.getEntryIdleTimeCustomExpiry().getClassName());
      expirationAttributes.setCustomExpiry(customExpiry);
      entryIdleTime.setExpirationAttributes(expirationAttributes);

      if (maybeEntryIdleAttr == null) {
        addAttribute(regionConfig, a -> a.setEntryIdleTime(entryIdleTime));
      }
    }

    if (args.getEntryExpirationTTL() != null) {
      RegionAttributesType.EntryTimeToLive entryExpTime =
          new RegionAttributesType.EntryTimeToLive();
      entryExpTime.setExpirationAttributes(
          args.getEntryExpirationTTL().getExpirationAttributes().toConfigType());
      addAttribute(regionConfig, a -> a.setEntryTimeToLive(entryExpTime));
    } else if (regionAttributes != null
        && regionAttributes.getEntryTimeToLive() != null
        && !regionAttributes.getEntryTimeToLive().isDefault()) {
      RegionAttributesType.EntryTimeToLive entryExpTime =
          new RegionAttributesType.EntryTimeToLive();
      entryExpTime.setExpirationAttributes(
          regionAttributes.getEntryTimeToLive().toConfigType());
      addAttribute(regionConfig, a -> a.setEntryTimeToLive(entryExpTime));
    }

    if (args.getRegionExpirationIdleTime() != null) {
      RegionAttributesType.RegionIdleTime regionIdleTime =
          new RegionAttributesType.RegionIdleTime();
      regionIdleTime.setExpirationAttributes(
          args.getRegionExpirationIdleTime().getExpirationAttributes().toConfigType());
      addAttribute(regionConfig, a -> a.setRegionIdleTime(regionIdleTime));
    } else if (regionAttributes != null
        && regionAttributes.getRegionIdleTimeout() != null
        && !regionAttributes.getRegionIdleTimeout().isDefault()) {
      RegionAttributesType.RegionIdleTime regionIdleTime =
          new RegionAttributesType.RegionIdleTime();
      regionIdleTime.setExpirationAttributes(
          regionAttributes.getRegionIdleTimeout().toConfigType());
      addAttribute(regionConfig, a -> a.setRegionIdleTime(regionIdleTime));
    }

    if (args.getRegionExpirationTTL() != null) {
      RegionAttributesType.RegionTimeToLive regionExpTime =
          new RegionAttributesType.RegionTimeToLive();
      regionExpTime.setExpirationAttributes(
          args.getRegionExpirationTTL().getExpirationAttributes().toConfigType());
      addAttribute(regionConfig, a -> a.setRegionTimeToLive(regionExpTime));
    } else if (regionAttributes != null
        && regionAttributes.getRegionTimeToLive() != null
        && !regionAttributes.getRegionTimeToLive().isDefault()) {
      RegionAttributesType.RegionTimeToLive regionExpTime =
          new RegionAttributesType.RegionTimeToLive();
      regionExpTime.setExpirationAttributes(
          regionAttributes.getRegionTimeToLive().toConfigType());
      addAttribute(regionConfig, a -> a.setRegionTimeToLive(regionExpTime));
    }

    if (args.getEntryTTLCustomExpiry() != null) {
      Object maybeEntryTTLAttr = getAttribute(regionConfig, a -> a.getEntryTimeToLive());
      RegionAttributesType.EntryTimeToLive entryTimeToLive =
          maybeEntryTTLAttr != null ? (RegionAttributesType.EntryTimeToLive) maybeEntryTTLAttr
              : new RegionAttributesType.EntryTimeToLive();

      ExpirationAttributesType expirationAttributes;
      if (entryTimeToLive.getExpirationAttributes() == null) {
        expirationAttributes = new ExpirationAttributesType();
        expirationAttributes.setTimeout("0");
      } else {
        expirationAttributes = entryTimeToLive.getExpirationAttributes();
      }

      DeclarableType customExpiry = new DeclarableType();
      customExpiry.setClassName(args.getEntryTTLCustomExpiry().getClassName());
      expirationAttributes.setCustomExpiry(customExpiry);
      entryTimeToLive.setExpirationAttributes(expirationAttributes);

      if (maybeEntryTTLAttr == null) {
        addAttribute(regionConfig, a -> a.setEntryTimeToLive(entryTimeToLive));
      }
    }

    if (args.getDiskStore() != null) {
      addAttribute(regionConfig, a -> a.setDiskStoreName(args.getDiskStore()));
    } else if (regionAttributes != null) {
      addAttribute(regionConfig, a -> a.setDiskStoreName(regionAttributes.getDiskStoreName()));
    }

    if (args.getDiskSynchronous() != null) {
      addAttribute(regionConfig, a -> a.setDiskSynchronous(args.getDiskSynchronous()));
    } else if (regionAttributes != null) {
      addAttribute(regionConfig, a -> a.setDiskSynchronous(regionAttributes.isDiskSynchronous()));
    }

    if (args.getEnableAsyncConflation() != null) {
      addAttribute(regionConfig, a -> a.setEnableAsyncConflation(args.getEnableAsyncConflation()));
    } else if (regionAttributes != null) {
      addAttribute(regionConfig, a -> a.setEnableAsyncConflation(regionAttributes
          .getEnableAsyncConflation()));
    }

    if (args.getEnableSubscriptionConflation() != null) {
      addAttribute(regionConfig,
          a -> a.setEnableSubscriptionConflation(args.getEnableSubscriptionConflation()));
    } else if (regionAttributes != null) {
      addAttribute(regionConfig, a -> a.setEnableSubscriptionConflation(regionAttributes
          .getEnableSubscriptionConflation()));
    }

    if (args.getConcurrencyChecksEnabled() != null) {
      addAttribute(regionConfig, a -> a.setConcurrencyChecksEnabled(
          args.getConcurrencyChecksEnabled()));
    } else if (regionAttributes != null) {
      addAttribute(regionConfig, a -> a.setConcurrencyChecksEnabled(regionAttributes
          .getConcurrencyChecksEnabled()));
    }

    if (args.getCloningEnabled() != null) {
      addAttribute(regionConfig, a -> a.setCloningEnabled(args.getCloningEnabled()));
    } else if (regionAttributes != null) {
      addAttribute(regionConfig, a -> a.setCloningEnabled(regionAttributes
          .getCloningEnabled()));
    }

    if (args.getOffHeap() != null) {
      addAttribute(regionConfig, a -> a.setOffHeap(args.getOffHeap()));
    } else if (regionAttributes != null) {
      addAttribute(regionConfig, a -> a.setOffHeap(regionAttributes.getOffHeap()));
    }

    if (args.getMcastEnabled() != null) {
      addAttribute(regionConfig, a -> a.setMulticastEnabled(args.getMcastEnabled()));
    } else if (regionAttributes != null) {
      addAttribute(regionConfig, a -> a.setMulticastEnabled(regionAttributes
          .getMulticastEnabled()));
    }

    if (args.getPartitionArgs() != null) {
      RegionAttributesType.PartitionAttributes partitionAttributes =
          new RegionAttributesType.PartitionAttributes();
      RegionFunctionArgs.PartitionArgs partitionArgs = args.getPartitionArgs();
      partitionAttributes.setColocatedWith(partitionArgs.getPrColocatedWith());
      partitionAttributes.setLocalMaxMemory(int2string(partitionArgs.getPrLocalMaxMemory()));
      partitionAttributes.setRecoveryDelay(long2string(partitionArgs.getPrRecoveryDelay()));
      partitionAttributes.setRedundantCopies(int2string(partitionArgs.getPrRedundantCopies()));
      partitionAttributes
          .setStartupRecoveryDelay(long2string(partitionArgs.getPrStartupRecoveryDelay()));
      partitionAttributes.setTotalMaxMemory(long2string(partitionArgs.getPrTotalMaxMemory()));
      partitionAttributes.setTotalNumBuckets(int2string(partitionArgs.getPrTotalNumBuckets()));

      if (partitionArgs.getPartitionResolver() != null) {
        DeclarableType partitionResolverType = new DeclarableType();
        partitionResolverType.setClassName(partitionArgs.getPartitionResolver());
        partitionAttributes.setPartitionResolver(partitionResolverType);
      }

      addAttribute(regionConfig, a -> a.setPartitionAttributes(partitionAttributes));
    } else if (regionAttributes != null && regionAttributes.getPartitionAttributes() != null) {
      addAttribute(regionConfig, a -> a.setPartitionAttributes(
          regionAttributes.getPartitionAttributes().convertToConfigPartitionAttributes()));
    }

    if (args.getGatewaySenderIds() != null && !args.getGatewaySenderIds().isEmpty()) {
      addAttribute(regionConfig, a -> a.setGatewaySenderIds(String.join(",",
          args.getGatewaySenderIds())));
    }

    if (args.getEvictionAttributes() != null) {
      addAttribute(regionConfig, a -> a.setEvictionAttributes(
          args.getEvictionAttributes().convertToConfigEvictionAttributes()));
    } else if (regionAttributes != null &&
        regionAttributes.getEvictionAttributes() != null &&
        !regionAttributes.getEvictionAttributes().isEmpty()) {
      addAttribute(regionConfig, a -> a.setEvictionAttributes(
          regionAttributes.getEvictionAttributes().convertToConfigEvictionAttributes()));
    }

    if (args.getAsyncEventQueueIds() != null && !args.getAsyncEventQueueIds().isEmpty()) {
      addAttribute(regionConfig,
          a -> a.setAsyncEventQueueIds(String.join(",", args.getAsyncEventQueueIds())));
    }

    if (args.getCacheListeners() != null && !args.getCacheListeners().isEmpty()) {
      addAttribute(regionConfig, a -> a.getCacheListeners().addAll(
          args.getCacheListeners().stream().map(l -> {
            DeclarableType declarableType = new DeclarableType();
            declarableType.setClassName(l.getClassName());
            return declarableType;
          }).collect(Collectors.toList())));
    }

    if (args.getCacheLoader() != null) {
      DeclarableType declarableType = new DeclarableType();
      declarableType.setClassName(args.getCacheLoader().getClassName());
      addAttribute(regionConfig, a -> a.setCacheLoader(declarableType));
    }

    if (args.getCacheWriter() != null) {
      DeclarableType declarableType = new DeclarableType();
      declarableType.setClassName(args.getCacheWriter().getClassName());
      addAttribute(regionConfig, a -> a.setCacheWriter(declarableType));
    }

    if (args.getCompressor() != null) {
      addAttribute(regionConfig, a -> a.setCompressor(new ClassNameType(args.getCompressor())));
      addAttribute(regionConfig, a -> a.setCloningEnabled(true));
    }

    if (args.getConcurrencyLevel() != null) {
      addAttribute(regionConfig, a -> a.setConcurrencyLevel(args.getConcurrencyLevel().toString()));
    } else if (regionAttributes != null) {
      addAttribute(regionConfig, a -> a.setConcurrencyLevel(Integer.toString(
          regionAttributes.getConcurrencyLevel())));
    }

    if (regionAttributes != null && regionAttributes.getDataPolicy() != null) {
      addAttribute(regionConfig,
          a -> a.setDataPolicy(regionAttributes.getDataPolicy().toConfigType()));
    }

    if (regionAttributes != null && regionAttributes.getScope() != null
        && !regionAttributes.getDataPolicy().withPartitioning()) {
      addAttribute(regionConfig,
          a -> a.setScope(
              RegionAttributesScope.fromValue(regionAttributes.getScope().toConfigTypeString())));
    }

    return regionConfig;
  }

  private String int2string(Integer i) {
    return Optional.ofNullable(i).map(j -> j.toString()).orElse(null);
  }

  private String long2string(Long i) {
    return Optional.ofNullable(i).map(j -> j.toString()).orElse(null);
  }

  private String getLeafRegion(String fullPath) {
    String regionPath = fullPath;
    String[] regions = regionPath.split("/");

    return regions[regions.length - 1];
  }

  private void addAttribute(RegionConfig config, Consumer<RegionAttributesType> consumer) {
    if (config.getRegionAttributes() == null) {
      config.setRegionAttributes(new RegionAttributesType());
    }

    consumer.accept(config.getRegionAttributes());
  }

  private Object getAttribute(RegionConfig config,
      Function<RegionAttributesType, Object> function) {
    if (config.getRegionAttributes() == null) {
      return null;
    }

    return function.apply(config.getRegionAttributes());
  }
}
