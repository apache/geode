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
package org.apache.geode.cache.configuration;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.geode.management.internal.cli.functions.RegionFunctionArgs;

public class RegionConfigFactory {
  public RegionConfig generate(RegionFunctionArgs args) {
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName(getLeafRegion(args.getRegionPath()));

    if (args.getRegionShortcut() != null) {
      regionConfig.setRefid(args.getRegionShortcut().toString());
    }

    if (args.getKeyConstraint() != null) {
      addAttribute(regionConfig, a -> a.setKeyConstraint(args.getKeyConstraint()));
    }

    if (args.getValueConstraint() != null) {
      addAttribute(regionConfig, a -> a.setValueConstraint(args.getValueConstraint()));
    }

    if (args.getStatisticsEnabled() != null) {
      addAttribute(regionConfig, a -> a.setStatisticsEnabled(args.getStatisticsEnabled()));
    }

    if (args.getEntryExpirationIdleTime() != null) {
      RegionAttributesType.EntryIdleTime entryIdleTime = new RegionAttributesType.EntryIdleTime();
      entryIdleTime.setExpirationAttributes(
          args.getEntryExpirationIdleTime().getExpirationAttributes().toConfigType());
      addAttribute(regionConfig, a -> a.setEntryIdleTime(entryIdleTime));
    }

    if (args.getEntryExpirationTTL() != null) {
      RegionAttributesType.EntryTimeToLive entryExpTime =
          new RegionAttributesType.EntryTimeToLive();
      entryExpTime.setExpirationAttributes(
          args.getEntryExpirationTTL().getExpirationAttributes().toConfigType());
      addAttribute(regionConfig, a -> a.setEntryTimeToLive(entryExpTime));
    }

    if (args.getRegionExpirationIdleTime() != null) {
      RegionAttributesType.RegionIdleTime regionIdleTime =
          new RegionAttributesType.RegionIdleTime();
      regionIdleTime.setExpirationAttributes(
          args.getRegionExpirationIdleTime().getExpirationAttributes().toConfigType());
      addAttribute(regionConfig, a -> a.setRegionIdleTime(regionIdleTime));
    }

    if (args.getRegionExpirationTTL() != null) {
      RegionAttributesType.RegionTimeToLive regionExpTime =
          new RegionAttributesType.RegionTimeToLive();
      regionExpTime.setExpirationAttributes(
          args.getRegionExpirationTTL().getExpirationAttributes().toConfigType());
      addAttribute(regionConfig, a -> a.setRegionTimeToLive(regionExpTime));
    }

    if (args.getEntryTTLCustomExpiry() != null) {
      Object maybeEntryTTLAttr = getRegionAttributeValue(regionConfig, a -> a.getEntryTimeToLive());
      RegionAttributesType.EntryTimeToLive entryTimeToLive =
          maybeEntryTTLAttr != null ? (RegionAttributesType.EntryTimeToLive) maybeEntryTTLAttr
              : new RegionAttributesType.EntryTimeToLive();
      ExpirationAttributesType expirationAttributes =
          entryTimeToLive.getExpirationAttributes() == null ? new ExpirationAttributesType()
              : entryTimeToLive.getExpirationAttributes();

      DeclarableType customExpiry = new DeclarableType();
      customExpiry.setClassName(args.getEntryTTLCustomExpiry().getClassName());
      expirationAttributes.setCustomExpiry(customExpiry);
      entryTimeToLive.setExpirationAttributes(expirationAttributes);

      if (maybeEntryTTLAttr == null) {
        addAttribute(regionConfig, a -> a.setEntryTimeToLive(entryTimeToLive));
      }
    }

    if (args.getEntryIdleTimeCustomExpiry() != null) {
      Object maybeEntryIdleAttr = getRegionAttributeValue(regionConfig, a -> a.getEntryIdleTime());
      RegionAttributesType.EntryIdleTime entryIdleTime =
          maybeEntryIdleAttr != null ? (RegionAttributesType.EntryIdleTime) maybeEntryIdleAttr
              : new RegionAttributesType.EntryIdleTime();
      ExpirationAttributesType expirationAttributes =
          entryIdleTime.getExpirationAttributes() == null ? new ExpirationAttributesType()
              : entryIdleTime.getExpirationAttributes();

      DeclarableType customExpiry = new DeclarableType();
      customExpiry.setClassName(args.getEntryIdleTimeCustomExpiry().getClassName());
      expirationAttributes.setCustomExpiry(customExpiry);
      entryIdleTime.setExpirationAttributes(expirationAttributes);

      if (maybeEntryIdleAttr == null) {
        addAttribute(regionConfig, a -> a.setEntryIdleTime(entryIdleTime));
      }
    }

    if (args.getDiskStore() != null) {
      addAttribute(regionConfig, a -> a.setDiskStoreName(args.getDiskStore()));
    }

    if (args.getDiskSynchronous() != null) {
      addAttribute(regionConfig, a -> a.setDiskSynchronous(args.getDiskSynchronous()));
    }

    if (args.getEnableAsyncConflation() != null) {
      addAttribute(regionConfig, a -> a.setEnableAsyncConflation(args.getEnableAsyncConflation()));
    }

    if (args.getEnableSubscriptionConflation() != null) {
      addAttribute(regionConfig,
          a -> a.setEnableSubscriptionConflation(args.getEnableSubscriptionConflation()));
    }

    if (args.getConcurrencyChecksEnabled() != null) {
      addAttribute(regionConfig, a -> a.setConcurrencyChecksEnabled(
          args.getConcurrencyChecksEnabled()));
    }

    if (args.getCloningEnabled() != null) {
      addAttribute(regionConfig, a -> a.setCloningEnabled(args.getCloningEnabled()));
    }

    if (args.getOffHeap() != null) {
      addAttribute(regionConfig, a -> a.setOffHeap(args.getOffHeap()));
    }

    if (args.getMcastEnabled() != null) {
      addAttribute(regionConfig, a -> a.setMulticastEnabled(args.getMcastEnabled()));
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

      DeclarableType partitionResolverType = new DeclarableType();
      partitionResolverType.setClassName(partitionArgs.getPartitionResolver());
      partitionAttributes.setPartitionResolver(partitionResolverType);

      addAttribute(regionConfig, a -> a.setPartitionAttributes(partitionAttributes));
    }

    if (args.getGatewaySenderIds() != null && !args.getGatewaySenderIds().isEmpty()) {
      addAttribute(regionConfig, a -> a.setGatewaySenderIds(String.join(",",
          args.getGatewaySenderIds())));
    }

    if (args.getEvictionAttributes() != null) {
      addAttribute(regionConfig, a -> a.setEvictionAttributes(
          args.getEvictionAttributes().convertToConfigEvictionAttributes()));
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
    }

    if (args.getConcurrencyLevel() != null) {
      addAttribute(regionConfig, a -> a.setConcurrencyLevel(args.getConcurrencyLevel().toString()));
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

  private void addAttribute(RegionConfig config, RegionAttributeSetFunction func) {
    final List<RegionAttributesType> regionAttributes = config.getRegionAttributes();
    if (regionAttributes.isEmpty()) {
      regionAttributes.add(new RegionAttributesType());
    }

    func.setAttributeValue(regionAttributes.get(0));
  }

  private Object getRegionAttributeValue(RegionConfig config, RegionAttributeGetFunction function) {
    return config.getRegionAttributes().stream()
        .findFirst()
        .map(a -> function.getValue(a))
        .orElse(null);
  }
}
