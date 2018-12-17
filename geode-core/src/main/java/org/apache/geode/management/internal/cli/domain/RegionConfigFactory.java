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

import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.configuration.ClassNameType;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.RegionAttributesScope;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.internal.cli.functions.RegionFunctionArgs;

public class RegionConfigFactory {
  public RegionConfig generate(RegionFunctionArgs args) {
    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName(getLeafRegion(args.getRegionPath()));
    RegionAttributesType regionAttributesType = new RegionAttributesType();
    regionConfig.setRegionAttributes(regionAttributesType);

    RegionAttributes<?, ?> regionAttributes = args.getRegionAttributes();

    if (args.getKeyConstraint() != null) {
      regionAttributesType.setKeyConstraint(args.getKeyConstraint());
    }

    if (args.getValueConstraint() != null) {
      regionAttributesType.setValueConstraint(args.getValueConstraint());
    }

    if (args.getStatisticsEnabled() != null) {
      regionAttributesType.setStatisticsEnabled(args.getStatisticsEnabled());
    } else if (regionAttributes != null) {
      regionAttributesType.setStatisticsEnabled(regionAttributes.getStatisticsEnabled());
    }

    // first get the expiration attributes from the command options
    regionAttributesType.setEntryIdleTime(getExpirationAttributes(args.getEntryExpirationIdleTime(),
        args.getEntryIdleTimeCustomExpiry()));
    regionAttributesType.setEntryTimeToLive(
        getExpirationAttributes(args.getEntryExpirationTTL(), args.getEntryTTLCustomExpiry()));
    regionAttributesType
        .setRegionIdleTime(getExpirationAttributes(args.getRegionExpirationIdleTime(), null));
    regionAttributesType
        .setRegionTimeToLive(getExpirationAttributes(args.getRegionExpirationTTL(), null));

    // if regionAttributes has these attributes, then use that
    if (regionAttributes != null) {
      if (regionAttributesType.getEntryIdleTime() == null) {
        regionAttributesType.setEntryIdleTime(getExpirationAttributes(
            regionAttributes.getEntryIdleTimeout(), regionAttributes.getCustomEntryIdleTimeout()));
      }
      if (regionAttributesType.getEntryTimeToLive() == null) {
        regionAttributesType.setEntryTimeToLive(getExpirationAttributes(
            regionAttributes.getEntryTimeToLive(), regionAttributes.getCustomEntryTimeToLive()));
      }

      if (regionAttributesType.getRegionIdleTime() == null) {
        regionAttributesType.setRegionIdleTime(
            getExpirationAttributes(regionAttributes.getRegionIdleTimeout(), null));
      }

      if (regionAttributesType.getRegionTimeToLive() == null) {
        regionAttributesType.setRegionTimeToLive(
            getExpirationAttributes(regionAttributes.getRegionTimeToLive(), null));
      }
    }


    if (args.getDiskStore() != null) {
      regionAttributesType.setDiskStoreName(args.getDiskStore());
    } else if (regionAttributes != null) {
      regionAttributesType.setDiskStoreName(regionAttributes.getDiskStoreName());
    }

    if (args.getDiskSynchronous() != null) {
      regionAttributesType.setDiskSynchronous(args.getDiskSynchronous());
    } else if (regionAttributes != null) {
      regionAttributesType.setDiskSynchronous(regionAttributes.isDiskSynchronous());
    }

    if (args.getEnableAsyncConflation() != null) {
      regionAttributesType.setEnableAsyncConflation(args.getEnableAsyncConflation());
    } else if (regionAttributes != null) {
      regionAttributesType.setEnableAsyncConflation(regionAttributes.getEnableAsyncConflation());
    }

    if (args.getEnableSubscriptionConflation() != null) {
      regionAttributesType.setEnableSubscriptionConflation(args.getEnableSubscriptionConflation());
    } else if (regionAttributes != null) {
      regionAttributesType
          .setEnableSubscriptionConflation(regionAttributes.getEnableSubscriptionConflation());
    }

    if (args.getConcurrencyChecksEnabled() != null) {
      regionAttributesType.setConcurrencyChecksEnabled(args.getConcurrencyChecksEnabled());
    } else if (regionAttributes != null) {
      regionAttributesType
          .setConcurrencyChecksEnabled(regionAttributes.getConcurrencyChecksEnabled());
    }

    if (args.getCloningEnabled() != null) {
      regionAttributesType.setCloningEnabled(args.getCloningEnabled());
    } else if (regionAttributes != null) {
      regionAttributesType.setCloningEnabled(regionAttributes.getCloningEnabled());
    }

    if (args.getOffHeap() != null) {
      regionAttributesType.setOffHeap(args.getOffHeap());
    } else if (regionAttributes != null) {
      regionAttributesType.setOffHeap(regionAttributes.getOffHeap());
    }

    if (args.getMcastEnabled() != null) {
      regionAttributesType.setMulticastEnabled(args.getMcastEnabled());
    } else if (regionAttributes != null) {
      regionAttributesType.setMulticastEnabled(regionAttributes.getMulticastEnabled());
    }

    if (args.getPartitionArgs() != null) {
      RegionAttributesType.PartitionAttributes partitionAttributes =
          new RegionAttributesType.PartitionAttributes();
      RegionFunctionArgs.PartitionArgs partitionArgs = args.getPartitionArgs();
      partitionAttributes.setColocatedWith(partitionArgs.getPrColocatedWith());
      partitionAttributes.setLocalMaxMemory(Objects.toString(partitionArgs.getPrLocalMaxMemory()));
      partitionAttributes.setRecoveryDelay(Objects.toString(partitionArgs.getPrRecoveryDelay()));
      partitionAttributes
          .setRedundantCopies(Objects.toString(partitionArgs.getPrRedundantCopies()));
      partitionAttributes
          .setStartupRecoveryDelay(Objects.toString(partitionArgs.getPrStartupRecoveryDelay()));
      partitionAttributes.setTotalMaxMemory(Objects.toString(partitionArgs.getPrTotalMaxMemory()));
      partitionAttributes
          .setTotalNumBuckets(Objects.toString(partitionArgs.getPrTotalNumBuckets()));

      if (partitionArgs.getPartitionResolver() != null) {
        DeclarableType partitionResolverType = new DeclarableType();
        partitionResolverType.setClassName(partitionArgs.getPartitionResolver());
        partitionAttributes.setPartitionResolver(partitionResolverType);
      }

      regionAttributesType.setPartitionAttributes(partitionAttributes);
    } else if (regionAttributes != null && regionAttributes.getPartitionAttributes() != null) {
      regionAttributesType.setPartitionAttributes(
          regionAttributes.getPartitionAttributes().convertToConfigPartitionAttributes());
    }

    if (args.getGatewaySenderIds() != null && !args.getGatewaySenderIds().isEmpty()) {
      regionAttributesType.setGatewaySenderIds(String.join(",", args.getGatewaySenderIds()));
    }

    if (args.getEvictionAttributes() != null) {
      regionAttributesType
          .setEvictionAttributes(args.getEvictionAttributes().convertToConfigEvictionAttributes());
    } else if (regionAttributes != null &&
        regionAttributes.getEvictionAttributes() != null &&
        !regionAttributes.getEvictionAttributes().isEmpty()) {
      regionAttributesType.setEvictionAttributes(
          regionAttributes.getEvictionAttributes().convertToConfigEvictionAttributes());
    }

    if (args.getAsyncEventQueueIds() != null && !args.getAsyncEventQueueIds().isEmpty()) {
      regionAttributesType.setAsyncEventQueueIds(String.join(",", args.getAsyncEventQueueIds()));
    }

    if (args.getCacheListeners() != null && !args.getCacheListeners().isEmpty()) {
      regionAttributesType.getCacheListeners().addAll(args.getCacheListeners().stream().map(l -> {
        DeclarableType declarableType = new DeclarableType();
        declarableType.setClassName(l.getClassName());
        return declarableType;
      }).collect(Collectors.toList()));
    }

    if (args.getCacheLoader() != null) {
      DeclarableType declarableType = new DeclarableType();
      declarableType.setClassName(args.getCacheLoader().getClassName());
      regionAttributesType.setCacheLoader(declarableType);
    }

    if (args.getCacheWriter() != null) {
      DeclarableType declarableType = new DeclarableType();
      declarableType.setClassName(args.getCacheWriter().getClassName());
      regionAttributesType.setCacheWriter(declarableType);
    }

    if (args.getCompressor() != null) {
      regionAttributesType.setCompressor(new ClassNameType(args.getCompressor()));
      regionAttributesType.setCloningEnabled(true);
    }

    if (args.getConcurrencyLevel() != null) {
      regionAttributesType.setConcurrencyLevel(args.getConcurrencyLevel().toString());
    } else if (regionAttributes != null) {
      regionAttributesType
          .setConcurrencyLevel(Integer.toString(regionAttributes.getConcurrencyLevel()));
    }

    if (regionAttributes != null && regionAttributes.getDataPolicy() != null) {
      regionAttributesType.setDataPolicy(regionAttributes.getDataPolicy().toConfigType());
    }

    if (regionAttributes != null && regionAttributes.getScope() != null
        && !regionAttributes.getDataPolicy().withPartitioning()) {
      regionAttributesType.setScope(
          RegionAttributesScope.fromValue(regionAttributes.getScope().toConfigTypeString()));
    }

    return regionConfig;
  }

  public static RegionAttributesType.ExpirationAttributesType getExpirationAttributes(
      ExpirationAttributes entryIdleTimeout, CustomExpiry<?, ?> customEntryIdleTimeout) {

    if ((entryIdleTimeout == null || entryIdleTimeout.isDefault())
        && customEntryIdleTimeout == null) {
      return null;
    }

    if (entryIdleTimeout == null || entryIdleTimeout.isDefault()) {
      return getExpirationAttributes(null, null,
          new ClassName<>(customEntryIdleTimeout.getClass().getName()));
    } else if (customEntryIdleTimeout == null) {
      return getExpirationAttributes(entryIdleTimeout.getTimeout(), entryIdleTimeout.getAction(),
          null);
    } else {
      return getExpirationAttributes(entryIdleTimeout.getTimeout(), entryIdleTimeout.getAction(),
          new ClassName<>(customEntryIdleTimeout.getClass().getName()));
    }
  }

  public static RegionAttributesType.ExpirationAttributesType getExpirationAttributes(
      RegionFunctionArgs.ExpirationAttrs expirationAttrs, ClassName<CustomExpiry> customExpiry) {
    if (expirationAttrs == null) {
      return getExpirationAttributes(null, null, customExpiry);
    } else {
      return getExpirationAttributes(expirationAttrs.getTime(), expirationAttrs.getAction(),
          customExpiry);
    }
  }

  public static RegionAttributesType.ExpirationAttributesType getExpirationAttributes(
      Integer timeout, ExpirationAction action, ClassName<CustomExpiry> expiry) {
    if (timeout == null && action == null && expiry == null) {
      return null;
    }
    RegionAttributesType.ExpirationAttributesType attributesType =
        new RegionAttributesType.ExpirationAttributesType();

    attributesType.setTimeout(Objects.toString(timeout, "0"));
    if (action == null) {
      action = ExpirationAction.INVALIDATE;
    }
    attributesType.setAction(action.toXmlString());

    if (expiry != null) {
      attributesType
          .setCustomExpiry(new DeclarableType(expiry.getClassName(), expiry.getInitProperties()));
    }

    return attributesType;
  }


  private String getLeafRegion(String fullPath) {
    String regionPath = fullPath;
    String[] regions = regionPath.split("/");

    return regions[regions.length - 1];
  }
}
