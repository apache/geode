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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.configuration.ClassNameType;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.EnumActionDestroyOverflow;
import org.apache.geode.cache.configuration.RegionAttributesScope;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;

public class RegionConfigFactory {
  public RegionConfig generate(
      String regionPath,
      String keyConstraint,
      String valueConstraint,
      Boolean statisticsEnabled,
      Integer entryExpirationIdleTime,
      ExpirationAction entryExpirationIdleAction,
      Integer entryExpirationTTL,
      ExpirationAction entryExpirationTTLAction,
      ClassName<CustomExpiry> entryIdleTimeCustomExpiry,
      ClassName<CustomExpiry> entryTTLCustomExpiry,
      Integer regionExpirationIdleTime,
      ExpirationAction regionExpirationIdleAction,
      Integer regionExpirationTTL,
      ExpirationAction regionExpirationTTLAction,
      String evictionAction,
      Integer evictionMaxMemory,
      Integer evictionEntryCount,
      String evictionObjectSizer,
      String diskStore,
      Boolean diskSynchronous,
      Boolean enableAsyncConflation,
      Boolean enableSubscriptionConflation,
      Set<ClassName<CacheListener>> cacheListeners,
      ClassName<CacheLoader> cacheLoader,
      ClassName<CacheWriter> cacheWriter,
      Set<String> asyncEventQueueIds,
      Set<String> gatewaySenderIds,
      Boolean concurrencyChecksEnabled,
      Boolean cloningEnabled,
      Boolean mcastEnabled,
      Integer concurrencyLevel,
      PartitionArgs partitionArgs,
      String compressor,
      Boolean offHeap,
      RegionAttributes<?, ?> regionAttributes) {

    RegionConfig regionConfig = new RegionConfig();
    regionConfig.setName(getLeafRegion(regionPath));
    RegionAttributesType regionAttributesType = new RegionAttributesType();
    regionConfig.setRegionAttributes(regionAttributesType);

    if (keyConstraint != null) {
      regionAttributesType.setKeyConstraint(keyConstraint);
    }

    if (valueConstraint != null) {
      regionAttributesType.setValueConstraint(valueConstraint);
    }

    if (statisticsEnabled != null) {
      regionAttributesType.setStatisticsEnabled(statisticsEnabled);
    } else if (regionAttributes != null) {
      regionAttributesType.setStatisticsEnabled(regionAttributes.getStatisticsEnabled());
    }

    // first get the expiration attributes from the command options
    regionAttributesType.setEntryIdleTime(
        getExpirationAttributes(entryExpirationIdleTime, entryExpirationIdleAction,
            entryIdleTimeCustomExpiry));
    regionAttributesType
        .setEntryTimeToLive(getExpirationAttributes(entryExpirationTTL, entryExpirationTTLAction,
            entryTTLCustomExpiry));
    regionAttributesType.setRegionIdleTime(
        getExpirationAttributes(regionExpirationIdleTime, regionExpirationIdleAction,
            null));
    regionAttributesType
        .setRegionTimeToLive(getExpirationAttributes(regionExpirationTTL, regionExpirationTTLAction,
            null));

    // if regionAttributes has these attributes, then use them instead
    if (regionAttributes != null) {
      ExpirationAttributes entryIdleTimeout = regionAttributes.getEntryIdleTimeout();
      if (entryIdleTimeout != null && !entryIdleTimeout.isDefault()
          && regionAttributesType.getEntryIdleTime() == null) {
        regionAttributesType.setEntryIdleTime(getExpirationAttributes(
            entryIdleTimeout.getTimeout(), entryIdleTimeout.getAction(),
            getClassName(regionAttributes.getCustomEntryIdleTimeout())));
      }

      ExpirationAttributes entryTimeToLive = regionAttributes.getEntryTimeToLive();
      if (entryTimeToLive != null && !entryTimeToLive.isDefault()
          && regionAttributesType.getEntryTimeToLive() == null) {
        regionAttributesType.setEntryTimeToLive(getExpirationAttributes(
            entryTimeToLive.getTimeout(), entryTimeToLive.getAction(),
            getClassName(regionAttributes.getCustomEntryTimeToLive())));
      }

      ExpirationAttributes regionIdleTimeout = regionAttributes.getRegionIdleTimeout();
      if (regionIdleTimeout != null && !regionIdleTimeout.isDefault()
          && regionAttributesType.getRegionIdleTime() == null) {
        regionAttributesType.setRegionIdleTime(
            getExpirationAttributes(regionIdleTimeout.getTimeout(),
                regionIdleTimeout.getAction(), null));
      }

      ExpirationAttributes regionTimeToLive = regionAttributes.getRegionTimeToLive();
      if (regionTimeToLive != null && !regionTimeToLive.isDefault()
          && regionAttributesType.getRegionTimeToLive() == null) {
        regionAttributesType.setRegionTimeToLive(
            getExpirationAttributes(regionTimeToLive.getTimeout(),
                regionTimeToLive.getAction(), null));
      }
    }

    if (diskStore != null) {
      regionAttributesType.setDiskStoreName(diskStore);
    } else if (regionAttributes != null) {
      regionAttributesType.setDiskStoreName(regionAttributes.getDiskStoreName());
    }

    if (diskSynchronous != null) {
      regionAttributesType.setDiskSynchronous(diskSynchronous);
    } else if (regionAttributes != null) {
      regionAttributesType.setDiskSynchronous(regionAttributes.isDiskSynchronous());
    }

    if (enableAsyncConflation != null) {
      regionAttributesType.setEnableAsyncConflation(enableAsyncConflation);
    } else if (regionAttributes != null) {
      regionAttributesType.setEnableAsyncConflation(regionAttributes.getEnableAsyncConflation());
    }

    if (enableSubscriptionConflation != null) {
      regionAttributesType.setEnableSubscriptionConflation(enableSubscriptionConflation);
    } else if (regionAttributes != null) {
      regionAttributesType
          .setEnableSubscriptionConflation(regionAttributes.getEnableSubscriptionConflation());
    }

    if (concurrencyChecksEnabled != null) {
      regionAttributesType.setConcurrencyChecksEnabled(concurrencyChecksEnabled);
    } else if (regionAttributes != null) {
      regionAttributesType
          .setConcurrencyChecksEnabled(regionAttributes.getConcurrencyChecksEnabled());
    }

    if (cloningEnabled != null) {
      regionAttributesType.setCloningEnabled(cloningEnabled);
    } else if (regionAttributes != null) {
      regionAttributesType.setCloningEnabled(regionAttributes.getCloningEnabled());
    }

    if (offHeap != null) {
      regionAttributesType.setOffHeap(offHeap);
    } else if (regionAttributes != null) {
      regionAttributesType.setOffHeap(regionAttributes.getOffHeap());
    }

    if (mcastEnabled != null) {
      regionAttributesType.setMulticastEnabled(mcastEnabled);
    } else if (regionAttributes != null) {
      regionAttributesType.setMulticastEnabled(regionAttributes.getMulticastEnabled());
    }

    if (partitionArgs != null && !partitionArgs.isEmpty()) {
      RegionAttributesType.PartitionAttributes partitionAttributes =
          new RegionAttributesType.PartitionAttributes();
      regionAttributesType.setPartitionAttributes(partitionAttributes);

      partitionAttributes.setColocatedWith(partitionArgs.prColocatedWith);
      partitionAttributes.setLocalMaxMemory(int2string(partitionArgs.prLocalMaxMemory));
      partitionAttributes.setRecoveryDelay(long2string(partitionArgs.prRecoveryDelay));
      partitionAttributes.setRedundantCopies(int2string(partitionArgs.prRedundantCopies));
      partitionAttributes
          .setStartupRecoveryDelay(long2string(partitionArgs.prStartupRecoveryDelay));
      partitionAttributes.setTotalMaxMemory(long2string(partitionArgs.prTotalMaxMemory));
      partitionAttributes.setTotalNumBuckets(int2string(partitionArgs.prTotalNumBuckets));

      if (partitionArgs.partitionResolver != null) {
        DeclarableType partitionResolverType = new DeclarableType();
        partitionResolverType.setClassName(partitionArgs.partitionResolver);
        partitionAttributes.setPartitionResolver(partitionResolverType);
      }
    }

    if (regionAttributes != null && regionAttributes.getPartitionAttributes() != null) {
      RegionAttributesType.PartitionAttributes partitionAttributes = Optional.ofNullable(
          regionAttributesType.getPartitionAttributes())
          .orElse(new RegionAttributesType.PartitionAttributes());
      regionAttributesType.setPartitionAttributes(partitionAttributes);

      RegionAttributesType.PartitionAttributes implicitPartitionAttributes =
          regionAttributes.getPartitionAttributes().convertToConfigPartitionAttributes();

      String implicitColocatedWith = implicitPartitionAttributes.getColocatedWith();
      if (partitionAttributes.getColocatedWith() == null && implicitColocatedWith != null) {
        partitionAttributes.setColocatedWith(implicitColocatedWith);
      }

      String implicitLocalMaxMemory = implicitPartitionAttributes.getLocalMaxMemory();
      if (partitionAttributes.getLocalMaxMemory() == null && implicitLocalMaxMemory != null) {
        partitionAttributes.setLocalMaxMemory(implicitLocalMaxMemory);
      }

      String implicitRecoveryDelay = implicitPartitionAttributes.getRecoveryDelay();
      if (partitionAttributes.getRecoveryDelay() == null && implicitRecoveryDelay != null) {
        partitionAttributes.setRecoveryDelay(implicitRecoveryDelay);
      }

      String implicitRedundantCopies = implicitPartitionAttributes.getRedundantCopies();
      if (partitionAttributes.getRedundantCopies() == null && implicitRedundantCopies != null) {
        partitionAttributes.setRedundantCopies(implicitRedundantCopies);
      }

      String implicitStartupRecoveryDelay = implicitPartitionAttributes.getStartupRecoveryDelay();
      if (partitionAttributes.getStartupRecoveryDelay() == null
          && implicitStartupRecoveryDelay != null) {
        partitionAttributes.setStartupRecoveryDelay(implicitStartupRecoveryDelay);
      }

      String implicitTotalMaxMemory = implicitPartitionAttributes.getTotalMaxMemory();
      if (partitionAttributes.getTotalMaxMemory() == null && implicitTotalMaxMemory != null) {
        partitionAttributes.setTotalMaxMemory(implicitTotalMaxMemory);
      }

      String implicitTotalNumBuckets = implicitPartitionAttributes.getTotalNumBuckets();
      if (partitionAttributes.getTotalNumBuckets() == null && implicitTotalNumBuckets != null) {
        partitionAttributes.setTotalNumBuckets(implicitTotalNumBuckets);
      }

      DeclarableType implicitPartitionResolver = implicitPartitionAttributes.getPartitionResolver();
      if (partitionAttributes.getPartitionResolver() == null && implicitPartitionResolver != null) {
        partitionAttributes.setPartitionResolver(implicitPartitionResolver);
      }
    }

    if (gatewaySenderIds != null && !gatewaySenderIds.isEmpty()) {
      regionAttributesType.setGatewaySenderIds(String.join(",", gatewaySenderIds));
    }

    if (evictionAction != null) {
      RegionAttributesType.EvictionAttributes evictionAttributes =
          generateEvictionAttributes(evictionAction, evictionMaxMemory, evictionEntryCount,
              evictionObjectSizer);
      regionAttributesType.setEvictionAttributes(evictionAttributes);
    } else if (regionAttributes != null && regionAttributes.getEvictionAttributes() != null
        && !regionAttributes.getEvictionAttributes().isNoEviction()) {
      regionAttributesType.setEvictionAttributes(regionAttributes.getEvictionAttributes()
          .convertToConfigEvictionAttributes());
    }

    if (asyncEventQueueIds != null && !asyncEventQueueIds.isEmpty()) {
      regionAttributesType.setAsyncEventQueueIds(String.join(",", asyncEventQueueIds));
    }

    if (cacheListeners != null && !cacheListeners.isEmpty()) {
      regionAttributesType.getCacheListeners().addAll(cacheListeners.stream().map(l -> {
        DeclarableType declarableType = new DeclarableType();
        declarableType.setClassName(l.getClassName());
        return declarableType;
      }).collect(Collectors.toList()));
    }

    if (cacheLoader != null) {
      DeclarableType declarableType = new DeclarableType();
      declarableType.setClassName(cacheLoader.getClassName());
      regionAttributesType.setCacheLoader(declarableType);
    }

    if (cacheWriter != null) {
      DeclarableType declarableType = new DeclarableType();
      declarableType.setClassName(cacheWriter.getClassName());
      regionAttributesType.setCacheWriter(declarableType);
    }

    if (compressor != null) {
      regionAttributesType.setCompressor(new ClassNameType(compressor));
      regionAttributesType.setCloningEnabled(true);
    }

    if (concurrencyLevel != null) {
      regionAttributesType.setConcurrencyLevel(concurrencyLevel.toString());
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

  private RegionAttributesType.EvictionAttributes generateEvictionAttributes(String evictionAction,
      Integer maxMemory, Integer maxEntryCount,
      String objectSizer) {
    RegionAttributesType.EvictionAttributes configAttributes =
        new RegionAttributesType.EvictionAttributes();
    EnumActionDestroyOverflow action = EnumActionDestroyOverflow.fromValue(evictionAction);

    if (maxMemory == null && maxEntryCount == null) {
      RegionAttributesType.EvictionAttributes.LruHeapPercentage heapPercentage =
          new RegionAttributesType.EvictionAttributes.LruHeapPercentage();
      heapPercentage.setAction(action);
      heapPercentage.setClassName(objectSizer);
      configAttributes.setLruHeapPercentage(heapPercentage);
    } else if (maxMemory != null) {
      RegionAttributesType.EvictionAttributes.LruMemorySize memorySize =
          new RegionAttributesType.EvictionAttributes.LruMemorySize();
      memorySize.setAction(action);
      memorySize.setClassName(objectSizer);
      memorySize.setMaximum(maxMemory.toString());
      configAttributes.setLruMemorySize(memorySize);
    } else {
      RegionAttributesType.EvictionAttributes.LruEntryCount entryCount =
          new RegionAttributesType.EvictionAttributes.LruEntryCount();
      entryCount.setAction(action);
      entryCount.setMaximum(maxEntryCount.toString());
      configAttributes.setLruEntryCount(entryCount);
    }

    return configAttributes;
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
      attributesType.setCustomExpiry(new DeclarableType(expiry.getClassName()));
    }

    return attributesType;
  }

  private static ClassName<CustomExpiry> getClassName(CustomExpiry expiry) {
    if (expiry == null) {
      return null;
    }

    return new ClassName<>(expiry.getClass().getName());
  }

  private static String int2string(Integer x) {
    return Optional.ofNullable(x).map(v -> v.toString()).orElse(null);
  }

  private static String long2string(Long x) {
    return Optional.ofNullable(x).map(v -> v.toString()).orElse(null);
  }

  private String getLeafRegion(String fullPath) {
    String regionPath = fullPath;
    String[] regions = regionPath.split("/");

    return regions[regions.length - 1];
  }
}
