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
package org.apache.geode.management.internal.cli.functions;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.AttributesMutator;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.ExpirationAction;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.partitioned.PRLocallyDestroyedException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.configuration.domain.DeclarableTypeInstantiator;

/**
 * Function used by the 'alter region' gfsh command to alter a region on each member.
 *
 * @since GemFire 8.0
 */
public class RegionAlterFunction extends CliFunction<RegionConfig> {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = -4846425364943216425L;
  private static final String NULLSTR = "null";

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<RegionConfig> context) {
    Cache cache = ((InternalCache) context.getCache()).getCacheForProcessingClientRequests();
    RegionConfig deltaConfig = context.getArguments();
    alterRegion(cache, deltaConfig);
    return new CliFunctionResult(context.getMemberName(), Result.Status.OK,
        String.format("Region %s altered", deltaConfig.getName()));
  }

  void alterRegion(Cache cache, RegionConfig deltaConfig) {
    final String regionPathString = deltaConfig.getName();

    AbstractRegion region = (AbstractRegion) cache.getRegion(regionPathString);
    if (region == null) {
      throw new IllegalArgumentException(String.format(
          "Region does not exist: %s", regionPathString));
    }

    RegionAttributesType regionAttributes = deltaConfig.getRegionAttributes();
    AttributesMutator mutator = region.getAttributesMutator();

    if (regionAttributes.isCloningEnabled() != null) {
      mutator.setCloningEnabled(regionAttributes.isCloningEnabled());
      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - cloning");
      }
    }

    if (regionAttributes.getEvictionAttributes() != null) {
      mutator.getEvictionAttributesMutator().setMaximum(Integer
          .parseInt(regionAttributes.getEvictionAttributes().getLruEntryCount().getMaximum()));
      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - eviction attributes max");
      }
    }

    // Alter expiration attributes
    updateExpirationAttributes(cache, regionAttributes.getEntryIdleTime(),
        region.getEntryIdleTimeout(), p -> mutator.setEntryIdleTimeout(p),
        p -> mutator.setCustomEntryIdleTimeout(p));
    updateExpirationAttributes(cache, regionAttributes.getEntryTimeToLive(),
        region.getEntryTimeToLive(), p -> mutator.setEntryTimeToLive(p),
        p -> mutator.setCustomEntryTimeToLive(p));
    updateExpirationAttributes(cache, regionAttributes.getRegionIdleTime(),
        region.getRegionIdleTimeout(), p -> mutator.setRegionIdleTimeout(p), null);
    updateExpirationAttributes(cache, regionAttributes.getRegionTimeToLive(),
        region.getRegionTimeToLive(), p -> mutator.setRegionTimeToLive(p), null);


    final Set<String> newGatewaySenderIds = regionAttributes.getGatewaySenderIdsAsSet();
    final Set<String> newAsyncEventQueueIds = regionAttributes.getAsyncEventQueueIdsAsSet();

    if (region instanceof PartitionedRegion) {
      Set<String> senderIds = new HashSet<>();
      if (newGatewaySenderIds != null) {
        validateParallelGatewaySenderIDs((PartitionedRegion) region, newGatewaySenderIds);
        senderIds.addAll(newGatewaySenderIds);
      } else if (region.getGatewaySenderIds() != null) {
        senderIds.addAll(region.getAllGatewaySenderIds());
      }
      if (newAsyncEventQueueIds != null) {
        validateParallelGatewaySenderIDs((PartitionedRegion) region, newAsyncEventQueueIds);
        senderIds.addAll(newAsyncEventQueueIds);
      } else if (region.getAsyncEventQueueIds() != null) {
        senderIds.addAll(region.getAsyncEventQueueIds());
      }
      ((PartitionedRegion) region).updatePRConfigWithNewSetOfGatewaySenders(senderIds);
    }

    // Alter Gateway Sender Ids
    if (newGatewaySenderIds != null) {
      // Remove old gateway sender ids that aren't in the new list
      Set<String> oldGatewaySenderIds = region.getGatewaySenderIds();
      if (!oldGatewaySenderIds.isEmpty()) {
        for (String gatewaySenderId : oldGatewaySenderIds) {
          if (!newGatewaySenderIds.contains(gatewaySenderId)) {
            mutator.removeGatewaySenderId(gatewaySenderId);
          }
        }
      }

      // Add new gateway sender ids that don't already exist
      for (String gatewaySenderId : newGatewaySenderIds) {
        if (!oldGatewaySenderIds.contains(gatewaySenderId)) {
          mutator.addGatewaySenderId(gatewaySenderId);
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - gateway sender IDs");
      }
    }

    // Alter Async Queue Ids
    if (newAsyncEventQueueIds != null) {

      // Remove old async event queue ids that aren't in the new list
      Set<String> oldAsyncEventQueueIds = region.getAsyncEventQueueIds();
      if (!oldAsyncEventQueueIds.isEmpty()) {
        for (String asyncEventQueueId : oldAsyncEventQueueIds) {
          if (!newAsyncEventQueueIds.contains(asyncEventQueueId)) {
            mutator.removeAsyncEventQueueId(asyncEventQueueId);
          }
        }
      }

      // Add new async event queue ids that don't already exist
      for (String asyncEventQueueId : newAsyncEventQueueIds) {
        if (!oldAsyncEventQueueIds.contains(asyncEventQueueId)) {
          mutator.addAsyncEventQueueId(asyncEventQueueId);
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - async event queue IDs");
      }
    }

    // Alter Cache Listeners
    final List<DeclarableType> newCacheListeners = regionAttributes.getCacheListeners();

    // user specified a new set of cache listeners
    if (!newCacheListeners.isEmpty()) {
      // remove the old ones, even if the new set includes the same class name, the init properties
      // might be different
      CacheListener[] oldCacheListeners = region.getCacheListeners();
      for (CacheListener oldCacheListener : oldCacheListeners) {
        mutator.removeCacheListener(oldCacheListener);
      }

      // Add new cache listeners
      for (DeclarableType newCacheListener : newCacheListeners) {
        if (!newCacheListener.equals(DeclarableType.EMPTY)) {
          mutator.addCacheListener(DeclarableTypeInstantiator.newInstance(newCacheListener, cache));
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - cache listeners");
      }
    }

    final DeclarableType cacheLoader = regionAttributes.getCacheLoader();
    if (cacheLoader != null) {
      if (cacheLoader.equals(DeclarableType.EMPTY)) {
        mutator.setCacheLoader(null);
      } else {
        mutator.setCacheLoader(DeclarableTypeInstantiator.newInstance(cacheLoader, cache));
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - cache loader");
      }
    }

    final DeclarableType cacheWriter = regionAttributes.getCacheWriter();
    if (cacheWriter != null) {
      if (cacheWriter.equals(DeclarableType.EMPTY)) {
        mutator.setCacheWriter(null);
      } else {
        mutator.setCacheWriter(DeclarableTypeInstantiator.newInstance(cacheWriter, cache));
      }

      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - cache writer");
      }
    }
  }

  private void updateExpirationAttributes(Cache cache,
      RegionAttributesType.ExpirationAttributesType newAttributes,
      ExpirationAttributes existingAttributes,
      Consumer<ExpirationAttributes> mutator1,
      Consumer<CustomExpiry> mutator2) {
    if (newAttributes == null) {
      return;
    }

    if (newAttributes.hasTimoutOrAction() && existingAttributes != null) {
      int existingTimeout = existingAttributes.getTimeout();
      ExpirationAction existingAction = existingAttributes.getAction();
      if (newAttributes.getTimeout() != null) {
        existingTimeout = Integer.parseInt(newAttributes.getTimeout());
      }

      if (newAttributes.getAction() != null) {
        existingAction = ExpirationAction.fromXmlString(newAttributes.getAction());
      }
      mutator1.accept(new ExpirationAttributes(existingTimeout, existingAction));
    }

    if (mutator2 == null) {
      return;
    }

    if (newAttributes.hasCustomExpiry()) {
      DeclarableType newCustomExpiry = newAttributes.getCustomExpiry();
      if (newCustomExpiry.equals(DeclarableType.EMPTY)) {
        mutator2.accept(null);
      } else {
        mutator2.accept(DeclarableTypeInstantiator.newInstance(newCustomExpiry, cache));
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Region successfully altered - entry idle timeout");
    }
  }


  private void validateParallelGatewaySenderIDs(PartitionedRegion region,
      Set<String> newGatewaySenderIds) {
    try {
      Set<String> parallelSenders = region.filterOutNonParallelGatewaySenders(newGatewaySenderIds);
      region.validateParallelGatewaySenderIds(parallelSenders);
    } catch (PRLocallyDestroyedException e) {
      throw new IllegalStateException("Partitioned Region not found registered", e);
    }
  }

  @Override
  public String getId() {
    return RegionAlterFunction.class.getName();
  }
}
