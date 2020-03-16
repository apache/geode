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
import java.util.function.Predicate;

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
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.CliFunction;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.configuration.domain.DeclarableTypeInstantiator;
import org.apache.geode.management.internal.functions.CliFunctionResult;

/**
 * Function used by the 'alter region' gfsh command to alter a region on each member.
 *
 * @since GemFire 8.0
 */
public class RegionAlterFunction extends CliFunction<RegionConfig> {
  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = -4846425364943216425L;

  @Override
  public boolean isHA() {
    return false;
  }

  @Override
  public String getId() {
    return RegionAlterFunction.class.getName();
  }

  @Override
  public CliFunctionResult executeFunction(FunctionContext<RegionConfig> context) {
    Cache cache = ((InternalCache) context.getCache()).getCacheForProcessingClientRequests();
    RegionConfig deltaConfig = context.getArguments();
    alterRegion(cache, deltaConfig);

    return new CliFunctionResult(context.getMemberName(), Result.Status.OK,
        String.format("Region %s altered", deltaConfig.getName()));
  }

  private static <T> Predicate<T> not(Predicate<T> t) {
    return t.negate();
  }

  void alterRegion(Cache cache, RegionConfig deltaConfig) {
    final String regionPathString = deltaConfig.getName();

    AbstractRegion region = (AbstractRegion) cache.getRegion(regionPathString);
    if (region == null) {
      throw new IllegalArgumentException(
          String.format("Region does not exist: %s", regionPathString));
    }

    @SuppressWarnings("unchecked")
    AttributesMutator<Object, Object> mutator = region.getAttributesMutator();
    RegionAttributesType regionAttributes = deltaConfig.getRegionAttributes();

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
        region.getEntryIdleTimeout(), mutator::setEntryIdleTimeout,
        mutator::setCustomEntryIdleTimeout);
    updateExpirationAttributes(cache, regionAttributes.getEntryTimeToLive(),
        region.getEntryTimeToLive(), mutator::setEntryTimeToLive,
        mutator::setCustomEntryTimeToLive);
    updateExpirationAttributes(cache, regionAttributes.getRegionIdleTime(),
        region.getRegionIdleTimeout(), mutator::setRegionIdleTimeout, null);
    updateExpirationAttributes(cache, regionAttributes.getRegionTimeToLive(),
        region.getRegionTimeToLive(), mutator::setRegionTimeToLive, null);

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
        validateParallelAsynchronousEventQueueIDs((PartitionedRegion) region,
            newAsyncEventQueueIds);
        senderIds.addAll(newAsyncEventQueueIds);
      } else if (region.getAsyncEventQueueIds() != null) {
        senderIds.addAll(region.getAsyncEventQueueIds());
      }

      ((PartitionedRegion) region)
          .updatePRConfigWithNewSetOfAsynchronousEventDispatchers(senderIds);
    }

    // Alter Gateway Sender Ids
    if (newGatewaySenderIds != null) {
      Set<String> oldGatewaySenderIds = region.getGatewaySenderIds();
      // Remove old gateway sender ids that aren't in the new list
      oldGatewaySenderIds.stream().filter(not(newGatewaySenderIds::contains))
          .forEach(mutator::removeGatewaySenderId);

      // Add new gateway sender ids that don't already exist
      newGatewaySenderIds.stream().filter(not(oldGatewaySenderIds::contains))
          .forEach(mutator::addGatewaySenderId);

      if (logger.isDebugEnabled()) {
        logger.debug("Region successfully altered - gateway sender IDs");
      }
    }

    // Alter Async Queue Ids
    if (newAsyncEventQueueIds != null) {
      Set<String> oldAsyncEventQueueIds = region.getAsyncEventQueueIds();

      // Remove old async event queue ids that aren't in the new list
      oldAsyncEventQueueIds.stream().filter(not(newAsyncEventQueueIds::contains))
          .forEach(mutator::removeAsyncEventQueueId);

      // Add new async event queue ids that don't already exist
      newAsyncEventQueueIds.stream().filter(not(oldAsyncEventQueueIds::contains))
          .forEach(mutator::addAsyncEventQueueId);

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
      @SuppressWarnings("unchecked")
      CacheListener<Object, Object>[] oldCacheListeners = region.getCacheListeners();
      for (CacheListener<Object, Object> oldCacheListener : oldCacheListeners) {
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
      ExpirationAttributes existingAttributes, Consumer<ExpirationAttributes> mutator1,
      Consumer<CustomExpiry<Object, Object>> mutator2) {
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
      region.validateParallelAsynchronousEventDispatcherIds(parallelSenders);
    } catch (PRLocallyDestroyedException e) {
      throw new IllegalStateException("Partitioned Region not found registered", e);
    }
  }

  private void validateParallelAsynchronousEventQueueIDs(PartitionedRegion region,
      Set<String> newAsyncEventQueueIds) {
    try {
      Set<String> parallelSenders =
          region.filterOutNonParallelAsyncEventQueues(newAsyncEventQueueIds);
      region.validateParallelAsynchronousEventDispatcherIds(parallelSenders);
    } catch (PRLocallyDestroyedException e) {
      throw new IllegalStateException("Partitioned Region not found registered", e);
    }
  }
}
