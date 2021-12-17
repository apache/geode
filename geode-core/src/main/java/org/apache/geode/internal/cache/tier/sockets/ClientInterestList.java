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
package org.apache.geode.internal.cache.tier.sockets;

import static org.apache.commons.lang3.ObjectUtils.isEmpty;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import org.apache.geode.CancelException;
import org.apache.geode.cache.InterestRegistrationEvent;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.internal.cache.CacheDistributionAdvisee;
import org.apache.geode.internal.cache.FilterProfile;
import org.apache.geode.internal.cache.InterestRegistrationEventImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Class <code>ClientInterestList</code> provides a convenient interface for manipulating client
 * interest information.
 */
class ClientInterestList {
  private static final Logger logger = LogService.getLogger();

  final CacheClientProxy cacheClientProxy;

  final Object id;

  /**
   * An object used for synchronizing the interest lists
   */
  private final Object interestListLock = new Object();

  /**
   * Regions that this client is interested in
   */
  protected final Set<String> regions = new HashSet<>();

  /**
   * Constructor.
   */
  protected ClientInterestList(final @NotNull CacheClientProxy cacheClientProxy,
      final @NotNull Object id) {
    this.cacheClientProxy = cacheClientProxy;
    this.id = id;
  }

  /**
   * Registers interest in the input region name and key
   */
  protected void registerClientInterest(final @NotNull String regionName,
      final @NotNull Object keyOfInterest, final @NotNull InterestType interestType,
      final boolean sendUpdatesAsInvalidates) {
    if (logger.isDebugEnabled()) {
      logger.debug("{}: registerClientInterest region={} key={}", cacheClientProxy, regionName,
          keyOfInterest);
    }
    final Set<?> keysRegistered;
    synchronized (interestListLock) {
      LocalRegion r = (LocalRegion) cacheClientProxy._cache.getRegion(regionName, true);
      if (r == null) {
        throw new RegionDestroyedException("Region could not be found for interest registration",
            regionName);
      }
      if (!(r instanceof CacheDistributionAdvisee)) {
        throw new IllegalArgumentException("region " + regionName
            + " is not distributed and does not support interest registration");
      }
      FilterProfile p = r.getFilterProfile();
      keysRegistered =
          p.registerClientInterest(id, keyOfInterest, interestType, sendUpdatesAsInvalidates);
      regions.add(regionName);
    }
    // Perform actions if any keys were registered
    if ((keysRegistered != null) && containsInterestRegistrationListeners()
        && !keysRegistered.isEmpty()) {
      handleInterestEvent(regionName, keysRegistered, interestType, true);
    }
  }


  protected FilterProfile getProfile(String regionName) {
    try {
      return cacheClientProxy._cache.getFilterProfile(regionName);
    } catch (CancelException e) {
      return null;
    }
  }

  /**
   * Unregisters interest in the input region name and key
   *
   * @param regionName The fully-qualified name of the region in which to unregister interest
   * @param keyOfInterest The key in which to unregister interest
   */
  protected void unregisterClientInterest(String regionName, Object keyOfInterest,
      final @NotNull InterestType interestType) {
    if (logger.isDebugEnabled()) {
      logger.debug("{}: unregisterClientInterest region={} key={}", cacheClientProxy, regionName,
          keyOfInterest);
    }
    FilterProfile p = getProfile(regionName);
    Set<?> keysUnregistered = null;
    synchronized (interestListLock) {
      if (p != null) {
        keysUnregistered = p.unregisterClientInterest(id, keyOfInterest, interestType);
        if (!p.hasInterestFor(id)) {
          regions.remove(regionName);
        }
      } else {
        regions.remove(regionName);
      }
    }
    if (keysUnregistered != null && !keysUnregistered.isEmpty()) {
      handleInterestEvent(regionName, keysUnregistered, interestType, false);
    }
  }

  /**
   * Registers interest in the input region name and list of keys
   *
   * @param regionName The fully-qualified name of the region in which to register interest
   * @param keysOfInterest The list of keys in which to register interest
   */
  protected void registerClientInterestList(final @NotNull String regionName,
      final @NotNull List<?> keysOfInterest,
      final boolean sendUpdatesAsInvalidates) {
    final FilterProfile p = getProfile(regionName);
    if (p == null) {
      throw new RegionDestroyedException("Region not found during client interest registration",
          regionName);
    }
    final Set<?> keysRegistered;
    synchronized (interestListLock) {
      keysRegistered = p.registerClientInterestList(id, keysOfInterest, sendUpdatesAsInvalidates);
      regions.add(regionName);
    }
    // Perform actions if any keys were registered
    if (containsInterestRegistrationListeners() && !keysRegistered.isEmpty()) {
      handleInterestEvent(regionName, keysRegistered, InterestType.KEY, true);
    }
  }

  /**
   * Unregisters interest in the input region name and list of keys
   *
   * @param regionName The fully-qualified name of the region in which to unregister interest
   * @param keysOfInterest The list of keys in which to unregister interest
   */
  protected void unregisterClientInterestList(final @NotNull String regionName,
      final @NotNull List<?> keysOfInterest) {
    final FilterProfile p = getProfile(regionName);
    Set<?> keysUnregistered = null;
    synchronized (interestListLock) {
      if (p != null) {
        keysUnregistered = p.unregisterClientInterestList(id, keysOfInterest);
        if (!p.hasInterestFor(id)) {
          regions.remove(regionName);
        }
      } else {
        regions.remove(regionName);
      }
    }
    // Perform actions if any keys were unregistered
    if (!isEmpty(keysUnregistered)) {
      handleInterestEvent(regionName, keysUnregistered, InterestType.KEY, false);
    }
  }

  /*
   * Returns whether this interest list has any keys, patterns or filters of interest. It answers
   * the question: Are any clients being notified because of this interest list? @return whether
   * this interest list has any keys, patterns or filters of interest
   */
  protected boolean hasInterest() {
    return regions.size() > 0;
  }

  protected void clearClientInterestList() {
    boolean isClosed = cacheClientProxy.getCache().isClosed();

    synchronized (interestListLock) {
      for (String regionName : regions) {
        FilterProfile p = getProfile(regionName);
        if (p == null) {
          continue;
        }
        if (!isClosed) {
          if (p.hasAllKeysInterestFor(id)) {
            final Set<String> allKeys = Collections.singleton(".*");
            handleInterestEvent(regionName, allKeys, InterestType.REGULAR_EXPRESSION, false);
          }
          Set<?> keysOfInterest = p.getKeysOfInterestFor(id);
          if (keysOfInterest != null && keysOfInterest.size() > 0) {
            handleInterestEvent(regionName, keysOfInterest, InterestType.KEY, false);
          }
          Map<String, Pattern> patternsOfInterest = p.getPatternsOfInterestFor(id);
          if (patternsOfInterest != null && patternsOfInterest.size() > 0) {
            handleInterestEvent(regionName, patternsOfInterest.keySet(),
                InterestType.REGULAR_EXPRESSION, false);
          }
        }
        p.clearInterestFor(id);
      }
      regions.clear();
    }
  }


  private void handleInterestEvent(@NotNull String regionName, @NotNull Set<?> keysOfInterest,
      final @NotNull InterestType interestType,
      boolean isRegister) {
    // Notify the region about this register interest event if:
    // - the application has requested it
    // - this is a primary CacheClientProxy (otherwise multiple notifications
    // may occur)
    // - it is a key interest type (regex is currently not supported)
    InterestRegistrationEvent event = null;
    if (CacheClientProxy.NOTIFY_REGION_ON_INTEREST && cacheClientProxy.isPrimary()
        && interestType == InterestType.KEY) {
      event = new InterestRegistrationEventImpl(cacheClientProxy, regionName, keysOfInterest,
          interestType, isRegister);
      try {
        notifyRegionOfInterest(event);
      } catch (Exception e) {
        logger.warn("Region notification of interest failed", e);
      }
    }
    // Invoke interest registration listeners
    if (containsInterestRegistrationListeners()) {
      if (event == null) {
        event = new InterestRegistrationEventImpl(cacheClientProxy, regionName, keysOfInterest,
            interestType, isRegister);
      }
      notifyInterestRegistrationListeners(event);
    }
  }

  private void notifyRegionOfInterest(InterestRegistrationEvent event) {
    cacheClientProxy.getCacheClientNotifier().handleInterestEvent(event);
  }

  private void notifyInterestRegistrationListeners(InterestRegistrationEvent event) {
    cacheClientProxy.getCacheClientNotifier().notifyInterestRegistrationListeners(event);
  }

  private boolean containsInterestRegistrationListeners() {
    return cacheClientProxy.getCacheClientNotifier().containsInterestRegistrationListeners();
  }
}
