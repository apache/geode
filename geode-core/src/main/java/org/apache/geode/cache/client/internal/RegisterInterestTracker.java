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
package org.apache.geode.cache.client.internal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.InterestResultPolicy;
import org.apache.geode.cache.query.internal.cq.InternalCqQuery;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.sockets.UnregisterAllInterest;
import org.apache.geode.internal.logging.LogService;

/**
 * Used to keep track of what interest a client has registered. This code was extracted from the old
 * ConnectionProxyImpl.
 *
 * @since GemFire 5.7
 */
public class RegisterInterestTracker {
  private static final Logger logger = LogService.getLogger();

  public static final int interestListIndex = 0;
  public static final int durableInterestListIndex = 1;
  public static final int interestListIndexForUpdatesAsInvalidates = 2;
  public static final int durableInterestListIndexForUpdatesAsInvalidates = 3;

  private final FailoverInterestList[] fils = new FailoverInterestList[4];

  /** Manages CQs */
  private final ConcurrentMap cqs /* <CqQuery,Boolean> */ = new ConcurrentHashMap();

  public RegisterInterestTracker() {
    this.fils[interestListIndex] = new FailoverInterestList();
    this.fils[interestListIndexForUpdatesAsInvalidates] = new FailoverInterestList();
    this.fils[durableInterestListIndex] = new FailoverInterestList();
    this.fils[durableInterestListIndexForUpdatesAsInvalidates] = new FailoverInterestList();
  }

  public static int getInterestLookupIndex(boolean isDurable, boolean receiveUpdatesAsInvalidates) {
    if (isDurable) {
      if (receiveUpdatesAsInvalidates) {
        return durableInterestListIndexForUpdatesAsInvalidates;
      } else {
        return durableInterestListIndex;
      }
    } else {
      if (receiveUpdatesAsInvalidates) {
        return interestListIndexForUpdatesAsInvalidates;
      } else {
        return interestListIndex;
      }
    }
  }

  public List getInterestList(String regionName, int interestType) {
    RegionInterestEntry rie1 = readRegionInterests(regionName, interestType, false, false);
    RegionInterestEntry rie2 = readRegionInterests(regionName, interestType, false, true);
    RegionInterestEntry rie3 = readRegionInterests(regionName, interestType, true, false);
    RegionInterestEntry rie4 = readRegionInterests(regionName, interestType, true, true);

    ArrayList result = new ArrayList();

    if (rie1 != null) {
      result.addAll(rie1.getInterests().keySet());
    }

    if (rie2 != null) {
      result.addAll(rie2.getInterests().keySet());
    }

    if (rie3 != null) {
      result.addAll(rie3.getInterests().keySet());
    }

    if (rie4 != null) {
      result.addAll(rie4.getInterests().keySet());
    }

    return result;
  }

  public void addSingleInterest(LocalRegion r, Object key, int interestType,
      InterestResultPolicy pol, boolean isDurable, boolean receiveUpdatesAsInvalidates) {
    RegionInterestEntry rie =
        getRegionInterests(r, interestType, false, isDurable, receiveUpdatesAsInvalidates);
    rie.getInterests().put(key, pol);
  }

  public boolean removeSingleInterest(LocalRegion r, Object key, int interestType,
      boolean isDurable, boolean receiveUpdatesAsInvalidates) {
    RegionInterestEntry rie =
        getRegionInterests(r, interestType, true, isDurable, receiveUpdatesAsInvalidates);
    if (rie == null) {
      return false;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("removeSingleInterest region={} key={}", r.getFullPath(), key);
    }
    Object interest = rie.getInterests().remove(key);
    if (interest == null) {
      logger.warn("removeSingleInterest: key {} not registered in the client",
          key);
      return false;
    } else {
      return true;
    }
  }

  public void addInterestList(LocalRegion r, List keys, InterestResultPolicy pol, boolean isDurable,
      boolean receiveUpdatesAsInvalidates) {
    RegionInterestEntry rie =
        getRegionInterests(r, InterestType.KEY, false, isDurable, receiveUpdatesAsInvalidates);
    for (int i = 0; i < keys.size(); i++) {
      rie.getInterests().put(keys.get(i), pol);
    }
  }

  public void addCq(InternalCqQuery cqi, boolean isDurable) {
    this.cqs.put(cqi, Boolean.valueOf(isDurable));
  }

  public void removeCq(InternalCqQuery cqi, boolean isDurable) {
    this.cqs.remove(cqi);
  }

  public Map getCqsMap() {
    return this.cqs;
  }

  /**
   * Unregisters everything registered on the given region name
   */
  public void unregisterRegion(ServerRegionProxy srp, boolean keepalive) {
    removeAllInterests(srp, InterestType.KEY, false, keepalive, false);
    removeAllInterests(srp, InterestType.FILTER_CLASS, false, keepalive, false);
    removeAllInterests(srp, InterestType.OQL_QUERY, false, keepalive, false);
    removeAllInterests(srp, InterestType.REGULAR_EXPRESSION, false, keepalive, false);
    removeAllInterests(srp, InterestType.KEY, false, keepalive, true);
    removeAllInterests(srp, InterestType.FILTER_CLASS, false, keepalive, true);
    removeAllInterests(srp, InterestType.OQL_QUERY, false, keepalive, true);
    removeAllInterests(srp, InterestType.REGULAR_EXPRESSION, false, keepalive, true);
    // durable
    if (srp.getPool().isDurableClient()) {
      removeAllInterests(srp, InterestType.KEY, true, keepalive, true);
      removeAllInterests(srp, InterestType.FILTER_CLASS, true, keepalive, true);
      removeAllInterests(srp, InterestType.OQL_QUERY, true, keepalive, true);
      removeAllInterests(srp, InterestType.REGULAR_EXPRESSION, true, keepalive, true);
      removeAllInterests(srp, InterestType.KEY, true, keepalive, false);
      removeAllInterests(srp, InterestType.FILTER_CLASS, true, keepalive, false);
      removeAllInterests(srp, InterestType.OQL_QUERY, true, keepalive, false);
      removeAllInterests(srp, InterestType.REGULAR_EXPRESSION, true, keepalive, false);
    }
  }

  /**
   * Remove all interests of a given type on the given proxy's region.
   *
   * @param interestType the interest type
   * @param durable a boolean stating whether to remove durable or non-durable registrations
   */
  private void removeAllInterests(ServerRegionProxy srp, int interestType, boolean durable,
      boolean keepAlive, boolean receiveUpdatesAsInvalidates) {
    String regName = srp.getRegionName();
    ConcurrentMap allInterests =
        getRegionToInterestsMap(interestType, durable, receiveUpdatesAsInvalidates);
    if (allInterests.remove(regName) != null) {
      if (logger.isDebugEnabled()) {
        logger.debug("removeAllInterests region={} type={}", regName,
            InterestType.getString(interestType));
      }
      try {
        // fix bug 35680 by using a UnregisterAllInterest token
        Object key = UnregisterAllInterest.singleton();
        // we have already cleaned up the tracker so send the op directly
        UnregisterInterestOp.execute(srp.getPool(), regName, key, interestType, true/* isClosing */,
            keepAlive);
      } catch (Exception e) {
        if (!srp.getPool().getCancelCriterion().isCancelInProgress()) {
          logger.warn("Problem removing all interest on region={} interestType={} :{}",
              new Object[] {regName, InterestType.getString(interestType),
                  e.getLocalizedMessage()});
        }
      }
    }
  }

  public boolean removeInterestList(LocalRegion r, List keys, boolean isDurable,
      boolean receiveUpdatesAsInvalidates) {
    RegionInterestEntry rie =
        getRegionInterests(r, InterestType.KEY, true, isDurable, receiveUpdatesAsInvalidates);
    if (rie == null) {
      return false;
    }
    if (logger.isDebugEnabled()) {
      logger.debug("removeInterestList region={} keys={}", r.getFullPath(), keys);
    }
    int removeCount = 0;
    for (int i = 0; i < keys.size(); i++) {
      Object key = keys.get(i);
      Object interest = rie.getInterests().remove(key);
      if (interest != null) {
        removeCount++;
      } else {
        logger.warn("removeInterestList: key {} not registered in the client",
            key);
      }
    }
    return removeCount != 0;
  }

  /**
   * Return keys of interest for a given region. The keys in this Map are the full names of the
   * regions. The values are instances of RegionInterestEntry.
   *
   * @param interestType the type to return
   * @return the map
   */
  public ConcurrentMap getRegionToInterestsMap(int interestType, boolean isDurable,
      boolean receiveUpdatesAsInvalidates) {
    FailoverInterestList fil =
        this.fils[getInterestLookupIndex(isDurable, receiveUpdatesAsInvalidates)];

    ConcurrentMap mapOfInterest = null;

    switch (interestType) {
      case InterestType.KEY:
        mapOfInterest = fil.keysOfInterest;
        break;
      case InterestType.REGULAR_EXPRESSION:
        mapOfInterest = fil.regexOfInterest;
        break;
      case InterestType.FILTER_CLASS:
        mapOfInterest = fil.filtersOfInterest;
        break;
      case InterestType.CQ:
        mapOfInterest = fil.cqsOfInterest;
        break;
      case InterestType.OQL_QUERY:
        mapOfInterest = fil.queriesOfInterest;
        break;
      default:
        throw new InternalGemFireError("Unknown interestType");
    }
    return mapOfInterest;
  }

  /**
   * Return the RegionInterestEntry for the given region. Create one if none exists and forRemoval
   * is false.
   *
   * @param r specified region
   * @param interestType desired interest type
   * @param forRemoval true if calls wants one for removal
   * @return the entry or null if none exists and forRemoval is true.
   */
  private RegionInterestEntry getRegionInterests(LocalRegion r, int interestType,
      boolean forRemoval, boolean isDurable, boolean receiveUpdatesAsInvalidates) {
    final String regionName = r.getFullPath();
    ConcurrentMap mapOfInterest =
        getRegionToInterestsMap(interestType, isDurable, receiveUpdatesAsInvalidates);
    RegionInterestEntry result = (RegionInterestEntry) mapOfInterest.get(regionName);
    if (result == null && !forRemoval) {
      RegionInterestEntry rie = new RegionInterestEntry(r);
      result = (RegionInterestEntry) mapOfInterest.putIfAbsent(regionName, rie);
      if (result == null) {
        result = rie;
      }
    }
    return result;
  }

  private RegionInterestEntry readRegionInterests(String regionName, int interestType,
      boolean isDurable, boolean receiveUpdatesAsInvalidates) {
    ConcurrentMap mapOfInterest =
        getRegionToInterestsMap(interestType, isDurable, receiveUpdatesAsInvalidates);
    return (RegionInterestEntry) mapOfInterest.get(regionName);
  }

  /**
   * A Holder object for client's register interest, this is required when a client fails over to
   * another server and does register interest based on this Data structure
   *
   * @since GemFire 5.5
   *
   */
  protected static class FailoverInterestList {
    /**
     * Record of enumerated keys of interest.
     *
     * This list is maintained here in case an endpoint (server) bounces. In that case, a message
     * will be sent to the endpoint as soon as it has restarted.
     *
     * The keys in this Map are the full names of the regions. The values are instances of
     * {@link RegionInterestEntry}.
     */
    final ConcurrentMap keysOfInterest = new ConcurrentHashMap();

    /**
     * Record of regular expression keys of interest.
     *
     * This list is maintained here in case an endpoint (server) bounces. In that case, a message
     * will be sent to the endpoint as soon as it has restarted.
     *
     * The keys in this Map are the full names of the regions. The values are instances of
     * {@link RegionInterestEntry}.
     */
    final ConcurrentMap regexOfInterest = new ConcurrentHashMap();

    /**
     * Record of filtered keys of interest.
     *
     * This list is maintained here in case an endpoint (server) bounces. In that case, a message
     * will be sent to the endpoint as soon as it has restarted.
     *
     * The keys in this Map are the full names of the regions. The values are instances of
     * {@link RegionInterestEntry}.
     */
    final ConcurrentMap filtersOfInterest = new ConcurrentHashMap();

    /**
     * Record of QOL keys of interest.
     *
     * This list is maintained here in case an endpoint (server) bounces. In that case, a message
     * will be sent to the endpoint as soon as it has restarted.
     *
     * The keys in this Map are the full names of the regions. The values are instances of
     * {@link RegionInterestEntry}.
     */
    final ConcurrentMap queriesOfInterest = new ConcurrentHashMap();

    /**
     * Record of registered CQs
     *
     */
    final ConcurrentMap cqsOfInterest = new ConcurrentHashMap();
  }

  /**
   * Description of the interests of a particular region.
   *
   *
   */
  public static class RegionInterestEntry {
    private final LocalRegion region;

    private final ConcurrentMap interests;

    RegionInterestEntry(LocalRegion r) {
      this.region = r;
      this.interests = new ConcurrentHashMap();
    }

    public LocalRegion getRegion() {
      return this.region;
    }

    public ConcurrentMap getInterests() {
      return this.interests;
    }
  }
}
