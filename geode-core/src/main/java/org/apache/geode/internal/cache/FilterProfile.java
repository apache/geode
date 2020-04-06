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
package org.apache.geode.internal.cache;

import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.ANY_INIT;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SystemFailure;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.SerializedCacheValue;
import org.apache.geode.cache.query.internal.CqStateImpl;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.CqServiceProvider;
import org.apache.geode.cache.query.internal.cq.ServerCQ;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.ClassLoadUtil;
import org.apache.geode.internal.CopyOnWriteHashSet;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.CacheDistributionAdvisor.CacheProfile;
import org.apache.geode.internal.cache.DistributedPutAllOperation.PutAllEntryData;
import org.apache.geode.internal.cache.DistributedRemoveAllOperation.RemoveAllEntryData;
import org.apache.geode.internal.cache.FilterRoutingInfo.FilterInfo;
import org.apache.geode.internal.cache.LocalRegion.InitializationLevel;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.UnregisterAllInterest;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.util.concurrent.CopyOnWriteHashMap;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * FilterProfile represents a distributed system member and is used for two purposes: processing
 * client-bound events, and providing information for profile exchanges.
 *
 * FilterProfiles represent client IDs, including durable Queue IDs, with long integers. This
 * reduces the size of routing information when sent over the network.
 *
 * @since GemFire 6.5
 */
public class FilterProfile implements DataSerializableFixedID {
  private static final Logger logger = LogService.getLogger();

  /** enumeration of distributed profile operations */
  enum operationType {
    REGISTER_KEY,
    REGISTER_KEYS,
    REGISTER_PATTERN,
    REGISTER_FILTER,
    UNREGISTER_KEY,
    UNREGISTER_KEYS,
    UNREGISTER_PATTERN,
    UNREGISTER_FILTER,
    CLEAR,
    HAS_CQ,
    REGISTER_CQ,
    CLOSE_CQ,
    STOP_CQ,
    SET_CQ_STATE
  }

  /**
   * these booleans tell whether the associated operationType pertains to CQs or not
   */
  @Immutable
  private static final boolean[] isCQOperation =
      {false, false, false, false, false, false, false, false, false, true, true, true, true, true};

  /**
   * types of interest a client may have<br>
   * NONE - not interested<br>
   * UPDATES - wants update messages<br>
   * INVALIDATES - wants invalidations instead of updates
   */
  public static enum interestType {
    NONE, UPDATES, INVALIDATES
  }

  /**
   * The keys in which clients are interested. This is a map keyed on client id, with a HashSet of
   * the interested keys as the values.
   */
  private final CopyOnWriteHashMap<Object, Set> keysOfInterest = new CopyOnWriteHashMap<>();

  private final CopyOnWriteHashMap<Object, Set> keysOfInterestInv = new CopyOnWriteHashMap<>();

  /**
   * The patterns in which clients are interested. This is a map keyed on client id, with a HashMap
   * (key name to compiled pattern) as the values.
   */
  private final CopyOnWriteHashMap<Object, Map<Object, Pattern>> patternsOfInterest =
      new CopyOnWriteHashMap<>();

  private final CopyOnWriteHashMap<Object, Map<Object, Pattern>> patternsOfInterestInv =
      new CopyOnWriteHashMap<>();

  /**
   * The filtering classes in which clients are interested. This is a map keyed on client id, with a
   * HashMap (key name to {@link InterestFilter}) as the values.
   */
  private final CopyOnWriteHashMap<Object, Map> filtersOfInterest = new CopyOnWriteHashMap<>();

  private final CopyOnWriteHashMap<Object, Map> filtersOfInterestInv = new CopyOnWriteHashMap<>();

  /**
   * Set of clients that we have ALL_KEYS interest for and who want updates
   */
  private final CopyOnWriteHashSet<Long> allKeyClients = new CopyOnWriteHashSet<>();

  /**
   * Set of clients that we have ALL_KEYS interest for and who want invalidations
   */
  private final CopyOnWriteHashSet<Long> allKeyClientsInv = new CopyOnWriteHashSet<>();

  /**
   * The region associated with this profile
   */
  transient LocalRegion region;

  /**
   * Whether this is a local profile or one that describes another process
   */
  private transient boolean isLocalProfile;

  /** Currently installed CQ count on the region */
  AtomicInteger cqCount;

  /** CQs that are registered on the remote node **/
  private final CopyOnWriteHashMap<String, ServerCQ> cqs = new CopyOnWriteHashMap<>();

  /* the ID of the member that this profile describes */
  private DistributedMember memberID;

  /**
   * since client identifiers can be long, we use a mapping for on-wire operations
   */
  IDMap clientMap;

  /**
   * since CQ identifiers can be long, we use a mapping for on-wire operations
   */
  IDMap cqMap;

  private final Object interestListLock = new Object();

  /**
   * Queues the Filter Profile messages that are received during profile exchange.
   */
  private volatile Map<InternalDistributedMember, LinkedList<OperationMessage>> filterProfileMsgQueue =
      new HashMap<>();

  public FilterProfile() {
    cqCount = new AtomicInteger();
  }

  /**
   * used for instantiation of a profile associated with a region and not describing region filters
   * in a different process. Do not use this method when instantiating profiles to store in
   * distribution advisor profiles.
   */
  public FilterProfile(LocalRegion r) {
    this(r, r.getMyId(), r.getGemFireCache().getCacheServers().size() > 0);
  }

  /**
   * used for instantiation of a profile associated with a region and not describing region filters
   * in a different process. Do not use this method when instantiating profiles to store in
   * distribution advisor profiles.
   */
  public FilterProfile(LocalRegion r, DistributedMember member, boolean hasCacheServer) {
    this.region = r;
    this.isLocalProfile = true;
    this.memberID = member;
    this.cqCount = new AtomicInteger();
    this.clientMap = new IDMap();
    this.cqMap = new IDMap();
    this.localProfile.hasCacheServer = hasCacheServer;
  }

  public static boolean isCqOp(operationType opType) {
    return isCQOperation[opType.ordinal()];
  }

  /** return a set containing all clients that have registered interest for values */
  private Set getAllClientsWithInterest() {
    Set clientsWithInterest = new HashSet(getAllKeyClients());
    clientsWithInterest.addAll(getPatternsOfInterest().keySet());
    clientsWithInterest.addAll(getFiltersOfInterest().keySet());
    clientsWithInterest.addAll(getKeysOfInterest().keySet());
    return clientsWithInterest;
  }

  /** return a set containing all clients that have registered interest for invalidations */
  private Set getAllClientsWithInterestInv() {
    Set clientsWithInterestInv = new HashSet(getAllKeyClientsInv());
    clientsWithInterestInv.addAll(getPatternsOfInterestInv().keySet());
    clientsWithInterestInv.addAll(getFiltersOfInterestInv().keySet());
    clientsWithInterestInv.addAll(getKeysOfInterestInv().keySet());
    return clientsWithInterestInv;
  }

  /**
   * Registers interest in the input region name and key
   *
   * @param inputClientID The identity of the interested client
   * @param interest The key in which to register interest
   * @param typeOfInterest the type of interest the client is registering
   * @param updatesAsInvalidates whether the client just wants invalidations
   * @return a set of the keys that were registered, which may be null
   */
  public Set registerClientInterest(Object inputClientID, Object interest, int typeOfInterest,
      boolean updatesAsInvalidates) {
    Set keysRegistered = new HashSet();
    operationType opType = null;

    Long clientID = getClientIDForMaps(inputClientID);
    synchronized (this.interestListLock) {
      switch (typeOfInterest) {
        case InterestType.KEY:
          opType = operationType.REGISTER_KEY;
          Map<Object, Set> koi =
              updatesAsInvalidates ? getKeysOfInterestInv() : getKeysOfInterest();
          registerKeyInMap(interest, keysRegistered, clientID, koi);
          break;
        case InterestType.REGULAR_EXPRESSION:
          opType = operationType.REGISTER_PATTERN;
          if (((String) interest).equals(".*")) {
            Set akc = updatesAsInvalidates ? getAllKeyClientsInv() : getAllKeyClients();
            if (akc.add(clientID)) {
              keysRegistered.add(interest);
            }
          } else {
            Map<Object, Map<Object, Pattern>> pats =
                updatesAsInvalidates ? getPatternsOfInterestInv() : getPatternsOfInterest();
            registerPatternInMap(interest, keysRegistered, clientID, pats);
          }
          break;
        case InterestType.FILTER_CLASS: {
          opType = operationType.REGISTER_FILTER;
          Map<Object, Map> filts =
              updatesAsInvalidates ? getFiltersOfInterestInv() : getFiltersOfInterest();
          registerFilterClassInMap(interest, clientID, filts);
          break;
        }

        default:
          throw new InternalGemFireError(
              "Unknown interest type");
      } // switch
      if (this.isLocalProfile && opType != null) {
        sendProfileOperation(clientID, opType, interest, updatesAsInvalidates);
      }
    } // synchronized
    return keysRegistered;
  }

  private void registerFilterClassInMap(Object interest, Long clientID, Map<Object, Map> filts) {
    // get instance of the filter
    Class filterClass;
    InterestFilter filter;
    try {
      filterClass = ClassLoadUtil.classFromName((String) interest);
      filter = (InterestFilter) filterClass.newInstance();
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException(String.format("Class %s not found in classpath.",
          interest), cnfe);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Class %s could not be instantiated.",
          interest), e);
    }
    Map interestMap = filts.get(clientID);
    if (interestMap == null) {
      interestMap = new CopyOnWriteHashMap();
      filts.put(clientID, interestMap);
    }
    interestMap.put(interest, filter);
  }

  private void registerPatternInMap(Object interest, Set keysRegistered, Long clientID,
      Map<Object, Map<Object, Pattern>> pats) {
    Pattern pattern = Pattern.compile((String) interest);
    Map<Object, Pattern> interestMap = pats.get(clientID);
    if (interestMap == null) {
      interestMap = new CopyOnWriteHashMap<Object, Pattern>();
      pats.put(clientID, interestMap);
    }
    Pattern oldPattern = interestMap.put(interest, pattern);
    if (oldPattern == null) {
      // If the pattern didn't exist, add it to the set of keys to pass to any listeners.
      keysRegistered.add(interest);
    }
  }

  private void registerKeyInMap(Object interest, Set keysRegistered, Long clientID,
      Map<Object, Set> koi) {
    Set interestList = koi.get(clientID);
    if (interestList == null) {
      interestList = new CopyOnWriteHashSet();
      koi.put(clientID, interestList);
    }
    interestList.add(interest);
    keysRegistered.add(interest);
  }

  /**
   * Unregisters a client's interest
   *
   * @param inputClientID The identity of the client that is losing interest
   * @param interest The key in which to unregister interest
   * @param interestType the type of uninterest
   * @return the keys unregistered, which may be null
   */
  public Set unregisterClientInterest(Object inputClientID, Object interest, int interestType) {
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long) inputClientID;
    } else {
      Map<Object, Long> cids = clientMap.realIDs; // read
      clientID = cids.get(inputClientID);
      if (clientID == null) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "region profile unable to find '{}' for unregistration.  Probably means there is no durable queue.",
              inputClientID);
        }
        return null;
      }
    }
    Set keysUnregistered = new HashSet();
    operationType opType = null;
    synchronized (this.interestListLock) {
      switch (interestType) {
        case InterestType.KEY: {
          opType = operationType.UNREGISTER_KEY;
          unregisterClientKeys(inputClientID, interest, clientID, keysUnregistered);
          break;
        }
        case InterestType.REGULAR_EXPRESSION: {
          opType = operationType.UNREGISTER_PATTERN;
          unregisterClientPattern(interest, clientID, keysUnregistered);
          break;
        }
        case InterestType.FILTER_CLASS: {
          opType = operationType.UNREGISTER_FILTER;
          unregisterClientFilterClass(interest, clientID);
          break;
        }
        default:
          throw new InternalGemFireError(
              "bad interest type");
      }
      if (this.region != null && this.isLocalProfile) {
        sendProfileOperation(clientID, opType, interest, false);
      }
    } // synchronized
    return keysUnregistered;
  }

  private void unregisterClientFilterClass(Object interest, Long clientID) {
    if (interest == UnregisterAllInterest.singleton()) {
      if (getFiltersOfInterest().get(clientID) != null) {
        getFiltersOfInterest().remove(clientID);
      }
      if (getFiltersOfInterestInv().get(clientID) != null) {
        getFiltersOfInterestInv().remove(clientID);
      }
      return;
    }
    Map interestMap = getFiltersOfInterest().get(clientID);
    if (interestMap != null) {
      interestMap.remove(interest);
      if (interestMap.isEmpty()) {
        getFiltersOfInterest().remove(clientID);
      }
    }
    interestMap = getFiltersOfInterestInv().get(clientID);
    if (interestMap != null) {
      interestMap.remove(interest);
      if (interestMap.isEmpty()) {
        this.getFiltersOfInterestInv().remove(clientID);
      }
    }
  }

  private void unregisterClientPattern(Object interest, Long clientID, Set keysUnregistered) {
    if (interest instanceof String && ((String) interest).equals(".*")) { // ALL_KEYS
      unregisterAllKeys(interest, clientID, keysUnregistered);
      return;
    }
    if (interest == UnregisterAllInterest.singleton()) {
      unregisterClientIDFromMap(clientID, getPatternsOfInterest(), keysUnregistered);
      unregisterClientIDFromMap(clientID, getPatternsOfInterestInv(), keysUnregistered);
      if (getAllKeyClients().remove(clientID)) {
        keysUnregistered.add(".*");
      }
      if (getAllKeyClientsInv().remove(clientID)) {
        keysUnregistered.add(".*");
      }
    } else {
      unregisterPatternFromMap(getPatternsOfInterest(), interest, clientID, keysUnregistered);
      unregisterPatternFromMap(getPatternsOfInterestInv(), interest, clientID, keysUnregistered);
    }
  }

  private void unregisterPatternFromMap(Map<Object, Map<Object, Pattern>> map, Object interest,
      Long clientID, Set keysUnregistered) {
    Map interestMap = map.get(clientID);
    if (interestMap != null) {
      Object obj = interestMap.remove(interest);
      if (obj != null) {
        keysUnregistered.add(interest);
      }
      if (interestMap.isEmpty()) {
        map.remove(clientID);
      }
    }
  }

  private void unregisterClientIDFromMap(Long clientID, Map interestMap, Set keysUnregistered) {
    if (interestMap.get(clientID) != null) {
      Map removed = (Map) interestMap.remove(clientID);
      if (removed != null) {
        keysUnregistered.addAll(removed.keySet());
      }
    }
  }

  private void unregisterAllKeys(Object interest, Long clientID, Set keysUnregistered) {
    if (getAllKeyClients().remove(clientID)) {
      keysUnregistered.add(interest);
    }
    if (getAllKeyClientsInv().remove(clientID)) {
      keysUnregistered.add(interest);
    }
  }

  private void unregisterClientKeys(Object inputClientID, Object interest, Long clientID,
      Set keysUnregistered) {
    if (interest == UnregisterAllInterest.singleton()) {
      clearInterestFor(inputClientID);
      return;
    }
    unregisterKeyFromMap(getKeysOfInterest(), interest, clientID, keysUnregistered);
    unregisterKeyFromMap(getKeysOfInterestInv(), interest, clientID, keysUnregistered);
    return;
  }

  private void unregisterKeyFromMap(Map<Object, Set> map, Object interest, Long clientID,
      Set keysUnregistered) {
    Set interestList = map.get(clientID);
    if (interestList != null) {
      boolean removed = interestList.remove(interest);
      if (removed) {
        keysUnregistered.add(interest);
      }
      if (interestList.isEmpty()) {
        map.remove(clientID);
      }
    }
  }

  /**
   * Registers interest in a set of keys for a client
   *
   * @param keys The list of keys in which to register interest
   * @param updatesAsInvalidates whether to send invalidations instead of updates
   * @return the registered keys
   */
  public Set registerClientInterestList(Object inputClientID, List keys,
      boolean updatesAsInvalidates) {
    Long clientID = getClientIDForMaps(inputClientID);
    Set keysRegistered = new HashSet(keys);
    synchronized (interestListLock) {
      Map<Object, Set> koi = updatesAsInvalidates ? getKeysOfInterestInv() : getKeysOfInterest();
      CopyOnWriteHashSet interestList = (CopyOnWriteHashSet) koi.get(clientID);
      if (interestList == null) {
        interestList = new CopyOnWriteHashSet();
        koi.put(clientID, interestList);
      } else {
        // Get the list of keys that will be registered new, not already registered.
        keysRegistered.removeAll(interestList.getSnapshot());
      }
      interestList.addAll(keys);

      if (this.region != null && this.isLocalProfile) {
        sendProfileOperation(clientID, operationType.REGISTER_KEYS, keys, updatesAsInvalidates);
      }
    } // synchronized
    return keysRegistered;
  }

  /**
   * Unregisters interest in given keys for the given client
   *
   * @param inputClientID The fully-qualified name of the region in which to unregister interest
   * @param keys The list of keys in which to unregister interest
   * @return the unregistered keys
   */
  public Set unregisterClientInterestList(Object inputClientID, List keys) {
    Long clientID = getClientIDForMaps(inputClientID);
    Set keysUnregistered = new HashSet(keys);
    Set keysNotUnregistered = new HashSet(keys);
    synchronized (interestListLock) {
      CopyOnWriteHashSet interestList = (CopyOnWriteHashSet) getKeysOfInterest().get(clientID);
      if (interestList != null) {
        // Get the list of keys that are not registered but in unregister set.
        keysNotUnregistered.removeAll(interestList.getSnapshot());
        interestList.removeAll(keys);

        if (interestList.isEmpty()) {
          getKeysOfInterest().remove(clientID);
        }
      }
      interestList = (CopyOnWriteHashSet) getKeysOfInterestInv().get(clientID);
      if (interestList != null) {
        keysNotUnregistered.removeAll(interestList.getSnapshot());
        interestList.removeAll(keys);

        if (interestList.isEmpty()) {
          getKeysOfInterestInv().remove(clientID);
        }
      }

      if (this.region != null && this.isLocalProfile) {
        sendProfileOperation(clientID, operationType.UNREGISTER_KEYS, keys, false);
      }
    } // synchronized
    // Get the keys that are not unregistered.
    keysUnregistered.removeAll(keysNotUnregistered);
    return keysUnregistered;
  }

  public Set getKeysOfInterestFor(Object inputClientID) {
    Long clientID = getClientIDForMaps(inputClientID);
    Set keys1 = this.getKeysOfInterest().get(clientID);
    Set keys2 = this.getKeysOfInterestInv().get(clientID);
    if (keys1 == null) {
      if (keys2 == null) {
        return null;
      }
      return Collections.unmodifiableSet(keys2);
    } else if (keys2 == null) {
      return Collections.unmodifiableSet(keys1);
    } else {
      Set result = new HashSet(keys1);
      result.addAll(keys2);
      return Collections.unmodifiableSet(result);
    }
  }

  public Map<String, Pattern> getPatternsOfInterestFor(Object inputClientID) {
    Long clientID = getClientIDForMaps(inputClientID);
    Map patterns1 = this.getPatternsOfInterest().get(clientID);
    Map patterns2 = this.getPatternsOfInterestInv().get(clientID);
    if (patterns1 == null) {
      if (patterns2 == null) {
        return null;
      }
      return Collections.unmodifiableMap(patterns2);
    } else if (patterns2 == null) {
      return Collections.unmodifiableMap(patterns1);
    } else {
      Map<String, Pattern> result = new HashMap(patterns1);
      result.putAll(patterns2);
      return Collections.unmodifiableMap(result);
    }
  }

  public boolean hasKeysOfInterestFor(Object inputClientID, boolean wantInvalidations) {
    Long clientID = getClientIDForMaps(inputClientID);
    if (wantInvalidations) {
      return this.getKeysOfInterestInv().containsKey(clientID);
    }
    return this.getKeysOfInterest().containsKey(clientID);
  }

  public boolean hasAllKeysInterestFor(Object inputClientID) {
    Long clientID = getClientIDForMaps(inputClientID);
    return hasAllKeysInterestFor(clientID, false) || hasAllKeysInterestFor(clientID, true);
  }

  public boolean hasAllKeysInterestFor(Object inputClientID, boolean wantInvalidations) {
    Long clientID = getClientIDForMaps(inputClientID);
    if (wantInvalidations) {
      return getAllKeyClientsInv().contains(clientID);
    }
    return getAllKeyClients().contains(clientID);
  }

  public boolean hasRegexInterestFor(Object inputClientID, boolean wantInvalidations) {
    Long clientID = getClientIDForMaps(inputClientID);
    if (wantInvalidations) {
      return (this.getPatternsOfInterestInv().containsKey(clientID));
    }
    return (this.getPatternsOfInterest().containsKey(clientID));
  }

  public boolean hasFilterInterestFor(Object inputClientID, boolean wantInvalidations) {
    Long clientID = getClientIDForMaps(inputClientID);
    if (wantInvalidations) {
      return this.getFiltersOfInterestInv().containsKey(clientID);
    }
    return this.getFiltersOfInterest().containsKey(clientID);
  }


  /** determines whether there is any remaining interest for the given identifier */
  public boolean hasInterestFor(Object inputClientID) {
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long) inputClientID;
    } else {
      Map<Object, Long> cids = clientMap.realIDs; // read
      clientID = cids.get(inputClientID);
      if (clientID == null) {
        return false;
      }
    }
    return this.hasAllKeysInterestFor(clientID, true) || this.hasKeysOfInterestFor(clientID, true)
        || this.hasRegexInterestFor(clientID, true) || this.hasFilterInterestFor(clientID, true)
        || this.hasKeysOfInterestFor(clientID, false) || this.hasRegexInterestFor(clientID, false)
        || this.hasAllKeysInterestFor(clientID, false)
        || this.hasFilterInterestFor(clientID, false);
  }

  /*
   * Returns whether this interest list has any keys, patterns or filters of interest. It answers
   * the question: Are any clients being notified because of this interest list? @return whether
   * this interest list has any keys, patterns or filters of interest
   */
  public boolean hasInterest() {
    return (!this.getAllKeyClients().isEmpty()) || (!this.getAllKeyClientsInv().isEmpty())
        || (!this.getKeysOfInterest().isEmpty()) || (!this.getPatternsOfInterest().isEmpty())
        || (!this.getFiltersOfInterest().isEmpty()) || (!this.getKeysOfInterestInv().isEmpty())
        || (!this.getPatternsOfInterestInv().isEmpty())
        || (!this.getFiltersOfInterestInv().isEmpty());
  }

  /** removes all interest for the given identifier */
  public void clearInterestFor(Object inputClientID) {
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long) inputClientID;
    } else {
      Map<Object, Long> cids = clientMap.realIDs;
      clientID = cids.get(inputClientID);
      if (clientID == null) {
        // haven't seen this client yet
        return;
      }
    }
    synchronized (interestListLock) {
      {
        Set akc = this.getAllKeyClients();
        if (akc.contains(clientID)) {
          akc.remove(clientID);
        }
      }
      {
        Set akci = this.getAllKeyClientsInv();
        if (akci.contains(clientID)) {
          akci.remove(clientID);
        }
      }
      {
        Map<Object, Set> keys = this.getKeysOfInterest();
        if (keys.containsKey(clientID)) {
          keys.remove(clientID);
        }
      }
      {
        Map<Object, Set> keys = this.getKeysOfInterestInv();
        if (keys.containsKey(clientID)) {
          keys.remove(clientID);
        }
      }
      {
        Map<Object, Map<Object, Pattern>> pats = this.getPatternsOfInterest();
        if (pats.containsKey(clientID)) {
          pats.remove(clientID);
        }
      }
      {
        Map<Object, Map<Object, Pattern>> pats = this.getPatternsOfInterestInv();
        if (pats.containsKey(clientID)) {
          pats.remove(clientID);
        }
      }
      {
        Map<Object, Map> filters = this.getFiltersOfInterest();
        if (filters.containsKey(clientID)) {
          filters.remove(clientID);
        }
      }
      {
        Map<Object, Map> filters = this.getFiltersOfInterestInv();
        if (filters.containsKey(clientID)) {
          filters.remove(clientID);
        }
      }
      if (clientMap != null) {
        clientMap.removeIDMapping(clientID);
      }
      if (this.region != null && this.isLocalProfile) {
        sendProfileOperation(clientID, operationType.CLEAR, null, false);
      }
    }
  }

  /**
   * Obtains the number of CQs registered on the region. Assumption: CQs are not duplicated among
   * clients.
   *
   * @return int currently registered CQs of the region
   */
  public int getCqCount() {
    return this.cqCount.get();
  }

  public void incCqCount() {
    this.cqCount.getAndIncrement();
  }

  public void decCqCount() {
    this.cqCount.decrementAndGet();
  }

  /**
   * Returns the CQs registered on this region.
   */
  public Map getCqMap() {
    return this.cqs;
  }

  /**
   * does this profile contain any continuous queries?
   */
  public boolean hasCQs() {
    return this.cqCount.get() > 0;
  }

  public ServerCQ getCq(String cqName) {
    return (ServerCQ) this.cqs.get(cqName);
  }

  public void registerCq(ServerCQ cq) {
    ensureCqID(cq);
    if (logger.isDebugEnabled()) {
      logger.debug("Adding CQ {} to this members FilterProfile.", cq.getServerCqName());
    }
    this.cqs.put(cq.getServerCqName(), cq);
    this.incCqCount();

    // cq.setFilterID(cqMap.getWireID(cq.getServerCqName()));
    this.sendCQProfileOperation(operationType.REGISTER_CQ, cq);
  }

  public void stopCq(ServerCQ cq) {
    ensureCqID(cq);
    if (logger.isDebugEnabled()) {
      logger.debug("Stopping CQ {} on this members FilterProfile.", cq.getServerCqName());
    }
    this.sendCQProfileOperation(operationType.STOP_CQ, cq);
  }

  public String generateCqName(String serverCqName) {
    // Set unique CQ Name with respect to the senders filterProfile.
    // To avoid any conflict with the CQ names while maintained in
    // the CqService.
    // The CQ will be identified in the remote node using its
    // serverCqName.
    return (serverCqName + this.hashCode());
  }

  void processRegisterCq(String serverCqName, ServerCQ ServerCQ, boolean addToCqMap) {
    processRegisterCq(serverCqName, ServerCQ, addToCqMap, GemFireCacheImpl.getInstance());
  }


  /**
   * adds a new CQ to this profile during a delta operation or deserialization
   *
   * @param serverCqName the query objects' name
   * @param ServerCQ the new query object
   * @param addToCqMap whether to add the query to this.cqs
   */
  void processRegisterCq(String serverCqName, ServerCQ ServerCQ, boolean addToCqMap,
      GemFireCacheImpl cache) {
    if (cache == null) {
      logger.info("Error while initializing the CQs with FilterProfile for CQ " + serverCqName
          + ", Error : Cache has been closed.");
      return;
    }
    ServerCQ cq = (ServerCQ) ServerCQ;
    try {
      CqService cqService = cache.getCqService();
      cqService.start();
      cq.setCqService(cqService);
      CqStateImpl cqState = (CqStateImpl) cq.getState();
      cq.setName(generateCqName(serverCqName));
      cq.registerCq(null, null, cqState.getState());
    } catch (Exception ex) {
      logger.info("Error while initializing the CQs with FilterProfile for CQ {}, Error : {}",
          serverCqName, ex.getMessage(), ex);

    }
    if (logger.isDebugEnabled()) {
      logger.debug("Adding CQ to remote members FilterProfile using name: {}", serverCqName);
    }

    // The region's FilterProfile is accessed through CQ reference as the
    // region is not set on the FilterProfile created for the peer nodes.
    if (cq.getCqBaseRegion() != null) {
      if (addToCqMap) {
        this.cqs.put(serverCqName, cq);
      }

      FilterProfile pf = cq.getCqBaseRegion().getFilterProfile();
      if (pf != null) {
        pf.incCqCount();
      }
    }
  }

  public void processCloseCq(String serverCqName) {
    ServerCQ cq = (ServerCQ) this.cqs.get(serverCqName);
    if (cq != null) {
      try {
        cq.close(false);
      } catch (Exception ex) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Unable to close the CQ with the filterProfile, on region {} for CQ {}, Error : {}",
              this.region.getFullPath(), serverCqName, ex.getMessage(), ex);
        }
      }
      this.cqs.remove(serverCqName);
      cq.getCqBaseRegion().getFilterProfile().decCqCount();
    }
  }

  public void processSetCqState(String serverCqName, ServerCQ ServerCQ) {
    ServerCQ cq = (ServerCQ) this.cqs.get(serverCqName);
    if (cq != null) {
      CqStateImpl cqState = (CqStateImpl) ServerCQ.getState();
      cq.setCqState(cqState.getState());
    }
  }

  public void processStopCq(String serverCqName) {
    ServerCQ cq = (ServerCQ) this.cqs.get(serverCqName);
    if (cq != null) {
      try {
        cq.stop();
      } catch (Exception ex) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Unable to stop the CQ with the filterProfile, on region {} for CQ {}, Error : {}",
              this.region.getFullPath(), serverCqName, ex.getMessage(), ex);
        }
      }
    }
  }

  public void setCqState(ServerCQ cq) {
    // This could be when CQ is stopped and restarted.
    ensureCqID(cq);
    if (logger.isDebugEnabled()) {
      logger.debug("Stopping CQ {} on this members FilterProfile.", cq.getServerCqName());
    }
    this.sendCQProfileOperation(operationType.SET_CQ_STATE, cq);
  }

  public void closeCq(ServerCQ cq) {
    ensureCqID(cq);
    String serverCqName = cq.getServerCqName();
    this.cqs.remove(serverCqName);
    if (this.cqMap != null) {
      this.cqMap.removeIDMapping(cq.getFilterID());
    }
    this.decCqCount();
    this.sendCQProfileOperation(operationType.CLOSE_CQ, cq);
  }

  void cleanupForClient(CacheClientNotifier ccn, ClientProxyMembershipID client) {
    Iterator cqIter = this.cqs.entrySet().iterator();
    while (cqIter.hasNext()) {
      Map.Entry cqEntry = (Map.Entry) cqIter.next();
      ServerCQ cq = (ServerCQ) cqEntry.getValue();
      ClientProxyMembershipID clientId = cq.getClientProxyId();
      if (clientId.equals(client)) {
        try {
          cq.close(false);
        } catch (Exception ignore) {
          if (logger.isDebugEnabled()) {
            logger.debug("Failed to remove CQ from the base region. CqName : {}", cq.getName());
          }
        }
        this.closeCq(cq);
      }
    }

    // Remove the client from the clientMap
    this.clientMap.removeIDMapping(client);
  }

  /**
   * this will be get called when we remove other server profile from region advisor.
   */
  void cleanUp() {
    Map tmpCq = this.cqs;

    if (tmpCq.size() > 0) {
      for (Object serverCqName : tmpCq.keySet()) {
        processCloseCq((String) serverCqName);
      }
    }
  }

  /**
   * Returns if old value is required for CQ processing or not. In order to reduce the query
   * processing time CQ caches the event keys its already seen, if the key is cached than the old
   * value is not required.
   */
  public boolean entryRequiresOldValue(Object key) {
    if (this.hasCQs()) {
      if (!CqServiceProvider.MAINTAIN_KEYS) {
        return true;
      }
      Iterator cqIter = this.cqs.values().iterator();
      while (cqIter.hasNext()) {
        ServerCQ cq = (ServerCQ) cqIter.next();
        if (cq.isOldValueRequiredForQueryProcessing(key)) {
          return true;
        }
      }
    }
    return false;
  }

  private void sendProfileOperation(Long clientID, operationType opType, Object interest,
      boolean updatesAsInvalidates) {
    if (!(this.region instanceof PartitionedRegion)) {
      return;
    }
    OperationMessage msg = new OperationMessage();
    msg.regionName = this.region.getFullPath();
    msg.clientID = clientID;
    msg.opType = opType;
    msg.interest = interest;
    msg.updatesAsInvalidates = updatesAsInvalidates;
    sendFilterProfileOperation(msg);
  }


  private void sendFilterProfileOperation(OperationMessage msg) {
    Set recipients =
        ((DistributionAdvisee) this.region).getDistributionAdvisor().adviseProfileUpdate();
    msg.setRecipients(recipients);
    ReplyProcessor21 rp = new ReplyProcessor21(this.region.getDistributionManager(), recipients);
    msg.processorId = rp.getProcessorId();
    this.region.getDistributionManager().putOutgoing(msg);
    try {
      rp.waitForReplies();
    } catch (InterruptedException ignore) {
      Thread.currentThread().interrupt();
    }
  }

  private void sendCQProfileOperation(operationType opType, ServerCQ cq) {
    // we only need to distribute for PRs. Other regions do local filter processing
    if (!(this.region instanceof PartitionedRegion)) {
      // note that we do not need to update requiresOldValueInEvents because
      // that flag is only used during region initialization. Otherwise we
      // would need to
      return;
    }
    OperationMessage msg = new OperationMessage();
    msg.regionName = this.region.getFullPath();
    msg.opType = opType;
    msg.cq = cq;
    try {
      sendFilterProfileOperation(msg);
    } catch (Exception ex) {
      if (logger.isDebugEnabled()) {
        logger.debug("Error sending CQ request to peers. {}", ex.getLocalizedMessage(), ex);
      }
    }
  }

  @Immutable
  static final Profile[] NO_PROFILES = new Profile[0];

  private final CacheProfile localProfile = new CacheProfile(this);

  private final Profile[] localProfileArray = new Profile[] {localProfile};

  /** compute local routing information */
  public FilterInfo getLocalFilterRouting(CacheEvent event) {
    FilterRoutingInfo fri = getFilterRoutingInfoPart2(null, event);
    if (fri != null) {
      return fri.getLocalFilterInfo();
    } else {
      return null;
    }
  }

  /**
   * @return the local CacheProfile for this FilterProfile's Region
   */
  public CacheProfile getLocalProfile() {
    return this.localProfile;
  }

  /**
   * Compute the full routing information for the given set of peers. This will not include local
   * routing information from interest processing. That is done by getFilterRoutingInfoPart2
   */
  public FilterRoutingInfo getFilterRoutingInfoPart1(CacheEvent event, Profile[] peerProfiles,
      Set cacheOpRecipients) {
    // early out if there are no cache servers in the system
    boolean anyServers = false;
    for (int i = 0; i < peerProfiles.length; i++) {
      if (((CacheProfile) peerProfiles[i]).hasCacheServer) {
        anyServers = true;
        break;
      }
    }
    if (!anyServers && !this.localProfile.hasCacheServer) {
      return null;
    }

    FilterRoutingInfo frInfo = null;

    CqService cqService = getCqService(event.getRegion());
    if (cqService.isRunning()) {
      frInfo = new FilterRoutingInfo();
      // bug #50809 - local routing for transactional ops must be done here
      // because the event isn't available later and we lose the old value for the entry
      final boolean processLocalProfile =
          event.getOperation().isEntry() && ((EntryEvent) event).getTransactionId() != null;
      fillInCQRoutingInfo(event, processLocalProfile, peerProfiles, frInfo);
    }

    // Process InterestList.
    // return fillInInterestRoutingInfo(event, peerProfiles, frInfo, cacheOpRecipients);
    frInfo = fillInInterestRoutingInfo(event, peerProfiles, frInfo, cacheOpRecipients);
    if (frInfo == null || !frInfo.hasMemberWithFilterInfo()) {
      return null;
    } else {
      return frInfo;
    }
  }



  /**
   * get local routing information
   *
   * @param part1Info routing information for peers, if any
   * @param event the event to process
   * @return routing information for clients connected to this server
   */
  public FilterRoutingInfo getFilterRoutingInfoPart2(FilterRoutingInfo part1Info,
      CacheEvent event) {
    FilterRoutingInfo result = part1Info;
    if (localProfile.hasCacheServer) {
      // bug #45520 - CQ events arriving out of order causes result set
      // inconsistency, so don't compute routings for events in conflict
      boolean isInConflict =
          event.getOperation().isEntry() && ((EntryEventImpl) event).isConcurrencyConflict();
      CqService cqService = getCqService(event.getRegion());
      if (!isInConflict && cqService.isRunning()
          && this.region != null /*
                                  * && !( this.region.isUsedForPartitionedRegionBucket() || //
                                  * partitioned region CQ this.region instanceof PartitionedRegion)
                                  */) { // processing is done in part 1
        if (result == null) {
          result = new FilterRoutingInfo();
        }
        if (logger.isDebugEnabled()) {
          logger.debug("getting local cq matches for {}", event);
        }
        fillInCQRoutingInfo(event, true, NO_PROFILES, result);
      }
      result = fillInInterestRoutingInfo(event, localProfileArray, result, Collections.emptySet());
    }
    return result;
  }

  /**
   * get continuous query routing information
   *
   * @param event the event to process
   * @param peerProfiles the profiles getting this event
   * @param frInfo the routing table to update
   */
  private void fillInCQRoutingInfo(CacheEvent event, boolean processLocalProfile,
      Profile[] peerProfiles, FilterRoutingInfo frInfo) {
    CqService cqService = getCqService(event.getRegion());
    if (cqService != null) {
      try {
        Profile local = processLocalProfile ? this.localProfile : null;
        cqService.processEvents(event, local, peerProfiles, frInfo);
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, re-throw the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        SystemFailure.checkFailure();
        logger.error("Exception occurred while processing CQs", t);
      }
    }
  }

  private CqService getCqService(Region region) {
    return ((InternalCache) region.getRegionService()).getCqService();
  }

  /**
   * computes FilterRoutingInfo objects for each of the given events
   */
  public void getLocalFilterRoutingForPutAllOp(DistributedPutAllOperation dpao,
      DistributedPutAllOperation.PutAllEntryData[] putAllData) {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    if (this.region != null && this.localProfile.hasCacheServer) {
      Set clientsInv = null;
      Set clients = null;
      int size = putAllData.length;
      CqService cqService = getCqService(dpao.getRegion());
      boolean doCQs = cqService.isRunning();
      for (int idx = 0; idx < size; idx++) {
        PutAllEntryData pEntry = putAllData[idx];
        if (pEntry != null) {
          @Unretained
          final EntryEventImpl ev = dpao.getEventForPosition(idx);
          FilterRoutingInfo fri = pEntry.filterRouting;
          FilterInfo fi = null;
          if (fri != null) {
            fi = fri.getLocalFilterInfo();
          }
          if (isDebugEnabled) {
            logger.debug("Finding locally interested clients for {}", ev);
          }
          if (doCQs) {
            if (fri == null) {
              fri = new FilterRoutingInfo();
            }
            fillInCQRoutingInfo(ev, true, NO_PROFILES, fri);
            fi = fri.getLocalFilterInfo();
          }
          clientsInv = this.getInterestedClients(ev, this.allKeyClientsInv,
              this.keysOfInterestInv, this.patternsOfInterestInv, this.filtersOfInterestInv);
          clients = this.getInterestedClients(ev, this.allKeyClients, this.keysOfInterest,
              this.patternsOfInterest, this.filtersOfInterest);
          if (clients != null || clientsInv != null) {
            if (fi == null) {
              fi = new FilterInfo();
              // no need to create or update a FilterRoutingInfo at this time
            }
            fi.setInterestedClients(clients);
            fi.setInterestedClientsInv(clientsInv);
          }
          ev.setLocalFilterInfo(fi);
        }
      }
    }
  }

  /**
   * computes FilterRoutingInfo objects for each of the given events
   */
  public void getLocalFilterRoutingForRemoveAllOp(DistributedRemoveAllOperation op,
      RemoveAllEntryData[] removeAllData) {
    if (this.region != null && this.localProfile.hasCacheServer) {
      Set clientsInv = null;
      Set clients = null;
      int size = removeAllData.length;
      CqService cqService = getCqService(op.getRegion());
      boolean doCQs = cqService.isRunning();
      for (int idx = 0; idx < size; idx++) {
        RemoveAllEntryData pEntry = removeAllData[idx];
        if (pEntry != null) {
          @Unretained
          final EntryEventImpl ev = op.getEventForPosition(idx);
          FilterRoutingInfo fri = pEntry.filterRouting;
          FilterInfo fi = null;
          if (fri != null) {
            fi = fri.getLocalFilterInfo();
          }
          if (logger.isDebugEnabled()) {
            logger.debug("Finding locally interested clients for {}", ev);
          }
          if (doCQs) {
            if (fri == null) {
              fri = new FilterRoutingInfo();
            }
            fillInCQRoutingInfo(ev, true, NO_PROFILES, fri);
            fi = fri.getLocalFilterInfo();
          }
          clientsInv = this.getInterestedClients(ev, this.allKeyClientsInv,
              this.keysOfInterestInv, this.patternsOfInterestInv, this.filtersOfInterestInv);
          clients = this.getInterestedClients(ev, this.allKeyClients, this.keysOfInterest,
              this.patternsOfInterest, this.filtersOfInterest);
          if (clients != null || clientsInv != null) {
            if (fi == null) {
              fi = new FilterInfo();
              // no need to create or update a FilterRoutingInfo at this time
            }
            fi.setInterestedClients(clients);
            fi.setInterestedClientsInv(clientsInv);
          }
          // if (this.logger.fineEnabled()) {
          // this.region.getLogWriterI18n().fine("setting event routing to " + fi);
          // }
          ev.setLocalFilterInfo(fi);
        }
      }
    }
  }

  /**
   * Fills in the routing information for clients that have registered interest in the given event.
   * The routing information is stored in the given FilterRoutingInfo object for use in message
   * delivery.
   *
   * @param event the event being applied to the cache
   * @param profiles the profiles of members having the affected region
   * @param filterRoutingInfo the routing object that is modified by this method (may be null)
   * @param cacheOpRecipients members that will receive a CacheDistributionMessage for the event
   * @return the resulting FilterRoutingInfo
   */
  public FilterRoutingInfo fillInInterestRoutingInfo(CacheEvent event, Profile[] profiles,
      FilterRoutingInfo filterRoutingInfo, Set cacheOpRecipients) {

    Set clientsInv = Collections.emptySet();
    Set clients = Collections.emptySet();

    if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER_VERBOSE)) {
      logger.trace(LogMarker.BRIDGE_SERVER_VERBOSE, "finding interested clients for {}", event);
    }

    FilterRoutingInfo frInfo = filterRoutingInfo;

    for (int i = 0; i < profiles.length; i++) {
      CacheProfile cf = (CacheProfile) profiles[i];

      if (!cf.hasCacheServer) {
        continue;
      }

      FilterProfile pf = cf.filterProfile;

      if (pf == null) {
        continue;
      }

      if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER_VERBOSE)) {
        logger.trace(LogMarker.BRIDGE_SERVER_VERBOSE, "Processing {}", pf);
      }

      if (!pf.hasInterest()) {
        // This block sends an empty routing table to a member that's going to
        // get a CacheDistributionMessage so that if, in flight, there is an
        // interest registration change the version held in the routing table
        // can be used to detect the change and the receiver can recompute
        // the routing.
        if (!pf.isLocalProfile() && cacheOpRecipients.contains(cf.getDistributedMember())) {
          if (frInfo == null)
            frInfo = new FilterRoutingInfo();
          frInfo.addInterestedClients(cf.getDistributedMember(), Collections.emptySet(),
              Collections.emptySet(), false);
        }
        continue;
      }

      if (event.getOperation() == null) {
        continue;
      }

      if (event.getOperation().isEntry()) {
        EntryEvent entryEvent = (EntryEvent) event;
        clientsInv = pf.getInterestedClients(entryEvent, pf.allKeyClientsInv,
            pf.keysOfInterestInv, pf.patternsOfInterestInv, pf.filtersOfInterestInv);
        clients = pf.getInterestedClients(entryEvent, pf.allKeyClients, pf.keysOfInterest,
            pf.patternsOfInterest, pf.filtersOfInterest);
      } else {
        if (event.getOperation().isRegionDestroy() || event.getOperation().isClear()) {
          clientsInv = pf.getAllClientsWithInterestInv();
          clients = pf.getAllClientsWithInterest();
        } else {
          return frInfo;
        }
      }

      if (pf.isLocalProfile) {
        if (logger.isDebugEnabled()) {
          logger.debug("Setting local interested clients={} and clientsInv={}", clients,
              clientsInv);
        }
        if (frInfo == null)
          frInfo = new FilterRoutingInfo();
        frInfo.setLocalInterestedClients(clients, clientsInv);
      } else {
        if (cacheOpRecipients.contains(cf.getDistributedMember()) || // always send a routing with
                                                                     // CacheOperationMessages
            (clients != null && !clients.isEmpty())
            || (clientsInv != null && !clientsInv.isEmpty())) {
          if (logger.isDebugEnabled()) {
            logger.debug("Adding interested clients={} and clientsIn={} to {}", clients, clientsInv,
                filterRoutingInfo);
          }
          if (frInfo == null)
            frInfo = new FilterRoutingInfo();
          frInfo.addInterestedClients(cf.getDistributedMember(), clients, clientsInv,
              this.clientMap.hasLongID);
        }
      }
    }
    return frInfo;
  }

  /**
   * get the clients interested in the given event that are attached to this server.
   *
   * @param event the entry event being applied to the cache
   * @param akc allKeyClients collection
   * @param koi keysOfInterest collection
   * @param pats patternsOfInterest collection
   * @param foi filtersOfInterest collection
   * @return a set of the clients interested in the event
   */
  private Set getInterestedClients(EntryEvent event, Set akc, Map<Object, Set> koi,
      Map<Object, Map<Object, Pattern>> pats, Map<Object, Map> foi) {
    Set result = null;
    if (akc != null) {
      result = new HashSet(akc);
      if (logger.isDebugEnabled()) {
        logger.debug("these clients matched for all-keys: {}", akc);
      }
    }
    if (koi != null) {
      for (Iterator it = koi.entrySet().iterator(); it.hasNext();) {
        Map.Entry entry = (Map.Entry) it.next();
        Set keys = (Set) entry.getValue();
        if (keys.contains(event.getKey())) {
          Object clientID = entry.getKey();
          if (result == null)
            result = new HashSet();
          result.add(clientID);
          if (logger.isDebugEnabled()) {
            logger.debug("client {} matched for key list (size {})", clientID,
                koi.get(clientID).size());
          }
        }
      }
    }
    if (pats != null && (event.getKey() instanceof String)) {
      for (Iterator it = pats.entrySet().iterator(); it.hasNext();) {
        Map.Entry entry = (Map.Entry) it.next();
        String stringKey = (String) event.getKey();
        Map<Object, Pattern> interestList = (Map<Object, Pattern>) entry.getValue();
        for (Pattern keyPattern : interestList.values()) {
          if (keyPattern.matcher(stringKey).matches()) {
            Object clientID = entry.getKey();
            if (result == null)
              result = new HashSet();
            result.add(clientID);
            if (logger.isDebugEnabled()) {
              logger.debug("client {} matched for pattern ({})", clientID, pats.get(clientID));
            }
            break;
          }
        }
      }
    }
    if (foi != null && foi.size() > 0) {
      Object value;
      boolean serialized;
      {
        SerializedCacheValue<?> serValue = event.getSerializedNewValue();
        serialized = (serValue != null);
        if (!serialized) {
          value = event.getNewValue();
        } else {
          value = serValue.getSerializedValue();
        }
      }
      InterestEvent iev = new InterestEvent(event.getKey(), value, !serialized);
      Operation op = event.getOperation();
      for (Iterator it = foi.entrySet().iterator(); it.hasNext();) {
        Map.Entry entry = (Map.Entry) it.next();
        Map<String, InterestFilter> interestList = (Map<String, InterestFilter>) entry.getValue();
        for (InterestFilter filter : interestList.values()) {
          if ((op.isCreate() && filter.notifyOnCreate(iev))
              || (op.isUpdate() && filter.notifyOnUpdate(iev))
              || (op.isDestroy() && filter.notifyOnDestroy(iev))
              || (op.isInvalidate() && filter.notifyOnInvalidate(iev))) {
            Object clientID = entry.getKey();
            if (result == null)
              result = new HashSet();
            result.add(clientID);
            if (logger.isDebugEnabled()) {
              logger.debug("client {} matched for filter ({})", clientID,
                  getFiltersOfInterest().get(clientID));
            }
            break;
          }
        }
      }
    }
    return result;
  }


  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    InternalDistributedMember id = new InternalDistributedMember();
    InternalDataSerializer.invokeFromData(id, in);
    this.memberID = id;

    this.allKeyClients.addAll(InternalDataSerializer.readSetOfLongs(in));
    this.keysOfInterest.putAll(DataSerializer.readHashMap(in));
    this.patternsOfInterest.putAll(DataSerializer.readHashMap(in));
    this.filtersOfInterest.putAll(DataSerializer.readHashMap(in));

    this.allKeyClientsInv.addAll(InternalDataSerializer.readSetOfLongs(in));
    this.keysOfInterestInv.putAll(DataSerializer.readHashMap(in));
    this.patternsOfInterestInv.putAll(DataSerializer.readHashMap(in));
    this.filtersOfInterestInv.putAll(DataSerializer.readHashMap(in));

    // Read CQ Info.
    int numCQs = InternalDataSerializer.readArrayLength(in);
    if (numCQs > 0) {
      final InitializationLevel oldLevel = LocalRegion.setThreadInitLevelRequirement(ANY_INIT);
      try {
        for (int i = 0; i < numCQs; i++) {
          String serverCqName = DataSerializer.readString(in);
          ServerCQ cq = CqServiceProvider.readCq(in);
          processRegisterCq(serverCqName, cq, false);
          this.cqs.put(serverCqName, cq);
        }
      } finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
      }
    }

  }

  @Override
  public int getDSFID() {
    return FILTER_PROFILE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    InternalDataSerializer.invokeToData(memberID, out);
    InternalDataSerializer.writeSetOfLongs(this.allKeyClients.getSnapshot(),
        this.clientMap.hasLongID, out);
    DataSerializer.writeHashMap(this.keysOfInterest.getSnapshot(), out);
    DataSerializer.writeHashMap(this.patternsOfInterest.getSnapshot(), out);
    DataSerializer.writeHashMap(this.filtersOfInterest.getSnapshot(), out);

    InternalDataSerializer.writeSetOfLongs(this.allKeyClientsInv.getSnapshot(),
        this.clientMap.hasLongID, out);
    DataSerializer.writeHashMap(this.keysOfInterestInv.getSnapshot(), out);
    DataSerializer.writeHashMap(this.patternsOfInterestInv.getSnapshot(), out);
    DataSerializer.writeHashMap(this.filtersOfInterestInv.getSnapshot(), out);

    // Write CQ info.
    Map<String, ServerCQ> theCQs = this.cqs.getSnapshot();
    int size = theCQs.size();
    InternalDataSerializer.writeArrayLength(size, out);
    for (Iterator<Map.Entry<String, ServerCQ>> it = theCQs.entrySet().iterator(); it.hasNext();) {
      Map.Entry<String, ServerCQ> entry = it.next();
      String name = entry.getKey();
      ServerCQ cq = entry.getValue();
      DataSerializer.writeString(name, out);
      InternalDataSerializer.invokeToData(cq, out);
    }
  }

  /**
   * @return the keysOfInterest
   */
  private Map<Object, Set> getKeysOfInterest() {
    return this.keysOfInterest;
  }

  /**
   * @return the keysOfInterestInv
   */
  private Map<Object, Set> getKeysOfInterestInv() {
    return this.keysOfInterestInv;
  }

  /**
   * @return the patternsOfInterest
   */
  private Map<Object, Map<Object, Pattern>> getPatternsOfInterest() {
    return this.patternsOfInterest;
  }

  /**
   * @return the patternsOfInterestInv
   */
  private Map<Object, Map<Object, Pattern>> getPatternsOfInterestInv() {
    return this.patternsOfInterestInv;
  }

  /**
   * @return the filtersOfInterestInv
   */
  Map<Object, Map> getFiltersOfInterestInv() {
    return this.filtersOfInterestInv;
  }

  /**
   * @return the filtersOfInterest
   */
  private Map<Object, Map> getFiltersOfInterest() {
    return this.filtersOfInterest;
  }

  private Set<Object> getAllKeyClients() {
    if (testHook != null) {
      testHook.await();
    }
    return (Set) this.allKeyClients;
  }

  public int getAllKeyClientsSize() {
    return this.getAllKeyClients().size();
  }

  private Set<Long> getAllKeyClientsInv() {
    return this.allKeyClientsInv;
  }

  public int getAllKeyClientsInvSize() {
    return this.getAllKeyClientsInv().size();
  }

  /**
   * When clients are registered they are assigned a Long identifier. This method maps between the
   * real client ID and its Long identifier.
   */
  private Long getClientIDForMaps(Object inputClientID) {
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long) inputClientID;
    } else {
      clientID = clientMap.getWireID(inputClientID);
    }
    return clientID;
  }

  @Override
  public String toString() {
    final boolean isDebugEnabled = logger.isTraceEnabled(LogMarker.BRIDGE_SERVER_VERBOSE);
    return "FilterProfile(id=" + (this.isLocalProfile ? "local" : this.memberID) + ";  numCQs: "
        + ((this.cqCount == null) ? 0 : this.cqCount.get())
        + (isDebugEnabled ? (";  " + getClientMappingString()) : "")
        + (isDebugEnabled ? (";  " + getCqMappingString()) : "") + ")";
  }

  /** for debugging we could sometimes use a dump of the Long->clientID table */
  private String getClientMappingString() {
    if (clientMap == null) {
      return "";
    }
    Map wids = clientMap.wireIDs;
    if (wids.size() == 0) {
      return "clients[]";
    }
    Set<Long> sorted = new TreeSet(wids.keySet());
    StringBuilder result = new StringBuilder(sorted.size() * 70);
    result.append("clients[");
    Iterator<Long> it = sorted.iterator();
    for (int i = 1; it.hasNext(); i++) {
      Long wireID = it.next();
      result.append(wireID).append("={").append(wids.get(wireID)).append('}');
      if (it.hasNext()) {
        result.append(", ");
      }
    }
    result.append("]");
    return result.toString();
  }


  /** for debugging we could sometimes use a dump of the Long->cq name table */
  private String getCqMappingString() {
    if (cqMap == null) {
      return "";
    }
    Map wids = cqMap.wireIDs;
    if (wids.size() == 0) {
      return "cqs[]";
    }
    Set<Long> sorted = new TreeSet(wids.keySet());
    StringBuilder result = new StringBuilder(sorted.size() * 70);
    result.append("cqs[");
    Iterator<Long> it = sorted.iterator();
    for (int i = 1; it.hasNext(); i++) {
      Long wireID = it.next();
      result.append(wireID).append("={").append(wids.get(wireID)).append('}');
      if (it.hasNext()) {
        result.append(", ");
      }
    }
    result.append("]");
    return result.toString();
  }

  /**
   * given a collection of on-wire identifiers, this returns a set of the client/server identifiers
   * for each client or durable queue
   *
   * @param integerIDs the integer ids of the clients/queues
   * @return the translated identifiers
   */
  public Set getRealClientIDs(Collection integerIDs) {
    return clientMap.getRealIDs(integerIDs);
  }

  /**
   * given a collection of on-wire identifiers, this returns a set of the CQ identifiers they
   * correspond to
   *
   * @param integerIDs the integer ids of the clients/queues
   * @return the translated identifiers
   */
  public Set getRealCqIDs(Collection integerIDs) {
    return cqMap.getRealIDs(integerIDs);
  }

  /**
   * given an on-wire filter ID, find and return the corresponding cq name
   *
   * @param integerID the on-wire ID
   * @return the translated id
   */
  public String getRealCqID(Long integerID) {
    return (String) cqMap.getRealID(integerID);
  }

  /**
   * Return the set of real client proxy ids
   *
   * @return the set of real client proxy ids
   */
  @VisibleForTesting
  public Set getRealClientIds() {
    return clientMap == null ? Collections.emptySet()
        : Collections.unmodifiableSet(clientMap.realIDs.keySet());
  }

  /**
   * Return the set of wire client proxy ids
   *
   * @return the set of wire client proxy ids
   */
  @VisibleForTesting
  public Set getWireClientIds() {
    return clientMap == null ? Collections.emptySet()
        : Collections.unmodifiableSet(clientMap.wireIDs.keySet());
  }

  /**
   * ensure that the given query contains a filter routing ID
   */
  private void ensureCqID(ServerCQ cq) {
    if (cq.getFilterID() == null) {
      // on-wire IDs are assigned in the VM where the CQ was registered
      assert this.isLocalProfile;
      cq.setFilterID(cqMap.getWireID(cq.getServerCqName()));
    }
  }

  /**
   * @return the isLocalProfile
   */
  public boolean isLocalProfile() {
    return isLocalProfile;
  }

  /**
   * Returns the filter profile messages received while members cache profile exchange was in
   * progress.
   *
   * @param member whose messages are returned.
   * @return filter profile messages that are queued for the member.
   */
  public List getQueuedFilterProfileMsgs(InternalDistributedMember member) {
    synchronized (this.filterProfileMsgQueue) {
      if (this.filterProfileMsgQueue.containsKey(member)) {
        return new LinkedList(this.filterProfileMsgQueue.get(member));
      }
    }
    return Collections.emptyList();
  }

  /**
   * Removes the filter profile messages from the queue that are received while the members cache
   * profile exchange was in progress.
   *
   * @param member whose messages are returned.
   * @return filter profile messages that are queued for the member.
   */
  public List removeQueuedFilterProfileMsgs(InternalDistributedMember member) {
    synchronized (this.filterProfileMsgQueue) {
      if (this.filterProfileMsgQueue.containsKey(member)) {
        return new LinkedList(this.filterProfileMsgQueue.remove(member));
      }
    }
    return Collections.emptyList();
  }

  /**
   * Adds the message to filter profile queue.
   */
  public void addToFilterProfileQueue(InternalDistributedMember member, OperationMessage message) {
    if (logger.isDebugEnabled()) {
      logger.debug("Adding message to filter profile queue: {} for member : {}", message, member);
    }
    synchronized (this.filterProfileMsgQueue) {
      LinkedList msgs = this.filterProfileMsgQueue.get(member);
      if (msgs == null) {
        msgs = new LinkedList();
        this.filterProfileMsgQueue.put(member, msgs);
      }
      msgs.add(message);
    }
  }

  /**
   * Process the filter profile messages.
   */
  public void processQueuedFilterProfileMsgs(List msgs) {
    final boolean isDebugEnabled = logger.isDebugEnabled();

    if (msgs != null) {
      Iterator iter = msgs.iterator();
      while (iter.hasNext()) {
        try {
          OperationMessage msg = (OperationMessage) iter.next();
          if (isDebugEnabled) {
            logger.debug("Processing the queued filter profile message :{}", msg);
          }
          msg.processRequest(this);
        } catch (Exception ex) {
          logger.warn("Exception thrown while processing queued profile messages.", ex);
        }
      }
    }
  }

  /**
   * OperationMessage synchronously propagates a change in the profile to another member. It is a
   * serial message so that there is no chance of out-of-order execution.
   */
  public static class OperationMessage extends HighPriorityDistributionMessage
      implements MessageWithReply {

    public long profileVersion;
    boolean updatesAsInvalidates;
    String regionName;
    operationType opType;
    long clientID;
    Object interest;
    int processorId;
    ServerCQ cq;
    String serverCqName;

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.distributed.internal.DistributionMessage#process(org.apache.geode.
     * distributed.internal.DistributionManager)
     */
    @Override
    protected void process(ClusterDistributionManager dm) {
      try {
        CacheDistributionAdvisee r = findRegion(dm);
        if (r == null) {
          if (logger.isDebugEnabled()) {
            logger.debug("Region not found, so ignoring filter profile update: {}", this);
          }
          return;
        }
        // we only need to record the delta if this is a partitioned region
        if (!(r instanceof PartitionedRegion)) {
          return;
        }

        CacheDistributionAdvisor cda = (CacheDistributionAdvisor) r.getDistributionAdvisor();
        CacheDistributionAdvisor.CacheProfile cp =
            (CacheDistributionAdvisor.CacheProfile) cda.getProfile(getSender());
        if (cp == null) { // PR accessors do not keep filter profiles around
          if (logger.isDebugEnabled()) {
            logger.debug(
                "No cache profile to update, adding filter profile message to queue. Message :{}",
                this);
          }
          FilterProfile localFP = ((LocalRegion) r).getFilterProfile();
          localFP.addToFilterProfileQueue(getSender(), this);
          dm.getCancelCriterion().checkCancelInProgress(null);
        } else {
          cp.hasCacheServer = true;
          FilterProfile fp = cp.filterProfile;
          if (fp == null) { // PR accessors do not keep filter profiles around
            if (logger.isDebugEnabled()) {
              logger.debug("No filter profile to update: {}", this);
            }
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug("Processing the filter profile request for : {}", this);
            }
            processRequest(fp);
          }
        }
      } catch (RuntimeException e) {
        logger.warn("Exception thrown while processing profile update", e);
      } finally {
        ReplyMessage reply = new ReplyMessage();
        reply.setProcessorId(this.processorId);
        reply.setRecipient(getSender());
        try {
          dm.putOutgoing(reply);
        } catch (CancelException ignore) {
          // can't send a reply, so ignore the exception
        }
      }
    }

    public void processRequest(FilterProfile fp) {
      switch (opType) {
        case REGISTER_KEY:
          fp.registerClientInterest(clientID, this.interest, InterestType.KEY,
              updatesAsInvalidates);
          break;
        case REGISTER_PATTERN:
          fp.registerClientInterest(clientID, this.interest, InterestType.REGULAR_EXPRESSION,
              updatesAsInvalidates);
          break;
        case REGISTER_FILTER:
          fp.registerClientInterest(clientID, this.interest, InterestType.FILTER_CLASS,
              updatesAsInvalidates);
          break;
        case REGISTER_KEYS:
          fp.registerClientInterestList(clientID, (List) this.interest, updatesAsInvalidates);
          break;
        case UNREGISTER_KEY:
          fp.unregisterClientInterest(clientID, this.interest, InterestType.KEY);
          break;
        case UNREGISTER_PATTERN:
          fp.unregisterClientInterest(clientID, this.interest, InterestType.REGULAR_EXPRESSION);
          break;
        case UNREGISTER_FILTER:
          fp.unregisterClientInterest(clientID, this.interest, InterestType.FILTER_CLASS);
          break;
        case UNREGISTER_KEYS:
          fp.unregisterClientInterestList(clientID, (List) this.interest);
          break;
        case CLEAR:
          fp.clearInterestFor(clientID);
          break;
        case HAS_CQ:
          fp.cqCount.set(1);
          break;
        case REGISTER_CQ:
          fp.processRegisterCq(this.serverCqName, this.cq, true);
          break;
        case CLOSE_CQ:
          fp.processCloseCq(this.serverCqName);
          break;
        case STOP_CQ:
          fp.processStopCq(this.serverCqName);
          break;
        case SET_CQ_STATE:
          fp.processSetCqState(this.serverCqName, this.cq);
          break;

        default:
          throw new IllegalArgumentException(
              "Unknown filter profile operation type in operation: " + this);
      }
    }

    private CacheDistributionAdvisee findRegion(ClusterDistributionManager dm) {
      CacheDistributionAdvisee result = null;
      try {
        InternalCache cache = dm.getCache();
        if (cache != null) {
          LocalRegion lr = (LocalRegion) cache.getRegionByPathForProcessing(regionName);
          if (lr instanceof CacheDistributionAdvisee) {
            result = (CacheDistributionAdvisee) lr;
          }
        }
      } catch (CancelException ignore) {
        // nothing to do
      }
      return result;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.internal.serialization.DataSerializableFixedID#getDSFID()
     */
    @Override
    public int getDSFID() {
      return FILTER_PROFILE_UPDATE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(this.processorId);
      out.writeUTF(this.regionName);
      out.writeShort(this.opType.ordinal());
      out.writeBoolean(this.updatesAsInvalidates);
      out.writeLong(this.profileVersion);

      if (isCqOp(this.opType)) {
        // For CQ info.
        // Write Server CQ Name.
        out.writeUTF(((ServerCQ) this.cq).getServerCqName());
        if (this.opType == operationType.REGISTER_CQ || this.opType == operationType.SET_CQ_STATE) {
          InternalDataSerializer.invokeToData((ServerCQ) this.cq, out);
        }
      } else {
        // For interest list.
        out.writeLong(this.clientID);
        context.getSerializer().writeObject(this.interest, out);
      }
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.processorId = in.readInt();
      this.regionName = in.readUTF();
      this.opType = operationType.values()[in.readShort()];
      this.updatesAsInvalidates = in.readBoolean();
      this.profileVersion = in.readLong();
      if (isCqOp(this.opType)) {
        this.serverCqName = in.readUTF();
        if (this.opType == operationType.REGISTER_CQ || this.opType == operationType.SET_CQ_STATE) {
          this.cq = CqServiceProvider.readCq(in);
        }
      } else {
        this.clientID = in.readLong();
        this.interest = context.getDeserializer().readObject(in);
      }
    }

    @Override
    public String toString() {
      return this.getShortClassName() + "(processorId=" + this.processorId + "; region="
          + this.regionName + "; operation=" + this.opType + "; clientID=" + this.clientID
          + "; profileVersion=" + this.profileVersion
          + (isCqOp(this.opType) ? ("; CqName=" + this.serverCqName) : "") + ")";
    }
  }


  class IDMap {
    long nextID = 1;
    Map<Object, Long> realIDs = new ConcurrentHashMap<Object, Long>();
    Map<Long, Object> wireIDs = new ConcurrentHashMap<Long, Object>();
    boolean hasLongID;

    synchronized boolean hasWireID(Object realId) {
      return this.realIDs.containsKey(realId);
    }

    /** return the on-wire routing identifier for the given ID */
    Long getWireID(Object realId) {
      Long result = this.realIDs.get(realId);
      if (result == null) {
        synchronized (this) {
          result = this.realIDs.get(realId);
          if (result == null) {
            if (nextID == Integer.MAX_VALUE) {
              this.hasLongID = true;
            }
            result = nextID++;
            this.realIDs.put(realId, result);
            this.wireIDs.put(result, realId);
          }
        }
        if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER_VERBOSE)) {
          logger.trace(LogMarker.BRIDGE_SERVER_VERBOSE, "Profile for {} mapped {} to {}",
              region.getFullPath(), realId, result);
        }
      }
      return result;
    }

    /** return the client or durable queue id for the given on-wire identifier */
    Object getRealID(Long wireID) {
      return wireIDs.get(wireID);
    }


    /**
     * given a collection of on-wire identifiers, this returns a set of the real identifiers (e.g.,
     * client IDs or durable queue IDs)
     *
     * @param integerIDs the integer ids
     * @return the translated identifiers
     */
    public Set getRealIDs(Collection integerIDs) {
      if (integerIDs.size() == 0) {
        return Collections.emptySet();
      }
      Set result = new HashSet(integerIDs.size());
      Map<Long, Object> wids = wireIDs;
      for (Object id : integerIDs) {
        Object realID = wids.get(id);
        if (realID != null) {
          result.add(realID);
        }
      }
      return result;
    }

    /**
     * remove the mapping for the given internal ID
     */
    void removeIDMapping(Long mappedId) {
      Object clientId = this.wireIDs.remove(mappedId);
      if (clientId != null) {
        this.realIDs.remove(clientId);
      }
    }

    /**
     * remove the mapping for the given proxy ID
     */
    void removeIDMapping(Object clientId) {
      Long mappedId = this.realIDs.remove(clientId);
      if (mappedId != null) {
        this.wireIDs.remove(mappedId);
      }
    }
  }

  /**
   * Returns true if the client is interested in all keys.
   *
   * @param id client identifier.
   * @return true if client is interested in all keys.
   */
  public boolean isInterestedInAllKeys(Object id) {
    if (!clientMap.hasWireID(id)) {
      return false;
    }
    return this.getAllKeyClients().contains(clientMap.getWireID(id));
  }

  /**
   * Returns true if the client is interested in all keys, for which updates are sent as
   * invalidates.
   *
   * @param id client identifier
   * @return true if client is interested in all keys.
   */
  public boolean isInterestedInAllKeysInv(Object id) {
    if (!clientMap.hasWireID(id)) {
      return false;
    }
    return this.getAllKeyClientsInv().contains(clientMap.getWireID(id));
  }

  /**
   * Returns the set of client interested keys.
   *
   * @param id client identifier
   * @return client interested keys.
   */
  public Set getKeysOfInterest(Object id) {
    if (!clientMap.hasWireID(id)) {
      return null;
    }
    return this.getKeysOfInterest().get(clientMap.getWireID(id));
  }

  public int getKeysOfInterestSize() {
    return this.getKeysOfInterest().size();
  }

  /**
   * Returns the set of client interested keys for which updates are sent as invalidates.
   *
   * @param id client identifier
   * @return client interested keys.
   */
  public Set getKeysOfInterestInv(Object id) {
    if (!clientMap.hasWireID(id)) {
      return null;
    }
    return this.getKeysOfInterestInv().get(clientMap.getWireID(id));
  }

  public int getKeysOfInterestInvSize() {
    return this.getKeysOfInterestInv().size();
  }

  /**
   * Returns the set of client interested patterns.
   *
   * @param id client identifier
   * @return client interested patterns.
   */
  public Set getPatternsOfInterest(Object id) {
    if (!clientMap.hasWireID(id)) {
      return null;
    }
    Map patterns = this.getPatternsOfInterest().get(clientMap.getWireID(id));
    if (patterns != null) {
      return new HashSet(patterns.keySet());
    }
    return null;
  }

  public int getPatternsOfInterestSize() {
    return this.getPatternsOfInterest().size();
  }

  /**
   * Returns the set of client interested patterns for which updates are sent as invalidates.
   *
   * @param id client identifier
   * @return client interested patterns.
   */
  public Set getPatternsOfInterestInv(Object id) {
    if (!clientMap.hasWireID(id)) {
      return null;
    }
    Map interests = this.getPatternsOfInterestInv().get(clientMap.getWireID(id));
    if (interests != null) {
      return new HashSet(interests.keySet());
    }
    return null;
  }

  public int getPatternsOfInterestInvSize() {
    return this.getPatternsOfInterestInv().size();
  }

  /**
   * Returns the set of client interested filters.
   *
   * @param id client identifier
   * @return client interested filters.
   */
  public Set getFiltersOfInterest(Object id) {
    if (!clientMap.hasWireID(id)) {
      return null;
    }
    Map interests = this.getFiltersOfInterest().get(clientMap.getWireID(id));
    if (interests != null) {
      return new HashSet(interests.keySet());
    }
    return null;
  }

  /**
   * Returns the set of client interested filters for which updates are sent as invalidates.
   *
   * @param id client identifier
   * @return client interested filters.
   */
  public Set getFiltersOfInterestInv(Object id) {
    if (!clientMap.hasWireID(id)) {
      return null;
    }
    Map interests = this.getFiltersOfInterestInv().get(clientMap.getWireID(id));
    if (interests != null) {
      return new HashSet(interests.keySet());
    }
    return null;
  }

  @MutableForTesting
  public static TestHook testHook = null;

  /** Test Hook */
  public interface TestHook {
    void await();

    void release();
  }

  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }
}
