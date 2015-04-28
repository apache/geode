/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

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

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheEvent;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.internal.CqStateImpl;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;
import com.gemstone.gemfire.cache.query.internal.cq.CqServiceProvider;
import com.gemstone.gemfire.cache.query.internal.cq.ServerCQ;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.ClassLoadUtil;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.CacheDistributionAdvisor.CacheProfile;
import com.gemstone.gemfire.internal.cache.DistributedPutAllOperation.PutAllEntryData;
import com.gemstone.gemfire.internal.cache.DistributedRemoveAllOperation.RemoveAllEntryData;
import com.gemstone.gemfire.internal.cache.FilterRoutingInfo.FilterInfo;
import com.gemstone.gemfire.internal.cache.tier.InterestType;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.UnregisterAllInterest;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * FilterProfile represents a distributed system member and is used for
 * two purposes: processing client-bound events, and providing information
 * for profile exchanges.
 * 
 * FilterProfiles represent client IDs, including durable Queue IDs, with
 * long integers.  This reduces the size of routing information when sent
 * over the network.
 * 
 * @since 6.5
 * @author bruce
 */
public class FilterProfile implements DataSerializableFixedID {
  private static final Logger logger = LogService.getLogger();
  
  /** enumeration of distributed profile operations */
  static enum operationType {
    REGISTER_KEY, REGISTER_KEYS, REGISTER_PATTERN, REGISTER_FILTER,
    UNREGISTER_KEY, UNREGISTER_KEYS, UNREGISTER_PATTERN, UNREGISTER_FILTER,
    CLEAR,
    HAS_CQ, REGISTER_CQ, CLOSE_CQ, STOP_CQ, SET_CQ_STATE
  }
  /**
   * these booleans tell whether the associated operationType pertains to CQs
   * or not
   */
  static boolean[] isCQOperation = {
    false, false, false, false,
    false, false, false, false,
    false,
    true, true, true, true, true
  };
  
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
   * this variable is used to ensure that the state of all of the interest
   * variables is consistent with other threads.  See
   * http://www.cs.umd.edu/~pugh/java/memoryModel/jsr-133-faq.html#volatile
   */
  @SuppressWarnings("unused")
  private volatile Object volatileBarrier = null;
  
  /**
   * The keys in which clients are interested. This is a map keyed on client id,
   * with a HashSet of the interested keys as the values.
   * 
   * This map is never modified in place. Updaters must synchronize via
   * {@link #interestListLock}.
   */
  private  Map<Object, Set> keysOfInterest;
  
  private  Map<Object, Set> keysOfInterestInv;

  /**
   * The patterns in which clients are interested. This is a map keyed on
   * client id, with a HashMap (key name to compiled pattern) as the values.
   *
   * This map is never modified in place. Updaters must synchronize via
   * {@link #interestListLock}.
   */
  private  Map<Object, Map<Object, Pattern>> patternsOfInterest;

  private  Map<Object, Map<Object, Pattern>> patternsOfInterestInv;
  
 /**
   * The filtering classes in which clients are interested. This is a map
   * keyed on client id, with a HashMap (key name to {@link InterestFilter})
   * as the values.
   *
   * This map is never modified in place. Updaters must synchronize via
   * {@link #interestListLock}.
   */
  private  Map<Object, Map> filtersOfInterest;

  private  Map<Object, Map> filtersOfInterestInv;

  /**
   * Set of clients that we have ALL_KEYS interest for and who want updates
   */
  private Set<Long> allKeyClients = null;
  
  /**
   * Set of clients that we have ALL_KEYS interest for and who want invalidations
   */
  private  Set<Long> allKeyClientsInv = null;
  
  /**
   * An object used for synchronizing the interest lists
   */
  private transient final Object interestListLock = new Object();
  
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
  private volatile Map cqs = Collections.EMPTY_MAP;

  /* the ID of the member that this profile describes */
  private DistributedMember memberID;
  
  /**
   * since client identifiers can be long, we use a mapping for on-wire
   * operations
   */
  IDMap clientMap;
  
  /**
   * since CQ identifiers can be long, we use a mapping for on-wire operations
   */
  IDMap cqMap;
  
  /**
   * Queues the Filter Profile messages that are received during profile 
   * exchange. 
   */
  private volatile Map<InternalDistributedMember, LinkedList<OperationMessage>> filterProfileMsgQueue = new HashMap();
  
  public FilterProfile() {
    cqCount = new AtomicInteger();
  }
  
  /**
   * used for instantiation of a profile associated with a region and
   * not describing region filters in a different process.  Do not use
   * this method when instantiating profiles to store in distribution
   * advisor profiles.
   * @param r
   */
  public FilterProfile(LocalRegion r) {
    this.region = r;
    this.isLocalProfile = true;
    this.memberID = region.getMyId();
    this.cqCount = new AtomicInteger();
    this.clientMap = new IDMap();
    this.cqMap = new IDMap();
    this.localProfile.hasCacheServer = (r.getGemFireCache().getCacheServers().size() > 0);
  }

  public static boolean isCqOp(operationType opType){
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
   * @param inputClientID 
   *          The identity of the interested client
   * @param interest
   *          The key in which to register interest
   * @param typeOfInterest the type of interest the client is registering
   * @param updatesAsInvalidates
   *          whether the client just wants invalidations
   * @return a set of the keys that were registered, which may be null
   */
  public Set registerClientInterest(Object inputClientID,
      Object interest, int typeOfInterest, boolean updatesAsInvalidates) {
    Set keysRegistered = null;
    operationType opType = null;
    
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long)inputClientID;
    } else {
      clientID = clientMap.getWireID(inputClientID);
    }
    synchronized(this.interestListLock) {
      switch (typeOfInterest) {
      case InterestType.KEY: {
        opType = operationType.REGISTER_KEY;
        Set oldInterestList;
        Map<Object, Set> koi = updatesAsInvalidates?
            this.getKeysOfInterestInv() : this.getKeysOfInterest();
        oldInterestList = koi.get(clientID);
        Set newInterestList = (oldInterestList == null)?
          new HashSet() : new HashSet(oldInterestList);
        newInterestList.add(interest);
        Map<Object,Set> newMap = new HashMap(koi);
        newMap.put(clientID, newInterestList);
        if (updatesAsInvalidates) {
          this.setKeysOfInterestInv(newMap);
        } else {
          this.setKeysOfInterest(newMap);
        }
        // Create a set of keys to pass to any listeners. 
        // There currently is no check to see if the key already exists. 
        keysRegistered = new HashSet(); 
        keysRegistered.add(interest);
        break;
      }

      case InterestType.REGULAR_EXPRESSION: {
        keysRegistered = new HashSet(); 
        opType = operationType.REGISTER_PATTERN;
        if (((String)interest).equals(".*")) {
          // ALL_KEYS
          Set akc = updatesAsInvalidates? this.getAllKeyClientsInv() : this.getAllKeyClients();
          akc = new HashSet(akc);
          if (akc.add(clientID)) {
            keysRegistered.add(interest);
            if (updatesAsInvalidates) {
              this.setAllKeyClientsInv(akc);
            } else {
              this.setAllKeyClients(akc);
            }
          }
        } else {
          Pattern pattern = Pattern.compile((String) interest);
          Map<Object, Map<Object, Pattern>> pats = updatesAsInvalidates?
              this.getPatternsOfInterestInv() : this.getPatternsOfInterest();
          Map<Object, Pattern> oldInterestMap = pats.get(clientID);
          Map<Object, Pattern> newInterestMap =(oldInterestMap == null)?
            new HashMap() : new HashMap(oldInterestMap);
          Pattern oldPattern = newInterestMap.put(interest, pattern);
          if (oldPattern == null) {
            // If the pattern didn't exist, add it to the set of keys to pass to any listeners. 
            keysRegistered.add(interest);
          }
          Map<Object, Map<Object, Pattern>> newMap = new HashMap(pats);
          newMap.put(clientID, newInterestMap);
          if(updatesAsInvalidates) {
            this.setPatternsOfInterestInv(newMap);
          } else {
            this.setPatternsOfInterest(newMap);
          }
        }
        break;
      }

      case InterestType.FILTER_CLASS: {
        // get instance of the filter
        Class filterClass;
        InterestFilter filter;
        try {
          filterClass = ClassLoadUtil.classFromName((String)interest);
          filter = (InterestFilter)filterClass.newInstance();
        }
        catch (ClassNotFoundException cnfe) {
          throw new RuntimeException(LocalizedStrings.CacheClientProxy_CLASS_0_NOT_FOUND_IN_CLASSPATH.toLocalizedString(interest), cnfe);
        }
        catch (Exception e) {
          throw new RuntimeException(LocalizedStrings.CacheClientProxy_CLASS_0_COULD_NOT_BE_INSTANTIATED.toLocalizedString(interest), e);
        }
        opType = operationType.REGISTER_FILTER;
        Map<Object, Map>filts = updatesAsInvalidates?
            this.getFiltersOfInterestInv() : this.getFiltersOfInterest();
        Map oldInterestMap = filts.get(clientID);
        Map newInterestMap = (oldInterestMap == null)?
          new HashMap() : new HashMap(oldInterestMap);
        newInterestMap.put(interest, filter);
        HashMap newMap = new HashMap(filts);
        newMap.put(clientID, newInterestMap);
        if (updatesAsInvalidates) {
          this.setFiltersOfInterestInv(newMap);
        } else {
          this.setFiltersOfInterest(newMap);
        }
        break;
      }
      default:
        throw new InternalGemFireError(LocalizedStrings.CacheClientProxy_UNKNOWN_INTEREST_TYPE.toLocalizedString());
      } // switch
      if (this.isLocalProfile && opType != null) {
        sendProfileOperation(clientID, opType, interest, updatesAsInvalidates);
      }
    } // synchronized
    return keysRegistered;
  }

  /**
   * Unregisters a client's interest
   *
   * @param inputClientID
   *          The identity of the client that is losing interest
   * @param interest
   *          The key in which to unregister interest
   * @param interestType the type of uninterest
   * @return the keys unregistered, which may be null
   */
  public Set unregisterClientInterest(Object inputClientID,
      Object interest, int interestType) {
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long)inputClientID;
    } else {
      Map<Object, Long> cids = clientMap.realIDs; // read
      clientID = cids.get(inputClientID);
      if (clientID == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("region profile unable to find '{}' for unregistration.  Probably means there is no durable queue.", inputClientID);
        }
        return null;
      }
    }
    Set keysUnregistered = null;
    operationType opType = null;
    synchronized (this.interestListLock) {
      switch (interestType) {
      case InterestType.KEY: {
        opType = operationType.UNREGISTER_KEY;
        if (interest == UnregisterAllInterest.singleton()) {
          clearInterestFor(inputClientID);
          // Bruce: this code removed since clearInterestFor() is more comprehensive
//          if (this.keysOfInterest.get(clientID) != null) {
//            Map newMap = new HashMap(this.keysOfInterest);
//            Set removed = (Set)newMap.remove(clientID);
//            // If something is removed, create a set of keys to pass
//            // to any listeners
//            if (removed != null) {
//              keysUnregistered = new HashSet();
//              keysUnregistered.addAll(removed);
//            } 
//            this.keysOfInterest = newMap;
//          }
//          if (this.keysOfInterestInv.get(clientID) != null) {
//            Map newMap = new HashMap(this.keysOfInterestInv);
//            Set removed = (Set)newMap.remove(clientID);
//            // If something is removed, create a set of keys to pass
//            // to any listeners
//            if (removed != null) {
//              if (keysUnregistered == null) keysUnregistered = new HashSet();
//              keysUnregistered.addAll(removed);
//            } 
//            this.keysOfInterestInv = newMap;
//          }
          break;
        }
        Set oldInterestList = this.getKeysOfInterest().get(clientID);
        if (oldInterestList != null) {
          Set newInterestList = new HashSet(oldInterestList);
          boolean removed = newInterestList.remove(interest);
          if (removed) {
            keysUnregistered = new HashSet();
            keysUnregistered.add(interest);
          } 
          Map newMap = new HashMap(this.getKeysOfInterest());
          if (newInterestList.size() > 0) {
            newMap.put(clientID, newInterestList);
          } else {
            newMap.remove(clientID);
          }
          this.setKeysOfInterest(newMap);
        }
        oldInterestList = this.getKeysOfInterestInv().get(clientID);
        if (oldInterestList != null) {
          Set newInterestList = new HashSet(oldInterestList);
          boolean removed = newInterestList.remove(interest);
          if (removed) {
            keysUnregistered = new HashSet();
            keysUnregistered.add(interest);
          } 
          Map newMap = new HashMap(this.getKeysOfInterestInv());
          if (newInterestList.size() > 0) {
            newMap.put(clientID, newInterestList);
          } else {
            newMap.remove(clientID);
          }
          this.setKeysOfInterestInv(newMap);
        }
        break;
      }
      case InterestType.REGULAR_EXPRESSION: {
        opType = operationType.UNREGISTER_PATTERN;
        if (interest == UnregisterAllInterest.singleton()) {
          if (this.getPatternsOfInterest().get(clientID) != null) {
            HashMap newMap = new HashMap(this.getPatternsOfInterest());
            Map removed = (Map)newMap.remove(clientID);
            if (removed != null) {
              keysUnregistered = new HashSet();
              keysUnregistered.addAll(removed.keySet());
            } 
            this.setPatternsOfInterest(newMap);
          }
          if (this.getPatternsOfInterestInv().get(clientID) != null) {
            HashMap newMap = new HashMap(this.getPatternsOfInterestInv());
            Map removed = (Map)newMap.remove(clientID);
            if (removed != null) {
              if (keysUnregistered == null) keysUnregistered = new HashSet();
              keysUnregistered.addAll(removed.keySet());
            } 
            this.setPatternsOfInterestInv(newMap);
          }
          Set oldSet = this.getAllKeyClients();
          if (!oldSet.isEmpty()) {
            Set newSet;
            newSet = new HashSet(oldSet);
            if (newSet.remove(clientID)) {
              if (newSet.isEmpty()) {
                newSet = null;
              }
              this.setAllKeyClients(newSet);
              if (keysUnregistered == null) {
                // keysUnregistered won't be null if somebody has registered
                // interest in a specific key, then in '.*'.
                keysUnregistered = new HashSet();
              }
              keysUnregistered.add(".*"); 
            }
          }
          oldSet = this.getAllKeyClientsInv();
          if (oldSet != null) {
            Set newSet;
            newSet = new HashSet(oldSet);
            if (newSet.remove(clientID)) {
              if (newSet.isEmpty()) {
                newSet = null;
              }
              this.setAllKeyClientsInv(newSet);
              if (keysUnregistered == null) {
                // keysUnregistered won't be null if somebody has registered
                // interest in a specific key, then in '.*'.
                keysUnregistered = new HashSet();
              }
              keysUnregistered.add(".*"); 
            }
          }
        }
        else if (((String)interest).equals(".*")) { // ALL_KEYS
          Set oldSet = this.getAllKeyClients();
          if (oldSet != null) {
            Set newSet;
            newSet = new HashSet(oldSet);
            if (newSet.remove(clientID)) {
              if (newSet.isEmpty()) {
                newSet = null;
              }
              this.setAllKeyClients(newSet);
              // Since something was removed, create a set of keys to pass to any
              // listeners
              keysUnregistered = new HashSet();
              keysUnregistered.add(interest); 
            }
          }
          oldSet = this.getAllKeyClientsInv();
          if (oldSet != null) {
            Set newSet;
            newSet = new HashSet(oldSet);
            if (newSet.remove(clientID)) {
              if (newSet.isEmpty()) {
                newSet = null;
              }
              this.setAllKeyClientsInv(newSet);
              // Since something was removed, create a set of keys to pass to any
              // listeners
              keysUnregistered = new HashSet();
              keysUnregistered.add(interest); 
            }
          }
        }
        else {
          Map oldInterestMap = this.getPatternsOfInterest().get(clientID);
          if (oldInterestMap != null) {
            Map newInterestMap = new HashMap(oldInterestMap);
            Object obj = newInterestMap.remove(interest);
            if (obj != null) {
              // Since something was removed, create a set of keys to pass to any
              // listeners
              keysUnregistered = new HashSet();
              keysUnregistered.add(interest); 
            }
            Map newMap = new HashMap(this.getPatternsOfInterest());
            if (newInterestMap.size() > 0)
              newMap.put(clientID, newInterestMap);
            else
              newMap.remove(clientID);
            this.setPatternsOfInterest(newMap);
          }
          oldInterestMap = this.getPatternsOfInterestInv().get(clientID);
          if (oldInterestMap != null) {
            Map newInterestMap = new HashMap(oldInterestMap);
            Object obj = newInterestMap.remove(interest);
            if (obj != null) {
              // Since something was removed, create a set of keys to pass to any
              // listeners
              keysUnregistered = new HashSet();
              keysUnregistered.add(interest); 
            }
            Map newMap = new HashMap(this.getPatternsOfInterestInv());
            if (newInterestMap.size() > 0)
              newMap.put(clientID, newInterestMap);
            else
              newMap.remove(clientID);
            this.setPatternsOfInterestInv(newMap);
          }
        }
        break;
      }
      case InterestType.FILTER_CLASS: {
        opType = operationType.UNREGISTER_FILTER;
        if (interest == UnregisterAllInterest.singleton()) {
          if (this.getFiltersOfInterest().get(clientID) != null) {
            Map newMap = new HashMap(this.getFiltersOfInterest());
            newMap.remove(clientID);
            this.setFiltersOfInterest(newMap);
          }
          if (this.getFiltersOfInterestInv().get(clientID) != null) {
            Map newMap = new HashMap(this.getFiltersOfInterestInv());
            newMap.remove(clientID);
            this.setFiltersOfInterestInv(newMap);
          }
          break;
        }
        Map oldInterestMap = this.getFiltersOfInterest().get(clientID);
        if (oldInterestMap != null) {
          Map newInterestMap = new HashMap(oldInterestMap);
          newInterestMap.remove(interest);
          Map newMap = new HashMap(this.getFiltersOfInterest());
          if (newInterestMap.size() > 0) {
            newMap.put(clientID, newInterestMap);
          } else {
            newMap.remove(clientID);
          }
          this.setFiltersOfInterest(newMap);
        }
        oldInterestMap = this.getFiltersOfInterestInv().get(clientID);
        if (oldInterestMap != null) {
          Map newInterestMap = new HashMap(oldInterestMap);
          newInterestMap.remove(interest);
          Map newMap = new HashMap(this.getFiltersOfInterestInv());
          if (newInterestMap.size() > 0) {
            newMap.put(clientID, newInterestMap);
          } else {
            newMap.remove(clientID);
          }
          this.setFiltersOfInterestInv(newMap);
        }
        break;
      }
      default:
        throw new InternalGemFireError(LocalizedStrings.CacheClientProxy_BAD_INTEREST_TYPE.toLocalizedString());
      }
      if (this.region != null && this.isLocalProfile) {
        sendProfileOperation(clientID, opType, interest, false);
      }
    } // synchronized
    return keysUnregistered;
  }

  /**
   * Registers interest in a set of keys for a client
   *
   * @param inputClientID
   * @param keys
   *          The list of keys in which to register interest
   * @param updatesAsInvalidates whether to send invalidations instead of updates
   * @return the registered keys
   */
  public Set registerClientInterestList(Object inputClientID,
      List keys, boolean updatesAsInvalidates) {
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long)inputClientID;
    } else {
      clientID = clientMap.getWireID(inputClientID);
    }
    Set keysRegistered = new HashSet();
    synchronized (interestListLock) {
      Map<Object, Set> koi = updatesAsInvalidates?
          this.getKeysOfInterestInv() : this.getKeysOfInterest();
      Set oldInterestList = koi.get(clientID);
      Set newInterestList = (oldInterestList == null)?
        new HashSet() : new HashSet(oldInterestList);
      for (Object key: keys) { 
        if (newInterestList.add(key)) { 
          keysRegistered.add(key); 
        } 
      } 
      Map newMap = new HashMap(koi);
      newMap.put(clientID, newInterestList);
      if (updatesAsInvalidates) {
        this.setKeysOfInterestInv(newMap);
      } else {
        this.setKeysOfInterest(newMap);
      }
      if (this.region != null && this.isLocalProfile) {
        sendProfileOperation(clientID, operationType.REGISTER_KEYS, keys, updatesAsInvalidates);
      }
    } // synchronized
    return keysRegistered;
  }
  
  /**
   * Unregisters interest in given keys for the given client
   *
   * @param inputClientID
   *          The fully-qualified name of the region in which to unregister
   *          interest
   * @param keys
   *          The list of keys in which to unregister interest
   * @return the unregistered keys
   */
  public Set unregisterClientInterestList(Object inputClientID,
      List keys) {
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long)inputClientID;
    } else {
      clientID = clientMap.getWireID(inputClientID);
    }
    Set keysUnregistered = new HashSet(); 
    synchronized (interestListLock) {
      Set oldInterestList = this.getKeysOfInterest().get(clientID);
      if (oldInterestList != null) {
        Set newInterestList = new HashSet(oldInterestList);
        for (Iterator i = keys.iterator(); i.hasNext();) { 
          Object keyOfInterest = i.next(); 
          if (newInterestList.remove(keyOfInterest)) { 
            keysUnregistered.add(keyOfInterest); 
          } 
        }       
        Map newMap = new HashMap(this.getKeysOfInterest());
        if (newInterestList.size() > 0)
          newMap.put(clientID, newInterestList);
        else
          newMap.remove(clientID);
        this.setKeysOfInterest(newMap);
      }
      oldInterestList = this.getKeysOfInterestInv().get(clientID);
      if (oldInterestList != null) {
        Set newInterestList = new HashSet(oldInterestList);
        for (Iterator i = keys.iterator(); i.hasNext();) { 
          Object keyOfInterest = i.next(); 
          if (newInterestList.remove(keyOfInterest)) { 
            keysUnregistered.add(keyOfInterest); 
          } 
        }       
        Map newMap = new HashMap(this.getKeysOfInterestInv());
        if (newInterestList.size() > 0)
          newMap.put(clientID, newInterestList);
        else
          newMap.remove(clientID);
        this.setKeysOfInterestInv(newMap);
      }
      if (this.region != null && this.isLocalProfile) {
        sendProfileOperation(clientID, operationType.UNREGISTER_KEYS, keys, false);
      }
    } // synchronized
    return keysUnregistered;
  }
  
  public Set getKeysOfInterestFor(Object inputClientID) {
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long)inputClientID;
    } else {
      clientID = clientMap.getWireID(inputClientID);
    }
    volatileBarrier();
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
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long)inputClientID;
    } else {
      clientID = clientMap.getWireID(inputClientID);
    }
    volatileBarrier();
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
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long)inputClientID;
    } else {
      clientID = clientMap.getWireID(inputClientID);
    }
    volatileBarrier();
    if (wantInvalidations) {
      return this.getKeysOfInterestInv().containsKey(clientID);
    }
    return this.getKeysOfInterestInv().containsKey(clientID);
  }
  
  public boolean hasAllKeysInterestFor(Object inputClientID) {
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long)inputClientID;
    } else {
      clientID = clientMap.getWireID(inputClientID);
    }
    volatileBarrier();
    return hasAllKeysInterestFor(clientID, false) ||
      hasAllKeysInterestFor(clientID, true);
  }

  public boolean hasAllKeysInterestFor(Object inputClientID, boolean wantInvalidations) {
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long)inputClientID;
    } else {
      clientID = clientMap.getWireID(inputClientID);
    }
    if (wantInvalidations) {
      volatileBarrier();
      return getAllKeyClientsInv().contains(clientID);
    }
    return getAllKeyClients().contains(clientID);
  }

  public boolean hasRegexInterestFor(Object inputClientID, boolean wantInvalidations) {
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long)inputClientID;
    } else {
      clientID = clientMap.getWireID(inputClientID);
    }
    volatileBarrier();
    if (wantInvalidations) {
      return (this.getPatternsOfInterestInv().containsKey(clientID));
    }
    return (this.getPatternsOfInterest().containsKey(clientID));
  }
  
  public boolean hasFilterInterestFor(Object inputClientID, boolean wantInvalidations) {
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long)inputClientID;
    } else {
      clientID = clientMap.getWireID(inputClientID);
    }
    volatileBarrier();
    if (wantInvalidations) {
      return this.getFiltersOfInterestInv().containsKey(clientID);
    }
    return this.getFiltersOfInterestInv().containsKey(clientID);
  }
  
 
  /** determines whether there is any remaining interest for the given identifier */
  public boolean hasInterestFor(Object inputClientID) {
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long)inputClientID;
    } else {
      Map<Object, Long> cids = clientMap.realIDs; // read
      clientID = cids.get(inputClientID);
      if (clientID == null) {
        return false;
      }
    }
    return
      this.hasAllKeysInterestFor(clientID, true)  ||
      this.hasKeysOfInterestFor(clientID, true)   ||
      this.hasRegexInterestFor(clientID, true)    ||
      this.hasFilterInterestFor(clientID, true)   ||
      this.hasKeysOfInterestFor(clientID, false)  ||
      this.hasRegexInterestFor(clientID, false)   ||
      this.hasAllKeysInterestFor(clientID, false) ||
      this.hasFilterInterestFor(clientID, false);
  }

  /*
   * Returns whether this interest list has any keys, patterns or filters of
   * interest. It answers the question: Are any clients being notified because
   * of this interest list? @return whether this interest list has any keys,
   * patterns or filters of interest
   */
  public boolean hasInterest() {
    return
      (!this.getAllKeyClients().isEmpty())         ||
      (!this.getAllKeyClientsInv().isEmpty())      ||
      (!this.getKeysOfInterest().isEmpty())        ||
      (!this.getPatternsOfInterest().isEmpty())    ||
      (!this.getFiltersOfInterest().isEmpty())     ||
      (!this.getKeysOfInterestInv().isEmpty())     ||
      (!this.getPatternsOfInterestInv().isEmpty()) ||
      (!this.getFiltersOfInterestInv().isEmpty());
  }

  /** removes all interest for the given identifier */
  public void clearInterestFor(Object inputClientID) {
    Long clientID;
    if (inputClientID instanceof Long) {
      clientID = (Long)inputClientID;
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
          akc = new HashSet(akc);
          akc.remove(clientID);
          this.setAllKeyClients(akc);
        }
      }
      {
        Set akci = this.getAllKeyClientsInv();
        if (akci.contains(clientID)) {
          akci = new HashSet(akci);
          akci.remove(clientID);
          this.setAllKeyClientsInv(akci);
        }
      }
      {
        Map<Object, Set> keys = this.getKeysOfInterest();
        if (keys.containsKey(clientID)) {
          Map newkeys = new HashMap(keys);
          newkeys.remove(clientID);
          this.setKeysOfInterest(newkeys);
        }
      }
      {
        Map<Object, Set> keys = this.getKeysOfInterestInv();
        if (keys.containsKey(clientID)) {
          Map newkeys = new HashMap(keys);
          newkeys.remove(clientID);
          this.setKeysOfInterestInv(newkeys);
        }
      }
      {
        Map<Object, Map<Object, Pattern>> pats = this.getPatternsOfInterest();
        if (pats.containsKey(clientID)) {
          Map newpats = new HashMap(pats);
          newpats.remove(clientID);
          this.setPatternsOfInterest(newpats);
        }
      }
      {
        Map<Object, Map<Object, Pattern>> pats = this.getPatternsOfInterestInv();
        if (pats.containsKey(clientID)) {
          Map newpats = new HashMap(pats);
          newpats.remove(clientID);
          this.setPatternsOfInterestInv(newpats);
        }
      }
      {
        Map<Object, Map> filters = this.getFiltersOfInterest();
        if (filters.containsKey(clientID)) {
          Map newfilts = new HashMap(filters);
          newfilts.remove(clientID);
          this.setFiltersOfInterest(newfilts);
        }
      }
      {
        Map<Object, Map> filters = this.getFiltersOfInterestInv();
        if (filters.containsKey(clientID)) {
          Map newfilts = new HashMap(filters);
          newfilts.remove(clientID);
          this.setFiltersOfInterestInv(newfilts);
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
   * Obtains the number of CQs registered on the region.
   * Assumption: CQs are not duplicated among clients.
   *
   * @return int currently registered CQs of the region
   */
  public int getCqCount() {
    return this.cqCount.get();
  }

  public void incCqCount(){
    this.cqCount.getAndIncrement();
  }

  public void decCqCount(){
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
    return (ServerCQ)this.cqs.get(cqName);
  }
  
  public void registerCq(ServerCQ cq) {
    ensureCqID(cq);
    if (logger.isDebugEnabled()) {
      logger.debug("Adding CQ {} to this members FilterProfile.", cq.getServerCqName()); 
    }
    Map newCqs = new HashMap(this.cqs);
    newCqs.put(cq.getServerCqName(), cq);
    this.cqs = newCqs;
    this.incCqCount();
    
    //cq.setFilterID(cqMap.getWireID(cq.getServerCqName()));
    this.sendCQProfileOperation(operationType.REGISTER_CQ, cq);
  }

  public void stopCq(ServerCQ cq) {
    ensureCqID(cq);
    if (logger.isDebugEnabled()) {
      this.logger.debug("Stopping CQ {} on this members FilterProfile.", cq.getServerCqName()); 
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
  
  /**
   * adds a new CQ to this profile during a delta operation or deserialization
   * @param serverCqName the query objects' name
   * @param ServerCQ the new query object
   * @param addToCqMap whether to add the query to this.cqs
   */
  void processRegisterCq(String serverCqName, ServerCQ ServerCQ,
      boolean addToCqMap) {
    ServerCQ cq = (ServerCQ)ServerCQ;
    try {
      CqService cqService = GemFireCacheImpl.getInstance().getCqService();
      cqService.start();
      cq.setCqService(cqService);
      CqStateImpl cqState = (CqStateImpl)cq.getState();
      cq.setName(generateCqName(serverCqName));
      cq.registerCq(null, null, cqState.getState());    
    } catch (Exception ex){
      // Change it to Info level.
      if (logger.isDebugEnabled()) {
        logger.debug("Error while initializing the CQs with FilterProfile for CQ {}, Error : {}", serverCqName, ex.getMessage(), ex);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Adding CQ to remote members FilterProfile using name: {}", serverCqName);
    }
    if (addToCqMap) {
      Map newCqs = new HashMap(this.cqs);
      newCqs.put(serverCqName, cq);
      this.cqs = newCqs;
    }
    
    // The region's FilterProfile is accessed through CQ reference as the
    // region is not set on the FilterProfile created for the peer nodes.
    if (cq.getCqBaseRegion() != null ) {
      FilterProfile pf =  cq.getCqBaseRegion().getFilterProfile();
      if (pf != null) {
        pf.incCqCount();
      }
    }
  }
  
  public void processCloseCq(String serverCqName) {
    ServerCQ cq = (ServerCQ)this.cqs.get(serverCqName);
    if (cq != null){
      try {
        cq.close(false);
      } catch (Exception ex){
        if (logger.isDebugEnabled()) {
          logger.debug("Unable to close the CQ with the filterProfile, on region {} for CQ {}, Error : {}", this.region.getFullPath(),
              serverCqName, ex.getMessage(), ex);
        }
      }
      Map newCqs = new HashMap(cqs);
      newCqs.remove(serverCqName);
      this.cqs = newCqs;
      cq.getCqBaseRegion().getFilterProfile().decCqCount();
    }
  }
  
  public void processSetCqState(String serverCqName, ServerCQ ServerCQ) {
    ServerCQ cq = (ServerCQ)this.cqs.get(serverCqName);
    if (cq != null){
      CqStateImpl cqState = (CqStateImpl)ServerCQ.getState(); 
      cq.setCqState(cqState.getState());
    }
  }

  public void processStopCq(String serverCqName) {
    ServerCQ cq = (ServerCQ)this.cqs.get(serverCqName);
    if (cq != null){
      try {
        cq.stop();
      } catch (Exception ex){
        if (logger.isDebugEnabled()) {
          logger.debug("Unable to stop the CQ with the filterProfile, on region {} for CQ {}, Error : {}",
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
    Map newCqs = new HashMap(this.cqs);
    newCqs.remove(serverCqName);
    this.cqs = newCqs;
    if (this.cqMap != null) {
      this.cqMap.removeIDMapping(cq.getFilterID());
    }
    this.decCqCount();
    this.sendCQProfileOperation(operationType.CLOSE_CQ, cq);
  }
  
  void cleanupForClient(CacheClientNotifier ccn,
      ClientProxyMembershipID client) {
    Iterator cqIter = this.cqs.entrySet().iterator();
    while (cqIter.hasNext()) {
      Map.Entry cqEntry = (Map.Entry)cqIter.next();
      ServerCQ cq = (ServerCQ)cqEntry.getValue();
      ClientProxyMembershipID clientId = cq.getClientProxyId();
      if (clientId.equals(client)){
        try {
          cq.close(false);
        } catch (Exception ex) {
          if (logger.isDebugEnabled()) {
            logger.debug("Failed to remove CQ from the base region. CqName : {}", cq.getName());
          }
        }
        this.closeCq(cq);
      }
    }
  }
  
  /**
   * this will be get called when we remove other server profile from
   * region advisor.
   */
  void cleanUp() {
    Map tmpCq = this.cqs;
    
    if(tmpCq.size() > 0) {
      for(Object serverCqName: tmpCq.keySet()) {
        processCloseCq((String)serverCqName);
      }
    }
  }
  
  /**
   * Returns if old value is required for CQ processing or not.
   * In order to reduce the query processing time CQ caches the 
   * event keys its already seen, if the key is cached than the 
   * old value is not required.
   */
  public boolean entryRequiresOldValue(Object key) {
    if (this.hasCQs()){
      if (!CqServiceProvider.MAINTAIN_KEYS){
        return true;
      }
      Iterator cqIter = this.cqs.values().iterator();
      while (cqIter.hasNext()) {
        ServerCQ cq = (ServerCQ)cqIter.next();
        if (cq.isOldValueRequiredForQueryProcessing(key)) {
          return true;
        }
      }  
    }
    return false;
  }

  private void sendProfileOperation(Long clientID, operationType opType, Object interest,
      boolean updatesAsInvalidates) {
    if (this.region == null || !(this.region instanceof PartitionedRegion)) {
      return;
    }
    OperationMessage msg = new OperationMessage();
    msg.regionName = this.region.getFullPath();
    msg.clientID = clientID.longValue();
    msg.opType = opType;
    msg.interest = interest;  
    msg.updatesAsInvalidates = updatesAsInvalidates;
    sendFilterProfileOperation(msg);
  }
  

  private void sendFilterProfileOperation(OperationMessage msg) {
    Set recipients = ((CacheDistributionAdvisee)this.region).getDistributionAdvisor()
      .adviseProfileUpdate();
    msg.setRecipients(recipients);
    ReplyProcessor21 rp = new ReplyProcessor21(this.region.getDistributionManager(), recipients);
    msg.processorId = rp.getProcessorId();
    this.region.getDistributionManager().putOutgoing(msg);
    try {
      rp.waitForReplies();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  private void sendCQProfileOperation(operationType opType, ServerCQ cq) {
    // we only need to distribute for PRs.  Other regions do local filter processing
    if ( ! (this.region instanceof PartitionedRegion) ) {
      // note that we do not need to update requiresOldValueInEvents because
      // that flag is only used during region initialization.  Otherwise we
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
  
  static final Profile[] NO_PROFILES = new Profile[0];
  private final CacheProfile localProfile = new CacheProfile(this);
  private final Profile[] localProfileArray = new Profile[]{localProfile};
  
  
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
   * Compute the full routing information for the given set of peers.  This will
   * not include local routing information from interest processing.  That
   * is done by getFilterRoutingInfoPart2 */
  public FilterRoutingInfo getFilterRoutingInfoPart1(CacheEvent event, Profile[] peerProfiles, Set cacheOpRecipients) {
    // early out if there are no cache servers in the system
    boolean anyServers = false;
    for (int i=0; i<peerProfiles.length; i++) {
      if (((CacheProfile)peerProfiles[i]).hasCacheServer) {
        anyServers = true;
        break;
      }
    }
    if (!anyServers && !this.localProfile.hasCacheServer) {
      return null;
    }

    volatileBarrier();
    
    FilterRoutingInfo frInfo = null;

    CqService cqService = getCqService(event.getRegion());
    if (cqService.isRunning()) {
      frInfo = new FilterRoutingInfo();
      // bug #50809 - local routing for transactional ops must be done here
      // because the event isn't available later and we lose the old value for the entry
      final boolean processLocalProfile = event.getOperation().isEntry() && ((EntryEventImpl)event).getTransactionId() != null;
      fillInCQRoutingInfo(event, processLocalProfile, peerProfiles, frInfo);
    }
    
    // Process InterestList.
//    return fillInInterestRoutingInfo(event, peerProfiles, frInfo, cacheOpRecipients);
    frInfo = fillInInterestRoutingInfo(event, peerProfiles, frInfo, cacheOpRecipients);
    if (frInfo == null || !frInfo.hasMemberWithFilterInfo()) {
      return null;
    } else {
      return frInfo;
    }
  }
  

  
  /**
   * get local routing information
   * @param part1Info routing information for peers, if any
   * @param event the event to process
   * @return routing information for clients connected to this server
   */
  public FilterRoutingInfo getFilterRoutingInfoPart2(FilterRoutingInfo part1Info,
      CacheEvent event) {
    volatileBarrier();
    FilterRoutingInfo result = part1Info;
    if (localProfile.hasCacheServer) {
      // bug #45520 - CQ events arriving out of order causes result set
      // inconsistency, so don't compute routings for events in conflict
      boolean isInConflict = event.getOperation().isEntry() &&
          ((EntryEventImpl)event).isConcurrencyConflict();
      CqService cqService = getCqService(event.getRegion());
      if (!isInConflict && cqService.isRunning() && this.region != null /*&& !(
          this.region.isUsedForPartitionedRegionBucket() ||   // partitioned region CQ 
          this.region instanceof PartitionedRegion)*/) {        // processing is done in part 1
        if (result == null) {
          result = new FilterRoutingInfo();
        }
        if (logger.isDebugEnabled()) {
          logger.debug("getting local cq matches for {}", event);
        }
        fillInCQRoutingInfo(event, true, NO_PROFILES, result);
      }
      result = fillInInterestRoutingInfo(event, localProfileArray, result, Collections.EMPTY_SET);
    }
    return result;
  }
  
  /**
   * get continuous query routing information
   * @param event the event to process
   * @param peerProfiles the profiles getting this event
   * @param frInfo the routing table to update
   */
  private void fillInCQRoutingInfo(CacheEvent event, boolean processLocalProfile,
      Profile[] peerProfiles, FilterRoutingInfo frInfo) {
    CqService cqService = getCqService(event.getRegion());
    if (cqService != null) {
      try {
        Profile local = processLocalProfile? this.localProfile : null;
        cqService.processEvents(event, local, peerProfiles, frInfo);
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, re-throw the error.  We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        SystemFailure.checkFailure();
        logger.error(LocalizedMessage.create(LocalizedStrings.CacheClientNotifier_EXCEPTION_OCCURRED_WHILE_PROCESSING_CQS), t);
      }
    }
  }

   private CqService getCqService(Region region) {
    return ((InternalCache) region.getRegionService()).getCqService();
  }

  /**
   *  computes FilterRoutingInfo objects for each of the given events
   */
  public void getLocalFilterRoutingForPutAllOp(DistributedPutAllOperation dpao, DistributedPutAllOperation.PutAllEntryData[] putAllData) {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    
    if (this.region != null && this.localProfile.hasCacheServer) {
      volatileBarrier();
      Set clientsInv = null;
      Set clients = null;
      int size = putAllData.length;
      CqService cqService = getCqService(dpao.getRegion());
      boolean doCQs = cqService.isRunning() && this.region != null /*&& !(this.region.isUsedForPartitionedRegionBucket()
          || (this.region instanceof PartitionedRegion))*/;
      for (int idx=0; idx < size; idx++) {
        PutAllEntryData pEntry = putAllData[idx];
        if (pEntry != null) {
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
          if (this.allKeyClientsInv != null || this.keysOfInterestInv != null
              || this.patternsOfInterestInv != null
              || this.filtersOfInterestInv != null) {
            clientsInv = this.getInterestedClients(ev, this.allKeyClientsInv,
                this.keysOfInterestInv, this.patternsOfInterestInv,
                this.filtersOfInterestInv);
          }
          if (this.allKeyClients != null || this.keysOfInterest != null
              || this.patternsOfInterest != null
              || this.filtersOfInterest != null) {
            clients = this.getInterestedClients(ev, this.allKeyClients,
                this.keysOfInterest, this.patternsOfInterest,
                this.filtersOfInterest);
          }
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
  *  computes FilterRoutingInfo objects for each of the given events
  */
 public void getLocalFilterRoutingForRemoveAllOp(DistributedRemoveAllOperation op, RemoveAllEntryData[] removeAllData) {
   if (this.region != null && this.localProfile.hasCacheServer) {
     volatileBarrier();
     Set clientsInv = null;
     Set clients = null;
     int size = removeAllData.length;
     CqService cqService = getCqService(op.getRegion());
     boolean doCQs = cqService.isRunning() && this.region != null;
     for (int idx=0; idx < size; idx++) {
       RemoveAllEntryData pEntry = removeAllData[idx];
       if (pEntry != null) {
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
         if (this.allKeyClientsInv != null || this.keysOfInterestInv != null
             || this.patternsOfInterestInv != null
             || this.filtersOfInterestInv != null) {
           clientsInv = this.getInterestedClients(ev, this.allKeyClientsInv,
               this.keysOfInterestInv, this.patternsOfInterestInv,
               this.filtersOfInterestInv);
         }
         if (this.allKeyClients != null || this.keysOfInterest != null
             || this.patternsOfInterest != null
             || this.filtersOfInterest != null) {
           clients = this.getInterestedClients(ev, this.allKeyClients,
               this.keysOfInterest, this.patternsOfInterest,
               this.filtersOfInterest);
         }
         if (clients != null || clientsInv != null) {
           if (fi == null) {
             fi = new FilterInfo();
             // no need to create or update a FilterRoutingInfo at this time
           }
           fi.setInterestedClients(clients);
           fi.setInterestedClientsInv(clientsInv);
         }
         //        if (this.logger.fineEnabled()) {
         //          this.region.getLogWriterI18n().fine("setting event routing to " + fi);
         //        }
         ev.setLocalFilterInfo(fi);
       }
     }
   }
 }

  /**
   * Fills in the routing information for clients that have registered
   * interest in the given event.  The routing information is stored in
   * the given FilterRoutingInfo object for use in message delivery.
   * @param event the event being applied to the cache
   * @param profiles the profiles of members having the affected region
   * @param filterRoutingInfo the routing object that is modified by this method (may be null)
   * @param cacheOpRecipients members that will receive a CacheDistributionMessage for the event
   * @return the resulting FilterRoutingInfo
   */
  public FilterRoutingInfo fillInInterestRoutingInfo(CacheEvent event, Profile[] profiles, 
      FilterRoutingInfo filterRoutingInfo, Set cacheOpRecipients) {

    Set clientsInv = Collections.EMPTY_SET;
    Set clients = Collections.EMPTY_SET;
    
    if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER)) {
      logger.trace(LogMarker.BRIDGE_SERVER, "finding interested clients for {}", event);
    }

    FilterRoutingInfo frInfo = filterRoutingInfo;
    
    for (int i=0; i < profiles.length; i++) {
      CacheProfile cf = (CacheProfile)profiles[i];
      
      if (!cf.hasCacheServer) {
        continue;
      }
      
      FilterProfile pf = cf.filterProfile;

      if (pf == null) {
        continue;
      }
      
      if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER)) {
        logger.trace(LogMarker.BRIDGE_SERVER, "Processing {}", pf);
      }
      
      if (!pf.hasInterest()) {
        // This block sends an empty routing table to a member that's going to
        // get a CacheDistributionMessage so that if, in flight, there is an
        // interest registration change the version held in the routing table
        // can be used to detect the change and the receiver can recompute
        // the routing.
        if (!pf.isLocalProfile() && cacheOpRecipients.contains(cf.getDistributedMember())) {
          if (frInfo == null) frInfo = new FilterRoutingInfo();
          frInfo.addInterestedClients(cf.getDistributedMember(),
              Collections.EMPTY_SET, Collections.EMPTY_SET, false);
        }
        continue;
      }

      if (event.getOperation().isEntry()) {
        EntryEvent entryEvent = (EntryEvent)event;
        if (pf.allKeyClientsInv != null || pf.keysOfInterestInv != null
            || pf.patternsOfInterestInv != null || pf.filtersOfInterestInv != null) {
          clientsInv = pf.getInterestedClients(entryEvent, pf.allKeyClientsInv,
              pf.keysOfInterestInv, pf.patternsOfInterestInv, pf.filtersOfInterestInv);
        }
        if (pf.allKeyClients != null || pf.keysOfInterest != null
            || pf.patternsOfInterest != null || pf.filtersOfInterest != null) {
          clients = pf.getInterestedClients(entryEvent, pf.allKeyClients,
              pf.keysOfInterest, pf.patternsOfInterest, pf.filtersOfInterest);
        }
      } else {
        if (event.getOperation().isRegionDestroy() || event.getOperation().isClear()) {
          clientsInv = pf.getAllClientsWithInterestInv();
          clients = pf.getAllClientsWithInterest();
        } else {
          return frInfo;
        }
      }

      if (pf.isLocalProfile){
        if (logger.isDebugEnabled()) {
          logger.debug("Setting local interested clients={} and clientsInv={}", clients, clientsInv);
        }
        if (frInfo == null) frInfo = new FilterRoutingInfo();
        frInfo.setLocalInterestedClients(clients, clientsInv);
      } else {
        if (cacheOpRecipients.contains(cf.getDistributedMember()) || // always send a routing with CacheOperationMessages
            (clients != null && !clients.isEmpty()) ||
            (clientsInv != null && !clientsInv.isEmpty())) {
          if (logger.isDebugEnabled()) {
            logger.debug("Adding interested clients={} and clientsIn={} to {}", clients, clientsInv, filterRoutingInfo);
          }
          if (frInfo == null) frInfo = new FilterRoutingInfo();
          frInfo.addInterestedClients(cf.getDistributedMember(),
            clients, clientsInv, this.clientMap.hasLongID);
        }
      }
    }
    return frInfo;
  }

  /**
   * get the clients interested in the given event that are attached to this
   * server.
   * @param event the entry event being applied to the cache
   * @param akc allKeyClients collection
   * @param koi keysOfInterest collection
   * @param pats patternsOfInterest collection
   * @param foi filtersOfInterest collection
   * @return a set of the clients interested in the event
   */
  private Set getInterestedClients(EntryEvent event,
      Set akc, Map<Object, Set> koi, Map<Object, Map<Object, Pattern>> pats,
      Map<Object, Map>foi) {
    Set result = null;
    if (akc != null) {
      result = new HashSet(akc);
      if (logger.isDebugEnabled()) {
        logger.debug("these clients matched for all-keys: {}", akc);
      }
    }
    if (koi != null) {
      for (Iterator it=koi.entrySet().iterator(); it.hasNext(); ) {
        Map.Entry entry = (Map.Entry)it.next();
        Set keys = (Set)entry.getValue();
        if (keys.contains(event.getKey())) {
          Object clientID = entry.getKey();
          if (result == null) result = new HashSet();
          result.add(clientID);
          if (logger.isDebugEnabled()) {
            logger.debug("client {} matched for key list (size {})", clientID, koi.get(clientID).size());
          }
        }
      }
    }
    if (pats != null && (event.getKey() instanceof String)) {
      for (Iterator it=pats.entrySet().iterator(); it.hasNext(); ) {
        Map.Entry entry = (Map.Entry)it.next();
        String stringKey = (String)event.getKey();
        Map<Object, Pattern> interestList = (Map<Object, Pattern>)entry.getValue();
        for (Pattern keyPattern: interestList.values()) {
          if (keyPattern.matcher(stringKey).matches()) {
            Object clientID = entry.getKey();
            if (result == null) result = new HashSet();
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
      Object value = event.getSerializedNewValue();
      boolean serialized = (value != null);
      if (!serialized) {
        value = event.getNewValue();
      }
      InterestEvent iev = new InterestEvent(event.getKey(), value, !serialized);
      Operation op = event.getOperation();
      for (Iterator it=foi.entrySet().iterator(); it.hasNext(); ) {
        Map.Entry entry = (Map.Entry)it.next();
        Map<String, InterestFilter> interestList = (Map<String, InterestFilter>)entry.getValue();
        for (InterestFilter filter: interestList.values()) {
          if (
           (op.isCreate()  && filter.notifyOnCreate(iev)) ||
           (op.isUpdate()  && filter.notifyOnUpdate(iev)) ||
           (op.isDestroy() && filter.notifyOnDestroy(iev)) ||
           (op.isInvalidate() && filter.notifyOnInvalidate(iev))
           ) {
            Object clientID = entry.getKey();
            if (result == null) result = new HashSet();
            result.add(clientID);
            if (logger.isDebugEnabled()) {
              logger.debug("client {} matched for filter ({})", clientID, getFiltersOfInterest().get(clientID));
            }
            break;
          }
        }
      }
    }
    return result;
  }
  
  
  
  
  /* DataSerializableFixedID methods ---------------------------------------- */

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    InternalDistributedMember id = new InternalDistributedMember();
    InternalDataSerializer.invokeFromData(id, in);
    this.memberID = id;
    
    this.allKeyClients = InternalDataSerializer.readSetOfLongs(in);
    this.keysOfInterest = DataSerializer.readHashMap(in);
    this.patternsOfInterest = DataSerializer.readHashMap(in);
    this.filtersOfInterest = DataSerializer.readHashMap(in);

    this.allKeyClientsInv = InternalDataSerializer.readSetOfLongs(in);
    this.keysOfInterestInv = DataSerializer.readHashMap(in);
    this.patternsOfInterestInv = DataSerializer.readHashMap(in);
    this.filtersOfInterestInv = DataSerializer.readHashMap(in);
    
    // Read CQ Info.
    int numCQs = InternalDataSerializer.readArrayLength(in);
    if (numCQs > 0) {
      Map theCQs = new HashMap(numCQs);
      int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.ANY_INIT); // do this before CacheFactory.getInstance for bug 33471
      try {
        for (int i=0; i < numCQs; i++){
          String serverCqName = DataSerializer.readString(in);
          ServerCQ cq = CqServiceProvider.readCq(in);
          processRegisterCq(serverCqName, cq, false);
          theCQs.put(serverCqName, cq);
        } 
      } finally {
        this.cqs = theCQs;
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
      }
    }
    
  }
  

  public int getDSFID() {
    return FILTER_PROFILE;
  }

  public void toData(DataOutput out) throws IOException {
    InternalDataSerializer.invokeToData(((InternalDistributedMember)memberID), out);
    InternalDataSerializer.writeSetOfLongs(this.allKeyClients, this.clientMap.hasLongID, out);
    DataSerializer.writeHashMap((HashMap)this.keysOfInterest, out);
    DataSerializer.writeHashMap((HashMap)this.patternsOfInterest, out);
    DataSerializer.writeHashMap((HashMap)this.filtersOfInterest, out);

    InternalDataSerializer.writeSetOfLongs(this.allKeyClientsInv, this.clientMap.hasLongID, out);
    DataSerializer.writeHashMap((HashMap)this.keysOfInterestInv, out);
    DataSerializer.writeHashMap((HashMap)this.patternsOfInterestInv, out);
    DataSerializer.writeHashMap((HashMap)this.filtersOfInterestInv, out);
    
    // Write CQ info.
    Map theCQs = this.cqs;
    int size = theCQs.size();
    InternalDataSerializer.writeArrayLength(size, out);
    for (Iterator it=theCQs.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry entry = (Map.Entry)it.next();
      String name = (String)entry.getKey();
      ServerCQ cq = (ServerCQ)entry.getValue();
      DataSerializer.writeString(name, out);
      InternalDataSerializer.invokeToData(cq, out);
    }
  }
  
  /**
   * @return the keysOfInterest
   */
  private Map<Object, Set> getKeysOfInterest() {
    Map<Object,Set> keysOfInterestRef = this.keysOfInterest;
    return keysOfInterestRef == null? Collections.EMPTY_MAP : keysOfInterestRef;
  }

  /**
   * @param keysOfInterest the keysOfInterest to set
   */
  private void setKeysOfInterest(Map keysOfInterest) {
    this.keysOfInterest = keysOfInterest;
  }

  /**
   * @return the keysOfInterestInv
   */
  private Map<Object, Set> getKeysOfInterestInv() {
    Map<Object, Set> keysOfInterestInvRef = this.keysOfInterestInv;
    return keysOfInterestInvRef == null? Collections.EMPTY_MAP : keysOfInterestInvRef;
  }

  /**
   * @param keysOfInterestInv the keysOfInterestInv to set
   */
  private void setKeysOfInterestInv(Map keysOfInterestInv) {
    this.keysOfInterestInv = keysOfInterestInv;
  }

  /**
   * @return the patternsOfInterest
   */
  private Map<Object, Map<Object, Pattern>> getPatternsOfInterest() {
    Map<Object, Map<Object, Pattern>> patternsOfInterestRef = this.patternsOfInterest;
    return patternsOfInterestRef == null? Collections.EMPTY_MAP : patternsOfInterestRef;
  }

  /**
   * @param patternsOfInterest the patternsOfInterest to set
   */
  private void setPatternsOfInterest(Map patternsOfInterest) {
    this.patternsOfInterest = patternsOfInterest;
  }

  /**
   * @return the patternsOfInterestInv
   */
  private Map<Object, Map<Object, Pattern>> getPatternsOfInterestInv() {
    Map<Object, Map<Object, Pattern>> patternsOfInterestInvRef = this.patternsOfInterestInv;
    return patternsOfInterestInvRef == null? Collections.EMPTY_MAP : patternsOfInterestInvRef;
  }

  /**
   * @param patternsOfInterestInv the patternsOfInterestInv to set
   */
  private void setPatternsOfInterestInv(Map patternsOfInterestInv) {
    this.patternsOfInterestInv = patternsOfInterestInv;
  }

  /**
   * @return the filtersOfInterestInv
   */
  Map<Object, Map> getFiltersOfInterestInv() {
    Map<Object, Map> filtersOfInterestInvRef = filtersOfInterestInv;
    return filtersOfInterestInvRef == null? Collections.EMPTY_MAP : filtersOfInterestInvRef;
  }

  /**
   * @param filtersOfInterestInv the filtersOfInterestInv to set
   */
  void setFiltersOfInterestInv(Map filtersOfInterestInv) {
    this.filtersOfInterestInv = filtersOfInterestInv;
  }



  /**
   * @return the filtersOfInterest
   */
  private Map<Object, Map> getFiltersOfInterest() {
    Map<Object, Map> filtersOfInterestRef = this.filtersOfInterest;
    return  filtersOfInterestRef == null? Collections.EMPTY_MAP : filtersOfInterestRef;
  }

  /**
   * @param filtersOfInterest the filtersOfInterest to set
   */
  private void setFiltersOfInterest(Map filtersOfInterest) {
    this.filtersOfInterest = filtersOfInterest;
  }

  /**
   * So far all calls to setAllKeyClients has occurred under synchronized blocks, locking on interestListLock
   * @param akc the allKeyClients to set
   */
  private void setAllKeyClients(Set akc) {
    this.allKeyClients = akc;
    if (logger.isDebugEnabled()) {
      logger.debug("{}: updated allKeyClients to {}", this, akc);
    }
  }
  
  /**
   * perform a volatile read to ensure that all state is consistent
   */
  private void volatileBarrier() {
    volatileBarrier = this.cqCount; // volatile write
  }

  /**
   * It is possible to do a get outside of a synch block, so it can change if another thread
   * calls setAllKeyClients.  So instead we store a ref to allKeyClients and if it changes
   * we at least have the one we expected to return.
   * @return the allKeyClients
   */
  private Set<Object> getAllKeyClients() {
    Set allKeysRef = this.allKeyClients;
    if (testHook != null) {
      testHook.await();
    }
    return allKeysRef == null? Collections.EMPTY_SET : allKeysRef;
  }

  public int getAllKeyClientsSize(){
    return this.getAllKeyClients().size();
  }

  /**
   * @param akc the allKeyClientsInv to set
   */
  private void setAllKeyClientsInv(Set akc) {
    this.allKeyClientsInv = akc;
  }

  /**
   * It is possible to do a get outside of a synch block, so it can change if another thread
   * calls setAllKeyClients.  So instead we store a ref to allKeyClientsInv and if it changes
   * we at least have the one we expected to return.
   * @return the allKeyClientsInv
   */
  private Set<Object> getAllKeyClientsInv() {
    Set allKeysInvRef = this.allKeyClientsInv;
    return allKeysInvRef == null? Collections.EMPTY_SET : allKeysInvRef;
  }

  public int getAllKeyClientsInvSize(){
    return this.getAllKeyClientsInv().size();
  }

  @Override
  public String toString() {
    final boolean isDebugEnabled = logger.isTraceEnabled(LogMarker.BRIDGE_SERVER);
    return "FilterProfile(id=" + (this.isLocalProfile? "local" : this.memberID)
//    + ";  allKeys: " + this.allKeyClients
//    + ";  keys: " + this.keysOfInterest
//    + ";  patterns: " + this.patternsOfInterest
//    + ";  filters: " + this.filtersOfInterest
//    + ";  allKeysInv: " + this.allKeyClientsInv
//    + ";  keysInv: " + this.keysOfInterestInv
//    + ";  patternsInv: " + this.patternsOfInterestInv
//    + ";  filtersInv: " + this.filtersOfInterestInv
    + ";  numCQs: " + ((this.cqCount == null)?0:this.cqCount.get())
    + (isDebugEnabled? (";  " + getClientMappingString()) : "")
    + (isDebugEnabled? (";  " + getCqMappingString()) : "")
    + ")";
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
    StringBuffer result = new StringBuffer(sorted.size() * 70);
    result.append("clients[");
    Iterator<Long> it = sorted.iterator();
    for (int i=1; it.hasNext(); i++) {
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
    StringBuffer result = new StringBuffer(sorted.size() * 70);
    result.append("cqs[");
    Iterator<Long> it = sorted.iterator();
    for (int i=1; it.hasNext(); i++) {
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
   * given a collection of on-wire identifiers, this returns a set of
   * the client/server identifiers for each client or durable queue
   * @param integerIDs the integer ids of the clients/queues
   * @return the translated identifiers
   */
  public Set getRealClientIDs(Collection integerIDs) {
    return clientMap.getRealIDs(integerIDs);
  }
  
  /**
   * given a collection of on-wire identifiers, this returns a set of
   * the CQ identifiers they correspond to
   * @param integerIDs the integer ids of the clients/queues
   * @return the translated identifiers
   */
  public Set getRealCqIDs(Collection integerIDs) {
    return cqMap.getRealIDs(integerIDs);
  }
  
  /**
   * given an on-wire filter ID, find and return the corresponding cq name
   * @param integerID the on-wire ID
   * @return the translated id
   */
  public String getRealCqID(Long integerID) {
    return (String)cqMap.getRealID(integerID);
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
   * Returns the filter profile messages received while members cache profile 
   * exchange was in progress. 
   * @param member whose messages are returned.
   * @return filter profile messages that are queued for the member.
   */
  public List getQueuedFilterProfileMsgs(InternalDistributedMember member){
    synchronized (this.filterProfileMsgQueue){
      if (this.filterProfileMsgQueue.containsKey(member)) {
        return new LinkedList(this.filterProfileMsgQueue.get(member));
      }
    }
    return Collections.EMPTY_LIST;
  }

  /**
   * Removes the filter profile messages from the queue that are received  
   * while the members cache profile exchange was in progress. 
   * @param member whose messages are returned.
   * @return filter profile messages that are queued for the member.
   */
  public List removeQueuedFilterProfileMsgs(InternalDistributedMember member){
    synchronized (this.filterProfileMsgQueue){
      if (this.filterProfileMsgQueue.containsKey(member)) {
        return new LinkedList(this.filterProfileMsgQueue.remove(member));
      }
    }
    return Collections.EMPTY_LIST;
  }
  
  /**
   * Adds the message to filter profile queue.
   * @param member
   * @param message
   */
  public void addToFilterProfileQueue(InternalDistributedMember member, OperationMessage message){
    if (logger.isDebugEnabled()) {
      logger.debug("Adding message to filter profile queue: {} for member : {}", message, member);
    }    
    synchronized (this.filterProfileMsgQueue){
      LinkedList msgs = this.filterProfileMsgQueue.get(member);
      if (msgs == null){
        msgs = new LinkedList();
        this.filterProfileMsgQueue.put(member, msgs);
      }
      msgs.add(message);
    }
  }
  
  /**
   * Process the filter profile messages.
   * @param msgs
   */
  public void processQueuedFilterProfileMsgs(List msgs){
    final boolean isDebugEnabled = logger.isDebugEnabled();
    
    if (msgs != null){
      Iterator iter = msgs.iterator();
      while (iter.hasNext()) {
        try {
          OperationMessage msg = (OperationMessage)iter.next();
          if (isDebugEnabled) {
            logger.debug("Processing the queued filter profile message :{}", msg);
          }
          msg.processRequest(this);
        } catch (Exception ex){
          logger.warn("Exception thrown while processing queued profile messages.", ex);
        }
      }
    }
  }
  
  /**
   * OperationMessage synchronously propagates a change in the profile to
   * another member.  It is a serial message so that there is no chance
   * of out-of-order execution.
   * @author bruce
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
    
    /* (non-Javadoc)
     * @see com.gemstone.gemfire.distributed.internal.DistributionMessage#process(com.gemstone.gemfire.distributed.internal.DistributionManager)
     */
    @Override
    protected void process(DistributionManager dm) {
      try {
        CacheDistributionAdvisee r = findRegion();
        if (r == null) {
          if (logger.isDebugEnabled()) {
            logger.debug("Region not found, so ignoring filter profile update: {}", this);
          }
          return;
        }
        // we only need to record the delta if this is a partitioned region 
        if ( ! (r instanceof PartitionedRegion) ) {
          return;
        }
 
        CacheDistributionAdvisor cda = (CacheDistributionAdvisor)r.getDistributionAdvisor();
        CacheDistributionAdvisor.CacheProfile cp =
          (CacheDistributionAdvisor.CacheProfile)cda.getProfile(getSender());
        if (cp == null) {  // PR accessors do not keep filter profiles around
          if (logger.isDebugEnabled()) {
            logger.debug("No cache profile to update, adding filter profile message to queue. Message :{}", this);
          }
          FilterProfile localFP = ((PartitionedRegion)r).getFilterProfile();
          localFP.addToFilterProfileQueue(getSender(), this);
          dm.getCancelCriterion().checkCancelInProgress(null);
        } else {
          cp.hasCacheServer = true;
          FilterProfile fp = cp.filterProfile;
          if (fp == null) {  // PR accessors do not keep filter profiles around
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
      }
      catch (RuntimeException e) {
        logger.warn("Exception thrown while processing profile update", e);
      }
      finally {
        ReplyMessage reply = new ReplyMessage();
        reply.setProcessorId(this.processorId);
        reply.setRecipient(getSender());
        try {
          dm.putOutgoing(reply);
        } catch (CancelException e) {
          // can't send a reply, so ignore the exception
        }
      }
    }

    public void processRequest(FilterProfile fp) {
      switch (opType) {
        case REGISTER_KEY:
          fp.registerClientInterest(clientID, this.interest, InterestType.KEY, updatesAsInvalidates);
          break;
        case REGISTER_PATTERN:
          fp.registerClientInterest(clientID, this.interest, InterestType.REGULAR_EXPRESSION, updatesAsInvalidates);
          break;
        case REGISTER_FILTER:
          fp.registerClientInterest(clientID, this.interest, InterestType.FILTER_CLASS, updatesAsInvalidates);
          break;
        case REGISTER_KEYS:
          fp.registerClientInterestList(clientID, (List)this.interest, updatesAsInvalidates);
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
          fp.unregisterClientInterestList(clientID, (List)this.interest);
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
          throw new IllegalArgumentException("Unknown filter profile operation type in operation: " + this);
      }
    }      
    
    private CacheDistributionAdvisee findRegion() {
      CacheDistributionAdvisee result = null;
      GemFireCacheImpl cache = null;
      try {
        cache = GemFireCacheImpl.getInstance();
        if (cache != null) {
          LocalRegion lr = cache.getRegionByPathForProcessing(regionName);
          if (lr instanceof CacheDistributionAdvisee) {
            result = (CacheDistributionAdvisee)lr;
          }
        }
      } catch (CancelException e) {
        // nothing to do
      }
      return result;
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.internal.DataSerializableFixedID#getDSFID()
     */
    public int getDSFID() {
      return FILTER_PROFILE_UPDATE;
    }
    
    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.processorId);
      out.writeUTF(this.regionName);
      out.writeShort(this.opType.ordinal());
      out.writeBoolean(this.updatesAsInvalidates);
      out.writeLong(this.profileVersion);
      
      if (isCqOp(this.opType)) {
        // For CQ info.
        // Write Server CQ Name.
        out.writeUTF(((ServerCQ)this.cq).getServerCqName());
        if (this.opType == operationType.REGISTER_CQ || 
            this.opType == operationType.SET_CQ_STATE){
          InternalDataSerializer.invokeToData((ServerCQ)this.cq, out);
        }
      } else {
        // For interest list.
        out.writeLong(this.clientID);
        DataSerializer.writeObject(this.interest, out);
      } 
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
      this.regionName = in.readUTF();
      this.opType = operationType.values()[in.readShort()];
      this.updatesAsInvalidates = in.readBoolean();
      this.profileVersion = in.readLong();
      if (isCqOp(this.opType)){
        this.serverCqName = in.readUTF();
        if (this.opType == operationType.REGISTER_CQ || 
            this.opType == operationType.SET_CQ_STATE){
          this.cq = CqServiceProvider.readCq(in);
        }
      } else {
        this.clientID = in.readLong();
        this.interest = DataSerializer.readObject(in);
      }
    }
    
    @Override
    public String toString() {
      return this.getShortClassName() + "(processorId=" + this.processorId
      + "; region=" + this.regionName
      + "; operation=" + this.opType
      + "; clientID=" + this.clientID
      + "; profileVersion=" + this.profileVersion
      + (isCqOp(this.opType)?("; CqName="+this.serverCqName):"")
      + ")";
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
        synchronized(this) {
          result = this.realIDs.get(realId);
          if (result == null) {
            if (nextID == Integer.MAX_VALUE) {
              this.hasLongID = true;
            }
            result = Long.valueOf(nextID++);
            this.realIDs.put(realId, result);
            this.wireIDs.put(result, realId);
          }
        }
        if (logger.isTraceEnabled(LogMarker.BRIDGE_SERVER)) {
          logger.trace(LogMarker.BRIDGE_SERVER, "Profile for {} mapped {} to {}", region.getFullPath(), realId, result);
        }
      }
      return result;
    }
    
    /** return the client or durable queue id for the given on-wire identifier */
    Object getRealID(Long wireID) {
      return wireIDs.get(wireID);
    }
    
    
    /**
     * given a collection of on-wire identifiers, this returns a set of
     * the real identifiers (e.g., client IDs or durable queue IDs)
     * @param integerIDs the integer ids
     * @return the translated identifiers
     */
    public Set getRealIDs(Collection integerIDs) {
      if (integerIDs.size() == 0) {
        return Collections.emptySet();
      }
      Set result = new HashSet(integerIDs.size());
      Map<Long, Object> wids = wireIDs;
      for (Object id: integerIDs) {
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

  }

  /**
   * Returns true if the client is interested in all keys.
   * @param id client identifier.
   * @return true if client is interested in all keys.
   */
  public boolean isInterestedInAllKeys(Object id){
    if (!clientMap.hasWireID(id)) {
      return false;
    }
    return this.getAllKeyClients().contains(clientMap.getWireID(id));
  }
  
  /**
   * Returns true if the client is interested in all keys, for which 
   * updates are sent as invalidates.  
   * @param id client identifier
   * @return true if client is interested in all keys.
   */
  public boolean isInterestedInAllKeysInv(Object id){
    if (!clientMap.hasWireID(id)) {
      return false;
    }
    return this.getAllKeyClientsInv().contains(clientMap.getWireID(id));
  }
  
  /**
   * Returns the set of client interested keys.
   * @param id client identifier
   * @return client interested keys.
   */
  public Set getKeysOfInterest(Object id){
    if (!clientMap.hasWireID(id)) {
      return null;
    }
    return this.getKeysOfInterest().get(clientMap.getWireID(id));
  }
  
  public int getKeysOfInterestSize(){
    return this.getKeysOfInterest().size();
  }
  
  /**
   * Returns the set of client interested keys for which updates are sent
   * as invalidates.  
   * @param id client identifier
   * @return client interested keys.
   */
  public Set getKeysOfInterestInv(Object id){
    if (!clientMap.hasWireID(id)) {
      return null;
    }
    return this.getKeysOfInterestInv().get(clientMap.getWireID(id));
  }
  
  public int getKeysOfInterestInvSize(){
    return this.getKeysOfInterestInv().size();
  }
  
  /**
   * Returns the set of client interested patterns.
   * @param id client identifier
   * @return client interested patterns.
   */
  public Set getPatternsOfInterest(Object id){
    if (!clientMap.hasWireID(id)) {
      return null;
    }
    Map patterns = this.getPatternsOfInterest().get(clientMap.getWireID(id)); 
    if (patterns != null){
      return new HashSet(patterns.keySet());
    }
    return null;
  }
  
  public int getPatternsOfInterestSize(){
    return  this.getPatternsOfInterest().size(); 
  }
  
  /**
   * Returns the set of client interested patterns for which updates are sent
   * as invalidates.  
   * @param id client identifier
   * @return client interested patterns.
   */
  public Set getPatternsOfInterestInv(Object id){
    if (!clientMap.hasWireID(id)) {
      return null;
    }
    Map interests = this.getPatternsOfInterestInv().get(clientMap.getWireID(id));
    if (interests != null){
      return new HashSet(interests.keySet());
    }
    return null;
  }
  
  public int getPatternsOfInterestInvSize(){
    return  this.getPatternsOfInterestInv().size(); 
  }
  
  /**
   * Returns the set of client interested filters.
   * @param id client identifier
   * @return client interested filters.
   */
  public Set getFiltersOfInterest(Object id){
    if (!clientMap.hasWireID(id)) {
      return null;
    }
    Map interests = this.getFiltersOfInterest().get(clientMap.getWireID(id));
    if (interests != null){
      return new HashSet(interests.keySet());
    }
    return null;
  }
  
  /**
   * Returns the set of client interested filters for which updates are sent
   * as invalidates.  
   * @param id client identifier
   * @return client interested filters.
   */
  public Set getFiltersOfInterestInv(Object id){
    if (!clientMap.hasWireID(id)) {
      return null;
    }
    Map interests = this.getFiltersOfInterestInv().get(clientMap.getWireID(id));
    if (interests != null){
      return new HashSet(interests.keySet());
    }
    return null;
  }
  
  public static TestHook testHook = null;
  
  /** Test Hook */
  public interface TestHook {
    public void await();
    public void release();
  }

  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }
}
