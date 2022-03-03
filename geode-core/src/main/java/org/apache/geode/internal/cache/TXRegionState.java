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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.TXEntryState.DistTxThinEntryState;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * TXRegionState is the entity that tracks all the changes a transaction has made to a region.
 *
 *
 * @since GemFire 4.0
 *
 * @see TXManagerImpl
 */
public class TXRegionState {
  private static final Logger logger = LogService.getLogger();

  // A map of Objects (entry keys) -> TXEntryState
  private final HashMap<Object, TXEntryState> entryMods;
  // A map of Objects (entry keys) -> TXEntryUserAttrState
  private HashMap uaMods;
  private Set<InternalDistributedMember> otherMembers = null;
  private final TXState txState;
  private final InternalRegion region;
  private final boolean needsRefCounts;
  private boolean cleanedUp;
  /*
   * For Distributed Tx Created during precommit, to apply changes on secondaries/replicates from
   * coordinator.
   */
  private boolean createdDuringCommit;

  public TXRegionState(InternalRegion r, TXState txState) {
    if (r.getPersistBackup() && !r.isMetaRegionWithTransactions()
        && !TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS) {
      throw new UnsupportedOperationException(
          "Operations on persist-backup regions are not allowed because this thread has an active transaction");
    }
    if (r.getScope().isGlobal()) {
      throw new UnsupportedOperationException(
          "Operations on global regions are not allowed because this thread has an active transaction");
    }
    entryMods = new HashMap<>();
    uaMods = null;
    region = r;
    this.txState = txState;
    needsRefCounts = r.isEntryEvictionPossible() || r.isEntryExpiryPossible();
    r.setInUseByTransaction(true);
  }

  public InternalRegion getRegion() {
    return region;
  }

  public boolean needsRefCounts() {
    return needsRefCounts;
  }



  public Set getEntryKeys() {
    return Collections.unmodifiableSet(entryMods.keySet());
  }

  public TXEntryState readEntry(Object entryKey) {
    return entryMods.get(entryKey);
  }

  public TXEntryState createReadEntry(LocalRegion r, Object entryKey, RegionEntry re, Object vId,
      Object pendingValue) {
    InternalCache cache = r.getCache();
    boolean isDistributed = false;
    if (cache.getTxManager().getTXState() != null) {
      isDistributed = cache.getTxManager().getTXState().isDistTx();
    } else {
      // TXCoordinator and datanode are same
      isDistributed = cache.getTxManager().isDistributed();
    }
    TXEntryState result = cache.getTXEntryStateFactory().createEntry(re, vId, pendingValue,
        entryKey, this, isDistributed);
    entryMods.put(entryKey, result);
    return result;
  }

  // public void rmEntry(Object entryKey, TXState txState, LocalRegion r) {
  // rmEntryUserAttr(entryKey);
  // TXEntryState e = (TXEntryState)this.entryMods.remove(entryKey);
  // if (e != null) {
  // e.cleanup(r);
  // }
  // if (this.uaMods == null && this.entryMods.size() == 0) {
  // txState.rmRegion(r);
  // }
  // }

  public TXEntryUserAttrState readEntryUserAttr(Object entryKey) {
    TXEntryUserAttrState result = null;
    if (uaMods != null) {
      result = (TXEntryUserAttrState) uaMods.get(entryKey);
    }
    return result;
  }

  public TXEntryUserAttrState writeEntryUserAttr(Object entryKey, LocalRegion r) {
    if (uaMods == null) {
      uaMods = new HashMap();
    }
    TXEntryUserAttrState result = (TXEntryUserAttrState) uaMods.get(entryKey);
    if (result == null) {
      result = new TXEntryUserAttrState(r.basicGetEntryUserAttribute(entryKey));
      uaMods.put(entryKey, result);
    }
    return result;
  }

  public void rmEntryUserAttr(Object entryKey) {
    if (uaMods != null) {
      if (uaMods.remove(entryKey) != null) {
        if (uaMods.size() == 0) {
          uaMods = null;
        }
      }
    }
  }

  /**
   * Returns the total number of modifications made by this transaction to this region's entry
   * count. The result will have a +1 for every create and a -1 for every destroy.
   */
  int entryCountMod() {
    int result = 0;
    for (final TXEntryState es : entryMods.values()) {
      result += es.entryCountMod();
    }
    return result;
  }

  TXEntryState getTXEntryState(Object key) {
    return entryMods.get(key);
  }

  /**
   * Fills in a set of any entries created by this transaction for the provided region.
   *
   * @param ret the HashSet to fill in with key objects
   */
  void fillInCreatedEntryKeys(HashSet ret) {
    for (final Entry<Object, TXEntryState> me : entryMods.entrySet()) {
      TXEntryState txes = me.getValue();
      if (txes.wasCreatedByTX()) {
        ret.add(me.getKey());
      }
    }
  }

  /**
   * Create a lock request on this region state and adds it to req
   */
  void createLockRequest(InternalRegion r, TXLockRequest req) {
    if (uaMods == null && entryMods.isEmpty()) {
      return;
    }
    if (TXState.logger.isDebugEnabled()) {
      TXState.logger.debug("TXRegionState.createLockRequest 1 " + r.getClass().getSimpleName()
          + " region-state=" + this);
    }
    if (r.getScope().isDistributed()) {
      // [DISTTX] Do not take lock for RR on replicates
      if (isCreatedDuringCommit()) {
        return;
      }
      DistributedRegion dr = (DistributedRegion) r;
      Set<InternalDistributedMember> advice = dr.getCacheDistributionAdvisor().adviseTX();
      if (!advice.isEmpty()) {
        otherMembers = advice; // remember for when it is time to distribute
      }
    }
    if (TXState.logger.isDebugEnabled()) {
      TXState.logger.debug("TXRegionState.createLockRequest 2");
    }
    // Bypass D-lock for Pr TX
    boolean byPassDLock = r instanceof BucketRegion;
    // BucketRegion br = (BucketRegion)r;
    // if (br.getRedundancyLevel() < 2) {
    // }
    final boolean distributedTX = !byPassDLock && r.getScope().isDistributedAck();
    if (uaMods != null || (!distributedTX && entryMods.size() > 0)) {
      // need some local locks
      TXRegionLockRequestImpl rlr = new TXRegionLockRequestImpl(r.getCache(), r);
      if (uaMods != null) {
        for (final Object o : uaMods.keySet()) {
          // add key with isEvent set to TRUE, for keep BC
          rlr.addEntryKey(o, Boolean.TRUE);
        }

      }
      if (!distributedTX && entryMods.size() > 0) {
        rlr.addEntryKeys(getLockRequestEntryKeys());
      }
      if (!rlr.isEmpty()) {
        req.addLocalRequest(rlr);
      }
    }
    if (distributedTX && entryMods.size() > 0) {
      // need some distributed locks
      TXRegionLockRequestImpl rlr = new TXRegionLockRequestImpl(r.getCache(), r);
      rlr.addEntryKeys(getLockRequestEntryKeys());
      if (!rlr.isEmpty()) {
        req.setOtherMembers(otherMembers);
        req.addDistributedRequest(rlr);
      }
    }
  }

  /**
   * Returns a map of entry keys that this tx needs to request a lock for at commit time.
   *
   * @return <code>null</code> if no entries need to be locked.
   */
  private Map getLockRequestEntryKeys() {
    HashMap<Object, Boolean> result = null;
    for (final Entry<Object, TXEntryState> objectTXEntryStateEntry : entryMods.entrySet()) {
      TXEntryState txes = (TXEntryState) ((Entry) objectTXEntryStateEntry).getValue();
      if (txes.isDirty() && !txes.isOpSearch()) {
        if (result == null) {
          result = new HashMap();
        }
        result.put(((Entry) objectTXEntryStateEntry).getKey(), txes.isOpAnyEvent(region));
      }
    }
    return result;
  }

  void checkForConflicts(InternalRegion r) throws CommitConflictException {
    if (isCreatedDuringCommit()) {
      return;
    }
    {
      for (final Entry<Object, TXEntryState> objectTXEntryStateEntry : entryMods.entrySet()) {
        Object eKey = ((Entry) objectTXEntryStateEntry).getKey();
        TXEntryState txes = (TXEntryState) ((Entry) objectTXEntryStateEntry).getValue();
        txes.checkForConflict(r, eKey);
      }
    }
    if (uaMods != null) {
      r.checkReadiness();
      for (final Object o : uaMods.entrySet()) {
        Entry me = (Entry) o;
        Object eKey = me.getKey();
        TXEntryUserAttrState txes = (TXEntryUserAttrState) me.getValue();
        txes.checkForConflict(r, eKey);
      }
    }
  }

  /**
   * For each entry that is not dirty (all we did was read it) decrement its refcount (so it can be
   * evicted as we apply our writes) and remove it from entryMods (so we don't keep iterating over
   * it and se we don't try to clean it up again later).
   */
  void cleanupNonDirtyEntries(InternalRegion r) {
    if (!entryMods.isEmpty()) {
      Iterator it = entryMods.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry me = (Map.Entry) it.next();
        // Object eKey = me.getKey();
        TXEntryState txes = (TXEntryState) me.getValue();
        if (txes.cleanupNonDirty(r)) {
          it.remove();
        }
      }
    }
  }

  void buildMessage(InternalRegion r, TXCommitMessage msg) {
    try {
      if (!r.getScope().isLocal() && !entryMods.isEmpty()) {

        msg.startRegion(r, entryMods.size());
        Iterator it = entryMods.entrySet().iterator();
        Set<InternalDistributedMember> newMemberSet = new HashSet<>();

        if (r.getScope().isDistributed()) {
          DistributedRegion dr = (DistributedRegion) r;
          msg.addViewVersion(dr, dr.getDistributionAdvisor().startOperation());
          newMemberSet.addAll(dr.getCacheDistributionAdvisor().adviseTX());
        }

        while (it.hasNext()) {
          Map.Entry me = (Map.Entry) it.next();
          Object eKey = me.getKey();
          TXEntryState txes = (TXEntryState) me.getValue();
          txes.buildMessage(r, eKey, msg);
          if (txes.getFilterRoutingInfo() != null) {
            newMemberSet.addAll(txes.getFilterRoutingInfo().getMembers());
          }
          if (txes.getAdjunctRecipients() != null) {
            newMemberSet.addAll(txes.getAdjunctRecipients());
          }

        }



        if (!newMemberSet.equals(otherMembers)) {
          // r.getCache().getLogger().info("DEBUG: participants list has changed! bug 32999.");
          // Flag the message that the lock manager needs to be updated with the new member set
          msg.setUpdateLockMembers();
          otherMembers = newMemberSet;
        }

        msg.finishRegion(otherMembers);
      }
    } catch (RegionDestroyedException ex) {
      // region was destroyed out from under us; after conflict checking
      // passed. So act as if the region destroy happened right after the
      // commit. We act this way by doing nothing; including distribution
      // of this region's commit data.
    } catch (CancelException ex) {
      // cache was closed out from under us; after conflict checking
      // passed. So do nothing.
    }
  }

  void buildMessageForAdjunctReceivers(InternalRegion r, TXCommitMessage msg) {
    try {
      if (!r.getScope().isLocal() && !entryMods.isEmpty()) {

        msg.startRegion(r, entryMods.size());
        Iterator it = entryMods.entrySet().iterator();
        Set<InternalDistributedMember> newMemberSet = new HashSet<>();

        while (it.hasNext()) {
          Map.Entry me = (Map.Entry) it.next();
          Object eKey = me.getKey();
          TXEntryState txes = (TXEntryState) me.getValue();
          txes.buildMessage(r, eKey, msg);
          if (txes.getFilterRoutingInfo() != null) {
            newMemberSet.addAll(txes.getFilterRoutingInfo().getMembers());
          }
          if (txes.getAdjunctRecipients() != null) {

            Set adjunctRecipients = txes.getAdjunctRecipients();
            newMemberSet.addAll(adjunctRecipients);
          }
        }


        if (!newMemberSet.equals(otherMembers)) {
          // r.getCache().getLogger().info("DEBUG: participants list has changed! bug 32999.");
          // Flag the message that the lock manager needs to be updated with the new member set
          msg.setUpdateLockMembers();
          otherMembers = newMemberSet;
        }

        msg.finishRegion(otherMembers);
      }
    } catch (RegionDestroyedException ex) {
      // region was destroyed out from under us; after conflict checking
      // passed. So act as if the region destroy happened right after the
      // commit. We act this way by doing nothing; including distribution
      // of this region's commit data.
    } catch (CancelException ex) {
      // cache was closed out from under us; after conflict checking
      // passed. So do nothing.
    }
  }


  void buildCompleteMessage(InternalRegion r, TXCommitMessage msg) {
    try {
      if (!entryMods.isEmpty()) {
        msg.startRegion(r, entryMods.size());
        for (final Entry<Object, TXEntryState> objectTXEntryStateEntry : entryMods.entrySet()) {
          Object eKey = ((Entry) objectTXEntryStateEntry).getKey();
          TXEntryState txes = (TXEntryState) ((Entry) objectTXEntryStateEntry).getValue();
          txes.buildCompleteMessage(r, eKey, msg);
        }
        msg.finishRegionComplete();
      }
    } catch (RegionDestroyedException ex) {
      // region was destroyed out from under us; after conflict checking
      // passed. So act as if the region destroy happened right after the
      // commit. We act this way by doing nothing; including distribution
      // of this region's commit data.
    } catch (CancelException ex) {
      // cache was closed out from under us; after conflict checking
      // passed. So do nothing.
    }
  }



  void applyChangesStart(InternalRegion r, TXStateInterface txState) {
    try {
      r.txLRUStart();
    } catch (RegionDestroyedException ex) {
      // region was destroyed out from under us; after conflict checking
      // passed. So act as if the region destroy happened right after the
      // commit. We act this way by doing nothing; including distribution
      // of this region's commit data.
    } catch (CancelException ex) {
      // cache was closed out from under us; after conflict checking
      // passed. So do nothing.
    }
  }

  void applyChangesEnd(InternalRegion r, TXStateInterface txState) {
    try {
      try {
        if (uaMods != null) {
          for (final Object o : uaMods.entrySet()) {
            Entry me = (Entry) o;
            Object eKey = me.getKey();
            TXEntryUserAttrState txes = (TXEntryUserAttrState) me.getValue();
            txes.applyChanges(r, eKey);
          }
        }
      } finally {
        r.txLRUEnd();
      }
    } catch (RegionDestroyedException ex) {
      // region was destroyed out from under us; after conflict checking
      // passed. So act as if the region destroy happened right after the
      // commit. We act this way by doing nothing; including distribution
      // of this region's commit data.
    } catch (CancelException ex) {
      // cache was closed out from under us; after conflict checking
      // passed. So do nothing.
    }
  }

  void getEvents(InternalRegion r, ArrayList events, TXState txs) {
    {
      for (final Entry<Object, TXEntryState> objectTXEntryStateEntry : entryMods.entrySet()) {
        Object eKey = ((Entry) objectTXEntryStateEntry).getKey();
        TXEntryState txes = (TXEntryState) ((Entry) objectTXEntryStateEntry).getValue();
        if (txes.isDirty() && txes.isOpAnyEvent(r)) {
          // OFFHEAP: these events are released when TXEvent.release is called
          events.add(txes.getEvent(r, eKey, txs));
        }
      }
    }
  }

  /**
   * Put all the entries this region knows about into the given "entries" list as instances of
   * TXEntryStateWithRegionAndKey.
   */
  void getEntries(ArrayList/* <TXEntryStateWithRegionAndKey> */ entries, InternalRegion r) {
    for (final Entry<Object, TXEntryState> objectTXEntryStateEntry : entryMods.entrySet()) {
      Object eKey = ((Entry) objectTXEntryStateEntry).getKey();
      TXEntryState txes = (TXEntryState) ((Entry) objectTXEntryStateEntry).getValue();
      entries.add(new TXState.TXEntryStateWithRegionAndKey(txes, r, eKey));
    }
  }

  void cleanup(InternalRegion r) {
    if (cleanedUp) {
      return;
    }
    cleanedUp = true;
    for (final TXEntryState es : entryMods.values()) {
      es.cleanup(r);
    }
    region.setInUseByTransaction(false);
  }

  int getChanges() {
    int changes = 0;
    for (final Entry<Object, TXEntryState> objectTXEntryStateEntry : entryMods.entrySet()) {
      TXEntryState txes = (TXEntryState) ((Entry) objectTXEntryStateEntry).getValue();
      if (txes.isDirty()) {
        changes++;
      }
    }
    if (uaMods != null) {
      changes += uaMods.size();
    }
    return changes;
  }

  public TXState getTXState() {
    return txState;
  }

  public void close() {
    for (TXEntryState e : entryMods.values()) {
      e.close();
    }
  }

  @Override
  public String toString() {
    return "{" + super.toString() + " "
        + " ,entryMods=" + entryMods
        + " ,isCreatedDuringCommit=" + isCreatedDuringCommit()
        + "}";
  }

  /**
   * @return the createdDuringCommit
   */
  public boolean isCreatedDuringCommit() {
    return createdDuringCommit;
  }

  /**
   * @param createdDuringCommit the createdDuringCommit to set
   */
  public void setCreatedDuringCommit(boolean createdDuringCommit) {
    this.createdDuringCommit = createdDuringCommit;
  }

  public boolean populateDistTxEntryStateList(ArrayList<DistTxThinEntryState> entryStateList) {
    String regionFullPath = getRegion().getFullPath();
    try {
      if (!entryMods.isEmpty()) {
        // [DISTTX] TODO Sort this first
        for (Entry<Object, TXEntryState> em : entryMods.entrySet()) {
          Object mKey = em.getKey();
          TXEntryState txes = em.getValue();
          DistTxThinEntryState thinEntryState = txes.getDistTxEntryStates();
          entryStateList.add(thinEntryState);
          if (logger.isDebugEnabled()) {
            logger.debug("TXRegionState.populateDistTxEntryStateList Added " + thinEntryState
                + " for key=" + mKey + " ,op=" + txes.opToString() + " ,region=" + regionFullPath);
          }
        }
      }
      return true;
    } catch (RegionDestroyedException ex) {
      // region was destroyed out from under us; after conflict checking
      // passed. So act as if the region destroy happened right after the
      // commit. We act this way by doing nothing; including distribution
      // of this region's commit data.
    } catch (CancelException ex) {
      // cache was closed out from under us; after conflict checking
      // passed. So do nothing.
    }
    if (logger.isDebugEnabled()) {
      logger.debug(
          "TXRegionState.populateDistTxEntryStateList Got exception for region " + regionFullPath);
    }
    return false;
  }

  public void setDistTxEntryStates(ArrayList<DistTxThinEntryState> entryEventList) {
    String regionFullPath = getRegion().getFullPath();
    int entryModsSize = entryMods.size();
    int entryEventListSize = entryEventList.size();
    if (entryModsSize != entryEventListSize) {
      throw new UnsupportedOperationInTransactionException(
          String.format("Expected %s during a distributed transaction but got %s",
              "entry size of " + entryModsSize + " for region " + regionFullPath,
              entryEventListSize));
    }

    int index = 0;
    // [DISTTX] TODO Sort this first
    for (Entry<Object, TXEntryState> em : entryMods.entrySet()) {
      Object mKey = em.getKey();
      TXEntryState txes = em.getValue();
      DistTxThinEntryState thinEntryState = entryEventList.get(index++);
      txes.setDistTxEntryStates(thinEntryState);
      if (logger.isDebugEnabled()) {
        logger.debug("TxRegionState.setDistTxEntryStates Added " + thinEntryState + " for key="
            + mKey + " ,op=" + txes.opToString() + " ,region=" + regionFullPath);
      }
    }
  }
}
