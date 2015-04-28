/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache;
import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.util.concurrent.StoppableReentrantReadWriteLock;

import java.util.*;
import java.util.Map.Entry;

/** TXRegionState is the entity that tracks all the changes a transaction
 * has made to a region.
 *
 * @author Darrel Schneider
 * 
 * @since 4.0
 * 
 * @see TXManagerImpl
 */
public class TXRegionState {
  // A map of Objects (entry keys) -> TXEntryState
  private final HashMap<Object, TXEntryState> entryMods;
  // A map of Objects (entry keys) -> TXEntryUserAttrState
  private HashMap uaMods;
  private Set<InternalDistributedMember> otherMembers = null;
  private LocalRegion region;
  private TXState txState;
  private final boolean needsRefCounts;
  private boolean cleanedUp;
  

  public TXRegionState(LocalRegion r,TXState txState) 
  {
    if (r.getPersistBackup() && !r.isMetaRegionWithTransactions() && !TXManagerImpl.ALLOW_PERSISTENT_TRANSACTIONS) {
      throw new UnsupportedOperationException(LocalizedStrings.TXRegionState_OPERATIONS_ON_PERSISTBACKUP_REGIONS_ARE_NOT_ALLOWED_BECAUSE_THIS_THREAD_HAS_AN_ACTIVE_TRANSACTION.toLocalizedString());
    }
    if (r.getScope().isGlobal()) {
      throw new UnsupportedOperationException(LocalizedStrings.TXRegionState_OPERATIONS_ON_GLOBAL_REGIONS_ARE_NOT_ALLOWED_BECAUSE_THIS_THREAD_HAS_AN_ACTIVE_TRANSACTION.toLocalizedString());
    }
    if (r.hasServerProxy()) {
//      throw new UnsupportedOperationException(LocalizedStrings.TXRegionState_OPERATIONS_ON_REGION_WITH_CLIENT_POOL_ARE_NOT_ALLOWED_BECAUSE_THIS_THREAD_HAS_AN_ACTIVE_TRANSACTION.toLocalizedString());
    }
    this.entryMods = new HashMap<Object, TXEntryState>();
    this.uaMods = null;
    this.region = r;
    this.txState = txState;
    this.needsRefCounts = r.isEntryEvictionPossible() || r.isEntryExpiryPossible();
    r.setInUseByTransaction(true);
  }
  
  public LocalRegion getRegion() {
    return region;
  }
  
  public boolean needsRefCounts() {
    return this.needsRefCounts;
  }

  
  
  public Set getEntryKeys() {
    return Collections.unmodifiableSet(this.entryMods.keySet());
  }
  public TXEntryState readEntry(Object entryKey) {
    return this.entryMods.get(entryKey);
  }
  public TXEntryState createReadEntry(LocalRegion r, Object entryKey, RegionEntry re, Object vId, Object pendingValue) {
    GemFireCacheImpl cache = r.getCache();
    TXEntryState result = cache.getTXEntryStateFactory().createEntry(re, vId, pendingValue, entryKey, this);
    this.entryMods.put(entryKey, result);
    return result;
  }

//   public void rmEntry(Object entryKey, TXState txState, LocalRegion r) {
//     rmEntryUserAttr(entryKey);
//     TXEntryState e = (TXEntryState)this.entryMods.remove(entryKey);
//     if (e != null) {
//       e.cleanup(r);
//     }
//     if (this.uaMods == null && this.entryMods.size() == 0) {
//       txState.rmRegion(r);
//     }
//   }

  public TXEntryUserAttrState readEntryUserAttr(Object entryKey) {
    TXEntryUserAttrState result = null;
    if (this.uaMods != null) {
      result = (TXEntryUserAttrState)this.uaMods.get(entryKey);
    }
    return result;
  }
  public TXEntryUserAttrState writeEntryUserAttr(Object entryKey, LocalRegion r) {
    if (this.uaMods == null) {
      this.uaMods = new HashMap();
    }
    TXEntryUserAttrState result = (TXEntryUserAttrState)this.uaMods.get(entryKey);
    if (result == null) {
      result = new TXEntryUserAttrState(r.basicGetEntryUserAttribute(entryKey));
      this.uaMods.put(entryKey, result);
    }
    return result;
  }
  public void rmEntryUserAttr(Object entryKey) {
    if (this.uaMods != null) {
      if (this.uaMods.remove(entryKey) != null) {
        if (this.uaMods.size() == 0) {
          this.uaMods = null;
        }
      }
    }
  }

  /**
   * Returns the total number of modifications made by this transaction
   * to this region's entry count. The result will have a +1 for every
   * create and a -1 for every destroy.
   */
  int entryCountMod() {
    int result = 0;
    Iterator it = this.entryMods.values().iterator();
    while (it.hasNext()) {
      TXEntryState es = (TXEntryState)it.next();
      result += es.entryCountMod();
    }
    return result;
  }

  /**
   * Fills in a set of any entries created by this transaction for the provided region.
   * @param ret the HashSet to fill in with key objects 
   */
  void fillInCreatedEntryKeys(HashSet ret) {
    Iterator<Entry<Object, TXEntryState>> it = this.entryMods.entrySet().iterator();
    while (it.hasNext()) {
      Entry<Object, TXEntryState> me = it.next();
      TXEntryState txes = me.getValue();
      if (txes.wasCreatedByTX()) {
        ret.add(me.getKey());
      }
    }
  }
  /**
   * Create a lock request on this region state and adds it to req
   */
  void createLockRequest(LocalRegion r, TXLockRequest req) {
    if (this.uaMods == null && this.entryMods.isEmpty()) {
      return;
    }
    if (r.getScope().isDistributed()) {
      DistributedRegion dr = (DistributedRegion)r;
      Set<InternalDistributedMember> advice = dr.getCacheDistributionAdvisor().adviseTX();
      if (!advice.isEmpty()) {
        this.otherMembers = advice; // remember for when it is time to distribute
      }
    }
    //Bypass D-lock for Pr TX
    boolean byPassDLock = false;
    if (r instanceof BucketRegion) {
      //BucketRegion br = (BucketRegion)r;
      //if (br.getRedundancyLevel() < 2) {
        byPassDLock = true;
      //}
    }
    final boolean distributedTX = !byPassDLock && r.getScope().isDistributedAck();
    if (this.uaMods != null
        || (!distributedTX && this.entryMods.size() > 0)) {
      // need some local locks
      TXRegionLockRequestImpl rlr = new TXRegionLockRequestImpl(r);
      if (this.uaMods != null) {
        rlr.addEntryKeys(this.uaMods.keySet());
      }
      if (!distributedTX && this.entryMods.size() > 0) {
        rlr.addEntryKeys(getLockRequestEntryKeys());
      }
      if (!rlr.isEmpty()) {
        req.addLocalRequest(rlr);
      }
    }
    if (distributedTX && this.entryMods.size() > 0) {
      // need some distributed locks
      TXRegionLockRequestImpl rlr = new TXRegionLockRequestImpl(r);
      rlr.addEntryKeys(getLockRequestEntryKeys());
      if (!rlr.isEmpty()) {
        req.setOtherMembers(this.otherMembers);
        req.addDistributedRequest(rlr);
      }
    }
  }
  /**
   * Returns a set of entry keys that this tx needs to request
   * a lock for at commit time.
   * @return <code>null</code> if no entries need to be locked.
   */
  private Set getLockRequestEntryKeys() {
    HashSet result = null;
    Iterator it = this.entryMods.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry me = (Map.Entry)it.next();
      TXEntryState txes = (TXEntryState)me.getValue();
      if (txes.isDirty() && !txes.isOpSearch()) {
        if (result == null) {
          result = new HashSet();
        }
        result.add(me.getKey());
      }
    }
    return result;
  }
    
  void checkForConflicts(LocalRegion r) throws CommitConflictException {
    {
      Iterator it = this.entryMods.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry me = (Map.Entry)it.next();
        Object eKey = me.getKey();
        TXEntryState txes = (TXEntryState)me.getValue();
        txes.checkForConflict(r, eKey);
      }
    }
    if (this.uaMods != null) {
      r.checkReadiness();
      Iterator it = this.uaMods.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry me = (Map.Entry)it.next();
        Object eKey = me.getKey();
        TXEntryUserAttrState txes = (TXEntryUserAttrState)me.getValue();
        txes.checkForConflict(r, eKey);
      }
    }
  }

  /**
   * For each entry that is not dirty (all we did was read it)
   * decrement its refcount (so it can be evicted as we apply our writes)
   * and remove it from entryMods (so we don't keep iterating over it
   * and se we don't try to clean it up again later).
   */
  void cleanupNonDirtyEntries(LocalRegion r) {
    if (!this.entryMods.isEmpty()) {
      Iterator it = this.entryMods.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry me = (Map.Entry)it.next();
        //Object eKey = me.getKey();
        TXEntryState txes = (TXEntryState)me.getValue();
        if (txes.cleanupNonDirty(r)) {
          it.remove();
        }
      }
    }
  }
  
  void buildMessage(LocalRegion r, TXCommitMessage msg) {
    try {
      if (!r.getScope().isLocal() && !this.entryMods.isEmpty()) {
        
        msg.startRegion(r, entryMods.size());
        Iterator it = this.entryMods.entrySet().iterator();
        Set<InternalDistributedMember> newMemberSet = new HashSet<InternalDistributedMember>();
        
        if (r.getScope().isDistributed()) {
          DistributedRegion dr = (DistributedRegion) r;
          msg.addViewVersion(dr, dr.getDistributionAdvisor().startOperation());
          newMemberSet.addAll(dr.getCacheDistributionAdvisor().adviseTX());
        }
        
        while (it.hasNext()) {
          Map.Entry me = (Map.Entry)it.next();
          Object eKey = me.getKey();
          TXEntryState txes = (TXEntryState)me.getValue();
          txes.buildMessage(r, eKey, msg,this.otherMembers);
          if(txes.getFilterRoutingInfo()!=null) {
            newMemberSet.addAll(txes.getFilterRoutingInfo().getMembers());
          }
          if(txes.getAdjunctRecipients()!=null) {
            newMemberSet.addAll(txes.getAdjunctRecipients());
          }
          
        }
        

        
        if (!newMemberSet.equals(this.otherMembers)) { 
          // r.getCache().getLogger().info("DEBUG: participants list has changed! bug 32999."); 
          // Flag the message that the lock manager needs to be updated with the new member set
          msg.setUpdateLockMembers();
          this.otherMembers = newMemberSet;
        }
        
        msg.finishRegion(this.otherMembers);
      }
    }
    catch (RegionDestroyedException ex) {
      // region was destroyed out from under us; after conflict checking
      // passed. So act as if the region destroy happened right after the
      // commit. We act this way by doing nothing; including distribution
      // of this region's commit data.
    }
    catch (CancelException ex) {
      // cache was closed out from under us; after conflict checking
      // passed. So do nothing.
    }
  }
  
  
  void buildCompleteMessage(LocalRegion r, TXCommitMessage msg) {
    try {
      if (!this.entryMods.isEmpty()) {
        msg.startRegion(r, entryMods.size());
        Iterator it = this.entryMods.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry me = (Map.Entry)it.next();
          Object eKey = me.getKey();
          TXEntryState txes = (TXEntryState)me.getValue();
          txes.buildCompleteMessage(r, eKey, msg,this.otherMembers);
        }
        msg.finishRegionComplete();
      }
    }
    catch (RegionDestroyedException ex) {
      // region was destroyed out from under us; after conflict checking
      // passed. So act as if the region destroy happened right after the
      // commit. We act this way by doing nothing; including distribution
      // of this region's commit data.
    }
    catch (CancelException ex) {
      // cache was closed out from under us; after conflict checking
      // passed. So do nothing.
    }
  }
  
  

  void applyChangesStart(LocalRegion r, TXStateInterface txState) {
    try {
      r.txLRUStart();
    } catch (RegionDestroyedException ex) {
      // region was destroyed out from under us; after conflict checking
      // passed. So act as if the region destroy happened right after the
      // commit. We act this way by doing nothing; including distribution
      // of this region's commit data.
    } 
    catch (CancelException ex) {
      // cache was closed out from under us; after conflict checking
      // passed. So do nothing.
    }
  }
  void applyChangesEnd(LocalRegion r, TXStateInterface txState) {
    try {
      try {
        if (this.uaMods != null) {
          Iterator it = this.uaMods.entrySet().iterator();
          while (it.hasNext()) {
            Map.Entry me = (Map.Entry)it.next();
            Object eKey = me.getKey();
            TXEntryUserAttrState txes = (TXEntryUserAttrState)me.getValue();
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
    } 
    catch (CancelException ex) {
      // cache was closed out from under us; after conflict checking
      // passed. So do nothing.
    }
  }

  void getEvents(LocalRegion r, ArrayList events, TXState txs) {
    {
      Iterator it = this.entryMods.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry me = (Map.Entry)it.next();
        Object eKey = me.getKey();
        TXEntryState txes = (TXEntryState)me.getValue();
        if (txes.isDirty() && txes.isOpAnyEvent(r)) {
          events.add(txes.getEvent(r, eKey, txs));
        }
      }
    }
  }

  /**
   * Put all the entries this region knows about into the given "entries" list
   * as instances of TXEntryStateWithRegionAndKey.
   */
  void getEntries(ArrayList/*<TXEntryStateWithRegionAndKey>*/ entries, LocalRegion r) {
    Iterator it = this.entryMods.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry me = (Map.Entry)it.next();
      Object eKey = me.getKey();
      TXEntryState txes = (TXEntryState)me.getValue();
      entries.add(new TXState.TXEntryStateWithRegionAndKey(txes, r, eKey));
    }
  }

  void cleanup(LocalRegion r) {
    if (this.cleanedUp) return;
    this.cleanedUp = true;
    Iterator it = this.entryMods.values().iterator();
    while (it.hasNext()) {
      TXEntryState es = (TXEntryState)it.next();
      es.cleanup(r);
    }
    this.region.setInUseByTransaction(false);
  }
  int getChanges() {
    int changes = 0;
    Iterator it = this.entryMods.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry me = (Map.Entry)it.next();
      TXEntryState txes = (TXEntryState)me.getValue();
      if (txes.isDirty()) {
        changes++;
      }
    }
    if (this.uaMods != null) {
      changes += this.uaMods.size();
    }
    return changes;
  }
  
  public Map<Object,TXEntryState> getEntriesInTxForSqlFabric() {
    return Collections.unmodifiableMap(this.entryMods);
  }

  public TXState getTXState() {
    // TODO Auto-generated method stub
    return txState;
  }
}
