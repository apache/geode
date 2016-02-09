/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.cache;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.TransactionDataRebalancedException;
import com.gemstone.gemfire.cache.TransactionWriter;
import com.gemstone.gemfire.cache.TransactionWriterException;
import com.gemstone.gemfire.cache.UnsupportedOperationInTransactionException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.TXEntryState.DistTxThinEntryState;
import com.gemstone.gemfire.internal.cache.partitioned.PutAllPRMessage;
import com.gemstone.gemfire.internal.cache.partitioned.RemoveAllPRMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.VersionedObjectList;
import com.gemstone.gemfire.internal.cache.tx.DistTxKeyInfo;
import com.gemstone.gemfire.internal.cache.tx.DistTxEntryEvent;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * TxState on a datanode VM
 * 
 * @author vivekb
 * 
 */
public class DistTXState extends TXState {

  private boolean updatingTxStateDuringPreCommit = false;

  public DistTXState(TXStateProxy proxy, boolean onBehalfOfRemoteStub) {
    super(proxy, onBehalfOfRemoteStub);
  }

  @Override
  protected void cleanup() {
    super.cleanup();
    // Do nothing for now
  }

  /*
   * If this is a primary member,
   * for each entry in TXState, generate next region version
   * and store in the entry.
   */
  public void updateRegionVersions() {

    Iterator<Map.Entry<LocalRegion, TXRegionState>> it = this.regions
        .entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<LocalRegion, TXRegionState> me = it.next();
      LocalRegion r = me.getKey();
      TXRegionState txrs = me.getValue();
      
      // Generate next region version only on the primary
      if (!txrs.isCreatedDuringCommit()) {
        try {
          Set entries = txrs.getEntryKeys();
          if (!entries.isEmpty()) {
            Iterator entryIt = entries.iterator();
            while (entryIt.hasNext()) {
              Object key = entryIt.next();
              TXEntryState txes = txrs.getTXEntryState(key);
              RegionVersionVector rvv = r.getVersionVector();
              if (rvv != null) {
                long v = rvv.getNextVersion();
                //txes.setNextRegionVersion(v);
                txes.getDistTxEntryStates().setRegionVersion(v);
                if (logger.isDebugEnabled()) {
                  logger.debug("Set next region version to "+ v + " for region="+r.getName() + "in TXEntryState for key"+key );  
                }
              }
            }
          }
        } catch (DiskAccessException dae) {
          r.handleDiskAccessException(dae);
          throw dae;
        }
      }
    }
  }  
  
  /*
   * Iterate through all changes and for those changes for which
   * this member hosts a primary bucket, generate a tail key and store in
   * the TXEntryState.  From there it is expected to be carried over
   * to the secondaries in phase-2 commit.
   * In phase-2 commit, the both the primary and secondaries should
   * use this tail key to enqueue into parallel queues.
   */
  public void generateTailKeysForParallelDispatcherEvents() {
    Iterator<Map.Entry<LocalRegion, TXRegionState>> it = this.regions
        .entrySet().iterator();

    while (it.hasNext()) {
      Map.Entry<LocalRegion, TXRegionState> me = it.next();
      LocalRegion r = me.getKey();
      TXRegionState txrs = me.getValue();
  
      LocalRegion region = txrs.getRegion();
      // Check if it is a bucket region
      if (region.isUsedForPartitionedRegionBucket()) {
        // Check if it is a primary bucket
        BucketRegion bRegion = (BucketRegion)region;
        if (!(bRegion instanceof AbstractBucketRegionQueue)) {
          if (bRegion.getBucketAdvisor().isPrimary()) {
            
            // Generate a tail key for each entry 
            Set entries = txrs.getEntryKeys();
            if (!entries.isEmpty()) {
              Iterator entryIt = entries.iterator();
              while (entryIt.hasNext()) {
                Object key = entryIt.next();
                TXEntryState txes = txrs.getTXEntryState(key);
                
                long tailKey = ((BucketRegion)region).generateTailKey();    
                txes.getDistTxEntryStates().setTailKey(tailKey);
              } 
            } 
          } // end if primary
        } // end non-hdfs buckets
      }
    }
  }

  
  /*
   * (non-Javadoc)
   * 
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#commit()
   * 
   * Take Locks Does conflict check on primary ([DISTTX] TODO on primary only)
   * Invoke TxWriter
   */
  @Override
  public void precommit() throws CommitConflictException,
      UnsupportedOperationInTransactionException {
    if (logger.isDebugEnabled()) {
      logger.debug("DistTXState.precommit transaction {} is closed {} ",
          getTransactionId(), this.closed, new Throwable());
    }

    if (this.closed) {
      return;
    }
    
    synchronized (this.completionGuard) {
      this.completionStarted = true;
    }

    if (onBehalfOfRemoteStub && !proxy.isCommitOnBehalfOfRemoteStub()) {
      throw new UnsupportedOperationInTransactionException(
          LocalizedStrings.TXState_CANNOT_COMMIT_REMOTED_TRANSACTION
              .toLocalizedString());
    }

    cleanupNonDirtyRegions();

    /*
     * Lock buckets so they can't be rebalanced then perform the conflict check
     * to fix #43489
     */
    try {
      lockBucketRegions();
    } catch (PrimaryBucketException pbe) {
      // not sure what to do here yet
      RuntimeException re = new TransactionDataRebalancedException(
          LocalizedStrings.PartitionedRegion_TRANSACTIONAL_DATA_MOVED_DUE_TO_REBALANCING
              .toLocalizedString());
      re.initCause(pbe);
      throw re;
    }

    if (this.locks == null) {
      reserveAndCheck();
    }

    // For internal testing
    if (this.internalAfterConflictCheck != null) {
      this.internalAfterConflictCheck.run();
    }
    
    updateRegionVersions();
    
    generateTailKeysForParallelDispatcherEvents();
    
    /*
     * If there is a TransactionWriter plugged in, we need to to give it an
     * opportunity to abort the transaction.
     */
    TransactionWriter writer = this.proxy.getTxMgr().getWriter();
    if (!firedWriter && writer != null) {
      try {
        firedWriter = true;
        writer.beforeCommit(getEvent());
      } catch (TransactionWriterException twe) {
        cleanup();
        throw new CommitConflictException(twe);
      } catch (VirtualMachineError err) {
        // cleanup(); this allocates objects so I don't think we can do it -
        // that leaves the TX open, but we are poison pilling so we should be
        // ok??

        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        cleanup(); // rollback the transaction!
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        throw new CommitConflictException(t);
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#commit()
   * 
   * Apply changes release locks
   */
  @Override
  public void commit() throws CommitConflictException {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "DistTXState.commit transaction {} is closed {} ",
          getTransactionId(), this.closed, new Throwable());
    }

    if (this.closed) {
      return;
    }

    try {
      List/* <TXEntryStateWithRegionAndKey> */entries = generateEventOffsets();
      if (logger.isDebugEnabled()) {
        logger.debug("commit entries " + entries);
      }
      TXCommitMessage msg = null;
      try {
        attachFilterProfileInformation(entries);

        if (GemFireCacheImpl.internalBeforeApplyChanges != null) {
          GemFireCacheImpl.internalBeforeApplyChanges.run();
        }
        
        // apply changes to the cache
        applyChanges(entries);
        
        // For internal testing
        if (this.internalAfterApplyChanges != null) {
          this.internalAfterApplyChanges.run();
        }

        // [DISTTX]TODO:
        // Build a message specifically for those nodes who
        // hold gateway senders and listeners but not a copy of the buckets
        // on which changes in this tx are done.
        // This is applicable only for partitioned regions and 
        // serial gateway senders.
        // This works only if the coordinator and sender are not the same node.
        // For same sender as coordinator, this results in a hang, which needs to be addressed.
        // If an another method of notifying adjunct receivers is implemented, 
        // the following two lines should be commented out.
        msg = buildMessageForAdjunctReceivers();
        msg.send(this.locks.getDistributedLockId());

        // Fire callbacks collected in the local txApply* executions
        firePendingCallbacks();
        
        this.commitMessage = buildCompleteMessage();

      } finally {
        if (msg != null) {
          msg.releaseViewVersions();
        }
        this.locks.releaseLocal();
        // For internal testing
        if (this.internalAfterReleaseLocalLocks != null) {
          this.internalAfterReleaseLocalLocks.run();
        }
      }
    } finally {
      cleanup();
    }
  }
  
  /**
   * this builds a new DistTXAdjunctCommitMessage and returns it
   * @return the new message
   */
  protected TXCommitMessage buildMessageForAdjunctReceivers() {
    TXCommitMessage msg = new DistTXAdjunctCommitMessage(this.proxy.getTxId(), this.proxy.getTxMgr().getDM(), this);
    Iterator<Map.Entry<LocalRegion, TXRegionState>> it = this.regions.entrySet().iterator();
    while (it.hasNext()) {
      Map.Entry<LocalRegion, TXRegionState> me = it.next();
      LocalRegion r = me.getKey();
      TXRegionState txrs = me.getValue();
      
      // only on the primary
      if (r.isUsedForPartitionedRegionBucket() && !txrs.isCreatedDuringCommit()) {
        txrs.buildMessageForAdjunctReceivers(r, msg);  
      }
    }
    return msg;
  }


  @Override
  public void rollback() {
    super.rollback();
    // Cleanup is called next
  }

  protected boolean applyOpsOnRedundantCopy(DistributedMember sender,
      ArrayList<DistTxEntryEvent> secondaryTransactionalOperations) {
    boolean returnValue = true;
    try {
      boolean result = true;
      
      // Start TxState Update During PreCommit phase
      setUpdatingTxStateDuringPreCommit(true); 
      
      if (logger.isDebugEnabled()) {
        logger.debug("DistTXState.applyOpOnRedundantCopy: size of "
            + "secondaryTransactionalOperations = {}",
            secondaryTransactionalOperations.size());
      }
      /*
       * Handle Put Operations meant for secondary.
       * 
       * @see com.gemstone.gemfire.internal.cache.partitioned.PutMessage.
       * operateOnPartitionedRegion(DistributionManager, PartitionedRegion,
       * long)
       * 
       * [DISTTX] TODO need to handle other operations
       */
      for (DistTxEntryEvent dtop : secondaryTransactionalOperations) {
        if (logger.isDebugEnabled()) {
          logger.debug("DistTXState.applyOpOnRedundantCopy: processing dist "
              + "tx operation {}", dtop);
        }
        dtop.setDistributedMember(sender);
        dtop.setOriginRemote(false);
        /*
         * [DISTTX} TODO handle call back argument version tag and other
         * settings in PutMessage
         */
        String failureReason = null;
        try {
          if (dtop.getKeyInfo().isDistKeyInfo()) {
            dtop.getKeyInfo().setCheckPrimary(false); 
          }
          else {
            dtop.setKeyInfo(new DistTxKeyInfo(dtop.getKeyInfo()));
            dtop.getKeyInfo().setCheckPrimary(false); 
          }

          //apply the op
          result = applyIndividualOp(dtop);
          
          if (!result) { // make sure the region hasn't gone away
            dtop.getRegion().checkReadiness();
          }
        } catch (CacheWriterException cwe) {
          result = false;
          failureReason = "CacheWriterException";
        } catch (PrimaryBucketException pbe) {
          result = false;
          failureReason = "PrimaryBucketException";
        } catch (InvalidDeltaException ide) {
          result = false;
          failureReason = "InvalidDeltaException";
        } catch (DataLocationException e) {
          result = false;
          failureReason = "DataLocationException";
        }
        if (logger.isDebugEnabled()) {
          logger.debug("DistTXState.applyOpOnRedundantCopy {} ##op {},  "
              + "##region {}, ##key {}", 
              (result ? " sucessfully applied op " : " failed to apply op due to "+ failureReason), 
              dtop.getOperation(), dtop.getRegion().getName(), dtop.getKey());
        }
        if (!result) {
          returnValue = false;
          break;
        }
      }
    } finally {
      // End TxState Update During PreCommit phase
      setUpdatingTxStateDuringPreCommit(false);
    }
    return returnValue;
  }

  /**
   * Apply the individual tx op on secondary
   * 
   * Calls local function such as putEntry instead of putEntryOnRemote as for
   * this {@link DistTXStateOnCoordinator} as events will always be local. In
   * parent {@link DistTXState} class will call remote version of functions
   * 
   */
  protected boolean applyIndividualOp(DistTxEntryEvent dtop)
      throws DataLocationException {
    boolean result = true;
    if (dtop.op.isUpdate() || dtop.op.isCreate()) { 
      if (dtop.op.isPutAll()) {
        assert(dtop.getPutAllOperation() != null);
        //[DISTTX] TODO what do with versions next?
        final VersionedObjectList versions = new VersionedObjectList(
            dtop.getPutAllOperation().putAllDataSize, true,
            dtop.region.concurrencyChecksEnabled);
        postPutAll(dtop.getPutAllOperation(), versions, dtop.region);
      } else {
        result = putEntryOnRemote(dtop, false/* ifNew */,
          dtop.hasDelta()/* ifOld */, null/* expectedOldValue */,
          false/* requireOldValue */, 0L/* lastModified */, true/*
                                                                 * overwriteDestroyed
                                                                 * *not*
                                                                 * used
                                                                 */);
      }
    } else if (dtop.op.isDestroy()) {
      if (dtop.op.isRemoveAll()) {
        assert (dtop.getRemoveAllOperation() != null);
        // [DISTTX] TODO what do with versions next?
        final VersionedObjectList versions = new VersionedObjectList(
            dtop.getRemoveAllOperation().removeAllDataSize, true,
            dtop.region.concurrencyChecksEnabled);
        postRemoveAll(dtop.getRemoveAllOperation(), versions, dtop.region);
      } else {
        destroyOnRemote(dtop, false/* TODO [DISTTX] */, null/*
                                                             * TODO
                                                             * [DISTTX]
                                                             */);
      }
    } else if (dtop.op.isInvalidate()) {
      invalidateOnRemote(dtop, true/* TODO [DISTTX] */, false/*
                                                              * TODO
                                                              * [DISTTX]
                                                              */);
    } else {
      logger.debug("DistTXCommitPhaseOneMessage: unsupported TX operation {}",
          dtop);
      assert (false);
    }
    return result;
  }
  

  public boolean isUpdatingTxStateDuringPreCommit() {
    return updatingTxStateDuringPreCommit;
  }

  /**
   * For Dist Tx
   * 
   * @param updatingTxState
   *          if updating TxState during Commit Phase
   */
  private void setUpdatingTxStateDuringPreCommit(boolean updatingTxState)
      throws UnsupportedOperationInTransactionException {
    this.updatingTxStateDuringPreCommit = updatingTxState;
    if (logger.isDebugEnabled()) {
      logger
          .debug(
              "DistTXState setUpdatingTxStateDuringPreCommit incoming {} final {} ",
              updatingTxState, this.updatingTxStateDuringPreCommit,
              new Throwable()); // [DISTTX] TODO: Remove throwable
    }
  }

  @Override
  public TXRegionState writeRegion(LocalRegion r) {
    TXRegionState result = readRegion(r);
    if (result == null) {
      if (r instanceof BucketRegion) {
        result = new TXBucketRegionState((BucketRegion) r, this);
      } else {
        result = new TXRegionState(r, this);
      }
      result.setCreatedDuringCommit(this.updatingTxStateDuringPreCommit);
      this.regions.put(r, result);
      if (logger.isDebugEnabled()) {
        logger.debug("DistTXState writeRegion flag {} new region-state {} ",
            this.updatingTxStateDuringPreCommit, result);
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("DistTXState writeRegion flag {} region-state {} ",
            this.updatingTxStateDuringPreCommit, result);
      }
    }

    return result;
  }
  
  
  /*
   * [DISTTX] Note: This has been overridden here to associate DistKeyInfo
   * with event to disable primary check(see DistKeyInfo.setCheckPrimary(false)) 
   * when this gets called on secondary of a PR 
   * 
   * For TX this needs to be a PR passed in as region
   * 
   * 
   * @see
   * com.gemstone.gemfire.internal.cache.InternalDataView#postPutAll(com.gemstone
   * .gemfire.internal.cache.DistributedPutAllOperation, java.util.Map,
   * com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public void postPutAll(final DistributedPutAllOperation putallOp,
      final VersionedObjectList successfulPuts, LocalRegion reg) {

    final LocalRegion theRegion;
    if (reg instanceof BucketRegion) {
      theRegion = ((BucketRegion) reg).getPartitionedRegion();
    } else {
      theRegion = reg;
    }
    /*
     * Don't fire events here.
     */
    /*
     * We are on the data store, we don't need to do anything here. Commit will
     * push them out.
     */
    /*
     * We need to put this into the tx state.
     */
    theRegion.syncBulkOp(new Runnable() {
      public void run() {
        // final boolean requiresRegionContext =
        // theRegion.keyRequiresRegionContext();
        InternalDistributedMember myId = theRegion.getDistributionManager()
            .getDistributionManagerId();
        for (int i = 0; i < putallOp.putAllDataSize; ++i) {
          EntryEventImpl ev = PutAllPRMessage.getEventFromEntry(theRegion,
              myId, myId, i, putallOp.putAllData, false, putallOp
                  .getBaseEvent().getContext(), false, !putallOp.getBaseEvent()
                  .isGenerateCallbacks(), false);
          try {
//            ev.setPutAllOperation(putallOp);
            
            // below if condition returns true on secondary when TXState is
            // updated in preCommit only on secondary
            // In this case disable the primary check by calling
            // distKeyInfo.setCheckPrimary(false);
            if (isUpdatingTxStateDuringPreCommit()) {
              KeyInfo keyInfo = ev.getKeyInfo();
              DistTxKeyInfo distKeyInfo = new DistTxKeyInfo(keyInfo);
              distKeyInfo.setCheckPrimary(false);
              ev.setKeyInfo(distKeyInfo);
            }
            /*
             * Whenever commit is called, especially when its a
             * DistTxStateOnCoordinator the txState is set to null in @see
             * TXManagerImpl.commit() and thus when @see LocalRegion.basicPut
             * will be called as in this function, they will not found a TxState
             * with call for getDataView()
             */
            if (!(theRegion.getDataView() instanceof TXStateInterface)) {
              if (putEntry(ev, false, false, null, false, 0L, false)) {
                successfulPuts.addKeyAndVersion(putallOp.putAllData[i].key,
                    null);
              }
            } else if (theRegion.basicPut(ev, false, false, null, false)) {
              successfulPuts.addKeyAndVersion(putallOp.putAllData[i].key, null);
            }
          } finally {
            ev.release();
          }
        }
      }
    }, putallOp.getBaseEvent().getEventId());

  }
  
  @Override
  public void postRemoveAll(final DistributedRemoveAllOperation op,
      final VersionedObjectList successfulOps, LocalRegion reg) {
    final LocalRegion theRegion;
    if (reg instanceof BucketRegion) {
      theRegion = ((BucketRegion) reg).getPartitionedRegion();
    } else {
      theRegion = reg;
    }
    /*
     * Don't fire events here. We are on the data store, we don't need to do
     * anything here. Commit will push them out. We need to put this into the tx
     * state.
     */
    theRegion.syncBulkOp(new Runnable() {
      public void run() {
        InternalDistributedMember myId = theRegion.getDistributionManager()
            .getDistributionManagerId();
        for (int i = 0; i < op.removeAllDataSize; ++i) {
          EntryEventImpl ev = RemoveAllPRMessage.getEventFromEntry(theRegion,
              myId, myId, i, op.removeAllData, false, op.getBaseEvent()
                  .getContext(), false, !op.getBaseEvent()
                  .isGenerateCallbacks());
          ev.setRemoveAllOperation(op);
          // below if condition returns true on secondary when TXState is
          // updated in preCommit only on secondary
          // In this case disable the primary check by calling
          // distKeyInfo.setCheckPrimary(false);
          if (isUpdatingTxStateDuringPreCommit()) {
            KeyInfo keyInfo = ev.getKeyInfo();
            DistTxKeyInfo distKeyInfo = new DistTxKeyInfo(keyInfo);
            distKeyInfo.setCheckPrimary(false);
            ev.setKeyInfo(distKeyInfo);
          }
          /*
           * Whenever commit is called, especially when its a
           * DistTxStateOnCoordinator the txState is set to null in @see
           * TXManagerImpl.commit() and thus when basicDestroy will be called
           * will be called as in i.e. @see LocalRegion.basicDestroy, they will
           * not found a TxState with call for getDataView()
           * 
           * [DISTTX] TODO verify if this is correct to call
           * destroyExistingEntry directly?
           */
          try {
            if (!(theRegion.getDataView() instanceof TXStateInterface)) {
              destroyExistingEntry(ev, true/* should we invoke cacheWriter? */,
                  null);
            } else {
              theRegion.basicDestroy(ev,
                  true/* should we invoke cacheWriter? */, null);
            }
          } catch (EntryNotFoundException ignore) {
          }
          successfulOps.addKeyAndVersion(op.removeAllData[i].key, null);
        }
      }
    }, op.getBaseEvent().getEventId());

  }
  
  @Override
  public boolean isDistTx() {
    return true;
  }
  
  /*
   * Populate list of entry states for each region while replying precommit
   */
  public boolean populateDistTxEntryStateList(
      TreeMap<String, ArrayList<DistTxThinEntryState>> entryStateSortedMap) {
    for (Map.Entry<LocalRegion, TXRegionState> me : this.regions.entrySet()) {
      LocalRegion r = me.getKey();
      TXRegionState txrs = me.getValue();
      String regionFullPath = r.getFullPath();
      if (!txrs.isCreatedDuringCommit()) {
        ArrayList<DistTxThinEntryState> entryStateList = new ArrayList<DistTxThinEntryState>();
        boolean returnValue = txrs.populateDistTxEntryStateList(entryStateList);
        if (returnValue) {
          if (logger.isDebugEnabled()) {
            logger
                .debug("DistTxState.populateDistTxEntryStateList Adding entries "
                    + " with count="
                    + entryStateList.size()
                    + " for region "
                    + regionFullPath + " . Added list=" + entryStateList);
          }
          entryStateSortedMap.put(regionFullPath, entryStateList);
        } else {
          if (logger.isDebugEnabled()) {
            logger
                .debug("DistTxState.populateDistTxEntryStateList Got exception for region "
                    + regionFullPath);
          }
          return false;
        }
      } else {
        if (logger.isDebugEnabled()) {
          logger
              .debug("DistTxState.populateDistTxEntryStateList Not adding entries for region "
                  + regionFullPath);
        }
      }
    }
    return true;
  }
  
  /*
   * Set list of entry states for each region while applying commit
   */
  public void setDistTxEntryStates(
      ArrayList<ArrayList<DistTxThinEntryState>> entryEventList) {
    TreeMap<String, TXRegionState> regionSortedMap = new TreeMap<>();
    for (TXRegionState txrs : this.regions.values()) {
      if (txrs.isCreatedDuringCommit()) {
        regionSortedMap.put(txrs.getRegion().getFullPath(), txrs);
      }
    }

    int index = 0;
    for (Entry<String, TXRegionState> me : regionSortedMap.entrySet()) {
      String regionFullPath = me.getKey();
      TXRegionState txrs = me.getValue();
      ArrayList<DistTxThinEntryState> entryEvents = entryEventList.get(index++);
      if (logger.isDebugEnabled()) {
        logger.debug("DistTxState.setDistTxEntryStates For region="
            + regionFullPath + " ,index=" + index + " ,entryEvents=("
            + entryEvents.size() + ")=" + entryEvents + " ,regionSortedMap="
            + regionSortedMap.keySet());
      }
      txrs.setDistTxEntryStates(entryEvents);
    }
  }
}
