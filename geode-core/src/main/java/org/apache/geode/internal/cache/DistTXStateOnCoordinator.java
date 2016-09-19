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
package org.apache.geode.internal.cache;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeSet;

import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.tx.DistTxEntryEvent;
import org.apache.geode.internal.i18n.LocalizedStrings;

/**
 * TxState on TX coordinator, created when coordinator is also a data node
 * 
 * 
 */
public final class DistTXStateOnCoordinator extends DistTXState implements
    DistTXCoordinatorInterface {
  
  private ArrayList<DistTxEntryEvent> primaryTransactionalOperations = null;
  private ArrayList<DistTxEntryEvent> secondaryTransactionalOperations = null;
  
  private boolean preCommitResponse = false;
  private boolean rollbackResponse = false;

  public DistTXStateOnCoordinator(TXStateProxy proxy,
      boolean onBehalfOfRemoteStub) {
    super(proxy, onBehalfOfRemoteStub);
    primaryTransactionalOperations = new ArrayList<DistTxEntryEvent>();
    secondaryTransactionalOperations = new ArrayList<DistTxEntryEvent>();
  }
  
  public final ArrayList<DistTxEntryEvent> getPrimaryTransactionalOperations()
      throws UnsupportedOperationInTransactionException {
    return primaryTransactionalOperations;
  }
  
  private final void addPrimaryTransactionalOperations(DistTxEntryEvent dtop) {
    if (logger.isDebugEnabled()) {
      // [DISTTX] TODO Remove these
      logger
          .debug("DistTXStateOnCoordinator.addPrimaryTransactionalOperations add "
              + dtop
              + " ,stub before="
              + this
              + " ,isUpdatingTxStateDuringPreCommit="
              + isUpdatingTxStateDuringPreCommit());
    }
    if (!isUpdatingTxStateDuringPreCommit()) {
      primaryTransactionalOperations.add(dtop);
   // [DISTTX] TODO Remove this
      if (logger.isDebugEnabled()) {
        logger.debug("DistTXStateOnCoordinator.addPrimaryTransactionalOperations "
            + " add primary op = {}", dtop);
        
      }
    }
    if (logger.isDebugEnabled()) {
      // [DISTTX] TODO Remove these
      logger
          .debug("DistTXStateOnCoordinator.addPrimaryTransactionalOperations stub after add = "
              + this);
    }
  }
  
  public final void addSecondaryTransactionalOperations(DistTxEntryEvent dtop)
      throws UnsupportedOperationInTransactionException {
    secondaryTransactionalOperations.add(dtop);
  }
  
  @Override
  public void precommit() {
    boolean retVal = applyOpsOnRedundantCopy(this.proxy.getCache()
        .getDistributedSystem().getDistributedMember(),
        this.secondaryTransactionalOperations);
    if (retVal) {
      super.precommit();
    }
    this.preCommitResponse = retVal; // Apply if no exception
  }
  
  @Override
  public void rollback() {
    super.rollback();
    this.rollbackResponse = true; // True if no exception 
    // Cleanup is called next
  }
    
  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.geode.internal.cache.TXStateStub#putEntry(org.apache.geode
   * .internal.cache.EntryEventImpl, boolean, boolean, java.lang.Object,
   * boolean, long, boolean)
   */
  @Override
  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) {
    if (logger.isDebugEnabled()) {
      // [DISTTX] TODO Remove throwable
      logger.debug("DistTXStateOnCoordinator.putEntry "
          + event.getKeyInfo().getKey(), new Throwable());
    }
    
    boolean returnValue = super.putEntry(event, ifNew, ifOld, expectedOldValue,
        requireOldValue, lastModified, overwriteDestroyed);
    
    // putAll event is already added in postPutAll, don't add individual events
    // from the putAll operation again
    if (!event.getOperation().isPutAll()) {
      addPrimaryTransactionalOperations(new DistTxEntryEvent(event));
    }
    return returnValue;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.geode.internal.cache.InternalDataView#putEntryOnRemote(org
   * .apache.geode.internal.cache.EntryEventImpl, boolean, boolean,
   * java.lang.Object, boolean, long, boolean)
   */
  @Override
  public boolean putEntryOnRemote(EntryEventImpl event, boolean ifNew,
      boolean ifOld, Object expectedOldValue, boolean requireOldValue,
      long lastModified, boolean overwriteDestroyed)
      throws DataLocationException {
    if (logger.isDebugEnabled()) {
      // [DISTTX] TODO Remove throwable
      logger.debug("DistTXStateOnCoordinator.putEntryOnRemote "
          + event.getKeyInfo().getKey(), new Throwable());
    }
    
    boolean returnValue = super.putEntryOnRemote(event, ifNew, ifOld, expectedOldValue,
        requireOldValue, lastModified, overwriteDestroyed);
    
    // putAll event is already added in postPutAll, don't add individual events
    // from the putAll operation again
    if (!event.getOperation().isPutAll()) {
      addPrimaryTransactionalOperations(new DistTxEntryEvent(event));
    }
    return returnValue;
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.geode.internal.cache.TXStateInterface#destroyExistingEntry
   * (org.apache.geode.internal.cache.EntryEventImpl, boolean,
   * java.lang.Object)
   */
  public void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue) throws EntryNotFoundException {
//    logger.debug("DistTXStateOnCoordinator.destroyExistingEntry", new Throwable());

    super.destroyExistingEntry(event, cacheWrite, expectedOldValue);
    
    // removeAll event is already added in postRemoveAll, don't add individual
    // events from the removeAll operation again
    if (!event.getOperation().isRemoveAll()) {
      addPrimaryTransactionalOperations(new DistTxEntryEvent(event));
    }
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.geode.internal.cache.InternalDataView#destroyOnRemote(java
   * .lang.Integer, org.apache.geode.internal.cache.EntryEventImpl,
   * java.lang.Object)
   */
  public void destroyOnRemote(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue) throws DataLocationException {
//    logger.debug("DistTXStateOnCoordinator.destroyOnRemote", new Throwable());
    
    super.destroyOnRemote(event, cacheWrite, expectedOldValue);
    
    // removeAll event is already added in postRemoveAll, don't add individual
    // events from the removeAll operation again
    if (!event.getOperation().isRemoveAll()) {
    addPrimaryTransactionalOperations(new DistTxEntryEvent(event));
    }
  }
  
  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.geode.internal.cache.TXStateInterface#invalidateExistingEntry
   * (org.apache.geode.internal.cache.EntryEventImpl, boolean, boolean)
   */
  public void invalidateExistingEntry(EntryEventImpl event,
      boolean invokeCallbacks, boolean forceNewEntry) {
//    logger
//        .debug("DistTXStateOnCoordinator.invalidateExistingEntry", new Throwable());
    
    super.invalidateExistingEntry(event, invokeCallbacks, forceNewEntry);
    addPrimaryTransactionalOperations(new DistTxEntryEvent(event));
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.apache.geode.internal.cache.InternalDataView#invalidateOnRemote
   * (org.apache.geode.internal.cache.EntryEventImpl, boolean, boolean)
   */
  public void invalidateOnRemote(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) throws DataLocationException {
//    logger.debug("DistTXStateOnCoordinator.invalidateOnRemote", new Throwable());
    super.invalidateExistingEntry(event, invokeCallbacks, forceNewEntry);
    addPrimaryTransactionalOperations(new DistTxEntryEvent(event));
  }
  
  
  public void postPutAll(DistributedPutAllOperation putallOp,
      VersionedObjectList successfulPuts, LocalRegion region) {
    super.postPutAll(putallOp, successfulPuts, region);
    // TODO DISTTX: event is never released
    EntryEventImpl event = EntryEventImpl.createPutAllEvent(putallOp, region,
        Operation.PUTALL_CREATE, putallOp.getBaseEvent().getKey(), putallOp
            .getBaseEvent().getValue());
    event.setEventId(putallOp.getBaseEvent().getEventId());
    DistTxEntryEvent dtop = new DistTxEntryEvent(event);
    dtop.setPutAllOperation(putallOp);
    addPrimaryTransactionalOperations(dtop);
  }
  
  public void postRemoveAll(DistributedRemoveAllOperation removeAllOp,
      VersionedObjectList successfulOps, LocalRegion region) {
    super.postRemoveAll(removeAllOp, successfulOps, region);
    // TODO DISTTX: event is never released
    EntryEventImpl event = EntryEventImpl.createRemoveAllEvent(removeAllOp,
        region, removeAllOp.getBaseEvent().getKey());
    event.setEventId(removeAllOp.getBaseEvent().getEventId());
    DistTxEntryEvent dtop = new DistTxEntryEvent(event);
    dtop.setRemoveAllOperation(removeAllOp);
    addPrimaryTransactionalOperations(dtop);
  }
  
  @Override
  public boolean getPreCommitResponse()
      throws UnsupportedOperationInTransactionException {
    return this.preCommitResponse;
  }

  @Override
  public boolean getRollbackResponse()
      throws UnsupportedOperationInTransactionException {
    return this.rollbackResponse;
  }
  
  @Override
  public void setPrecommitMessage(DistTXPrecommitMessage precommitMsg, DM dm)
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        LocalizedStrings.Dist_TX_PRECOMMIT_NOT_SUPPORTED_IN_A_TRANSACTION
            .toLocalizedString("setPrecommitMessage"));
  }
  
  @Override
  public void setCommitMessage(DistTXCommitMessage commitMsg, DM dm)
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        LocalizedStrings.Dist_TX_PRECOMMIT_NOT_SUPPORTED_IN_A_TRANSACTION
            .toLocalizedString("setCommitMessage"));
  }

  @Override
  public void setRollbackMessage(DistTXRollbackMessage rollbackMsg, DM dm)
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        LocalizedStrings.Dist_TX_ROLLBACK_NOT_SUPPORTED_IN_A_TRANSACTION
            .toLocalizedString("setRollbackMessage"));
  }
  
  @Override
  public void gatherAffectedRegions(HashSet<LocalRegion> regionSet,
      boolean includePrimaryRegions, boolean includeRedundantRegions)
      throws UnsupportedOperationInTransactionException {
    if (includePrimaryRegions) {
      for (DistTxEntryEvent dtos : this.primaryTransactionalOperations) {
        regionSet.add(dtos.getRegion());
      }
    }
    if (includeRedundantRegions) {
      for (DistTxEntryEvent dtos : this.secondaryTransactionalOperations) {
        regionSet.add(dtos.getRegion());
      }
    }
  }
  
  @Override
  public void gatherAffectedRegionsName(TreeSet<String> sortedRegionName,
      boolean includePrimaryRegions, boolean includeRedundantRegions)
      throws UnsupportedOperationInTransactionException {
    if (includePrimaryRegions) {
      gatherAffectedRegions(sortedRegionName,
          this.primaryTransactionalOperations);
    }
    if (includeRedundantRegions) {
      gatherAffectedRegions(sortedRegionName,
          this.secondaryTransactionalOperations);
    }
  }
  
  public static void gatherAffectedRegions(TreeSet<String> sortedRegionName,
      ArrayList<DistTxEntryEvent> regionOps) {
    for (DistTxEntryEvent dtos : regionOps) {
      LocalRegion lr = dtos.getRegion();
      if (lr instanceof PartitionedRegion) {
        sortedRegionName.add(PartitionedRegionHelper.getBucketFullPath(
            lr.getFullPath(), dtos.getKeyInfo().getBucketId()));
      } else {
        sortedRegionName.add(lr.getFullPath());
      }
    }
  }
  
  /**
   * {@inheritDoc}
   * 
   */
  @Override
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
        result = putEntry(dtop, false/* ifNew */,
          false/* ifOld */, null/* expectedOldValue */,
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
        destroyExistingEntry(dtop, false/* TODO [DISTTX] */, null/*
                                                             * TODO
                                                             * [DISTTX]
                                                             */);
      }
    } else if (dtop.op.isInvalidate()) {
      invalidateExistingEntry(dtop, true/* TODO [DISTTX] */, false/*
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

  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(super.toString());
    builder.append(" ,primary txOps=").append(this.primaryTransactionalOperations);
    builder.append(" ,secondary txOps=").append(this.secondaryTransactionalOperations);
    builder.append(" ,preCommitResponse=").append(this.preCommitResponse);
    builder.append(" ,rollbackResponse=").append(this.rollbackResponse);
    return builder.toString();
  }

  @Override
  public boolean isCreatedOnDistTxCoordinator() {
    return true;
  }
  
  @Override
  public void finalCleanup() {
    cleanup();
  }
}
