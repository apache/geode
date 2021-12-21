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
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.tx.DistTxEntryEvent;

/**
 * DistPeerTXStateStub lives on the transaction coordinator for a distributed transaction </br>
 * 1. It forwards TX operations to primary or a selected replica (in case of RR) for each op </br>
 * 2.It also records those transactional operations in order to send those to
 * secondaries/replicas(in one batch) at commit time.
 */
public class DistPeerTXStateStub extends PeerTXStateStub implements DistTXCoordinatorInterface {

  private ArrayList<DistTxEntryEvent> primaryTransactionalOperations = null;
  private ArrayList<DistTxEntryEvent> secondaryTransactionalOperations = null;
  private DistTXPrecommitMessage precommitDistTxMsg = null;
  private DistTXCommitMessage commitDistTxMsg = null;
  private DistTXRollbackMessage rollbackDistTxMsg = null;
  private DistributionManager dm = null;

  public DistPeerTXStateStub(TXStateProxy stateProxy, DistributedMember target,
      InternalDistributedMember onBehalfOfClient) {
    super(stateProxy, target, onBehalfOfClient);
    primaryTransactionalOperations = new ArrayList<DistTxEntryEvent>();
    secondaryTransactionalOperations = new ArrayList<DistTxEntryEvent>();
  }

  @Override
  public void precommit() throws CommitConflictException {
    if (logger.isDebugEnabled()) {
      logger.debug("DistPeerTXStateStub.precommit target=" + target
          + " ,primaryTransactionalOperations=" + primaryTransactionalOperations
          + " ,secondaryTransactionalOperations=" + secondaryTransactionalOperations);
    }
    assert target != null;
    assert primaryTransactionalOperations != null || secondaryTransactionalOperations != null;

    // [DISTTX] TODO Handle Stats

    precommitDistTxMsg.setSecondaryTransactionalOperations(secondaryTransactionalOperations);
    final Set<DistributedMember> recipients = Collections.singleton(target);
    precommitDistTxMsg.setRecipients(recipients);
    dm.putOutgoing(precommitDistTxMsg);
    precommitDistTxMsg.resetRecipients();

    // TODO [DISTTX] any precommit hooks
  }

  @Override
  public void commit() throws CommitConflictException {
    if (logger.isDebugEnabled()) {
      logger.debug("DistPeerTXStateStub.commit target=" + target);
    }

    // [DISTTX] TODO Handle Stats
    dm.getStats().incSentCommitMessages(1L);

    final Set<DistributedMember> recipients = Collections.singleton(target);
    commitDistTxMsg.setRecipients(recipients);
    dm.putOutgoing(commitDistTxMsg);
    commitDistTxMsg.resetRecipients();
  }

  @Override
  public void rollback() {
    if (logger.isDebugEnabled()) {
      logger.debug("DistPeerTXStateStub.rollback target=" + target);
    }

    // [DISTTX] TODO Handle callbacks
    // if (this.internalAfterSendRollback != null) {
    // this.internalAfterSendRollback.run();
    // }

    final Set<DistributedMember> recipients = Collections.singleton(target);
    rollbackDistTxMsg.setRecipients(recipients);
    dm.putOutgoing(rollbackDistTxMsg);
    rollbackDistTxMsg.resetRecipients();
  }

  @Override
  public ArrayList<DistTxEntryEvent> getPrimaryTransactionalOperations()
      throws UnsupportedOperationInTransactionException {
    return primaryTransactionalOperations;
  }

  private void addPrimaryTransactionalOperations(DistTxEntryEvent dtop) {
    if (logger.isDebugEnabled()) {
      // [DISTTX] TODO Remove these
      logger.debug("DistPeerTXStateStub.addPrimaryTransactionalOperations add " + dtop
          + " ,stub before=" + this);
    }
    primaryTransactionalOperations.add(dtop);
    if (logger.isDebugEnabled()) {
      // [DISTTX] TODO Remove these
      logger
          .debug("DistPeerTXStateStub.addPrimaryTransactionalOperations stub after add = " + this);
    }
  }

  @Override
  public void addSecondaryTransactionalOperations(DistTxEntryEvent dtop)
      throws UnsupportedOperationInTransactionException {
    secondaryTransactionalOperations.add(dtop);
  }

  @Override
  protected void cleanup() {
    super.cleanup();
  }

  @Override
  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) {
    return putEntry(event, ifNew, ifOld, expectedOldValue, requireOldValue, lastModified,
        overwriteDestroyed, true,
        false);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateStub#putEntry(org.apache.geode
   * .internal.cache.EntryEventImpl, boolean, boolean, java.lang.Object, boolean, long, boolean)
   */
  @Override
  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed, boolean invokeCallbacks, boolean throwConcurrentModification) {
    if (logger.isDebugEnabled()) {
      // [DISTTX] TODO Remove throwable
      logger.debug("DistPeerTXStateStub.putEntry " + event.getKeyInfo().getKey(), new Throwable());
    }
    boolean returnValue = super.putEntry(event, ifNew, ifOld, expectedOldValue, requireOldValue,
        lastModified, overwriteDestroyed, invokeCallbacks, throwConcurrentModification);
    addPrimaryTransactionalOperations(new DistTxEntryEvent(event));

    return returnValue;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#putEntryOnRemote(org
   * .apache.geode.internal.cache.EntryEventImpl, boolean, boolean, java.lang.Object, boolean, long,
   * boolean)
   */
  @Override
  public boolean putEntryOnRemote(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) throws DataLocationException {
    if (logger.isDebugEnabled()) {
      // [DISTTX] TODO Remove throwable
      logger.debug("DistPeerTXStateStub.putEntryOnRemote " + event.getKeyInfo().getKey(),
          new Throwable());
    }
    boolean returnValue = super.putEntryOnRemote(event, ifNew, ifOld, expectedOldValue,
        requireOldValue, lastModified, overwriteDestroyed);
    addPrimaryTransactionalOperations(new DistTxEntryEvent(event));

    return returnValue;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#destroyExistingEntry
   * (org.apache.geode.internal.cache.EntryEventImpl, boolean, java.lang.Object)
   */
  @Override
  public void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue) throws EntryNotFoundException {
    // logger.debug("DistPeerTXStateStub.destroyExistingEntry", new Throwable());
    primaryTransactionalOperations.add(new DistTxEntryEvent(event));
    super.destroyExistingEntry(event, cacheWrite, expectedOldValue);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#destroyOnRemote(java .lang.Integer,
   * org.apache.geode.internal.cache.EntryEventImpl, java.lang.Object)
   */
  @Override
  public void destroyOnRemote(EntryEventImpl event, boolean cacheWrite, Object expectedOldValue)
      throws DataLocationException {
    // logger.debug("DistPeerTXStateStub.destroyOnRemote", new Throwable());
    super.destroyOnRemote(event, cacheWrite, expectedOldValue);
    primaryTransactionalOperations.add(new DistTxEntryEvent(event));
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#invalidateExistingEntry
   * (org.apache.geode.internal.cache.EntryEventImpl, boolean, boolean)
   */
  @Override
  public void invalidateExistingEntry(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) {
    // logger
    // .debug("DistPeerTXStateStub.invalidateExistingEntry", new Throwable());
    super.invalidateExistingEntry(event, invokeCallbacks, forceNewEntry);
    primaryTransactionalOperations.add(new DistTxEntryEvent(event));
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.InternalDataView#invalidateOnRemote
   * (org.apache.geode.internal.cache.EntryEventImpl, boolean, boolean)
   */
  @Override
  public void invalidateOnRemote(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) throws DataLocationException {
    // logger.debug("DistPeerTXStateStub.invalidateOnRemote", new Throwable());
    super.invalidateExistingEntry(event, invokeCallbacks, forceNewEntry);
    primaryTransactionalOperations.add(new DistTxEntryEvent(event));
  }

  @Override
  public void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,
      InternalRegion reg) {
    super.postPutAll(putallOp, successfulPuts, reg);
    // TODO DISTTX: event is never released
    EntryEventImpl event = EntryEventImpl.createPutAllEvent(putallOp, reg, Operation.PUTALL_CREATE,
        putallOp.getBaseEvent().getKey(), putallOp.getBaseEvent().getValue());
    event.setEventId(putallOp.getBaseEvent().getEventId());
    DistTxEntryEvent dtop = new DistTxEntryEvent(event);
    dtop.setPutAllOperation(putallOp);
    primaryTransactionalOperations.add(dtop);
  }

  @Override
  public void postRemoveAll(DistributedRemoveAllOperation removeAllOp,
      VersionedObjectList successfulOps, InternalRegion reg) {
    super.postRemoveAll(removeAllOp, successfulOps, reg);
    // TODO DISTTX: event is never released
    EntryEventImpl event =
        EntryEventImpl.createRemoveAllEvent(removeAllOp, reg, removeAllOp.getBaseEvent().getKey());
    event.setEventId(removeAllOp.getBaseEvent().getEventId());
    DistTxEntryEvent dtop = new DistTxEntryEvent(event);
    dtop.setRemoveAllOperation(removeAllOp);
    primaryTransactionalOperations.add(dtop);
  }


  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(super.toString());
    builder.append(" ,primary txOps=").append(primaryTransactionalOperations);
    builder.append(" ,secondary txOps=").append(secondaryTransactionalOperations);
    return builder.toString();
  }

  @Override
  public boolean getPreCommitResponse() throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        String.format("precommit() operation %s meant for Dist Tx is not supported",
            "getPreCommitResponse"));
  }

  @Override
  public boolean getRollbackResponse() throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        String.format("rollback() operation %s meant for Dist Tx is not supported",
            "getRollbackResponse"));
  }

  @Override
  public void setPrecommitMessage(DistTXPrecommitMessage precommitMsg, DistributionManager dm)
      throws UnsupportedOperationInTransactionException {
    precommitDistTxMsg = precommitMsg;
    this.dm = dm;
  }

  @Override
  public void setCommitMessage(DistTXCommitMessage commitMsg, DistributionManager dm)
      throws UnsupportedOperationInTransactionException {
    commitDistTxMsg = commitMsg;
    this.dm = dm;
  }

  @Override
  public void setRollbackMessage(DistTXRollbackMessage rollbackMsg, DistributionManager dm)
      throws UnsupportedOperationInTransactionException {
    rollbackDistTxMsg = rollbackMsg;
    this.dm = dm;
  }

  @Override
  public void gatherAffectedRegions(HashSet<InternalRegion> regionSet,
      boolean includePrimaryRegions, boolean includeRedundantRegions)
      throws UnsupportedOperationInTransactionException {
    if (includePrimaryRegions) {
      for (DistTxEntryEvent dtos : primaryTransactionalOperations) {
        regionSet.add(dtos.getRegion());
      }
    }
    if (includeRedundantRegions) {
      for (DistTxEntryEvent dtos : secondaryTransactionalOperations) {
        regionSet.add(dtos.getRegion());
      }
    }
  }

  @Override
  public void gatherAffectedRegionsName(TreeSet<String> sortedRegionName,
      boolean includePrimaryRegions, boolean includeRedundantRegions)
      throws UnsupportedOperationInTransactionException {
    if (includePrimaryRegions) {
      DistTXStateOnCoordinator.gatherAffectedRegions(sortedRegionName,
          primaryTransactionalOperations);
    }
    if (includeRedundantRegions) {
      DistTXStateOnCoordinator.gatherAffectedRegions(sortedRegionName,
          secondaryTransactionalOperations);
    }
  }

  @Override
  public boolean isDistTx() {
    return true;
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
