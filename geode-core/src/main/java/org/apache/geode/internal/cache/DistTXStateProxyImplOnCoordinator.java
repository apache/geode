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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.TransactionInDoubtException;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.DistTXPrecommitMessage.DistTxPrecommitResponse;
import org.apache.geode.internal.cache.TXEntryState.DistTxThinEntryState;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.tx.DistClientTXStateStub;
import org.apache.geode.internal.cache.tx.DistTxEntryEvent;

public class DistTXStateProxyImplOnCoordinator extends DistTXStateProxyImpl {

  /**
   * A map of distributed system member to either {@link DistPeerTXStateStub} or
   * {@link DistTXStateOnCoordinator} (in case of TX coordinator is also a data node)
   */
  private final HashMap<DistributedMember, DistTXCoordinatorInterface> target2realDeals =
      new HashMap<>();

  private HashMap<InternalRegion, DistributedMember> rrTargets;

  private Set<DistributedMember> txRemoteParticpants = null; // other than local

  private HashMap<String, ArrayList<DistTxThinEntryState>> txEntryEventMap = null;

  public DistTXStateProxyImplOnCoordinator(InternalCache cache, TXManagerImpl managerImpl, TXId id,
      InternalDistributedMember clientMember) {
    super(cache, managerImpl, id, clientMember);
  }

  public DistTXStateProxyImplOnCoordinator(InternalCache cache, TXManagerImpl managerImpl, TXId id,
      boolean isjta) {
    super(cache, managerImpl, id, isjta);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#commit()
   *
   * [DISTTX] TODO Catch all exceptions in precommit and rollback and make sure these messages reach
   * all
   */
  @Override
  public void commit() throws CommitConflictException {
    boolean preserveTx = false;
    boolean precommitResult = false;
    try {
      // create a map of secondary(for PR) / replica(for RR) to stubs to send
      // commit message to those
      HashMap<DistributedMember, DistTXCoordinatorInterface> otherTargets2realDeals =
          getSecondariesAndReplicasForTxOps();
      // add it to the existing map and then send commit to all copies
      target2realDeals.putAll(otherTargets2realDeals);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator.commit target2realDeals = " + target2realDeals);
      }

      precommitResult = doPrecommit();
      if (precommitResult) {
        if (logger.isDebugEnabled()) {
          logger.debug("DistTXStateProxyImplOnCoordinator.commit Going for commit ");
        }
        boolean phase2commitDone = doCommit();
        if (logger.isDebugEnabled()) {
          logger.debug("DistTXStateProxyImplOnCoordinator.commit Commit "
              + (phase2commitDone ? "Done" : "Failed"));
        }
        if (!phase2commitDone) {
          throw new TransactionInDoubtException(
              "Commit failed on cache server");
        }
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "DistTXStateProxyImplOnCoordinator.commit precommitResult = " + precommitResult);
        }
      }
    } catch (UnsupportedOperationInTransactionException e) {
      // fix for #42490
      preserveTx = true;
      throw e;
    } finally {
      if (!precommitResult) {
        rollback();
      }

      inProgress = preserveTx;
    }
  }

  /**
   * creates a map of all secondaries(for PR) / replicas(for RR) to stubs to send commit message to
   * those
   */
  private HashMap<DistributedMember, DistTXCoordinatorInterface> getSecondariesAndReplicasForTxOps() {
    InternalDistributedMember currentNode =
        getCache().getInternalDistributedSystem().getDistributedMember();

    HashMap<DistributedMember, DistTXCoordinatorInterface> secondaryTarget2realDeals =
        new HashMap<>();
    for (Entry<DistributedMember, DistTXCoordinatorInterface> e : target2realDeals.entrySet()) {
      DistributedMember originalTarget = e.getKey();
      DistTXCoordinatorInterface distPeerTxStateStub = e.getValue();

      ArrayList<DistTxEntryEvent> primaryTxOps =
          distPeerTxStateStub.getPrimaryTransactionalOperations();
      for (DistTxEntryEvent dtop : primaryTxOps) {
        InternalRegion internalRegion = dtop.getRegion();
        // replicas or secondaries
        Set<InternalDistributedMember> otherNodes = null;
        if (internalRegion instanceof PartitionedRegion) {
          Set<InternalDistributedMember> allNodes = ((PartitionedRegion) dtop.getRegion())
              .getRegionAdvisor().getBucketOwners(dtop.getKeyInfo().getBucketId());
          allNodes.remove(originalTarget);
          otherNodes = allNodes;
        } else if (internalRegion instanceof DistributedRegion) {
          otherNodes = ((DistributedRegion) internalRegion).getCacheDistributionAdvisor()
              .adviseInitializedReplicates();
          otherNodes.remove(originalTarget);
        }

        if (otherNodes != null) {
          for (InternalDistributedMember dm : otherNodes) {
            // whether the target already exists due to other Tx op on the node
            DistTXCoordinatorInterface existingDistPeerTXStateStub = target2realDeals.get(dm);
            if (existingDistPeerTXStateStub == null) {
              existingDistPeerTXStateStub = secondaryTarget2realDeals.get(dm);
              if (existingDistPeerTXStateStub == null) {
                DistTXCoordinatorInterface newTxStub = null;
                if (currentNode.equals(dm)) {
                  // [DISTTX] TODO add a test case for this condition?
                  newTxStub = new DistTXStateOnCoordinator(this, false);
                } else {
                  newTxStub = new DistPeerTXStateStub(this, dm, onBehalfOfClientMember);
                }
                newTxStub.addSecondaryTransactionalOperations(dtop);
                secondaryTarget2realDeals.put(dm, newTxStub);
              } else {
                existingDistPeerTXStateStub.addSecondaryTransactionalOperations(dtop);
              }
            } else {
              existingDistPeerTXStateStub.addSecondaryTransactionalOperations(dtop);
            }
          }
        }
      }
    }
    return secondaryTarget2realDeals;
  }

  @Override
  public void rollback() {
    if (logger.isDebugEnabled()) {
      logger.debug("DistTXStateProxyImplOnCoordinator.rollback Going for rollback ");
    }

    boolean finalResult = false;
    final DistributionManager dm = getCache().getDistributionManager();
    try {
      // Create Tx Participants
      Set<DistributedMember> txRemoteParticpants = getTxRemoteParticpants(dm);

      // create processor and rollback message
      DistTXRollbackMessage.DistTxRollbackReplyProcessor processor =
          new DistTXRollbackMessage.DistTxRollbackReplyProcessor(this.getTxId(), dm,
              txRemoteParticpants, target2realDeals);
      // TODO [DISTTX} whats ack threshold?
      processor.enableSevereAlertProcessing();
      final DistTXRollbackMessage rollbackMsg =
          new DistTXRollbackMessage(this.getTxId(), this.onBehalfOfClientMember, processor);

      // send rollback message to remote nodes
      for (DistributedMember remoteNode : txRemoteParticpants) {
        DistTXCoordinatorInterface remoteTXStateStub = target2realDeals.get(remoteNode);
        if (remoteTXStateStub.isTxState()) {
          throw new UnsupportedOperationInTransactionException(
              String.format("Expected %s during a distributed transaction but got %s",
                  "DistPeerTXStateStub",
                  remoteTXStateStub.getClass().getSimpleName()));
        }
        try {
          remoteTXStateStub.setRollbackMessage(rollbackMsg, dm);
          remoteTXStateStub.rollback();
        } finally {
          remoteTXStateStub.setRollbackMessage(null, null);
          remoteTXStateStub.finalCleanup();
        }
        if (logger.isDebugEnabled()) { // TODO - make this trace level
          logger.debug("DistTXStateProxyImplOnCoordinator.rollback target = " + remoteNode);
        }
      }

      // Do rollback on local node
      DistTXCoordinatorInterface localTXState = target2realDeals.get(dm.getId());
      if (localTXState != null) {
        if (!localTXState.isTxState()) {
          throw new UnsupportedOperationInTransactionException(
              String.format("Expected %s during a distributed transaction but got %s",
                  "DistTXStateOnCoordinator",
                  localTXState.getClass().getSimpleName()));
        }
        localTXState.rollback();
        boolean localResult = localTXState.getRollbackResponse();
        if (logger.isDebugEnabled()) {
          logger.debug("DistTXStateProxyImplOnCoordinator.rollback local = " + dm.getId()
              + " ,result= " + localResult + " ,finalResult-old= " + finalResult);
        }
        finalResult = finalResult && localResult;
      }

      /*
       * [DISTTX] TODO Any test hooks
       */
      // if (internalAfterIndividualSend != null) {
      // internalAfterIndividualSend.run();
      // }

      /*
       * [DISTTX] TODO see how to handle exception
       */

      /*
       * [DISTTX] TODO Any test hooks
       */
      // if (internalAfterIndividualCommitProcess != null) {
      // // Testing callback
      // internalAfterIndividualCommitProcess.run();
      // }

      { // Wait for results
        dm.getCancelCriterion().checkCancelInProgress(null);
        processor.waitForPrecommitCompletion();

        // [DISTTX} TODO Handle stats
        // dm.getStats().incCommitWaits();

        Map<DistributedMember, Boolean> remoteResults = processor.getRollbackResponseMap();
        for (Entry<DistributedMember, Boolean> e : remoteResults.entrySet()) {
          DistributedMember target = e.getKey();
          Boolean remoteResult = e.getValue();
          if (logger.isDebugEnabled()) { // TODO - make this trace level
            logger.debug("DistTXStateProxyImplOnCoordinator.rollback target = " + target
                + " ,result= " + remoteResult + " ,finalResult-old= " + finalResult);
          }
          finalResult = finalResult && remoteResult;
        }
      }

    } finally {
      inProgress = false;
    }

    /*
     * [DISTTX] TODO Write similar method to take out exception
     *
     * [DISTTX] TODO Handle Reliable regions
     */
    // if (this.hasReliableRegions) {
    // checkDistributionReliability(distMap, processor);
    // }

    if (logger.isDebugEnabled()) {
      logger.debug("DistTXStateProxyImplOnCoordinator.rollback finalResult= " + finalResult);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TXStateInterface getRealDeal(KeyInfo key, InternalRegion r) {
    if (r != null) {
      target = null;
      // wait for the region to be initialized fixes bug 44652
      r.waitOnInitialization(r.getInitializationLatchBeforeGetInitialImage());
      if (r instanceof PartitionedRegion) {
        target = getOwnerForKey(r, key);
      } else if (r instanceof BucketRegion) {
        target = ((BucketRegion) r).getBucketAdvisor().getPrimary();
        // target = r.getMyId();
      } else { // replicated region
        target = getRRTarget(key, r);
      }
      this.realDeal = target2realDeals.get(target);
    }
    if (this.realDeal == null) {
      // assert (r != null);
      if (r == null) { // TODO: stop gap to get tests working
        this.realDeal = new DistTXStateOnCoordinator(this, false);
        target = this.txMgr.getDM().getId();
      } else {
        // Code to keep going forward
        if (r.hasServerProxy()) {
          // TODO [DISTTX] See what we need for client?
          this.realDeal =
              new DistClientTXStateStub(r.getCache(), r.getDistributionManager(), this, target, r);
          if (r.getScope().isDistributed()) {
            if (txDistributedClientWarningIssued.compareAndSet(false, true)) {
              logger.warn(
                  "Distributed region {} is being used in a client-initiated transaction.  The transaction will only affect servers and this client.  To keep from seeing this message use 'local' scope in client regions used in transactions.",
                  r.getFullPath());
            }
          }
        } else {
          // (r != null) code block above
          if (target == null || target.equals(this.txMgr.getDM().getId())) {
            this.realDeal = new DistTXStateOnCoordinator(this, false);
          } else {
            this.realDeal = new DistPeerTXStateStub(this, target, onBehalfOfClientMember);
          }
        }
      }
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator::getRealDeal Built a new TXState: {} txMge:{} proxy {} target {}",
            this.realDeal, this.txMgr.getDM().getId(), this, target/* , new Throwable() */);
      }
      target2realDeals.put(target, (DistTXCoordinatorInterface) realDeal);
      if (logger.isDebugEnabled()) {
        logger
            .debug("DistTXStateProxyImplOnCoordinator.getRealDeal added TxState target2realDeals = "
                + target2realDeals);
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator::getRealDeal Found TXState: {} proxy {} target {} target2realDeals {}",
            this.realDeal, this, target, target2realDeals);
      }
    }
    return this.realDeal;
  }

  @Override
  public TXStateInterface getRealDeal(DistributedMember t) {
    assert t != null;
    this.realDeal = target2realDeals.get(target);
    if (this.realDeal == null) {
      this.target = t;
      this.realDeal = new DistPeerTXStateStub(this, target, onBehalfOfClientMember);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator::getRealDeal(t) Built a new TXState: {} me:{}",
            this.realDeal, this.txMgr.getDM().getId());
      }
      if (!this.realDeal.isDistTx() || this.realDeal.isCreatedOnDistTxCoordinator()
          || !this.realDeal.isTxState()) {
        throw new UnsupportedOperationInTransactionException(
            String.format("Expected %s during a distributed transaction but got %s",
                "DistPeerTXStateStub", this.realDeal.getClass().getSimpleName()));
      }
      target2realDeals.put(target, (DistPeerTXStateStub) realDeal);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator.getRealDeal(t) added TxState target2realDeals = "
                + target2realDeals);
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator::getRealDeal(t) Found TXState: {} proxy {} target {} target2realDeals {}",
            this.realDeal, this, target, target2realDeals);
      }
    }
    return this.realDeal;
  }

  /*
   * [DISTTX] TODO Do some optimization
   */
  private DistributedMember getRRTarget(KeyInfo key, InternalRegion r) {
    if (this.rrTargets == null) {
      this.rrTargets = new HashMap();
    }
    DistributedMember m = this.rrTargets.get(r);
    if (m == null) {
      m = getOwnerForKey(r, key);
      this.rrTargets.put(r, m);
    }
    return m;
  }

  private Set<DistributedMember> getTxRemoteParticpants(final DistributionManager dm) {
    if (this.txRemoteParticpants == null) {
      Set<DistributedMember> txParticpants = target2realDeals.keySet();
      this.txRemoteParticpants = new HashSet<DistributedMember>(txParticpants);
      // Remove local member from remote participant list
      this.txRemoteParticpants.remove(dm.getId());
      if (logger.isDebugEnabled()) {
        logger.debug("DistTXStateProxyImplOnCoordinator.doPrecommit txParticpants = "
            + txParticpants + " ,txRemoteParticpants=" + this.txRemoteParticpants + " ,originator="
            + dm.getId());
      }
    }
    return txRemoteParticpants;
  }

  private boolean doPrecommit() {
    boolean finalResult = true;
    final DistributionManager dm = getCache().getDistributionManager();
    Set<DistributedMember> txRemoteParticpants = getTxRemoteParticpants(dm);

    // create processor and precommit message
    DistTXPrecommitMessage.DistTxPrecommitReplyProcessor processor =
        new DistTXPrecommitMessage.DistTxPrecommitReplyProcessor(this.getTxId(), dm,
            txRemoteParticpants, target2realDeals);
    // TODO [DISTTX} whats ack threshold?
    processor.enableSevereAlertProcessing();
    final DistTXPrecommitMessage precommitMsg =
        new DistTXPrecommitMessage(this.getTxId(), this.onBehalfOfClientMember, processor);

    // send precommit message to remote nodes
    for (DistributedMember remoteNode : txRemoteParticpants) {
      DistTXCoordinatorInterface remoteTXStateStub = target2realDeals.get(remoteNode);
      if (remoteTXStateStub.isTxState()) {
        throw new UnsupportedOperationInTransactionException(
            String.format("Expected %s during a distributed transaction but got %s",
                "DistPeerTXStateStub",
                remoteTXStateStub.getClass().getSimpleName()));
      }
      try {
        remoteTXStateStub.setPrecommitMessage(precommitMsg, dm);
        remoteTXStateStub.precommit();
      } finally {
        remoteTXStateStub.setPrecommitMessage(null, null);
      }
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator.doPrecommit Sent Message to target = " + remoteNode);
      }
    }

    // Do precommit on local node
    TreeSet<String> sortedRegionName = new TreeSet<>();
    DistTXCoordinatorInterface localTXState = target2realDeals.get(dm.getId());
    if (localTXState != null) {
      if (!localTXState.isTxState()) {
        throw new UnsupportedOperationInTransactionException(
            String.format("Expected %s during a distributed transaction but got %s",
                "DistTXStateOnCoordinator",
                localTXState.getClass().getSimpleName()));
      }
      localTXState.precommit();
      boolean localResult = localTXState.getPreCommitResponse();
      TreeMap<String, ArrayList<DistTxThinEntryState>> entryStateSortedMap =
          new TreeMap<String, ArrayList<DistTxThinEntryState>>();
      ArrayList<ArrayList<DistTxThinEntryState>> entryEventList = null;
      if (localResult) {
        localResult = ((DistTXStateOnCoordinator) localTXState)
            .populateDistTxEntryStateList(entryStateSortedMap);
        if (localResult) {
          entryEventList =
              new ArrayList<ArrayList<DistTxThinEntryState>>(entryStateSortedMap.values());
          populateEntryEventMap(dm.getId(), entryEventList, sortedRegionName);
        }
      }

      if (logger.isDebugEnabled()) {
        logger.debug("DistTXStateProxyImplOnCoordinator.doPrecommit local = " + dm.getId()
            + " ,entryEventList=" + printEntryEventList(entryEventList) + " ,txRegionVersionsMap="
            + printEntryEventMap(this.txEntryEventMap) + " ,result= " + localResult
            + " ,finalResult-old= " + finalResult);
      }
      finalResult = finalResult && localResult;
    }

    /*
     * [DISTTX] TODO Any test hooks
     */
    // if (internalAfterIndividualSend != null) {
    // internalAfterIndividualSend.run();
    // }

    /*
     * [DISTTX] TODO see how to handle exception
     */

    /*
     * [DISTTX] TODO Any test hooks
     */
    // if (internalAfterIndividualCommitProcess != null) {
    // // Testing callback
    // internalAfterIndividualCommitProcess.run();
    // }

    { // Wait for results
      dm.getCancelCriterion().checkCancelInProgress(null);
      processor.waitForPrecommitCompletion();

      // [DISTTX} TODO Handle stats
      // dm.getStats().incCommitWaits();

      Map<DistributedMember, DistTxPrecommitResponse> remoteResults =
          processor.getCommitResponseMap();
      for (Entry<DistributedMember, DistTxPrecommitResponse> e : remoteResults.entrySet()) {
        DistributedMember target = e.getKey();
        DistTxPrecommitResponse remoteResponse = e.getValue();
        ArrayList<ArrayList<DistTxThinEntryState>> entryEventList =
            remoteResponse.getDistTxEntryEventList();
        populateEntryEventMap(target, entryEventList, sortedRegionName);
        if (logger.isDebugEnabled()) {
          logger.debug("DistTXStateProxyImplOnCoordinator.doPrecommit got reply from target = "
              + target + " ,sortedRegions" + sortedRegionName + " ,entryEventList="
              + printEntryEventList(entryEventList) + " ,txEntryEventMap="
              + printEntryEventMap(this.txEntryEventMap) + " ,result= "
              + remoteResponse.getCommitState() + " ,finalResult-old= " + finalResult);
        }
        finalResult = finalResult && remoteResponse.getCommitState();
      }
    }

    /*
     * [DISTTX] TODO Write similar method to take out exception
     *
     * [DISTTX] TODO Handle Reliable regions
     */
    // if (this.hasReliableRegions) {
    // checkDistributionReliability(distMap, processor);
    // }

    if (logger.isDebugEnabled()) {
      logger.debug("DistTXStateProxyImplOnCoordinator.doPrecommit finalResult= " + finalResult);
    }
    return finalResult;
  }

  /*
   * Handle response of precommit reply
   *
   * Go over list of region versions for this target and fill map
   */
  private void populateEntryEventMap(DistributedMember target,
      ArrayList<ArrayList<DistTxThinEntryState>> entryEventList, TreeSet<String> sortedRegionName) {
    if (this.txEntryEventMap == null) {
      this.txEntryEventMap = new HashMap<String, ArrayList<DistTxThinEntryState>>();
    }

    DistTXCoordinatorInterface distTxIface = target2realDeals.get(target);
    if (distTxIface.getPrimaryTransactionalOperations() != null
        && distTxIface.getPrimaryTransactionalOperations().size() > 0) {
      sortedRegionName.clear();
      distTxIface.gatherAffectedRegionsName(sortedRegionName, true, false);

      if (sortedRegionName.size() != entryEventList.size()) {
        throw new UnsupportedOperationInTransactionException(
            String.format("Expected %s during a distributed transaction but got %s",
                "size of " + sortedRegionName.size() + " {" + sortedRegionName + "}"
                    + " for target=" + target,
                entryEventList.size() + " {" + entryEventList + "}"));
      }

      int index = 0;
      // Get region as per sorted order of region path
      for (String rName : sortedRegionName) {
        txEntryEventMap.put(rName, entryEventList.get(index++));
      }
    }
  }

  /*
   * Populate list of regions for this target, while sending commit messages
   */
  private void populateEntryEventList(DistributedMember target,
      ArrayList<ArrayList<DistTxThinEntryState>> entryEventList, TreeSet<String> sortedRegionMap) {
    DistTXCoordinatorInterface distTxItem = target2realDeals.get(target);
    sortedRegionMap.clear();
    distTxItem.gatherAffectedRegionsName(sortedRegionMap, false, true);

    // Get region as per sorted order of region path
    entryEventList.clear();
    for (String rName : sortedRegionMap) {
      ArrayList<DistTxThinEntryState> entryStates = this.txEntryEventMap.get(rName);
      if (entryStates == null) {
        throw new UnsupportedOperationInTransactionException(
            String.format("Expected %s during a distributed transaction but got %s",
                "entryStates for " + rName + " at target " + target, "null"));
      }
      entryEventList.add(entryStates);
    }
  }

  /*
   * [DISTTX] TODO - Handle result TXMessage
   */
  private boolean doCommit() {
    boolean finalResult = true;
    final DistributionManager dm = getCache().getDistributionManager();

    // Create Tx Participants
    Set<DistributedMember> txRemoteParticpants = getTxRemoteParticpants(dm);

    // create processor and commit message
    DistTXCommitMessage.DistTxCommitReplyProcessor processor =
        new DistTXCommitMessage.DistTxCommitReplyProcessor(this.getTxId(), dm, txRemoteParticpants,
            target2realDeals);
    // TODO [DISTTX} whats ack threshold?
    processor.enableSevereAlertProcessing();
    final DistTXCommitMessage commitMsg =
        new DistTXCommitMessage(this.getTxId(), this.onBehalfOfClientMember, processor);

    // send commit message to remote nodes
    ArrayList<ArrayList<DistTxThinEntryState>> entryEventList = new ArrayList<>();
    TreeSet<String> sortedRegionName = new TreeSet<>();
    for (DistributedMember remoteNode : txRemoteParticpants) {
      DistTXCoordinatorInterface remoteTXStateStub = target2realDeals.get(remoteNode);
      if (remoteTXStateStub.isTxState()) {
        throw new UnsupportedOperationInTransactionException(
            String.format("Expected %s during a distributed transaction but got %s",
                "DistPeerTXStateStub",
                remoteTXStateStub.getClass().getSimpleName()));
      }
      try {
        populateEntryEventList(remoteNode, entryEventList, sortedRegionName);
        commitMsg.setEntryStateList(entryEventList);
        remoteTXStateStub.setCommitMessage(commitMsg, dm);
        remoteTXStateStub.commit();
      } finally {
        remoteTXStateStub.setCommitMessage(null, null);
        remoteTXStateStub.finalCleanup();
      }
      if (logger.isDebugEnabled()) {
        logger.debug("DistTXStateProxyImplOnCoordinator.doCommit Sent Message target = "
            + remoteNode + " ,sortedRegions=" + sortedRegionName + " ,entryEventList="
            + printEntryEventList(entryEventList) + " ,txEntryEventMap="
            + printEntryEventMap(this.txEntryEventMap));
      }
    }

    // Do commit on local node
    DistTXCoordinatorInterface localTXState = target2realDeals.get(dm.getId());
    if (localTXState != null) {
      if (!localTXState.isTxState()) {
        throw new UnsupportedOperationInTransactionException(
            String.format("Expected %s during a distributed transaction but got %s",
                "DistTXStateOnCoordinator",
                localTXState.getClass().getSimpleName()));
      }
      populateEntryEventList(dm.getId(), entryEventList, sortedRegionName);
      ((DistTXStateOnCoordinator) localTXState).setDistTxEntryStates(entryEventList);
      localTXState.commit();
      TXCommitMessage localResultMsg = localTXState.getCommitMessage();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator.doCommit local = " + dm.getId() + " ,sortedRegions="
                + sortedRegionName + " ,entryEventList=" + printEntryEventList(entryEventList)
                + " ,txEntryEventMap=" + printEntryEventMap(this.txEntryEventMap) + " ,result= "
                + (localResultMsg != null) + " ,finalResult-old= " + finalResult);
      }
      finalResult = finalResult && (localResultMsg != null);
    }

    /*
     * [DISTTX] TODO Any test hooks
     */
    // if (internalAfterIndividualSend != null) {
    // internalAfterIndividualSend.run();
    // }

    /*
     * [DISTTX] TODO see how to handle exception
     */

    /*
     * [DISTTX] TODO Any test hooks
     */
    // if (internalAfterIndividualCommitProcess != null) {
    // // Testing callback
    // internalAfterIndividualCommitProcess.run();
    // }

    { // Wait for results
      dm.getCancelCriterion().checkCancelInProgress(null);
      processor.waitForPrecommitCompletion();

      // [DISTTX} TODO Handle stats
      dm.getStats().incCommitWaits();

      Map<DistributedMember, TXCommitMessage> remoteResults = processor.getCommitResponseMap();
      for (Entry<DistributedMember, TXCommitMessage> e : remoteResults.entrySet()) {
        DistributedMember target = e.getKey();
        TXCommitMessage remoteResultMsg = e.getValue();
        if (logger.isDebugEnabled()) { // TODO - make this trace level
          logger.debug(
              "DistTXStateProxyImplOnCoordinator.doCommit got results from target = " + target
                  + " ,result= " + (remoteResultMsg != null) + " ,finalResult-old= " + finalResult);
        }
        finalResult = finalResult && remoteResultMsg != null;
      }
    }

    /*
     * [DISTTX] TODO Write similar method to take out exception
     *
     * [DISTTX] TODO Handle Reliable regions
     */
    // if (this.hasReliableRegions) {
    // checkDistributionReliability(distMap, processor);
    // }

    if (logger.isDebugEnabled()) {
      logger.debug("DistTXStateProxyImplOnCoordinator.doCommit finalResult= " + finalResult);
    }
    return finalResult;
  }

  /**
   * For distributed transactions, this divides the user's putAll operation into multiple per bucket
   * putAll ops(with entries to be put in that bucket) and then fires those using using appropriate
   * TXStateStub (for target that host the corresponding bucket)
   */
  @Override
  public void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,
      InternalRegion reg) {
    if (putallOp.putAllData.length == 0) {
      return;
    }
    if (reg instanceof DistributedRegion) {
      super.postPutAll(putallOp, successfulPuts, reg);
    } else {
      reg.getCancelCriterion().checkCancelInProgress(null); // fix for bug
      // #43651

      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator.postPutAll "
                + "processing putAll op for region {}, size of putAllOp " + "is {}",
            reg, putallOp.putAllData.length);
      }


      // map of bucketId to putall op for this bucket
      HashMap<Integer, DistributedPutAllOperation> bucketToPutallMap =
          new HashMap<Integer, DistributedPutAllOperation>();
      // map of bucketId to TXStateStub for target that hosts this bucket
      HashMap<Integer, DistTXCoordinatorInterface> bucketToTxStateStubMap =
          new HashMap<Integer, DistTXCoordinatorInterface>();

      // separate the putall op per bucket
      for (int i = 0; i < putallOp.putAllData.length; i++) {
        assert (putallOp.putAllData[i] != null);
        Object key = putallOp.putAllData[i].key;
        int bucketId = putallOp.putAllData[i].getBucketId();

        DistributedPutAllOperation putAllForBucket = bucketToPutallMap.get(bucketId);;
        if (putAllForBucket == null) {
          // TODO DISTTX: event is never released
          EntryEventImpl event = EntryEventImpl.createPutAllEvent(null, reg,
              Operation.PUTALL_CREATE, key, putallOp.putAllData[i].getValue(reg.getCache()));
          event.setEventId(putallOp.putAllData[i].getEventID());
          putAllForBucket =
              new DistributedPutAllOperation(event, putallOp.putAllDataSize, putallOp.isBridgeOp);
          bucketToPutallMap.put(bucketId, putAllForBucket);
        }
        putallOp.putAllData[i].setFakeEventID();
        putAllForBucket.addEntry(putallOp.putAllData[i]);

        KeyInfo ki = new KeyInfo(key, null, null);
        DistTXCoordinatorInterface tsi = (DistTXCoordinatorInterface) getRealDeal(ki, reg);
        bucketToTxStateStubMap.put(bucketId, tsi);
      }

      // fire a putAll operation for each bucket using appropriate TXStateStub
      // (for target that host this bucket)

      // [DISTTX] [TODO] Perf: Can this be further optimized?
      // This sends putAll in a loop to each target bucket (and waits for ack)
      // one after another.Could we send respective putAll messages to all
      // targets using same reply processor and wait on it?
      for (Entry<Integer, DistTXCoordinatorInterface> e : bucketToTxStateStubMap.entrySet()) {
        Integer bucketId = e.getKey();
        DistTXCoordinatorInterface dtsi = e.getValue();
        DistributedPutAllOperation putAllForBucket = bucketToPutallMap.get(bucketId);

        if (logger.isDebugEnabled()) {
          logger.debug(
              "DistTXStateProxyImplOnCoordinator.postPutAll processing"
                  + " putAll for ##bucketId = {}, ##txStateStub = {}, " + "##putAllOp = {}",
              bucketId, dtsi, putAllForBucket);
        }
        dtsi.postPutAll(putAllForBucket, successfulPuts, reg);
      }
    }
  }

  /**
   * For distributed transactions, this divides the user's removeAll operation into multiple per
   * bucket removeAll ops(with entries to be removed from that bucket) and then fires those using
   * using appropriate TXStateStub (for target that host the corresponding bucket)
   */
  @Override
  public void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps,
      InternalRegion reg) {
    if (op.removeAllData.length == 0) {
      return;
    }
    if (reg instanceof DistributedRegion) {
      super.postRemoveAll(op, successfulOps, reg);
    } else {
      reg.getCancelCriterion().checkCancelInProgress(null); // fix for bug
      // #43651
      if (logger.isDebugEnabled()) {
        logger.debug(
            "DistTXStateProxyImplOnCoordinator.postRemoveAll "
                + "processing removeAll op for region {}, size of removeAll " + "is {}",
            reg, op.removeAllDataSize);
      }

      // map of bucketId to removeAll op for this bucket
      HashMap<Integer, DistributedRemoveAllOperation> bucketToRemoveAllMap =
          new HashMap<Integer, DistributedRemoveAllOperation>();
      // map of bucketId to TXStateStub for target that hosts this bucket
      HashMap<Integer, DistTXCoordinatorInterface> bucketToTxStateStubMap =
          new HashMap<Integer, DistTXCoordinatorInterface>();

      // separate the removeAll op per bucket
      for (int i = 0; i < op.removeAllData.length; i++) {
        assert (op.removeAllData[i] != null);
        Object key = op.removeAllData[i].key;
        int bucketId = op.removeAllData[i].getBucketId();

        DistributedRemoveAllOperation removeAllForBucket = bucketToRemoveAllMap.get(bucketId);
        if (removeAllForBucket == null) {
          // TODO DISTTX: event is never released
          EntryEventImpl event = EntryEventImpl.createRemoveAllEvent(op, reg, key);
          event.setEventId(op.removeAllData[i].getEventID());
          removeAllForBucket =
              new DistributedRemoveAllOperation(event, op.removeAllDataSize, op.isBridgeOp);
          bucketToRemoveAllMap.put(bucketId, removeAllForBucket);
        }
        op.removeAllData[i].setFakeEventID();
        removeAllForBucket.addEntry(op.removeAllData[i]);

        KeyInfo ki = new KeyInfo(key, null, null);
        DistTXCoordinatorInterface tsi = (DistTXCoordinatorInterface) getRealDeal(ki, reg);
        bucketToTxStateStubMap.put(bucketId, tsi);
      }

      // fire a removeAll operation for each bucket using appropriate TXStateStub
      // (for target that host this bucket)

      // [DISTTX] [TODO] Perf: Can this be further optimized?
      // This sends putAll in a loop to each target bucket (and waits for ack)
      // one after another.Could we send respective putAll messages to all
      // targets using same reply processor and wait on it?
      for (Entry<Integer, DistTXCoordinatorInterface> e : bucketToTxStateStubMap.entrySet()) {
        Integer bucketId = e.getKey();
        DistTXCoordinatorInterface dtsi = e.getValue();
        DistributedRemoveAllOperation removeAllForBucket = bucketToRemoveAllMap.get(bucketId);

        if (logger.isDebugEnabled()) {
          logger.debug(
              "DistTXStateProxyImplOnCoordinator.postRemoveAll processing"
                  + " removeAll for ##bucketId = {}, ##txStateStub = {}, " + "##removeAllOp = {}",
              bucketId, dtsi, removeAllForBucket);
        }
        dtsi.postRemoveAll(removeAllForBucket, successfulOps, reg);
      }

    }
  }

  @Override
  public boolean isCreatedOnDistTxCoordinator() {
    return true;
  }

  public static String printEntryEventMap(
      HashMap<String, ArrayList<DistTxThinEntryState>> txRegionVersionsMap) {
    StringBuilder str = new StringBuilder();
    str.append(" (");
    str.append(txRegionVersionsMap.size());
    str.append(")=[ ");
    for (Map.Entry<String, ArrayList<DistTxThinEntryState>> entry : txRegionVersionsMap
        .entrySet()) {
      str.append(" {").append(entry.getKey());
      str.append(":").append("size(").append(entry.getValue().size()).append(")");
      str.append("=").append(entry.getValue()).append("}, ");
    }
    str.append(" } ");
    return str.toString();
  }

  public static String printEntryEventList(
      ArrayList<ArrayList<DistTxThinEntryState>> entryEventList) {
    StringBuilder str = new StringBuilder();
    str.append(" (");
    str.append(entryEventList.size());
    str.append(")=[ ");
    for (ArrayList<DistTxThinEntryState> entry : entryEventList) {
      str.append(" ( ");
      str.append(entry.size());
      str.append(" )={").append(entry);
      str.append(" } ");
    }
    str.append(" ] ");
    return str.toString();
  }

  /*
   * Do not return null
   */
  public DistributedMember getOwnerForKey(InternalRegion r, KeyInfo key) {
    DistributedMember m = r.getOwnerForKey(key);
    if (m == null) {
      m = getCache().getDistributedSystem().getDistributedMember();
    }
    return m;
  }
}
