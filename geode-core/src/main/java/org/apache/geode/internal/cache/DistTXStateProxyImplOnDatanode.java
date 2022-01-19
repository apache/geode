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
import java.util.TreeMap;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.TXEntryState.DistTxThinEntryState;
import org.apache.geode.internal.statistics.StatisticsClock;

public class DistTXStateProxyImplOnDatanode extends DistTXStateProxyImpl {

  private DistTXPrecommitMessage preCommitMessage = null;
  private boolean preCommitResponse = false;

  public DistTXStateProxyImplOnDatanode(InternalCache cache, TXManagerImpl managerImpl, TXId id,
      InternalDistributedMember clientMember, StatisticsClock statisticsClock) {
    super(cache, managerImpl, id, clientMember, statisticsClock);
  }

  public DistTXStateProxyImplOnDatanode(InternalCache cache, TXManagerImpl managerImpl, TXId id,
      boolean isjta, StatisticsClock statisticsClock) {
    super(cache, managerImpl, id, isjta, statisticsClock);
  }

  @Override
  public TXStateInterface getRealDeal(KeyInfo key, InternalRegion r) {
    if (realDeal == null) {
      realDeal = new DistTXState(this, false, getStatisticsClock());
      if (r != null) {
        // wait for the region to be initialized fixes bug 44652
        r.waitOnInitialization(r.getInitializationLatchBeforeGetInitialImage());
        target = r.getOwnerForKey(key);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Built a new DistTXState: {} me:{}", realDeal,
            txMgr.getDM().getId());
      }
    }
    return realDeal;
  }

  @Override
  public TXStateInterface getRealDeal(DistributedMember t) {
    assert t != null;
    if (realDeal == null) {
      target = t;
      realDeal = new DistTXState(this, false, getStatisticsClock());
      if (logger.isDebugEnabled()) {
        logger.debug("Built a new DistTXState: {} me:{}", realDeal,
            txMgr.getDM().getId());
      }
    }
    return realDeal;
  }

  private DistTXState getRealDeal() throws UnsupportedOperationInTransactionException {
    if (realDeal == null || !realDeal.isDistTx() || !realDeal.isTxState()
        || realDeal.isCreatedOnDistTxCoordinator()) {
      throw new UnsupportedOperationInTransactionException(
          String.format("Expected %s during a distributed transaction but got %s",
              "DistTXStateOnDatanode",
              realDeal != null ? realDeal.getClass().getSimpleName() : "null"));
    }
    return (DistTXState) realDeal;
  }

  @Override
  public void precommit()
      throws CommitConflictException, UnsupportedOperationInTransactionException {
    try {
      DistTXState txState = getRealDeal();
      boolean retVal = txState.applyOpsOnRedundantCopy(preCommitMessage.getSender(),
          preCommitMessage.getSecondaryTransactionalOperations());
      if (retVal) {
        setCommitOnBehalfOfRemoteStub(true);
        txState.precommit();
      }
      preCommitResponse = retVal; // assign at last, if no exception
    } finally {
      inProgress = true;
    }
  }

  public void setPreCommitMessage(DistTXPrecommitMessage preCommitMessage) {
    this.preCommitMessage = preCommitMessage;
  }

  public boolean getPreCommitResponse() {
    return preCommitResponse;
  }

  /*
   * Populate list of versions for each region while replying precommit
   */
  public boolean populateDistTxEntryStateList(
      TreeMap<String, ArrayList<DistTxThinEntryState>> entryStateSortedMap) {
    return getRealDeal().populateDistTxEntryStateList(entryStateSortedMap);
  }

  public void populateDistTxEntryStates(ArrayList<ArrayList<DistTxThinEntryState>> entryEventList) {
    getRealDeal().setDistTxEntryStates(entryEventList);
  }
}
