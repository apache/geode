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

public class DistTXStateProxyImplOnDatanode extends DistTXStateProxyImpl {

  private DistTXPrecommitMessage preCommitMessage = null;
  private boolean preCommitResponse = false;

  public DistTXStateProxyImplOnDatanode(InternalCache cache, TXManagerImpl managerImpl, TXId id,
      InternalDistributedMember clientMember) {
    super(cache, managerImpl, id, clientMember);
  }

  public DistTXStateProxyImplOnDatanode(InternalCache cache, TXManagerImpl managerImpl, TXId id,
      boolean isjta) {
    super(cache, managerImpl, id, isjta);
  }

  @Override
  public TXStateInterface getRealDeal(KeyInfo key, InternalRegion r) {
    if (this.realDeal == null) {
      this.realDeal = new DistTXState(this, false);
      if (r != null) {
        // wait for the region to be initialized fixes bug 44652
        r.waitOnInitialization(r.getInitializationLatchBeforeGetInitialImage());
        target = r.getOwnerForKey(key);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Built a new DistTXState: {} me:{}", this.realDeal,
            this.txMgr.getDM().getId());
      }
    }
    return this.realDeal;
  }

  @Override
  public TXStateInterface getRealDeal(DistributedMember t) {
    assert t != null;
    if (this.realDeal == null) {
      this.target = t;
      this.realDeal = new DistTXState(this, false);
      if (logger.isDebugEnabled()) {
        logger.debug("Built a new DistTXState: {} me:{}", this.realDeal,
            this.txMgr.getDM().getId());
      }
    }
    return this.realDeal;
  }

  private DistTXState getRealDeal() throws UnsupportedOperationInTransactionException {
    if (this.realDeal == null || !this.realDeal.isDistTx() || !this.realDeal.isTxState()
        || this.realDeal.isCreatedOnDistTxCoordinator()) {
      throw new UnsupportedOperationInTransactionException(
          String.format("Expected %s during a distributed transaction but got %s",
              "DistTXStateOnDatanode",
              this.realDeal != null ? this.realDeal.getClass().getSimpleName() : "null"));
    }
    return (DistTXState) this.realDeal;
  }

  @Override
  public void precommit()
      throws CommitConflictException, UnsupportedOperationInTransactionException {
    try {
      DistTXState txState = getRealDeal();
      boolean retVal = txState.applyOpsOnRedundantCopy(this.preCommitMessage.getSender(),
          this.preCommitMessage.getSecondaryTransactionalOperations());
      if (retVal) {
        setCommitOnBehalfOfRemoteStub(true);
        txState.precommit();
      }
      this.preCommitResponse = retVal; // assign at last, if no exception
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
