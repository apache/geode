package com.gemstone.gemfire.internal.cache;

import java.util.ArrayList;
import java.util.TreeMap;

import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.cache.UnsupportedOperationInTransactionException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.TXEntryState.DistTxThinEntryState;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

public class DistTXStateProxyImplOnDatanode extends DistTXStateProxyImpl {

  private DistTXPrecommitMessage preCommitMessage = null;
  private boolean preCommitResponse = false;
  
  public DistTXStateProxyImplOnDatanode(TXManagerImpl managerImpl, TXId id,
      InternalDistributedMember clientMember) {
    super(managerImpl, id, clientMember);
  }

  public DistTXStateProxyImplOnDatanode(TXManagerImpl managerImpl, TXId id,
      boolean isjta) {
    super(managerImpl, id, isjta);
  }

  @Override
  public TXStateInterface getRealDeal(KeyInfo key, LocalRegion r) {
    if (this.realDeal == null) {
      this.realDeal = new DistTXState(this, false);
      if (r != null) {
        // wait for the region to be initialized fixes bug 44652
        r.waitOnInitialization(r.initializationLatchBeforeGetInitialImage);
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
  
  private DistTXState getRealDeal()
      throws UnsupportedOperationInTransactionException {
    if (this.realDeal == null || !this.realDeal.isDistTx()
        || !this.realDeal.isTxState()
        || this.realDeal.isCreatedOnDistTxCoordinator()) {
      throw new UnsupportedOperationInTransactionException(
          LocalizedStrings.DISTTX_TX_EXPECTED.toLocalizedString(
              "DistTXStateOnDatanode", this.realDeal != null ? this.realDeal
                  .getClass().getSimpleName() : "null"));
    }
    return (DistTXState) this.realDeal;
  }
  
  @Override
  public void precommit() throws CommitConflictException,
      UnsupportedOperationInTransactionException {
    try {
      DistTXState txState = getRealDeal();
      boolean retVal = txState.applyOpsOnRedundantCopy(
          this.preCommitMessage.getSender(),
          this.preCommitMessage.getSecondaryTransactionalOperations());
      if (retVal) {
        setCommitOnBehalfOfRemoteStub(true);
        txState.precommit();
      }
      this.preCommitResponse = retVal; // assign at last, if no exception
    } catch (UnsupportedOperationInTransactionException e) {
      throw e;
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
  
  public void populateDistTxEntryStates(
      ArrayList<ArrayList<DistTxThinEntryState>> entryEventList) {
    getRealDeal().setDistTxEntryStates(entryEventList);
  }
}
