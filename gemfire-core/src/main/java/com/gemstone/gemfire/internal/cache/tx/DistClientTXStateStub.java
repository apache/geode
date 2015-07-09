/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tx;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.gemstone.gemfire.cache.UnsupportedOperationInTransactionException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.internal.cache.DistTXCommitMessage;
import com.gemstone.gemfire.internal.cache.DistTXPrecommitMessage;
import com.gemstone.gemfire.internal.cache.DistTXCoordinatorInterface;
import com.gemstone.gemfire.internal.cache.DistTXRollbackMessage;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.TXStateProxy;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * @author vivekb
 * 
 */
public class DistClientTXStateStub extends ClientTXStateStub implements
    DistTXCoordinatorInterface {

  /**
   * @param stateProxy
   * @param target
   * @param firstRegion
   */
  public DistClientTXStateStub(TXStateProxy stateProxy,
      DistributedMember target, LocalRegion firstRegion) {
    super(stateProxy, target, firstRegion);
    // TODO Auto-generated constructor stub
  }

  @Override
  public boolean getPreCommitResponse()
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        LocalizedStrings.Dist_TX_PRECOMMIT_NOT_SUPPORTED_IN_A_TRANSACTION
            .toLocalizedString("getPreCommitResponse"));
  }

  @Override
  public boolean getRollbackResponse()
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        LocalizedStrings.Dist_TX_ROLLBACK_NOT_SUPPORTED_IN_A_TRANSACTION
            .toLocalizedString("getRollbackResponse"));
  }

  @Override
  public ArrayList<DistTxEntryEvent> getPrimaryTransactionalOperations()
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        LocalizedStrings.Dist_TX_PRECOMMIT_NOT_SUPPORTED_IN_A_TRANSACTION
            .toLocalizedString("getPrimaryTransactionalOperations"));
  }

  @Override
  public void addSecondaryTransactionalOperations(DistTxEntryEvent dtop)
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        LocalizedStrings.Dist_TX_PRECOMMIT_NOT_SUPPORTED_IN_A_TRANSACTION
            .toLocalizedString("addSecondaryTransactionalOperations"));
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
  public void gatherAffectedRegions(HashSet<LocalRegion> regionSet, boolean includePrimaryRegions, boolean includeRedundantRegions)
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        LocalizedStrings.Dist_TX_PRECOMMIT_NOT_SUPPORTED_IN_A_TRANSACTION
            .toLocalizedString("gatherAffectedRegions"));
  }
  
  @Override
  public void gatherAffectedRegionsName(TreeSet<String> sortedRegionName,
      boolean includePrimaryRegions, boolean includeRedundantRegions)
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        LocalizedStrings.Dist_TX_PRECOMMIT_NOT_SUPPORTED_IN_A_TRANSACTION
            .toLocalizedString("gatherAffectedRegions"));
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
    // Do nothing
  }
}