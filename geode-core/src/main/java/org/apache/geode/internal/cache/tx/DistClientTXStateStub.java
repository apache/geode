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
package org.apache.geode.internal.cache.tx;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.internal.cache.DistTXCommitMessage;
import org.apache.geode.internal.cache.DistTXPrecommitMessage;
import org.apache.geode.internal.cache.DistTXCoordinatorInterface;
import org.apache.geode.internal.cache.DistTXRollbackMessage;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.i18n.LocalizedStrings;

/**
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
