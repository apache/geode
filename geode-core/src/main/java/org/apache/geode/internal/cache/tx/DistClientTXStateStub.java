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
package org.apache.geode.internal.cache.tx;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeSet;

import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.DistTXCommitMessage;
import org.apache.geode.internal.cache.DistTXCoordinatorInterface;
import org.apache.geode.internal.cache.DistTXPrecommitMessage;
import org.apache.geode.internal.cache.DistTXRollbackMessage;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.TXStateProxy;

public class DistClientTXStateStub extends ClientTXStateStub implements DistTXCoordinatorInterface {

  public DistClientTXStateStub(InternalCache cache, DistributionManager dm, TXStateProxy stateProxy,
      DistributedMember target, InternalRegion firstRegion) {
    super(cache, dm, stateProxy, target, firstRegion);
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
  public ArrayList<DistTxEntryEvent> getPrimaryTransactionalOperations()
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        String.format("precommit() operation %s meant for Dist Tx is not supported",
            "getPrimaryTransactionalOperations"));
  }

  @Override
  public void addSecondaryTransactionalOperations(DistTxEntryEvent dtop)
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        String.format("precommit() operation %s meant for Dist Tx is not supported",
            "addSecondaryTransactionalOperations"));
  }

  @Override
  public void setPrecommitMessage(DistTXPrecommitMessage precommitMsg, DistributionManager dm)
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        String.format("precommit() operation %s meant for Dist Tx is not supported",
            "setPrecommitMessage"));
  }

  @Override
  public void setCommitMessage(DistTXCommitMessage commitMsg, DistributionManager dm)
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        String.format("precommit() operation %s meant for Dist Tx is not supported",
            "setCommitMessage"));
  }

  @Override
  public void setRollbackMessage(DistTXRollbackMessage rollbackMsg, DistributionManager dm)
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        String.format("rollback() operation %s meant for Dist Tx is not supported",
            "setRollbackMessage"));
  }

  @Override
  public void gatherAffectedRegions(HashSet<InternalRegion> regionSet,
      boolean includePrimaryRegions, boolean includeRedundantRegions)
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        String.format("precommit() operation %s meant for Dist Tx is not supported",
            "gatherAffectedRegions"));
  }

  @Override
  public void gatherAffectedRegionsName(TreeSet<String> sortedRegionName,
      boolean includePrimaryRegions, boolean includeRedundantRegions)
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        String.format("precommit() operation %s meant for Dist Tx is not supported",
            "gatherAffectedRegions"));
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
