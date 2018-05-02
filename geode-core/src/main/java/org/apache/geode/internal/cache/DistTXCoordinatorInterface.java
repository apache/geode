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
import java.util.HashSet;
import java.util.TreeSet;

import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.cache.tx.DistTxEntryEvent;

/**
 * [DISTTX] For Distributed Transaction
 *
 * An entity that works as stub for DistTX on Coordinator.
 *
 */
public interface DistTXCoordinatorInterface extends TXStateInterface {
  /**
   * Response for Precommit
   */
  boolean getPreCommitResponse() throws UnsupportedOperationInTransactionException;

  /**
   * Response for Rollback
   */
  boolean getRollbackResponse() throws UnsupportedOperationInTransactionException;

  ArrayList<DistTxEntryEvent> getPrimaryTransactionalOperations()
      throws UnsupportedOperationInTransactionException;

  void addSecondaryTransactionalOperations(DistTxEntryEvent dtop)
      throws UnsupportedOperationInTransactionException;

  void setPrecommitMessage(DistTXPrecommitMessage precommitMsg, DistributionManager dm)
      throws UnsupportedOperationInTransactionException;

  void setCommitMessage(DistTXCommitMessage commitMsg, DistributionManager dm)
      throws UnsupportedOperationInTransactionException;

  void setRollbackMessage(DistTXRollbackMessage rollbackMsg, DistributionManager dm)
      throws UnsupportedOperationInTransactionException;

  void gatherAffectedRegions(HashSet<InternalRegion> regionSet, boolean includePrimaryRegions,
      boolean includeRedundantRegions) throws UnsupportedOperationInTransactionException;

  void gatherAffectedRegionsName(TreeSet<String> sortedRegionName, boolean includePrimaryRegions,
      boolean includeRedundantRegions) throws UnsupportedOperationInTransactionException;

  void finalCleanup();
}
