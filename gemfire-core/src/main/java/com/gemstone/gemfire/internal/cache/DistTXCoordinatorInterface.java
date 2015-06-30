/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.gemstone.gemfire.cache.UnsupportedOperationInTransactionException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.internal.cache.tx.DistTxEntryEvent;

/**
 * [DISTTX] For Distributed Transaction
 * 
 * An entity that works as stub for DistTX on Coordinator.
 * 
 * @author vivekb
 */
public interface DistTXCoordinatorInterface extends TXStateInterface {
  /**
   * Response for Precommit
   */
  public boolean getPreCommitResponse()
      throws UnsupportedOperationInTransactionException;

  /**
   * Response for Rollback
   */
  public boolean getRollbackResponse()
      throws UnsupportedOperationInTransactionException;

  public ArrayList<DistTxEntryEvent> getPrimaryTransactionalOperations()
      throws UnsupportedOperationInTransactionException;

  public void addSecondaryTransactionalOperations(DistTxEntryEvent dtop)
      throws UnsupportedOperationInTransactionException;
  
  public void setPrecommitMessage(DistTXPrecommitMessage precommitMsg, DM dm)
      throws UnsupportedOperationInTransactionException;
  
  public void setCommitMessage(DistTXCommitMessage commitMsg, DM dm)
      throws UnsupportedOperationInTransactionException;
  
  public void setRollbackMessage(DistTXRollbackMessage rollbackMsg, DM dm)
      throws UnsupportedOperationInTransactionException;
  
  public void gatherAffectedRegions(HashSet<LocalRegion> regionSet,
      boolean includePrimaryRegions, boolean includeRedundantRegions)
      throws UnsupportedOperationInTransactionException;
  
  public void gatherAffectedRegionsName(
      TreeSet<String> sortedRegionName,
      boolean includePrimaryRegions, boolean includeRedundantRegions)
      throws UnsupportedOperationInTransactionException;
  
  public void finalCleanup();
}
