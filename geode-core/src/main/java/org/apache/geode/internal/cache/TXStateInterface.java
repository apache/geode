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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import javax.transaction.Synchronization;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.cache.client.internal.ServerRegionDataAccess;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.tx.TransactionalOperation.ServerRegionOperation;

/**
 * An entity that tracks transactions must implement this interface.
 */
public interface TXStateInterface extends Synchronization, InternalDataView {

  TransactionId getTransactionId();

  /**
   * Used by transaction operations that are doing a read operation on the specified region.
   *
   * @return the TXRegionState for the given LocalRegion or null if no state exists
   */
  TXRegionState readRegion(InternalRegion r);

  /**
   * Used by transaction operations that are doing a write operation on the specified region.
   *
   * @return the TXRegionState for the given LocalRegion
   */
  TXRegionState writeRegion(InternalRegion r);

  /**
   * Returns a nanotimer timestamp that marks when begin was called on this transaction.
   */
  long getBeginTime();

  /**
   * Returns the number of changes this transaction would have made if it successfully committed.
   */
  int getChanges();

  /**
   * Determines if a transaction is in progress. Transactions are in progress until they commit or
   * rollback.
   *
   * @return true if this transaction has completed.
   */
  boolean isInProgress();

  /**
   * Returns the next modification serial number. Note this method is not thread safe but does not
   * need to be since a single thread owns a transaction.
   */
  int nextModSerialNum();

  /**
   * Return true if mod counts for this transaction can not be represented by a byte
   *
   * @since GemFire 5.0
   */
  boolean needsLargeModCount();

  /*
   * Only applicable for Distributed transaction.
   */
  void precommit() throws CommitConflictException, UnsupportedOperationInTransactionException;

  void commit() throws CommitConflictException;

  void rollback();

  List getEvents();



  /** Implement TransactionEvent's getCache */
  InternalCache getCache();

  Collection<InternalRegion> getRegions();

  @Override
  void invalidateExistingEntry(final EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry);

  /**
   * @return a Region.Entry if it exists either in committed state or in transactional state,
   *         otherwise returns null
   */
  @Override
  Entry getEntry(final KeyInfo keyInfo, final LocalRegion region, boolean allowTombstones);

  TXEvent getEvent();

  TXRegionState txWriteRegion(final InternalRegion internalRegion, final KeyInfo entryKey);

  TXRegionState txReadRegion(InternalRegion internalRegion);

  boolean txPutEntry(final EntryEventImpl event, boolean ifNew, boolean requireOldValue,
      boolean checkResources, Object expectedOldValue);

  /**
   * @param entryKey TODO
   * @param localRegion TODO
   * @param rememberRead true if the value read from committed state needs to be remembered in tx
   *        state for repeatable read.
   * @param createTxEntryIfAbsent should a transactional entry be created if not present.
   * @return a txEntryState or null if the entry doesn't exist in the transaction and/or committed
   *         state.
   */
  TXEntryState txReadEntry(KeyInfo entryKey, LocalRegion localRegion, boolean rememberRead,
      boolean createTxEntryIfAbsent);

  void rmRegion(LocalRegion r);

  /**
   *
   * @return true if transaction is in progress and the given state has the same identity as this
   *         instance
   */
  boolean isInProgressAndSameAs(TXStateInterface state);

  /**
   *
   * @return true if callbacks should be fired for this TXState
   */
  boolean isFireCallbacks();

  /**
   * On the remote node, the tx can potentially be accessed by multiple threads, specially with
   * function execution. This lock should be used to synchronize access to the tx state.
   *
   * @return the lock to be used
   */
  ReentrantLock getLock();

  boolean isRealDealLocal();

  boolean isMemberIdForwardingRequired();

  InternalDistributedMember getOriginatingMember();

  TXCommitMessage getCommitMessage();

  /**
   * perform additional tasks to suspend a transaction
   */
  void suspend();

  /**
   * perform additional tasks to resume a suspended transaction
   */
  void resume();

  /**
   * record a transactional operation for possible later replay
   */
  void recordTXOperation(ServerRegionDataAccess region, ServerRegionOperation op, Object key,
      Object arguments[]);

  void close();

  /*
   * Determine if its TxState or not
   */
  boolean isTxState();

  /*
   * Determine if is TxStateStub or not
   */
  boolean isTxStateStub();

  /*
   * Determine if is TxStateProxy or not
   */
  boolean isTxStateProxy();

  /*
   * Is class related to Distributed Transaction, and not colocated transaction
   */
  boolean isDistTx();

  /*
   * Is class meant for Coordinator for Distributed Transaction
   *
   * Will be true for DistTXCoordinatorInterface
   */
  boolean isCreatedOnDistTxCoordinator();
}
