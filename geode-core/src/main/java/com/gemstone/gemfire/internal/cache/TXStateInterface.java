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
/**
 * File comment
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import javax.transaction.Synchronization;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.cache.UnsupportedOperationInTransactionException;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.client.internal.ServerRegionDataAccess;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.tx.TransactionalOperation.ServerRegionOperation;

/**
 * An entity that tracks transactions must implement this interface. 
 * 
 */
public interface TXStateInterface extends Synchronization, InternalDataView {

  public TransactionId getTransactionId();

  /**
   * Used by transaction operations that are doing a read
   * operation on the specified region.
   * @return the TXRegionState for the given LocalRegion
   * or null if no state exists
   */
  public TXRegionState readRegion(LocalRegion r);

  /**
   * Used by transaction operations that are doing a write
   * operation on the specified region.
   * @return the TXRegionState for the given LocalRegion
   */
  public TXRegionState writeRegion(LocalRegion r);

  /**
   * Returns a nanotimer timestamp that marks when begin was
   * called on this transaction.
   */
  public long getBeginTime();

  /**
   * Returns the number of changes this transaction would have made
   * if it successfully committed.
   */
  public int getChanges();

  /**
   * Determines if a transaction is in progress.
   * Transactions are in progress until they commit or rollback.
   * @return true if this transaction has completed.
   */
  public boolean isInProgress();

  /**
   * Returns the next modification serial number.
   * Note this method is not thread safe but does not need to be since
   * a single thread owns a transaction.
   */
  public int nextModSerialNum();

  /**
   * Return true if mod counts for this transaction can not be represented by a byte
   * @since GemFire 5.0
   */
  public boolean needsLargeModCount();
  
  /*
   * Only applicable for Distributed transaction.
   */
  public void precommit() throws CommitConflictException, UnsupportedOperationInTransactionException;

  public void commit() throws CommitConflictException;

  public void rollback();

  public List getEvents();

 

  /** Implement TransactionEvent's getCache */
  public Cache getCache();

  public Collection<LocalRegion> getRegions();

  public void invalidateExistingEntry(final EntryEventImpl event,
      boolean invokeCallbacks, boolean forceNewEntry);

  /**
   * @param region
   * @param keyInfo
   * @param allowTombstones
   * @return a Region.Entry if it exists either in committed state or in transactional state, otherwise returns null  
   */
  public Entry getEntry(final KeyInfo keyInfo, final LocalRegion region, boolean allowTombstones);

  /**
   * @param keyInfo
   * @param localRegion
   * @param updateStats TODO
   */
  public Object getDeserializedValue(KeyInfo keyInfo,
                                     LocalRegion localRegion,
                                     boolean updateStats,
                                     boolean disableCopyOnRead,
                                     boolean preferCD,
                                     EntryEventImpl clientEvent,
                                     boolean returnTombstones,
                                     boolean retainResult);

  public TXEvent getEvent();

  public TXRegionState txWriteRegion(final LocalRegion localRegion, final KeyInfo entryKey);

  public TXRegionState txReadRegion(LocalRegion localRegion);

  public boolean txPutEntry(final EntryEventImpl event, boolean ifNew, boolean requireOldValue, boolean checkResources, Object expectedOldValue);

  /**
   * @param entryKey TODO
   * @param localRegion TODO
   * @param rememberRead true if the value read from committed state
   *   needs to be remembered in tx state for repeatable read.
   * @param  createTxEntryIfAbsent should a transactional entry be created if not present. 
   * @return a txEntryState or null if the entry doesn't exist in the transaction and/or committed state. 
   */
  public TXEntryState txReadEntry(KeyInfo entryKey, LocalRegion localRegion, boolean rememberRead
      ,boolean createTxEntryIfAbsent);

  public void rmRegion(LocalRegion r);

  /**
   * 
   * @param state
   * @return true if transaction is in progress and the given state has the same identity as this instance
   */
  public boolean isInProgressAndSameAs(TXStateInterface state);

  /**
   * 
   * @return true if callbacks should be fired for this TXState
   */
  public boolean isFireCallbacks();
  
  /**
   * On the remote node, the tx can potentially be accessed by multiple threads,
   * specially with function execution. This lock should be used to synchronize
   * access to the tx state.
   * @return the lock to be used
   */
  public ReentrantLock getLock();
  
  public boolean isRealDealLocal();
  
  public boolean isMemberIdForwardingRequired();

  public InternalDistributedMember getOriginatingMember();
  
  public TXCommitMessage getCommitMessage();
  
  /**
   * perform additional tasks to suspend a transaction
   */
  public void suspend();
  
  /**
   * perform additional tasks to resume a suspended transaction
   */
  public void resume();
  
  /**
   * record a transactional operation for possible later replay
   */
  public void recordTXOperation(ServerRegionDataAccess region, ServerRegionOperation op, Object key, Object arguments[]);

  public void close();
  
  /*
   * Determine if its TxState or not
   */
  public boolean isTxState();
  
  /*
   * Determine if is TxStateStub or not
   */
  public boolean isTxStateStub();
  
  /*
   * Determine if is TxStateProxy or not
   */
  public boolean isTxStateProxy();
  
  /*
   * Is class related to Distributed Transaction, and not colocated transaction
   */
  public boolean isDistTx();
  
  /*
   * Is class meant for Coordinator for Distributed Transaction
   * 
   * Will be true for DistTXCoordinatorInterface
   */
  public boolean isCreatedOnDistTxCoordinator();
}
