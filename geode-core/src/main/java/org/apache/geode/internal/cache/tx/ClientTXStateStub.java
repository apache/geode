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

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.TransactionDataNodeHasDepartedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.TransactionInDoubtException;
import org.apache.geode.cache.client.internal.ServerRegionDataAccess;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.*;
import org.apache.geode.internal.cache.locks.TXRegionLockRequest;
import org.apache.geode.internal.cache.tx.TransactionalOperation.ServerRegionOperation;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import javax.transaction.Status;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ClientTXStateStub extends TXStateStub {
  private static final Logger logger = LogService.getLogger();
  
//  /** a flag to turn off automatic replay of transactions.  Maybe this should be a pool property? */
//  private static final boolean ENABLE_REPLAY = Boolean.getBoolean("gemfire.enable-transaction-replay");
//  
//  /** time to pause between transaction replays, in millis */
//  private static final int TRANSACTION_REPLAY_PAUSE = Integer.getInteger("gemfire.transaction-replay-pause", 500).intValue();

  /** test hook - used to find out what operations were performed in the last tx */
  private static ThreadLocal<List<TransactionalOperation>> recordedTransactionalOperations = null; 
  
  private final ServerRegionProxy firstProxy;
  
  private ServerLocation serverAffinityLocation;

  /** the operations performed in the current transaction are held in this list */
  private List<TransactionalOperation> recordedOperations
    = Collections.synchronizedList(new LinkedList<TransactionalOperation>());

  /** lock request for obtaining local locks */
  private TXLockRequest lockReq;

  private Runnable internalAfterLocalLocks;

  /**
   * System property to disable conflict checks on clients.
   */
  private static final boolean DISABLE_CONFLICT_CHECK_ON_CLIENT = Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "disableConflictChecksOnClient");

  /**
   * @return true if transactional operation recording is enabled (test hook)
   */
  public static boolean transactionRecordingEnabled() {
    return !DISABLE_CONFLICT_CHECK_ON_CLIENT || recordedTransactionalOperations != null;
  }
  
  /**
   * test hook
   *   
   * @param t a ThreadLocal to hold lists of TransactionalOperations
   */
  public static void setTransactionalOperationContainer(ThreadLocal<List<TransactionalOperation>> t) {
    recordedTransactionalOperations = t;
  }


  
  public ClientTXStateStub(TXStateProxy stateProxy, DistributedMember target,LocalRegion firstRegion) {
    super(stateProxy, target);
    firstProxy = firstRegion.getServerProxy();
    this.firstProxy.getPool().setupServerAffinity(true);
    if (recordedTransactionalOperations != null) {
      recordedTransactionalOperations.set(this.recordedOperations);
    }
  }

  @Override
  public void commit() throws CommitConflictException {
    obtainLocalLocks();
    try {
      TXCommitMessage txcm = firstProxy.commit(proxy.getTxId().getUniqId());
      afterServerCommit(txcm);
    } catch (TransactionDataNodeHasDepartedException e) {
      throw new TransactionInDoubtException(e);
    } finally {
      lockReq.releaseLocal();
      this.firstProxy.getPool().releaseServerAffinity();
    }
  }
  
  /**
   * Lock the keys in a local transaction manager
   * 
   * @throws CommitConflictException
   *           if the key is already locked by some other transaction
   */
  private void obtainLocalLocks() {
    lockReq = new TXLockRequest();
    GemFireCacheImpl cache = GemFireCacheImpl.getExisting("");
    for (TransactionalOperation txOp : this.recordedOperations) {
      if (ServerRegionOperation.lockKeyForTx(txOp.getOperation())) {
        TXRegionLockRequest rlr = lockReq.getRegionLockRequest(txOp
            .getRegionName());
        if (rlr == null) {
          rlr = new TXRegionLockRequestImpl(cache.getRegionByPath(txOp
              .getRegionName()));
          lockReq.addLocalRequest(rlr);
        }
        if (txOp.getOperation() == ServerRegionOperation.PUT_ALL || txOp.getOperation() == ServerRegionOperation.REMOVE_ALL) {
          rlr.addEntryKeys(txOp.getKeys());
        } else {
          rlr.addEntryKey(txOp.getKey());
        }
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("TX: client localLockRequest: {}", lockReq);
    }
    try {
      lockReq.obtain();
    } catch (CommitConflictException e) {
      rollback(); // cleanup tx artifacts on server
      throw e;
    }
    if (internalAfterLocalLocks != null) {
      internalAfterLocalLocks.run();
    }
  }
  
  /** perform local cache modifications using the server's TXCommitMessage */
  private void afterServerCommit(TXCommitMessage txcm) {
    if (this.internalAfterSendCommit != null) {
      this.internalAfterSendCommit.run();
    }

    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache == null) {
      // fixes bug 42933
      return;
    }
    cache.getCancelCriterion().checkCancelInProgress(null);
    InternalDistributedSystem ds = cache.getDistributedSystem();
    DM dm = ds.getDistributionManager();

    txcm.setDM(dm);
    txcm.setAckRequired(false);
    txcm.setDisableListeners(true);
    cache.getTxManager().setTXState(null);
    txcm.hookupRegions(dm);
    txcm.basicProcess();
  }

  
  @Override 
  protected TXRegionStub generateRegionStub(LocalRegion region) {
    return new ClientTXRegionStub(region);
  }
  
  @Override 
  protected void validateRegionCanJoinTransaction(LocalRegion region) throws TransactionException {
    if(!region.hasServerProxy()) {
      throw new TransactionException("Region " + region.getName() + " is local to this client and cannot be used in a transaction.");
    } else if (this.firstProxy != null && this.firstProxy.getPool() != region.getServerProxy().getPool()) {
      throw new TransactionException("Region " + region.getName() + " is using a different server pool than other regions in this transaction.");
    }
  }

  @Override
  public void rollback() {
    if (this.internalAfterSendRollback != null) {
      this.internalAfterSendRollback.run();
    }
    try {
      this.firstProxy.rollback(proxy.getTxId().getUniqId());
    } finally {
      this.firstProxy.getPool().releaseServerAffinity();
    }
  }

  @Override
  public void afterCompletion(int status) {
    try {
      TXCommitMessage txcm = this.firstProxy.afterCompletion(status, proxy.getTxId().getUniqId());
      if (status == Status.STATUS_COMMITTED) { 
        if (txcm == null) {
        throw new TransactionInDoubtException(LocalizedStrings.ClientTXStateStub_COMMIT_FAILED_ON_SERVER.toLocalizedString());
        } else {
          afterServerCommit(txcm);
        }
      } else if (status == Status.STATUS_ROLLEDBACK){
        if (this.internalAfterSendRollback != null) {
          this.internalAfterSendRollback.run();
        }
        this.firstProxy.getPool().releaseServerAffinity();
      }
    } finally {
      if (status == Status.STATUS_COMMITTED) {
        // rollback does not grab locks
        this.lockReq.releaseLocal();
      }
      this.firstProxy.getPool().releaseServerAffinity();
    }
  }
  
  @Override
  public void beforeCompletion() {
    obtainLocalLocks();
    this.firstProxy.beforeCompletion(proxy.getTxId().getUniqId());
  }

  public InternalDistributedMember getOriginatingMember() {
    /*
     * Client member id is implied from the connection so we don't need this
     */
    return null;
  }

  public boolean isMemberIdForwardingRequired() {
    /*
     * Client member id is implied from the connection so we don't need this
     * Forwarding will occur on the server-side stub
     */
    return false;
  }

  public TXCommitMessage getCommitMessage() {
    /* client gets the txcommit message during Op processing and doesn't need it here */
    return null;
  }

  public void suspend() {
    this.serverAffinityLocation = this.firstProxy.getPool().getServerAffinityLocation();
    this.firstProxy.getPool().releaseServerAffinity();
    if (logger.isDebugEnabled()) {
      logger.debug("TX: suspending transaction: {} server delegate: {}", getTransactionId(), this.serverAffinityLocation);
    }
  }

  public void resume() {
    this.firstProxy.getPool().setupServerAffinity(true);
    this.firstProxy.getPool().setServerAffinityLocation(this.serverAffinityLocation);
    if (logger.isDebugEnabled()) {
      logger.debug("TX: resuming transaction: {} server delegate: {}", getTransactionId(), this.serverAffinityLocation);
    }
  }

  /**
   * test hook - maintain a list of tx operations
   */
  public void recordTXOperation(ServerRegionDataAccess region, ServerRegionOperation op, Object key, Object arguments[]) {
    if (ClientTXStateStub.transactionRecordingEnabled()) {
      this.recordedOperations.add(new TransactionalOperation(this, region.getRegionName(), op, key, arguments));
    }
  }

  /** Add an internal callback which is run after the the local locks
   * are obtained
   */
  public void setAfterLocalLocks(Runnable afterLocalLocks) {
    this.internalAfterLocalLocks = afterLocalLocks;
  }
}
