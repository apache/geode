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


import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.TransactionDataRebalancedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.cache.TransactionInDoubtException;
import org.apache.geode.cache.client.internal.ServerRegionDataAccess;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ReliableReplyException;
import org.apache.geode.distributed.internal.ReliableReplyProcessor21;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.TXRemoteCommitMessage.RemoteCommitResponse;
import org.apache.geode.internal.cache.tx.BucketTXRegionStub;
import org.apache.geode.internal.cache.tx.DistributedTXRegionStub;
import org.apache.geode.internal.cache.tx.PartitionedTXRegionStub;
import org.apache.geode.internal.cache.tx.TXRegionStub;
import org.apache.geode.internal.cache.tx.TransactionalOperation.ServerRegionOperation;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;

public class PeerTXStateStub extends TXStateStub {

  protected static final Logger logger = LogService.getLogger();

  private InternalDistributedMember originatingMember = null;
  protected TXCommitMessage commitMessage = null;

  public PeerTXStateStub(TXStateProxy stateProxy, DistributedMember target,
      InternalDistributedMember onBehalfOfClient) {
    super(stateProxy, target);
    this.originatingMember = onBehalfOfClient;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.TXStateInterface#rollback()
   */
  @Override
  public void rollback() {
    /*
     * txtodo: work this into client realm
     */
    ReliableReplyProcessor21 response = TXRemoteRollbackMessage.send(this.proxy.getCache(),
        this.proxy.getTxId().getUniqId(), getOriginatingMember(), this.target);
    if (this.internalAfterSendRollback != null) {
      this.internalAfterSendRollback.run();
    }

    try {
      response.waitForReplies();
    } catch (PrimaryBucketException pbe) {
      // ignore this
    } catch (ReplyException e) {
      this.proxy.getCache().getCancelCriterion().checkCancelInProgress(e);
      if (e.getCause() != null && e.getCause() instanceof CancelException) {
        // other cache must have closed (bug #43649), so the transaction is lost
        if (this.internalAfterSendRollback != null) {
          this.internalAfterSendRollback.run();
        }
      } else {
        throw new TransactionException(
            LocalizedStrings.TXStateStub_ROLLBACK_ON_NODE_0_FAILED.toLocalizedString(target), e);
      }
    } catch (Exception e) {
      this.getCache().getCancelCriterion().checkCancelInProgress(e);
      throw new TransactionException(
          LocalizedStrings.TXStateStub_ROLLBACK_ON_NODE_0_FAILED.toLocalizedString(target), e);
    } finally {
      cleanup();
    }
  }

  @Override
  public void commit() throws CommitConflictException {
    assert target != null;
    /*
     * txtodo: Going to need to deal with client here
     */
    RemoteCommitResponse message = TXRemoteCommitMessage.send(this.proxy.getCache(),
        this.proxy.getTxId().getUniqId(), this.getOriginatingMember(), target);

    if (this.internalAfterSendCommit != null) {
      this.internalAfterSendCommit.run();
    }

    try {
      commitMessage = message.waitForResponse();
    } catch (CommitConflictException e) {
      throw e;
    } catch (TransactionException te) {
      throw te;
    } catch (ReliableReplyException e) {
      if (e.getCause() != null) {
        throw new TransactionInDoubtException(e.getCause());
      } else {
        throw new TransactionInDoubtException(e);
      }
    } catch (ReplyException e) {
      if (e.getCause() instanceof CommitConflictException) {
        throw (CommitConflictException) e.getCause();
      } else if (e.getCause() instanceof TransactionException) {
        throw (TransactionException) e.getCause();
      }
      /*
       * if(e.getCause()!=null) { throw new CommitConflictException(e.getCause()); } else { throw
       * new CommitConflictException(e); }
       */
      if (e.getCause() != null) {
        throw new TransactionInDoubtException(e.getCause());
      } else {
        throw new TransactionInDoubtException(e);
      }
    } catch (Exception e) {
      this.getCache().getCancelCriterion().checkCancelInProgress(e);
      Throwable eCause = e.getCause();
      if (eCause != null) {
        if (eCause instanceof ForceReattemptException) {
          if (eCause.getCause() instanceof PrimaryBucketException) {
            // data rebalanced
            TransactionDataRebalancedException tdnce =
                new TransactionDataRebalancedException(eCause.getCause().getMessage());
            tdnce.initCause(eCause.getCause());
            throw tdnce;
          } else {
            // We cannot be sure that the member departed starting to process commit request,
            // so throw a TransactionInDoubtException rather than a TransactionDataNodeHasDeparted.
            // fixes 44939
            TransactionInDoubtException tdnce =
                new TransactionInDoubtException(e.getCause().getMessage());
            tdnce.initCause(eCause);
            throw tdnce;
          }
        }
        throw new TransactionInDoubtException(eCause);
      } else {
        throw new TransactionInDoubtException(e);
      }
    } finally {
      cleanup();
    }
  }

  protected void cleanup() {
    for (TXRegionStub regionStub : regionStubs.values()) {
      regionStub.cleanup();
    }
  }

  @Override
  protected TXRegionStub generateRegionStub(InternalRegion region) {
    TXRegionStub stub = null;
    if (region.getPartitionAttributes() != null) {
      // a partitioned region
      stub = new PartitionedTXRegionStub(this, (PartitionedRegion) region);
    } else if (region.getScope().isLocal()) {
      // GEODE-3744 Local region should not be involved in a transaction on a PeerTXStateStub
      throw new TransactionException(
          "Local region " + region + " should not participate in a transaction not hosted locally");
    } else if (region.isUsedForPartitionedRegionBucket()) {
      stub = new BucketTXRegionStub(this, (BucketRegion) region);
    } else {
      // This is a dist region
      stub = new DistributedTXRegionStub(this, (DistributedRegion) region);
    }
    return stub;
  }

  @Override
  protected void validateRegionCanJoinTransaction(InternalRegion region)
      throws TransactionException {
    /*
     * Ok is this region legit to enter into tx?
     */
    if (region.hasServerProxy()) {
      /*
       * This is a c/s region in a peer tx. nope!
       */
      throw new TransactionException("Can't involve c/s region in peer tx");
    }

  }


  @Override
  public void afterCompletion(int status) {
    RemoteCommitResponse response = JtaAfterCompletionMessage.send(this.proxy.getCache(),
        this.proxy.getTxId().getUniqId(), getOriginatingMember(), status, this.target);
    try {
      this.proxy.getTxMgr().setTXState(null);
      this.commitMessage = response.waitForResponse();
      if (logger.isDebugEnabled()) {
        logger.debug("afterCompletion received commit response of {}", this.commitMessage);
      }

    } catch (Exception e) {
      throw new TransactionException(e);
      // TODO throw a better exception
    } finally {
      cleanup();
    }
  }

  public InternalDistributedMember getOriginatingMember() {
    /*
     * This needs to be set to the clients member id if the client originated the tx
     */
    return originatingMember;
  }

  public void setOriginatingMember(InternalDistributedMember clientMemberId) {
    /*
     * This TX is on behalf of a client, so we have to send the client's member id around
     */
    this.originatingMember = clientMemberId;
  }

  @Override
  public boolean isMemberIdForwardingRequired() {
    return getOriginatingMember() != null;
  }

  @Override
  public TXCommitMessage getCommitMessage() {
    return commitMessage;
  }

  @Override
  public void suspend() {
    // no special tasks to perform
  }


  @Override
  public void resume() {
    // no special tasks to perform
  }

  @Override
  public void recordTXOperation(ServerRegionDataAccess region, ServerRegionOperation op, Object key,
      Object arguments[]) {
    // no-op here
  }
}
