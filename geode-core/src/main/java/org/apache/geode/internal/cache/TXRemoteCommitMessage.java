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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ReplySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.tx.RemoteOperationMessage.RemoteOperationResponse;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

public class TXRemoteCommitMessage extends TXMessage {
  private static final Logger logger = LogService.getLogger();

  /** for deserialization */
  public TXRemoteCommitMessage() {
    // nothing
  }

  public TXRemoteCommitMessage(int txUniqId, InternalDistributedMember onBehalfOfClientMember,
      ReplyProcessor21 processor) {
    super(txUniqId, onBehalfOfClientMember, processor);
  }

  @Override
  public int getProcessorType() {
    return OperationExecutors.WAITING_POOL_EXECUTOR;
  }

  public static RemoteCommitResponse send(Cache cache, int txUniqId,
      InternalDistributedMember onBehalfOfClientMember, DistributedMember recipient) {
    final InternalDistributedSystem system =
        (InternalDistributedSystem) cache.getDistributedSystem();
    final Set<DistributedMember> recipients = Collections.singleton(recipient);
    RemoteCommitResponse p = new RemoteCommitResponse(system, recipients);
    TXMessage msg = new TXRemoteCommitMessage(txUniqId, onBehalfOfClientMember, p);

    msg.setRecipients(recipients);
    system.getDistributionManager().putOutgoing(msg);
    return p;
  }

  @Override
  protected boolean operateOnTx(TXId txId, ClusterDistributionManager dm)
      throws RemoteOperationException {
    InternalCache cache = dm.getCache();
    TXManagerImpl txMgr = cache.getTXMgr();

    if (logger.isDebugEnabled()) {
      logger.debug("TX: Committing: {}", txId);
    }
    final TXStateProxy txState = txMgr.getTXState();
    TXCommitMessage commitMessage = txMgr.getRecentlyCompletedMessage(txId);
    try {
      // do the actual commit, only if it was not done before
      if (commitMessage != null) {
        if (logger.isDebugEnabled()) {
          logger.debug("TX: found a previously committed transaction:{}", txId);
        }
        if (txMgr.isExceptionToken(commitMessage)) {
          throw txMgr.getExceptionForToken(commitMessage, txId);
        }
      } else {
        // if no TXState was created (e.g. due to only getEntry/size operations
        // that don't start remote TX) then ignore
        if (txState != null) {
          txState.setCommitOnBehalfOfRemoteStub(true);
          txMgr.commit();
          commitMessage = txState.getCommitMessage();
        }
      }
    } finally {
      txMgr.removeHostedTXState(txId);
    }
    TXRemoteCommitReplyMessage.send(getSender(), getProcessorId(), commitMessage,
        getReplySender(dm));

    /*
     * return false so there isn't another reply
     */
    return false;
  }

  @Override
  public int getDSFID() {
    return TX_REMOTE_COMMIT_MESSAGE;
  }

  @Override
  public boolean canStartRemoteTransaction() {
    // we can reach here when a transaction already completed prior to client failover (bug #42743)
    return true;
  }

  /**
   * This message is used for the reply to a remote commit operation: a commit from a stub to the tx
   * host. This is the reply to a {@link TXRemoteCommitMessage}.
   *
   * @since GemFire 6.5
   */
  public static class TXRemoteCommitReplyMessage extends ReplyMessage {

    private transient TXCommitMessage commitMessage;

    /*
     * Used on the fromData side to transfer the value bytes to the requesting thread
     */
    public transient byte[] valueInBytes;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public TXRemoteCommitReplyMessage() {
      // nothing
    }

    public TXRemoteCommitReplyMessage(DataInput in) throws IOException, ClassNotFoundException {
      fromData(in, InternalDataSerializer.createDeserializationContext(in));
    }

    private TXRemoteCommitReplyMessage(int processorId, TXCommitMessage val) {
      setProcessorId(processorId);
      this.commitMessage = val;
    }

    /** GetReplyMessages are always processed in-line */
    @Override
    public boolean getInlineProcess() {
      return true;
    }

    /**
     * Return the value from the get operation, serialize it bytes as late as possible to avoid
     * making un-neccesary byte[] copies. De-serialize those same bytes as late as possible to avoid
     * using precious threads (aka P2P readers).
     *
     * @param recipient the origin VM that performed the get
     * @param processorId the processor on which the origin thread is waiting
     * @param val the raw value that will eventually be serialized
     * @param replySender distribution manager used to send the reply
     */
    public static void send(InternalDistributedMember recipient, int processorId,
        TXCommitMessage val, ReplySender replySender) throws RemoteOperationException {
      Assert.assertTrue(recipient != null, "TXRemoteCommitReply NULL reply message");
      if (val != null) {
        val.setClientVersion(null);
      }
      TXRemoteCommitReplyMessage m = new TXRemoteCommitReplyMessage(processorId, val);
      m.setRecipient(recipient);
      replySender.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the message.
     *
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DistributionManager dm, ReplyProcessor21 processor) {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE,
            "TXRemoteCommitReply process invoking reply processor with processorId:{}",
            this.processorId);
      }

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "TXRemoteCommitReply processor not found");
        }
        return;
      }
      processor.process(this);
    }

    @Override
    public int getDSFID() {
      return R_REMOTE_COMMIT_REPLY_MESSAGE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeObject(commitMessage, out);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.commitMessage = (TXCommitMessage) DataSerializer.readObject(in);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("TXRemoteCommitReplyMessage ").append("processorid=").append(this.processorId)
          .append(" reply to sender ").append(this.getSender());
      return sb.toString();
    }

    public TXCommitMessage getCommitMessage() {
      // TODO Auto-generated method stub
      return commitMessage;
    }
  }

  /**
   * A processor to capture the value returned by
   * {@link org.apache.geode.internal.cache.TXRemoteCommitMessage.TXRemoteCommitReplyMessage}
   *
   * @since GemFire 6.6
   */
  public static class RemoteCommitResponse extends RemoteOperationResponse {
    private volatile TXCommitMessage commitMessage;
    private volatile long start;

    public RemoteCommitResponse(InternalDistributedSystem ds, Set recipients) {
      super(ds, recipients, true);
    }

    public TXCommitMessage getCommitMessage() {
      // TODO Auto-generated method stub
      return commitMessage;
    }

    @Override
    public void process(DistributionMessage msg) {
      if (DistributionStats.enableClockStats) {
        this.start = DistributionStats.getStatTime();
      }
      if (msg instanceof TXRemoteCommitReplyMessage) {
        TXRemoteCommitReplyMessage reply = (TXRemoteCommitReplyMessage) msg;
        // De-serialization needs to occur in the requesting thread, not a P2P thread
        // (or some other limited resource)
        this.commitMessage = reply.getCommitMessage();
      }
      super.process(msg);
    }

    /**
     * @return Object associated with the key that was sent in the get message
     */
    public TXCommitMessage waitForResponse() throws RemoteOperationException {
      waitForRemoteResponse();
      if (DistributionStats.enableClockStats) {
        getDistributionManager().getStats().incReplyHandOffTime(this.start);
      }
      return commitMessage;
    }
  }

}
