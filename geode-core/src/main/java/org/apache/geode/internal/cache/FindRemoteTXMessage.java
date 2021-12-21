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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ReplySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.partitioned.PartitionMessage;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Message to all the peers to ask which member hosts the transaction for the given transaction id
 */
public class FindRemoteTXMessage extends HighPriorityDistributionMessage
    implements MessageWithReply {

  private static final Logger logger = LogService.getLogger();

  private TXId txId;

  private int processorId;

  public FindRemoteTXMessage() {
    // do nothing
  }

  public FindRemoteTXMessage(TXId txid, int processorId, Set recipients) {
    super();
    setRecipients(recipients);
    txId = txid;
    this.processorId = processorId;
  }

  /**
   * Asks all the peers if they host a transaction for the given txId
   *
   * @param txId the transaction id
   * @return reply processor containing memberId of the member that hosts the transaction and a
   *         recently committed transactionMessage if any
   */
  public static FindRemoteTXMessageReplyProcessor send(Cache cache, TXId txId) {
    final InternalDistributedSystem system =
        (InternalDistributedSystem) cache.getDistributedSystem();
    DistributionManager dm = system.getDistributionManager();
    Set recipients = dm.getOtherDistributionManagerIds();
    FindRemoteTXMessageReplyProcessor processor =
        new FindRemoteTXMessageReplyProcessor(dm, recipients, txId);
    FindRemoteTXMessage msg = new FindRemoteTXMessage(txId, processor.getProcessorId(), recipients);
    dm.putOutgoing(msg);
    return processor;
  }

  @Override
  public int getDSFID() {
    return FIND_REMOTE_TX_MESSAGE;
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    boolean sendReply = true;
    Throwable thr = null;
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("processing {}", this);
      }
      FindRemoteTXMessageReply reply = new FindRemoteTXMessageReply();
      InternalCache cache = dm.getCache();
      if (cache != null) {
        TXManagerImpl mgr = (TXManagerImpl) cache.getCacheTransactionManager();
        mgr.waitForCompletingTransaction(txId); // in case there is a lost commit going on
        reply.isHostingTx = mgr.isHostedTxInProgress(txId) || mgr.isHostedTxRecentlyCompleted(txId);
        if (!reply.isHostingTx) {
          // lookup in CMTTracker if a partial commit message exists
          TXCommitMessage partialMessage = TXCommitMessage.getTracker().getTXCommitMessage(txId);
          if (partialMessage != null) {
            reply.txCommitMessage = partialMessage;
            reply.isPartialCommitMessage = true;
          }
          // cleanup the local txStateProxy fixes bug 43069
          mgr.removeHostedTXState(txId, true);
        }
      }
      reply.setRecipient(getSender());
      reply.setProcessorId(processorId);
      getReplySender(dm).putOutgoing(reply);
      sendReply = false;
      if (logger.isDebugEnabled()) {
        logger.debug("TX: FoundRemoteTXMessage: isHostingTx for txid:{}? {} isPartialCommit? {}",
            txId, reply.isHostingTx, reply.isPartialCommitMessage);
      }
    } catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error. We're poisoned
      // now, so don't let this thread continue.
      throw err;
    } catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above). However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      if (sendReply) {
        thr = t;
      }
    } finally {
      ReplySender rs = getReplySender(dm);
      if (sendReply && (processorId != 0 || (rs != dm))) {
        ReplyException rex = null;
        if (thr != null) {
          rex = new ReplyException(thr);
        }
        ReplyMessage.send(getSender(), processorId, rex, getReplySender(dm));
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder buff = new StringBuilder();
    String className = getClass().getName();
    buff.append(className.substring(
        className.indexOf(PartitionMessage.PN_TOKEN) + PartitionMessage.PN_TOKEN.length())); // partition.<foo>
    buff.append("(txId=").append(txId).append("; sender=").append(getSender())
        .append("; processorId=").append(processorId);
    buff.append(")");
    return buff.toString();
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(txId, out);
    out.writeInt(processorId);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    txId = DataSerializer.readObject(in);
    processorId = in.readInt();
  }

  public static class FindRemoteTXMessageReplyProcessor extends ReplyProcessor21 {

    private InternalDistributedMember hostingMember;
    private TXCommitMessage txCommit;
    private final TXId txId;
    private final Set<TXCommitMessage> partialCommitMessages = new HashSet<TXCommitMessage>();

    public FindRemoteTXMessageReplyProcessor(DistributionManager dm, Collection initMembers,
        TXId txId) {
      super(dm, initMembers);
      this.txId = txId;
    }

    @Override
    public void process(DistributionMessage msg) {
      if (msg instanceof FindRemoteTXMessageReply) {
        FindRemoteTXMessageReply reply = (FindRemoteTXMessageReply) msg;
        if (reply.isHostingTx) {
          hostingMember = msg.getSender();
        } else if (reply.isPartialCommitMessage) {
          partialCommitMessages.add(reply.txCommitMessage);
        }
      }
      super.process(msg);
    }

    /**
     * @return the member that is hosting the tx
     */
    public InternalDistributedMember getHostingMember() {
      return hostingMember;
    }

    @Override
    public boolean stillWaiting() {
      return hostingMember == null && super.stillWaiting();
    }

    /**
     * @return if hosting member is null, the rebuilt TXCommitMessage from partial TXCommitMessages
     *         distributed to peers during commit processing
     */
    public TXCommitMessage getTxCommitMessage() {
      if (txCommit != null) {
        return txCommit;
      }
      if (!partialCommitMessages.isEmpty()) {
        TXCommitMessage localTXMessage = TXCommitMessage.getTracker().getTXCommitMessage(txId);
        if (localTXMessage != null) {
          partialCommitMessages.add(localTXMessage);
        }
        txCommit = TXCommitMessage.combine(partialCommitMessages);
      }
      return txCommit;
    }
  }

  @Override
  public boolean sendViaUDP() {
    return true;
  }

  /**
   * Reply message for {@link FindRemoteTXMessage}. Reply is a boolean to indicate if the recipient
   * hosts or has recently hosted the tx state. If the member did host the txState previously, reply
   * contains the complete TXCommitMessage representing the tx.
   */
  public static class FindRemoteTXMessageReply extends ReplyMessage {
    protected boolean isHostingTx;
    protected boolean isPartialCommitMessage;
    protected TXCommitMessage txCommitMessage;

    public FindRemoteTXMessageReply() {}

    @Override
    public int getDSFID() {
      return FIND_REMOTE_TX_REPLY;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeBoolean(isHostingTx);
      boolean sendTXCommitMessage = txCommitMessage != null;
      out.writeBoolean(sendTXCommitMessage);
      if (sendTXCommitMessage) {
        out.writeBoolean(isPartialCommitMessage);
        // since this message is going to a peer, reset client version
        txCommitMessage.setClientVersion(null); // fixes bug 46529
        InternalDataSerializer.writeDSFID(txCommitMessage, out);
      }
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      isHostingTx = in.readBoolean();
      if (in.readBoolean()) {
        isPartialCommitMessage = in.readBoolean();
        txCommitMessage = (TXCommitMessage) InternalDataSerializer.readDSFID(in);
      }
    }
  }
}
