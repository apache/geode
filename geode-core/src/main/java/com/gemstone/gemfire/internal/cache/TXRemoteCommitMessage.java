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
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.TransactionDataNotColocatedException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.DistributionStats;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.ReplySender;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.RemoteOperationMessage.RemoteOperationResponse;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * 
 *
 */
public class TXRemoteCommitMessage extends TXMessage {

  private static final Logger logger = LogService.getLogger();
  
  /** for deserialization */
  public TXRemoteCommitMessage() {
  }

  public TXRemoteCommitMessage(int txUniqId,InternalDistributedMember onBehalfOfClientMember, ReplyProcessor21 processor) {
    super(txUniqId,onBehalfOfClientMember, processor);
  }
  
  @Override
  public int getProcessorType() {
    return DistributionManager.WAITING_POOL_EXECUTOR;
  }

  public static RemoteCommitResponse send(Cache cache,
      int txUniqId,InternalDistributedMember onBehalfOfClientMember, DistributedMember recipient) {
    final InternalDistributedSystem system = 
                    (InternalDistributedSystem)cache.getDistributedSystem();
    final Set<DistributedMember> recipients = Collections.singleton(recipient);
    RemoteCommitResponse p = new RemoteCommitResponse(system,recipients);
    TXMessage msg = new TXRemoteCommitMessage(txUniqId,onBehalfOfClientMember, p);
    
    msg.setRecipients(recipients);
    system.getDistributionManager().putOutgoing(msg); 
    return p;
  }

  @Override
  protected boolean operateOnTx(TXId txId,DistributionManager dm) throws RemoteOperationException {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    TXManagerImpl txMgr = cache.getTXMgr();
    
    if (logger.isDebugEnabled()) {
      logger.debug("TX: Committing: {}", txId);
    }
    final TXStateProxy txState = txMgr.getTXState();
    TXCommitMessage cmsg = null;
    try {
      // do the actual commit, only if it was not done before
      if (txMgr.isHostedTxRecentlyCompleted(txId)) {
        if (logger.isDebugEnabled()) {
          logger.debug("TX: found a previously committed transaction:{}", txId);
        }
        cmsg = txMgr.getRecentlyCompletedMessage(txId);
        if (txMgr.isExceptionToken(cmsg)) {
          throw txMgr.getExceptionForToken(cmsg, txId);
        }
      } else {
        // if no TXState was created (e.g. due to only getEntry/size operations
        // that don't start remote TX) then ignore
        if (txState != null) {
          txState.setCommitOnBehalfOfRemoteStub(true);
          txMgr.commit();
          cmsg = txState.getCommitMessage();
        }
      }
    } finally {
      txMgr.removeHostedTXState(txId);
    }
    TXRemoteCommitReplyMessage.send(getSender(), getProcessorId(), cmsg, getReplySender(dm));
    
    /*
     * return false so there isn't another reply */
    return false;
  }

  public int getDSFID() {
    return TX_REMOTE_COMMIT_MESSAGE;
  }
  
  @Override
  public boolean canStartRemoteTransaction() {
    // we can reach here when a transaction already completed prior to client failover (bug #42743)
    return true;
  }
  

  
  
  /**
   * This message is used for the reply to a
   * remote commit operation: a commit from a stub to the tx host. This is the
   * reply to a {@link TXRemoteCommitMessage}.
   * 
   * @since 6.5
   */
  public static final class TXRemoteCommitReplyMessage extends ReplyMessage
   {
    private transient TXCommitMessage commitMessage;
    /*
     * Used on the fromData side to transfer the value bytes to the requesting
     * thread
     */
    public transient byte[] valueInBytes;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public TXRemoteCommitReplyMessage() {
    }

    public TXRemoteCommitReplyMessage(DataInput in) throws IOException,
        ClassNotFoundException {
      fromData(in);
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
     * Return the value from the get operation, serialize it bytes as late as
     * possible to avoid making un-neccesary byte[] copies.  De-serialize those 
     * same bytes as late as possible to avoid using precious threads (aka P2P readers). 
     * @param recipient the origin VM that performed the get
     * @param processorId the processor on which the origin thread is waiting
     * @param val the raw value that will eventually be serialized 
     * @param replySender distribution manager used to send the reply
     */
    public static void send(InternalDistributedMember recipient, 
        int processorId, TXCommitMessage val, ReplySender replySender)
        throws RemoteOperationException
    {
      Assert.assertTrue(recipient != null,
          "TXRemoteCommitReply NULL reply message");
      TXRemoteCommitReplyMessage m = new TXRemoteCommitReplyMessage(processorId, val);
      m.setRecipient(recipient);
      replySender.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the
     * message.
     * 
     * @param dm
     *          the distribution manager that is processing the message.
     */
    @Override
    public void process(final DM dm, ReplyProcessor21 processor)
    {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "TXRemoteCommitReply process invoking reply processor with processorId:{}", this.processorId);
      }

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "TXRemoteCommitReply processor not found");
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
    public void toData(DataOutput out) throws IOException
    {
      super.toData(out);
      DataSerializer.writeObject(commitMessage, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException
    {
      super.fromData(in);
      this.commitMessage = (TXCommitMessage)DataSerializer.readObject(in);
    }

    @Override
    public String toString()
    {
      StringBuffer sb = new StringBuffer();
      sb.append("TXRemoteCommitReplyMessage ").append("processorid=").append(
          this.processorId).append(" reply to sender ")
          .append(this.getSender());
      return sb.toString();
    }

    public TXCommitMessage getCommitMessage() {
      // TODO Auto-generated method stub
      return commitMessage;
    }
  }

  /**
   * A processor to capture the value returned by {@link 
   * com.gemstone.gemfire.internal.cache.TXRemoteCommitMessage.TXRemoteCommitReplyMessage}
   * 
   * @since 6.6
   */
  public static class RemoteCommitResponse extends RemoteOperationResponse
   {
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
    public void process(DistributionMessage msg)
    {
      if (DistributionStats.enableClockStats) {
        this.start = DistributionStats.getStatTime();
      }
      if (msg instanceof TXRemoteCommitReplyMessage) {
        TXRemoteCommitReplyMessage reply = (TXRemoteCommitReplyMessage)msg;
        // De-serialization needs to occur in the requesting thread, not a P2P thread
        // (or some other limited resource)
        this.commitMessage = reply.getCommitMessage();
      }
      super.process(msg);
    }

    /**
     * @return Object associated with the key that was sent in the get message
     */
    public TXCommitMessage waitForResponse() 
        throws RemoteOperationException {
      try {
//        waitForRepliesUninterruptibly();
          waitForCacheException();
          if (DistributionStats.enableClockStats) {
            getDistributionManager().getStats().incReplyHandOffTime(this.start);
          }
      }
      catch (RemoteOperationException e) {
        final String msg = "RemoteCommitResponse got RemoteOperationException; rethrowing";
        logger.debug(msg, e);
        throw e;
      } catch (TransactionDataNotColocatedException e) {
        // Throw this up to user!
        throw e;
      }
      return commitMessage;
    }
  }

}
