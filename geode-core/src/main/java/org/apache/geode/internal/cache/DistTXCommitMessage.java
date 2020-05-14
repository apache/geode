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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CommitIncompleteException;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ReplySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.TXEntryState.DistTxThinEntryState;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class DistTXCommitMessage extends TXMessage {

  private static final Logger logger = LogService.getLogger();

  protected ArrayList<ArrayList<DistTxThinEntryState>> entryStateList = null;

  /** for deserialization */
  public DistTXCommitMessage() {}

  public DistTXCommitMessage(TXId txUniqId, InternalDistributedMember onBehalfOfClientMember,
      ReplyProcessor21 processor) {
    super(txUniqId.getUniqId(), onBehalfOfClientMember, processor);
  }

  @Override
  public int getDSFID() {
    return DISTTX_COMMIT_MESSAGE;
  }

  @Override
  protected boolean operateOnTx(TXId txId, ClusterDistributionManager dm)
      throws RemoteOperationException {
    if (logger.isDebugEnabled()) {
      logger.debug("DistTXCommitMessage.operateOnTx: Tx {}", txId);
    }

    InternalCache cache = dm.getCache();
    TXManagerImpl txMgr = cache.getTXMgr();
    final TXStateProxy txStateProxy = txMgr.getTXState();
    TXCommitMessage commitMessage = txMgr.getRecentlyCompletedMessage(txId);
    try {
      // do the actual commit, only if it was not done before
      if (commitMessage != null) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "DistTXCommitMessage.operateOnTx: found a previously committed transaction:{}", txId);
        }
        if (txMgr.isExceptionToken(commitMessage)) {
          throw txMgr.getExceptionForToken(commitMessage, txId);
        }
      } else {
        // [DISTTX] TODO - Handle scenarios of no txState
        // if no TXState was created (e.g. due to only getEntry/size operations
        // that don't start remote TX) then ignore
        if (txStateProxy != null) {
          /*
           * [DISTTX] TODO See how other exceptions are caught and send on wire, than throwing?
           *
           * This can be spared since it will be programming bug
           */
          if (!txStateProxy.isDistTx() || txStateProxy.isCreatedOnDistTxCoordinator()) {
            throw new UnsupportedOperationInTransactionException(
                String.format("Expected %s during a distributed transaction but got %s",
                    "DistTXStateProxyImplOnDatanode", txStateProxy.getClass().getSimpleName()));
          }
          if (logger.isDebugEnabled()) {
            logger.debug(
                "DistTXCommitMessage.operateOnTx Commiting {} "
                    + " incoming entryEventList:{} coming from {} ",
                txId, DistTXStateProxyImplOnCoordinator.printEntryEventList(this.entryStateList),
                this.getSender().getId());
          }

          // Set Member's ID to all entry states
          String memberID = this.getSender().getId();
          for (ArrayList<DistTxThinEntryState> esList : this.entryStateList) {
            for (DistTxThinEntryState es : esList) {
              es.setMemberID(memberID);
            }
          }

          ((DistTXStateProxyImplOnDatanode) txStateProxy)
              .populateDistTxEntryStates(this.entryStateList);
          txStateProxy.setCommitOnBehalfOfRemoteStub(true);

          txMgr.commit();

          commitMessage = txStateProxy.getCommitMessage();
        }
      }
    } finally {
      txMgr.removeHostedTXState(txId);
    }
    DistTXCommitReplyMessage.send(getSender(), getProcessorId(), commitMessage, getReplySender(dm));

    /*
     * return false so there isn't another reply
     */
    return false;
  }


  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.entryStateList = DataSerializer.readArrayList(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeArrayList(entryStateList, out);
  }

  @Override
  public boolean isTransactionDistributed() {
    return true;
  }

  @Override
  public boolean canStartRemoteTransaction() {
    return true;
  }

  public void setEntryStateList(ArrayList<ArrayList<DistTxThinEntryState>> entryStateList) {
    this.entryStateList = entryStateList;
  }

  /**
   * This message is used for the reply to a Dist Tx Phase Two commit operation: a commit from a
   * stub to the tx host. This is the reply to a {@link DistTXCommitMessage}.
   *
   */
  public static class DistTXCommitReplyMessage extends ReplyMessage {
    private transient TXCommitMessage commitMessage;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public DistTXCommitReplyMessage() {}

    public DistTXCommitReplyMessage(DataInput in) throws IOException, ClassNotFoundException {
      fromData(in, InternalDataSerializer.createDeserializationContext(in));
    }

    private DistTXCommitReplyMessage(int processorId, TXCommitMessage val) {
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
      Assert.assertTrue(recipient != null, "DistTXCommitPhaseTwoReplyMessage NULL reply message");
      DistTXCommitReplyMessage m = new DistTXCommitReplyMessage(processorId, val);
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
            "DistTXCommitPhaseTwoReplyMessage process invoking reply processor with processorId:{}",
            this.processorId);
      }

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE,
              "DistTXCommitPhaseTwoReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);
    }

    @Override
    public int getDSFID() {
      return DISTTX_COMMIT_REPLY_MESSAGE;
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
      sb.append("DistTXCommitPhaseTwoReplyMessage ").append("processorid=").append(this.processorId)
          .append(" reply to sender ").append(this.getSender());
      return sb.toString();
    }

    public TXCommitMessage getCommitMessage() {
      // TODO Auto-generated method stub
      return commitMessage;
    }
  }

  /**
   * Reply processor which collects all CommitReplyExceptions for Dist Tx and emits a detailed
   * failure exception if problems occur
   *
   * [DISTTX] TODO see if need ReliableReplyProcessor21? departed members?
   */
  public static class DistTxCommitReplyProcessor extends ReplyProcessor21 {
    private HashMap<DistributedMember, DistTXCoordinatorInterface> msgMap;
    private Map<DistributedMember, TXCommitMessage> commitResponseMap;
    private transient TXId txIdent = null;

    public DistTxCommitReplyProcessor(TXId txUniqId, DistributionManager dm, Set initMembers,
        HashMap<DistributedMember, DistTXCoordinatorInterface> msgMap) {
      super(dm, initMembers);
      this.msgMap = msgMap;
      // [DISTTX] TODO Do we need synchronised map?
      this.commitResponseMap =
          Collections.synchronizedMap(new HashMap<DistributedMember, TXCommitMessage>());
      this.txIdent = txUniqId;
    }

    @Override
    public void process(DistributionMessage msg) {
      if (msg instanceof DistTXCommitReplyMessage) {
        DistTXCommitReplyMessage reply = (DistTXCommitReplyMessage) msg;
        this.commitResponseMap.put(reply.getSender(), reply.getCommitMessage());
      }
      super.process(msg);
    }

    public void waitForPrecommitCompletion() {
      try {
        waitForRepliesUninterruptibly();
      } catch (DistTxCommitExceptionCollectingException e) {
        e.handlePotentialCommitFailure(msgMap);
      }
    }

    @Override
    protected void processException(DistributionMessage msg, ReplyException ex) {
      if (msg instanceof ReplyMessage) {
        synchronized (this) {
          if (this.exception == null) {
            // Exception Container
            this.exception = new DistTxCommitExceptionCollectingException(txIdent);
          }
          DistTxCommitExceptionCollectingException cce =
              (DistTxCommitExceptionCollectingException) this.exception;
          if (ex instanceof CommitReplyException) {
            CommitReplyException cre = (CommitReplyException) ex;
            cce.addExceptionsFromMember(msg.getSender(), cre.getExceptions());
          } else {
            cce.addExceptionsFromMember(msg.getSender(), Collections.singleton(ex));
          }
        }
      }
    }

    @Override
    protected boolean stopBecauseOfExceptions() {
      return false;
    }

    public Set getCacheClosedMembers() {
      if (this.exception != null) {
        DistTxCommitExceptionCollectingException cce =
            (DistTxCommitExceptionCollectingException) this.exception;
        return cce.getCacheClosedMembers();
      } else {
        return Collections.emptySet();
      }
    }

    public Set getRegionDestroyedMembers(String regionFullPath) {
      if (this.exception != null) {
        DistTxCommitExceptionCollectingException cce =
            (DistTxCommitExceptionCollectingException) this.exception;
        return cce.getRegionDestroyedMembers(regionFullPath);
      } else {
        return Collections.emptySet();
      }
    }

    public Map<DistributedMember, TXCommitMessage> getCommitResponseMap() {
      return commitResponseMap;
    }
  }

  /**
   * An Exception that collects many remote CommitExceptions
   *
   * @see TXCommitMessage.CommitExceptionCollectingException
   */
  public static class DistTxCommitExceptionCollectingException extends ReplyException {
    private static final long serialVersionUID = -2681117727592137893L;
    /** Set of members that threw CacheClosedExceptions */
    private final Set<InternalDistributedMember> cacheExceptions;
    /** key=region path, value=Set of members */
    private final Map<String, Set<InternalDistributedMember>> regionExceptions;
    /** List of exceptions that were unexpected and caused the tx to fail */
    private final Map fatalExceptions;

    private final TXId id;

    /*
     * [DISTTX] TODO Actually handle exceptions like commit conflict, primary bucket moved, etc
     */
    public DistTxCommitExceptionCollectingException(TXId txIdent) {
      this.cacheExceptions = new HashSet<InternalDistributedMember>();
      this.regionExceptions = new HashMap<String, Set<InternalDistributedMember>>();
      this.fatalExceptions = new HashMap();
      this.id = txIdent;
    }

    /**
     * Determine if the commit processing was incomplete, if so throw a detailed exception
     * indicating the source of the problem
     */
    public void handlePotentialCommitFailure(
        HashMap<DistributedMember, DistTXCoordinatorInterface> msgMap) {
      if (fatalExceptions.size() > 0) {
        StringBuilder errorMessage = new StringBuilder("Incomplete commit of transaction ")
            .append(id).append(".  Caused by the following exceptions: ");
        for (Iterator i = fatalExceptions.entrySet().iterator(); i.hasNext();) {
          Map.Entry me = (Map.Entry) i.next();
          DistributedMember mem = (DistributedMember) me.getKey();
          errorMessage.append(" From member: ").append(mem).append(" ");
          List exceptions = (List) me.getValue();
          for (Iterator ei = exceptions.iterator(); ei.hasNext();) {
            Exception e = (Exception) ei.next();
            errorMessage.append(e);
            for (StackTraceElement ste : e.getStackTrace()) {
              errorMessage.append("\n\tat ").append(ste);
            }
            if (ei.hasNext()) {
              errorMessage.append("\nAND\n");
            }
          }
          errorMessage.append(".");
        }
        throw new CommitIncompleteException(errorMessage.toString());
      }

      /* [DISTTX] TODO Not Sure if required */
      // Mark any persistent members as offline
      // handleClosedMembers(msgMap);
      // handleRegionDestroyed(msgMap);
    }

    public Set<InternalDistributedMember> getCacheClosedMembers() {
      return this.cacheExceptions;
    }

    public Set getRegionDestroyedMembers(String regionFullPath) {
      Set members = (Set) this.regionExceptions.get(regionFullPath);
      if (members == null) {
        members = Collections.emptySet();
      }
      return members;
    }

    /**
     * Protected by (this)
     */
    public void addExceptionsFromMember(InternalDistributedMember member, Set exceptions) {
      for (Iterator iter = exceptions.iterator(); iter.hasNext();) {
        Exception ex = (Exception) iter.next();
        if (ex instanceof CancelException) {
          cacheExceptions.add(member);
        } else if (ex instanceof RegionDestroyedException) {
          String r = ((RegionDestroyedException) ex).getRegionFullPath();
          Set<InternalDistributedMember> members = regionExceptions.get(r);
          if (members == null) {
            members = new HashSet<InternalDistributedMember>();
            regionExceptions.put(r, members);
          }
          members.add(member);
        } else {
          List el = (List) this.fatalExceptions.get(member);
          if (el == null) {
            el = new ArrayList(2);
            this.fatalExceptions.put(member, el);
          }
          el.add(ex);
        }
      }
    }
  }
}
