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
package org.apache.geode.internal.cache.tx;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TransactionException;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.TXStateProxy;
import org.apache.geode.internal.cache.TransactionMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * The base message type upon which other messages that need to be sent to a remote member that is
 * hosting a transaction should be based. Note that, currently, some of these message are not used
 * by transactions. This is a misuse of these messages and needs to be corrected.
 *
 * @since GemFire 6.5
 */
public abstract class RemoteOperationMessage extends DistributionMessage
    implements MessageWithReply, TransactionMessage {
  private static final Logger logger = LogService.getLogger();

  protected int processorId;

  /** the type of executor to use */
  protected int processorType;

  protected String regionPath;

  /**
   * The unique transaction Id on the sending member, used to construct a TXId on the receiving side
   */
  private int txUniqId = TXManagerImpl.NOTX;

  private InternalDistributedMember txMemberId = null;

  protected transient short flags;

  /* TODO [DISTTX] Convert into flag */
  protected boolean isTransactionDistributed = false;

  public RemoteOperationMessage() {
    // do nothing
  }

  public RemoteOperationMessage(InternalDistributedMember recipient, String regionPath,
      ReplyProcessor21 processor) {
    this(regionPath, processor);
    Assert.assertTrue(recipient != null, "RemoteMesssage recipient can not be null");
    setRecipient(recipient);
  }

  private RemoteOperationMessage(String regionPath, ReplyProcessor21 processor) {
    this.regionPath = regionPath;
    processorId = processor == null ? 0 : processor.getProcessorId();
    if (processor != null && isSevereAlertCompatible()) {
      processor.enableSevereAlertProcessing();
    }
    txUniqId = TXManagerImpl.getCurrentTXUniqueId();
    TXStateProxy txState = TXManagerImpl.getCurrentTXState();
    if (txState != null && txState.isMemberIdForwardingRequired()) {
      txMemberId = txState.getOriginatingMember();
    }
    setIfTransactionDistributed(processor);
  }

  /**
   * Severe alert processing enables suspect processing at the ack-wait-threshold and issuing of a
   * severe alert at the end of the ack-severe-alert-threshold. Some messages should not support
   * this type of processing (e.g., GII, or DLockRequests)
   *
   * @return whether severe-alert processing may be performed on behalf of this message
   */
  @Override
  public boolean isSevereAlertCompatible() {
    return true;
  }

  @Override
  public int getProcessorType() {
    return OperationExecutors.SERIAL_EXECUTOR;
  }

  /**
   * @return the full path of the region
   */
  public String getRegionPath() {
    return regionPath;
  }

  /**
   * @return the {@link ReplyProcessor21}id associated with the message, null if no acknowlegement
   *         is required.
   */
  @Override
  public int getProcessorId() {
    return processorId;
  }

  /**
   * check to see if the cache is closing
   */
  public boolean checkCacheClosing(InternalCache cache) {
    return cache == null || cache.isClosed();
  }

  /**
   * check to see if the distributed system is closing
   *
   * @return true if the distributed system is closing
   */
  public boolean checkDSClosing(ClusterDistributionManager dm) {
    InternalDistributedSystem ds = dm.getSystem();
    return (ds == null || ds.isDisconnecting());
  }

  /**
   * Upon receipt of the message, both process the message and send an acknowledgement, not
   * necessarily in that order. Note: Any hang in this message may cause a distributed deadlock for
   * those threads waiting for an acknowledgement.
   *
   * @throws RegionDestroyedException if the region does not exist
   */
  @Override
  public void process(final ClusterDistributionManager dm) {
    InternalCache cache = getCache(dm);
    if (cache == null) {
      String message = getCacheClosedMessage(dm);
      ReplyException replyException = new ReplyException(new CacheClosedException(message));
      sendReply(getSender(), processorId, dm, replyException, null, 0);
      return;
    }

    if (dm.getSystem().threadOwnsResources()) {
      // reply inline if thread owns socket.
      doRemoteOperation(dm, cache);
      return;
    }

    if (isTransactional()) {
      dm.getExecutors().getWaitingThreadPool().execute(() -> doRemoteOperation(dm, cache));
    } else {
      // reply inline for non-transactional case.
      doRemoteOperation(dm, cache);
    }
  }

  boolean isTransactional() {
    return getTXUniqId() != TXManagerImpl.NOTX && canParticipateInTransaction();
  }

  void doRemoteOperation(ClusterDistributionManager dm, InternalCache cache) {
    Throwable thr = null;
    boolean sendReply = true;
    LocalRegion r = null;
    long startTime = 0;
    try {
      if (checkCacheClosing(cache) || checkDSClosing(dm)) {
        thr = cache.getCacheClosedException(getCacheClosedMessage(dm));
        return;
      }
      r = getRegionByPath(cache);
      if (r == null && failIfRegionMissing()) {
        thr = new RegionDestroyedException(
            String.format("%s : could not find region %s",
                dm.getDistributionManagerId(), regionPath),
            regionPath);
        return; // reply sent in finally block below
      }

      // [bruce] r might be null here, so we have to go to the cache instance to get the txmgr
      TXManagerImpl txMgr = getTXManager(cache);
      TXStateProxy tx = txMgr.masqueradeAs(this);
      if (tx == null) {
        sendReply = operateOnRegion(dm, r, startTime);
      } else {
        try {
          if (txMgr.isClosed()) {
            // NO DISTRIBUTED MESSAGING CAN BE DONE HERE!
            sendReply = false;
          } else if (tx.isInProgress()) {
            sendReply = operateOnRegion(dm, r, startTime);
            tx.updateProxyServer(getSender());
          } else {
            /*
             * This can occur when processing an in-flight message after the transaction has
             * been failed over and committed.
             */
            throw new TransactionException("transactional operation elided because transaction {"
                + tx.getTxId() + "} is closed");
          }
        } finally {
          txMgr.unmasquerade(tx);
        }
      }
    } catch (RegionDestroyedException | RemoteOperationException | TransactionException ex) {
      thr = ex;
    } catch (DistributedSystemDisconnectedException se) {
      // bug 37026: this is too noisy...
      // throw new CacheClosedException("remote system shutting down");
      // thr = se; cache is closed, no point trying to send a reply
      thr = null;
      sendReply = false;
      if (logger.isDebugEnabled()) {
        logger.debug("shutdown caught, abandoning message: {}", se.getMessage(), se);
      }
    } catch (VirtualMachineError err) {
      thr = new RemoteOperationException("VirtualMachineError", err);
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
      if (sendReply) {
        thr = new RemoteOperationException("system failure", SystemFailure.getFailure());
      }
      checkForSystemFailure();
      if (sendReply) {
        if (!checkDSClosing(dm)) {
          thr = t;
        } else {
          // don't pass arbitrary runtime exceptions and errors back if this
          // cache/vm is closing
          thr = new RemoteOperationException("cache is closing", new CacheClosedException());
        }
      }
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE) && (t instanceof RuntimeException)) {
        logger.trace(LogMarker.DM_VERBOSE, "Exception caught while processing message", t);
      }
    } finally {
      if (sendReply) {
        ReplyException rex = null;

        if (thr != null) {
          // don't transmit the exception if this message was to a listener
          // and this listener is shutting down
          rex = new ReplyException(thr);
        }

        // Send the reply if the operateOnPartitionedRegion returned true
        sendReply(getSender(), processorId, dm, rex, r, startTime);
      }
    }
  }

  @NotNull
  private String getCacheClosedMessage(ClusterDistributionManager dm) {
    return "Remote cache is closed: " + dm.getId();
  }

  protected void checkForSystemFailure() {
    SystemFailure.checkFailure();
  }

  TXManagerImpl getTXManager(InternalCache cache) {
    return cache.getTxManager();
  }

  LocalRegion getRegionByPath(InternalCache internalCache) {
    return (LocalRegion) internalCache.getRegionByPathForProcessing(getRegionPath());
  }

  InternalCache getCache(final ClusterDistributionManager dm) {
    return dm.getExistingCache();
  }

  /**
   * Send a generic ReplyMessage. This is in a method so that subclasses can override the reply
   * message type
   */
  protected void sendReply(InternalDistributedMember member, int procId, DistributionManager dm,
      ReplyException ex, InternalRegion r, long startTime) {
    ReplyMessage.send(member, procId, ex, getReplySender(dm), r != null && r.isInternalRegion());
  }

  /**
   * Allow classes that over-ride to choose whether a RegionDestroyException is thrown if no
   * partitioned region is found (typically occurs if the message will be sent before the
   * PartitionedRegion has been fully constructed.
   *
   * @return true if throwing a {@link RegionDestroyedException} is acceptable
   */
  protected boolean failIfRegionMissing() {
    return true;
  }

  protected abstract boolean operateOnRegion(ClusterDistributionManager dm, LocalRegion r,
      long startTime) throws RemoteOperationException;

  /**
   * Fill out this instance of the message using the <code>DataInput</code> Required to be a
   * {@link org.apache.geode.DataSerializable}Note: must be symmetric with
   * {@link DataSerializableFixedID#toData(DataOutput, SerializationContext)}in what it reads
   */
  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    flags = in.readShort();
    setFlags(flags, in, context);
    regionPath = DataSerializer.readString(in);
    isTransactionDistributed = in.readBoolean();
  }

  @Override
  public InternalDistributedMember getTXOriginatorClient() {
    return txMemberId;
  }

  /**
   * Send the contents of this instance to the DataOutput Required to be a
   * {@link org.apache.geode.DataSerializable}Note: must be symmetric with
   * {@link DataSerializableFixedID#fromData(DataInput, DeserializationContext)}in what it writes
   */
  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    short flags = computeCompressedShort();
    out.writeShort(flags);
    if (processorId != 0) {
      out.writeInt(processorId);
    }
    if (processorType != 0) {
      out.writeByte(processorType);
    }
    if (getTXUniqId() != TXManagerImpl.NOTX) {
      out.writeInt(getTXUniqId());
    }
    if (getTXMemberId() != null) {
      context.getSerializer().writeObject(getTXMemberId(), out);
    }
    DataSerializer.writeString(regionPath, out);
    out.writeBoolean(isTransactionDistributed);
  }

  protected short computeCompressedShort() {
    short flags = 0;
    if (processorId != 0) {
      flags |= HAS_PROCESSOR_ID;
    }
    if (processorType != 0) {
      flags |= HAS_PROCESSOR_TYPE;
    }
    if (getTXUniqId() != TXManagerImpl.NOTX) {
      flags |= HAS_TX_ID;
    }
    if (getTXMemberId() != null) {
      flags |= HAS_TX_MEMBERID;
    }
    return flags;
  }

  protected void setFlags(short flags, DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    if ((flags & HAS_PROCESSOR_ID) != 0) {
      processorId = in.readInt();
      ReplyProcessor21.setMessageRPId(processorId);
    }
    if ((flags & HAS_PROCESSOR_TYPE) != 0) {
      processorType = in.readByte();
    }
    if ((flags & HAS_TX_ID) != 0) {
      txUniqId = in.readInt();
    }
    if ((flags & HAS_TX_MEMBERID) != 0) {
      txMemberId = context.getDeserializer().readObject(in);
    }
  }

  protected InternalDistributedMember getTXMemberId() {
    return txMemberId;
  }

  private static final String PN_TOKEN = ".cache.";

  @Override
  public String toString() {
    StringBuffer buff = new StringBuffer();
    String className = getClass().getName();
    // className.substring(className.lastIndexOf('.', className.lastIndexOf('.') - 1) + 1); //
    // partition.<foo> more generic version
    buff.append(className.substring(className.indexOf(PN_TOKEN) + PN_TOKEN.length())); // partition.<foo>
    buff.append("(regionPath="); // make sure this is the first one
    buff.append(regionPath);
    appendFields(buff);
    buff.append(" ,distTx=");
    buff.append(isTransactionDistributed);
    buff.append(")");
    return buff.toString();
  }

  /**
   * Helper class of {@link #toString()}
   *
   * @param buff buffer in which to append the state of this instance
   */
  protected void appendFields(StringBuffer buff) {
    buff.append("; sender=").append(getSender()).append("; recipients=[");
    List<InternalDistributedMember> recipients = getRecipients();
    for (int i = 0; i < recipients.size() - 1; i++) {
      buff.append(recipients.get(i)).append(',');
    }
    if (recipients.size() > 0) {
      buff.append(recipients.get(recipients.size() - 1));
    }
    buff.append("]; processorId=").append(processorId);
  }

  public InternalDistributedMember getRecipient() {
    return getRecipients().get(0);
  }

  public void setOperation(Operation op) {
    // override in subclasses holding operations
  }

  /**
   * added to support old value to be written on wire.
   *
   * @param value true or false
   * @since GemFire 6.5
   */
  public void setHasOldValue(boolean value) {
    // override in subclasses which need old value to be serialized.
    // overridden by classes like PutMessage, DestroyMessage.
  }

  /**
   * @return the txUniqId
   */
  @Override
  public int getTXUniqId() {
    return txUniqId;
  }

  @Override
  public InternalDistributedMember getMemberToMasqueradeAs() {
    if (txMemberId == null) {
      return getSender();
    }
    return txMemberId;
  }

  @Override
  public boolean canStartRemoteTransaction() {
    return true;
  }

  @Override
  public boolean canParticipateInTransaction() {
    return true;
  }

  /**
   * A processor on which to await a response from the {@link RemoteOperationMessage} recipient,
   * capturing any CacheException thrown by the recipient and handle it as an expected exception.
   *
   * @since GemFire 6.5
   * @see #waitForRemoteResponse()
   */
  public static class RemoteOperationResponse extends DirectReplyProcessor {

    private volatile RemoteOperationException memberDepartedException;

    /**
     * Whether a response has been received
     */
    private volatile boolean responseReceived;

    /**
     * whether a response is required
     */
    private boolean responseRequired;

    public RemoteOperationResponse(InternalDistributedSystem dm, Collection<?> initMembers,
        boolean register) {
      super(dm, initMembers);
      if (register) {
        register();
      }
    }

    public RemoteOperationResponse(InternalDistributedSystem dm, InternalDistributedMember member) {
      this(dm, member, true);
    }

    public RemoteOperationResponse(InternalDistributedSystem dm, InternalDistributedMember member,
        boolean register) {
      super(dm, member);
      if (register) {
        register();
      }
    }

    /**
     * require a response message to be received
     */
    public void requireResponse() {
      responseRequired = true;
    }

    @Override
    public void memberDeparted(DistributionManager distributionManager,
        final InternalDistributedMember id, final boolean crashed) {
      if (id != null) {
        if (removeMember(id, true)) {
          memberDepartedException = new RemoteOperationException(
              "memberDeparted event for <" + id + "> crashed = " + crashed);
        }
        checkIfDone();
      } else {
        Exception e = new Exception("memberDeparted got null memberId");
        logger.info("memberDeparted got null memberId crashed=" + crashed, e);
      }
    }

    public RemoteOperationException getMemberDepartedException() {
      return memberDepartedException;
    }

    /**
     * Waits for the response from the {@link RemoteOperationMessage}'s recipient
     *
     * @throws RemoteOperationException if the member we waited for a response from departed
     * @throws RemoteOperationException if a response was required and not received
     * @throws RemoteOperationException if the ReplyException was caused by a
     *         RemoteOperationException
     * @throws RemoteOperationException if the remote side's cache was closed
     */
    public void waitForRemoteResponse() throws RemoteOperationException {
      try {
        waitForRepliesUninterruptibly();
        RemoteOperationException ex = getMemberDepartedException();
        if (ex != null) {
          throw ex;
        }
        if (responseRequired && !responseReceived) {
          throw new RemoteOperationException("response required but not received");
        }
      } catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof RemoteOperationException) {
          // no need to create a local RemoteOperationException to wrap the one from the reply
          throw (RemoteOperationException) t;
        } else if (t instanceof CancelException) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "RemoteOperationResponse got CacheClosedException from {}, throwing RemoteOperationException",
                e.getSender(), t);
          }
          throw new RemoteOperationException("remote cache was closed", t);
        }
        e.handleCause();
      }
    }

    /* overridden from ReplyProcessor21 */
    @Override
    public void process(DistributionMessage msg) {
      responseReceived = true;
      super.process(msg);
    }
  }

  @Override
  public boolean isTransactionDistributed() {
    return isTransactionDistributed;
  }

  /*
   * For Distributed Tx
   */
  private void setIfTransactionDistributed(ReplyProcessor21 processor) {
    if (processor != null) {
      DistributionManager distributionManager = processor.getDistributionManager();
      if (distributionManager != null) {
        InternalCache cache = distributionManager.getCache();
        if (cache != null && cache.getTxManager() != null) {
          isTransactionDistributed = cache.getTxManager().isDistributed();
        }
      }
    }
  }
}
