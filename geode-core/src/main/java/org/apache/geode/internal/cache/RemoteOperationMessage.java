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
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.LowMemoryException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.partitioned.PutMessage;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;
import org.apache.geode.internal.logging.log4j.LogMarker;

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

  /** default exception to ensure a false-positive response is never returned */
  static final ForceReattemptException UNHANDLED_EXCEPTION =
      (ForceReattemptException) new ForceReattemptException(
          LocalizedStrings.PartitionMessage_UNKNOWN_EXCEPTION.toLocalizedString())
              .fillInStackTrace();

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
    Assert.assertTrue(recipient != null, "RemoteMesssage recipient can not be null");
    setRecipient(recipient);
    this.regionPath = regionPath;
    this.processorId = processor == null ? 0 : processor.getProcessorId();
    if (processor != null && isSevereAlertCompatible()) {
      processor.enableSevereAlertProcessing();
    }
    this.txUniqId = TXManagerImpl.getCurrentTXUniqueId();
    TXStateProxy txState = TXManagerImpl.getCurrentTXState();
    if (txState != null && txState.isMemberIdForwardingRequired()) {
      this.txMemberId = txState.getOriginatingMember();
    }
    setIfTransactionDistributed();
  }

  public RemoteOperationMessage(Set recipients, String regionPath, ReplyProcessor21 processor) {
    setRecipients(recipients);
    this.regionPath = regionPath;
    this.processorId = processor == null ? 0 : processor.getProcessorId();
    if (processor != null && isSevereAlertCompatible()) {
      processor.enableSevereAlertProcessing();
    }
    this.txUniqId = TXManagerImpl.getCurrentTXUniqueId();
    TXStateProxy txState = TXManagerImpl.getCurrentTXState();
    if (txState != null && txState.isMemberIdForwardingRequired()) {
      this.txMemberId = txState.getOriginatingMember();
    }
    setIfTransactionDistributed();
  }

  /**
   * Copy constructor that initializes the fields declared in this class
   */
  public RemoteOperationMessage(RemoteOperationMessage other) {
    this.regionPath = other.regionPath;
    this.processorId = other.processorId;
    this.txUniqId = other.getTXUniqId();
    this.txMemberId = other.getTXMemberId();
    this.isTransactionDistributed = other.isTransactionDistributed;
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
    return ClusterDistributionManager.SERIAL_EXECUTOR;
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
    return this.processorId;
  }

  /**
   * check to see if the cache is closing
   */
  public boolean checkCacheClosing(ClusterDistributionManager dm) {
    InternalCache cache = dm.getCache();
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
    Throwable thr = null;
    boolean sendReply = true;
    LocalRegion r = null;
    long startTime = 0;
    try {
      InternalCache cache = getCache(dm);
      if (checkCacheClosing(dm) || checkDSClosing(dm)) {
        String message = "Remote cache is closed: " + dm.getId();
        if (cache == null) {
          thr = new CacheClosedException(message);
        } else {
          thr = cache.getCacheClosedException(message);
        }
        return;
      }
      r = getRegionByPath(cache);
      if (r == null && failIfRegionMissing()) {
        thr = new RegionDestroyedException(
            LocalizedStrings.RemoteOperationMessage_0_COULD_NOT_FIND_REGION_1
                .toLocalizedString(dm.getDistributionManagerId(), regionPath),
            regionPath);
        return; // reply sent in finally block below
      }

      thr = UNHANDLED_EXCEPTION;

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
            tx.updateProxyServer(this.getSender());
          }
        } finally {
          txMgr.unmasquerade(tx);
        }
      }
      thr = null;

    } catch (RemoteOperationException fre) {
      thr = fre;
    } catch (DistributedSystemDisconnectedException se) {
      // bug 37026: this is too noisy...
      // throw new CacheClosedException("remote system shutting down");
      // thr = se; cache is closed, no point trying to send a reply
      thr = null;
      sendReply = false;
      if (logger.isDebugEnabled()) {
        logger.debug("shutdown caught, abandoning message: {}", se.getMessage(), se);
      }
    } catch (RegionDestroyedException rde) {
      // [bruce] RDE does not always mean that the sender's region is also
      // destroyed, so we must send back an exception. If the sender's
      // region is also destroyed, who cares if we send it an exception
      // if (pr != null && pr.isClosed) {
      thr = new ForceReattemptException(LocalizedStrings.PartitionMessage_REGION_IS_DESTROYED_IN_0
          .toLocalizedString(dm.getDistributionManagerId()), rde);
      // }
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
      checkForSystemFailure();
      thr = null;
      if (sendReply) {
        if (!checkDSClosing(dm)) {
          thr = t;
        } else {
          // don't pass arbitrary runtime exceptions and errors back if this
          // cache/vm is closing
          thr = new ForceReattemptException(
              LocalizedStrings.PartitionMessage_DISTRIBUTED_SYSTEM_IS_DISCONNECTING
                  .toLocalizedString());
        }
      }
      if (logger.isTraceEnabled(LogMarker.DM) && (t instanceof RuntimeException)) {
        logger.trace(LogMarker.DM, "Exception caught while processing message", t);
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
        sendReply(getSender(), this.processorId, dm, rex, r, startTime);
      }
    }
  }

  protected void checkForSystemFailure() {
    SystemFailure.checkFailure();
  }

  TXManagerImpl getTXManager(InternalCache cache) {
    return cache.getTxManager();
  }

  LocalRegion getRegionByPath(InternalCache internalCache) {
    return internalCache.getRegionByPathForProcessing(this.regionPath);
  }

  InternalCache getCache(final ClusterDistributionManager dm) {
    return dm.getExistingCache();
  }

  /**
   * Send a generic ReplyMessage. This is in a method so that subclasses can override the reply
   * message type
   */
  protected void sendReply(InternalDistributedMember member, int procId, DistributionManager dm,
      ReplyException ex, LocalRegion r, long startTime) {
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


  /**
   * return a new reply processor for this class, for use in relaying a response. This <b>must</b>
   * be an instance method so subclasses can override it properly.
   */
  RemoteOperationResponse createReplyProcessor(PartitionedRegion r, Set recipients) {
    return new RemoteOperationResponse(r.getSystem(), recipients);
  }


  protected abstract boolean operateOnRegion(ClusterDistributionManager dm, LocalRegion r,
      long startTime) throws RemoteOperationException;

  /**
   * Fill out this instance of the message using the <code>DataInput</code> Required to be a
   * {@link org.apache.geode.DataSerializable}Note: must be symmetric with
   * {@link #toData(DataOutput)}in what it reads
   */
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.flags = in.readShort();
    setFlags(this.flags, in);
    this.regionPath = DataSerializer.readString(in);
    this.isTransactionDistributed = in.readBoolean();
  }

  public InternalDistributedMember getTXOriginatorClient() {
    return this.txMemberId;
  }

  /**
   * Send the contents of this instance to the DataOutput Required to be a
   * {@link org.apache.geode.DataSerializable}Note: must be symmetric with
   * {@link #fromData(DataInput)}in what it writes
   */
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    short flags = computeCompressedShort();
    out.writeShort(flags);
    if (this.processorId != 0) {
      out.writeInt(this.processorId);
    }
    if (this.processorType != 0) {
      out.writeByte(this.processorType);
    }
    if (this.getTXUniqId() != TXManagerImpl.NOTX) {
      out.writeInt(this.getTXUniqId());
    }
    if (this.getTXMemberId() != null) {
      DataSerializer.writeObject(this.getTXMemberId(), out);
    }
    DataSerializer.writeString(this.regionPath, out);
    out.writeBoolean(this.isTransactionDistributed);
  }

  protected short computeCompressedShort() {
    short flags = 0;
    if (this.processorId != 0)
      flags |= HAS_PROCESSOR_ID;
    if (this.processorType != 0)
      flags |= HAS_PROCESSOR_TYPE;
    if (this.getTXUniqId() != TXManagerImpl.NOTX)
      flags |= HAS_TX_ID;
    if (this.getTXMemberId() != null)
      flags |= HAS_TX_MEMBERID;
    return flags;
  }

  protected void setFlags(short flags, DataInput in) throws IOException, ClassNotFoundException {
    if ((flags & HAS_PROCESSOR_ID) != 0) {
      this.processorId = in.readInt();
      ReplyProcessor21.setMessageRPId(this.processorId);
    }
    if ((flags & HAS_PROCESSOR_TYPE) != 0) {
      this.processorType = in.readByte();
    }
    if ((flags & HAS_TX_ID) != 0) {
      this.txUniqId = in.readInt();
    }
    if ((flags & HAS_TX_MEMBERID) != 0) {
      this.txMemberId = DataSerializer.readObject(in);
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
    buff.append(this.regionPath);
    appendFields(buff);
    buff.append(" ,distTx=");
    buff.append(this.isTransactionDistributed);
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
    InternalDistributedMember[] recips = getRecipients();
    for (int i = 0; i < recips.length - 1; i++) {
      buff.append(recips[i]).append(',');
    }
    if (recips.length > 0) {
      buff.append(recips[recips.length - 1]);
    }
    buff.append("]; processorId=").append(this.processorId);
  }

  public InternalDistributedMember getRecipient() {
    return getRecipients()[0];
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
  public int getTXUniqId() {
    return txUniqId;
  }

  public InternalDistributedMember getMemberToMasqueradeAs() {
    if (txMemberId == null) {
      return getSender();
    }
    return txMemberId;
  }

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
   * @see #waitForCacheException()
   */
  public static class RemoteOperationResponse extends DirectReplyProcessor {
    /**
     * The exception thrown when the recipient does not reply
     */
    volatile ForceReattemptException prce;

    /**
     * Whether a response has been received
     */
    volatile boolean responseReceived;

    /**
     * whether a response is required
     */
    boolean responseRequired;

    public RemoteOperationResponse(InternalDistributedSystem dm, Collection initMembers) {
      this(dm, initMembers, true);
    }

    public RemoteOperationResponse(InternalDistributedSystem dm, Collection initMembers,
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
      this.responseRequired = true;
    }

    @Override
    public void memberDeparted(final InternalDistributedMember id, final boolean crashed) {
      if (id != null) {
        if (removeMember(id, true)) {
          this.prce = new ForceReattemptException(
              LocalizedStrings.PartitionMessage_PARTITIONRESPONSE_GOT_MEMBERDEPARTED_EVENT_FOR_0_CRASHED_1
                  .toLocalizedString(id, crashed));
        }
        checkIfDone();
      } else {
        Exception e = new Exception(
            LocalizedStrings.PartitionMessage_MEMBERDEPARTED_GOT_NULL_MEMBERID.toLocalizedString());
        logger.info(LocalizedMessage.create(
            LocalizedStrings.PartitionMessage_MEMBERDEPARTED_GOT_NULL_MEMBERID_CRASHED_0, crashed),
            e);
      }
    }

    /**
     * Waits for the response from the {@link RemoteOperationMessage}'s recipient
     *
     * @throws CacheException if the recipient threw a cache exception during message processing
     */
    public void waitForCacheException()
        throws CacheException, RemoteOperationException, PrimaryBucketException {
      try {
        waitForRepliesUninterruptibly();
        if (this.prce != null || (this.responseRequired && !this.responseReceived)) {
          throw new RemoteOperationException(
              LocalizedStrings.PartitionMessage_ATTEMPT_FAILED.toLocalizedString(), this.prce);
        }
      } catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof CacheException) {
          throw (CacheException) t;
        } else if (t instanceof RemoteOperationException) {
          RemoteOperationException ft = (RemoteOperationException) t;
          // See FetchEntriesMessage, which can marshal a ForceReattempt
          // across to the sender
          RemoteOperationException fre = new RemoteOperationException(
              LocalizedStrings.PartitionMessage_PEER_REQUESTS_REATTEMPT.toLocalizedString(), t);
          if (ft.hasHash()) {
            fre.setHash(ft.getHash());
          }
          throw fre;
        } else if (t instanceof PrimaryBucketException) {
          // See FetchEntryMessage, GetMessage, InvalidateMessage,
          // PutMessage
          // which can marshal a ForceReattemptacross to the sender
          throw new PrimaryBucketException(
              LocalizedStrings.PartitionMessage_PEER_FAILED_PRIMARY_TEST.toLocalizedString(), t);
        } else if (t instanceof RegionDestroyedException) {
          throw (RegionDestroyedException) t;
        } else if (t instanceof CancelException) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                "RemoteOperationResponse got CacheClosedException from {}, throwing ForceReattemptException",
                e.getSender(), t);
          }
          throw new RemoteOperationException(
              LocalizedStrings.PartitionMessage_PARTITIONRESPONSE_GOT_REMOTE_CACHECLOSEDEXCEPTION
                  .toLocalizedString(),
              t);
        } else if (t instanceof LowMemoryException) {
          if (logger.isDebugEnabled()) {
            logger.debug("RemoteOperationResponse re-throwing remote LowMemoryException from {}",
                e.getSender(), t);
          }
          throw (LowMemoryException) t;
        }
        e.handleAsUnexpected();
      }
    }

    /* overridden from ReplyProcessor21 */
    @Override
    public void process(DistributionMessage msg) {
      this.responseReceived = true;
      super.process(msg);
    }
  }

  @Override
  public boolean isTransactionDistributed() {
    return this.isTransactionDistributed;
  }

  /*
   * For Distributed Tx
   */
  public void setTransactionDistributed(boolean isDistTx) {
    this.isTransactionDistributed = isDistTx;
  }

  /*
   * For Distributed Tx
   */
  private void setIfTransactionDistributed() {
    InternalCache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      if (cache.getTxManager() != null) {
        this.isTransactionDistributed = cache.getTxManager().isDistributed();
      }
    }
  }
}
