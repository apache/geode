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
import java.util.Collection;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.LowMemoryException;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.MessageWithReply;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.partitioned.PutMessage;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * The base PartitionedRegion message type upon which other messages should be
 * based.
 * 
 * @author gregp
 * @since 6.5
 */
public abstract class RemoteOperationMessage extends DistributionMessage implements 
    MessageWithReply, TransactionMessage
{
  private static final Logger logger = LogService.getLogger();
  
  /** default exception to ensure a false-positive response is never returned */
  static final ForceReattemptException UNHANDLED_EXCEPTION
     = (ForceReattemptException)new ForceReattemptException(LocalizedStrings.PartitionMessage_UNKNOWN_EXCEPTION.toLocalizedString()).fillInStackTrace();

  protected int processorId;
  
  /** the type of executor to use */
  protected int processorType;

  protected String regionPath;
  
  /** The unique transaction Id on the sending member, used to construct a TXId on the receiving side */
  private int txUniqId = TXManagerImpl.NOTX;
  private InternalDistributedMember txMemberId = null;

  protected transient short flags;
  
  /*TODO [DISTTX] Convert into flag*/
  protected boolean isTransactionDistributed = false;

  public RemoteOperationMessage() {
  }

 
  public RemoteOperationMessage(InternalDistributedMember recipient, String regionPath,
      ReplyProcessor21 processor) {
    Assert.assertTrue(recipient != null, "RemoteMesssage recipient can not be null");
    setRecipient(recipient);
    this.regionPath = regionPath;
    this.processorId = processor==null? 0 : processor.getProcessorId();
    if (processor != null && this.isSevereAlertCompatible()) {
      processor.enableSevereAlertProcessing();
    }
    this.txUniqId = TXManagerImpl.getCurrentTXUniqueId();
    TXStateProxy txState = TXManagerImpl.getCurrentTXState();
    if(txState!=null && txState.isMemberIdForwardingRequired()) {
      this.txMemberId = txState.getOriginatingMember();
    }
    setIfTransactionDistributed();
  }

  public RemoteOperationMessage(Set recipients, String regionPath, ReplyProcessor21 processor) {
    setRecipients(recipients);
    this.regionPath = regionPath;
    this.processorId = processor==null? 0 : processor.getProcessorId();
    if (processor != null && this.isSevereAlertCompatible()) {
      processor.enableSevereAlertProcessing();
    }
    this.txUniqId = TXManagerImpl.getCurrentTXUniqueId();
    TXStateProxy txState = TXManagerImpl.getCurrentTXState();
    if(txState!=null && txState.isMemberIdForwardingRequired()) {
      this.txMemberId = txState.getOriginatingMember();
    }
    setIfTransactionDistributed();
  }

  /**
   * Copy constructor that initializes the fields declared in this class
   * @param other
   */
  public RemoteOperationMessage(RemoteOperationMessage other) {
    this.regionPath = other.regionPath;
    this.processorId = other.processorId;
    this.txUniqId = other.getTXUniqId();
    this.txMemberId = other.getTXMemberId();
    this.isTransactionDistributed = other.isTransactionDistributed;
  }

  /**
   * Severe alert processing enables suspect processing at the ack-wait-threshold
   * and issuing of a severe alert at the end of the ack-severe-alert-threshold.
   * Some messages should not support this type of processing
   * (e.g., GII, or DLockRequests)
   * @return whether severe-alert processing may be performed on behalf
   * of this message
   */
  @Override
  public boolean isSevereAlertCompatible() {
    return true;
  }
  
  @Override
  public int getProcessorType() {
      return DistributionManager.PARTITIONED_REGION_EXECUTOR;
  }

  /**
   * @return the full path of the region
   */
  public final String getRegionPath()
  {
    return regionPath;
  }

  /**
   * @return the {@link ReplyProcessor21}id associated with the message, null
   *         if no acknowlegement is required.
   */
  @Override
  public final int getProcessorId()
  {
    return this.processorId;
  }

  /**
   * @param processorId1 the {@link 
   * com.gemstone.gemfire.distributed.internal.ReplyProcessor21} id associated 
   * with the message, null if no acknowlegement is required.
   */
  public final void registerProcessor(int processorId1)
  {
    this.processorId = processorId1;
  }
  
  public void setCacheOpRecipients(Collection cacheOpRecipients) {
    // TODO need to implement this for other remote ops
    assert this instanceof RemotePutMessage;
  }


  /**
   * check to see if the cache is closing
   */
  final public boolean checkCacheClosing(DistributionManager dm) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    // return (cache != null && cache.isClosed());
    return cache == null || cache.isClosed();
  }

  /**
   * check to see if the distributed system is closing
   * @return true if the distributed system is closing
   */
  final public boolean checkDSClosing(DistributionManager dm) {
    InternalDistributedSystem ds = dm.getSystem();
    return (ds == null || ds.isDisconnecting());
  }
  
  /**
   * Upon receipt of the message, both process the message and send an
   * acknowledgement, not necessarily in that order. Note: Any hang in this
   * message may cause a distributed deadlock for those threads waiting for an
   * acknowledgement.
   * 
   * @throws PartitionedRegionException if the region does not exist (typically, if it has been destroyed)
   */
  @Override
  public void process(final DistributionManager dm) 
  {
    Throwable thr = null;
    boolean sendReply = true;
    LocalRegion r = null;
    long startTime = 0;    
    try {
      if (checkCacheClosing(dm) || checkDSClosing(dm)) {
        thr = new CacheClosedException(LocalizedStrings.PartitionMessage_REMOTE_CACHE_IS_CLOSED_0.toLocalizedString(dm.getId()));
        return;
      }
      GemFireCacheImpl gfc = (GemFireCacheImpl)CacheFactory.getInstance(dm.getSystem());
      r = gfc.getRegionByPathForProcessing(this.regionPath);
      if (r == null && failIfRegionMissing()) {
        // if the distributed system is disconnecting, don't send a reply saying
        // the partitioned region can't be found (bug 36585)
        thr = new RegionDestroyedException(LocalizedStrings.RemoteOperationMessage_0_COULD_NOT_FIND_REGION_1
                .toLocalizedString(new Object[] {dm.getDistributionManagerId(), regionPath }), regionPath);
        return;  // reply sent in finally block below
      }

      thr = UNHANDLED_EXCEPTION;
      
      // [bruce] r might be null here, so we have to go to the cache instance to get the txmgr
      TXManagerImpl txMgr = GemFireCacheImpl.getInstance().getTxManager();
      TXStateProxy tx = null;
      try {
        tx = txMgr.masqueradeAs(this);
        sendReply = operateOnRegion(dm, r, startTime);
      } finally {
        txMgr.unmasquerade(tx);
      }
      thr = null;
          
    } catch (RemoteOperationException fre) {
      thr = fre;
    }
    catch (DistributedSystemDisconnectedException se) {
      // bug 37026: this is too noisy...
//      throw new CacheClosedException("remote system shutting down");
//      thr = se; cache is closed, no point trying to send a reply
      thr = null;
      sendReply = false;
      if (logger.isDebugEnabled()) {
        logger.debug("shutdown caught, abandoning message: {}", se.getMessage(), se);
      }
    }
    catch (RegionDestroyedException rde) {
      // [bruce] RDE does not always mean that the sender's region is also
      //         destroyed, so we must send back an exception.  If the sender's
      //         region is also destroyed, who cares if we send it an exception
      //if (pr != null && pr.isClosed) {
        thr = new ForceReattemptException(LocalizedStrings.PartitionMessage_REGION_IS_DESTROYED_IN_0.toLocalizedString(dm.getDistributionManagerId()), rde);
      //}
    }
    catch (VirtualMachineError err) {
      SystemFailure.initiateFailure(err);
      // If this ever returns, rethrow the error.  We're poisoned
      // now, so don't let this thread continue.
      throw err;
    }
    catch (Throwable t) {
      // Whenever you catch Error or Throwable, you must also
      // catch VirtualMachineError (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      // log the exception at fine level if there is no reply to the message
      thr = null;
      if (sendReply) {
        if (!checkDSClosing(dm)) {
          thr = t;
        }
        else {
          // don't pass arbitrary runtime exceptions and errors back if this
          // cache/vm is closing
          thr = new ForceReattemptException(LocalizedStrings.PartitionMessage_DISTRIBUTED_SYSTEM_IS_DISCONNECTING.toLocalizedString());
        }
      }
      if (logger.isTraceEnabled(LogMarker.DM) && (t instanceof RuntimeException)) {
        logger.trace(LogMarker.DM, "Exception caught while processing message", t);
      }
    }
    finally {
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
  
  /** Send a generic ReplyMessage.  This is in a method so that subclasses can override the reply message type
   * @param pr the Partitioned Region for the message whose statistics are incremented
   * @param startTime the start time of the operation in nanoseconds
   *  @see PutMessage#sendReply
   */
  protected void sendReply(InternalDistributedMember member, int procId, DM dm, ReplyException ex, LocalRegion pr, long startTime) {
//    if (pr != null && startTime > 0) {
      //pr.getPrStats().endRemoteOperationMessagesProcessing(startTime); 
//    }

    ReplyMessage.send(member, procId, ex, getReplySender(dm), pr != null && pr.isInternalRegion());
  }
  
  /**
   * Allow classes that over-ride to choose whether 
   * a RegionDestroyException is thrown if no partitioned region is found (typically occurs if the message will be sent 
   * before the PartitionedRegion has been fully constructed.
   * @return true if throwing a {@link RegionDestroyedException} is acceptable
   */
  protected boolean failIfRegionMissing() {
    return true;
  }

  
  /**
   * return a new reply processor for this class, for use in relaying a response.
   * This <b>must</b> be an instance method so subclasses can override it
   * properly.
   */
  RemoteOperationResponse createReplyProcessor(PartitionedRegion r, Set recipients) {
    return new RemoteOperationResponse(r.getSystem(), recipients);
  }
  

  protected abstract boolean operateOnRegion(DistributionManager dm,
      LocalRegion r,long startTime) throws RemoteOperationException;

  /**
   * Fill out this instance of the message using the <code>DataInput</code>
   * Required to be a {@link com.gemstone.gemfire.DataSerializable}Note: must
   * be symmetric with {@link #toData(DataOutput)}in what it reads
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
   * {@link com.gemstone.gemfire.DataSerializable}Note: must be symmetric with
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
      DataSerializer.writeObject(this.getTXMemberId(),out);
    }
    DataSerializer.writeString(this.regionPath,out);
    out.writeBoolean(this.isTransactionDistributed);
  }

  protected short computeCompressedShort() {
    short flags = 0;
    if (this.processorId != 0) flags |= HAS_PROCESSOR_ID;
    if (this.processorType != 0) flags |= HAS_PROCESSOR_TYPE;
    if (this.getTXUniqId() != TXManagerImpl.NOTX) flags |= HAS_TX_ID;
    if (this.getTXMemberId() != null) flags |= HAS_TX_MEMBERID;
    return flags;
  }

  protected void setFlags(short flags, DataInput in) throws IOException,
      ClassNotFoundException {
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

  protected final InternalDistributedMember getTXMemberId() {
    return txMemberId;
  }

  private final static String PN_TOKEN = ".cache."; 
  @Override
  public String toString()
  {
    StringBuffer buff = new StringBuffer();
    String className = getClass().getName();
//    className.substring(className.lastIndexOf('.', className.lastIndexOf('.') - 1) + 1);  // partition.<foo> more generic version 
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
   * @param buff
   *          buffer in which to append the state of this instance
   */
  protected void appendFields(StringBuffer buff)
  {
    buff.append("; sender=").append(getSender())
      .append("; recipients=[");
    InternalDistributedMember[] recips = getRecipients();
    for(int i=0; i<recips.length-1; i++) {
      buff.append(recips[i]).append(',');
    }
    if (recips.length > 0) {
      buff.append(recips[recips.length-1]);
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
   * @param value true or false
   * @since 6.5
   */
  public void setHasOldValue(boolean value) {
    // override in subclasses which need old value to be serialized.
    // overridden by classes like PutMessage, DestroyMessage.
  }
  
  /**
   * @return the txUniqId
   */
  public final int getTXUniqId() {
    return txUniqId;
  }

  
  public final InternalDistributedMember getMemberToMasqueradeAs() {
    if(txMemberId==null) {
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
   * A processor on which to await a response from the {@link RemoteOperationMessage}
   * recipient, capturing any CacheException thrown by the recipient and handle
   * it as an expected exception.
   * 
   * @author Greg Passmore
   * @since 6.5
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
    
    public RemoteOperationResponse(InternalDistributedSystem dm, Collection initMembers, boolean register) {
      super(dm, initMembers);
      if(register) {
        register();
      }
    }
    
    public RemoteOperationResponse(InternalDistributedSystem dm, InternalDistributedMember member) {
      this(dm, member, true);
    }
    
    public RemoteOperationResponse(InternalDistributedSystem dm, InternalDistributedMember member, boolean register) {
      super(dm, member);
      if(register) {
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
          this.prce =  new ForceReattemptException(LocalizedStrings.PartitionMessage_PARTITIONRESPONSE_GOT_MEMBERDEPARTED_EVENT_FOR_0_CRASHED_1.toLocalizedString(new Object[] {id, Boolean.valueOf(crashed)}));
        }
        checkIfDone();
      } else {
        Exception e = new Exception(LocalizedStrings.PartitionMessage_MEMBERDEPARTED_GOT_NULL_MEMBERID.toLocalizedString());
        logger.info(LocalizedMessage.create(LocalizedStrings.PartitionMessage_MEMBERDEPARTED_GOT_NULL_MEMBERID_CRASHED_0, Boolean.valueOf(crashed)), e);
      }
    }

    /**
     * Waits for the response from the {@link RemoteOperationMessage}'s recipient
     * @throws CacheException  if the recipient threw a cache exception during message processing 
     * @throws PrimaryBucketException 
     */
    final public void waitForCacheException() 
        throws CacheException, RemoteOperationException, PrimaryBucketException {
      try {
        waitForRepliesUninterruptibly();
        if (this.prce!=null || (this.responseRequired && !this.responseReceived)) {
          throw new RemoteOperationException(LocalizedStrings.PartitionMessage_ATTEMPT_FAILED.toLocalizedString(), this.prce);
        }
      }
      catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof CacheException) {
          throw (CacheException)t;
        }
        else if (t instanceof RemoteOperationException) {
          RemoteOperationException ft = (RemoteOperationException)t;
          // See FetchEntriesMessage, which can marshal a ForceReattempt
          // across to the sender
          RemoteOperationException fre = new RemoteOperationException(LocalizedStrings.PartitionMessage_PEER_REQUESTS_REATTEMPT.toLocalizedString(), t);
          if (ft.hasHash()) {
            fre.setHash(ft.getHash());
          }
          throw fre;
        }
        else if (t instanceof PrimaryBucketException) {
          // See FetchEntryMessage, GetMessage, InvalidateMessage,
          // PutMessage
          // which can marshal a ForceReattemptacross to the sender
          throw new PrimaryBucketException(LocalizedStrings.PartitionMessage_PEER_FAILED_PRIMARY_TEST.toLocalizedString(), t);
        }
        else if (t instanceof RegionDestroyedException) {
          RegionDestroyedException rde = (RegionDestroyedException) t;
          throw rde;
        }
        else if (t instanceof CancelException) {
          if (logger.isDebugEnabled()) {
            logger.debug("RemoteOperationResponse got CacheClosedException from {}, throwing ForceReattemptException", e.getSender(), t);
          }
          throw new RemoteOperationException(LocalizedStrings.PartitionMessage_PARTITIONRESPONSE_GOT_REMOTE_CACHECLOSEDEXCEPTION.toLocalizedString(), t);
        }
        else if (t instanceof LowMemoryException) {
          if (logger.isDebugEnabled()) {
            logger.debug("RemoteOperationResponse re-throwing remote LowMemoryException from {}", e.getSender(), t);
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
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      if (cache.getTxManager() != null) {
        this.isTransactionDistributed = cache.getTxManager().isDistributed();
      }
    }
  }
}
