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
package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.cache.LowMemoryException;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
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
import com.gemstone.gemfire.internal.cache.DataLocationException;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.FilterRoutingInfo;
import com.gemstone.gemfire.internal.cache.ForceReattemptException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionException;
import com.gemstone.gemfire.internal.cache.PrimaryBucketException;
import com.gemstone.gemfire.internal.cache.TXManagerImpl;
import com.gemstone.gemfire.internal.cache.TXStateProxy;
import com.gemstone.gemfire.internal.cache.TransactionMessage;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.sequencelog.EntryLogger;

/**
 * The base PartitionedRegion message type upon which other messages should be
 * based.
 * 
 * @author mthomas
 * @author bruce
 * @since 5.0
 */
public abstract class PartitionMessage extends DistributionMessage implements 
    MessageWithReply, TransactionMessage
{
  private static final Logger logger = LogService.getLogger();
  
  /** default exception to ensure a false-positive response is never returned */
  static final ForceReattemptException UNHANDLED_EXCEPTION
     = (ForceReattemptException)new ForceReattemptException(LocalizedStrings.PartitionMessage_UNKNOWN_EXCEPTION.toLocalizedString()).fillInStackTrace();

  int regionId;

  int processorId;
  
  /**
   * whether this message is being sent for listener notification
   */
  boolean notificationOnly;

  protected short flags = 0;

  /* these bit masks are used for encoding the bits of a short on the wire 
   * instead of transmitting booleans. Any subclasses interested in saving
   * bits on the wire should add a mask here and then override
   *  computeCompressedShort and setBooleans
   * 
   */
  /** flag to indicate notification message */
  protected static final short NOTIFICATION_ONLY =
    DistributionMessage.UNRESERVED_FLAGS_START;
  /** flag to indicate ifNew in PutMessages */
  protected static final short IF_NEW = (NOTIFICATION_ONLY << 1);
  /** flag to indicate ifOld in PutMessages */
  protected static final short IF_OLD = (IF_NEW << 1);
  /** flag to indicate that oldValue is required for PutMessages and others */
  protected static final short REQUIRED_OLD_VAL = (IF_OLD << 1);
  /** flag to indicate filterInfo in message */
  protected static final short HAS_FILTER_INFO = (REQUIRED_OLD_VAL << 1);
  /** flag to indicate delta as value in message */
  protected static final short HAS_DELTA = (HAS_FILTER_INFO << 1);
  /** the unreserved flags start for child classes */
  protected static final short UNRESERVED_FLAGS_START = (HAS_DELTA << 1);

  private InternalDistributedMember txMemberId = null;

  /** The unique transaction Id on the sending member, used to construct a TXId on the receiving side */
  private int txUniqId = TXManagerImpl.NOTX;
  
  protected boolean sendDeltaWithFullValue = true;

  /*TODO [DISTTX] Convert into flag*/
  protected boolean isTransactionDistributed = false;

  public PartitionMessage() {
  }

 
  public PartitionMessage(InternalDistributedMember recipient, int regionId,
      ReplyProcessor21 processor) {
    Assert.assertTrue(recipient != null, "PartitionMesssage recipient can not be null");
    setRecipient(recipient);
    this.regionId = regionId;
    this.processorId = processor==null? 0 : processor.getProcessorId();
    if (processor != null && this.isSevereAlertCompatible()) {
      processor.enableSevereAlertProcessing();
    }
    initTxMemberId();
    setIfTransactionDistributed();
  }

  public PartitionMessage(Collection<InternalDistributedMember> recipients, int regionId, ReplyProcessor21 processor) {
    setRecipients(recipients);
    this.regionId = regionId;
    this.processorId = processor==null? 0 : processor.getProcessorId();
    if (processor != null && this.isSevereAlertCompatible()) {
      processor.enableSevereAlertProcessing();
    }
    initTxMemberId();
    setIfTransactionDistributed();
  }

  
  public void initTxMemberId() {
    this.txUniqId = TXManagerImpl.getCurrentTXUniqueId();
    TXStateProxy txState = TXManagerImpl.getCurrentTXState();
    if (txState != null) {
      // [DISTTX] Lets not throw this exception for Dist Tx
      if (canStartRemoteTransaction() && txState.isRealDealLocal() && !txState.isDistTx()) {
        //logger.error("sending rmt txId even though tx is local! txState=" + txState, new RuntimeException("STACK"));
        throw new IllegalStateException("Sending remote txId even though transaction is local. This should never happen: txState=" + txState);
      }
    }
    if(txState!=null && txState.isMemberIdForwardingRequired()) {
      this.txMemberId = txState.getOriginatingMember();
    }
  }
  /**
   * Copy constructor that initializes the fields declared in this class
   * @param other
   */
  public PartitionMessage(PartitionMessage other) {
    this.regionId = other.regionId;
    this.processorId = other.processorId;
    this.notificationOnly = other.notificationOnly;
    this.txUniqId = other.getTXUniqId();
    this.txMemberId = other.getTXOriginatorClient();
    this.isTransactionDistributed = other.isTransactionDistributed;
  }

  /*
   * (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TransactionMessage#getTXOriginatorClient()
   */
  public final InternalDistributedMember getTXOriginatorClient() {
    return txMemberId;
  }

  public final InternalDistributedMember getMemberToMasqueradeAs() {
    if(txMemberId==null) {
      return getSender();
    }
    return txMemberId;
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
    if (this.notificationOnly) {
      return DistributionManager.SERIAL_EXECUTOR;
    }
    else {
      return DistributionManager.PARTITIONED_REGION_EXECUTOR;
    }
  }

  /**
   * @return the compact value that will be sent which represents the
   *         PartitionedRegion
   * @see PartitionedRegion#getPRId()
   */
  public final int getRegionId()
  {
    return regionId;
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

  /**
   * @return return the message that should be sent to listeners, or null if this message
   * should not be relayed
   */
  public PartitionMessage getMessageForRelayToListeners(EntryEventImpl event, Set recipients) {
    return null;
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
    PartitionedRegion pr = null;
    long startTime = 0;
    EntryLogger.setSource(getSender(), "PR");
    try {
      if (checkCacheClosing(dm) || checkDSClosing(dm)) {
        thr = new CacheClosedException(LocalizedStrings.PartitionMessage_REMOTE_CACHE_IS_CLOSED_0.toLocalizedString(dm.getId()));
        return;
      }
      pr = PartitionedRegion.getPRFromId(this.regionId);
      if (pr == null && failIfRegionMissing()) {
        // if the distributed system is disconnecting, don't send a reply saying
        // the partitioned region can't be found (bug 36585)
        thr = new ForceReattemptException(LocalizedStrings.PartitionMessage_0_COULD_NOT_FIND_PARTITIONED_REGION_WITH_ID_1.toLocalizedString(new Object[] {dm.getDistributionManagerId(), Integer.valueOf(regionId)}));
        return;  // reply sent in finally block below
      }

      if (pr != null) {
        startTime = pr.getPrStats().startPartitionMessageProcessing();
      }
      thr = UNHANDLED_EXCEPTION;
      
      GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if(cache==null) {
        throw new ForceReattemptException(LocalizedStrings.PartitionMessage_REMOTE_CACHE_IS_CLOSED_0.toLocalizedString());
      }
      TXManagerImpl txMgr = cache.getTxManager();
      TXStateProxy tx = null;
      try {
        tx = txMgr.masqueradeAs(this);
        sendReply = operateOnPartitionedRegion(dm, pr, startTime);
      } finally {
        txMgr.unmasquerade(tx);
      }
      thr = null;
          
    } catch (ForceReattemptException fre) {
      thr = fre;
    } catch (DataLocationException fre) {
      thr = new ForceReattemptException(fre.getMessage(), fre);
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
    catch (RegionDestroyedException | RegionNotFoundException rde ) {
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
        logger.trace(LogMarker.DM, "Exception caught while processing message: ", t.getMessage(), t);
      }
    }
    finally {
      if (sendReply) {
        ReplyException rex = null;
        
        if (thr != null) {
          // don't transmit the exception if this message was to a listener
          // and this listener is shutting down
          boolean excludeException = 
            this.notificationOnly
                 && ((thr instanceof CancelException)
                      || (thr instanceof ForceReattemptException));
          
          if (!excludeException) {
            rex = new ReplyException(thr);
          }
        }

        // Send the reply if the operateOnPartitionedRegion returned true
        sendReply(getSender(), this.processorId, dm, rex, pr, startTime);
        EntryLogger.clearSource();
      } 
    }
  }
  
  /** Send a generic ReplyMessage.  This is in a method so that subclasses can override the reply message type
   * @param pr the Partitioned Region for the message whose statistics are incremented
   * @param startTime the start time of the operation in nanoseconds
   *  @see PutMessage#sendReply
   */
  protected void sendReply(InternalDistributedMember member, int procId, DM dm, ReplyException ex, PartitionedRegion pr, long startTime) {
    if (pr != null && startTime > 0) {
      pr.getPrStats().endPartitionMessagesProcessing(startTime); 
    }

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
   * relay this message to another set of recipients for event notification
   * @param cacheOpRecipients recipients of associated bucket CacheOperationMessage
   * @param adjunctRecipients recipients who unconditionally get the message
   * @param filterRoutingInfo routing information for all recipients
   * @param event the event causing this message
   * @param r the region being operated on
   * @param processor the reply processor to be notified
   */
  public Set relayToListeners(Set cacheOpRecipients, Set adjunctRecipients,
      FilterRoutingInfo filterRoutingInfo, 
      EntryEventImpl event, PartitionedRegion r, DirectReplyProcessor processor)
  {
    this.processorId = processor == null? 0 : processor.getProcessorId();
    this.notificationOnly = true;
        
    //Set sqlfAsyncListenerRecepients = r.getRegionAdvisor().adviseSqlfAsyncEventListenerHub();
    //sqlfAsyncListenerRecepients.retainAll(adjunctRecipients);
    //Now remove those adjunct recepients which are present in SqlfAsyncListenerRecepients
    //adjunctRecipients.removeAll(sqlfAsyncListenerRecepients);
    this.setFilterInfo(filterRoutingInfo);
    Set failures1= null;
    if(!adjunctRecipients.isEmpty()) {
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "Relaying partition message to other processes for listener notification");
      }
      resetRecipients();
      setRecipients(adjunctRecipients);
      failures1 = r.getDistributionManager().putOutgoing(this);
    }
    /*
    //Now distribute message with old value to Sqlf Hub nodes
    if(!sqlfAsyncListenerRecepients.isEmpty()) {
      //System.out.println("Asif1: sqlf hub  recepients ="+sqlfHubRecepients);
      resetRecipients();
      setRecipients(sqlfAsyncListenerRecepients);
      event.applyDelta(true);
      Set failures2 = r.getDistributionManager().putOutgoing(this);
      if(failures1 == null) {
        failures1 = failures2;
      }else if(failures2 != null) {
        failures1.addAll(failures2);
      }
    }*/
    
    return failures1;
  }
    
  /**
   * return a new reply processor for this class, for use in relaying a response.
   * This <b>must</b> be an instance method so subclasses can override it
   * properly.
   */
  PartitionResponse createReplyProcessor(PartitionedRegion r, Set recipients) {
    return new PartitionResponse(r.getSystem(), recipients);
  }
  

  protected boolean operateOnRegion(DistributionManager dm,
      PartitionedRegion pr) {
    throw new InternalGemFireError(LocalizedStrings.PartitionMessage_SORRY_USE_OPERATEONPARTITIONEDREGION_FOR_PR_MESSAGES.toLocalizedString());
  }

  /**
   * An operation upon the messages partitioned region which each subclassing
   * message must implement
   * 
   * @param dm
   *          the manager that received the message
   * @param pr
   *          the partitioned region that should be modified
   * @param startTime the start time of the operation
   * @return true if a reply message should be sent
   * @throws CacheException if an error is generated in the remote cache
   * @throws DataLocationException if the peer is no longer available
   */
  protected abstract boolean operateOnPartitionedRegion(DistributionManager dm,
      PartitionedRegion pr, long startTime) throws CacheException, QueryException,
      DataLocationException, InterruptedException, IOException;

  /**
   * Fill out this instance of the message using the <code>DataInput</code>
   * Required to be a {@link com.gemstone.gemfire.DataSerializable}Note: must
   * be symmetric with {@link #toData(DataOutput)}in what it reads
   */
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.flags = in.readShort();
    setBooleans(this.flags, in);
    this.regionId = in.readInt();
    // extra field post 9.0
    if (InternalDataSerializer.getVersionForDataStream(in).compareTo(
        Version.GFE_90) >= 0) {
      this.isTransactionDistributed = in.readBoolean();
    }
  }

  /**
   * Re-construct the booleans using the compressed short. A subclass must override
   * this method if it is using bits in the compressed short.
   */
  protected void setBooleans(short s, DataInput in) throws IOException,
      ClassNotFoundException {
    if ((s & HAS_PROCESSOR_ID) != 0) {
      this.processorId = in.readInt();
      ReplyProcessor21.setMessageRPId(this.processorId);
    }
    if ((s & NOTIFICATION_ONLY) != 0) this.notificationOnly = true;
    if ((s & HAS_TX_ID) != 0) this.txUniqId = in.readInt();
    if ((s & HAS_TX_MEMBERID) != 0) {
      this.txMemberId = (InternalDistributedMember)DataSerializer.readObject(in);
    }
  }
 
  /**
   * Send the contents of this instance to the DataOutput Required to be a
   * {@link com.gemstone.gemfire.DataSerializable}Note: must be symmetric with
   * {@link #fromData(DataInput)}in what it writes
   */
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    short compressedShort = 0;
    compressedShort = computeCompressedShort(compressedShort);
    out.writeShort(compressedShort);
    if (this.processorId != 0) out.writeInt(this.processorId);
    if (this.txUniqId != TXManagerImpl.NOTX) out.writeInt(this.txUniqId);
    if (this.txMemberId != null) DataSerializer.writeObject(this.txMemberId, out);
    out.writeInt(this.regionId);
    // extra field post 9.0
    if (InternalDataSerializer.getVersionForDataStream(out).compareTo(
        Version.GFE_90) >= 0) {
      out.writeBoolean(this.isTransactionDistributed);
    }
  }

  /**
   * Sets the bits of a short by using the bit masks. A subclass must override
   * this method if it is using bits in the compressed short.
   * @return short with appropriate bits set
   */
  protected short computeCompressedShort(short s) {
    if (this.processorId != 0) s |= HAS_PROCESSOR_ID;
    if (this.notificationOnly) s |= NOTIFICATION_ONLY;
    if (this.getTXUniqId() != TXManagerImpl.NOTX) {
      s |= HAS_TX_ID;
      if (this.txMemberId != null) {
        s |= HAS_TX_MEMBERID;
      }
    }
    return s;
  }

  public final static String PN_TOKEN = ".cache."; 

  @Override
  public String toString()
  {
    StringBuffer buff = new StringBuffer();
    String className = getClass().getName();
//    className.substring(className.lastIndexOf('.', className.lastIndexOf('.') - 1) + 1);  // partition.<foo> more generic version 
    buff.append(className.substring(className.indexOf(PN_TOKEN) + PN_TOKEN.length())); // partition.<foo>
    buff.append("(prid="); // make sure this is the first one
    buff.append(this.regionId);
    
    // Append name, if we have it
    String name = null;
    try {
      PartitionedRegion pr = PartitionedRegion.getPRFromId(this.regionId);
      if (pr != null) {
        name = pr.getFullPath();
      }
    }
    catch (Exception e) {
      /* ignored */
      name = null;
    }
    if (name != null) {
      buff.append(" (name = \"").append(name).append("\")");
    }

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
    buff.append(" processorId=").append(this.processorId);
    if (this.notificationOnly) {
      buff.append(" notificationOnly=").append(this.notificationOnly);
    }
    if (this.txUniqId != TXManagerImpl.NOTX) {
      buff.append(" txId=").append(this.txUniqId);
    }
    if (this.txMemberId != null) {
      buff.append(" txMemberId=").append(this.txMemberId);
    }
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
   * @since 5.5
   */
  public void setHasOldValue(boolean value) {
    // override in subclasses which need old value to be serialized.
    // overridden by classes like PutMessage, DestroyMessage.
  }
  
  /**
   * added to support routing of notification-only messages to clients
   */
  public void setFilterInfo(FilterRoutingInfo filterInfo) {
    // subclasses that support routing to clients should reimplement this method
  }
  /*
  public void appendOldValueToMessage(EntryEventImpl event) {
    
  }*/
  /**
   * @return the txUniqId
   */
  public final int getTXUniqId() {
    return txUniqId;
  }

  public boolean canStartRemoteTransaction() {
    return false;
  }

  public void setSendDeltaWithFullValue(boolean bool) {
    this.sendDeltaWithFullValue = bool;
  }
  
  @Override
  public boolean canParticipateInTransaction() {
    return true;
  }
  
  protected final boolean _mayAddToMultipleSerialGateways(DistributionManager dm) {
    try {
      PartitionedRegion pr = PartitionedRegion.getPRFromId(this.regionId);
      if (pr == null) {
        return false;
      }
      return pr.notifiesMultipleSerialGateways();
    } catch (PRLocallyDestroyedException e) {
      return false;
    } catch (RuntimeException ignore) {
      return false;
    }
  }

  /**
   * A processor on which to await a response from the {@link PartitionMessage}
   * recipient, capturing any CacheException thrown by the recipient and handle
   * it as an expected exception.
   * 
   * @author Mitch Thomas
   * @since 5.0
   * @see #waitForCacheException()
   */
  public static class PartitionResponse extends DirectReplyProcessor {
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
    
    public PartitionResponse(InternalDistributedSystem dm, Set initMembers) {
      this(dm, initMembers, true);
    }
    
    public PartitionResponse(InternalDistributedSystem dm, Set initMembers, boolean register) {
      super(dm, initMembers);
      if(register) {
        register();
      }
    }
    
    public PartitionResponse(InternalDistributedSystem dm, InternalDistributedMember member) {
      this(dm, member, true);
    }
    
    public PartitionResponse(InternalDistributedSystem dm, InternalDistributedMember member, boolean register) {
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
     * Waits for the response from the {@link PartitionMessage}'s recipient
     * @throws CacheException  if the recipient threw a cache exception during message processing 
     * @throws ForceReattemptException if the recipient left the distributed system before the response
     * was received.  
     * @throws PrimaryBucketException 
     */
    final public void waitForCacheException() 
        throws CacheException, ForceReattemptException, PrimaryBucketException {
      try {
        waitForRepliesUninterruptibly();
        if (this.prce!=null || (this.responseRequired && !this.responseReceived)) {
          throw new ForceReattemptException(LocalizedStrings.PartitionMessage_ATTEMPT_FAILED.toLocalizedString(), this.prce);
        }
      }
      catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof CacheException) {
          throw (CacheException)t;
        }
        else if (t instanceof ForceReattemptException) {
          ForceReattemptException ft = (ForceReattemptException)t;
          // See FetchEntriesMessage, which can marshal a ForceReattempt
          // across to the sender
          ForceReattemptException fre = new ForceReattemptException(LocalizedStrings.PartitionMessage_PEER_REQUESTS_REATTEMPT.toLocalizedString(), t);
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
        else if (t instanceof CancelException) {
          logger.debug("PartitionResponse got CacheClosedException from {}, throwing ForceReattemptException", e.getSender(), t);
          throw new ForceReattemptException(LocalizedStrings.PartitionMessage_PARTITIONRESPONSE_GOT_REMOTE_CACHECLOSEDEXCEPTION.toLocalizedString(), t);
        }
        else if (t instanceof DiskAccessException) {
          logger.debug("PartitionResponse got DiskAccessException from {}, throwing ForceReattemptException", e.getSender(), t);
          throw new ForceReattemptException(LocalizedStrings.PartitionMessage_PARTITIONRESPONSE_GOT_REMOTE_CACHECLOSEDEXCEPTION.toLocalizedString(), t);
        }
        else if (t instanceof LowMemoryException) {
          logger.debug("PartitionResponse re-throwing remote LowMemoryException from {}", e.getSender(), t);
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
