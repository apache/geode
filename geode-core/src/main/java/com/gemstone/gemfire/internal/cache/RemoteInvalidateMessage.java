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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.TransactionDataNotColocatedException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.ReplySender;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.versions.DiskVersionTag;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import static com.gemstone.gemfire.internal.cache.DistributedCacheOperation.VALUE_IS_BYTES;
import static com.gemstone.gemfire.internal.cache.DistributedCacheOperation.VALUE_IS_SERIALIZED_OBJECT;

public final class RemoteInvalidateMessage extends RemoteDestroyMessage {

  private static final Logger logger = LogService.getLogger();
  
  /**
   * Empty constructor to satisfy {@link com.gemstone.gemfire.DataSerializer}
   * requirements
   */
  public RemoteInvalidateMessage() {
  }

  private RemoteInvalidateMessage(Set recipients,
                            String regionPath,
                            DirectReplyProcessor processor,
                            EntryEventImpl event,
                            boolean useOriginRemote, boolean possibleDuplicate) {
    super(recipients,
          regionPath,
          processor,
          event,
          null, DistributionManager.PARTITIONED_REGION_EXECUTOR, useOriginRemote,
          possibleDuplicate);
  }

  public static boolean distribute(EntryEventImpl event, boolean onlyPersistent) {
    boolean successful = false;
    DistributedRegion r = (DistributedRegion)event.getRegion();
    Collection replicates = onlyPersistent ? r.getCacheDistributionAdvisor()
        .adviseInitializedPersistentMembers().keySet() : r
        .getCacheDistributionAdvisor().adviseInitializedReplicates();
    if (replicates.isEmpty()) {
      return false;
    }
    if (replicates.size() > 1) {
      ArrayList l = new ArrayList(replicates);
      Collections.shuffle(l);
      replicates = l;
    }
    int attempts = 0;
    for (Iterator<InternalDistributedMember> it=replicates.iterator(); it.hasNext(); ) {
      InternalDistributedMember replicate = it.next();
      try {
        attempts++;
        final boolean posDup = (attempts > 1);
        InvalidateResponse processor = send(replicate, event.getRegion(), 
            event, DistributionManager.SERIAL_EXECUTOR, false, posDup);
        processor.waitForCacheException();
        VersionTag versionTag = processor.getVersionTag();
        if (versionTag != null) {
          event.setVersionTag(versionTag);
          if (event.getRegion().getVersionVector() != null) {
            event.getRegion().getVersionVector().recordVersion(versionTag.getMemberID(), versionTag);
          }
        }
        event.setInhibitDistribution(true);
        return true;

      } catch (TransactionDataNotColocatedException enfe) {
        throw enfe;
  
      } catch (CancelException e) {
        event.getRegion().getCancelCriterion().checkCancelInProgress(e);
      
      } catch (EntryNotFoundException e) {
        throw new EntryNotFoundException(""+event.getKey());
        
      } catch (CacheException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("RemoteDestroyMessage caught CacheException during distribution", e);
        }
        successful = true; // not a cancel-exception, so don't complain any more about it
      
      } catch(RemoteOperationException e) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "RemoteDestroyMessage caught an unexpected exception during distribution", e);
        }
      }
    }
    return successful;
  }

  /**
   * Sends a transactional RemoteInvalidateMessage
   * {@link com.gemstone.gemfire.cache.Region#invalidate(Object)}message to the
   * recipient
   * 
   * @param recipient the recipient of the message
   * @param r
   *          the ReplicateRegion for which the invalidate was performed
   * @param event the event causing this message
   * @param processorType the type of executor to use
   * @param useOriginRemote whether the receiver should use originRemote=true in its event
   * @return the InvalidateResponse processor used to await the potential
   *         {@link com.gemstone.gemfire.cache.CacheException}
   */
  public static InvalidateResponse send(DistributedMember recipient,
      LocalRegion r, EntryEventImpl event, int processorType,
      boolean useOriginRemote, boolean possibleDuplicate)
      throws RemoteOperationException
  {
    //Assert.assertTrue(recipient != null, "RemoteInvalidateMessage NULL recipient");  recipient may be null for remote notifications
    Set recipients = Collections.singleton(recipient);
    InvalidateResponse p = new InvalidateResponse(r.getSystem(), recipients, event.getKey());
    RemoteInvalidateMessage m = new RemoteInvalidateMessage(recipients,
        r.getFullPath(), p, event, useOriginRemote, possibleDuplicate);
    m.setTransactionDistributed(r.getCache().getTxManager().isDistributed());
    Set failures =r.getDistributionManager().putOutgoing(m); 
    if (failures != null && failures.size() > 0 ) {
      throw new RemoteOperationException(LocalizedStrings.InvalidateMessage_FAILED_SENDING_0.toLocalizedString(m));
    }
    return p;
  }

  /**
   * This method is called upon receipt and make the desired changes to the
   * PartitionedRegion Note: It is very important that this message does NOT
   * cause any deadlocks as the sender will wait indefinitely for the
   * acknowledgement
   * 
   * @throws EntryExistsException
   */
  @Override
  protected boolean operateOnRegion(DistributionManager dm,
      LocalRegion r, long startTime)
      throws EntryExistsException, RemoteOperationException
  {

    InternalDistributedMember eventSender = originalSender;
    if (eventSender == null) {
       eventSender = getSender();
    }
    final Object key = getKey();
    if (r.keyRequiresRegionContext()) {
      ((KeyWithRegionContext)key).setRegionContext(r);
    }
    final EntryEventImpl event = EntryEventImpl.create(
        r,
        getOperation(),
        key,
        null, /*newValue*/
        getCallbackArg(),
        this.useOriginRemote/*originRemote - false to force distribution in buckets*/,
        eventSender,
        true/*generateCallbacks*/,
        false/*initializeId*/);
    try {
    if (this.bridgeContext != null) {
      event.setContext(this.bridgeContext);
    }
    
    event.setCausedByMessage(this);

    if (this.versionTag != null) {
      this.versionTag.replaceNullIDs(getSender());
      event.setVersionTag(this.versionTag);
    }
    Assert.assertTrue(eventId != null);
    event.setEventId(eventId);
    
    event.setPossibleDuplicate(this.possibleDuplicate);
    
    // for cqs, which needs old value based on old value being sent on wire.
    boolean eventShouldHaveOldValue = getHasOldValue();
    if (eventShouldHaveOldValue){
      if (getOldValueIsSerialized()){
        event.setSerializedOldValue(getOldValueBytes());
      }
      else{
        event.setOldValue(getOldValueBytes());
      }
    }
    boolean sendReply = true;
      try {
        r.checkReadiness();
        r.checkForLimitedOrNoAccess();
        r.basicInvalidate(event);
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "remoteInvalidated key: {}", key);
        }
        sendReply(getSender(), this.processorId, dm, /*ex*/null, 
            event.getRegion(), event.getVersionTag(), startTime);
        sendReply = false;
      }
      catch (EntryNotFoundException eee) {
        //        failed = true;
        if (logger.isDebugEnabled()) {
          logger.debug("operateOnRegion caught EntryNotFoundException");
        }
        sendReply(getSender(), getProcessorId(), dm, new ReplyException(eee), r, null, startTime);
        sendReply = false; // this prevents us from acking later
      }
      catch (PrimaryBucketException pbe) {
        sendReply(getSender(), getProcessorId(), dm, new ReplyException(pbe), r, startTime);
        return false;
      }

    return sendReply;
    } finally {
      event.release();
    }
  }


  // override reply processor type from PartitionMessage
  RemoteOperationResponse createReplyProcessor(PartitionedRegion r, Set recipients, Object key) {
    return new InvalidateResponse(r.getSystem(), recipients, key);
  }
  
  // override reply message type from PartitionMessage
  @Override
  protected void sendReply(InternalDistributedMember member, int procId, DM dm,
      ReplyException ex, LocalRegion r, long startTime) {
    sendReply(member, procId, dm, ex, r, null, startTime);
  }
  
  protected void sendReply(InternalDistributedMember member, int procId, DM dm, ReplyException ex, 
      LocalRegion r, VersionTag versionTag, long startTime) {
    /*if (pr != null && startTime > 0) {
      pr.getPrStats().endPartitionMessagesProcessing(startTime); 
    }*/
    InvalidateReplyMessage.send(member, procId, getReplySender(dm), versionTag, ex);
  }

  @Override
  public int getDSFID() {
    return R_INVALIDATE_MESSAGE;
  }
  

  public static final class InvalidateReplyMessage extends ReplyMessage {
    private VersionTag versionTag;

    private static final byte HAS_VERSION = 0x01;
    private static final byte PERSISTENT  = 0x02;
    /**
     * DSFIDFactory constructor
     */
    public InvalidateReplyMessage() {
    }
  
    private InvalidateReplyMessage(int processorId, VersionTag versionTag, ReplyException ex)
    {
      super();
      setProcessorId(processorId);
      this.versionTag = versionTag;
      setException(ex);
    }
    
    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId,
        ReplySender replySender, VersionTag versionTag, ReplyException ex) 
    {
      Assert.assertTrue(recipient != null, "InvalidateReplyMessage NULL reply message");
      InvalidateReplyMessage m = new InvalidateReplyMessage(processorId, versionTag, ex);
      m.setRecipient(recipient);
      replySender.putOutgoing(m);
    }
      
    /**
     * Processes this message.  This method is invoked by the receiver
     * of the message.
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DM dm, final ReplyProcessor21 rp) {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "InvalidateReplyMessage process invoking reply processor with processorId:{}", this.processorId);
      }
  
      if (rp == null) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "InvalidateReplyMessage processor not found");
        }
        return;
      }
      if (this.versionTag != null) {
        this.versionTag.replaceNullIDs(getSender());
      }
      if (rp instanceof InvalidateResponse) {
        InvalidateResponse processor = (InvalidateResponse)rp;
        processor.setResponse(this.versionTag);
      }
      rp.process(this);
  
      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "{} processed {}", rp, this);
      }

      dm.getStats().incReplyMessageTime(NanoTimer.getTime()-startTime);
    }
    
    @Override
    public int getDSFID() {
      return R_INVALIDATE_REPLY_MESSAGE;
    }
    
    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      byte b = 0;
      if(this.versionTag != null) {
        b |= HAS_VERSION;
      }
      if(this.versionTag instanceof DiskVersionTag) {
        b |= PERSISTENT;
      }
      out.writeByte(b);
      if (this.versionTag != null) {
        InternalDataSerializer.invokeToData(this.versionTag, out);
    }
    }
  
    @Override
    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
      super.fromData(in);
      byte b = in.readByte();
      boolean hasTag = (b & HAS_VERSION) != 0; 
      boolean persistentTag = (b & PERSISTENT) != 0;
      if (hasTag) {
        this.versionTag = VersionTag.create(persistentTag, in);
      }
    }
  
    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("InvalidateReplyMessage ")
      .append("processorid=").append(this.processorId)
      .append(" exception=").append(getException());
      if (this.versionTag != null) {
        sb.append("version=").append(this.versionTag);
      }
      return sb.toString();
    }

  }
  /**
   * A processor to capture the value returned by {@link RemoteInvalidateMessage}
   * @since 6.5
   */
  public static class InvalidateResponse extends RemoteOperationResponse  {
    private volatile boolean returnValueReceived;
    final Object key;
    VersionTag versionTag;
    
    public InvalidateResponse(InternalDistributedSystem ds, Set recipients, Object key) {
      super(ds, recipients, true);
      this.key = key;
    }

    public void setResponse(VersionTag versionTag) {
      this.returnValueReceived = true;
      this.versionTag = versionTag;
    }

    /**
     * @throws CacheException if the peer generates an error
     */
    public void waitForResult() throws CacheException, RemoteOperationException
    {
      try {
        waitForCacheException();
      }
      catch (RemoteOperationException e) {
        e.checkKey(key);
        throw e;
      }
      if (!this.returnValueReceived) {
        throw new RemoteOperationException(LocalizedStrings.InvalidateMessage_NO_RESPONSE_CODE_RECEIVED.toLocalizedString());
      }
      return;
    }
    
    public VersionTag getVersionTag() {
      return this.versionTag;
  }
  }
  


}
