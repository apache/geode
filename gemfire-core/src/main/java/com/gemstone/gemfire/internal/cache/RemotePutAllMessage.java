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
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.Operation;
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
import com.gemstone.gemfire.internal.ByteArrayDataInput;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.DistributedPutAllOperation.EntryVersionsList;
import com.gemstone.gemfire.internal.cache.DistributedPutAllOperation.PutAllEntryData;
import com.gemstone.gemfire.internal.cache.partitioned.PutAllPRMessage;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.VersionedObjectList;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;

/**
 * A Replicate Region putAll message.  Meant to be sent only to
 * the peer who hosts transactional data.
 *
 * @since 6.5
 */
public final class RemotePutAllMessage extends RemoteOperationMessageWithDirectReply
  {
  private static final Logger logger = LogService.getLogger();
  
  private PutAllEntryData[] putAllData;

  private int putAllDataCount = 0;

  /** An additional object providing context for the operation, e.g., for BridgeServer notification */
  ClientProxyMembershipID bridgeContext;

  private boolean posDup;

  protected static final short HAS_BRIDGE_CONTEXT = UNRESERVED_FLAGS_START;
  protected static final short SKIP_CALLBACKS = (HAS_BRIDGE_CONTEXT << 1);
  protected static final short IS_PUT_DML = (SKIP_CALLBACKS << 1);

  private EventID eventId;
  
  private boolean skipCallbacks;
  
  private Object callbackArg;

//  private boolean useOriginRemote;

  private boolean isPutDML;
  
  public void addEntry(PutAllEntryData entry) {
    this.putAllData[this.putAllDataCount++] = entry;
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }
  
  public int getSize() {
    return putAllDataCount;
  }
  
  /*
   * this is similar to send() but it selects an initialized replicate
   * that is used to proxy the message
   * 
   */
  public static boolean distribute(EntryEventImpl event, PutAllEntryData[] data,
      int dataCount) {
    boolean successful = false;
    DistributedRegion r = (DistributedRegion)event.getRegion();
    Collection replicates = r.getCacheDistributionAdvisor().adviseInitializedReplicates();
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
        PutAllResponse response = send(replicate, event, data, dataCount, false,
            DistributionManager.SERIAL_EXECUTOR, posDup);
        response.waitForCacheException();
        VersionedObjectList result = response.getResponse();

        // Set successful version tags in PutAllEntryData.
        List successfulKeys = result.getKeys();
        List<VersionTag> versions = result.getVersionTags();
        for (PutAllEntryData putAllEntry : data) {
          Object key = putAllEntry.getKey();
          if (successfulKeys.contains(key)) {
            int index = successfulKeys.indexOf(key);
            putAllEntry.versionTag = versions.get(index);
          }
        }
        return true;

      } catch (TransactionDataNotColocatedException enfe) {
        throw enfe;
      
      } catch (CancelException e) {
        event.getRegion().getCancelCriterion().checkCancelInProgress(e);
      
      } catch (CacheException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("RemotePutMessage caught CacheException during distribution", e);
        }
        successful = true; // not a cancel-exception, so don't complain any more about it
      } catch(RemoteOperationException e) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "RemotePutMessage caught an unexpected exception during distribution", e);
        }
      }
    }
    return successful;
  }

  RemotePutAllMessage(EntryEventImpl event, Set recipients, DirectReplyProcessor p,
      PutAllEntryData[] putAllData, int putAllDataCount,
      boolean useOriginRemote, int processorType, boolean possibleDuplicate, boolean skipCallbacks) {
    super(recipients, event.getRegion().getFullPath(), p);
    this.resetRecipients();
    if (recipients != null) {
      setRecipients(recipients);
    }
    this.processor = p;
    this.processorId = p==null? 0 : p.getProcessorId();
    if (p != null && this.isSevereAlertCompatible()) {
      p.enableSevereAlertProcessing();
    }
    this.putAllData = putAllData;
    this.putAllDataCount = putAllDataCount;
//    this.useOriginRemote = useOriginRemote;
//    this.processorType = processorType;
    this.posDup = possibleDuplicate;
    this.eventId = event.getEventId();
    this.skipCallbacks = skipCallbacks;
    this.callbackArg = event.getCallbackArgument();
	this.isPutDML = event.isPutDML();
  }

  public RemotePutAllMessage() {
  }


  /*
   * Sends a LocalRegion RemotePutAllMessage to the recipient
   * @param recipient the member to which the put message is sent
   * @param r  the LocalRegion for which the put was performed
   * @return the processor used to await acknowledgement that the update was
   *         sent, or null to indicate that no acknowledgement will be sent
   * @throws ForceReattemptException if the peer is no longer available
   */
  public static PutAllResponse send(DistributedMember recipient, EntryEventImpl event,
      PutAllEntryData[] putAllData, int putAllDataCount, boolean useOriginRemote,
      int processorType, boolean possibleDuplicate) throws RemoteOperationException {
    //Assert.assertTrue(recipient != null, "RemotePutAllMessage NULL recipient");  recipient can be null for event notifications
    Set recipients = Collections.singleton(recipient);
    PutAllResponse p = new PutAllResponse(event.getRegion().getSystem(), recipients);
    RemotePutAllMessage msg = new RemotePutAllMessage(event, recipients, p,
        putAllData, putAllDataCount, useOriginRemote, processorType, possibleDuplicate, !event.isGenerateCallbacks());
    msg.setTransactionDistributed(event.getRegion().getCache().getTxManager().isDistributed());
    Set failures = event.getRegion().getDistributionManager().putOutgoing(msg);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(LocalizedStrings.RemotePutMessage_FAILED_SENDING_0.toLocalizedString(msg));
    }
    return p;
  }
  
  public void setBridgeContext(ClientProxyMembershipID contx) {
    Assert.assertTrue(contx != null);
    this.bridgeContext = contx;
  }

  public int getDSFID() {
    return REMOTE_PUTALL_MESSAGE;
  }

  @Override
  public final void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.eventId = (EventID)DataSerializer.readObject(in);
    this.callbackArg = DataSerializer.readObject(in);
    this.posDup = (flags & POS_DUP) != 0;
    if ((flags & HAS_BRIDGE_CONTEXT) != 0) {
      this.bridgeContext = DataSerializer.readObject(in);
    }
    this.skipCallbacks = (flags & SKIP_CALLBACKS) != 0;
    this.isPutDML = (flags & IS_PUT_DML) != 0;
    this.putAllDataCount = (int)InternalDataSerializer.readUnsignedVL(in);
    this.putAllData = new PutAllEntryData[putAllDataCount];
    if (this.putAllDataCount > 0) {
      final Version version = InternalDataSerializer
          .getVersionForDataStreamOrNull(in);
      final ByteArrayDataInput bytesIn = new ByteArrayDataInput();
      for (int i = 0; i < this.putAllDataCount; i++) {
        this.putAllData[i] = new PutAllEntryData(in, this.eventId, i, version,
            bytesIn);
      }

      boolean hasTags = in.readBoolean();
      if (hasTags) {
        EntryVersionsList versionTags = EntryVersionsList.create(in);
        for (int i = 0; i < this.putAllDataCount; i++) {
          this.putAllData[i].versionTag = versionTags.get(i);
        }
      }
    }
  }

  @Override
  public final void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.eventId, out);
    DataSerializer.writeObject(this.callbackArg, out);
    if (this.bridgeContext != null) {
      DataSerializer.writeObject(this.bridgeContext, out);
    }

    InternalDataSerializer.writeUnsignedVL(this.putAllDataCount, out);

    if (this.putAllDataCount > 0) {

      EntryVersionsList versionTags = new EntryVersionsList(putAllDataCount);

      boolean hasTags = false;
      // get the "keyRequiresRegionContext" flag from first element assuming
      // all key objects to be uniform
      final boolean requiresRegionContext =
        (this.putAllData[0].key instanceof KeyWithRegionContext);
      for (int i = 0; i < this.putAllDataCount; i++) {
        if (!hasTags && putAllData[i].versionTag != null) {
          hasTags = true;
        }
        VersionTag<?> tag = putAllData[i].versionTag;
        versionTags.add(tag);
        putAllData[i].versionTag = null;
        this.putAllData[i].toData(out, requiresRegionContext);
        this.putAllData[i].versionTag = tag;
      }

      out.writeBoolean(hasTags);
      if (hasTags) {
        InternalDataSerializer.invokeToData(versionTags, out);
      }
    }
  }

  @Override
  protected short computeCompressedShort() {
    short flags = super.computeCompressedShort();
    if (this.posDup) flags |= POS_DUP;
    if (this.bridgeContext != null) flags |= HAS_BRIDGE_CONTEXT;
    if (this.skipCallbacks) flags |= SKIP_CALLBACKS;
    if (this.isPutDML) flags |= IS_PUT_DML;
    return flags;
  }

  @Override
  public EventID getEventID() {
    return this.eventId;
  }

  @Override
  protected boolean operateOnRegion(DistributionManager dm,
      LocalRegion r,long startTime) throws RemoteOperationException {

    final boolean sendReply;

    InternalDistributedMember eventSender = getSender();

    long lastModified = 0L;
    try {
      sendReply = doLocalPutAll(r, eventSender, lastModified);
    }
    catch (RemoteOperationException e) {
      sendReply(getSender(), getProcessorId(), dm, 
          new ReplyException(e), r, startTime);
      return false;
    }

    if (sendReply) {
      sendReply(getSender(), getProcessorId(), dm, null, r, startTime);
    }
    return false;
  }

  /* we need a event with content for waitForNodeOrCreateBucket() */
  /**
   * This method is called by both operateOnLocalRegion() when processing a remote msg
   * or by sendMsgByBucket() when processing a msg targeted to local Jvm. 
   * LocalRegion Note: It is very important that this message does NOT
   * cause any deadlocks as the sender will wait indefinitely for the
   * acknowledgment
   * @param r partitioned region
   *        eventSender the endpoint server who received request from client
   *        lastModified timestamp for last modification
   * @return If succeeds, return true, otherwise, throw exception
   */
  public final boolean doLocalPutAll(final LocalRegion r, final InternalDistributedMember eventSender, long lastModified)
    throws EntryExistsException, RemoteOperationException {
    final DistributedRegion dr = (DistributedRegion)r;
    
    // create a base event and a DPAO for PutAllMessage distributed btw redundant buckets
    EntryEventImpl baseEvent = EntryEventImpl.create(
        r, Operation.PUTALL_CREATE,
        null, null, this.callbackArg, false, eventSender, !skipCallbacks);
    try {

    baseEvent.setCausedByMessage(this);
    
    // set baseEventId to the first entry's event id. We need the thread id for DACE
    baseEvent.setEventId(this.eventId);
    if (this.bridgeContext != null) {
      baseEvent.setContext(this.bridgeContext);
    }
    baseEvent.setPossibleDuplicate(this.posDup);
	baseEvent.setPutDML(this.isPutDML);
    if (logger.isDebugEnabled()) {
      logger.debug("RemotePutAllMessage.doLocalPutAll: eventSender is {}, baseEvent is {}, msg is {}",
          eventSender, baseEvent, this);
    }
    final DistributedPutAllOperation dpao = new DistributedPutAllOperation(baseEvent, putAllDataCount, false);
    try {
    final VersionedObjectList versions = new VersionedObjectList(putAllDataCount, true, dr.concurrencyChecksEnabled);
    dr.syncBulkOp(new Runnable() {
      @SuppressWarnings("synthetic-access")
      public void run() {
//        final boolean requiresRegionContext = dr.keyRequiresRegionContext();
        InternalDistributedMember myId = r.getDistributionManager().getDistributionManagerId();
        for (int i = 0; i < putAllDataCount; ++i) {
          EntryEventImpl ev = PutAllPRMessage.getEventFromEntry(r, myId, eventSender, i, putAllData, false, bridgeContext, posDup, !skipCallbacks, isPutDML);
          try {
          ev.setPutAllOperation(dpao);
          if (logger.isDebugEnabled()) {
            logger.debug("invoking basicPut with {}", ev);
          }
          if (dr.basicPut(ev, false, false, null, false)) {
            putAllData[i].versionTag = ev.getVersionTag();
            versions.addKeyAndVersion(putAllData[i].key, ev.getVersionTag());
          }
          } finally {
            ev.release();
          }
        }
      }
    }, baseEvent.getEventId());
    if(getTXUniqId()!=TXManagerImpl.NOTX || dr.getConcurrencyChecksEnabled()) {
    	dr.getDataView().postPutAll(dpao, versions, dr);
    }
    PutAllReplyMessage.send(getSender(), this.processorId, 
        getReplySender(r.getDistributionManager()), versions, this.putAllData, this.putAllDataCount);
    return false;
    } finally {
      dpao.freeOffHeapResources();
    }
    } finally {
      baseEvent.release();
    }
  }

  
  // override reply processor type from PartitionMessage
  RemoteOperationResponse createReplyProcessor(LocalRegion r, Set recipients, Object key) {
    return new PutAllResponse(r.getSystem(), recipients);
  }

  // override reply message type from PartitionMessage
  @Override
  protected void sendReply(InternalDistributedMember member, int procId, DM dm, ReplyException ex, LocalRegion r, long startTime) {
    ReplyMessage.send(member, procId, ex, getReplySender(dm), r != null && r.isInternalRegion());
  }


  @Override
  protected final void appendFields(StringBuffer buff)
  {
    super.appendFields(buff);
    buff.append("; putAllDataCount=").append(putAllDataCount);
    if (this.bridgeContext != null) {
      buff.append("; bridgeContext=").append(this.bridgeContext);
    }
    for (int i=0; i<putAllDataCount; i++) {
      buff.append("; entry"+i+":").append(putAllData[i]==null? "null" : putAllData[i].getKey());
    }
  }
  
  public static final class PutAllReplyMessage extends ReplyMessage {
    /** Result of the PutAll operation */
    //private PutAllResponseData[] responseData;
    private VersionedObjectList versions;

    @Override
    public boolean getInlineProcess() {
      return true;
    }

    private PutAllReplyMessage(int processorId, VersionedObjectList versionList, PutAllEntryData[] putAllData, int putAllCount)  {
      super();
      this.versions = versionList;
      setProcessorId(processorId);
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId, ReplySender dm, VersionedObjectList versions,
        PutAllEntryData[] putAllData, int putAllDataCount)  {
      Assert.assertTrue(recipient != null, "PutAllReplyMessage NULL reply message");
      PutAllReplyMessage m = new PutAllReplyMessage(processorId, versions, putAllData, putAllDataCount);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    /**
     * Processes this message.  This method is invoked by the receiver
     * of the message.
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DM dm, final ReplyProcessor21 rp) {
      final long startTime = getTimestamp();

      if (rp == null) {
        if (logger.isTraceEnabled(LogMarker.DM)) {
          logger.trace(LogMarker.DM, "PutAllReplyMessage processor not found");
        }
        return;
      }
      if (rp instanceof PutAllResponse) {
        PutAllResponse processor = (PutAllResponse)rp;
        processor.setResponse(this);
      }
      rp.process(this);

      if (logger.isTraceEnabled(LogMarker.DM)) {
        logger.trace(LogMarker.DM, "{} processed {}", rp, this);
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime()-startTime);
    }

    @Override
    public int getDSFID() {
      return REMOTE_PUTALL_REPLY_MESSAGE;
    }

    public PutAllReplyMessage() {
    }

    @Override
    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.versions = (VersionedObjectList)DataSerializer.readObject(in); 
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeObject(this.versions, out);
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("PutAllReplyMessage ")
        .append(" processorid=").append(this.processorId)
        .append(" returning versionTags=").append(this.versions);
      return sb.toString();
    }

  }
  
  /**
   * A processor to capture the value returned by {@link RemotePutAllMessage}
   */
  public static class PutAllResponse extends RemoteOperationResponse {
    //private volatile PutAllResponseData[] returnValue;
    private VersionedObjectList versions;

    public PutAllResponse(InternalDistributedSystem ds, Set recipients) {
      super(ds, recipients, false);
    }


    public void setResponse(PutAllReplyMessage putAllReplyMessage) {
      if (putAllReplyMessage.versions != null) {
        this.versions = putAllReplyMessage.versions;
        this.versions.replaceNullIDs(putAllReplyMessage.getSender());
      }
    }
    
    public VersionedObjectList getResponse() {
      return this.versions;
    }
  }
}
