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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.TransactionDataNotColocatedException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ReplySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.ByteArrayDataInput;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.DistributedPutAllOperation;
import org.apache.geode.internal.cache.DistributedPutAllOperation.EntryVersionsList;
import org.apache.geode.internal.cache.DistributedPutAllOperation.PutAllEntryData;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.partitioned.PutAllPRMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.annotations.Released;

/**
 * A Replicate Region putAll message. Meant to be sent only to the peer who hosts transactional
 * data. It is also used to implement non-transactional putAlls, see:
 * DistributedPutAllOperation.initMessage
 *
 * @since GemFire 6.5
 */
public class RemotePutAllMessage extends RemoteOperationMessageWithDirectReply {
  private static final Logger logger = LogService.getLogger();

  private PutAllEntryData[] putAllData;

  private int putAllDataCount = 0;

  /**
   * An additional object providing context for the operation, e.g., for BridgeServer notification
   */
  ClientProxyMembershipID bridgeContext;

  private boolean posDup;

  protected static final short HAS_BRIDGE_CONTEXT = UNRESERVED_FLAGS_START;
  protected static final short SKIP_CALLBACKS = (HAS_BRIDGE_CONTEXT << 1);

  private EventID eventId;

  private boolean skipCallbacks;

  private Object callbackArg;

  public void addEntry(PutAllEntryData entry) {
    this.putAllData[this.putAllDataCount++] = entry;
  }

  public int getSize() {
    return putAllDataCount;
  }

  /*
   * this is similar to send() but it selects an initialized replicate that is used to proxy the
   * message
   *
   */
  public static boolean distribute(EntryEventImpl event, PutAllEntryData[] data, int dataCount) {
    boolean successful = false;
    DistributedRegion r = (DistributedRegion) event.getRegion();
    Collection<InternalDistributedMember> replicates =
        r.getCacheDistributionAdvisor().adviseInitializedReplicates();
    if (replicates.isEmpty()) {
      return false;
    }
    if (replicates.size() > 1) {
      ArrayList<InternalDistributedMember> l = new ArrayList<>(replicates);
      Collections.shuffle(l);
      replicates = l;
    }
    int attempts = 0;
    for (InternalDistributedMember replicate : replicates) {
      try {
        attempts++;
        final boolean posDup = (attempts > 1);
        PutAllResponse response = send(replicate, event, data, dataCount, false, posDup);
        response.waitForRemoteResponse();
        VersionedObjectList result = response.getResponse();

        // Set successful version tags in PutAllEntryData.
        List<Object> successfulKeys = result.getKeys();
        @SuppressWarnings("rawtypes")
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
          logger.debug("RemotePutAllMessage caught CacheException during distribution", e);
        }
        successful = true; // not a cancel-exception, so don't complain any more about it
      } catch (RegionDestroyedException | RemoteOperationException e) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE,
              "RemotePutAllMessage caught an exception during distribution; retrying to another member",
              e);
        }
      }
    }
    return successful;
  }

  RemotePutAllMessage(EntryEventImpl event, DistributedMember recipient, DirectReplyProcessor p,
      PutAllEntryData[] putAllData, int putAllDataCount, boolean useOriginRemote,
      boolean possibleDuplicate, boolean skipCallbacks) {
    super((InternalDistributedMember) recipient, event.getRegion().getFullPath(), p);
    this.processor = p;
    this.processorId = p == null ? 0 : p.getProcessorId();
    if (p != null && this.isSevereAlertCompatible()) {
      p.enableSevereAlertProcessing();
    }
    this.putAllData = putAllData;
    this.putAllDataCount = putAllDataCount;
    this.posDup = possibleDuplicate;
    this.eventId = event.getEventId();
    this.skipCallbacks = skipCallbacks;
    this.callbackArg = event.getCallbackArgument();
  }

  public RemotePutAllMessage() {}


  /*
   * Sends a LocalRegion RemotePutAllMessage to the recipient
   *
   * @param recipient the member to which the put message is sent
   *
   * @param r the LocalRegion for which the put was performed
   *
   * @return the processor used to await acknowledgement that the update was sent, or null to
   * indicate that no acknowledgement will be sent
   *
   * @throws RemoteOperationException if the peer is no longer available
   */
  public static PutAllResponse send(DistributedMember recipient, EntryEventImpl event,
      PutAllEntryData[] putAllData, int putAllDataCount, boolean useOriginRemote,
      boolean possibleDuplicate) throws RemoteOperationException {
    PutAllResponse p = new PutAllResponse(event.getRegion().getSystem(), recipient);
    RemotePutAllMessage msg = new RemotePutAllMessage(event, recipient, p, putAllData,
        putAllDataCount, useOriginRemote, possibleDuplicate, !event.isGenerateCallbacks());
    Set<?> failures = event.getRegion().getDistributionManager().putOutgoing(msg);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(
          String.format("Failed sending < %s >", msg));
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
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.eventId = (EventID) DataSerializer.readObject(in);
    this.callbackArg = DataSerializer.readObject(in);
    this.posDup = (flags & POS_DUP) != 0;
    if ((flags & HAS_BRIDGE_CONTEXT) != 0) {
      this.bridgeContext = DataSerializer.readObject(in);
    }
    this.skipCallbacks = (flags & SKIP_CALLBACKS) != 0;
    this.putAllDataCount = (int) InternalDataSerializer.readUnsignedVL(in);
    this.putAllData = new PutAllEntryData[putAllDataCount];
    if (this.putAllDataCount > 0) {
      final Version version = InternalDataSerializer.getVersionForDataStreamOrNull(in);
      final ByteArrayDataInput bytesIn = new ByteArrayDataInput();
      for (int i = 0; i < this.putAllDataCount; i++) {
        this.putAllData[i] = new PutAllEntryData(in, this.eventId, i, version, bytesIn);
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
  public void toData(DataOutput out) throws IOException {
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
      for (int i = 0; i < this.putAllDataCount; i++) {
        if (!hasTags && putAllData[i].versionTag != null) {
          hasTags = true;
        }
        VersionTag<?> tag = putAllData[i].versionTag;
        versionTags.add(tag);
        putAllData[i].versionTag = null;
        this.putAllData[i].toData(out);
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
    if (this.posDup)
      flags |= POS_DUP;
    if (this.bridgeContext != null)
      flags |= HAS_BRIDGE_CONTEXT;
    if (this.skipCallbacks)
      flags |= SKIP_CALLBACKS;
    return flags;
  }

  @Override
  public EventID getEventID() {
    return this.eventId;
  }

  @Override
  protected boolean operateOnRegion(ClusterDistributionManager dm, LocalRegion r, long startTime)
      throws RemoteOperationException {

    final boolean sendReply;

    InternalDistributedMember eventSender = getSender();

    long lastModified = 0L;
    try {
      sendReply = doLocalPutAll(r, eventSender, lastModified);
    } catch (RemoteOperationException e) {
      sendReply(getSender(), getProcessorId(), dm, new ReplyException(e), r, startTime);
      return false;
    }

    if (sendReply) {
      sendReply(getSender(), getProcessorId(), dm, null, r, startTime);
    }
    return false;
  }

  /**
   * This method is called by both operateOnLocalRegion() when processing a remote msg or by
   * sendMsgByBucket() when processing a msg targeted to local Jvm. LocalRegion Note: It is very
   * important that this message does NOT cause any deadlocks as the sender will wait indefinitely
   * for the acknowledgment
   *
   * @param r region
   * @param eventSender the endpoint server who received request from client
   * @param lastModified timestamp for last modification
   * @return If succeeds, return true, otherwise, throw exception
   */
  public boolean doLocalPutAll(final LocalRegion r, final InternalDistributedMember eventSender,
      long lastModified) throws EntryExistsException, RemoteOperationException {
    final DistributedRegion dr = (DistributedRegion) r;

    @Released
    EntryEventImpl baseEvent = EntryEventImpl.create(r, Operation.PUTALL_CREATE, null, null,
        this.callbackArg, false, eventSender, !skipCallbacks);
    try {

      baseEvent.setCausedByMessage(this);

      // set baseEventId to the first entry's event id. We need the thread id for DACE
      baseEvent.setEventId(this.eventId);
      if (this.bridgeContext != null) {
        baseEvent.setContext(this.bridgeContext);
      }
      baseEvent.setPossibleDuplicate(this.posDup);
      if (logger.isDebugEnabled()) {
        logger.debug(
            "RemotePutAllMessage.doLocalPutAll: eventSender is {}, baseEvent is {}, msg is {}",
            eventSender, baseEvent, this);
      }
      final DistributedPutAllOperation dpao =
          new DistributedPutAllOperation(baseEvent, putAllDataCount, false);
      try {
        final VersionedObjectList versions =
            new VersionedObjectList(putAllDataCount, true, dr.getConcurrencyChecksEnabled());
        dr.syncBulkOp(new Runnable() {
          @SuppressWarnings("synthetic-access")
          public void run() {
            InternalDistributedMember myId = r.getDistributionManager().getDistributionManagerId();
            for (int i = 0; i < putAllDataCount; ++i) {
              @Released
              EntryEventImpl ev = PutAllPRMessage.getEventFromEntry(r, myId, eventSender, i,
                  putAllData, false, bridgeContext, posDup, !skipCallbacks);
              try {
                ev.setPutAllOperation(dpao);
                if (logger.isDebugEnabled()) {
                  logger.debug("invoking basicPut with {}", ev);
                }
                if (dr.basicPut(ev, false, false, null, false)) {
                  putAllData[i].versionTag = ev.getVersionTag();
                  versions.addKeyAndVersion(putAllData[i].getKey(), ev.getVersionTag());
                }
              } finally {
                ev.release();
              }
            }
          }
        }, baseEvent.getEventId());
        if (getTXUniqId() != TXManagerImpl.NOTX || dr.getConcurrencyChecksEnabled()) {
          dr.getDataView().postPutAll(dpao, versions, dr);
        }
        PutAllReplyMessage.send(getSender(), this.processorId,
            getReplySender(r.getDistributionManager()), versions, this.putAllData,
            this.putAllDataCount);
        return false;
      } finally {
        dpao.freeOffHeapResources();
      }
    } finally {
      baseEvent.release();
    }
  }

  @Override
  protected void appendFields(StringBuffer buff) {
    super.appendFields(buff);
    buff.append("; putAllDataCount=").append(putAllDataCount);
    if (this.bridgeContext != null) {
      buff.append("; bridgeContext=").append(this.bridgeContext);
    }
    for (int i = 0; i < putAllDataCount; i++) {
      buff.append("; entry" + i + ":")
          .append(putAllData[i] == null ? "null" : putAllData[i].getKey());
    }
  }

  public static class PutAllReplyMessage extends ReplyMessage {
    private VersionedObjectList versions;

    @Override
    public boolean getInlineProcess() {
      return true;
    }

    private PutAllReplyMessage(int processorId, VersionedObjectList versionList,
        PutAllEntryData[] putAllData, int putAllCount) {
      super();
      this.versions = versionList;
      setProcessorId(processorId);
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId, ReplySender dm,
        VersionedObjectList versions, PutAllEntryData[] putAllData, int putAllDataCount) {
      Assert.assertTrue(recipient != null, "PutAllReplyMessage NULL reply message");
      PutAllReplyMessage m =
          new PutAllReplyMessage(processorId, versions, putAllData, putAllDataCount);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the message.
     *
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DistributionManager dm, final ReplyProcessor21 rp) {
      final long startTime = getTimestamp();

      if (rp == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "PutAllReplyMessage processor not found");
        }
        return;
      }
      if (rp instanceof PutAllResponse) {
        PutAllResponse processor = (PutAllResponse) rp;
        processor.setResponse(this);
      }
      rp.process(this);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} processed {}", rp, this);
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return REMOTE_PUTALL_REPLY_MESSAGE;
    }

    public PutAllReplyMessage() {}

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.versions = (VersionedObjectList) DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeObject(this.versions, out);
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("PutAllReplyMessage ").append(" processorid=").append(this.processorId)
          .append(" returning versionTags=").append(this.versions);
      return sb.toString();
    }

  }

  /**
   * A processor to capture the value returned by {@link RemotePutAllMessage}
   */
  public static class PutAllResponse extends RemoteOperationResponse {
    private VersionedObjectList versions;

    public PutAllResponse(InternalDistributedSystem ds, DistributedMember recipient) {
      super(ds, (InternalDistributedMember) recipient, false);
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
