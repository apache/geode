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
import org.apache.geode.cache.EntryNotFoundException;
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
import org.apache.geode.internal.cache.DistributedPutAllOperation.EntryVersionsList;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.DistributedRemoveAllOperation;
import org.apache.geode.internal.cache.DistributedRemoveAllOperation.RemoveAllEntryData;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.cache.TXManagerImpl;
import org.apache.geode.internal.cache.partitioned.RemoveAllPRMessage;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.annotations.Released;

/**
 * A Replicate Region removeAll message. Meant to be sent only to the peer who hosts transactional
 * data. It is also used to implement non-transactional removeAlls, see:
 * DistributedRemoveAllOperation.initMessage
 *
 * @since GemFire 8.1
 */
public class RemoteRemoveAllMessage extends RemoteOperationMessageWithDirectReply {
  private static final Logger logger = LogService.getLogger();

  private RemoveAllEntryData[] removeAllData;

  private int removeAllDataCount = 0;

  /**
   * An additional object providing context for the operation, e.g., for BridgeServer notification
   */
  ClientProxyMembershipID bridgeContext;

  private boolean posDup;

  protected static final short HAS_BRIDGE_CONTEXT = UNRESERVED_FLAGS_START;

  private EventID eventId;

  private Object callbackArg;

  public void addEntry(RemoveAllEntryData entry) {
    this.removeAllData[this.removeAllDataCount++] = entry;
  }

  public int getSize() {
    return removeAllDataCount;
  }

  /*
   * this is similar to send() but it selects an initialized replicate that is used to proxy the
   * message
   */
  public static boolean distribute(EntryEventImpl event, RemoveAllEntryData[] data, int dataCount) {
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
        RemoveAllResponse response = send(replicate, event, data, dataCount, false, posDup);
        response.waitForRemoteResponse();
        VersionedObjectList result = response.getResponse();

        // Set successful version tags in RemoveAllEntryData.
        List<Object> successfulKeys = result.getKeys();
        @SuppressWarnings("rawtypes")
        List<VersionTag> versions = result.getVersionTags();
        for (RemoveAllEntryData removeAllEntry : data) {
          Object key = removeAllEntry.getKey();
          if (successfulKeys.contains(key)) {
            int index = successfulKeys.indexOf(key);
            removeAllEntry.versionTag = versions.get(index);
          }
        }
        return true;

      } catch (TransactionDataNotColocatedException enfe) {
        throw enfe;

      } catch (CancelException e) {
        event.getRegion().getCancelCriterion().checkCancelInProgress(e);

      } catch (CacheException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("RemoteRemoveAllMessage caught CacheException during distribution", e);
        }
        successful = true; // not a cancel-exception, so don't complain any more about it
      } catch (RegionDestroyedException | RemoteOperationException e) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE,
              "RemoteRemoveAllMessage caught an exception during distribution; retrying to another member",
              e);
        }
      }
    }
    return successful;
  }

  RemoteRemoveAllMessage(EntryEventImpl event, DistributedMember recipient, DirectReplyProcessor p,
      RemoveAllEntryData[] removeAllData, int removeAllDataCount, boolean useOriginRemote,
      boolean possibleDuplicate) {
    super((InternalDistributedMember) recipient, event.getRegion().getFullPath(), p);
    this.processor = p;
    this.processorId = p == null ? 0 : p.getProcessorId();
    if (p != null && this.isSevereAlertCompatible()) {
      p.enableSevereAlertProcessing();
    }
    this.removeAllData = removeAllData;
    this.removeAllDataCount = removeAllDataCount;
    this.posDup = possibleDuplicate;
    this.eventId = event.getEventId();
    this.callbackArg = event.getCallbackArgument();
  }

  public RemoteRemoveAllMessage() {}


  /*
   * Sends a LocalRegion RemoteRemoveAllMessage to the recipient
   *
   * @param recipient the member to which the message is sent
   *
   * @param r the LocalRegion for which the op was performed
   *
   * @return the processor used to await acknowledgement that the message was sent, or null to
   * indicate that no acknowledgement will be sent
   *
   * @throws RemoteOperationException if the peer is no longer available
   */
  public static RemoveAllResponse send(DistributedMember recipient, EntryEventImpl event,
      RemoveAllEntryData[] removeAllData, int removeAllDataCount, boolean useOriginRemote,
      boolean possibleDuplicate) throws RemoteOperationException {
    RemoveAllResponse p = new RemoveAllResponse(event.getRegion().getSystem(), recipient);
    RemoteRemoveAllMessage msg = new RemoteRemoveAllMessage(event, recipient, p, removeAllData,
        removeAllDataCount, useOriginRemote, possibleDuplicate);
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
    return REMOTE_REMOVE_ALL_MESSAGE;
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
    this.removeAllDataCount = (int) InternalDataSerializer.readUnsignedVL(in);
    this.removeAllData = new RemoveAllEntryData[removeAllDataCount];
    if (this.removeAllDataCount > 0) {
      final Version version = InternalDataSerializer.getVersionForDataStreamOrNull(in);
      final ByteArrayDataInput bytesIn = new ByteArrayDataInput();
      for (int i = 0; i < this.removeAllDataCount; i++) {
        this.removeAllData[i] = new RemoveAllEntryData(in, this.eventId, i, version, bytesIn);
      }

      boolean hasTags = in.readBoolean();
      if (hasTags) {
        EntryVersionsList versionTags = EntryVersionsList.create(in);
        for (int i = 0; i < this.removeAllDataCount; i++) {
          this.removeAllData[i].versionTag = versionTags.get(i);
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

    InternalDataSerializer.writeUnsignedVL(this.removeAllDataCount, out);

    if (this.removeAllDataCount > 0) {

      EntryVersionsList versionTags = new EntryVersionsList(removeAllDataCount);

      boolean hasTags = false;
      for (int i = 0; i < this.removeAllDataCount; i++) {
        if (!hasTags && removeAllData[i].versionTag != null) {
          hasTags = true;
        }
        VersionTag<?> tag = removeAllData[i].versionTag;
        versionTags.add(tag);
        removeAllData[i].versionTag = null;
        this.removeAllData[i].toData(out);
        this.removeAllData[i].versionTag = tag;
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

    try {
      sendReply = doLocalRemoveAll(r, eventSender);
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
   * @return If succeeds, return true, otherwise, throw exception
   */
  public boolean doLocalRemoveAll(final LocalRegion r, final InternalDistributedMember eventSender)
      throws EntryExistsException, RemoteOperationException {
    final DistributedRegion dr = (DistributedRegion) r;

    @Released
    EntryEventImpl baseEvent = EntryEventImpl.create(r, Operation.REMOVEALL_DESTROY, null, null,
        this.callbackArg, false, eventSender, true);
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
            "RemoteRemoveAllMessage.doLocalRemoveAll: eventSender is {}, baseEvent is {}, msg is {}",
            eventSender, baseEvent, this);
      }
      final DistributedRemoveAllOperation op =
          new DistributedRemoveAllOperation(baseEvent, removeAllDataCount, false);
      try {
        final VersionedObjectList versions =
            new VersionedObjectList(removeAllDataCount, true, dr.getConcurrencyChecksEnabled());
        dr.syncBulkOp(new Runnable() {
          @SuppressWarnings("synthetic-access")
          public void run() {
            InternalDistributedMember myId = r.getDistributionManager().getDistributionManagerId();
            for (int i = 0; i < removeAllDataCount; ++i) {
              @Released
              EntryEventImpl ev = RemoveAllPRMessage.getEventFromEntry(r, myId, eventSender, i,
                  removeAllData, false, bridgeContext, posDup, false);
              try {
                ev.setRemoveAllOperation(op);
                if (logger.isDebugEnabled()) {
                  logger.debug("invoking basicDestroy with {}", ev);
                }
                try {
                  dr.basicDestroy(ev, true, null);
                } catch (EntryNotFoundException ignore) {
                }
                removeAllData[i].versionTag = ev.getVersionTag();
                versions.addKeyAndVersion(removeAllData[i].getKey(), ev.getVersionTag());
              } finally {
                ev.release();
              }
            }
          }
        }, baseEvent.getEventId());
        if (getTXUniqId() != TXManagerImpl.NOTX || dr.getConcurrencyChecksEnabled()) {
          dr.getDataView().postRemoveAll(op, versions, dr);
        }
        RemoveAllReplyMessage.send(getSender(), this.processorId,
            getReplySender(r.getDistributionManager()), versions, this.removeAllData,
            this.removeAllDataCount);
        return false;
      } finally {
        op.freeOffHeapResources();
      }
    } finally {
      baseEvent.release();
    }
  }

  @Override
  protected void appendFields(StringBuffer buff) {
    super.appendFields(buff);
    buff.append("; removeAllDataCount=").append(removeAllDataCount);
    if (this.bridgeContext != null) {
      buff.append("; bridgeContext=").append(this.bridgeContext);
    }
    for (int i = 0; i < removeAllDataCount; i++) {
      buff.append("; entry" + i + ":")
          .append(removeAllData[i] == null ? "null" : removeAllData[i].getKey());
    }
  }

  public static class RemoveAllReplyMessage extends ReplyMessage {
    private VersionedObjectList versions;

    @Override
    public boolean getInlineProcess() {
      return true;
    }

    private RemoveAllReplyMessage(int processorId, VersionedObjectList versionList,
        RemoveAllEntryData[] removeAllData, int removeAllCount) {
      super();
      this.versions = versionList;
      setProcessorId(processorId);
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId, ReplySender dm,
        VersionedObjectList versions, RemoveAllEntryData[] removeAllData, int removeAllDataCount) {
      Assert.assertTrue(recipient != null, "RemoveAllReplyMessage NULL reply message");
      RemoveAllReplyMessage m =
          new RemoveAllReplyMessage(processorId, versions, removeAllData, removeAllDataCount);
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
          logger.trace("RemoveAllReplyMessage processor not found");
        }
        return;
      }
      if (rp instanceof RemoveAllResponse) {
        RemoveAllResponse processor = (RemoveAllResponse) rp;
        processor.setResponse(this);
      }
      rp.process(this);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} Processed {}", rp, this);
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return REMOTE_REMOVE_ALL_REPLY_MESSAGE;
    }

    public RemoveAllReplyMessage() {}

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
      sb.append("RemoveAllReplyMessage ").append(" processorid=").append(this.processorId)
          .append(" returning versionTags=").append(this.versions);
      return sb.toString();
    }

  }

  /**
   * A processor to capture the value returned by {@link RemoteRemoveAllMessage}
   */
  public static class RemoveAllResponse extends RemoteOperationResponse {
    private VersionedObjectList versions;

    public RemoveAllResponse(InternalDistributedSystem ds, DistributedMember recipient) {
      super(ds, (InternalDistributedMember) recipient, false);
    }


    public void setResponse(RemoveAllReplyMessage removeAllReplyMessage) {
      if (removeAllReplyMessage.versions != null) {
        this.versions = removeAllReplyMessage.versions;
        this.versions.replaceNullIDs(removeAllReplyMessage.getSender());
      }
    }

    public VersionedObjectList getResponse() {
      return this.versions;
    }
  }
}
