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
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.EntryNotFoundException;
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
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.cache.versions.DiskVersionTag;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.annotations.Released;

/**
 * This message is used by transactions to invalidate an entry on a transaction hosted on a remote
 * member. It is also used by non-transactional region invalidates that need to generate a
 * VersionTag on a remote member.
 */
public class RemoteInvalidateMessage extends RemoteDestroyMessage {

  private static final Logger logger = LogService.getLogger();

  /**
   * Empty constructor to satisfy {@link org.apache.geode.DataSerializer} requirements
   */
  public RemoteInvalidateMessage() {}

  private RemoteInvalidateMessage(DistributedMember recipient, String regionPath,
      DirectReplyProcessor processor, EntryEventImpl event, boolean useOriginRemote,
      boolean possibleDuplicate) {
    super(recipient, regionPath, processor, event, null, useOriginRemote, possibleDuplicate);
  }

  @SuppressWarnings("unchecked")
  public static boolean distribute(EntryEventImpl event, boolean onlyPersistent) {
    boolean successful = false;
    DistributedRegion r = (DistributedRegion) event.getRegion();
    Collection<InternalDistributedMember> replicates = onlyPersistent
        ? r.getCacheDistributionAdvisor().adviseInitializedPersistentMembers().keySet()
        : r.getCacheDistributionAdvisor().adviseInitializedReplicates();
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
        InvalidateResponse processor = send(replicate, event.getRegion(), event, false, posDup);
        processor.waitForRemoteResponse();
        VersionTag<?> versionTag = processor.getVersionTag();
        if (versionTag != null) {
          event.setVersionTag(versionTag);
          if (event.getRegion().getVersionVector() != null) {
            event.getRegion().getVersionVector().recordVersion(versionTag.getMemberID(),
                versionTag);
          }
        }
        event.setInhibitDistribution(true);
        return true;

      } catch (TransactionDataNotColocatedException enfe) {
        throw enfe;

      } catch (CancelException e) {
        event.getRegion().getCancelCriterion().checkCancelInProgress(e);

      } catch (EntryNotFoundException e) {
        throw new EntryNotFoundException("" + event.getKey());

      } catch (CacheException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("RemoteInvalidateMessage caught CacheException during distribution", e);
        }
        successful = true; // not a cancel-exception, so don't complain any more about it

      } catch (RegionDestroyedException | RemoteOperationException e) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE,
              "RemoteInvalidateMessage caught an exception during distribution; retrying to another member",
              e);
        }
      }
    }
    return successful;
  }

  /**
   * Sends a transactional RemoteInvalidateMessage
   * {@link org.apache.geode.cache.Region#invalidate(Object)}message to the recipient
   *
   * @param recipient the recipient of the message
   * @param r the ReplicateRegion for which the invalidate was performed
   * @param event the event causing this message
   * @param useOriginRemote whether the receiver should use originRemote=true in its event
   * @return the InvalidateResponse processor used to await the potential
   *         {@link org.apache.geode.cache.CacheException}
   */
  public static InvalidateResponse send(DistributedMember recipient, InternalRegion r,
      EntryEventImpl event, boolean useOriginRemote, boolean possibleDuplicate)
      throws RemoteOperationException {
    InvalidateResponse p = new InvalidateResponse(r.getSystem(), recipient, event.getKey());
    RemoteInvalidateMessage m = new RemoteInvalidateMessage(recipient, r.getFullPath(), p, event,
        useOriginRemote, possibleDuplicate);
    Set<?> failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(
          String.format("Failed sending < %s >", m));
    }
    return p;
  }

  /**
   * This method is called upon receipt and make the desired changes to the region. Note: It is very
   * important that this message does NOT cause any deadlocks as the sender will wait indefinitely
   * for the acknowledgement
   */
  @Override
  protected boolean operateOnRegion(ClusterDistributionManager dm, LocalRegion r, long startTime)
      throws EntryExistsException, RemoteOperationException {

    InternalDistributedMember eventSender = originalSender;
    if (eventSender == null) {
      eventSender = getSender();
    }
    final Object key = getKey();
    @Released
    final EntryEventImpl event = EntryEventImpl.create(r, getOperation(), key, null, /* newValue */
        getCallbackArg(),
        this.useOriginRemote/* originRemote - false to force distribution in buckets */,
        eventSender, true/* generateCallbacks */, false/* initializeId */);
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
      if (eventShouldHaveOldValue) {
        if (getOldValueIsSerialized()) {
          event.setSerializedOldValue(getOldValueBytes());
        } else {
          event.setOldValue(getOldValueBytes());
        }
      }
      boolean sendReply = true;
      try {
        r.checkReadiness();
        r.checkForLimitedOrNoAccess();
        r.basicInvalidate(event);
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "remoteInvalidated key: {}", key);
        }
        sendReply(getSender(), this.processorId, dm, /* ex */null, event.getRegion(),
            event.getVersionTag(), startTime);
        sendReply = false;
      } catch (EntryNotFoundException eee) {
        // failed = true;
        if (logger.isDebugEnabled()) {
          logger.debug("operateOnRegion caught EntryNotFoundException");
        }
        sendReply(getSender(), getProcessorId(), dm, new ReplyException(eee), r, null, startTime);
        sendReply = false; // this prevents us from acking later
      }

      return sendReply;
    } finally {
      event.release();
    }
  }

  @Override
  protected void sendReply(InternalDistributedMember member, int procId, DistributionManager dm,
      ReplyException ex, InternalRegion r, long startTime) {
    sendReply(member, procId, dm, ex, r, null, startTime);
  }

  protected void sendReply(InternalDistributedMember member, int procId, DistributionManager dm,
      ReplyException ex, InternalRegion r, VersionTag<?> versionTag, long startTime) {
    InvalidateReplyMessage.send(member, procId, getReplySender(dm), versionTag, ex);
  }

  @Override
  public int getDSFID() {
    return R_INVALIDATE_MESSAGE;
  }


  public static class InvalidateReplyMessage extends ReplyMessage {
    private VersionTag<?> versionTag;

    private static final byte HAS_VERSION = 0x01;
    private static final byte PERSISTENT = 0x02;

    /**
     * DSFIDFactory constructor
     */
    public InvalidateReplyMessage() {}

    private InvalidateReplyMessage(int processorId, VersionTag<?> versionTag, ReplyException ex) {
      super();
      setProcessorId(processorId);
      this.versionTag = versionTag;
      setException(ex);
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId,
        ReplySender replySender, VersionTag<?> versionTag, ReplyException ex) {
      Assert.assertTrue(recipient != null, "InvalidateReplyMessage NULL reply message");
      InvalidateReplyMessage m = new InvalidateReplyMessage(processorId, versionTag, ex);
      m.setRecipient(recipient);
      replySender.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the message.
     *
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DistributionManager dm, final ReplyProcessor21 rp) {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE,
            "InvalidateReplyMessage process invoking reply processor with processorId:{}",
            this.processorId);
      }

      if (rp == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "InvalidateReplyMessage processor not found");
        }
        return;
      }
      if (this.versionTag != null) {
        this.versionTag.replaceNullIDs(getSender());
      }
      if (rp instanceof InvalidateResponse) {
        InvalidateResponse processor = (InvalidateResponse) rp;
        processor.setResponse(this.versionTag);
      }
      rp.process(this);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} processed {}", rp, this);
      }

      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return R_INVALIDATE_REPLY_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      byte b = 0;
      if (this.versionTag != null) {
        b |= HAS_VERSION;
      }
      if (this.versionTag instanceof DiskVersionTag) {
        b |= PERSISTENT;
      }
      out.writeByte(b);
      if (this.versionTag != null) {
        InternalDataSerializer.invokeToData(this.versionTag, out);
      }
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
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
      sb.append("InvalidateReplyMessage ").append("processorid=").append(this.processorId)
          .append(" exception=").append(getException());
      if (this.versionTag != null) {
        sb.append("version=").append(this.versionTag);
      }
      return sb.toString();
    }

  }
  /**
   * A processor to capture the value returned by {@link RemoteInvalidateMessage}
   *
   * @since GemFire 6.5
   */
  public static class InvalidateResponse extends RemoteOperationResponse {
    private volatile boolean returnValueReceived;
    final Object key;
    VersionTag<?> versionTag;

    public InvalidateResponse(InternalDistributedSystem ds, DistributedMember recipient,
        Object key) {
      super(ds, (InternalDistributedMember) recipient, true);
      this.key = key;
    }

    public void setResponse(VersionTag<?> versionTag) {
      this.returnValueReceived = true;
      this.versionTag = versionTag;
    }

    /**
     * @throws CacheException if the peer generates an error
     */
    public void waitForResult() throws CacheException, RemoteOperationException {
      waitForRemoteResponse();
      if (!this.returnValueReceived) {
        throw new RemoteOperationException(
            "no response code received");
      }
      return;
    }

    public VersionTag<?> getVersionTag() {
      return this.versionTag;
    }
  }
}
