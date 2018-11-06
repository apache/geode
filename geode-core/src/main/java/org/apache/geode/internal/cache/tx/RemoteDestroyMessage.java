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

import static org.apache.geode.internal.offheap.annotations.OffHeapIdentifier.ENTRY_EVENT_OLD_VALUE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheWriterException;
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
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.AbstractRegion;
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.DataLocationException;
import org.apache.geode.internal.cache.DistributedCacheOperation;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EntryEventImpl.OldValueImporter;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.DiskVersionTag;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Unretained;

/**
 * A class that specifies a destroy operation. Used by ReplicateRegions. Note: The reason for
 * different classes for Destroy and Invalidate is to prevent sending an extra bit for every
 * RemoteDestroyMessage to differentiate an invalidate versus a destroy. The assumption is that
 * these operations are used frequently, if they are not then it makes sense to fold the destroy and
 * the invalidate into the same message and use an extra bit to differentiate.
 *
 * This message is used by transactions to destroy an entry on a transaction hosted on a remote
 * member. It is also used by non-transactional region destroys that need to generate a VersionTag
 * on a remote member.
 *
 * @since GemFire 6.5
 *
 */
public class RemoteDestroyMessage extends RemoteOperationMessageWithDirectReply
    implements OldValueImporter {

  private static final Logger logger = LogService.getLogger();

  /** The key associated with the value that must be sent */
  private Object key;

  /** The callback arg */
  private Object cbArg;

  /** The operation performed on the sender */
  private Operation op;

  /**
   * An additional object providing context for the operation, e.g., for BridgeServer notification
   */
  ClientProxyMembershipID bridgeContext;

  /** event identifier */
  EventID eventId;

  /** for relayed messages, this is the original sender of the message */
  InternalDistributedMember originalSender;

  /** whether the message has old value */
  private boolean hasOldValue = false;

  /** whether old value is serialized */
  private boolean oldValueIsSerialized = false;

  private Object expectedOldValue;

  private byte[] oldValBytes;

  @Unretained(ENTRY_EVENT_OLD_VALUE)
  private transient Object oldValObj;

  boolean useOriginRemote;

  protected boolean possibleDuplicate;

  VersionTag<?> versionTag;

  // additional bitmask flags used for serialization/deserialization

  protected static final short USE_ORIGIN_REMOTE = UNRESERVED_FLAGS_START;
  protected static final short HAS_OLD_VALUE = (USE_ORIGIN_REMOTE << 1);
  protected static final short CACHE_WRITE = (HAS_OLD_VALUE << 1);
  protected static final short HAS_BRIDGE_CONTEXT = (CACHE_WRITE << 1);
  protected static final short HAS_ORIGINAL_SENDER = (HAS_BRIDGE_CONTEXT << 1);
  protected static final int HAS_VERSION_TAG = (HAS_ORIGINAL_SENDER << 1);

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public RemoteDestroyMessage() {}

  protected RemoteDestroyMessage(DistributedMember recipient, String regionPath,
      DirectReplyProcessor processor, EntryEventImpl event, Object expectedOldValue,
      boolean useOriginRemote, boolean possibleDuplicate) {
    super((InternalDistributedMember) recipient, regionPath, processor);
    this.expectedOldValue = expectedOldValue;
    this.key = event.getKey();
    this.cbArg = event.getRawCallbackArgument();
    this.op = event.getOperation();
    this.bridgeContext = event.getContext();
    this.eventId = event.getEventId();
    this.useOriginRemote = useOriginRemote;
    this.possibleDuplicate = possibleDuplicate;
    this.versionTag = event.getVersionTag();
    Assert.assertTrue(this.eventId != null);

    // added for old value if available sent over the wire for cache servers.
    if (event.hasOldValue()) {
      this.hasOldValue = true;
      event.exportOldValue(this);
    }
  }

  private void setOldValBytes(byte[] valBytes) {
    this.oldValBytes = valBytes;
  }

  private void setOldValObj(@Unretained(ENTRY_EVENT_OLD_VALUE) Object o) {
    this.oldValObj = o;
  }

  public byte[] getOldValueBytes() {
    return this.oldValBytes;
  }

  private Object getOldValObj() {
    return this.oldValObj;
  }

  protected boolean getHasOldValue() {
    return this.hasOldValue;
  }

  protected boolean getOldValueIsSerialized() {
    return this.oldValueIsSerialized;
  }

  /**
   * Set the old value for this message, only used if there are cqs registered on one of the bridge
   * servers.
   *
   * @param event underlying event.
   * @since GemFire 5.5
   */
  public void setOldValue(EntryEventImpl event) {
    if (event.hasOldValue()) {
      this.hasOldValue = true;
      CachedDeserializable cd = (CachedDeserializable) event.getSerializedOldValue();
      if (cd != null) {
        if (!cd.isSerialized()) {
          // it is a byte[]
          this.oldValueIsSerialized = false;
          setOldValBytes((byte[]) cd.getDeserializedForReading());
        } else {
          this.oldValueIsSerialized = true;
          Object o = cd.getValue();
          if (o instanceof byte[]) {
            setOldValBytes((byte[]) o);
          } else {
            // Defer serialization until toData is called.
            setOldValObj(o);
          }
        }
      } else {
        Object old = event.getRawOldValue();
        if (old instanceof byte[]) {
          this.oldValueIsSerialized = false;
          setOldValBytes((byte[]) old);
        } else {
          this.oldValueIsSerialized = true;
          setOldValObj(AbstractRegion.handleNotAvailable(old));
        }
      }
    }
  }


  @SuppressWarnings("unchecked")
  public static boolean distribute(EntryEventImpl event, Object expectedOldValue,
      boolean onlyPersistent) {
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
        RemoteDestroyReplyProcessor processor =
            send(replicate, event.getRegion(), event, expectedOldValue, false, posDup);
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

      } catch (EntryNotFoundException e) {
        throw new EntryNotFoundException("" + event.getKey());

      } catch (TransactionDataNotColocatedException enfe) {
        throw enfe;

      } catch (CancelException e) {
        event.getRegion().getCancelCriterion().checkCancelInProgress(e);

      } catch (CacheException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("RemoteDestroyMessage caught CacheException during distribution", e);
        }
        successful = true; // not a cancel-exception, so don't complain any more about it

      } catch (RegionDestroyedException | RemoteOperationException e) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE,
              "RemoteDestroyMessage caught an exception during distribution; retrying to another member",
              e);
        }
      }
    }
    return successful;
  }

  /**
   * Sends a RemoteDestroyMessage {@link org.apache.geode.cache.Region#destroy(Object)}message to
   * the recipient
   *
   * @param recipient the recipient of the message
   * @param r the ReplicateRegion for which the destroy was performed
   * @param event the event causing this message
   * @return the processor used to await the potential {@link org.apache.geode.cache.CacheException}
   */
  public static RemoteDestroyReplyProcessor send(DistributedMember recipient, InternalRegion r,
      EntryEventImpl event, Object expectedOldValue, boolean useOriginRemote,
      boolean possibleDuplicate) throws RemoteOperationException {
    RemoteDestroyReplyProcessor p =
        new RemoteDestroyReplyProcessor(r.getSystem(), recipient, false);
    p.requireResponse();
    RemoteDestroyMessage m = new RemoteDestroyMessage(recipient, r.getFullPath(), p, event,
        expectedOldValue, useOriginRemote, possibleDuplicate);
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
    @Released
    EntryEventImpl event = null;
    try {
      if (this.bridgeContext != null) {
        event = EntryEventImpl.create(r, getOperation(), getKey(), null/* newValue */,
            getCallbackArg(), false/* originRemote */, eventSender, true/* generateCallbacks */);
        event.setContext(this.bridgeContext);

        // for cq processing and client notification by BS.
        if (this.hasOldValue) {
          if (this.oldValueIsSerialized) {
            event.setSerializedOldValue(getOldValueBytes());
          } else {
            event.setOldValue(getOldValueBytes());
          }
        }
      } // bridgeContext != null
      else {
        event = EntryEventImpl.create(r, getOperation(), getKey(), null, /* newValue */
            getCallbackArg(), this.useOriginRemote, eventSender, true/* generateCallbacks */,
            false/* initializeId */);
      }

      event.setCausedByMessage(this);

      if (this.versionTag != null) {
        this.versionTag.replaceNullIDs(getSender());
        event.setVersionTag(this.versionTag);
      }
      // for cq processing and client notification by BS.
      if (this.hasOldValue) {
        if (this.oldValueIsSerialized) {
          event.setSerializedOldValue(getOldValueBytes());
        } else {
          event.setOldValue(getOldValueBytes());
        }
      }

      Assert.assertTrue(eventId != null);
      event.setEventId(eventId);

      event.setPossibleDuplicate(this.possibleDuplicate);

      try {
        r.getDataView().destroyOnRemote(event, true, this.expectedOldValue);
        sendReply(dm, event.getVersionTag());
      } catch (CacheWriterException cwe) {
        sendReply(getSender(), this.processorId, dm, new ReplyException(cwe), r, startTime);
        return false;
      } catch (EntryNotFoundException eee) {
        if (logger.isDebugEnabled()) {
          logger.debug("operateOnRegion caught EntryNotFoundException", eee);
        }
        ReplyMessage.send(getSender(), getProcessorId(), new ReplyException(eee),
            getReplySender(dm), r.isInternalRegion());
      } catch (DataLocationException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("operateOnRegion caught DataLocationException");
        }
        ReplyMessage.send(getSender(), getProcessorId(), new ReplyException(e), getReplySender(dm),
            r.isInternalRegion());
      }
      return false;
    } finally {
      if (event != null) {
        event.release();
      }
    }
  }

  public int getDSFID() {
    return R_DESTROY_MESSAGE;
  }

  private void sendReply(DistributionManager dm, VersionTag<?> versionTag) {
    DestroyReplyMessage.send(this.getSender(), getReplySender(dm), this.processorId, versionTag);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    setKey(DataSerializer.readObject(in));
    this.cbArg = DataSerializer.readObject(in);
    this.op = Operation.fromOrdinal(in.readByte());
    if ((flags & HAS_BRIDGE_CONTEXT) != 0) {
      this.bridgeContext = DataSerializer.readObject(in);
    }
    if ((flags & HAS_ORIGINAL_SENDER) != 0) {
      this.originalSender = DataSerializer.readObject(in);
    }
    this.eventId = DataSerializer.readObject(in);

    // for old values for CQs
    if (this.hasOldValue) {
      // out.writeBoolean(this.hasOldValue);
      // below boolean is not strictly required, but this is for compatibility
      in.readByte();
      setOldValBytes(DataSerializer.readByteArray(in));
    }
    this.expectedOldValue = DataSerializer.readObject(in);
    // to prevent bug 51024 always call readObject for versionTag
    // since toData always calls writeObject for versionTag.
    this.versionTag = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(getKey(), out);
    DataSerializer.writeObject(this.cbArg, out);
    out.writeByte(this.op.ordinal);
    if (this.bridgeContext != null) {
      DataSerializer.writeObject(this.bridgeContext, out);
    }
    if (this.originalSender != null) {
      DataSerializer.writeObject(this.originalSender, out);
    }
    DataSerializer.writeObject(this.eventId, out);

    // this will be on wire for cqs old value generations.
    if (this.hasOldValue) {
      out.writeByte(this.oldValueIsSerialized ? 1 : 0);
      byte policy = DistributedCacheOperation.valueIsToDeserializationPolicy(oldValueIsSerialized);
      DistributedCacheOperation.writeValue(policy, getOldValObj(), getOldValueBytes(), out);
    }
    DataSerializer.writeObject(this.expectedOldValue, out);
    DataSerializer.writeObject(this.versionTag, out);
  }

  @Override
  protected void setFlags(short flags, DataInput in) throws IOException, ClassNotFoundException {
    super.setFlags(flags, in);
    this.hasOldValue = (flags & HAS_OLD_VALUE) != 0;
    this.useOriginRemote = (flags & USE_ORIGIN_REMOTE) != 0;
    this.possibleDuplicate = (flags & POS_DUP) != 0;
  }

  @Override
  protected short computeCompressedShort() {
    short s = super.computeCompressedShort();
    // this will be on wire for cqs old value generations.
    if (this.hasOldValue)
      s |= HAS_OLD_VALUE;
    if (this.useOriginRemote)
      s |= USE_ORIGIN_REMOTE;
    if (this.possibleDuplicate)
      s |= POS_DUP;
    if (this.bridgeContext != null)
      s |= HAS_BRIDGE_CONTEXT;
    if (this.originalSender != null)
      s |= HAS_ORIGINAL_SENDER;
    if (this.versionTag != null)
      s |= HAS_VERSION_TAG;
    return s;
  }

  @Override
  public EventID getEventID() {
    return this.eventId;
  }

  /**
   * Assists the toString method in reporting the contents of this message
   *
   */
  @Override
  protected void appendFields(StringBuffer buff) {
    super.appendFields(buff);
    buff.append("; key=").append(getKey());
    if (originalSender != null) {
      buff.append("; originalSender=").append(originalSender);
    }
    if (bridgeContext != null) {
      buff.append("; bridgeContext=").append(bridgeContext);
    }
    if (eventId != null) {
      buff.append("; eventId=").append(eventId);
    }
    buff.append("; hasOldValue= ").append(this.hasOldValue);
  }

  protected Object getKey() {
    return this.key;
  }

  private void setKey(Object key) {
    this.key = key;
  }

  public Operation getOperation() {
    return this.op;
  }

  protected Object getCallbackArg() {
    return this.cbArg;
  }

  @Override
  public boolean prefersOldSerialized() {
    return true;
  }

  @Override
  public boolean isUnretainedOldReferenceOk() {
    return true;
  }

  @Override
  public boolean isCachedDeserializableValueOk() {
    return false;
  }

  private void setOldValueIsSerialized(boolean isSerialized) {
    if (isSerialized) {
      // Defer serialization until toData is called.
      this.oldValueIsSerialized = true; // VALUE_IS_SERIALIZED_OBJECT;
    } else {
      this.oldValueIsSerialized = false; // VALUE_IS_BYTES;
    }
  }

  @Override
  public void importOldObject(@Unretained(ENTRY_EVENT_OLD_VALUE) Object ov, boolean isSerialized) {
    setOldValueIsSerialized(isSerialized);
    // Defer serialization until toData is called.
    setOldValObj(ov);
  }

  @Override
  public void importOldBytes(byte[] ov, boolean isSerialized) {
    setOldValueIsSerialized(isSerialized);
    setOldValBytes(ov);
  }

  public static class DestroyReplyMessage extends ReplyMessage {

    private static final byte HAS_VERSION = 0x01;
    private static final byte PERSISTENT = 0x02;

    private VersionTag<?> versionTag;

    /** DSFIDFactory constructor */
    public DestroyReplyMessage() {}

    static void send(InternalDistributedMember recipient, ReplySender dm, int procId,
        VersionTag<?> versionTag) {
      Assert.assertTrue(recipient != null, "DestroyReplyMessage NULL recipient");
      DestroyReplyMessage m = new DestroyReplyMessage(recipient, procId, versionTag);
      dm.putOutgoing(m);
    }

    DestroyReplyMessage(InternalDistributedMember recipient, int procId, VersionTag<?> versionTag) {
      this.setProcessorId(procId);
      this.setRecipient(recipient);
      this.versionTag = versionTag;
    }

    @Override
    public boolean getInlineProcess() {
      return true;
    }

    @Override
    public int getDSFID() {
      return R_DESTROY_REPLY_MESSAGE;
    }

    @Override
    public void process(final DistributionManager dm, final ReplyProcessor21 rp) {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE,
            "DestroyReplyMessage process invoking reply processor with processorId:{}",
            this.processorId);
      }
      if (rp == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "DestroyReplyMessage processor not found");
        }
        return;
      }
      if (this.versionTag != null) {
        this.versionTag.replaceNullIDs(getSender());
      }
      if (rp instanceof RemoteDestroyReplyProcessor) {
        RemoteDestroyReplyProcessor processor = (RemoteDestroyReplyProcessor) rp;
        processor.setResponse(this.versionTag);
      }
      rp.process(this);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} processed {}", rp, this);
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
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
      StringBuilder sb = super.getStringBuilder();
      sb.append(getShortClassName());
      sb.append(" processorId=");
      sb.append(this.processorId);
      if (this.versionTag != null) {
        sb.append(" version=").append(this.versionTag);
      }
      sb.append(" from ");
      sb.append(this.getSender());
      ReplyException ex = getException();
      if (ex != null) {
        sb.append(" with exception ");
        sb.append(ex);
      }
      return sb.toString();
    }

  }
  static class RemoteDestroyReplyProcessor extends RemoteOperationResponse {
    VersionTag<?> versionTag;

    RemoteDestroyReplyProcessor(InternalDistributedSystem ds, DistributedMember recipient,
        Object key) {
      super(ds, (InternalDistributedMember) recipient, false);
    }

    void setResponse(VersionTag<?> versionTag) {
      this.versionTag = versionTag;
    }

    VersionTag<?> getVersionTag() {
      return this.versionTag;
    }
  }
}
