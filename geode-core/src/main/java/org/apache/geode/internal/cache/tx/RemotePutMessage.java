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

import static org.apache.geode.internal.offheap.annotations.OffHeapIdentifier.ENTRY_EVENT_NEW_VALUE;
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
import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.DistributedCacheOperation;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EntryEventImpl.NewValueImporter;
import org.apache.geode.internal.cache.EntryEventImpl.OldValueImporter;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.cache.VMCachedDeserializable;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.versions.DiskVersionTag;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.StoredObject;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Unretained;

/**
 * This message is used by transactions to update an entry on a transaction hosted on a remote
 * member. It is also used by non-transactional region updates that need to generate a VersionTag on
 * a remote member.
 *
 * @since GemFire 6.5
 */
public class RemotePutMessage extends RemoteOperationMessageWithDirectReply
    implements NewValueImporter, OldValueImporter {
  private static final Logger logger = LogService.getLogger();

  /** The key associated with the value that must be sent */
  private Object key;

  /** The value associated with the key that must be sent */
  private byte[] valBytes;

  private byte[] oldValBytes;

  /**
   * Used on sender side only to defer serialization until toData is called.
   */
  @Unretained(ENTRY_EVENT_NEW_VALUE)
  private transient Object valObj;

  @Unretained(ENTRY_EVENT_OLD_VALUE)
  private transient Object oldValObj;

  /** The callback arg of the operation */
  private Object cbArg;

  /** The time stamp when the value was created */
  protected long lastModified;

  /** The operation performed on the sender */
  private Operation op;

  /**
   * An additional object providing context for the operation, e.g., for BridgeServer notification
   */
  ClientProxyMembershipID bridgeContext;

  /** event identifier */
  EventID eventId;

  /**
   * for relayed messages, this is the sender of the original message. It should be used in
   * constructing events for listener notification.
   */
  InternalDistributedMember originalSender;

  /**
   * Indicates if and when the new value should be deserialized on the the receiver. Distinguishes
   * between a non-byte[] value that was serialized (DESERIALIZATION_POLICY_LAZY) and a byte[] array
   * value that didn't need to be serialized (DESERIALIZATION_POLICY_NONE). While this seems like an
   * extra data, it isn't, because serializing a byte[] causes the type (a byte) to be written in
   * the stream, AND what's better is that handling this distinction at this level reduces
   * processing for values that are byte[].
   */
  protected byte deserializationPolicy = DistributedCacheOperation.DESERIALIZATION_POLICY_NONE;

  protected boolean oldValueIsSerialized = false;

  /**
   * whether it's okay to create a new key
   */
  private boolean ifNew;

  /**
   * whether it's okay to update an existing key
   */
  private boolean ifOld;

  /**
   * Whether an old value is required in the response
   */
  private boolean requireOldValue;

  /**
   * For put to happen, the old value must be equal to this expectedOldValue.
   *
   * @see PartitionedRegion#replace(Object, Object, Object)
   */
  private Object expectedOldValue;

  private VersionTag<?> versionTag;

  private transient InternalDistributedSystem internalDs;

  /**
   * state from operateOnRegion that must be preserved for transmission from the waiting pool
   */
  transient boolean result = false;

  private boolean hasOldValue = false;

  /** whether value has delta **/
  private boolean hasDelta = false;

  /** whether new value is formed by applying delta **/
  private transient boolean applyDeltaBytes = false;

  /** delta bytes read in fromData that will be used in operate() */
  private transient byte[] deltaBytes;

  /** whether to send delta or full value **/
  private transient boolean sendDelta = false;

  private EntryEventImpl event = null;

  private boolean useOriginRemote;

  private boolean possibleDuplicate;

  protected static final short IF_NEW = UNRESERVED_FLAGS_START;
  protected static final short IF_OLD = (IF_NEW << 1);
  protected static final short REQUIRED_OLD_VAL = (IF_OLD << 1);
  protected static final short HAS_OLD_VAL = (REQUIRED_OLD_VAL << 1);
  protected static final short HAS_DELTA_BYTES = (HAS_OLD_VAL << 1);
  protected static final short USE_ORIGIN_REMOTE = (HAS_DELTA_BYTES << 1);
  protected static final short CACHE_WRITE = (USE_ORIGIN_REMOTE << 1);
  protected static final short HAS_EXPECTED_OLD_VAL = (CACHE_WRITE << 1);

  // below flags go into deserializationPolicy
  protected static final int HAS_BRIDGE_CONTEXT =
      getNextByteMask(DistributedCacheOperation.DESERIALIZATION_POLICY_END);
  protected static final int HAS_ORIGINAL_SENDER = getNextByteMask(HAS_BRIDGE_CONTEXT);
  protected static final int HAS_VERSION_TAG = getNextByteMask(HAS_ORIGINAL_SENDER);
  protected static final int HAS_CALLBACKARG = getNextByteMask(HAS_VERSION_TAG);

  /**
   * Empty constructor to satisfy {@link DataSerializer}requirements
   */
  public RemotePutMessage() {}

  private RemotePutMessage(DistributedMember recipient, String regionPath,
      DirectReplyProcessor processor, EntryEventImpl event, final long lastModified, boolean ifNew,
      boolean ifOld, Object expectedOldValue, boolean requireOldValue, boolean useOriginRemote,
      boolean possibleDuplicate) {
    super((InternalDistributedMember) recipient, regionPath, processor);
    this.processor = processor;
    this.requireOldValue = requireOldValue;
    this.expectedOldValue = expectedOldValue;
    this.useOriginRemote = useOriginRemote;
    this.key = event.getKey();
    this.possibleDuplicate = possibleDuplicate;

    // useOriginRemote is true for TX single hops only as of now.
    event.setOriginRemote(useOriginRemote);

    if (event.hasNewValue()) {
      this.deserializationPolicy = DistributedCacheOperation.DESERIALIZATION_POLICY_LAZY;
      event.exportNewValue(this);
    } else {
      // assert that if !event.hasNewValue, then deserialization policy is NONE
      assert this.deserializationPolicy == DistributedCacheOperation.DESERIALIZATION_POLICY_NONE : this.deserializationPolicy;
    }

    // added for cqs on cache servers. rdubey


    if (event.hasOldValue()) {
      this.hasOldValue = true;
      event.exportOldValue(this);
    }

    this.event = event;

    this.cbArg = event.getRawCallbackArgument();
    this.lastModified = lastModified;
    this.op = event.getOperation();
    this.bridgeContext = event.getContext();
    this.eventId = event.getEventId();
    Assert.assertTrue(this.eventId != null);
    this.ifNew = ifNew;
    this.ifOld = ifOld;
    this.versionTag = event.getVersionTag();
  }

  /**
   * this is similar to send() but it selects an initialized replicate that is used to proxy the
   * message
   *
   * @param event represents the current operation
   * @param lastModified lastModified time
   * @param ifNew whether a new entry can be created
   * @param ifOld whether an old entry can be used (updates are okay)
   * @param expectedOldValue the value being overwritten is required to match this value
   * @param requireOldValue whether the old value should be returned
   * @param onlyPersistent send message to persistent members only
   * @return whether the message was successfully distributed to another member
   */
  @SuppressWarnings("unchecked")
  public static boolean distribute(EntryEventImpl event, long lastModified, boolean ifNew,
      boolean ifOld, Object expectedOldValue, boolean requireOldValue, boolean onlyPersistent) {
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
    if (logger.isDebugEnabled()) {
      logger.debug("performing remote put messaging for {}", event);
    }
    for (InternalDistributedMember replicate : replicates) {
      try {
        attempts++;
        final boolean posDup = (attempts > 1);
        RemotePutResponse response = send(replicate, event.getRegion(), event, lastModified, ifNew,
            ifOld, expectedOldValue, requireOldValue, false, posDup);
        PutResult result = response.waitForResult();
        event.setOldValue(result.oldValue, true/* force */);
        event.setOperation(result.op);
        if (result.versionTag != null) {
          event.setVersionTag(result.versionTag);
          if (event.getRegion().getVersionVector() != null) {
            event.getRegion().getVersionVector().recordVersion(result.versionTag.getMemberID(),
                result.versionTag);
          }
        }
        event.setInhibitDistribution(true);
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

      } catch (RegionDestroyedException | RemoteOperationException e) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE,
              "RemotePutMessage caught an exception during distribution; retrying to another member",
              e);
        }
      }
    }
    return successful;
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  /**
   * Sends a ReplicateRegion {@link org.apache.geode.cache.Region#put(Object, Object)} message to
   * the recipient
   *
   * @param recipient the member to which the put message is sent
   * @param r the PartitionedRegion for which the put was performed
   * @param event the event prompting this message
   * @param ifNew whether a new entry must be created
   * @param ifOld whether an old entry must be updated (no creates)
   * @return the processor used to await acknowledgement that the update was sent, or null to
   *         indicate that no acknowledgement will be sent
   * @throws RemoteOperationException if the peer is no longer available
   */
  public static RemotePutResponse txSend(DistributedMember recipient, InternalRegion r,
      EntryEventImpl event, final long lastModified, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue) throws RemoteOperationException {
    return send(recipient, r, event, lastModified, ifNew, ifOld, expectedOldValue, requireOldValue,
        true, false);
  }

  /**
   * Sends a ReplicateRegion {@link org.apache.geode.cache.Region#put(Object, Object)} message to
   * the recipient
   *
   * @param recipient the member to which the put message is sent
   * @param r the region for which the put was performed
   * @param event the event prompting this message
   * @param ifNew whether a new entry must be created
   * @param ifOld whether an old entry must be updated (no creates)
   * @param useOriginRemote whether the receiver should consider the event local or remote
   * @return the processor used to await acknowledgement that the update was sent, or null to
   *         indicate that no acknowledgement will be sent
   * @throws RemoteOperationException if the peer is no longer available
   */
  public static RemotePutResponse send(DistributedMember recipient, InternalRegion r,
      EntryEventImpl event, final long lastModified, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, boolean useOriginRemote,
      boolean possibleDuplicate) throws RemoteOperationException {

    RemotePutResponse processor = new RemotePutResponse(r.getSystem(), recipient, false);

    RemotePutMessage m =
        new RemotePutMessage(recipient, r.getFullPath(), processor, event, lastModified, ifNew,
            ifOld, expectedOldValue, requireOldValue, useOriginRemote, possibleDuplicate);
    m.setInternalDs(r.getSystem());
    m.setSendDelta(true);

    processor.setRemotePutMessage(m);

    Set<?> failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(
          String.format("Failed sending < %s >", m));
    }
    return processor;
  }

  public Object getKey() {
    return this.key;
  }

  public void setKey(Object key) {
    this.key = key;
  }

  public byte[] getValBytes() {
    return this.valBytes;
  }

  public byte[] getOldValueBytes() {

    return this.oldValBytes;
  }

  private void setValBytes(byte[] valBytes) {
    this.valBytes = valBytes;
  }

  private void setOldValBytes(byte[] valBytes) {
    this.oldValBytes = valBytes;
  }

  private Object getOldValObj() {
    return this.oldValObj;
  }

  private void setValObj(@Unretained(ENTRY_EVENT_NEW_VALUE) Object o) {
    this.valObj = o;
  }

  private void setOldValObj(@Unretained(ENTRY_EVENT_OLD_VALUE) Object o) {
    this.oldValObj = o;
  }

  public Object getCallbackArg() {
    return this.cbArg;
  }

  protected Operation getOperation() {
    return this.op;
  }

  @Override
  public void setOperation(Operation operation) {
    this.op = operation;
  }

  /**
   * sets the instance variable hasOldValue to the giving boolean value.
   */
  @Override
  public void setHasOldValue(final boolean value) {
    this.hasOldValue = value;
  }

  public int getDSFID() {
    return R_PUT_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    setKey(DataSerializer.readObject(in));

    final int extraFlags = in.readUnsignedByte();
    this.deserializationPolicy =
        (byte) (extraFlags & DistributedCacheOperation.DESERIALIZATION_POLICY_MASK);
    this.cbArg = DataSerializer.readObject(in);
    this.lastModified = in.readLong();
    this.op = Operation.fromOrdinal(in.readByte());
    if ((extraFlags & HAS_BRIDGE_CONTEXT) != 0) {
      this.bridgeContext = DataSerializer.readObject(in);
    }
    if ((extraFlags & HAS_ORIGINAL_SENDER) != 0) {
      this.originalSender = DataSerializer.readObject(in);
    }
    this.eventId = new EventID();
    InternalDataSerializer.invokeFromData(this.eventId, in);

    if ((flags & HAS_EXPECTED_OLD_VAL) != 0) {
      this.expectedOldValue = DataSerializer.readObject(in);
    }

    if (this.hasOldValue) {
      this.oldValueIsSerialized = (in.readByte() == 1);
      setOldValBytes(DataSerializer.readByteArray(in));
    }
    setValBytes(DataSerializer.readByteArray(in));
    if ((flags & HAS_DELTA_BYTES) != 0) {
      this.applyDeltaBytes = true;
      this.deltaBytes = DataSerializer.readByteArray(in);
    }
    if ((extraFlags & HAS_VERSION_TAG) != 0) {
      this.versionTag = DataSerializer.readObject(in);
    }
  }

  @Override
  protected void setFlags(short flags, DataInput in) throws IOException, ClassNotFoundException {
    super.setFlags(flags, in);
    this.ifNew = (flags & IF_NEW) != 0;
    this.ifOld = (flags & IF_OLD) != 0;
    this.requireOldValue = (flags & REQUIRED_OLD_VAL) != 0;
    this.hasOldValue = (flags & HAS_OLD_VAL) != 0;
    this.useOriginRemote = (flags & USE_ORIGIN_REMOTE) != 0;
  }

  @Override
  public EventID getEventID() {
    return this.eventId;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    this.hasDelta = false;
    super.toData(out);
    DataSerializer.writeObject(getKey(), out);

    int extraFlags = this.deserializationPolicy;
    if (this.bridgeContext != null)
      extraFlags |= HAS_BRIDGE_CONTEXT;
    if (this.originalSender != null)
      extraFlags |= HAS_ORIGINAL_SENDER;
    if (this.versionTag != null)
      extraFlags |= HAS_VERSION_TAG;
    out.writeByte(extraFlags);

    DataSerializer.writeObject(getCallbackArg(), out);
    out.writeLong(this.lastModified);
    out.writeByte(this.op.ordinal);
    if (this.bridgeContext != null) {
      DataSerializer.writeObject(this.bridgeContext, out);
    }
    if (this.originalSender != null) {
      DataSerializer.writeObject(this.originalSender, out);
    }
    InternalDataSerializer.invokeToData(this.eventId, out);

    if (this.expectedOldValue != null) {
      DataSerializer.writeObject(this.expectedOldValue, out);
    }
    // this will be on wire for cqs old value generations.
    if (this.hasOldValue) {
      out.writeByte(this.oldValueIsSerialized ? 1 : 0);
      byte policy = DistributedCacheOperation.valueIsToDeserializationPolicy(oldValueIsSerialized);
      DistributedCacheOperation.writeValue(policy, getOldValObj(), getOldValueBytes(), out);
    }
    DistributedCacheOperation.writeValue(this.deserializationPolicy, this.valObj, getValBytes(),
        out);
    if (this.event.getDeltaBytes() != null) {
      DataSerializer.writeByteArray(this.event.getDeltaBytes(), out);
    }
    if (this.versionTag != null) {
      DataSerializer.writeObject(this.versionTag, out);
    }
  }

  @Override
  protected short computeCompressedShort() {
    short s = super.computeCompressedShort();
    if (this.ifNew)
      s |= IF_NEW;
    if (this.ifOld)
      s |= IF_OLD;
    if (this.requireOldValue)
      s |= REQUIRED_OLD_VAL;
    if (this.hasOldValue)
      s |= HAS_OLD_VAL;
    if (this.event.getDeltaBytes() != null)
      s |= HAS_DELTA_BYTES;
    if (this.expectedOldValue != null)
      s |= HAS_EXPECTED_OLD_VAL;
    if (this.useOriginRemote)
      s |= USE_ORIGIN_REMOTE;
    if (this.possibleDuplicate)
      s |= POS_DUP;
    return s;
  }

  /**
   * This method is called upon receipt and make the desired changes to the Replicate Region. Note:
   * It is very important that this message does NOT cause any deadlocks as the sender will wait
   * indefinitely for the acknowledgement
   */
  @Override
  protected boolean operateOnRegion(ClusterDistributionManager dm, LocalRegion r, long startTime)
      throws EntryExistsException, RemoteOperationException {
    this.setInternalDs(r.getSystem());// set the internal DS. Required to
                                      // checked DS level delta-enabled property
                                      // while sending delta
    boolean sendReply = true;

    InternalDistributedMember eventSender = originalSender;
    if (eventSender == null) {
      eventSender = getSender();
    }
    @Released
    EntryEventImpl eei = EntryEventImpl.create(r, getOperation(), getKey(), null, /* newValue */
        getCallbackArg(),
        useOriginRemote, /* originRemote - false to force distribution in buckets */
        eventSender, true/* generateCallbacks */, false/* initializeId */);
    this.event = eei;
    try {
      if (this.versionTag != null) {
        this.versionTag.replaceNullIDs(getSender());
        event.setVersionTag(this.versionTag);
      }
      this.event.setCausedByMessage(this);

      event.setPossibleDuplicate(this.possibleDuplicate);
      if (this.bridgeContext != null) {
        event.setContext(this.bridgeContext);
      }

      Assert.assertTrue(eventId != null);
      event.setEventId(eventId);

      // added for cq procesing
      if (this.hasOldValue) {
        if (this.oldValueIsSerialized) {
          event.setSerializedOldValue(getOldValueBytes());
        } else {
          event.setOldValue(getOldValueBytes());
        }
      }

      if (this.applyDeltaBytes) {
        event.setNewValue(this.valObj);
        event.setDeltaBytes(this.deltaBytes);
      } else {
        switch (this.deserializationPolicy) {
          case DistributedCacheOperation.DESERIALIZATION_POLICY_LAZY:
            event.setSerializedNewValue(getValBytes());
            break;
          case DistributedCacheOperation.DESERIALIZATION_POLICY_NONE:
            event.setNewValue(getValBytes());
            break;
          default:
            throw new AssertionError("unknown deserialization policy: " + deserializationPolicy);
        }
      }

      try {
        result = r.getDataView().putEntry(event, this.ifNew, this.ifOld, this.expectedOldValue,
            this.requireOldValue, this.lastModified, true);

        if (!this.result) { // make sure the region hasn't gone away
          r.checkReadiness();
          if (!this.ifNew && !this.ifOld) {
            // no reason to be throwing an exception, so let's retry
            RemoteOperationException ex = new RemoteOperationException(
                "unable to perform put, but operation should not fail");
            sendReply(getSender(), getProcessorId(), dm, new ReplyException(ex), r, startTime);
          }
        }
      } catch (CacheWriterException cwe) {
        sendReply(getSender(), getProcessorId(), dm, new ReplyException(cwe), r, startTime);
        return false;
      }

      setOperation(event.getOperation()); // set operation for reply message

      if (sendReply) {
        sendReply(getSender(), getProcessorId(), dm, null, r, startTime, event);
      }
      return false;
    } finally {
      this.event.release(); // OFFHEAP this may be too soon to make this call
    }
  }


  protected void sendReply(InternalDistributedMember member, int procId, DistributionManager dm,
      ReplyException ex, LocalRegion pr, long startTime, EntryEventImpl event) {
    PutReplyMessage.send(member, procId, getReplySender(dm), result, getOperation(), ex, this,
        event);
  }

  @Override
  protected void sendReply(InternalDistributedMember member, int procId, DistributionManager dm,
      ReplyException ex, InternalRegion r, long startTime) {
    PutReplyMessage.send(member, procId, getReplySender(dm), result, getOperation(), ex, this,
        null);
  }

  @Override
  protected void appendFields(StringBuffer buff) {
    super.appendFields(buff);
    buff.append("; key=").append(getKey()).append("; value=");
    // buff.append(getValBytes());
    buff.append(getValBytes() == null ? this.valObj : "(" + getValBytes().length + " bytes)");
    buff.append("; callback=").append(this.cbArg).append("; op=").append(this.op);
    if (this.originalSender != null) {
      buff.append("; originalSender=").append(originalSender);
    }
    if (this.bridgeContext != null) {
      buff.append("; bridgeContext=").append(this.bridgeContext);
    }
    if (this.eventId != null) {
      buff.append("; eventId=").append(this.eventId);
    }
    buff.append("; ifOld=").append(this.ifOld).append("; ifNew=").append(this.ifNew).append("; op=")
        .append(this.getOperation());
    buff.append("; hadOldValue=").append(this.hasOldValue);
    if (this.hasOldValue) {
      byte[] ov = getOldValueBytes();
      if (ov != null) {
        buff.append("; oldValueLength=").append(ov.length);
      }
    }
    buff.append("; deserializationPolicy=");
    buff.append(
        DistributedCacheOperation.deserializationPolicyToString(this.deserializationPolicy));
    buff.append("; hasDelta=");
    buff.append(this.hasDelta);
    buff.append("; sendDelta=");
    buff.append(this.sendDelta);
    buff.append("; isDeltaApplied=");
    buff.append(this.applyDeltaBytes);
  }

  public InternalDistributedSystem getInternalDs() {
    return internalDs;
  }

  public void setInternalDs(InternalDistributedSystem internalDs) {
    this.internalDs = internalDs;
  }

  public static class PutReplyMessage extends ReplyMessage implements OldValueImporter {

    static final byte FLAG_RESULT = 0x01;
    static final byte FLAG_HASVERSION = 0x02;
    static final byte FLAG_PERSISTENT = 0x04;

    /** Result of the Put operation */
    boolean result;

    /** The Operation actually performed */
    Operation op;

    /**
     * Set to true by the import methods if the oldValue is already serialized. In that case toData
     * should just copy the bytes to the stream. In either case fromData just calls readObject.
     */
    private transient boolean oldValueIsSerialized;

    /**
     * Old value in serialized form: either a byte[] or CachedDeserializable, or null if not set.
     */
    @Unretained(ENTRY_EVENT_OLD_VALUE)
    private Object oldValue;

    /**
     * version tag for concurrency control
     */
    VersionTag<?> versionTag;

    @Override
    public boolean getInlineProcess() {
      return true;
    }

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public PutReplyMessage() {}

    // unit tests may call this constructor
    PutReplyMessage(int processorId, boolean result, Operation op, ReplyException ex,
        Object oldValue, VersionTag<?> versionTag) {
      super();
      this.op = op;
      this.result = result;
      setProcessorId(processorId);
      setException(ex);
      this.oldValue = oldValue;
      this.versionTag = versionTag;
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId, ReplySender dm,
        boolean result, Operation op, ReplyException ex, RemotePutMessage sourceMessage,
        EntryEventImpl event) {
      Assert.assertTrue(recipient != null, "PutReplyMessage NULL recipient");
      PutReplyMessage m = new PutReplyMessage(processorId, result, op, ex, null,
          event != null ? event.getVersionTag() : null);

      if (sourceMessage.requireOldValue && event != null) {
        event.exportOldValue(m);
      }

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
      if (logger.isDebugEnabled()) {
        logger.debug("Processing {}", this);
      }
      if (rp == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("PutReplyMessage processor not found");
        }
        return;
      }
      if (this.versionTag != null) {
        this.versionTag.replaceNullIDs(getSender());
      }
      if (rp instanceof RemotePutResponse) {
        RemotePutResponse processor = (RemotePutResponse) rp;
        processor.setResponse(this);
      }
      rp.process(this);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} processed {}", rp, this);
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    /**
     * Return oldValue as a byte[] or as a CachedDeserializable. This method used to deserialize a
     * CachedDeserializable but that is too soon. This method is called during message processing.
     * The deserialization needs to be deferred until we get back to the application thread which
     * happens for this oldValue when they call EntryEventImpl.getOldValue.
     */
    public Object getOldValue() {
      // oldValue field is in serialized form, either a CachedDeserializable,
      // a byte[], or null if not set
      return this.oldValue;
    }

    @Override
    public int getDSFID() {
      return R_PUT_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      byte flags = (byte) (in.readByte() & 0xff);
      this.result = (flags & FLAG_RESULT) != 0;
      this.op = Operation.fromOrdinal(in.readByte());
      this.oldValue = DataSerializer.readObject(in);
      if ((flags & FLAG_HASVERSION) != 0) {
        boolean persistentTag = (flags & FLAG_PERSISTENT) != 0;
        this.versionTag = VersionTag.create(persistentTag, in);
      }
    }

    public static void oldValueToData(DataOutput out, Object ov, boolean ovIsSerialized)
        throws IOException {
      if (ovIsSerialized && ov != null) {
        byte[] oldValueBytes;
        if (ov instanceof byte[]) {
          oldValueBytes = (byte[]) ov;
          DataSerializer.writeObject(new VMCachedDeserializable(oldValueBytes), out);
        } else if (ov instanceof CachedDeserializable) {
          if (ov instanceof StoredObject) {
            ((StoredObject) ov).sendAsCachedDeserializable(out);
          } else {
            DataSerializer.writeObject(ov, out);
          }
        } else {
          oldValueBytes = EntryEventImpl.serialize(ov);
          DataSerializer.writeObject(new VMCachedDeserializable(oldValueBytes), out);
        }
      } else {
        DataSerializer.writeObject(ov, out);
      }

    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      byte flags = 0;
      if (this.result)
        flags |= FLAG_RESULT;
      if (this.versionTag != null)
        flags |= FLAG_HASVERSION;
      if (this.versionTag instanceof DiskVersionTag)
        flags |= FLAG_PERSISTENT;
      out.writeByte(flags);
      out.writeByte(this.op.ordinal);
      oldValueToData(out, getOldValue(), this.oldValueIsSerialized);
      if (this.versionTag != null) {
        InternalDataSerializer.invokeToData(this.versionTag, out);
      }
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("PutReplyMessage ").append("processorid=").append(this.processorId)
          .append(" returning ").append(this.result).append(" op=").append(op).append(" exception=")
          .append(getException());
      if (this.versionTag != null) {
        sb.append(" version=").append(this.versionTag);
      }
      return sb.toString();
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
      return true;
    }

    @Override
    public void importOldObject(@Unretained(ENTRY_EVENT_OLD_VALUE) Object ov,
        boolean isSerialized) {
      this.oldValueIsSerialized = isSerialized;
      this.oldValue = ov;
    }

    @Override
    public void importOldBytes(byte[] ov, boolean isSerialized) {
      importOldObject(ov, isSerialized);
    }
  }

  /**
   * A processor to capture the value returned by {@link RemotePutMessage}
   *
   * @since GemFire 5.1
   */
  public static class RemotePutResponse extends RemoteOperationResponse {
    private volatile boolean returnValue;
    private volatile Operation op;
    private volatile Object oldValue;
    private RemotePutMessage putMessage;
    private VersionTag<?> versionTag;

    public RemotePutResponse(InternalDistributedSystem ds, DistributedMember recipient,
        boolean register) {
      super(ds, (InternalDistributedMember) recipient, register);
    }


    public void setRemotePutMessage(RemotePutMessage putMessage) {
      this.putMessage = putMessage;
    }

    public RemotePutMessage getRemotePutMessage() {
      return this.putMessage;
    }

    public void setResponse(PutReplyMessage msg) {
      this.returnValue = msg.result;
      this.op = msg.op;
      this.oldValue = msg.getOldValue();
      this.versionTag = msg.versionTag;
    }

    /**
     * @return the result of the remote put operation
     * @throws RemoteOperationException if the peer is no longer available
     * @throws CacheException if the peer generates an error
     */
    public PutResult waitForResult() throws CacheException, RemoteOperationException {
      waitForRemoteResponse();
      if (this.op == null) {
        throw new RemoteOperationException(
            "did not receive a valid reply");
      }
      return new PutResult(this.returnValue, this.op, this.oldValue, this.versionTag);
    }

  }

  public static class PutResult {
    /** the result of the put operation */
    public boolean returnValue;
    /** the actual operation performed (CREATE/UPDATE) */
    public Operation op;

    /** the old value, or null if not set */
    public Object oldValue;

    /** the concurrency control version tag */
    public VersionTag<?> versionTag;

    public PutResult(boolean flag, Operation actualOperation, Object oldValue,
        VersionTag<?> versionTag) {
      this.returnValue = flag;
      this.op = actualOperation;
      this.oldValue = oldValue;
      this.versionTag = versionTag;
    }
  }

  public void setSendDelta(boolean sendDelta) {
    this.sendDelta = sendDelta;
  }

  @Override
  public boolean prefersNewSerialized() {
    return true;
  }

  @Override
  public boolean isUnretainedNewReferenceOk() {
    return true;
  }

  private void setDeserializationPolicy(boolean isSerialized) {
    if (!isSerialized) {
      this.deserializationPolicy = DistributedCacheOperation.DESERIALIZATION_POLICY_NONE;
    }
  }

  @Override
  public void importNewObject(@Unretained(ENTRY_EVENT_NEW_VALUE) Object nv, boolean isSerialized) {
    setDeserializationPolicy(isSerialized);
    setValObj(nv);
  }

  @Override
  public void importNewBytes(byte[] nv, boolean isSerialized) {
    setDeserializationPolicy(isSerialized);
    setValBytes(nv);
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
}
