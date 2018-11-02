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
package org.apache.geode.internal.cache.partitioned;

import static org.apache.geode.internal.offheap.annotations.OffHeapIdentifier.ENTRY_EVENT_NEW_VALUE;
import static org.apache.geode.internal.offheap.annotations.OffHeapIdentifier.ENTRY_EVENT_OLD_VALUE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryExistsException;
import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
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
import org.apache.geode.internal.cache.DataLocationException;
import org.apache.geode.internal.cache.DistributedCacheOperation;
import org.apache.geode.internal.cache.EntryEventImpl;
import org.apache.geode.internal.cache.EntryEventImpl.NewValueImporter;
import org.apache.geode.internal.cache.EntryEventImpl.OldValueImporter;
import org.apache.geode.internal.cache.EnumListenerEvent;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.FilterRoutingInfo;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.PrimaryBucketException;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tx.RemotePutMessage;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;

/**
 * A Partitioned Region update message. Meant to be sent only to a bucket's primary owner. In
 * addition to updating an entry it is also used to send Partitioned Region event information.
 *
 * @since GemFire 5.0
 */
public class PutMessage extends PartitionMessageWithDirectReply implements NewValueImporter {
  private static final Logger logger = LogService.getLogger();

  /** The key associated with the value that must be sent */
  private Object key;

  /** The value associated with the key that must be sent */
  private byte[] valBytes;

  /**
   * Used on sender side only to defer serialization until toData is called.
   */
  @Unretained(ENTRY_EVENT_NEW_VALUE)
  private transient Object valObj;

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

  private transient InternalDistributedSystem internalDs;

  /**
   * state from operateOnRegion that must be preserved for transmission from the waiting pool
   */
  transient boolean result = false;

  /** client routing information for notificationOnly=true messages */
  private FilterRoutingInfo filterInfo;

  private boolean hasFilterInfo;

  /** whether value has delta **/
  private boolean hasDelta = false;

  /** whether new value is formed by applying delta **/
  private transient boolean isDeltaApplied = false;

  /** whether to send delta or full value **/
  private transient boolean sendDelta = false;

  private EntryEventImpl event = null;

  private byte[] deltaBytes = null;

  private VersionTag versionTag;

  // additional bitmask flags used for serialization/deserialization

  protected static final short CACHE_WRITE = UNRESERVED_FLAGS_START;
  protected static final short HAS_EXPECTED_OLD_VAL = (CACHE_WRITE << 1);
  protected static final short HAS_VERSION_TAG = (HAS_EXPECTED_OLD_VAL << 1);

  // extraFlags
  protected static final int HAS_BRIDGE_CONTEXT =
      getNextByteMask(DistributedCacheOperation.DESERIALIZATION_POLICY_END);
  protected static final int HAS_ORIGINAL_SENDER = getNextByteMask(HAS_BRIDGE_CONTEXT);
  protected static final int HAS_DELTA_WITH_FULL_VALUE = getNextByteMask(HAS_ORIGINAL_SENDER);
  protected static final int HAS_CALLBACKARG = getNextByteMask(HAS_DELTA_WITH_FULL_VALUE);
  // TODO this should really have been at the PartitionMessage level but all
  // masks there are taken
  // also switching the masks will impact backwards compatibility. Need to
  // verify if it is ok to break backwards compatibility

  /*
   * private byte[] oldValBytes; private transient Object oldValObj; private boolean hasOldValue =
   * false; private boolean oldValueIsSerialized = false;
   */
  /**
   * Empty constructor to satisfy {@link DataSerializer}requirements
   */
  public PutMessage() {}

  /** cloning constructor for relaying to listeners */
  PutMessage(PutMessage original, EntryEventImpl event, Set members) {
    super(original, event);
    this.key = original.key;
    if (original.valBytes != null) {
      this.valBytes = original.valBytes;
    } else {
      if (original.valObj instanceof CachedDeserializable) {
        CachedDeserializable cd = (CachedDeserializable) original.valObj;
        if (!cd.isSerialized()) {
          this.valObj = cd.getDeserializedForReading();
        } else {
          Object val = cd.getValue();
          if (val instanceof byte[]) {
            this.valBytes = (byte[]) val;
          } else {
            this.valObj = val;
          }
        }
      } else {
        this.valObj = original.valObj;
      }
    }
    this.cbArg = original.cbArg;
    this.lastModified = original.lastModified;
    this.op = original.op;
    this.bridgeContext = original.bridgeContext;
    this.deserializationPolicy = original.deserializationPolicy;
    this.originalSender = original.getSender();
    Assert.assertTrue(original.eventId != null);
    this.eventId = original.eventId;
    this.result = original.result;
    this.ifNew = original.ifNew;
    this.ifOld = original.ifOld;
    this.internalDs = original.internalDs;
    this.requireOldValue = original.requireOldValue;
    this.expectedOldValue = original.expectedOldValue;
    this.processor = original.processor;
    this.event = event;
    this.versionTag = event.getVersionTag();
  }

  /**
   * copy constructor
   */
  PutMessage(PutMessage original) {
    super(original, null);
    this.bridgeContext = original.bridgeContext;
    this.cbArg = original.cbArg;
    this.deserializationPolicy = original.deserializationPolicy;
    this.event = original.event;
    this.eventId = original.eventId;
    this.expectedOldValue = original.expectedOldValue;
    this.hasDelta = original.hasDelta;
    this.ifNew = original.ifNew;
    this.ifOld = original.ifOld;
    this.internalDs = original.internalDs;
    this.isDeltaApplied = original.isDeltaApplied;
    this.key = original.key;
    this.lastModified = original.lastModified;
    this.notificationOnly = original.notificationOnly;
    this.op = original.op;
    this.originalSender = original.originalSender;
    this.requireOldValue = original.requireOldValue;
    this.result = original.result;
    this.sendDelta = original.sendDelta;
    this.sender = original.sender;
    this.valBytes = original.valBytes;
    this.valObj = original.valObj;
    this.filterInfo = original.filterInfo;
    this.versionTag = original.versionTag;
    /*
     * this.oldValBytes = original.oldValBytes; this.oldValObj = original.oldValObj;
     * this.oldValueIsSerialized = original.oldValueIsSerialized;
     */
  }


  @Override
  public PartitionMessage getMessageForRelayToListeners(EntryEventImpl ev, Set members) {
    PutMessage msg = new PutMessage(this, ev, members);
    msg.requireOldValue = false;
    msg.expectedOldValue = null;
    return msg;
  }

  /**
   * send a notification-only message to a set of listeners. The processor id is passed with the
   * message for reply message processing. This method does not wait on the processor.
   *
   * @param cacheOpReceivers receivers of associated bucket CacheOperationMessage
   * @param adjunctRecipients receivers that must get the event
   * @param filterInfo all client routing information
   * @param r the region affected by the event
   * @param event the event that prompted this action
   * @param processor the processor to reply to
   * @return members that could not be notified
   */
  public static Set notifyListeners(Set cacheOpReceivers, Set adjunctRecipients,
      FilterRoutingInfo filterInfo, PartitionedRegion r, EntryEventImpl event, boolean ifNew,
      boolean ifOld, DirectReplyProcessor processor, boolean sendDeltaWithFullValue) {
    PutMessage msg = new PutMessage(Collections.EMPTY_SET, true, r.getPRId(), processor, event, 0,
        ifNew, ifOld, null, false);
    msg.setTransactionDistributed(r.getCache().getTxManager().isDistributed());
    msg.setInternalDs(r.getSystem());
    msg.versionTag = event.getVersionTag();
    msg.setSendDeltaWithFullValue(sendDeltaWithFullValue);
    return msg.relayToListeners(cacheOpReceivers, adjunctRecipients, filterInfo, event, r,
        processor);
  }

  PutMessage(Set recipients, boolean notifyOnly, int regionId, DirectReplyProcessor processor,
      EntryEventImpl event, final long lastModified, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue) {
    super(recipients, regionId, processor, event);
    this.processor = processor;
    this.notificationOnly = notifyOnly;
    this.requireOldValue = requireOldValue;
    this.expectedOldValue = expectedOldValue;
    this.key = event.getKey();
    if (event.hasNewValue()) {
      this.deserializationPolicy = DistributedCacheOperation.DESERIALIZATION_POLICY_LAZY;
      event.exportNewValue(this);
    } else {
      // assert that if !event.hasNewValue, then deserialization policy is NONE
      assert this.deserializationPolicy == DistributedCacheOperation.DESERIALIZATION_POLICY_NONE : this.deserializationPolicy;
    }

    this.event = event;

    this.cbArg = event.getRawCallbackArgument();
    this.lastModified = lastModified;
    this.op = event.getOperation();
    this.bridgeContext = event.getContext();
    this.eventId = event.getEventId();
    this.versionTag = event.getVersionTag();
    Assert.assertTrue(this.eventId != null);
    this.ifNew = ifNew;
    this.ifOld = ifOld;
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    // TODO Auto-generated method stub
    return super.clone();
  }

  /**
   * Sends a PartitionedRegion {@link org.apache.geode.cache.Region#put(Object, Object)} message to
   * the recipient
   *
   * @param recipient the member to which the put message is sent
   * @param r the PartitionedRegion for which the put was performed
   * @param event the event prompting this message
   * @param ifNew whether a new entry must be created
   * @param ifOld whether an old entry must be updated (no creates)
   * @return the processor used to await acknowledgement that the update was sent, or null to
   *         indicate that no acknowledgement will be sent
   * @throws ForceReattemptException if the peer is no longer available
   */
  public static PartitionResponse send(DistributedMember recipient, PartitionedRegion r,
      EntryEventImpl event, final long lastModified, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue) throws ForceReattemptException {
    // Assert.assertTrue(recipient != null, "PutMessage NULL recipient"); recipient can be null for
    // event notifications
    Set recipients = Collections.singleton(recipient);

    PutResponse processor = new PutResponse(r.getSystem(), recipients, event.getKey());

    PutMessage m = new PutMessage(recipients, false, r.getPRId(), processor, event, lastModified,
        ifNew, ifOld, expectedOldValue, requireOldValue);
    m.setInternalDs(r.getSystem());
    m.setSendDelta(true);
    m.setTransactionDistributed(r.getCache().getTxManager().isDistributed());

    processor.setPutMessage(m);

    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException(
          String.format("Failed sending < %s >", m));
    }
    return processor;
  }


  /**
   * create a new EntryEvent to be used in notifying listeners, cache servers, etc. Caller must
   * release result if it is != to sourceEvent
   */
  @Retained
  EntryEventImpl createListenerEvent(EntryEventImpl sourceEvent, PartitionedRegion r,
      InternalDistributedMember member) {
    final EntryEventImpl e2;
    if (this.notificationOnly && this.bridgeContext == null) {
      e2 = sourceEvent;
    } else {
      e2 = new EntryEventImpl(sourceEvent);
      if (this.bridgeContext != null) {
        e2.setContext(this.bridgeContext);
      }
    }
    e2.setRegion(r);
    e2.setOriginRemote(true);
    e2.setInvokePRCallbacks(!notificationOnly);

    if (!sourceEvent.hasOldValue()) {
      e2.oldValueNotAvailable();
    }

    if (this.filterInfo != null) {
      e2.setLocalFilterInfo(this.filterInfo.getFilterInfo(member));
    }

    if (this.versionTag != null) {
      this.versionTag.replaceNullIDs(getSender());
      e2.setVersionTag(this.versionTag);
    }

    return e2;
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

  private void setValBytes(byte[] valBytes) {
    this.valBytes = valBytes;
  }

  private void setValObj(@Unretained(ENTRY_EVENT_NEW_VALUE) Object o) {
    this.valObj = o;
  }

  /**
   * (ashetkar) Strictly for Delta Propagation purpose.
   *
   * @param o Object of type Delta
   */
  public void setDeltaValObj(Object o) {
    if (this.valObj == null) {
      this.valObj = o;
    }
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


  @Override
  public void setFilterInfo(FilterRoutingInfo filterInfo) {
    if (filterInfo != null) {
      this.filterInfo = filterInfo;
    }
  }

  public int getDSFID() {
    return PR_PUT_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);

    final int extraFlags = in.readUnsignedByte();
    setKey(DataSerializer.readObject(in));
    this.cbArg = DataSerializer.readObject(in);
    this.lastModified = in.readLong();
    this.op = Operation.fromOrdinal(in.readByte());
    if ((extraFlags & HAS_BRIDGE_CONTEXT) != 0) {
      this.bridgeContext = ClientProxyMembershipID.readCanonicalized(in);
    }
    if ((extraFlags & HAS_ORIGINAL_SENDER) != 0) {
      this.originalSender = (InternalDistributedMember) DataSerializer.readObject(in);
    }
    this.eventId = new EventID();
    InternalDataSerializer.invokeFromData(this.eventId, in);

    if ((flags & HAS_EXPECTED_OLD_VAL) != 0) {
      this.expectedOldValue = DataSerializer.readObject(in);
    }
    if (this.hasFilterInfo) {
      this.filterInfo = new FilterRoutingInfo();
      InternalDataSerializer.invokeFromData(this.filterInfo, in);
    }
    this.deserializationPolicy =
        (byte) (extraFlags & DistributedCacheOperation.DESERIALIZATION_POLICY_MASK);

    if (this.hasDelta) {
      this.deltaBytes = DataSerializer.readByteArray(in);
    } else {
      setValBytes(DataSerializer.readByteArray(in));
      if ((extraFlags & HAS_DELTA_WITH_FULL_VALUE) != 0) {
        this.deltaBytes = DataSerializer.readByteArray(in);
      }
    }
    if ((flags & HAS_VERSION_TAG) != 0) {
      this.versionTag = DataSerializer.readObject(in);
    }
  }

  @Override
  public EventID getEventID() {
    return this.eventId;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    PartitionedRegion region = null;
    try {
      boolean flag = internalDs.getConfig().getDeltaPropagation();
      if (this.event.getDeltaBytes() != null && flag && this.sendDelta) {
        this.hasDelta = true;
      } else {
        // Reset the flag when sending full object.
        this.hasDelta = false;
      }
    } catch (RuntimeException re) {
      throw new InvalidDeltaException(re);
    }
    super.toData(out);

    int extraFlags = this.deserializationPolicy;
    if (this.bridgeContext != null)
      extraFlags |= HAS_BRIDGE_CONTEXT;
    if (this.deserializationPolicy != DistributedCacheOperation.DESERIALIZATION_POLICY_NONE
        && (this.valObj != null || getValBytes() != null) && this.sendDeltaWithFullValue
        && this.event.getDeltaBytes() != null) {
      extraFlags |= HAS_DELTA_WITH_FULL_VALUE;
    }
    if (this.originalSender != null)
      extraFlags |= HAS_ORIGINAL_SENDER;
    out.writeByte(extraFlags);

    DataSerializer.writeObject(getKey(), out);
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
    if (this.hasFilterInfo) {
      InternalDataSerializer.invokeToData(this.filterInfo, out);
    }
    if (this.hasDelta) {
      try {
        region = PartitionedRegion.getPRFromId(this.regionId);
      } catch (PRLocallyDestroyedException e) {
        throw new IOException("Delta can not be extracted as region is locally destroyed");
      }
      if (region == null || region.getCachePerfStats() == null) {
        throw new IOException(
            "Delta can not be extracted as region can't be found or is in an invalid state");
      }
      DataSerializer.writeByteArray(this.event.getDeltaBytes(), out);
      region.getCachePerfStats().incDeltasSent();
    } else {
      DistributedCacheOperation.writeValue(this.deserializationPolicy, this.valObj, getValBytes(),
          out);
      if ((extraFlags & HAS_DELTA_WITH_FULL_VALUE) != 0) {
        DataSerializer.writeByteArray(this.event.getDeltaBytes(), out);
      }
    }
    if (this.versionTag != null) {
      DataSerializer.writeObject(this.versionTag, out);
    }
  }

  @Override
  protected short computeCompressedShort(short s) {
    s = super.computeCompressedShort(s);
    if (this.ifNew)
      s |= IF_NEW;
    if (this.ifOld)
      s |= IF_OLD;
    if (this.requireOldValue)
      s |= REQUIRED_OLD_VAL;
    if (this.expectedOldValue != null)
      s |= HAS_EXPECTED_OLD_VAL;
    if (this.filterInfo != null) {
      s |= HAS_FILTER_INFO;
      this.hasFilterInfo = true;
    }
    if (this.hasDelta) {
      s |= HAS_DELTA;
      if (this.bridgeContext != null) {
        // delta bytes sent by client to accessor or secondary data store
        // requires to set data policy explicitly to LAZY.
        this.deserializationPolicy = DistributedCacheOperation.DESERIALIZATION_POLICY_LAZY;
      }
    }
    if (this.versionTag != null)
      s |= HAS_VERSION_TAG;
    return s;
  }

  @Override
  protected void setBooleans(short s, DataInput in) throws IOException, ClassNotFoundException {
    super.setBooleans(s, in);
    this.ifNew = ((s & IF_NEW) != 0);
    this.ifOld = ((s & IF_OLD) != 0);
    this.requireOldValue = ((s & REQUIRED_OLD_VAL) != 0);
    this.hasFilterInfo = ((s & HAS_FILTER_INFO) != 0);
    this.hasDelta = ((s & HAS_DELTA) != 0);
  }

  /**
   * This method is called upon receipt and make the desired changes to the PartitionedRegion Note:
   * It is very important that this message does NOT cause any deadlocks as the sender will wait
   * indefinitely for the acknowledgement
   */
  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion r,
      long startTime) throws EntryExistsException, DataLocationException, IOException {
    this.setInternalDs(r.getSystem());// set the internal DS. Required to
                                      // checked DS level delta-enabled property
                                      // while sending delta
    PartitionedRegionDataStore ds = r.getDataStore();
    boolean sendReply = true;

    InternalDistributedMember eventSender = originalSender;
    if (eventSender == null) {
      eventSender = getSender();
    }
    @Released
    final EntryEventImpl ev =
        EntryEventImpl.create(r, getOperation(), getKey(), null, /* newValue */
            getCallbackArg(), false/* originRemote - false to force distribution in buckets */,
            eventSender, true/* generateCallbacks */, false/* initializeId */);
    try {
      if (this.versionTag != null) {
        this.versionTag.replaceNullIDs(getSender());
        ev.setVersionTag(this.versionTag);
      }
      if (this.bridgeContext != null) {
        ev.setContext(this.bridgeContext);
      }
      Assert.assertTrue(eventId != null);
      ev.setEventId(eventId);
      ev.setCausedByMessage(this);
      ev.setInvokePRCallbacks(!notificationOnly);
      ev.setPossibleDuplicate(this.posDup);
      /*
       * if (this.hasOldValue) { if (this.oldValueIsSerialized) {
       * ev.setSerializedOldValue(getOldValueBytes()); } else { ev.setOldValue(getOldValueBytes());
       * } }
       */

      ev.setDeltaBytes(this.deltaBytes);
      if (this.hasDelta) {
        this.valObj = null;
        // New value will be set once it is generated with fromDelta() inside
        // EntryEventImpl.processDeltaBytes()
        ev.setNewValue(this.valObj);
      } else {
        switch (this.deserializationPolicy) {
          case DistributedCacheOperation.DESERIALIZATION_POLICY_LAZY:
            ev.setSerializedNewValue(getValBytes());
            break;
          case DistributedCacheOperation.DESERIALIZATION_POLICY_NONE:
            ev.setNewValue(getValBytes());
            break;
          default:
            throw new AssertionError("unknown deserialization policy: " + deserializationPolicy);
        }
      }

      if (!notificationOnly) {
        if (ds == null) {
          throw new AssertionError(
              "This process should have storage" + " for this operation: " + this.toString());
        }
        try {
          ev.setOriginRemote(false);
          result =
              r.getDataView().putEntryOnRemote(ev, this.ifNew, this.ifOld, this.expectedOldValue,
                  this.requireOldValue, this.lastModified, true/* overwriteDestroyed *not* used */);

          if (!this.result) { // make sure the region hasn't gone away
            r.checkReadiness();
          }
        } catch (CacheWriterException | PrimaryBucketException cwe) {
          sendReply(getSender(), getProcessorId(), dm, new ReplyException(cwe), r, startTime);
          return false;
        } catch (InvalidDeltaException ide) {
          sendReply(getSender(), getProcessorId(), dm, new ReplyException(ide), r, startTime);
          r.getCachePerfStats().incDeltaFullValuesRequested();
          return false;
        }
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "PutMessage {} with key: {} val: {}",
              (result ? "updated bucket" : "did not update bucket"), getKey(),
              (getValBytes() == null ? "null" : "(" + getValBytes().length + " bytes)"));
        }
      } else { // notificationOnly
        @Released
        EntryEventImpl e2 = createListenerEvent(ev, r, dm.getDistributionManagerId());
        final EnumListenerEvent le;
        try {
          if (e2.getOperation().isCreate()) {
            le = EnumListenerEvent.AFTER_CREATE;
          } else {
            le = EnumListenerEvent.AFTER_UPDATE;
          }
          r.invokePutCallbacks(le, e2, r.isInitialized(), true);
        } finally {
          // if e2 == ev then no need to free it here. The outer finally block will get it.
          if (e2 != ev) {
            e2.release();
          }
        }
        result = true;
      }

      setOperation(ev.getOperation()); // set operation for reply message

      if (sendReply) {
        sendReply(getSender(), getProcessorId(), dm, null, r, startTime, ev);
      }
      return false;
    } finally {
      ev.release();
    }
  }


  // override reply processor type from PartitionMessage
  PartitionResponse createReplyProcessor(PartitionedRegion r, Set recipients, Object k) {
    return new PutResponse(r.getSystem(), recipients, k);
  }


  protected void sendReply(InternalDistributedMember member, int procId, DistributionManager dm,
      ReplyException ex, PartitionedRegion pr, long startTime, EntryEventImpl ev) {
    if (pr != null && startTime > 0) {
      pr.getPrStats().endPartitionMessagesProcessing(startTime);
      pr.getCancelCriterion().checkCancelInProgress(null); // bug 39014 - don't send a positive
                                                           // response if we may have failed
    }
    PutReplyMessage.send(member, procId, getReplySender(dm), result, getOperation(), ex, this, ev);
  }


  @Override
  protected void appendFields(StringBuilder buff) {
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
    if (this.versionTag != null) {
      buff.append("; version=").append(this.versionTag);
    }
    buff.append("; deserializationPolicy=");
    buff.append(
        DistributedCacheOperation.deserializationPolicyToString(this.deserializationPolicy));
    if (this.hasDelta) {
      buff.append("; hasDelta=");
      buff.append(this.hasDelta);
    }
    if (this.sendDelta) {
      buff.append("; sendDelta=");
      buff.append(this.sendDelta);
    }
    if (this.isDeltaApplied) {
      buff.append("; isDeltaApplied=");
      buff.append(this.isDeltaApplied);
    }
    if (this.filterInfo != null) {
      buff.append("; ");
      buff.append(this.filterInfo.toString());
    }
  }

  public InternalDistributedSystem getInternalDs() {
    return internalDs;
  }

  public void setInternalDs(InternalDistributedSystem internalDs) {
    this.internalDs = internalDs;
  }

  @Override
  protected boolean mayAddToMultipleSerialGateways(ClusterDistributionManager dm) {
    return _mayAddToMultipleSerialGateways(dm);
  }

  public static class PutReplyMessage extends ReplyMessage implements OldValueImporter {
    /** Result of the Put operation */
    boolean result;

    /** The Operation actually performed */
    Operation op;

    /**
     * Old value in serialized form: either a byte[] or CachedDeserializable, or null if not set.
     */
    @Unretained(ENTRY_EVENT_OLD_VALUE)
    Object oldValue;

    VersionTag versionTag;

    /**
     * Set to true by the import methods if the oldValue is already serialized. In that case toData
     * should just copy the bytes to the stream. In either case fromData just calls readObject.
     */
    private transient boolean oldValueIsSerialized;

    @Override
    public boolean getInlineProcess() {
      return true;
    }

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public PutReplyMessage() {}

    // package access for unit test
    PutReplyMessage(int processorId, boolean result, Operation op, ReplyException ex,
        Object oldValue, VersionTag version) {
      super();
      this.op = op;
      this.result = result;
      setProcessorId(processorId);
      setException(ex);
      this.oldValue = oldValue;
      this.versionTag = version;
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId, ReplySender dm,
        boolean result, Operation op, ReplyException ex, PutMessage sourceMessage,
        EntryEventImpl ev) {
      Assert.assertTrue(recipient != null, "PutReplyMessage NULL reply message");
      PutReplyMessage m =
          new PutReplyMessage(processorId, result, op, ex, null, ev.getVersionTag());
      if (!sourceMessage.notificationOnly && sourceMessage.requireOldValue) {
        ev.exportOldValue(m);
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
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE,
            "PutReplyMessage process invoking reply processor with processorId: {}",
            this.processorId);
      }
      if (rp == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "PutReplyMessage processor not found");
        }
        return;
      }
      if (rp instanceof PutResponse) {
        PutResponse processor = (PutResponse) rp;
        processor.setResponse(this);
      }
      rp.process(this);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} processed {}", rp, this);
      }
      dm.getStats().incReplyMessageTime(NanoTimer.getTime() - startTime);
    }

    /** Return oldValue in serialized form */
    public Object getOldValue() {
      return this.oldValue;
    }

    @Override
    public int getDSFID() {
      return PR_PUT_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.result = in.readBoolean();
      this.op = Operation.fromOrdinal(in.readByte());
      this.oldValue = DataSerializer.readObject(in);
      this.versionTag = (VersionTag) DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeBoolean(this.result);
      out.writeByte(this.op.ordinal);
      Object ov = getOldValue();
      RemotePutMessage.PutReplyMessage.oldValueToData(out, getOldValue(),
          this.oldValueIsSerialized);
      DataSerializer.writeObject(this.versionTag, out);
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("PutReplyMessage ").append("processorid=").append(this.processorId)
          .append(" returning ").append(this.result).append(" op=").append(op).append(" exception=")
          .append(getException()).append(" oldValue=")
          .append(this.oldValue == null ? "null" : "not null").append(" version=")
          .append(this.versionTag);
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
      this.oldValue = ov;
      this.oldValueIsSerialized = isSerialized;
    }

    @Override
    public void importOldBytes(byte[] ov, boolean isSerialized) {
      importOldObject(ov, isSerialized);
    }
  }

  /**
   * A processor to capture the value returned by {@link PutMessage}
   *
   * @since GemFire 5.1
   */
  public static class PutResponse extends PartitionResponse {
    private volatile boolean returnValue;
    private volatile Operation op;
    private volatile Object oldValue;
    private final Object key;
    private PutMessage putMessage;
    private VersionTag versionTag;

    public PutResponse(InternalDistributedSystem ds, Set recipients, Object key) {
      super(ds, recipients, false);
      this.key = key;
    }


    public void setPutMessage(PutMessage putMessage) {
      this.putMessage = putMessage;
    }

    public void setResponse(PutReplyMessage response) {
      // boolean response, Operation op, Object oldValue) {

      this.returnValue = response.result;
      this.op = response.op;
      this.oldValue = response.oldValue;
      this.versionTag = response.versionTag;
      if (this.versionTag != null) {
        this.versionTag.replaceNullIDs(response.getSender());
      }
    }

    /**
     * @return the result of the remote put operation
     * @throws ForceReattemptException if the peer is no longer available
     * @throws CacheException if the peer generates an error
     */
    public PutResult waitForResult() throws CacheException, ForceReattemptException {
      try {
        waitForCacheException();
      } catch (ForceReattemptException e) {
        e.checkKey(key);
        throw e;
      }
      if (this.op == null) {
        throw new ForceReattemptException(
            "did not receive a valid reply");
      }
      // try {
      // waitForRepliesUninterruptibly();
      // }
      // catch (ReplyException e) {
      // Throwable t = e.getCause();
      // if (t instanceof CacheClosedException) {
      // throw new PartitionedRegionCommunicationException("Put operation received an exception",
      // t);
      // }
      // e.handleAsUnexpected();
      // }
      return new PutResult(this.returnValue, this.op, this.oldValue, this.versionTag);
    }

    @Override
    public void process(final DistributionMessage msg) {

      if (msg instanceof ReplyMessage) {
        ReplyException ex = ((ReplyMessage) msg).getException();
        if (this.putMessage.bridgeContext == null
            // Why is this code not happening for bug 41916?
            && (ex != null && ex.getCause() instanceof InvalidDeltaException)) {
          final PutMessage putMsg = new PutMessage(this.putMessage);
          final DistributionManager dm = getDistributionManager();
          Runnable sendFullObject = new Runnable() {
            public void run() {
              putMsg.resetRecipients();
              putMsg.setRecipient(msg.getSender());
              putMsg.setSendDelta(false);
              if (logger.isDebugEnabled()) {
                logger.debug("Sending full object({}) to {}", putMsg,
                    Arrays.toString(putMsg.getRecipients()));
              }
              dm.putOutgoing(putMsg);

              // Update stats
              try {
                PartitionedRegion.getPRFromId(putMsg.regionId).getCachePerfStats()
                    .incDeltaFullValuesSent();
              } catch (Exception e) {
              }
            }

            @Override
            public String toString() {
              return "Sending full object {" + putMsg.toString() + "}";
            }
          };
          if (isExpectingDirectReply()) {
            sendFullObject.run();
          } else {
            getDistributionManager().getWaitingThreadPool().execute(sendFullObject);
          }
          return;
        }
      }
      super.process(msg);
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
    public VersionTag versionTag;

    public PutResult(boolean flag, Operation actualOperation, Object oldValue, VersionTag version) {
      this.returnValue = flag;
      this.op = actualOperation;
      this.oldValue = oldValue;
      this.versionTag = version;
    }
  }

  public void setSendDelta(boolean sendDelta) {
    this.sendDelta = sendDelta;
  }

  // NewValueImporter methods

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
}
