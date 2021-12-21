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

package org.apache.geode.internal.cache;

import static org.apache.geode.internal.offheap.annotations.OffHeapIdentifier.ENTRY_EVENT_NEW_VALUE;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.distributed.internal.ConflationKey;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.EntryEventImpl.NewValueImporter;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.offheap.annotations.Unretained;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Handles distribution messaging for updating an entry in a region.
 *
 */
public class UpdateOperation extends AbstractUpdateOperation {
  private static final Logger logger = LogService.getLogger();

  /** Creates a new instance of UpdateOperation */
  public UpdateOperation(EntryEventImpl event, long lastModifiedTime) {
    super(event, lastModifiedTime);
  }

  // protected Set getRecipients() {
  // DistributionAdvisor advisor = getRegion().getDistributionAdvisor();
  // return super.getRecipients();
  // }

  @Override
  protected boolean supportsDeltaPropagation() {
    return true;
  }

  @Override
  protected CacheOperationMessage createMessage() {
    EntryEventImpl ev = getEvent();
    if (ev.isBridgeEvent()) {
      UpdateWithContextMessage mssgwithContxt = new UpdateWithContextMessage();
      // getContext is not in EntryEvent interface because it exposes a private
      // class
      mssgwithContxt.clientID = ev.getContext();
      return mssgwithContxt;
    } else {
      return new UpdateMessage();
    }
  }

  @Override
  protected void initMessage(CacheOperationMessage msg, DirectReplyProcessor p) {
    super.initMessage(msg, p);
    UpdateMessage m = (UpdateMessage) msg;
    EntryEventImpl ev = getEvent();
    m.event = ev;
    m.eventId = ev.getEventId();
    m.key = ev.getKey();
    m.deserializationPolicy = DESERIALIZATION_POLICY_LAZY;
    ev.exportNewValue(m);
  }

  @Override
  protected void initProcessor(CacheOperationReplyProcessor p, CacheOperationMessage msg) {
    if (processor != null) {
      if (msg instanceof UpdateWithContextMessage) {
        processor.msg = new UpdateWithContextMessage((UpdateWithContextMessage) msg);
      } else {
        processor.msg = new UpdateMessage((UpdateMessage) msg);
      }
    }
  }

  public static class UpdateMessage extends AbstractUpdateMessage implements NewValueImporter {

    /**
     * Indicates if and when the new value should be deserialized on the the receiver
     */
    protected byte deserializationPolicy;

    protected EntryEventImpl event = null;

    protected EventID eventId = null;

    protected Object key;

    protected byte[] newValue;

    @Unretained(ENTRY_EVENT_NEW_VALUE)
    protected transient Object newValueObj;

    private byte[] deltaBytes;

    private boolean sendDeltaWithFullValue = true;

    // extraFlags
    static final int HAS_EVENTID = getNextByteMask(DESERIALIZATION_POLICY_END);
    static final int HAS_DELTA_WITH_FULL_VALUE = getNextByteMask(HAS_EVENTID);

    private Long tailKey = 0L;

    public UpdateMessage() {}

    /**
     * copy constructor
     */
    public UpdateMessage(UpdateMessage upMsg) {
      appliedOperation = upMsg.appliedOperation;
      callbackArg = upMsg.callbackArg;
      deserializationPolicy = upMsg.deserializationPolicy;
      directAck = upMsg.directAck;
      event = upMsg.event;
      eventId = upMsg.eventId;
      hasDelta = upMsg.hasDelta;
      key = upMsg.key;
      lastModified = upMsg.lastModified;
      newValue = upMsg.newValue;
      newValueObj = upMsg.newValueObj;
      op = upMsg.op;
      owner = upMsg.owner;
      possibleDuplicate = upMsg.possibleDuplicate;
      processorId = upMsg.processorId;
      regionAllowsConflation = upMsg.regionAllowsConflation;
      regionPath = upMsg.regionPath;
      sendDelta = upMsg.sendDelta;
      sender = upMsg.sender;
      processor = upMsg.processor;
      filterRouting = upMsg.filterRouting;
      needsRouting = upMsg.needsRouting;
      versionTag = upMsg.versionTag;
    }

    @Override
    public ConflationKey getConflationKey() {
      if (!super.regionAllowsConflation || directAck || getProcessorId() != 0) {
        // if the publisher's region attributes do not support conflation
        // or if it is an ack region
        // then don't even bother with a conflation key
        return null;
      } else {
        // only conflate if it is not a create
        // and we don't want an ack
        return new ConflationKey(key, super.regionPath, getOperation().isUpdate());
      }
    }

    @Override
    @Retained
    protected InternalCacheEvent createEvent(DistributedRegion rgn) throws EntryNotFoundException {
      EntryEventImpl ev = createEntryEvent(rgn);
      boolean evReturned = false;
      try {
        ev.setEventId(eventId);

        ev.setDeltaBytes(deltaBytes);

        if (hasDelta()) {
          newValueObj = null;
          // New value will be set once it is generated with fromDelta() inside
          // EntryEventImpl.processDeltaBytes()
          ev.setNewValue(newValueObj);
        } else {
          setNewValueInEvent(newValue, newValueObj, ev, deserializationPolicy);
        }
        if (filterRouting != null) {
          ev.setLocalFilterInfo(filterRouting.getFilterInfo(rgn.getMyId()));
        }
        ev.setTailKey(tailKey);

        ev.setVersionTag(versionTag);

        ev.setInhibitAllNotifications(inhibitAllNotifications);

        evReturned = true;
        return ev;
      } finally {
        if (!evReturned) {
          ev.release();
        }
      }
    }

    @Override
    boolean processReply(final ReplyMessage replyMessage, CacheOperationReplyProcessor processor) {
      ReplyException ex = replyMessage.getException();
      if (ex != null && ex.getCause() instanceof InvalidDeltaException) {
        // msg can be null when PR data store throws exception back to
        // accessor.
        UpdateMessage message = this;
        if (!(message.hasBridgeContext() && message.getDataPolicy() == DataPolicy.EMPTY)) {
          final UpdateMessage updateMsg;
          final DistributionManager dm = event.getRegion().getDistributionManager();
          if (this instanceof UpdateWithContextMessage) {
            updateMsg =
                new UpdateOperation.UpdateWithContextMessage((UpdateWithContextMessage) this);
          } else {
            updateMsg = new UpdateOperation.UpdateMessage(this);
          }
          Runnable sendMessage = new Runnable() {
            @Override
            public void run() {
              synchronized (updateMsg) { // prevent concurrent update of
                // recipient list
                updateMsg.resetRecipients();
                updateMsg.setRecipient(replyMessage.getSender());
                updateMsg.setSendDelta(false);
                updateMsg.setSendDeltaWithFullValue(false);
                if (logger.isDebugEnabled()) {
                  logger.debug("Sending full object ({}) to {}", updateMsg,
                      replyMessage.getSender());
                }
                dm.putOutgoing(updateMsg);
              }
              updateMsg.event.getRegion().getCachePerfStats().incDeltaFullValuesSent();
            }

            @Override
            public String toString() {
              return "Sending full object {" + updateMsg + "}";
            }
          };

          if (processor.isExpectingDirectReply()) {
            sendMessage.run();
          } else {
            dm.getExecutors().getWaitingThreadPool().execute(sendMessage);
          }
          return false;
        }
      }
      return true;
    }

    /**
     * Utility to set the new value in the EntryEventImpl based on the given deserialization value;
     * also called from QueuedOperation
     */
    static void setNewValueInEvent(byte[] newValue, Object newValueObj, EntryEventImpl event,
        byte deserializationPolicy) {
      if (newValue == null) {
        // in an UpdateMessage this results from a create(key, null) call,
        // set local invalid flag in event if this is a normal region. Otherwise
        // it should be a distributed invalid.
        if (event.getRegion().getAttributes().getDataPolicy() == DataPolicy.NORMAL) {
          event.setLocalInvalid(true);
        }
        event.setNewValue(newValue);
        Assert.assertTrue(deserializationPolicy == DESERIALIZATION_POLICY_NONE);
        return;
      }

      switch (deserializationPolicy) {
        case DESERIALIZATION_POLICY_LAZY:
          event.setSerializedNewValue(newValue);
          break;
        case DESERIALIZATION_POLICY_NONE:
          event.setNewValue(newValue);
          break;
        default:
          throw new InternalGemFireError(
              String.format("unknown deserialization policy: %s",
                  Byte.valueOf(deserializationPolicy)));
      }
    }

    @Retained
    protected EntryEventImpl createEntryEvent(DistributedRegion rgn) {
      Object argNewValue = null;
      final boolean originRemote = true, generateCallbacks = true;
      @Retained
      EntryEventImpl result = EntryEventImpl.create(rgn, getOperation(), key, argNewValue, // oldValue,
          callbackArg, originRemote, getSender(), generateCallbacks);
      setOldValueInEvent(result);
      result.setTailKey(tailKey);
      if (versionTag != null) {
        result.setVersionTag(versionTag);
      }
      return result;
    }

    @Override
    protected void appendFields(StringBuilder buff) {
      super.appendFields(buff);
      buff.append("; key=");
      buff.append(key);
      if (hasDelta()) {
        byte[] bytes;
        if (event != null) {
          bytes = event.getDeltaBytes();
        } else {
          bytes = deltaBytes;
        }
        if (bytes == null) {
          buff.append("; null delta bytes");
        } else {
          buff.append("; ").append(bytes.length).append(" delta bytes");
        }
      } else if (newValueObj != null) {
        buff.append("; newValueObj=");
        buff.append(newValueObj);
      } else {
        buff.append("; newValue=");
        // buff.append(this.newValue);
        buff.append(newValue == null ? "null" : "(" + newValue.length + " bytes)");
      }
      if (eventId != null) {
        buff.append("; eventId=").append(eventId);
      }
      buff.append("; deserializationPolicy=");
      buff.append(deserializationPolicyToString(deserializationPolicy));
    }

    @Override
    public int getDSFID() {
      return UPDATE_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      final byte extraFlags = in.readByte();
      final boolean hasEventId = (extraFlags & HAS_EVENTID) != 0;
      if (hasEventId) {
        eventId = new EventID();
        InternalDataSerializer.invokeFromData(eventId, in);

        boolean hasTailKey = in.readBoolean();
        if (hasTailKey) {
          tailKey = in.readLong();
        }
      } else {
        eventId = null;
      }
      key = DataSerializer.readObject(in);

      deserializationPolicy = (byte) (extraFlags & DESERIALIZATION_POLICY_MASK);
      if (hasDelta()) {
        deltaBytes = DataSerializer.readByteArray(in);
      } else {
        newValue = DataSerializer.readByteArray(in);
        if ((extraFlags & HAS_DELTA_WITH_FULL_VALUE) != 0) {
          deltaBytes = DataSerializer.readByteArray(in);
        }
      }
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      DistributedRegion region = (DistributedRegion) event.getRegion();
      setDeltaFlag(region);
      super.toData(out, context);

      byte extraFlags = deserializationPolicy;
      if (eventId != null) {
        extraFlags |= HAS_EVENTID;
      }
      if (deserializationPolicy != DistributedCacheOperation.DESERIALIZATION_POLICY_NONE
          && sendDeltaWithFullValue && event.getDeltaBytes() != null) {
        extraFlags |= HAS_DELTA_WITH_FULL_VALUE;
      }
      out.writeByte(extraFlags);

      if (eventId != null) {
        InternalDataSerializer.invokeToData(eventId, out);
        if (region instanceof BucketRegion) {
          PartitionedRegion pr = region.getPartitionedRegion();
          // TODO Kishor: Since here we are talking about tail key
          // then we are surely considering Paralle Gateway
          if (!pr.isParallelWanEnabled()) {
            out.writeBoolean(false);
          } else {
            out.writeBoolean(true);
            out.writeLong(event.getTailKey());
          }
        } else {
          out.writeBoolean(false);
        }
      }
      DataSerializer.writeObject(key, out);

      if (hasDelta()) {
        DataSerializer.writeByteArray(event.getDeltaBytes(), out);
        event.getRegion().getCachePerfStats().incDeltasSent();
      } else {
        DistributedCacheOperation.writeValue(deserializationPolicy, newValueObj,
            newValue, out);
        if ((extraFlags & HAS_DELTA_WITH_FULL_VALUE) != 0) {
          DataSerializer.writeByteArray(event.getDeltaBytes(), out);
        }
      }
    }

    @Override
    public EventID getEventID() {
      return eventId;
    }

    private void setDeltaFlag(DistributedRegion region) {
      try {
        if (region != null && region.getSystem().getConfig().getDeltaPropagation() && sendDelta
            && !region.scope.isDistributedNoAck() && event.getDeltaBytes() != null) {
          setHasDelta(true);
          return;
        }
        setHasDelta(false);
      } catch (RuntimeException re) {
        throw new InvalidDeltaException(
            "Caught exception while sending delta. ",
            re);
      }
    }

    public boolean hasBridgeContext() {
      if (event != null) {
        return event.getContext() != null;
      }
      return false;
    }

    public DataPolicy getDataPolicy() {
      if (event != null) {
        return event.getRegion().getAttributes().getDataPolicy();
      }
      return null;
    }

    public void setSendDeltaWithFullValue(boolean bool) {
      sendDeltaWithFullValue = bool;
    }

    @Override
    public boolean prefersNewSerialized() {
      return true;
    }

    @Override
    public boolean isUnretainedNewReferenceOk() {
      return true;
    }

    @Override
    public void importNewObject(@Unretained(ENTRY_EVENT_NEW_VALUE) Object nv,
        boolean isSerialized) {
      if (nv == null) {
        deserializationPolicy = DESERIALIZATION_POLICY_NONE;
        newValue = null;
      } else {
        if (!isSerialized) {
          deserializationPolicy = DESERIALIZATION_POLICY_NONE;
        }
        newValueObj = nv;
      }
    }

    @Override
    public void importNewBytes(byte[] nv, boolean isSerialized) {
      if (!isSerialized) {
        deserializationPolicy = DESERIALIZATION_POLICY_NONE;
      }
      newValue = nv;
    }
  }

  public static class UpdateWithContextMessage extends UpdateMessage {

    protected transient ClientProxyMembershipID clientID;

    @Override
    @Retained
    public EntryEventImpl createEntryEvent(DistributedRegion rgn) {
      // Object oldValue = null;
      final Object argNewValue = null;
      // boolean localLoad = false, netLoad = false, netSearch = false,
      // distributed = true;
      final boolean originRemote = true, generateCallbacks = true;
      @Retained
      EntryEventImpl ev = EntryEventImpl.create(rgn, getOperation(), key, argNewValue,
          callbackArg, originRemote, getSender(), generateCallbacks);
      ev.setContext(clientID);
      setOldValueInEvent(ev);
      return ev;
      // localLoad, netLoad, netSearch,
      // distributed, this.isExpiration, originRemote, this.context);
    }

    public UpdateWithContextMessage() {}

    public UpdateWithContextMessage(UpdateWithContextMessage msg) {
      super(msg);
      clientID = msg.clientID;
    }

    @Override
    protected void appendFields(StringBuilder buff) {
      super.appendFields(buff);
      buff.append("; context=").append(clientID);
    }

    @Override
    public int getDSFID() {
      return UPDATE_WITH_CONTEXT_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      clientID = ClientProxyMembershipID.readCanonicalized(in);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeObject(clientID, out);
    }
  }

}
