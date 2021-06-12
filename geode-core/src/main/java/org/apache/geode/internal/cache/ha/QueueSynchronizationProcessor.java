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
package org.apache.geode.internal.cache.ha;

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.HARegion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Synchronize queue with primary holder
 */
public class QueueSynchronizationProcessor extends ReplyProcessor21 {
  private static final Logger logger = LogService.getLogger();

  QueueSynchronizationReplyMessage reply;

  QueueSynchronizationProcessor(DistributionManager dm, InternalDistributedMember primary) {
    super(dm, primary);
  }

  static List<EventID> getDispatchedEvents(final DistributionManager dm,
      final InternalDistributedMember primary, String regionName, List<EventID> events) {
    QueueSynchronizationProcessor processor = new QueueSynchronizationProcessor(dm, primary);

    QueueSynchronizationMessage message = new QueueSynchronizationMessage();
    message.setEventIdList(events);
    message.setProcessorId(processor.getProcessorId());
    message.setRegionName(regionName);

    message.setRecipient(primary);
    dm.putOutgoing(message);

    try {
      processor.waitForRepliesUninterruptibly();
    } catch (ReplyException e) {
      e.handleCause();
    }

    if (processor.reply != null) {
      return processor.reply.getDispatchedEvents();
    }
    // Failed to get reply.
    return null;
  }

  @Override
  public void process(DistributionMessage msg) {
    try {
      reply = (QueueSynchronizationReplyMessage) msg;
    } finally {
      super.process(msg);
    }
  }

  // -------------------------------------------------------------------------
  // QueueSynchronizationMessage
  // This message is sent to the the primary queue holder after receiving QueueRemovalMessage.
  // It contains the list of eventIDs that has been GIIed but not yet removed.
  // The primary queue holder will check and sent back a list of event ids that has been dispatched.
  // -------------------------------------------------------------------------
  public static class QueueSynchronizationMessage extends PooledDistributionMessage implements
      MessageWithReply {
    private List<EventID> eventIds;

    private int processorId;
    private String regionName;

    public QueueSynchronizationMessage() {}

    void setEventIdList(List<EventID> message) {
      eventIds = message;
    }

    void setRegionName(String regionName) {
      this.regionName = regionName;
    }

    void setProcessorId(int processorId) {
      this.processorId = processorId;
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      final QueueSynchronizationReplyMessage replyMessage =
          createQueueSynchronizationReplyMessage();
      ReplyException replyException = null;

      final InternalCache cache = dm.getCache();
      try {
        if (cache != null) {
          List<EventID> dispatched = getDispatchedEvents(cache);
          if (dispatched != null) {
            replyMessage.setEventIds(dispatched);
            replyMessage.setSuccess();
          }
        }
      } catch (RuntimeException | Error e) {
        replyException = new ReplyException(e);
        throw e;
      } finally {
        replyMessage.setProcessorId(processorId);
        replyMessage.setRecipient(getSender());
        if (replyException != null) {
          replyMessage.setException(replyException);
        }
        dm.putOutgoing(replyMessage);
      }
    }

    QueueSynchronizationReplyMessage createQueueSynchronizationReplyMessage() {
      return new QueueSynchronizationReplyMessage();
    }

    List<EventID> getDispatchedEvents(InternalCache cache) {
      LocalRegion region = (LocalRegion) cache.getRegion(regionName);
      if (region == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("processing QueueSynchronizationMessage region {} does not exist.",
              regionName);
        }
        return null;
      }
      HARegionQueue haRegionQueue = ((HARegion) region).getOwner();
      return haRegionQueue.getDispatchedEvents(eventIds);
    }

    @Override
    public int getDSFID() {
      return QUEUE_SYNCHRONIZATION_MESSAGE;
    }

    @Override
    public int getProcessorId() {
      return processorId;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeString(regionName, out);
      DataSerializer.writeInteger(processorId, out);
      int numberOfIds = eventIds.size();
      DataSerializer.writeInteger(numberOfIds, out);
      for (EventID eventId : eventIds) {
        DataSerializer.writeObject(eventId, out);
      }
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context)
        throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      eventIds = new LinkedList<>();

      regionName = DataSerializer.readString(in);
      processorId = DataSerializer.readInteger(in);
      int size = DataSerializer.readInteger(in);
      for (int i = 0; i < size; i++) {
        eventIds.add(uncheckedCast(DataSerializer.readObject(in)));
      }
    }
  }

  // -------------------------------------------------------------------------
  // QueueSynchronizationReplyMessage
  // -------------------------------------------------------------------------
  public static class QueueSynchronizationReplyMessage extends ReplyMessage {
    private List<EventID> events;
    private boolean succeed = false;

    public QueueSynchronizationReplyMessage() {}

    void setEventIds(List<EventID> events) {
      this.events = events;
    }

    List<EventID> getDispatchedEvents() {
      return events;
    }

    void setSuccess() {
      succeed = true;
    }

    @Override
    public int getDSFID() {
      return QUEUE_SYNCHRONIZATION_REPLY_MESSAGE;
    }

    @Override
    public void toData(DataOutput out, SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeBoolean(succeed, out);
      if (succeed) {
        DataSerializer.writeInteger(events.size(), out);
        for (EventID eventId : events) {
          DataSerializer.writeObject(eventId, out);
        }
      }
    }

    @Override
    public void fromData(DataInput in, DeserializationContext context)
        throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      succeed = DataSerializer.readBoolean(in);
      if (succeed) {
        events = new LinkedList<>();
        int size = DataSerializer.readInteger(in);
        for (int i = 0; i < size; i++) {
          events.add(uncheckedCast(DataSerializer.readObject(in)));
        }
      }
    }
  }
}
