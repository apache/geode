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
package org.apache.geode.internal.cache.wan;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InitialImageOperation;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;

public class GatewaySenderQueueEntrySynchronizationOperation {

  private InternalDistributedMember recipient;

  private InternalRegion region;

  private List<GatewaySenderQueueEntrySynchronizationEntry> entriesToSynchronize;

  private static final Logger logger = LogService.getLogger();

  protected GatewaySenderQueueEntrySynchronizationOperation(InternalDistributedMember recipient,
      InternalRegion internalRegion, List<InitialImageOperation.Entry> giiEntriesToSynchronize) {
    this.recipient = recipient;
    this.region = internalRegion;
    initializeEntriesToSynchronize(giiEntriesToSynchronize);
  }

  protected void synchronizeEntries() {
    if (logger.isDebugEnabled()) {
      logger.debug(
          "{}: Requesting synchronization from member={}; regionPath={}; entriesToSynchronize={}",
          getClass().getSimpleName(), this.recipient, this.region.getFullPath(),
          this.entriesToSynchronize);
    }
    // Create and send message
    DistributionManager dm = this.region.getDistributionManager();
    GatewaySenderQueueEntrySynchronizationReplyProcessor processor =
        new GatewaySenderQueueEntrySynchronizationReplyProcessor(dm, this.recipient, this);
    GatewaySenderQueueEntrySynchronizationMessage message =
        new GatewaySenderQueueEntrySynchronizationMessage(this.recipient,
            processor.getProcessorId(), this);
    dm.putOutgoing(message);

    // Wait for replies
    try {
      processor.waitForReplies();
    } catch (ReplyException e) {
      e.handleCause();
    } catch (InterruptedException e) {
      dm.getCancelCriterion().checkCancelInProgress(e);
      Thread.currentThread().interrupt();
    }
  }

  protected GemFireCacheImpl getCache() {
    return (GemFireCacheImpl) CacheFactory.getAnyInstance();
  }

  private void initializeEntriesToSynchronize(
      List<InitialImageOperation.Entry> giiEntriesToSynchronize) {
    this.entriesToSynchronize = new ArrayList<>();
    for (InitialImageOperation.Entry entry : giiEntriesToSynchronize) {
      this.entriesToSynchronize.add(
          new GatewaySenderQueueEntrySynchronizationEntry(entry.getKey(), entry.getVersionTag()));
    }
  }

  public static class GatewaySenderQueueEntrySynchronizationReplyProcessor
      extends ReplyProcessor21 {

    private GatewaySenderQueueEntrySynchronizationOperation operation;

    public GatewaySenderQueueEntrySynchronizationReplyProcessor(DistributionManager dm,
        InternalDistributedMember recipient,
        GatewaySenderQueueEntrySynchronizationOperation operation) {
      super(dm, recipient);
      this.operation = operation;
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof ReplyMessage) {
          ReplyMessage reply = (ReplyMessage) msg;
          if (reply.getException() == null) {
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "{}: Processing reply from member={}; regionPath={}; key={}; entriesToSynchronize={}",
                  getClass().getSimpleName(), reply.getSender(),
                  this.operation.region.getFullPath(), this.operation.entriesToSynchronize,
                  reply.getReturnValue());
            }
            List<Map<String, GatewayQueueEvent>> events =
                (List<Map<String, GatewayQueueEvent>>) reply.getReturnValue();
            for (int i = 0; i < events.size(); i++) {
              Map<String, GatewayQueueEvent> eventsForOneEntry = events.get(i);
              if (events.isEmpty()) {
                GatewaySenderQueueEntrySynchronizationEntry entry =
                    this.operation.entriesToSynchronize.get(i);
                logger.info(
                    "Synchronization event reply from member={}; regionPath={}; key={}; entryVersion={} is empty",
                    new Object[] {reply.getSender(), this.operation.region.getFullPath(),
                        entry.key,
                        entry.entryVersion});
              } else {
                putSynchronizationEvents(eventsForOneEntry);
              }
            }
          }
        }
      } finally {
        super.process(msg);
      }
    }

    private void putSynchronizationEvents(Map<String, GatewayQueueEvent> senderIdsAndEvents) {
      for (Map.Entry<String, GatewayQueueEvent> senderIdAndEvent : senderIdsAndEvents.entrySet()) {
        AbstractGatewaySender sender =
            (AbstractGatewaySender) getCache().getGatewaySender(senderIdAndEvent.getKey());
        sender.putSynchronizationEvent(senderIdAndEvent.getValue());
      }
    }

    private Cache getCache() {
      return CacheFactory.getAnyInstance();
    }
  }

  public static class GatewaySenderQueueEntrySynchronizationMessage
      extends PooledDistributionMessage implements MessageWithReply {

    private int processorId;

    private String regionPath;

    private List<GatewaySenderQueueEntrySynchronizationEntry> entriesToSynchronize;

    /* For serialization */
    public GatewaySenderQueueEntrySynchronizationMessage() {}

    protected GatewaySenderQueueEntrySynchronizationMessage(InternalDistributedMember recipient,
        int processorId, GatewaySenderQueueEntrySynchronizationOperation operation) {
      super();
      setRecipient(recipient);
      this.processorId = processorId;
      this.regionPath = operation.region.getFullPath();
      this.entriesToSynchronize = operation.entriesToSynchronize;
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      Object result = null;
      ReplyException replyException = null;
      try {
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Providing synchronization region={}; entriesToSynchronize={}",
              getClass().getSimpleName(), this.regionPath, this.entriesToSynchronize);
        }
        result = getSynchronizationEvents();
      } catch (Throwable t) {
        replyException = new ReplyException(t);
      } finally {
        ReplyMessage replyMsg = new ReplyMessage();
        replyMsg.setRecipient(getSender());
        replyMsg.setProcessorId(this.processorId);
        if (replyException == null) {
          replyMsg.setReturnValue(result);
        } else {
          replyMsg.setException(replyException);
        }
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Sending synchronization reply returnValue={}; exception={}",
              getClass().getSimpleName(), replyMsg.getReturnValue(), replyMsg.getException());
        }
        dm.putOutgoing(replyMsg);
      }
    }

    private Object getSynchronizationEvents() {
      List<Map<String, GatewayQueueEvent>> results = new ArrayList<>();
      // Get the region
      GemFireCacheImpl gfci = (GemFireCacheImpl) getCache();
      LocalRegion region = (LocalRegion) gfci.getRegion(this.regionPath);

      // Add the appropriate GatewaySenderEventImpl from each GatewaySender for each entry
      Set<String> allGatewaySenderIds = region.getAllGatewaySenderIds();
      for (GatewaySender sender : gfci.getAllGatewaySenders()) {
        if (allGatewaySenderIds.contains(sender.getId())) {
          for (GatewaySenderQueueEntrySynchronizationEntry entry : this.entriesToSynchronize) {
            Map<String, GatewayQueueEvent> resultForOneEntry = new HashMap<>();
            GatewayQueueEvent event = ((AbstractGatewaySender) sender)
                .getSynchronizationEvent(entry.key, entry.entryVersion.getVersionTimeStamp());
            if (event != null) {
              resultForOneEntry.put(sender.getId(), event);
            }
            results.add(resultForOneEntry);
          }
        }
      }

      return results;
    }

    private Cache getCache() {
      return CacheFactory.getAnyInstance();
    }

    @Override
    public int getDSFID() {
      return GATEWAY_SENDER_QUEUE_ENTRY_SYNCHRONIZATION_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.processorId);
      DataSerializer.writeString(this.regionPath, out);
      DataSerializer.writeArrayList((ArrayList) this.entriesToSynchronize, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
      this.regionPath = DataSerializer.readString(in);
      this.entriesToSynchronize = DataSerializer.readArrayList(in);
    }
  }

  public static class GatewaySenderQueueEntrySynchronizationEntry
      implements DataSerializableFixedID {

    private Object key;

    private VersionTag entryVersion;

    /* For serialization */
    public GatewaySenderQueueEntrySynchronizationEntry() {}

    public GatewaySenderQueueEntrySynchronizationEntry(Object key, VersionTag entryVersion) {
      this.key = key;
      this.entryVersion = entryVersion;
    }

    @Override
    public int getDSFID() {
      return GATEWAY_SENDER_QUEUE_ENTRY_SYNCHRONIZATION_ENTRY;
    }

    @Override
    public Version[] getSerializationVersions() {
      return null;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeObject(this.key, out);
      DataSerializer.writeObject(this.entryVersion, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.key = DataSerializer.readObject(in);
      this.entryVersion = DataSerializer.readObject(in);
    }

    @Override
    public String toString() {
      return new StringBuilder().append(getClass().getSimpleName()).append("[").append("key=")
          .append(this.key).append("; entryVersion=").append(this.entryVersion).append("]")
          .toString();
    }
  }
}
