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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class FindVersionTagOperation {
  private static final Logger logger = LogService.getLogger();

  public static VersionTag findVersionTag(InternalRegion r, EventID eventId, boolean isBulkOp) {
    DistributionManager dm = r.getDistributionManager();
    Set recipients;
    if (r instanceof DistributedRegion) {
      recipients = ((DistributedRegion) r).getDistributionAdvisor().adviseCacheOp();
    } else {
      recipients = ((PartitionedRegion) r).getRegionAdvisor().adviseDataStore();
    }
    ResultReplyProcessor processor = new ResultReplyProcessor(dm, recipients);
    FindVersionTagMessage msg = new FindVersionTagMessage(recipients, processor.getProcessorId(),
        r.getFullPath(), eventId, isBulkOp);
    dm.putOutgoing(msg);
    try {
      processor.waitForReplies();
    } catch (InterruptedException e) {
      dm.getCancelCriterion().checkCancelInProgress(e);
      Thread.currentThread().interrupt();
      return null;
    }
    return processor.getVersionTag();
  }

  public static class ResultReplyProcessor extends ReplyProcessor21 {

    VersionTag versionTag;

    public ResultReplyProcessor(DistributionManager dm, Collection initMembers) {
      super(dm, initMembers);
    }

    @Override
    public void process(DistributionMessage msg) {
      if (msg instanceof VersionTagReply) {
        VersionTagReply reply = (VersionTagReply) msg;
        if (reply.versionTag != null) {
          this.versionTag = reply.versionTag;
          this.versionTag.replaceNullIDs(reply.getSender());
        }
      }
      super.process(msg);
    }

    public VersionTag getVersionTag() {
      return versionTag;
    }

    @Override
    public boolean stillWaiting() {
      return this.versionTag == null && super.stillWaiting();
    }
  }

  /**
   * FindVersionTagOperation searches other members for version information for a replayed
   * operation. If we don't have version information the op may be applied by this cache as a new
   * event. When the event is then propagated to other servers that have already seen the event it
   * will be ignored, causing an inconsistency.
   */
  public static class FindVersionTagMessage extends HighPriorityDistributionMessage
      implements MessageWithReply {

    int processorId;
    String regionName;
    EventID eventId;
    private boolean isBulkOp;

    protected FindVersionTagMessage(Collection recipients, int processorId, String regionName,
        EventID eventId, boolean isBulkOp) {
      super();
      setRecipients(recipients);
      this.processorId = processorId;
      this.regionName = regionName;
      this.eventId = eventId;
      this.isBulkOp = isBulkOp;
    }

    /** for deserialization */
    public FindVersionTagMessage() {}

    @Override
    protected void process(ClusterDistributionManager dm) {
      VersionTag result = null;
      try {
        LocalRegion r = (LocalRegion) findRegion(dm);
        if (r == null) {
          if (logger.isDebugEnabled()) {
            logger.debug("Region not found, so ignoring version tag request: {}", this);
          }
          return;
        }
        if (isBulkOp) {
          result = r.findVersionTagForClientBulkOp(eventId);

        } else {
          result = r.findVersionTagForEvent(eventId);
        }
        if (result != null) {
          result.replaceNullIDs(r.getVersionMember());
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Found version tag {}", result);
        }

      } catch (RuntimeException e) {
        logger.warn("Exception thrown while searching for a version tag", e);
      } finally {
        VersionTagReply reply = new VersionTagReply(result);
        reply.setProcessorId(this.processorId);
        reply.setRecipient(getSender());
        try {
          dm.putOutgoing(reply);
        } catch (CancelException e) {
          // can't send a reply, so ignore the exception
        }
      }
    }

    private InternalRegion findRegion(ClusterDistributionManager dm) {
      try {
        InternalCache cache = dm.getCache();
        if (cache != null) {
          return cache.getRegionByPathForProcessing(regionName);
        }
      } catch (CancelException e) {
        // nothing to do
      }
      return null;
    }

    @Override
    public int getDSFID() {
      return FIND_VERSION_TAG;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(this.processorId);
      out.writeUTF(this.regionName);
      context.getSerializer().invokeToData(this.eventId, out);
      out.writeBoolean(this.isBulkOp);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.processorId = in.readInt();
      this.regionName = in.readUTF();
      this.eventId = new EventID();
      context.getDeserializer().invokeFromData(this.eventId, in);
      this.isBulkOp = in.readBoolean();
    }

    @Override
    public String toString() {
      return this.getShortClassName() + "(processorId=" + this.processorId + ";region="
          + this.regionName + ";eventId=" + this.eventId + ";isBulkOp=" + this.isBulkOp + ")";
    }
  }

  public static class VersionTagReply extends ReplyMessage {
    VersionTag versionTag;

    VersionTagReply(VersionTag result) {
      this.versionTag = result;
    }

    /** for deserialization */
    public VersionTagReply() {}

    @Override
    public String toString() {
      return "VersionTagReply(" + this.versionTag + ")";
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeObject(this.versionTag, out);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.versionTag = (VersionTag) DataSerializer.readObject(in);
    }

    @Override
    public int getDSFID() {
      return VERSION_TAG_REPLY;
    }
  }

}
