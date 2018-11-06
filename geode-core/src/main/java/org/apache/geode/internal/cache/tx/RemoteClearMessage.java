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
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ReplySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * This message is used to implement clear. It is only used when a region is not a replica and has
 * concurrency checks enabled and some other member has a replica. In that case it sends this
 * message to the replica to ask it to do the clear.
 *
 * @since GemFire 7.0
 */
public class RemoteClearMessage extends RemoteOperationMessageWithDirectReply {
  private static final Logger logger = LogService.getLogger();

  private enum Operation {
    CLEAR,
  }

  private transient DistributedRegion region;

  public RemoteClearMessage() {}

  public static RemoteClearMessage create(InternalDistributedMember recipient,
      DistributedRegion region) {
    return new RemoteClearMessage(recipient, region, Operation.CLEAR);
  }

  private RemoteClearMessage(InternalDistributedMember recipient, DistributedRegion region,
      Operation op) {
    super(recipient, region.getFullPath(),
        new RemoteOperationResponse(region.getSystem(), recipient));
    this.region = region;
  }

  public void distribute() throws RemoteOperationException {
    RemoteOperationResponse p = (RemoteOperationResponse) this.processor;

    Set<?> failures = region.getDistributionManager().putOutgoing(this);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(String.format("Failed sending < %s >", this));
    }

    p.waitForRemoteResponse();
  }

  @Override
  protected boolean operateOnRegion(ClusterDistributionManager dm, LocalRegion r, long startTime)
      throws CacheException, RemoteOperationException {
    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace(LogMarker.DM_VERBOSE, "RemoteClearMessage operateOnRegion: {}", r.getFullPath());
    }

    r.waitOnInitialization(); // bug #43371 - accessing a region before it's initialized

    r.clear();

    RemoteClearReplyMessage.send(getSender(), getProcessorId(), getReplySender(dm));

    // Unless there was an exception thrown, this message handles sending the response
    return false;
  }

  @Override
  protected void appendFields(StringBuffer buff) {
    super.appendFields(buff);
  }

  public int getDSFID() {
    return R_CLEAR_MSG;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    in.readByte(); // for backwards compatibility
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeByte(Operation.CLEAR.ordinal()); // for backwards compatibility
  }

  public static class RemoteClearReplyMessage extends ReplyMessage {

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public RemoteClearReplyMessage() {}

    private RemoteClearReplyMessage(int processorId) {
      this.processorId = processorId;
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId,
        ReplySender replySender) {
      Assert.assertTrue(recipient != null, "RemoteClearReplyMessage NULL recipient");
      RemoteClearReplyMessage m = new RemoteClearReplyMessage(processorId);
      m.setRecipient(recipient);
      replySender.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the message.
     *
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DistributionManager dm, ReplyProcessor21 processor) {
      final long startTime = getTimestamp();

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "RemoteClearReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return R_CLEAR_MSG_REPLY;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("RemoteClearReplyMessage ").append("processorid=").append(this.processorId)
          .append(" reply to sender ").append(this.getSender());
      return sb.toString();
    }
  }
}
