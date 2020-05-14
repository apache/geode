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

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.InternalRegion;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * This message is used by a transaction to determine the size of a region on a remote member.
 *
 * @since GemFire 5.0
 */
public class RemoteSizeMessage extends RemoteOperationMessage {
  private static final Logger logger = LogService.getLogger();

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public RemoteSizeMessage() {}

  /**
   * The message sent to a member to get the size of their region
   *
   * @param recipient member to send the message to
   * @param regionPath the path to the region
   * @param processor the reply processor used to wait on the response
   */
  private RemoteSizeMessage(DistributedMember recipient, String regionPath,
      ReplyProcessor21 processor) {
    super((InternalDistributedMember) recipient, regionPath, processor);
  }

  public RemoteSizeMessage(DataInput in) throws IOException, ClassNotFoundException {
    fromData(in, InternalDataSerializer.createDeserializationContext(in));
  }

  /**
   * Sends a message for {@link java.util.Map#size()} ignoring any errors on send
   *
   * @param distributedMember the set of members that the size message is sent to
   * @param r the Region to get the size of
   * @return the processor used to read the returned size
   */
  public static SizeResponse send(DistributedMember distributedMember, InternalRegion r) {
    Assert.assertTrue(distributedMember != null, "RemoteSizeMessage NULL recipients set");
    SizeResponse p = new SizeResponse(r.getSystem(), distributedMember);
    RemoteSizeMessage m = new RemoteSizeMessage(distributedMember, r.getFullPath(), p);
    r.getDistributionManager().putOutgoing(m);
    return p;
  }

  @Override
  public boolean canStartRemoteTransaction() {
    return false;
  }

  @Override
  protected boolean operateOnRegion(ClusterDistributionManager dm, LocalRegion r, long startTime)
      throws RemoteOperationException {
    int size = r.size();
    SizeReplyMessage.send(getSender(), getProcessorId(), dm, size);
    return false;
  }

  @Override
  protected void appendFields(StringBuffer buff) {
    super.appendFields(buff);
  }

  @Override
  public int getDSFID() {
    return R_SIZE_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    DataSerializer.readArrayList(in); /* read unused data for backwards compatibility */
    in.readByte(); /* read unused data for backwards compatibility */
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeArrayList(null, out);
    out.writeByte(0);
  }

  public static class SizeReplyMessage extends ReplyMessage {
    private int size;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public SizeReplyMessage() {}

    private SizeReplyMessage(int processorId, int size) {
      this.processorId = processorId;
      this.size = size;
    }

    public SizeReplyMessage(DataInput in) throws IOException, ClassNotFoundException {
      fromData(in, InternalDataSerializer.createDeserializationContext(in));
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId,
        DistributionManager dm, int size) {
      Assert.assertTrue(recipient != null, "SizeReplyMessage NULL reply message");
      SizeReplyMessage m = new SizeReplyMessage(processorId, size);
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    /**
     * Processes this message. This method is invoked by the receiver of the message.
     *
     * @param dm the distribution manager that is processing the message.
     */
    @Override
    public void process(final DistributionManager dm, final ReplyProcessor21 processor) {
      final long startTime = getTimestamp();
      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE,
            "{}: process invoking reply processor with processorId: {}", this.processorId);
      }

      if (processor == null) {
        if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
          logger.trace(LogMarker.DM_VERBOSE, "{} processor not found", getClass().getName());
        }
        return;
      }
      processor.process(this);

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} processed {}", getClass().getName(), this);
      }
      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(size);
    }

    @Override
    public int getDSFID() {
      return R_SIZE_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.size = in.readInt();
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append(this.getClass().getName()).append(" processorid=").append(this.processorId)
          .append(" reply to sender ").append(this.getSender()).append(" returning size=")
          .append(getSize());
      return sb.toString();
    }

    public int getSize() {
      return size;
    }
  }

  /**
   * A processor to capture the value returned by RemoteSizeMessage
   *
   * @since GemFire 5.0
   */
  public static class SizeResponse extends ReplyProcessor21 {
    private int returnValue;

    public SizeResponse(InternalDistributedSystem ds, DistributedMember recipient) {
      super(ds, (InternalDistributedMember) recipient);
    }

    // Note that this causes GEODE-4612 and should be removed
    @Override
    protected synchronized void processException(ReplyException ex) {
      logger.debug("SizeResponse ignoring exception: {}", ex.getMessage(), ex);
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof SizeReplyMessage) {
          SizeReplyMessage reply = (SizeReplyMessage) msg;
          this.returnValue = reply.getSize();
        }
      } finally {
        super.process(msg);
      }
    }

    /**
     * @return wait for and return the size
     */
    public int waitForSize() {
      try {
        waitForRepliesUninterruptibly();
      } catch (ReplyException e) {
        Throwable cause = e.getCause();
        if (cause instanceof RegionDestroyedException) {
          RegionDestroyedException rde = (RegionDestroyedException) cause;
          throw rde;
        }
        throw e;
      }
      return this.returnValue;
    }
  }

}
