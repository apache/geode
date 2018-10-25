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

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DirectReplyProcessor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.ReplySender;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RemoteOperationException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * This message is used be a replicate region to send a contains key/value request to another peer.
 *
 * @since GemFire 6.5
 */
public class RemoteContainsKeyValueMessage extends RemoteOperationMessageWithDirectReply {
  private static final Logger logger = LogService.getLogger();

  private boolean valueCheck;

  private Object key;

  protected static final short VALUE_CHECK = UNRESERVED_FLAGS_START;

  public RemoteContainsKeyValueMessage() {}

  public RemoteContainsKeyValueMessage(InternalDistributedMember recipient, String regionPath,
      DirectReplyProcessor processor, Object key, boolean valueCheck) {
    super(recipient, regionPath, processor);
    this.valueCheck = valueCheck;
    this.key = key;
  }

  /**
   * Sends a ReplicateRegion message for either
   * {@link org.apache.geode.cache.Region#containsKey(Object)}or
   * {@link org.apache.geode.cache.Region#containsValueForKey(Object)} depending on the
   * <code>valueCheck</code> argument
   *
   * @param recipient the member that the contains keys/value message is sent to
   * @param r the LocalRegion
   * @param key the key to be queried
   * @param valueCheck true if {@link org.apache.geode.cache.Region#containsValueForKey(Object)} is
   *        desired, false if {@link org.apache.geode.cache.Region#containsKey(Object)}is desired
   * @return the processor used to read the returned keys
   */
  public static RemoteContainsKeyValueResponse send(InternalDistributedMember recipient,
      LocalRegion r, Object key, boolean valueCheck) throws RemoteOperationException {
    Assert.assertTrue(recipient != null, "recipient can not be NULL");

    RemoteContainsKeyValueResponse p =
        new RemoteContainsKeyValueResponse(r.getSystem(), recipient, key);
    RemoteContainsKeyValueMessage m =
        new RemoteContainsKeyValueMessage(recipient, r.getFullPath(), p, key, valueCheck);

    Set<?> failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new RemoteOperationException(String.format("Failed sending < %s >", m));
    }
    return p;
  }

  @Override
  protected boolean operateOnRegion(ClusterDistributionManager dm, LocalRegion r, long startTime)
      throws CacheException, RemoteOperationException {
    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace(LogMarker.DM_VERBOSE,
          "DistributedRemoteContainsKeyValueMessage operateOnRegion: {}", r.getFullPath());
    }

    if (!(r instanceof PartitionedRegion)) { // prs already wait on initialization
      r.waitOnInitialization(); // bug #43371 - accessing a region before it's initialized
    }

    final boolean replyVal;
    if (this.valueCheck) {
      replyVal = r.containsValueForKey(this.key);
    } else {
      replyVal = r.containsKey(this.key);
    }

    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace(
          "DistributedRemoteContainsKeyValueMessage sending reply back using processorId: {}",
          getProcessorId());
    }

    RemoteContainsKeyValueReplyMessage.send(getSender(), getProcessorId(), getReplySender(dm),
        replyVal);

    // Unless there was an exception thrown, this message handles sending the
    // response
    return false;
  }

  @Override
  protected void appendFields(StringBuffer buff) {
    super.appendFields(buff);
    buff.append("; valueCheck=").append(this.valueCheck).append("; key=").append(this.key);
  }

  public int getDSFID() {
    return R_CONTAINS_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.key = DataSerializer.readObject(in);
    this.valueCheck = (flags & VALUE_CHECK) != 0;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.key, out);
  }

  @Override
  protected short computeCompressedShort() {
    short flags = super.computeCompressedShort();
    if (this.valueCheck)
      flags |= VALUE_CHECK;
    return flags;
  }

  public static class RemoteContainsKeyValueReplyMessage extends ReplyMessage {

    /** Propagated exception from remote node to operation initiator */
    private boolean containsKeyValue;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public RemoteContainsKeyValueReplyMessage() {}

    private RemoteContainsKeyValueReplyMessage(int processorId, boolean containsKeyValue) {
      this.processorId = processorId;
      this.containsKeyValue = containsKeyValue;
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId,
        ReplySender replySender, boolean containsKeyValue) {
      Assert.assertTrue(recipient != null, "ContainsKeyValueReplyMessage NULL reply message");
      RemoteContainsKeyValueReplyMessage m =
          new RemoteContainsKeyValueReplyMessage(processorId, containsKeyValue);
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
          logger.trace(LogMarker.DM_VERBOSE, "ContainsKeyValueReplyMessage processor not found");
        }
        return;
      }
      processor.process(this);

      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return R_CONTAINS_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.containsKeyValue = in.readBoolean();
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeBoolean(this.containsKeyValue);
    }

    @Override
    public String toString() {
      StringBuffer sb = new StringBuffer();
      sb.append("ContainsKeyValueReplyMessage ").append("processorid=").append(this.processorId)
          .append(" reply to sender ").append(this.getSender())
          .append(" returning containsKeyValue=").append(doesItContainKeyValue());
      return sb.toString();
    }

    public boolean doesItContainKeyValue() {
      return this.containsKeyValue;
    }
  }

  /**
   * A processor to capture the value returned by
   * {@link org.apache.geode.internal.cache.tx.RemoteContainsKeyValueMessage.RemoteContainsKeyValueReplyMessage}
   *
   * @since GemFire 6.5
   */
  public static class RemoteContainsKeyValueResponse extends RemoteOperationResponse {
    private volatile boolean returnValue;
    private volatile boolean returnValueReceived;
    final Object key;

    public RemoteContainsKeyValueResponse(InternalDistributedSystem ds,
        InternalDistributedMember recipient, Object key) {
      super(ds, recipient, false);
      this.key = key;
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof RemoteContainsKeyValueReplyMessage) {
          RemoteContainsKeyValueReplyMessage reply = (RemoteContainsKeyValueReplyMessage) msg;
          this.returnValue = reply.doesItContainKeyValue();
          this.returnValueReceived = true;
          if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
            logger.trace(LogMarker.DM_VERBOSE, "ContainsKeyValueResponse return value is {}",
                this.returnValue);
          }
        }
      } finally {
        super.process(msg);
      }
    }

    /**
     * @return Set the keys associated with the ReplicateRegion of the
     *         {@link RemoteContainsKeyValueMessage}
     */
    public boolean waitForContainsResult() throws RemoteOperationException {
      try {
        waitForRemoteResponse();
      } catch (CacheException ce) {
        logger.debug("ContainsKeyValueResponse got remote CacheException", ce);
        throw new RemoteOperationException(
            "RemoteContainsKeyResponse got remote CacheException; triggering RemoteOperationException.",
            ce);
      }
      if (!this.returnValueReceived) {
        throw new RemoteOperationException(
            "no return value received");
      }
      return this.returnValue;
    }
  }

}
