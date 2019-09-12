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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.CacheException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 *
 * The primary request message signals bucket owners to re-select the primary owner of the bucket.
 * It is sent by client threads which need to use the bucket, however do not know of its primary
 * location.
 *
 *
 */
public class PrimaryRequestMessage extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 1L;

  /** The bucketId needing primary */
  private int bucketId;

  /**
   * Send request for primary election
   *
   * @param recipients those members which own the bucket
   * @param r the Partitioned Region which uses/owns the bucket
   * @param bucketId the idenity of the bucket
   * @return a response object on which the caller waits for acknowledgement of which member is the
   *         primary
   * @throws ForceReattemptException if the message was unable to be sent
   */
  public static PrimaryResponse send(Set recipients, PartitionedRegion r, int bucketId)
      throws ForceReattemptException {
    Assert.assertTrue(recipients != null, "PrimaryRequestMessage NULL recipient");
    PrimaryResponse p = new PrimaryResponse(r.getSystem(), recipients);
    PrimaryRequestMessage m = new PrimaryRequestMessage(recipients, r.getPRId(), p, bucketId);
    m.setTransactionDistributed(r.getCache().getTxManager().isDistributed());

    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException(
          String.format("Failed sending < %s >", m));
    }

    return p;
  }

  public PrimaryRequestMessage() {}

  private PrimaryRequestMessage(Set recipients, int regionId, ReplyProcessor21 processor, int bId) {
    super(recipients, regionId, processor);
    this.bucketId = bId;
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion pr,
      long startTime) throws CacheException, ForceReattemptException {
    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace(LogMarker.DM_VERBOSE, "PrimaryRequestMessage operateOnRegion: {}",
          pr.getFullPath());
    }

    pr.checkReadiness();
    final boolean isPrimary;
    // TODO, I am sure if this is the method to call to elect the primary -- mthomas 4/19/2007
    if (dm.getId().equals(pr.getBucketPrimary(this.bucketId))) {
      isPrimary = true;
    } else {
      isPrimary = false;
    }

    PrimaryRequestReplyMessage.sendReply(getSender(), getProcessorId(), isPrimary, dm);
    return false;
  }

  @Override
  public int getDSFID() {
    return PR_PRIMARY_REQUEST_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.bucketId = in.readInt();
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(this.bucketId);
  }

  @Override
  public int getProcessorType() {
    return OperationExecutors.WAITING_POOL_EXECUTOR;
  }

  /**
   * The reply to a PrimarRequestMessage, indicating if the sender is the primary
   */
  public static class PrimaryRequestReplyMessage extends ReplyMessage {
    private static final long serialVersionUID = 1L;

    public volatile boolean isPrimary;

    protected static void sendReply(InternalDistributedMember member, int procId, boolean isPrimary,
        DistributionManager dm) {
      dm.putOutgoing(new PrimaryRequestReplyMessage(member, procId, isPrimary));
    }

    public PrimaryRequestReplyMessage() {}

    private PrimaryRequestReplyMessage(InternalDistributedMember member, int procId,
        boolean isPrimary2) {
      setRecipient(member);
      setProcessorId(procId);
      this.isPrimary = isPrimary2;
    }

    @Override
    public int getDSFID() {
      return PR_PRIMARY_REQUEST_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.isPrimary = in.readBoolean();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeBoolean(this.isPrimary);
    }
  }

  /**
   * A processor to capture the member who was selected as primary for the bucket requested
   *
   * @since GemFire 5.1
   */
  public static class PrimaryResponse extends ReplyProcessor21 {
    private volatile PrimaryRequestReplyMessage msg;

    protected PrimaryResponse(InternalDistributedSystem ds, Set recipients) {
      super(ds, recipients);
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof PrimaryRequestReplyMessage) {
          PrimaryRequestReplyMessage reply = (PrimaryRequestReplyMessage) msg;
          if (reply.isPrimary) {
            this.msg = reply;
            if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
              logger.trace(LogMarker.DM_VERBOSE, "PrimaryRequestResponse primary is {}",
                  this.msg.getSender());
            }
          } else {
            if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
              logger.trace("PrimaryRequestResponse {} is not primary", this.msg.getSender());
            }
          }
        } else {
          Assert.assertTrue(msg instanceof ReplyMessage);
        }
      } finally {
        super.process(msg);
      }
    }

    public InternalDistributedMember waitForPrimary() throws ForceReattemptException {
      try {
        waitForRepliesUninterruptibly();
      } catch (ReplyException e) {
        Throwable t = e.getCause();
        if (t instanceof CancelException) {
          if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
            logger.trace(LogMarker.DM_VERBOSE,
                "NodeResponse got remote CacheClosedException, throwing PartitionedRegionCommunication Exception. {}",
                t.getMessage(), t);
          }
          throw new ForceReattemptException(
              "NodeResponse got remote CacheClosedException, throwing PartitionedRegionCommunication Exception.",
              t);
        }
        e.handleCause();
      }
      return this.msg.getSender();
    }
  }

}
