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

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.CacheRuntimeException;
import org.apache.geode.cache.Scope;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.SerialDistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * A Partitioned Region specific message whose reply guarantees that all operations have completed
 * for a given Partitioned Region's bucket.
 *
 * <p>
 * Currently this message does not support conserve-sockets=false, that is it only flushes the
 * shared communication channels.
 * </p>
 *
 * <p>
 * This messages implementation is unique in that it uses another instance of itself as the reply.
 * This was to leverage the fact that the message is a
 * {@link org.apache.geode.distributed.internal.SerialDistributionMessage}.
 * </p>
 *
 * @since GemFire 5.1
 */
public class FlushMessage extends SerialDistributionMessage implements MessageWithReply {
  private static final Logger logger = LogService.getLogger();

  private static final long serialVersionUID = 1L;

  int prId;
  int bucketId;
  int processorId;

  public FlushMessage() {}

  private FlushMessage(int prId, int bucketId, int processorId,
      InternalDistributedMember recipient) {
    this.prId = prId;
    this.bucketId = bucketId;
    this.processorId = processorId;
    setRecipient(recipient);
  }

  /*
   * Used both for the receipt of a FlushMessage and the reply to a Flushmessage
   */
  @Override
  protected void process(ClusterDistributionManager dm) {
    if (bucketId != Integer.MIN_VALUE) {
      if (logger.isDebugEnabled()) {
        logger.debug("Received sent FlushMessage {}", this);
      }
      try {
        final PartitionedRegion p = PartitionedRegion.getPRFromId(prId);
        Assert.assertTrue(p.getRegionAdvisor().isPrimaryForBucket(bucketId));
      } catch (PRLocallyDestroyedException fre) {
        if (logger.isDebugEnabled()) {
          logger.debug("Sending reply despite Region getting locally destroyed prId={}", prId,
              fre);
        }
      } catch (CacheRuntimeException ce) {
        logger.debug("Sending reply despite unavailable Partitioned Region using prId={}",
            prId, ce);
      } finally {
        dm.putOutgoing(
            new FlushMessage(prId, Integer.MIN_VALUE, getProcessorId(), getSender()));
      }
    } else {
      if (logger.isDebugEnabled()) {
        logger.debug("Processing FlushMessage as a response {}", this);
      }

      ReplyProcessor21 rep = ReplyProcessor21.getProcessor(processorId);
      if (rep != null) {
        rep.process(this);
      }
    }
  }

  /**
   * Send this message to the bucket primary, after the
   * {@link ReplyProcessor21#waitForRepliesUninterruptibly()} returns, all updates from the primary
   * should be complete. Use this from a host of a backup bucket (aka secondary) when the update
   * operations originating from the primary {@link Scope#DISTRIBUTED_NO_ACK do not require an
   * acknowldgement}
   *
   * @return a processor on which to wait for the flush operation to complete
   */
  public static ReplyProcessor21 send(InternalDistributedMember primary, PartitionedRegion p,
      int bucketId) {
    ReplyProcessor21 reply = new ReplyProcessor21(p.getDistributionManager(), primary);
    FlushMessage fm = new FlushMessage(p.getPRId(), bucketId, reply.getProcessorId(), primary);
    p.getDistributionManager().putOutgoing(fm);
    return reply;
  }

  @Override
  public int getProcessorId() {
    return processorId;
  }

  @Override
  public int getDSFID() {
    return PR_FLUSH_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    prId = in.readInt();
    bucketId = in.readInt();
    processorId = in.readInt();
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    // TODO Auto-generated method stub
    super.toData(out, context);
    out.writeInt(prId);
    out.writeInt(bucketId);
    out.writeInt(processorId);
  }



}
