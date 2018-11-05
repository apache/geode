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
import java.util.Collections;
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
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.PrimaryBucketException;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

public class ContainsKeyValueMessage extends PartitionMessageWithDirectReply {
  private static final Logger logger = LogService.getLogger();

  private boolean valueCheck;

  private Object key;

  private Integer bucketId;

  public ContainsKeyValueMessage() {
    super();
  }

  public ContainsKeyValueMessage(InternalDistributedMember recipient, int regionId,
      DirectReplyProcessor processor, Object key, Integer bucketId, boolean valueCheck) {
    super(recipient, regionId, processor);
    this.valueCheck = valueCheck;
    this.key = key;
    this.bucketId = bucketId;
  }

  /**
   * Sends a PartitionedRegion message for either
   * {@link org.apache.geode.cache.Region#containsKey(Object)}or
   * {@link org.apache.geode.cache.Region#containsValueForKey(Object)} depending on the
   * <code>valueCheck</code> argument
   *
   * @param recipient the member that the contains keys/value message is sent to
   * @param r the PartitionedRegion that contains the bucket
   * @param key the key to be queried
   * @param bucketId the identity of the bucket to be queried
   * @param valueCheck true if {@link org.apache.geode.cache.Region#containsValueForKey(Object)} is
   *        desired, false if {@link org.apache.geode.cache.Region#containsKey(Object)}is desired
   * @return the processor used to read the returned keys
   * @throws ForceReattemptException if the peer is no longer available
   */
  public static ContainsKeyValueResponse send(InternalDistributedMember recipient,
      PartitionedRegion r, Object key, Integer bucketId, boolean valueCheck)
      throws ForceReattemptException {
    Assert.assertTrue(recipient != null, "PRDistribuedContainsKeyValueMessage NULL reply message");

    ContainsKeyValueResponse p =
        new ContainsKeyValueResponse(r.getSystem(), Collections.singleton(recipient), key);
    ContainsKeyValueMessage m =
        new ContainsKeyValueMessage(recipient, r.getPRId(), p, key, bucketId, valueCheck);
    m.setTransactionDistributed(r.getCache().getTxManager().isDistributed());

    Set failures = r.getDistributionManager().putOutgoing(m);
    if (failures != null && failures.size() > 0) {
      throw new ForceReattemptException(
          String.format("Failed sending < %s >", m));
    }
    return p;
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion r,
      long startTime) throws CacheException, ForceReattemptException {
    PartitionedRegionDataStore ds = r.getDataStore();
    final boolean replyVal;
    if (ds != null) {
      try {
        if (this.valueCheck) {
          replyVal = ds.containsValueForKeyLocally(this.bucketId, this.key);
        } else {
          replyVal = ds.containsKeyLocally(this.bucketId, this.key);
        }
      } catch (PRLocallyDestroyedException pde) {
        throw new ForceReattemptException(
            "Enountered PRLocallyDestroyedException",
            pde);
      }

      r.getPrStats().endPartitionMessagesProcessing(startTime);
      ContainsKeyValueReplyMessage.send(getSender(), getProcessorId(), getReplySender(dm),
          replyVal);
    } else {
      logger.fatal("Partitioned Region <> is not configured to store data",
          r.getFullPath());
      ForceReattemptException fre = new ForceReattemptException(
          String.format("Partitioned Region %s on %s is not configured to store data",
              new Object[] {r.getFullPath(), dm.getId()}));
      fre.setHash(key.hashCode());
      throw fre;
    }

    // Unless there was an exception thrown, this message handles sending the
    // response
    return false;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; valueCheck=").append(this.valueCheck).append("; key=").append(this.key)
        .append("; bucketId=").append(this.bucketId);
  }

  public int getDSFID() {
    return PR_CONTAINS_KEY_VALUE_MESSAGE;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.key = DataSerializer.readObject(in);
    this.valueCheck = in.readBoolean();
    this.bucketId = Integer.valueOf(in.readInt());
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.key, out);
    out.writeBoolean(this.valueCheck);
    out.writeInt(this.bucketId.intValue());
  }

  public static class ContainsKeyValueReplyMessage extends ReplyMessage {

    /** Propagated exception from remote node to operation initiator */
    private boolean containsKeyValue;

    /**
     * Empty constructor to conform to DataSerializable interface
     */
    public ContainsKeyValueReplyMessage() {}

    private ContainsKeyValueReplyMessage(int processorId, boolean containsKeyValue) {
      this.processorId = processorId;
      this.containsKeyValue = containsKeyValue;
    }

    /** Send an ack */
    public static void send(InternalDistributedMember recipient, int processorId,
        ReplySender replySender, boolean containsKeyValue) {
      Assert.assertTrue(recipient != null, "ContainsKeyValueReplyMessage NULL reply message");
      ContainsKeyValueReplyMessage m =
          new ContainsKeyValueReplyMessage(processorId, containsKeyValue);
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

      if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
        logger.trace(LogMarker.DM_VERBOSE, "{} Processed {}", processor, this);
      }
      dm.getStats().incReplyMessageTime(DistributionStats.getStatTime() - startTime);
    }

    @Override
    public int getDSFID() {
      return PR_CONTAINS_KEY_VALUE_REPLY_MESSAGE;
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
          .append(" returning ").append(doesItContainKeyValue());
      return sb.toString();
    }

    public boolean doesItContainKeyValue() {
      return this.containsKeyValue;
    }
  }

  /**
   * A processor to capture the value returned by
   * {@link org.apache.geode.internal.cache.partitioned.ContainsKeyValueMessage.ContainsKeyValueReplyMessage}
   *
   * @since GemFire 5.0
   */
  public static class ContainsKeyValueResponse extends PartitionResponse {
    private volatile boolean returnValue;
    private volatile boolean returnValueReceived;
    final Object key;

    public ContainsKeyValueResponse(InternalDistributedSystem ds, Set recipients, Object key) {
      super(ds, recipients, false);
      this.key = key;
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof ContainsKeyValueReplyMessage) {
          ContainsKeyValueReplyMessage reply = (ContainsKeyValueReplyMessage) msg;
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
     * @return Set the keys associated with the bucketid of the {@link ContainsKeyValueMessage}
     * @throws ForceReattemptException if the peer is no longer available
     * @throws PrimaryBucketException if the instance of the bucket that received this operation was
     *         not primary
     */
    public boolean waitForContainsResult() throws PrimaryBucketException, ForceReattemptException {
      try {
        waitForCacheException();
      } catch (ForceReattemptException rce) {
        rce.checkKey(key);
        throw rce;
      } catch (CacheException ce) {
        logger.debug("ContainsKeyValueResponse got remote CacheException; forcing reattempt. {}",
            ce.getMessage(), ce);
        throw new ForceReattemptException(
            "ContainsKeyValueResponse got remote CacheException; forcing reattempt.",
            ce);
      }
      if (!this.returnValueReceived) {
        throw new ForceReattemptException(
            "no return value received");
      }
      return this.returnValue;
    }
  }

}
