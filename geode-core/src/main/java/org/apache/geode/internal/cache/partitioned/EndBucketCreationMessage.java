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
import java.util.Collection;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.ForceReattemptException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * This message is sent to a member to make it attempt to become primary. This message is sent at
 * the end of PRHARedundancyProvider .createBucketAtomically, because the buckets created during
 * that time do not volunteer for primary until receiving this message.
 *
 */
public class EndBucketCreationMessage extends PartitionMessage {

  private int bucketId;
  private InternalDistributedMember newPrimary;

  /**
   * Empty constructor to satisfy {@link DataSerializer} requirements
   */
  public EndBucketCreationMessage() {}

  private EndBucketCreationMessage(Collection<InternalDistributedMember> recipients, int regionId,
      ReplyProcessor21 processor, int bucketId, InternalDistributedMember newPrimary) {
    super(recipients, regionId, processor);
    this.bucketId = bucketId;
    this.newPrimary = newPrimary;
  }

  /**
   * Sends a message to make the recipient primary for the bucket.
   *
   *
   * @param newPrimary the member to to become primary
   * @param pr the PartitionedRegion of the bucket
   * @param bid the bucket to become primary for
   */
  public static void send(Collection<InternalDistributedMember> acceptedMembers,
      InternalDistributedMember newPrimary, PartitionedRegion pr, int bid) {

    Assert.assertTrue(newPrimary != null, "VolunteerPrimaryBucketMessage NULL recipient");

    ReplyProcessor21 response = new ReplyProcessor21(pr.getSystem(), acceptedMembers);
    EndBucketCreationMessage msg =
        new EndBucketCreationMessage(acceptedMembers, pr.getPRId(), response, bid, newPrimary);
    msg.setTransactionDistributed(pr.getCache().getTxManager().isDistributed());

    pr.getDistributionManager().putOutgoing(msg);
  }

  public EndBucketCreationMessage(DataInput in) throws IOException, ClassNotFoundException {
    fromData(in, InternalDataSerializer.createDeserializationContext(in));
  }

  @Override
  public int getProcessorType() {
    // use the waiting pool because operateOnPartitionedRegion will
    // try to get a dlock
    return OperationExecutors.WAITING_POOL_EXECUTOR;
  }

  @Override
  public boolean isSevereAlertCompatible() {
    // allow forced-disconnect processing for all cache op messages
    return true;
  }

  @Override
  public boolean canParticipateInTransaction() {
    return false;
  }

  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm,
      PartitionedRegion region, long startTime) throws ForceReattemptException {

    // this is executing in the WAITING_POOL_EXECUTOR

    try {
      region.getRedundancyProvider().endBucketCreationLocally(bucketId, newPrimary);

    } finally {
      region.getPrStats().endPartitionMessagesProcessing(startTime);
    }

    return false;

  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; bucketId=").append(this.bucketId);
    buff.append("; newPrimary=").append(this.newPrimary);
  }

  @Override
  public int getDSFID() {
    return END_BUCKET_CREATION_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.bucketId = in.readInt();
    newPrimary = new InternalDistributedMember();
    InternalDataSerializer.invokeFromData(newPrimary, in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeInt(this.bucketId);
    InternalDataSerializer.invokeToData(newPrimary, out);
  }
}
