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
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.partitioned.PartitionMessage;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class ClearPartitionedRegionMessage extends PartitionMessage {
  private static final Logger logger = LogService.getLogger();

  public static enum OperationType {
    OP_LOCK_FOR_CLEAR, OP_UNLOCK_FOR_CLEAR, OP_CLEAR,
  }

  private Object cbArg;

  private OperationType op;

  private EventID eventID;

  private PartitionedRegion partitionedRegion;

  private Set<InternalDistributedMember> recipients;

  @Override
  public EventID getEventID() {
    return eventID;
  }

  public ClearPartitionedRegionMessage() {}

  ClearPartitionedRegionMessage(Set recipients, PartitionedRegion region,
      ReplyProcessor21 processor, ClearPartitionedRegionMessage.OperationType operationType,
      final RegionEventImpl event) {
    super(recipients, region.getPRId(), processor);
    this.recipients = recipients;
    partitionedRegion = region;
    op = operationType;
    cbArg = event.getRawCallbackArgument();
    eventID = event.getEventId();
  }

  public OperationType getOp() {
    return op;
  }

  public void send() {
    Assert.assertTrue(recipients != null, "ClearMessage NULL recipients set");
    setTransactionDistributed(partitionedRegion.getCache().getTxManager().isDistributed());
    partitionedRegion.getDistributionManager().putOutgoing(this);
  }

  @Override
  protected Throwable processCheckForPR(PartitionedRegion pr,
      DistributionManager distributionManager) {
    if (pr != null && !pr.getDistributionAdvisor().isInitialized()) {
      Throwable thr = new ForceReattemptException(
          String.format("%s : could not find partitioned region with Id %s",
              distributionManager.getDistributionManagerId(),
              pr.getRegionIdentifier()));
      return thr;
    }
    return null;
  }


  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion r,
      long startTime) throws CacheException {

    if (r == null) {
      return true;
    }

    if (r.isDestroyed()) {
      return true;
    }

    if (op == OperationType.OP_LOCK_FOR_CLEAR) {
      r.getClearPartitionedRegion().obtainClearLockLocal(getSender());
    } else if (op == OperationType.OP_UNLOCK_FOR_CLEAR) {
      r.getClearPartitionedRegion().releaseClearLockLocal();
    } else {
      RegionEventImpl event =
          new RegionEventImpl(r, Operation.REGION_CLEAR, this.cbArg, true, r.getMyId(),
              getEventID());
      r.getClearPartitionedRegion().clearRegionLocal(event, false, null);
    }
    return true;
  }

  @Override
  protected void appendFields(StringBuilder buff) {
    super.appendFields(buff);
    buff.append("; cbArg=").append(this.cbArg).append("; op=").append(this.op);
  }

  @Override
  public int getDSFID() {
    return CLEAR_PARTITIONED_REGION_MESSAGE;
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    this.cbArg = DataSerializer.readObject(in);
    op = ClearPartitionedRegionMessage.OperationType.values()[in.readByte()];
    eventID = DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(this.cbArg, out);
    out.writeByte(op.ordinal());
    DataSerializer.writeObject(eventID, out);
  }

  /**
   * The response on which to wait for all the replies. This response ignores any exceptions
   * received from the "far side"
   *
   * @since GemFire 5.0
   */
  public static class ClearPartitionedRegionResponse extends ReplyProcessor21 {
    public ClearPartitionedRegionResponse(InternalDistributedSystem system, Set initMembers) {
      super(system, initMembers);
    }

    @Override
    protected void processException(ReplyException ex) {
      // retry on ForceReattempt in case the region is still being initialized
      if (ex.getRootCause() instanceof ForceReattemptException) {
        super.processException(ex);
      }
      // other errors are ignored
      else if (logger.isDebugEnabled()) {
        logger.debug("DestroyRegionResponse ignoring exception", ex);
      }
    }
  }

}
