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

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.OperationExecutors;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message sent to a data store telling that data store to globally destroy the region on behalf
 * of a PR accessor.
 *
 * @since GemFire 5.0
 */
public class DestroyRegionOnDataStoreMessage extends PartitionMessage {

  private Object callbackArg;

  /**
   * Empty contstructor provided for {@link org.apache.geode.DataSerializer}
   */
  public DestroyRegionOnDataStoreMessage() {
    super();
  }

  private DestroyRegionOnDataStoreMessage(InternalDistributedMember recipient, int regionId,
      ReplyProcessor21 rp, Object callbackArg) {
    super(recipient, regionId, rp);
    this.callbackArg = callbackArg;
  }

  /**
   * Sends a DestroyRegionOnDataStoreMessage requesting that another VM destroy an existing region
   *
   */
  public static void send(InternalDistributedMember recipient, PartitionedRegion r,
      Object callbackArg) {
    DistributionManager dm = r.getDistributionManager();
    ReplyProcessor21 rp = new ReplyProcessor21(dm, recipient);
    int procId = rp.getProcessorId();
    DestroyRegionOnDataStoreMessage m =
        new DestroyRegionOnDataStoreMessage(recipient, r.getPRId(), rp, callbackArg);
    m.setTransactionDistributed(r.getCache().getTxManager().isDistributed());
    r.getDistributionManager().putOutgoing(m);
    rp.waitForRepliesUninterruptibly();
  }

  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion pr,
      long startTime) throws CacheException {

    // This call has come to an uninitialized region.
    if (pr == null || !pr.isInitialized()) {
      return true;
    }


    org.apache.logging.log4j.Logger logger = pr.getLogger();
    if (logger.isTraceEnabled(LogMarker.DM_VERBOSE)) {
      logger.trace(LogMarker.DM_VERBOSE,
          "DestroyRegionOnDataStore operateOnRegion: " + pr.getFullPath());
    }
    pr.destroyRegion(callbackArg);
    return true;
  }

  @Override
  public int getProcessorType() {
    return OperationExecutors.WAITING_POOL_EXECUTOR;
  }

  @Override
  public int getDSFID() {
    return PR_DESTROY_ON_DATA_STORE_MESSAGE;
  }

  @Override
  public void fromData(final DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    callbackArg = DataSerializer.readObject(in);
  }

  @Override
  public void toData(final DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(callbackArg, out);
  }
}
