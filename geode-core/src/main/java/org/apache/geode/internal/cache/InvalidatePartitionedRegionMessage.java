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

import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.query.QueryException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.cache.partitioned.PartitionMessage;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

public class InvalidatePartitionedRegionMessage extends PartitionMessage {

  private Object callbackArg;

  @Override
  public EventID getEventID() {
    return eventID;
  }

  private EventID eventID;

  public InvalidatePartitionedRegionMessage() {}

  public InvalidatePartitionedRegionMessage(Set recipients, Object callbackArg, PartitionedRegion r,
      ReplyProcessor21 processor, EventID eventID) {
    super(recipients, r.getPRId(), processor);
    this.callbackArg = callbackArg;
    this.eventID = eventID;
  }

  public static ReplyProcessor21 send(Set recipients, PartitionedRegion r, RegionEventImpl event) {
    ReplyProcessor21 response = new ReplyProcessor21(r.getSystem(), recipients);
    InvalidatePartitionedRegionMessage msg = new InvalidatePartitionedRegionMessage(recipients,
        event.getCallbackArgument(), r, response, event.getEventId());
    msg.setTransactionDistributed(r.getCache().getTxManager().isDistributed());
    r.getSystem().getDistributionManager().putOutgoing(msg);
    return response;
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.cache.partitioned.PartitionMessage#operateOnPartitionedRegion(org.
   * apache.geode.distributed.internal.DistributionManager,
   * org.apache.geode.internal.cache.PartitionedRegion, long)
   */
  @Override
  protected boolean operateOnPartitionedRegion(ClusterDistributionManager dm, PartitionedRegion pr,
      long startTime)
      throws CacheException, QueryException, ForceReattemptException, InterruptedException {

    RegionEventImpl event = new RegionEventImpl(pr, Operation.REGION_INVALIDATE, callbackArg,
        !dm.getId().equals(getSender()), getSender(), getEventID());
    pr.basicInvalidateRegion(event);
    return true;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.serialization.DataSerializableFixedID#getDSFID()
   */
  @Override
  public int getDSFID() {
    return INVALIDATE_PARTITIONED_REGION_MESSAGE;
  }

  public void fromDataPre_GEODE_1_9_0_0(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    callbackArg = context.getDeserializer().readObject(in);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.partitioned.PartitionMessage#fromData(java.io.DataInput)
   */
  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    fromDataPre_GEODE_1_9_0_0(in, context);
    eventID = context.getDeserializer().readObject(in);
  }

  public void toDataPre_GEODE_1_9_0_0(DataOutput out, SerializationContext context)
      throws IOException {
    super.toData(out, context);
    context.getSerializer().writeObject(callbackArg, out);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.cache.partitioned.PartitionMessage#toData(java.io.DataOutput)
   */
  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    toDataPre_GEODE_1_9_0_0(out, context);
    context.getSerializer().writeObject(eventID, out);
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return new KnownVersion[] {KnownVersion.GEODE_1_9_0};
  }
}
