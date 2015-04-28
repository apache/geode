/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.internal.cache.partitioned.PartitionMessage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

/**
 * @author sbawaska
 *
 */
public class InvalidatePartitionedRegionMessage extends PartitionMessage {

  private Object callbackArg;

  /**
   * 
   */
  public InvalidatePartitionedRegionMessage() {
  }

  public InvalidatePartitionedRegionMessage(Set recipients, Object callbackArg,
      PartitionedRegion r, ReplyProcessor21 processor) {
    super(recipients, r.getPRId(), processor);
    this.callbackArg = callbackArg;
  }

  public static ReplyProcessor21 send(Set recipients, PartitionedRegion r, RegionEventImpl event) {
    ReplyProcessor21 response = 
          new ReplyProcessor21(r.getSystem(), recipients);
    InvalidatePartitionedRegionMessage msg = new InvalidatePartitionedRegionMessage(recipients,
        event.getCallbackArgument(), r, response);
    r.getSystem().getDistributionManager().putOutgoing(msg);
    return response;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.partitioned.PartitionMessage#operateOnPartitionedRegion(com.gemstone.gemfire.distributed.internal.DistributionManager, com.gemstone.gemfire.internal.cache.PartitionedRegion, long)
   */
  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm,
      PartitionedRegion pr, long startTime) throws CacheException,
      QueryException, ForceReattemptException, InterruptedException {
    
    RegionEventImpl event = new RegionEventImpl(pr,Operation.REGION_INVALIDATE,
        this.callbackArg, !dm.getId().equals(getSender()), getSender());
    pr.basicInvalidateRegion(event);
    return true;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.DataSerializableFixedID#getDSFID()
   */
  public int getDSFID() {
    return INVALIDATE_PARTITIONED_REGION_MESSAGE;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.partitioned.PartitionMessage#fromData(java.io.DataInput)
   */
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.callbackArg = DataSerializer.readObject(in);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.partitioned.PartitionMessage#toData(java.io.DataOutput)
   */
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.callbackArg, out);
  }
}
