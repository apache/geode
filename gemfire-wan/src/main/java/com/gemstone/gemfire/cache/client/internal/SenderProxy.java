/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import java.util.List;

import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.distributed.internal.ServerLocation;

/**
 * Used to send operations from a sender to a receiver.
 * @author skumar
 * @since 8.1
 */
public class SenderProxy extends ServerProxy{
  public SenderProxy(InternalPool pool) {
    super(pool);
  }

  public void dispatchBatch_NewWAN(Connection con, List events, int batchId, boolean removeFromQueueOnException, boolean isRetry)
  {
    GatewaySenderBatchOp.executeOn(con, this.pool, events, batchId, removeFromQueueOnException, isRetry);
  }
  
  public Object receiveAckFromReceiver(Connection con)
  {
    return GatewaySenderBatchOp.executeOn(con, this.pool);
  }
}
