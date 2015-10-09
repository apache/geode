/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.membership.gms.mgr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.SerialDistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;


/**
  LocalViewMessage is used to pass a new membership view to the GemFire cache
  in an orderly manner.  It is intended to be queued with serially
  executed messages so that the view takes effect at the proper time.
  
  @author Bruce Schuchardt
 */

public final class LocalViewMessage extends SerialDistributionMessage
{

  private GMSMembershipManager manager;
  private long viewId;
  private NetView view;
  
  public LocalViewMessage(
    InternalDistributedMember addr,
    long viewId,
    NetView view,
    GMSMembershipManager manager
    )
  {
    super();
    this.sender = addr;
    this.viewId = viewId;
    this.view = view;
    this.manager = manager;
  }
  
  @Override
  final public int getProcessorType() {
    return DistributionManager.VIEW_EXECUTOR;
  }


  @Override
  protected void process(DistributionManager dm) {
    //dm.getLogger().info("view message processed", new Exception());
    manager.processView(viewId, view);
  }

  // These "messages" are never DataSerialized 

  public int getDSFID() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    throw new UnsupportedOperationException();
  }
}    
