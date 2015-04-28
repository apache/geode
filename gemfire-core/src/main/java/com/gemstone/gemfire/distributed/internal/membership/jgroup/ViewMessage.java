/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.membership.jgroup;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.SerialDistributionMessage;
import java.io.*;


/**
  ViewMessage is used to pass a new membership view to the GemFire cache
  in an orderly manner.  It is intended to be queued with serially
  executed messages so that the view takes effect at the proper time.
  
  @author Bruce Schuchardt
 */

public final class ViewMessage extends SerialDistributionMessage
{

  private JGroupMembershipManager manager;
  private long viewId;
  private NetView view;
  
  public ViewMessage(
    InternalDistributedMember addr,
    long viewId,
    NetView view,
    JGroupMembershipManager manager
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
