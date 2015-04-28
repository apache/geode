/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author xzhou
 *
 */
public class ShutdownAllResponse extends AdminResponse {
  private transient boolean isToShutDown = true;
  public ShutdownAllResponse() {
  }
  
  @Override
  public boolean getInlineProcess() {
    return true;
  }
  
  @Override
  public boolean sendViaJGroups() {
    return true;
  }
  
  @Override
  public boolean orderedDelivery() {
    return true;
  }

  public ShutdownAllResponse(InternalDistributedMember sender, boolean isToShutDown) {
    this.setRecipient(sender);
    this.isToShutDown = isToShutDown;
  }

  public int getDSFID() {
    return SHUTDOWN_ALL_RESPONSE;
  }
  
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeBoolean(isToShutDown);
  }

  @Override  
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.isToShutDown = in.readBoolean();
  }

  @Override
  public String toString() {
    return "ShutdownAllResponse from " + this.getSender()
    + " msgId=" + this.getMsgId() + " isToShutDown=" + this.isToShutDown; 
  }

  public boolean isToShutDown() {
    return isToShutDown;
  }
}
