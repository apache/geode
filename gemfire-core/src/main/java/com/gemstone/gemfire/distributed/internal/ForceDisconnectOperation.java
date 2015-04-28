/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * Used to force a current member of the distributed system to disconnect.
 * @deprecated starting with Ganges, use MembeshipManager.requestMemberRemoval instead
 * @author Darrel
 * @since 4.2.1
 */
@Deprecated
public class ForceDisconnectOperation  {
//  protected DM dm;
//  protected ReplyProcessor21 processor;
//  protected ForceDisconnectMessage msg;

  public ForceDisconnectOperation(DM dm, InternalDistributedMember destination) {
    throw new UnsupportedOperationException("ForceDisconnectOperation is no longer supported - use MembershipManager.requestMemberRemoval instead");
//    this.dm = dm;
//    this.processor = new ReplyProcessor21(dm, destination);
//    this.msg = new ForceDisconnectMessage();
//    msg.setRecipient(destination);
//    msg.setProcessorId(this.processor.getProcessorId());
  }

}
