/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import java.util.Collection;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;


/**
 * TODO prpersist. This code really needs to be merged with the 
 * AdminReplyProcessor. However, we're getting close to the release
 * and I don't want to mess with all of the admin code right now. We
 * need this class to handle failures from admin messages that expect
 * replies from multiple members.
 * @author dsmith
 *
 */
public class AdminMultipleReplyProcessor extends ReplyProcessor21 {

  public AdminMultipleReplyProcessor(DM dm, Collection initMembers) {
    super(dm, initMembers);
  }

  public AdminMultipleReplyProcessor(DM dm, InternalDistributedMember member) {
    super(dm, member);
  }

  public AdminMultipleReplyProcessor(DM dm, InternalDistributedSystem system,
      Collection initMembers, CancelCriterion cancelCriterion, boolean register) {
    super(dm, system, initMembers, cancelCriterion, register);
  }

  public AdminMultipleReplyProcessor(InternalDistributedSystem system,
      Collection initMembers, CancelCriterion cancelCriterion) {
    super(system, initMembers, cancelCriterion);
  }

  public AdminMultipleReplyProcessor(InternalDistributedSystem system,
      Collection initMembers) {
    super(system, initMembers);
  }

  public AdminMultipleReplyProcessor(InternalDistributedSystem system,
      InternalDistributedMember member, CancelCriterion cancelCriterion) {
    super(system, member, cancelCriterion);
  }

  public AdminMultipleReplyProcessor(InternalDistributedSystem system,
      InternalDistributedMember member) {
    super(system, member);
  }

  @Override
  protected void process(DistributionMessage msg, boolean warn) {
    if (msg instanceof AdminFailureResponse) {
      Exception ex = ((AdminFailureResponse)msg).getCause();
      if (ex != null) {
        ReplyException rex = new ReplyException(ex);
        rex.setSenderIfNull(msg.getSender());
        processException(msg, rex);
      }
    }
    super.process(msg, warn);
  }

  
  

}
