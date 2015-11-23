package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

public interface HasMemberID {

  public InternalDistributedMember getMemberID();
  
}
