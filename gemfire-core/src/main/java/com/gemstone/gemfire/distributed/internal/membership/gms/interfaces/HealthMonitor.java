package com.gemstone.gemfire.distributed.internal.membership.gms.interfaces;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

public interface HealthMonitor extends Service {

  public abstract void contactedBy(InternalDistributedMember sender);

  public abstract void suspect(InternalDistributedMember mbr, String reason);

  public abstract void checkSuspect(DistributedMember mbr, String reason);

}
