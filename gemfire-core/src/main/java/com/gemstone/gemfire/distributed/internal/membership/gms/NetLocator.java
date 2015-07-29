package com.gemstone.gemfire.distributed.internal.membership.gms;

import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpHandler;

public interface NetLocator extends TcpHandler {

  /**
   * This must be called after booting the membership manager so
   * that the locator can use its services
   * @param mgr
   */
  public void setMembershipManager(MembershipManager mgr);

}
