package com.gemstone.gemfire.distributed.internal.membership.gms.interfaces;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetView;

public interface JoinLeave extends Service {

  /**
   * joins the distributed system and returns true if successful, false if not.
   * Throws SystemConnectException and GemFireConfigException
   */
  boolean join();

  /**
   * leaves the distributed system.  Should be invoked before stop()
   */
  void leave();

  /**
   * force another member out of the system
   */
  void remove(InternalDistributedMember m, String reason);
  
  /**
   * Invoked by the Manager, this notifies the HealthMonitor that a
   * ShutdownMessage has been received from the given member
   */
  public void memberShutdown(DistributedMember mbr, String reason);
  
  /**
   * returns the local address
   */
  InternalDistributedMember getMemberID();
  
  /**
   * returns the current membership view
   */
  NetView getView();

  /**
   * test hook
   */
  void disableDisconnectOnQuorumLossForTesting();
}
