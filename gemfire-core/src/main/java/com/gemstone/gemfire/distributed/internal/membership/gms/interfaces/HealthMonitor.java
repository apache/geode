package com.gemstone.gemfire.distributed.internal.membership.gms.interfaces;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.NetMember;

public interface HealthMonitor extends Service {

  /**
   * Note that this member has been contacted by the given member
   * @param sender
   */
  public void contactedBy(InternalDistributedMember sender);

  /**
   * initiate, asynchronously, suspicion that the member is no longer available
   * @param mbr
   * @param reason
   */
  public void suspect(InternalDistributedMember mbr, String reason);

  /**
   * Check on the health of the given member, initiating suspicion if it
   * fails.  Return true if the member is found to be available, false
   * if it isn't.
   * @param mbr
   * @param reason the reason this check is being performed
   * @param initiateRemoval if the member should be removed if it is not available
   * @return 
   */
  public boolean checkIfAvailable(DistributedMember mbr, String reason, boolean initiateRemoval);
  
  /**
   * Invoked by the Manager, this notifies the HealthMonitor that a
   * ShutdownMessage has been received from the given member
   */
  public void memberShutdown(DistributedMember mbr, String reason);
  
  /**
   * Returns a map that describes the members and their server sockets
   */
  public Map<InternalDistributedMember, InetSocketAddress> getSocketInfo();

  /**
   * Update the information of the members and their server sockets
   * 
   * @param members
   * @param portsForMembers List of socket ports for each member
   */
  public void installSocketInfo(List<InternalDistributedMember> members, List<Integer> portsForMembers);
}
