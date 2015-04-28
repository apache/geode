/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.membership;

import java.net.InetAddress;

import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.JGroupMemberFactory;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;

/**
 * Create a new Member based on the given inputs.
 * TODO: need to implement a real factory implementation based on gemfire.properties
 * 
 * @see com.gemstone.gemfire.distributed.internal.membership.NetMember
 * @author D. Jason Penney
 */
public class MemberFactory {
  
  private static final MemberServices services = new JGroupMemberFactory();

  /**
   * Return a blank NetMember (used by externalization)
   * @return the new NetMember
   */
  static public NetMember newNetMember() {
    return services.newNetMember();
  }
  
  /**
   * Return a new NetMember, possibly for a different host
   * 
   * @param i the name of the host for the specified NetMember, the current host (hopefully)
   * if there are any problems.
   * @param p the membership port
   * @param splitBrainEnabled whether the member has this feature enabled
   * @param canBeCoordinator whether the member can be membership coordinator
   * @param payload the payload for this member
   * @return the new NetMember
   */
  static public NetMember newNetMember(InetAddress i, int p,
      boolean splitBrainEnabled, boolean canBeCoordinator, MemberAttributes payload) {
    return services.newNetMember(i, p, splitBrainEnabled, canBeCoordinator, payload);
  }

  /**
   * Return a new NetMember representing current host
   * @param i an InetAddress referring to the current host
   * @param p the membership port being used
   * @return the new NetMember
   */
  static public NetMember newNetMember(InetAddress i, int p) {
    NetMember result = services.newNetMember(i, p);
    return result;
  }

  /**
   * Return a new NetMember representing current host
   * 
   * @param s a String referring to the current host
   * @param p the membership port being used
   * @return the new member
   */
  static public NetMember newNetMember(String s, int p) {
    return services.newNetMember(s, p);
  }
  
  /**
   * Create a new MembershipManager.  Be sure to send the manager a postConnect() message
   * before you start using it.
   * 
   * @param listener the listener to notify for callbacks
   * @param config the configuration of connection to distributed system
   * @param transport holds configuration information that can be used by the manager to configure itself
   * @param stats are used for recording statistical communications information
   * @return a MembershipManager
   */
  static public MembershipManager newMembershipManager(DistributedMembershipListener listener,
          DistributionConfig config,
          RemoteTransportConfig transport,
          DMStats stats)
  {
    return services.newMembershipManager(listener, config, transport, stats);
  }
}
