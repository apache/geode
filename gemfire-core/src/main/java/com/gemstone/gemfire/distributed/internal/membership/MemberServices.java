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
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;

/**
 * This is the SPI for a provider of membership services.
 * 
 * @see com.gemstone.gemfire.distributed.internal.membership.NetMember
 * @author D. Jason Penney
 */
public interface MemberServices {

  /**
   * Return a blank NetMember (used by externalization)
   * @return the new NetMember
   */
  public abstract NetMember newNetMember();
  
  /**
   * Return a new NetMember, possibly for a different host
   * 
   * @param i the name of the host for the specified NetMember, the current host (hopefully)
   * if there are any problems.
   * @param port the membership port
   * @param splitBrainEnabled whether the member has this feature enabled
   * @param canBeCoordinator whether the member can be membership coordinator
   * @param payload the payload to be associated with the resulting object
   * @return the new NetMember
   */
  public abstract NetMember newNetMember(InetAddress i, int port, 
      boolean splitBrainEnabled, boolean canBeCoordinator, MemberAttributes payload);

  /**
   * Return a new NetMember representing current host
   * @param i an InetAddress referring to the current host
   * @param port the membership port being used
   * 
   * @return the new NetMember
   */
  public abstract NetMember newNetMember(InetAddress i, int port);

  /**
   * Return a new NetMember representing current host
   * 
   * @param s a String referring to the current host
   * @param p the membership port being used
   * @return the new member
   */
  public abstract NetMember newNetMember(String s, int p);
  
   /**
   * Create a new MembershipManager
   * 
   * @param listener the listener to notify for callbacks
   * @param transport holds configuration information that can be used by the manager to configure itself
   * @param stats a gemfire statistics collection object for communications stats
   * @return a MembershipManager
   */
  public abstract MembershipManager newMembershipManager(DistributedMembershipListener listener,
          DistributionConfig config,
          RemoteTransportConfig transport,
          DMStats stats);
}
