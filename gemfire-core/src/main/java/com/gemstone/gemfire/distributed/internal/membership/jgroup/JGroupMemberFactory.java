/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.membership.jgroup;

import java.net.InetAddress;

import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.DistributedMembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.MemberAttributes;
import com.gemstone.gemfire.distributed.internal.membership.MemberServices;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.distributed.internal.membership.NetMember;
import com.gemstone.gemfire.internal.OSProcess;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.tcp.ConnectionException;

/**
 * Create a new Member based on the given inputs.
 * TODO: need to implement a real factory implementation based on gemfire.properties
 * 
 * @see com.gemstone.gemfire.distributed.internal.membership.NetMember
 * @author D. Jason Penney
 */
public class JGroupMemberFactory implements MemberServices {

  /**
   * Return a new NetMember, possibly for a different host
   * 
   * @param i the name of the host for the specified NetMember, the current host (hopefully)
   * if there are any problems.
   * @param p the membership port
   * @param splitBrainEnabled whether the member has this feature enabled
   * @param canBeCoordinator whether the member can be membership coordinator
   * @param attr the MemberAttributes
   * @return the new NetMember
   */
  public NetMember newNetMember(InetAddress i, int p, boolean splitBrainEnabled,
      boolean canBeCoordinator, MemberAttributes attr) {
    JGroupMember result = new JGroupMember(i, p, splitBrainEnabled, canBeCoordinator);
    result.setAttributes(attr);
    return result;
  }

  private MemberAttributes getDefaultAttributes() {
    // TODO can we get rid of this??
    if (MemberAttributes.DEFAULT.getVmPid() == -1 ||
        MemberAttributes.DEFAULT.getVmKind() == -1) {
      MemberAttributes.setDefaults(
          -1, 
          OSProcess.getId(), 
          -1,
          DistributionManager.getDistributionManagerType(), 
          null,
          null, null);
    }
    return MemberAttributes.DEFAULT;
  }
  
  /**
   * Return a new NetMember representing current host.  This assumes that
   * the member does not have network partition detection enabled and can
   * be group coordinator
   * @param i an InetAddress referring to the current host
   * @param p the membership port being used
   * @return the new NetMember
   */
  public NetMember newNetMember(InetAddress i, int p) {
    JGroupMember result = new JGroupMember(i, p, false, true);
    result.setAttributes(getDefaultAttributes());
    return result;
  }

  /**
   * Return a new NetMember representing current host
   * 
   * @param s a String referring to the current host
   * @param p the membership port being used
   * @return the new member
   */
  public NetMember newNetMember(String s, int p) {
    JGroupMember result = new JGroupMember(s, p);
    result.setAttributes(getDefaultAttributes());
    return result;
  }
  
  /**
   * Return a new Member
   * 
   * Used by externalization only.
   * 
   * @return blank member for use with externalization
   */
  public NetMember newNetMember() {
    return new JGroupMember();
  }

  public MembershipManager newMembershipManager(DistributedMembershipListener listener,
          DistributionConfig config,
          RemoteTransportConfig transport,
          DMStats stats) throws DistributionException
  {
    try {
      return new JGroupMembershipManager().initialize(listener,
          config, transport, stats);
    }
    catch (ConnectionException e) {
      throw new DistributionException(LocalizedStrings.JGroupMemberFactory_UNABLE_TO_CREATE_MEMBERSHIP_MANAGER.toLocalizedString(), e);
    }
  }
}
