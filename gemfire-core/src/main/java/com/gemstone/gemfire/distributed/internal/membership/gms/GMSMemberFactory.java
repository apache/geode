/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.membership.gms;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

import com.gemstone.gemfire.GemFireConfigException;
import com.gemstone.gemfire.SystemConnectException;
import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionException;
import com.gemstone.gemfire.distributed.internal.LocatorStats;
import com.gemstone.gemfire.distributed.internal.membership.DistributedMembershipListener;
import com.gemstone.gemfire.distributed.internal.membership.MemberAttributes;
import com.gemstone.gemfire.distributed.internal.membership.MemberServices;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.distributed.internal.membership.NetMember;
import com.gemstone.gemfire.distributed.internal.membership.gms.locator.GMSLocator;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.tcp.ConnectionException;
import com.gemstone.gemfire.security.AuthenticationFailedException;
import com.gemstone.gemfire.security.AuthenticationRequiredException;

/**
 * Create a new Member based on the given inputs.
 * 
 * @see com.gemstone.gemfire.distributed.internal.membership.NetMember
 */
public class GMSMemberFactory implements MemberServices {

  /**
   * Return a new NetMember, possibly for a different host
   * 
   * @param i the name of the host for the specified NetMember, the current host (hopefully)
   * if there are any problems.
   * @param splitBrainEnabled whether the member has this feature enabled
   * @param canBeCoordinator whether the member can be membership coordinator
   * @param p the membership port
   * @param attr the MemberAttributes
   * @return the new NetMember
   */
  public NetMember newNetMember(InetAddress i, int p, boolean splitBrainEnabled,
      boolean canBeCoordinator, MemberAttributes attr, short version) {
    GMSMember result = new GMSMember(attr, i, p, splitBrainEnabled, canBeCoordinator, version, 0, 0);
    return result;
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
    return new GMSMember(MemberAttributes.INVALID, i, p, false, true, Version.CURRENT_ORDINAL, 0, 0);
  }

  /**
   * Return a new NetMember representing current host.  This
   * is used for testing, so we ignore host-name lookup
   * localhost inetAddress
   * 
   * @param s a String referring to a host - ignored
   * @param p the membership port being used
   * @return the new member
   */
  public NetMember newNetMember(String s, int p) {
    InetAddress inetAddr = null;
    try {
      inetAddr=SocketCreator.getLocalHost();
    } catch (UnknownHostException e2) {
      throw new RuntimeException("Unable to create an identifier for testing for " + s, e2);
    }
    return newNetMember(inetAddr, p);
  }
  
  /**
   * Return a new Member
   * 
   * Used by externalization only.
   * 
   * @return blank member for use with externalization
   */
  public NetMember newNetMember() {
    return new GMSMember();
  }

  public MembershipManager newMembershipManager(DistributedMembershipListener listener,
          DistributionConfig config,
          RemoteTransportConfig transport, DMStats stats) throws DistributionException
  {
    Services services = new Services(listener, config, transport, stats);
    try {
      services.init();
      services.start();
    }
    catch (ConnectionException e) {
      throw new DistributionException(LocalizedStrings.JGroupMemberFactory_UNABLE_TO_CREATE_MEMBERSHIP_MANAGER.toLocalizedString(), e);
    }
    catch (GemFireConfigException
        | SystemConnectException
        | AuthenticationFailedException
        | AuthenticationRequiredException e) {
      throw e;
    }
    catch (RuntimeException e) {
      Services.getLogger().error("Unexpected problem starting up membership services", e);
      throw new SystemConnectException("Problem starting up membership services", e);
    }
    return (MembershipManager)services.getManager();
  }
  
  @Override
  public NetLocator newLocatorHandler(InetAddress bindAddress,
      File stateFile,
      String locatorString,
      boolean usePreferredCoordinators,
      boolean networkPartitionDetectionEnabled, LocatorStats stats) {
    
    return new GMSLocator(bindAddress, stateFile, locatorString, usePreferredCoordinators, networkPartitionDetectionEnabled, stats);
  }
}
