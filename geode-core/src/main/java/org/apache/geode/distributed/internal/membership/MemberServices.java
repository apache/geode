/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.distributed.internal.membership;

import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.LocatorStats;
import org.apache.geode.distributed.internal.membership.gms.NetLocator;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;

import java.io.File;
import java.net.InetAddress;

/**
 * This is the SPI for a provider of membership services.
 * 
 * @see org.apache.geode.distributed.internal.membership.NetMember
 */
public interface MemberServices {

  /**
   * Return a new NetMember, possibly for a different host
   * 
   * @param i the name of the host for the specified NetMember, the current host (hopefully) if
   *        there are any problems.
   * @param port the membership port
   * @param splitBrainEnabled whether the member has this feature enabled
   * @param canBeCoordinator whether the member can be membership coordinator
   * @param payload the payload to be associated with the resulting object
   * @param version TODO
   * @return the new NetMember
   */
  public abstract NetMember newNetMember(InetAddress i, int port, boolean splitBrainEnabled,
      boolean canBeCoordinator, MemberAttributes payload, short version);

  /**
   * Return a new NetMember representing current host
   * 
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
   * @param transport holds configuration information that can be used by the manager to configure
   *        itself
   * @param stats a gemfire statistics collection object for communications stats
   * 
   * @return a MembershipManager
   */
  public abstract MembershipManager newMembershipManager(DistributedMembershipListener listener,
      DistributionConfig config, RemoteTransportConfig transport, DMStats stats);


  /**
   * currently this is a test method but it ought to be used by InternalLocator to create the peer
   * location TcpHandler
   */
  public abstract NetLocator newLocatorHandler(InetAddress bindAddress, File stateFile,
      String locatorString, boolean usePreferredCoordinators,
      boolean networkPartitionDetectionEnabled, LocatorStats stats, String securityUDPDHAlgo);
}
