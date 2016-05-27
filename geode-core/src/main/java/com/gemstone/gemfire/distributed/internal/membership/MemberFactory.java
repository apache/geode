/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed.internal.membership;

import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.LocatorStats;
import com.gemstone.gemfire.distributed.internal.membership.gms.GMSMemberFactory;
import com.gemstone.gemfire.distributed.internal.membership.gms.NetLocator;
import com.gemstone.gemfire.internal.admin.remote.RemoteTransportConfig;

import java.io.File;
import java.net.InetAddress;

/**
 * Create a new Member based on the given inputs.
 * TODO: need to implement a real factory implementation based on gemfire.properties
 * 
 * @see com.gemstone.gemfire.distributed.internal.membership.NetMember
 */
public class MemberFactory {
  
  private static final MemberServices services = new GMSMemberFactory();

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
      boolean splitBrainEnabled, boolean canBeCoordinator, short version, MemberAttributes payload) {
    return services.newNetMember(i, p, splitBrainEnabled, canBeCoordinator, payload, version);
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
  
  /**
   * currently this is a test method but it ought to be used by InternalLocator
   * to create the peer location TcpHandler
   */
  static public NetLocator newLocatorHandler(InetAddress bindAddress,
          File stateFile,
          String locatorString,
          boolean usePreferredCoordinators,
          boolean networkPartitionDetectionEnabled, LocatorStats stats) {
    return services.newLocatorHandler(bindAddress, stateFile, locatorString,
        usePreferredCoordinators, networkPartitionDetectionEnabled, stats);
  }

}
