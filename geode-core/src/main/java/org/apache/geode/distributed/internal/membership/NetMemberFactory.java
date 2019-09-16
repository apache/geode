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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.internal.membership.adapter.GMSMemberAdapter;
import org.apache.geode.distributed.internal.membership.gms.GMSMember;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.serialization.Version;

public class NetMemberFactory {
  /**
   * Return a new NetMember, possibly for a different host
   *
   * @param host the name of the host for the specified NetMember, the current host (hopefully) if
   *        there are any problems.
   * @param port the membership port
   * @param splitBrainEnabled whether the member has this feature enabled
   * @param canBeCoordinator whether the member can be membership coordinator
   * @param payload the payload for this member
   * @return the new NetMember
   */
  public static NetMember newNetMember(InetAddress host, String hostName, int port,
      boolean splitBrainEnabled,
      boolean canBeCoordinator, short version,
      MemberAttributes payload) {
    DurableClientAttributes durableClientAttributes = payload.getDurableClientAttributes();
    String durableId = null;
    int durableTimeout = 0;
    if (durableClientAttributes != null) {
      durableId = durableClientAttributes.getId();
      durableTimeout = durableClientAttributes.getTimeout();
    }
    return new GMSMemberAdapter(
        new GMSMember(host, hostName, port, payload.getVmPid(), (byte) payload.getVmKind(),
            payload.getPort(), payload.getVmViewId(), payload.getName(), payload.getGroups(),
            durableId, durableTimeout, splitBrainEnabled, canBeCoordinator, version, 0, 0));
  }

  /**
   * Return a new NetMember representing current host
   *
   * @param host an InetAddress referring to the current host
   * @param port the membership port being used
   * @return the new NetMember
   */
  public static NetMember newNetMember(InetAddress host, int port) {
    DurableClientAttributes durableClientAttributes =
        MemberAttributes.DEFAULT.getDurableClientAttributes();
    String durableId = null;
    int durableTimeout = 0;
    if (durableClientAttributes != null) {
      durableId = durableClientAttributes.getId();
      durableTimeout = durableClientAttributes.getTimeout();
    }
    return new GMSMemberAdapter(new GMSMember(host, host.getHostName(), port,
        MemberAttributes.DEFAULT.getVmPid(), (byte) MemberAttributes.DEFAULT
            .getVmKind(),
        MemberAttributes.DEFAULT.getPort(), MemberAttributes.DEFAULT.getVmViewId(),
        MemberAttributes.DEFAULT
            .getName(),
        MemberAttributes.DEFAULT.getGroups(),
        durableId, durableTimeout, false, true, Version.CURRENT_ORDINAL, 0, 0));
  }

  /**
   * Return a new NetMember representing current host
   *
   * @param host a String referring to the current host
   * @param port the membership port being used
   * @return the new member
   */
  public static NetMember newNetMember(String host, int port) {
    InetAddress inetAddr = null;
    try {
      inetAddr = SocketCreator.getLocalHost();
    } catch (UnknownHostException e2) {
      throw new RuntimeException("Unable to create an identifier for testing for " + host, e2);
    }
    DurableClientAttributes durableClientAttributes =
        MemberAttributes.DEFAULT.getDurableClientAttributes();
    String durableId = null;
    int durableTimeout = 0;
    if (durableClientAttributes != null) {
      durableId = durableClientAttributes.getId();
      durableTimeout = durableClientAttributes.getTimeout();
    }
    return new GMSMemberAdapter(new GMSMember(inetAddr, inetAddr.getHostName(),
        port, MemberAttributes.DEFAULT.getVmPid(), (byte) MemberAttributes.DEFAULT.getVmKind(),
        MemberAttributes.DEFAULT.getPort(), MemberAttributes.DEFAULT.getVmViewId(),
        MemberAttributes.DEFAULT
            .getName(),
        MemberAttributes.DEFAULT.getGroups(),
        durableId, durableTimeout, false, true, Version.CURRENT_ORDINAL, 0, 0));
  }
}
