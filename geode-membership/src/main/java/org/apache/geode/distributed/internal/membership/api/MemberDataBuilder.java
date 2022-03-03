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
package org.apache.geode.distributed.internal.membership.api;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.geode.distributed.internal.membership.gms.MemberDataBuilderImpl;

/**
 * Membership identifiers must hold a MemberData object. This builder let's you create
 * one.<br>
 * See {@link MemberIdentifierFactory}<br>
 */
public interface MemberDataBuilder {

  /**
   * Create a builder for the given host machine and host name
   */
  static MemberDataBuilder newBuilder(InetAddress hostAddress, String hostName) {
    return MemberDataBuilderImpl.newBuilder(hostAddress, hostName);
  }

  /**
   * Create a builder for the machine hosting this process
   */
  static MemberDataBuilder newBuilderForLocalHost(String hostName) {
    return MemberDataBuilderImpl.newBuilderForLocalHost(hostName);
  }

  /** Parses comma-separated-roles/groups into array of groups (strings). */
  static String[] parseGroups(String csvRoles, String csvGroups) {
    List<String> groups = new ArrayList<>();
    parseCsv(groups, csvRoles);
    parseCsv(groups, csvGroups);
    return groups.toArray(new String[groups.size()]);
  }

  static void parseCsv(List<String> groups, String csv) {
    if (csv == null || csv.length() == 0) {
      return;
    }
    StringTokenizer st = new StringTokenizer(csv, ",");
    while (st.hasMoreTokens()) {
      String groupName = st.nextToken().trim();
      if (!groups.contains(groupName)) { // only add each group once
        groups.add(groupName);
      }
    }
  }

  MemberDataBuilder setMembershipPort(int membershipPort);

  MemberDataBuilder setDirectChannelPort(int directChannelPort);

  MemberDataBuilder setVmPid(int vmPid);

  MemberDataBuilder setVmKind(int vmKind);

  MemberDataBuilder setVmViewId(int vmViewId);

  MemberDataBuilder setName(String name);

  MemberDataBuilder setGroups(String[] groups);

  MemberDataBuilder setDurableId(String durableId);

  MemberDataBuilder setDurableTimeout(int durableTimeout);

  MemberDataBuilder setPreferredForCoordinator(boolean preferredForCoordinator);

  MemberDataBuilder setNetworkPartitionDetectionEnabled(boolean networkPartitionDetectionEnabled);

  MemberDataBuilder setVersionOrdinal(short versionOrdinal);

  MemberDataBuilder setUuidMostSignificantBits(long uuidMostSignificantBits);

  MemberDataBuilder setUuidLeastSignificantBits(long uuidLeastSignificantBits);

  MemberDataBuilder setIsPartial(boolean isPartial);

  MemberDataBuilder setUniqueTag(String uniqueTag);

  MemberData build();

}
