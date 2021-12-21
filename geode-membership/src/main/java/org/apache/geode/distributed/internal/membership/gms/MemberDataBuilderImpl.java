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
package org.apache.geode.distributed.internal.membership.gms;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.geode.distributed.internal.membership.api.MemberData;
import org.apache.geode.distributed.internal.membership.api.MemberDataBuilder;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.serialization.KnownVersion;

/**
 * MemberDataBuilderImpl is the implementation of MemberDataBuilder. It constructs a
 * MemberData and is exposed to geode-core to support construction of identifiers in
 * deserialization code and in tests.
 */
public class MemberDataBuilderImpl implements MemberDataBuilder {

  private static final String EMPTY_STRING = "";

  private final InetAddress inetAddress;
  private final String hostName;
  private int membershipPort = -1;
  private int directChannelPort = -1;
  private int vmPid = -1;
  private int vmKind = MemberIdentifier.NORMAL_DM_TYPE;
  private int vmViewId = -1;
  private String name = EMPTY_STRING;
  private String[] groups;
  private String durableId;
  private int durableTimeout = -1;
  private boolean preferredForCoordinator = true;
  private boolean networkPartitionDetectionEnabled;
  private short versionOrdinal = KnownVersion.CURRENT_ORDINAL;
  private long uuidMostSignificantBits = 0;
  private long uuidLeastSignificantBits = 0;
  private boolean isPartial;
  private String uniqueTag;

  public void setMemberWeight(byte memberWeight) {
    this.memberWeight = memberWeight;
  }

  private byte memberWeight = 0;

  /**
   * Create a builder for the given host machine and host name
   */
  public static MemberDataBuilderImpl newBuilder(InetAddress hostAddress, String hostName) {
    return new MemberDataBuilderImpl(hostAddress, hostName);
  }

  /**
   * Create a builder for the machine hosting this process
   */
  public static MemberDataBuilderImpl newBuilderForLocalHost(String hostName) {
    return new MemberDataBuilderImpl(hostName);
  }

  private MemberDataBuilderImpl(InetAddress hostAddress, String hostName) {
    inetAddress = hostAddress;
    this.hostName = hostName;
  }

  private MemberDataBuilderImpl(String hostName) {
    try {
      inetAddress = LocalHostUtil.getLocalHost();
    } catch (UnknownHostException e2) {
      throw new RuntimeException("Unable to resolve local host address", e2);
    }
    this.hostName = hostName;
  }

  public MemberDataBuilderImpl setMembershipPort(int membershipPort) {
    this.membershipPort = membershipPort;
    return this;
  }

  public MemberDataBuilderImpl setDirectChannelPort(int directChannelPort) {
    this.directChannelPort = directChannelPort;
    return this;
  }

  public MemberDataBuilderImpl setVmPid(int vmPid) {
    this.vmPid = vmPid;
    return this;
  }

  public MemberDataBuilderImpl setVmKind(int vmKind) {
    this.vmKind = vmKind;
    return this;
  }

  public MemberDataBuilderImpl setVmViewId(int vmViewId) {
    this.vmViewId = vmViewId;
    return this;
  }

  public MemberDataBuilderImpl setName(String name) {
    this.name = name;
    return this;
  }

  public MemberDataBuilderImpl setGroups(String[] groups) {
    this.groups = groups;
    return this;
  }

  public MemberDataBuilderImpl setDurableId(String durableId) {
    this.durableId = durableId;
    return this;
  }

  public MemberDataBuilderImpl setDurableTimeout(int durableTimeout) {
    this.durableTimeout = durableTimeout;
    return this;
  }

  public MemberDataBuilderImpl setPreferredForCoordinator(boolean preferredForCoordinator) {
    this.preferredForCoordinator = preferredForCoordinator;
    return this;
  }

  public MemberDataBuilderImpl setNetworkPartitionDetectionEnabled(
      boolean networkPartitionDetectionEnabled) {
    this.networkPartitionDetectionEnabled = networkPartitionDetectionEnabled;
    return this;
  }

  public MemberDataBuilderImpl setVersionOrdinal(short versionOrdinal) {
    this.versionOrdinal = versionOrdinal;
    return this;
  }

  public MemberDataBuilderImpl setUuidMostSignificantBits(long uuidMostSignificantBits) {
    this.uuidMostSignificantBits = uuidMostSignificantBits;
    return this;
  }

  public MemberDataBuilderImpl setUuidLeastSignificantBits(long uuidLeastSignificantBits) {
    this.uuidLeastSignificantBits = uuidLeastSignificantBits;
    return this;
  }

  public MemberDataBuilderImpl setIsPartial(boolean partial) {
    isPartial = partial;
    return this;
  }

  @Override
  public MemberDataBuilder setUniqueTag(String uniqueTag) {
    this.uniqueTag = uniqueTag;
    return this;
  }

  public MemberData build() {
    return new GMSMemberData(inetAddress, hostName,
        membershipPort, vmPid, (byte) vmKind, directChannelPort,
        vmViewId, name, groups, durableId, durableTimeout,
        networkPartitionDetectionEnabled, preferredForCoordinator, versionOrdinal,
        uuidMostSignificantBits, uuidLeastSignificantBits, memberWeight, isPartial, uniqueTag);
  }

}
