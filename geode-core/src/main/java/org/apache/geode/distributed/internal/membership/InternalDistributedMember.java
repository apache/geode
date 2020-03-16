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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.jgroups.util.UUID;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.Role;
import org.apache.geode.distributed.internal.DistributionAdvisor.ProfileId;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.api.MemberData;
import org.apache.geode.distributed.internal.membership.api.MemberDataBuilder;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierFactoryImpl;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.logging.internal.OSProcess;

/**
 * This is the fundamental representation of a member of a GemFire distributed system.
 */
public class InternalDistributedMember
    implements DistributedMember, Externalizable, ProfileId, VersionSource<DistributedMember>,
    MemberIdentifier, DataSerializableFixedID {
  private static final long serialVersionUID = -2785249969777296507L;
  public static final int DEFAULT_DURABLE_CLIENT_TIMEOUT = 300;

  @Immutable
  public static final MemberIdentifierFactoryImpl MEMBER_IDENTIFIER_FACTORY =
      new MemberIdentifierFactoryImpl();

  private final MemberIdentifier memberIdentifier;

  @VisibleForTesting
  // Cached to prevent creating a new instance every single time.
  protected volatile DurableClientAttributes durableClientAttributes;

  // Used only by deserialization
  public InternalDistributedMember() {
    memberIdentifier = MEMBER_IDENTIFIER_FACTORY.create(null);
  }

  /**
   * Construct a InternalDistributedMember
   * <p>
   *
   * This, and the following constructor are the only valid ways to create an ID for a distributed
   * member for use in the P2P cache. Use of other constructors can break
   * network-partition-detection.
   *
   * @param i the inet address
   * @param membershipPort the membership port
   * @param splitBrainEnabled whether this feature is enabled for the member
   * @param canBeCoordinator whether the member is eligible to be the membership coordinator
   */
  public InternalDistributedMember(InetAddress i, int membershipPort, boolean splitBrainEnabled,
      boolean canBeCoordinator) {

    memberIdentifier =
        MEMBER_IDENTIFIER_FACTORY.create(MemberDataBuilder.newBuilder(i, getHostName(i))
            .setMembershipPort(membershipPort)
            .setNetworkPartitionDetectionEnabled(splitBrainEnabled)
            .setPreferredForCoordinator(canBeCoordinator)
            .build());
  }

  private static String getHostName(InetAddress i) {
    return SocketCreator.resolve_dns ? SocketCreator.getHostName(i) : i.getHostAddress();
  }

  /**
   * Construct a InternalDistributedMember based on the given member data.
   *
   */
  public InternalDistributedMember(MemberData m) {
    memberIdentifier = MEMBER_IDENTIFIER_FACTORY.create(m);

    if (getHostName() == null || isPartial()) {
      String hostName = getHostName(m.getInetAddress());
      memberIdentifier.setHostName(hostName);
    }
  }

  /**
   * Create a InternalDistributedMember referring to the current host (as defined by the given
   * string).
   * <p>
   *
   * <b> THIS METHOD IS FOR TESTING ONLY. DO NOT USE IT TO CREATE IDs FOR USE IN THE PRODUCT. IT
   * DOES NOT PROPERLY INITIALIZE ATTRIBUTES NEEDED FOR P2P FUNCTIONALITY. </b>
   *
   *
   * @param i the hostname, stored in the member ID but not resolved - local host inet addr is used
   * @param p the membership listening port
   * @throws RuntimeException if the given hostname cannot be resolved
   */
  @VisibleForTesting
  public InternalDistributedMember(String i, int p) {
    this(MemberDataBuilder.newBuilderForLocalHost(i)
        .setMembershipPort(p)
        .build());
  }

  /**
   * Creates a new InternalDistributedMember for use in notifying listeners in client
   * caches. The version information in the ID is set to Version.CURRENT and the host name
   * is left unresolved (DistributedMember doesn't expose the InetAddress).
   *
   * @param location the coordinates of the server
   */

  public InternalDistributedMember(ServerLocation location) {
    memberIdentifier =
        MEMBER_IDENTIFIER_FACTORY.create(
            MemberDataBuilder.newBuilderForLocalHost(location.getHostName())
                .setMembershipPort(location.getPort())
                .setNetworkPartitionDetectionEnabled(false)
                .setPreferredForCoordinator(true)
                .build());
  }

  /**
   * Create a InternalDistributedMember referring to the current host (as defined by the given
   * string) with additional info including optional connection name and an optional unique string.
   * Currently these two optional fields (and this constructor) are only used by the
   * LonerDistributionManager.
   * <p>
   *
   * < b> DO NOT USE THIS METHOD TO CREATE ANYTHING OTHER THAN A LONER ID. IT DOES NOT PROPERLY
   * INITIALIZE THE ID. </b>
   *
   * @param host the hostname, must be for the current host
   * @param p the membership port
   * @param n member name
   * @param u unique string used make the member more unique
   * @param vmKind the dmType
   * @param groups the server groups / roles
   * @param attr durable client attributes, if any
   *
   */
  public InternalDistributedMember(String host, int p, String n, String u, int vmKind,
      String[] groups, DurableClientAttributes attr) {
    durableClientAttributes = attr;
    memberIdentifier =
        MEMBER_IDENTIFIER_FACTORY.create(createMemberData(host, p, n, vmKind, groups, attr, u));

    defaultToCurrentHost();
  }

  private static MemberData createMemberData(String host, int p, String n, int vmKind,
      String[] groups,
      DurableClientAttributes attr, String u) {
    InetAddress addr = LocalHostUtil.toInetAddress(host);
    MemberDataBuilder builder = MemberDataBuilder.newBuilder(addr, host)
        .setName(n)
        .setMembershipPort(p)
        .setDirectChannelPort(p)
        .setPreferredForCoordinator(false)
        .setNetworkPartitionDetectionEnabled(true)
        .setVmKind(vmKind)
        .setUniqueTag(u)
        .setGroups(groups);
    if (attr != null) {
      builder.setDurableId(attr.getId())
          .setDurableTimeout(attr.getTimeout());
    }
    return builder.build();
  }

  /**
   * Create a InternalDistributedMember
   * <p>
   *
   * <b> THIS METHOD IS FOR TESTING ONLY. DO NOT USE IT TO CREATE IDs FOR USE IN THE PRODUCT. IT
   * DOES NOT PROPERLY INITIALIZE ATTRIBUTES NEEDED FOR P2P FUNCTIONALITY. </b>
   *
   *
   * @param i the host address
   * @param p the membership listening port
   */
  public InternalDistributedMember(InetAddress i, int p) {
    memberIdentifier = MEMBER_IDENTIFIER_FACTORY.create(MemberDataBuilder.newBuilder(i, "localhost")
        .setMembershipPort(p)
        .build());
    defaultToCurrentHost();
  }

  /**
   * Create a InternalDistributedMember as defined by the given address.
   * <p>
   *
   * <b> THIS METHOD IS FOR TESTING ONLY. DO NOT USE IT TO CREATE IDs FOR USE IN THE PRODUCT. IT
   * DOES NOT PROPERLY INITIALIZE ATTRIBUTES NEEDED FOR P2P FUNCTIONALITY. </b>
   *
   * @param addr address of the server
   * @param p the listening port of the server
   * @param isCurrentHost true if the given host refers to the current host (bridge and gateway use
   *        false to create a temporary id for the OTHER side of a connection)
   */
  public InternalDistributedMember(InetAddress addr, int p, boolean isCurrentHost) {
    memberIdentifier =
        MEMBER_IDENTIFIER_FACTORY.create(MemberDataBuilder.newBuilder(addr, "localhost")
            .setMembershipPort(p).build());
    if (isCurrentHost) {
      defaultToCurrentHost();
    }
  }


  /** this reads an ID written with writeEssentialData */
  public static InternalDistributedMember readEssentialData(DataInput in)
      throws IOException, ClassNotFoundException {
    final InternalDistributedMember mbr = new InternalDistributedMember();
    mbr._readEssentialData(in, InternalDistributedMember::getHostName);
    return mbr;
  }

  /**
   * Returns this client member's durable attributes or null if no durable attributes were created.
   */
  @Override
  public DurableClientAttributes getDurableClientAttributes() {
    assert !this.isPartial();

    if (durableClientAttributes == null) {
      String durableId = memberIdentifier.getDurableId();

      if (durableId == null || durableId.isEmpty()) {
        durableClientAttributes = new DurableClientAttributes("", DEFAULT_DURABLE_CLIENT_TIMEOUT);
      } else {
        durableClientAttributes =
            new DurableClientAttributes(durableId, memberIdentifier.getDurableTimeout());
      }
    }

    return durableClientAttributes;
  }

  /**
   * Returns an unmodifiable Set of this member's Roles.
   */
  @Override
  public Set<Role> getRoles() {

    if (getGroups() == null) {
      return Collections.emptySet();
    }
    return getGroups().stream().map(InternalRole::getRole).collect(Collectors.toSet());
  }

  @Override
  public int compareTo(DistributedMember o) {
    return compareTo(o, false, true);
  }

  private int compareTo(DistributedMember o, boolean compareMemberData, boolean compareViewIds) {
    if (this == o) {
      return 0;
    }
    // obligatory type check
    if (!(o instanceof InternalDistributedMember))
      throw new ClassCastException(
          "InternalDistributedMember.compareTo(): comparison between different classes");
    InternalDistributedMember other = (InternalDistributedMember) o;

    return compareTo(other.memberIdentifier, compareMemberData, compareViewIds);
  }

  @Override
  public int compareTo(
      MemberIdentifier memberIdentifier, boolean compareMemberData, boolean compareViewIds) {
    return this.memberIdentifier.compareTo(memberIdentifier, compareMemberData, compareViewIds);
  }

  private void defaultToCurrentHost() {
    memberIdentifier.setProcessId(OSProcess.getId());
    try {
      if (SocketCreator.resolve_dns) {
        setHostName(LocalHostUtil.getLocalHostName());
      } else {
        setHostName(LocalHostUtil.getLocalHost().getHostAddress());
      }
    } catch (UnknownHostException ee) {
      throw new InternalGemFireError(ee);
    }
  }

  @Override
  public int getDSFID() {
    return DISTRIBUTED_MEMBER;
  }

  @Override
  public void setDurableTimeout(int newValue) {
    memberIdentifier.setDurableTimeout(newValue);
    durableClientAttributes = null;
  }

  @Override
  public void setDurableId(String id) {
    memberIdentifier.setDurableId(id);
    durableClientAttributes = null;
  }

  @Override
  public void setMemberData(MemberData m) {
    memberIdentifier.setMemberData(m);
    durableClientAttributes = null;
  }

  @Override
  public InetAddress getInetAddress() {
    return memberIdentifier.getInetAddress();
  }

  @Override
  public int getMembershipPort() {
    return memberIdentifier.getMembershipPort();
  }

  @Override
  public short getVersionOrdinal() {
    return memberIdentifier.getVersionOrdinal();
  }

  @Override
  public int getDirectChannelPort() {
    return memberIdentifier.getDirectChannelPort();
  }

  @Override
  public int getVmKind() {
    return memberIdentifier.getVmKind();
  }

  @Override
  public int getMemberWeight() {
    return memberIdentifier.getMemberWeight();
  }

  @Override
  public int getVmViewId() {
    return memberIdentifier.getVmViewId();
  }

  @Override
  public boolean preferredForCoordinator() {
    return memberIdentifier.preferredForCoordinator();
  }

  @Override
  public List<String> getGroups() {
    return memberIdentifier.getGroups();
  }

  @Override
  public void setVmViewId(int p) {
    memberIdentifier.setVmViewId(p);
  }

  @Override
  public void setPreferredForCoordinator(boolean preferred) {
    memberIdentifier.setPreferredForCoordinator(preferred);
  }

  @Override
  public void setDirectChannelPort(int dcPort) {
    memberIdentifier.setDirectChannelPort(dcPort);
  }

  @Override
  public void setVmKind(int dmType) {
    memberIdentifier.setVmKind(dmType);
  }

  @Override
  public String getName() {
    return memberIdentifier.getName();
  }

  @Override
  public boolean isPartial() {
    return memberIdentifier.isPartial();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InternalDistributedMember that = (InternalDistributedMember) o;
    return memberIdentifier.equals(that.memberIdentifier);
  }

  @Override
  public int hashCode() {
    return memberIdentifier.hashCode();
  }

  @Override
  public String toString() {
    return memberIdentifier.toString();
  }

  public void addFixedToString(StringBuilder sb, boolean useIpAddress) {
    memberIdentifier.addFixedToString(sb, useIpAddress);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    memberIdentifier.writeExternal(out);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    memberIdentifier.readExternal(in);
    durableClientAttributes = null;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context)
      throws IOException {
    memberIdentifier.toData(out, context);
  }

  public void toDataPre_GFE_9_0_0_0(DataOutput out,
      SerializationContext context)
      throws IOException {
    memberIdentifier.toDataPre_GFE_9_0_0_0(out, context);
  }

  public void toDataPre_GFE_7_1_0_0(DataOutput out,
      SerializationContext context)
      throws IOException {
    memberIdentifier.toDataPre_GFE_7_1_0_0(out, context);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context)
      throws IOException, ClassNotFoundException {
    memberIdentifier.fromData(in, context);
    durableClientAttributes = null;
  }

  @Override
  public void fromDataPre_GFE_9_0_0_0(DataInput in,
      DeserializationContext context)
      throws IOException, ClassNotFoundException {
    memberIdentifier.fromDataPre_GFE_9_0_0_0(in, context);
    durableClientAttributes = null;
  }

  @Override
  public void fromDataPre_GFE_7_1_0_0(DataInput in,
      DeserializationContext context)
      throws IOException, ClassNotFoundException {
    memberIdentifier.fromDataPre_GFE_7_1_0_0(in, context);
    durableClientAttributes = null;
  }

  @Override
  public void _readEssentialData(DataInput in,
      Function<InetAddress, String> hostnameResolver)
      throws IOException, ClassNotFoundException {
    memberIdentifier._readEssentialData(in, hostnameResolver);
  }

  @Override
  public void writeEssentialData(DataOutput out) throws IOException {
    memberIdentifier.writeEssentialData(out);
  }

  public void setPort(int p) {
    memberIdentifier.setPort(p);
  }

  @Override
  public MemberData getMemberData() {
    return memberIdentifier.getMemberData();
  }

  @Override
  public String getHostName() {
    return memberIdentifier.getHostName();
  }

  @Override
  public String getHost() {
    return memberIdentifier.getHost();
  }

  @Override
  public int getProcessId() {
    return memberIdentifier.getProcessId();
  }

  @Override
  public String getId() {
    return memberIdentifier.getId();
  }

  @Override
  public String getUniqueId() {
    return memberIdentifier.getUniqueId();
  }

  public void setVersionObjectForTest(Version v) {
    memberIdentifier.setVersionObjectForTest(v);
  }

  @Override
  public Version getVersionObject() {
    return memberIdentifier.getVersionObject();
  }

  @Override
  public Version[] getSerializationVersions() {
    return memberIdentifier.getSerializationVersions();
  }

  @Override
  public String getUniqueTag() {
    return memberIdentifier.getUniqueTag();
  }

  public void setUniqueTag(String tag) {
    memberIdentifier.setUniqueTag(tag);
  }

  @Override
  public void setIsPartial(boolean value) {
    memberIdentifier.setIsPartial(value);
  }

  @Override
  public void setName(String name) {
    memberIdentifier.setName(name);
  }

  @Override
  public String getDurableId() {
    return memberIdentifier.getDurableId();
  }

  @Override
  public int getDurableTimeout() {
    return memberIdentifier.getDurableTimeout();
  }

  @Override
  public void setHostName(String hostName) {
    memberIdentifier.setHostName(hostName);
  }

  @Override
  public void setProcessId(int id) {
    memberIdentifier.setProcessId(id);
  }

  @Override
  public boolean hasUUID() {
    return memberIdentifier.hasUUID();
  }

  @Override
  public long getUuidLeastSignificantBits() {
    return memberIdentifier.getUuidLeastSignificantBits();
  }

  @Override
  public long getUuidMostSignificantBits() {
    return memberIdentifier.getUuidMostSignificantBits();
  }

  @Override
  public boolean isNetworkPartitionDetectionEnabled() {
    return memberIdentifier.isNetworkPartitionDetectionEnabled();
  }

  @Override
  public void setUUID(UUID randomUUID) {
    memberIdentifier.setUUID(randomUUID);
  }

  @Override
  public void setMemberWeight(byte b) {
    memberIdentifier.setMemberWeight(b);
  }

  @Override
  public void setUdpPort(int i) {
    memberIdentifier.setUdpPort(i);
  }

  @Override
  public UUID getUUID() {
    return memberIdentifier.getUUID();
  }

  @FunctionalInterface
  public interface HostnameResolver {
    InetAddress getInetAddress(ServerLocation location) throws UnknownHostException;
  }
}
