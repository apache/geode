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
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.Role;
import org.apache.geode.distributed.internal.DistributionAdvisor.ProfileId;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.api.MemberData;
import org.apache.geode.distributed.internal.membership.api.MemberDataBuilder;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifierImpl;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.OSProcess;

/**
 * This is the fundamental representation of a member of a GemFire distributed system.
 */
public class InternalDistributedMember extends MemberIdentifierImpl
    implements DistributedMember, Externalizable, ProfileId, VersionSource<DistributedMember> {
  private static final long serialVersionUID = -2785249969777296507L;

  /** Retrieves an InetAddress given the provided hostname */
  @MutableForTesting
  protected static HostnameResolver hostnameResolver =
      (location) -> InetAddress.getByName(location.getHostName());

  /** lock object used when getting/setting roles/rolesSet fields */

  // Used only by deserialization
  public InternalDistributedMember() {}

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

    super(MemberDataBuilder.newBuilder(i, getHostName(i))
        .setMembershipPort(membershipPort)
        .setNetworkPartitionDetectionEnabled(splitBrainEnabled)
        .setPreferredForCoordinator(canBeCoordinator)
        .build(), null);
  }

  private static String getHostName(InetAddress i) {
    return SocketCreator.resolve_dns ? SocketCreator.getHostName(i) : i.getHostAddress();
  }

  /**
   * Construct a InternalDistributedMember based on the given member data.
   *
   */
  public InternalDistributedMember(MemberData m) {
    super(m, null);

    if (getMemberData().getHostName() == null || getMemberData().isPartial()) {
      String hostName = getHostName(m.getInetAddress());
      getMemberData().setHostName(hostName);
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
   * caches. The version information in the ID is set to Version.CURRENT.
   *
   * @param location the coordinates of the server
   */

  public InternalDistributedMember(ServerLocation location) {
    super(MemberDataBuilder.newBuilder(getInetAddress(location), location.getHostName())
        .setMembershipPort(location.getPort())
        .setNetworkPartitionDetectionEnabled(false)
        .setPreferredForCoordinator(true)
        .build(), null);
  }

  private static InetAddress getInetAddress(ServerLocation location) {
    final InetAddress addr;
    try {
      addr = hostnameResolver.getInetAddress(location);
    } catch (UnknownHostException e) {
      throw new ServerConnectivityException("Unable to resolve server location " + location, e);
    }
    return addr;
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
   * @throws UnknownHostException if the given hostname cannot be resolved
   */
  public InternalDistributedMember(String host, int p, String n, String u, int vmKind,
      String[] groups, DurableClientAttributes attr) throws UnknownHostException {
    super(createMemberData(host, p, n, vmKind, groups, attr), u);

    defaultToCurrentHost();
  }

  private static MemberData createMemberData(String host, int p, String n, int vmKind,
      String[] groups,
      DurableClientAttributes attr) {
    InetAddress addr = LocalHostUtil.toInetAddress(host);
    MemberDataBuilder builder = MemberDataBuilder.newBuilder(addr, host)
        .setName(n)
        .setMembershipPort(p)
        .setDirectChannelPort(p)
        .setPreferredForCoordinator(false)
        .setNetworkPartitionDetectionEnabled(true)
        .setVmKind(vmKind)
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
    super(MemberDataBuilder.newBuilder(i, "localhost")
        .setMembershipPort(p)
        .build(), null);
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
    super(MemberDataBuilder.newBuilder(addr, "localhost")
        .setMembershipPort(p).build(), null);
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

  public static void setHostnameResolver(final HostnameResolver hostnameResolver) {
    InternalDistributedMember.hostnameResolver = hostnameResolver;
  }

  /**
   * Returns this client member's durable attributes or null if no durable attributes were created.
   */
  public DurableClientAttributes getDurableClientAttributes() {
    assert !this.isPartial();
    String durableId = getMemberData().getDurableId();
    if (durableId == null || durableId.isEmpty()) {
      return new DurableClientAttributes("", 300);
    }
    return new DurableClientAttributes(durableId, getMemberData().getDurableTimeout());
  }

  /**
   * Returns an unmodifiable Set of this member's Roles.
   */
  public Set<Role> getRoles() {

    if (getMemberData().getGroups() == null) {
      return Collections.emptySet();
    }
    return getGroups().stream().map(InternalRole::getRole).collect(Collectors.toSet());
  }

  public int compareTo(DistributedMember o) {
    return compareTo(o, false, true);
  }

  public int compareTo(DistributedMember o, boolean compareMemberData, boolean compareViewIds) {
    if (this == o) {
      return 0;
    }
    // obligatory type check
    if (!(o instanceof InternalDistributedMember))
      throw new ClassCastException(
          "InternalDistributedMember.compareTo(): comparison between different classes");
    MemberIdentifierImpl other = (MemberIdentifierImpl) o;

    return compareTo(other, compareMemberData, compareViewIds);
  }

  protected void defaultToCurrentHost() {
    getMemberData().setProcessId(OSProcess.getId());
    try {
      if (SocketCreator.resolve_dns) {
        getMemberData().setHostName(SocketCreator.getHostName(LocalHostUtil.getLocalHost()));
      } else {
        getMemberData().setHostName(LocalHostUtil.getLocalHost().getHostAddress());
      }
    } catch (UnknownHostException ee) {
      throw new InternalGemFireError(ee);
    }
  }

  @Override
  public void toDataPre_GFE_9_0_0_0(DataOutput out, SerializationContext context)
      throws IOException {
    super.toDataPre_GFE_9_0_0_0(out, context);
  }

  @Override
  public void toDataPre_GFE_7_1_0_0(DataOutput out, SerializationContext context)
      throws IOException {
    super.toDataPre_GFE_7_1_0_0(out, context);
  }

  @Override
  public void fromDataPre_GFE_9_0_0_0(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromDataPre_GFE_9_0_0_0(in, context);
  }

  @Override
  public void fromDataPre_GFE_7_1_0_0(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    super.fromDataPre_GFE_7_1_0_0(in, context);
  }

  @FunctionalInterface
  public interface HostnameResolver {
    InetAddress getInetAddress(ServerLocation location) throws UnknownHostException;
  }
}
