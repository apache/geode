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
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.Role;
import org.apache.geode.distributed.internal.DistributionAdvisor.ProfileId;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.membership.gms.api.MemberData;
import org.apache.geode.distributed.internal.membership.gms.api.MemberDataBuilder;
import org.apache.geode.distributed.internal.membership.gms.api.MemberIdentifier;
import org.apache.geode.distributed.internal.membership.gms.api.MemberIdentifierImpl;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.internal.serialization.UnsupportedSerializationVersionException;
import org.apache.geode.internal.serialization.Version;

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
  private final Object rolesLock = new Object();
  /**
   * The roles, if any, of this member. Lazily created first time getRoles() is called.
   */
  private volatile Set<Role> rolesSet = null;

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
    String hostName = getHostName(i);

    this.memberData = MemberDataBuilder.newBuilder(i, hostName)
        .setMembershipPort(membershipPort)
        .setNetworkPartitionDetectionEnabled(splitBrainEnabled)
        .setPreferredForCoordinator(canBeCoordinator)
        .build();
    this.versionObj = Version.CURRENT;
  }

  private String getHostName(InetAddress i) {
    return SocketCreator.resolve_dns ? SocketCreator.getHostName(i) : i.getHostAddress();
  }


  /**
   * Construct a InternalDistributedMember based on the given member data.
   *
   */
  public InternalDistributedMember(MemberData m) {
    memberData = m;

    if (memberData.getHostName() == null || memberData.isPartial()) {
      String hostName = getHostName(m.getInetAddress());
      memberData.setHostName(hostName);
    }

    short version = m.getVersionOrdinal();
    try {
      this.versionObj = Version.fromOrdinal(version);
    } catch (UnsupportedSerializationVersionException e) {
      this.versionObj = Version.CURRENT;
    }
    cachedToString = null;
    this.isPartial = m.isPartial();
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
    final InetAddress addr;
    try {
      addr = hostnameResolver.getInetAddress(location);
    } catch (UnknownHostException e) {
      throw new ServerConnectivityException("Unable to resolve server location " + location, e);
    }

    memberData = MemberDataBuilder.newBuilder(addr, location.getHostName())
        .setMembershipPort(location.getPort())
        .setNetworkPartitionDetectionEnabled(false)
        .setPreferredForCoordinator(true)
        .build();
    versionObj = Version.CURRENT;
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
    memberData = builder.build();
    defaultToCurrentHost();
    this.uniqueTag = u;
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
    memberData = MemberDataBuilder.newBuilder(i, "localhost")
        .setMembershipPort(p)
        .build();
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
    memberData = MemberDataBuilder.newBuilder(addr, "localhost")
        .setMembershipPort(p).build();
    if (isCurrentHost) {
      defaultToCurrentHost();
    }
  }


  /** this reads an ID written with writeEssentialData */
  public static InternalDistributedMember readEssentialData(DataInput in)
      throws IOException, ClassNotFoundException {
    final InternalDistributedMember mbr = new InternalDistributedMember();
    mbr._readEssentialData(in);
    return mbr;
  }

  public static void setHostnameResolver(final HostnameResolver hostnameResolver) {
    InternalDistributedMember.hostnameResolver = hostnameResolver;
  }

  /**
   * Returns this client member's durable attributes or null if no durable attributes were created.
   */
  public DurableClientAttributes getDurableClientAttributes() {
    assert !this.isPartial;
    String durableId = memberData.getDurableId();
    if (durableId == null || durableId.isEmpty()) {
      return new DurableClientAttributes("", 300);
    }
    return new DurableClientAttributes(durableId, memberData.getDurableTimeout());
  }

  /**
   * Returns an unmodifiable Set of this member's Roles.
   */
  public Set<Role> getRoles() {
    Set<Role> tmpRolesSet = this.rolesSet;
    if (tmpRolesSet != null) {
      return tmpRolesSet;
    }
    assert !this.isPartial;
    synchronized (this.rolesLock) {
      tmpRolesSet = this.rolesSet;
      if (tmpRolesSet == null) {
        final String[] tmpRoles = memberData.getGroups();
        // convert array of string role names to array of Roles...
        if (tmpRoles == null || tmpRoles.length == 0) {
          tmpRolesSet = Collections.emptySet();
        } else {
          tmpRolesSet = new HashSet<Role>(tmpRoles.length);
          for (int i = 0; i < tmpRoles.length; i++) {
            tmpRolesSet.add(InternalRole.getRole(tmpRoles[i]));
          }
          tmpRolesSet = Collections.unmodifiableSet(tmpRolesSet);
        }
        this.rolesSet = tmpRolesSet;
      }
    }
    Assert.assertTrue(tmpRolesSet != null);
    return tmpRolesSet;
  }

  public void setGroups(String[] newGroups) {
    assert !this.isPartial;
    assert newGroups != null;
    synchronized (this.rolesLock) {
      memberData.setGroups(newGroups);
      this.rolesSet = null;
      this.cachedToString = null;
    }
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
    memberData.setProcessId(OSProcess.getId());
    try {
      if (SocketCreator.resolve_dns) {
        memberData.setHostName(SocketCreator.getHostName(LocalHostUtil.getLocalHost()));
      } else {
        memberData.setHostName(LocalHostUtil.getLocalHost().getHostAddress());
      }
    } catch (UnknownHostException ee) {
      throw new InternalGemFireError(ee);
    }
  }

  public void _readEssentialData(DataInput in) throws IOException, ClassNotFoundException {
    this.isPartial = true;
    InetAddress inetAddr = StaticSerialization.readInetAddress(in);
    int port = in.readInt();

    String hostName = getHostName(inetAddr);

    int flags = in.readUnsignedByte();
    boolean sbEnabled = (flags & NPD_ENABLED_BIT) != 0;
    boolean elCoord = (flags & COORD_ENABLED_BIT) != 0;

    int vmKind = in.readUnsignedByte();
    int vmViewId = -1;

    if (vmKind == MemberIdentifier.LONER_DM_TYPE) {
      this.uniqueTag = StaticSerialization.readString(in);
    } else {
      String str = StaticSerialization.readString(in);
      if (str != null) { // backward compatibility from earlier than 6.5
        vmViewId = Integer.parseInt(str);
      }
    }

    String name = StaticSerialization.readString(in);

    memberData = MemberDataBuilder.newBuilder(inetAddr, hostName)
        .setMembershipPort(port)
        .setName(name)
        .setNetworkPartitionDetectionEnabled(sbEnabled)
        .setPreferredForCoordinator(elCoord)
        .setVersionOrdinal(StaticSerialization.getVersionForDataStream(in).ordinal())
        .setVmKind(vmKind)
        .setVmViewId(vmViewId)
        .build();

    if (StaticSerialization.getVersionForDataStream(in).compareTo(Version.GFE_90) == 0) {
      memberData.readAdditionalData(in);
    }
  }

  public void writeEssentialData(DataOutput out) throws IOException {
    assert memberData.getVmKind() > 0;
    StaticSerialization.writeInetAddress(getInetAddress(), out);
    out.writeInt(getMembershipPort());

    int flags = 0;
    if (memberData.isNetworkPartitionDetectionEnabled())
      flags |= NPD_ENABLED_BIT;
    if (memberData.isPreferredForCoordinator())
      flags |= COORD_ENABLED_BIT;
    flags |= PARTIAL_ID_BIT;
    out.writeByte((byte) (flags & 0xff));

    // out.writeInt(dcPort);
    byte vmKind = memberData.getVmKind();
    out.writeByte(vmKind);

    if (vmKind == MemberIdentifier.LONER_DM_TYPE) {
      StaticSerialization.writeString(this.uniqueTag, out);
    } else { // added in 6.5 for unique identifiers in P2P
      StaticSerialization.writeString(String.valueOf(memberData.getVmViewId()), out);
    }
    // write name last to fix bug 45160
    StaticSerialization.writeString(memberData.getName(), out);

    Version outputVersion = StaticSerialization.getVersionForDataStream(out);
    if (0 <= outputVersion.compareTo(Version.GFE_90)
        && outputVersion.compareTo(Version.GEODE_1_1_0) < 0) {
      memberData.writeAdditionalData(out);
    }
  }

  @FunctionalInterface
  public interface HostnameResolver {
    InetAddress getInetAddress(ServerLocation location) throws UnknownHostException;
  }
}
