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
import java.io.EOFException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MutableForTesting;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.Role;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionAdvisor.ProfileId;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.UnsupportedSerializationVersionException;
import org.apache.geode.internal.serialization.Version;

/**
 * This is the fundamental representation of a member of a GemFire distributed system.
 */
public class InternalDistributedMember implements DistributedMember, Externalizable,
    DataSerializableFixedID, ProfileId, VersionSource<DistributedMember> {
  private static final long serialVersionUID = -2785249969777296507L;

  protected NetMember netMbr; // the underlying member object

  /**
   * whether this is a partial member ID (without roles, durable attributes). We use partial IDs in
   * EventID objects to reduce their size. It would be better to use canonical IDs but there is
   * currently no central mechanism that would allow that for both server and client identifiers
   */
  private boolean isPartial;

  /**
   * The roles, if any, of this member. Lazily created first time getRoles() is called.
   */
  private volatile Set<Role> rolesSet = null;

  /** lock object used when getting/setting roles/rolesSet fields */
  private final Object rolesLock = new Object();

  /**
   * Unique tag (such as randomly generated bytes) to help enforce uniqueness. Note: this should be
   * displayable.
   */
  private String uniqueTag = null;

  /** serialization bit flag */
  private static final int NPD_ENABLED_BIT = 0x1;

  /** serialization bit flag */
  private static final int COORD_ENABLED_BIT = 0x2;

  /** partial ID bit flag */
  private static final int PARTIAL_ID_BIT = 0x4;

  /** product version bit flag */
  private static final int VERSION_BIT = 0x8;

  public int getVmPid() {
    return netMbr.getProcessId();
  }

  @FunctionalInterface
  public interface HostnameResolver {
    InetAddress getInetAddress(ServerLocation location) throws UnknownHostException;
  }

  public static void setHostnameResolver(final HostnameResolver hostnameResolver) {
    InternalDistributedMember.hostnameResolver = hostnameResolver;
  }

  /** Retrieves an InetAddress given the provided hostname */
  @MutableForTesting
  private static HostnameResolver hostnameResolver =
      (location) -> InetAddress.getByName(location.getHostName());

  private transient Version versionObj = Version.CURRENT;

  /** The versions in which this message was modified */
  @Immutable
  private static final Version[] dsfidVersions = new Version[] {
      Version.GFE_71, Version.GFE_90};

  private void defaultToCurrentHost() {
    netMbr.setProcessId(OSProcess.getId());
    try {
      if (SocketCreator.resolve_dns) {
        netMbr.setHostName(SocketCreator.getHostName(SocketCreator.getLocalHost()));
      } else {
        netMbr.setHostName(SocketCreator.getLocalHost().getHostAddress());
      }
    } catch (UnknownHostException ee) {
      throw new InternalGemFireError(ee);
    }
  }


  // Used only by Externalization
  public InternalDistributedMember() {}

  /**
   * Construct a InternalDistributedMember. All fields are specified.
   * <p>
   *
   * This, and the following constructor are the only valid ways to create an ID for a distributed
   * member for use in the P2P cache. Use of other constructors can break
   * network-partition-detection.
   *
   * @param i the inet address
   * @param p the membership port
   * @param splitBrainEnabled whether this feature is enabled for the member
   * @param canBeCoordinator whether the member is eligible to be the membership coordinator
   * @param attr the member's attributes
   */
  public InternalDistributedMember(InetAddress i, int p, boolean splitBrainEnabled,
      boolean canBeCoordinator, MemberAttributes attr) {

    String hostName = SocketCreator.resolve_dns ? SocketCreator.getHostName(i) : i.getHostAddress();

    this.netMbr = NetMemberFactory.newNetMember(i, hostName, p, splitBrainEnabled, canBeCoordinator,
        Version.CURRENT_ORDINAL,
        attr);

    short version = netMbr.getVersionOrdinal();
    try {
      this.versionObj = Version.fromOrdinal(version);
    } catch (UnsupportedSerializationVersionException e) {
      this.versionObj = Version.CURRENT;
    }
    // checkHostName();
  }


  /**
   * Construct a InternalDistributedMember based on the given NetMember.
   *
   */
  public InternalDistributedMember(NetMember m) {
    netMbr = m;

    if (netMbr.getHostName() == null || netMbr.isPartial()) {
      String hostName = SocketCreator.resolve_dns ? SocketCreator.getHostName(m.getInetAddress())
          : m.getInetAddress().getHostAddress();
      netMbr.setHostName(hostName);
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
   * Replace the current NetMember with the given member. This can be used to fill out an
   * InternalDistributedMember that was created from a partial NetMember created by
   * readEssentialData.
   *
   * @param m the replacement NetMember
   */
  public void setNetMember(NetMember m) {
    this.netMbr = m;
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
   * @param i the hostname, must be for the current host
   * @param p the membership listening port
   * @throws UnknownHostException if the given hostname cannot be resolved
   */
  public InternalDistributedMember(String i, int p) {
    this(NetMemberFactory.newNetMember(i, p));
  }

  /**
   * Creates a new InternalDistributedMember for use in notifying membership listeners. The version
   * information in the ID is set to Version.CURRENT.
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

    netMbr =
        NetMemberFactory.newNetMember(addr, location.getHostName(), location.getPort(), false, true,
            Version.CURRENT_ORDINAL, MemberAttributes.DEFAULT);
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
   * @param p the membership listening port
   * @param n gemfire properties connection name
   * @param u unique string used make the member more unique
   * @param vmKind the dmType
   * @param groups the server groups / roles
   * @param attr durable client attributes, if any
   *
   * @throws UnknownHostException if the given hostname cannot be resolved
   */
  public InternalDistributedMember(String host, int p, String n, String u, int vmKind,
      String[] groups, DurableClientAttributes attr) throws UnknownHostException {
    MemberAttributes mattr = new MemberAttributes(p, org.apache.geode.internal.OSProcess.getId(),
        vmKind, -1, n, groups, attr);
    InetAddress addr = SocketCreator.toInetAddress(host);
    netMbr = NetMemberFactory
        .newNetMember(addr, host, p, false, true, Version.CURRENT_ORDINAL, mattr);
    defaultToCurrentHost();
    netMbr.setName(n);
    this.uniqueTag = u;
    netMbr.setVmKind(vmKind);
    netMbr.setDirectPort(p);
    netMbr.setDurableClientAttributes(attr);
    netMbr.setGroups(groups);
  }

  /**
   * Create a InternalDistributedMember referring to the current host (as defined by the given
   * address).
   * <p>
   *
   * <b> THIS METHOD IS FOR TESTING ONLY. DO NOT USE IT TO CREATE IDs FOR USE IN THE PRODUCT. IT
   * DOES NOT PROPERLY INITIALIZE ATTRIBUTES NEEDED FOR P2P FUNCTIONALITY. </b>
   *
   *
   * @param i the hostname, must be for the current host
   * @param p the membership listening port
   */
  public InternalDistributedMember(InetAddress i, int p) {
    netMbr = NetMemberFactory.newNetMember(i, p);
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
    netMbr = NetMemberFactory.newNetMember(addr, p);
    if (isCurrentHost) {
      defaultToCurrentHost();
    }
  }

  /**
   * Return the underlying host address
   *
   * @return the underlying host address
   */
  public InetAddress getInetAddress() {
    return netMbr.getInetAddress();
  }

  public NetMember getNetMember() {
    return netMbr;
  }

  /**
   * Return the underlying port (membership port)
   *
   * @return the underlying membership port
   */
  public int getPort() {
    return netMbr.getPort();
  }


  /**
   * Returns the port on which the direct channel runs
   */
  public int getDirectChannelPort() {
    assert !this.isPartial;
    return netMbr.getDirectPort();
  }

  /**
   * [GemStone] Returns the kind of VM that hosts the distribution manager with this address.
   *
   * @see ClusterDistributionManager#getDMType()
   * @see ClusterDistributionManager#NORMAL_DM_TYPE
   */
  public int getVmKind() {
    return netMbr.getVmKind();
  }

  /**
   * Returns the membership view ID that this member was born in. For backward compatibility reasons
   * this is limited to 16 bits.
   */
  public int getVmViewId() {
    return netMbr.getVmViewId();
  }

  /**
   * Returns an unmodifiable Set of this member's Roles.
   */
  @Override
  public Set<Role> getRoles() {
    Set<Role> tmpRolesSet = this.rolesSet;
    if (tmpRolesSet != null) {
      return tmpRolesSet;
    }
    assert !this.isPartial;
    synchronized (this.rolesLock) {
      tmpRolesSet = this.rolesSet;
      if (tmpRolesSet == null) {
        final String[] tmpRoles = netMbr.getGroups();
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

  @Override
  public List<String> getGroups() {
    return Collections.unmodifiableList(Arrays.asList(netMbr.getGroups()));
  }

  public void setGroups(String[] newGroups) {
    assert !this.isPartial;
    assert newGroups != null;
    synchronized (this.rolesLock) {
      netMbr.setGroups(newGroups);
      this.rolesSet = null;
      this.cachedToString = null;
    }
  }

  public void setVmViewId(int p) {
    netMbr.setVmViewId(p);
    cachedToString = null;
  }

  /**
   * Returns the name of this member's distributed system connection or null if no name was
   * specified.
   */
  @Override
  public String getName() {
    String result = netMbr.getName();
    if (result == null) {
      result = "";
    }
    return result;
  }

  /**
   * Returns this client member's durable attributes or null if no durable attributes were created.
   */
  @Override
  public DurableClientAttributes getDurableClientAttributes() {
    assert !this.isPartial;
    DurableClientAttributes attributes = netMbr.getDurableClientAttributes();
    if (attributes == null) {
      attributes = new DurableClientAttributes("", 300);
      netMbr.setDurableClientAttributes(attributes);
    }
    return netMbr.getDurableClientAttributes();
  }

  /**
   * implements the java.lang.Comparable interface
   *
   * @see java.lang.Comparable
   * @param o - the Object to be compared
   * @return a negative integer, zero, or a positive integer as this object is less than, equal to,
   *         or greater than the specified object.
   * @exception java.lang.ClassCastException - if the specified object's type prevents it from being
   *            compared to this Object.
   */
  @Override
  public int compareTo(DistributedMember o) {
    return compareTo(o, true);
  }

  public int compareTo(DistributedMember o, boolean checkNetMembersIfEqual) {
    return compareTo(o, checkNetMembersIfEqual, true);
  }

  public int compareTo(DistributedMember o, boolean checkNetMembersIfEqual, boolean verifyViewId) {
    if (this == o) {
      return 0;
    }
    // obligatory type check
    if (!(o instanceof InternalDistributedMember))
      throw new ClassCastException(
          "InternalDistributedMember.compareTo(): comparison between different classes");
    InternalDistributedMember other = (InternalDistributedMember) o;

    int myPort = getPort();
    int otherPort = other.getPort();
    if (myPort < otherPort)
      return -1;
    if (myPort > otherPort)
      return 1;


    InetAddress myAddr = getInetAddress();
    InetAddress otherAddr = other.getInetAddress();

    // Discard null cases
    if (myAddr == null && otherAddr == null) {
      return 0;
    } else if (myAddr == null) {
      return -1;
    } else if (otherAddr == null)
      return 1;

    byte[] myBytes = myAddr.getAddress();
    byte[] otherBytes = otherAddr.getAddress();

    if (myBytes != otherBytes) {
      for (int i = 0; i < myBytes.length; i++) {
        if (i >= otherBytes.length)
          return -1; // same as far as they go, but shorter...
        if (myBytes[i] < otherBytes[i])
          return -1;
        if (myBytes[i] > otherBytes[i])
          return 1;
      }
      if (myBytes.length > otherBytes.length)
        return 1; // same as far as they go, but longer...
    }

    String myName = getName();
    String otherName = other.getName();
    if (!(other.isPartial || this.isPartial)) {
      if (myName == null && otherName == null) {
        // do nothing
      } else if (myName == null) {
        return -1;
      } else if (otherName == null) {
        return 1;
      } else {
        int i = myName.compareTo(otherName);
        if (i != 0) {
          return i;
        }
      }
    }

    if (this.uniqueTag == null && other.uniqueTag == null) {
      if (verifyViewId) {
        // not loners, so look at P2P view ID
        int thisViewId = getVmViewId();
        int otherViewId = other.getVmViewId();
        if (thisViewId >= 0 && otherViewId >= 0) {
          if (thisViewId < otherViewId) {
            return -1;
          } else if (thisViewId > otherViewId) {
            return 1;
          } // else they're the same, so continue
        }
      }
    } else if (this.uniqueTag == null) {
      return -1;
    } else if (other.uniqueTag == null) {
      return 1;
    } else {
      int i = this.uniqueTag.compareTo(other.uniqueTag);
      if (i != 0) {
        return i;
      }
    }

    if (checkNetMembersIfEqual && this.netMbr != null && other.netMbr != null) {
      return this.netMbr.compareAdditionalData(other.netMbr);
    } else {
      return 0;
    }

    // purposely avoid comparing roles
    // @todo Add durableClientAttributes to compare
  }

  /**
   * An InternalDistributedMember created for a test or via readEssentialData will be a Partial ID,
   * possibly not having ancillary info like "name".
   *
   * @return true if this is a partial ID
   */
  public boolean isPartial() {
    return isPartial;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    // GemStone fix for 29125
    if (!(obj instanceof InternalDistributedMember)) {
      return false;
    }
    InternalDistributedMember other = (InternalDistributedMember) obj;

    int myPort = getPort();
    int otherPort = other.getPort();
    if (myPort != otherPort) {
      return false;
    }

    InetAddress myAddr = getInetAddress();
    InetAddress otherAddr = other.getInetAddress();
    if (myAddr == null && otherAddr == null) {
      return true;
    } else if (!Objects.equals(myAddr, otherAddr)) {
      return false;
    }

    if (!isPartial() && !other.isPartial()) {
      if (!Objects.equals(getName(), other.getName())) {
        return false;
      }
    }

    if (this.uniqueTag == null && other.uniqueTag == null) {
      // not loners, so look at P2P view ID
      int thisViewId = getVmViewId();
      int otherViewId = other.getVmViewId();
      if (thisViewId >= 0 && otherViewId >= 0) {
        if (thisViewId != otherViewId) {
          return false;
        } // else they're the same, so continue
      }
    } else if (!Objects.equals(this.uniqueTag, other.uniqueTag)) {
      return false;
    }

    if (this.netMbr != null && other.netMbr != null) {
      if (0 != this.netMbr.compareAdditionalData(other.netMbr)) {
        return false;
      }
    }

    // purposely avoid checking roles
    // @todo Add durableClientAttributes to equals

    return true;
  }

  @Override
  public int hashCode() {
    int result = 0;
    result = result + netMbr.getInetAddress().hashCode();
    result = result + getPort();
    return result;
  }

  private String shortName(String hostname) {
    if (hostname == null)
      return "<null inet_addr hostname>";
    int index = hostname.indexOf('.');

    if (index > 0 && !Character.isDigit(hostname.charAt(0)))
      return hostname.substring(0, index);
    else
      return hostname;
  }


  /** the cached string description of this object */
  private transient String cachedToString;

  @Override
  public String toString() {
    String result = cachedToString;
    if (result == null) {
      final StringBuilder sb = new StringBuilder();
      addFixedToString(sb);

      // add version if not current
      short version = netMbr.getVersionOrdinal();
      if (version != Version.CURRENT.ordinal()) {
        sb.append("(version:").append(Version.toString(version)).append(')');
      }

      // leave out Roles on purpose

      // if (netMbr instanceof GMSMember) {
      // sb.append("(UUID=").append(((GMSMember)netMbr).getUUID()).append(")");
      // }

      result = sb.toString();
      cachedToString = result;
    }
    return result;
  }

  public void addFixedToString(StringBuilder sb) {
    // Note: This method is used to generate the HARegion name. If it is changed, memory and GII
    // issues will occur in the case of clients with subscriptions during rolling upgrade.
    String host;

    InetAddress add = getInetAddress();
    if (add.isMulticastAddress())
      host = add.getHostAddress();
    else {
      String hostName = netMbr.getHostName();
      host = SocketCreator.resolve_dns ? shortName(hostName) : hostName;
    }

    sb.append(host);

    String myName = getName();
    int vmPid = netMbr.getProcessId();
    int vmKind = netMbr.getVmKind();
    if (vmPid > 0 || vmKind != ClusterDistributionManager.NORMAL_DM_TYPE || !"".equals(myName)) {
      sb.append("(");

      if (!"".equals(myName)) {
        sb.append(myName);
        if (vmPid > 0) {
          sb.append(':');
        }
      }

      if (vmPid > 0)
        sb.append(vmPid);

      String vmStr = "";
      switch (vmKind) {
        case ClusterDistributionManager.NORMAL_DM_TYPE:
          // vmStr = ":local"; // let this be silent
          break;
        case ClusterDistributionManager.LOCATOR_DM_TYPE:
          vmStr = ":locator";
          break;
        case ClusterDistributionManager.ADMIN_ONLY_DM_TYPE:
          vmStr = ":admin";
          break;
        case ClusterDistributionManager.LONER_DM_TYPE:
          vmStr = ":loner";
          break;
        default:
          vmStr = ":<unknown:" + vmKind + ">";
          break;
      }
      sb.append(vmStr);
      sb.append(")");
    }
    if (vmKind != ClusterDistributionManager.LONER_DM_TYPE && netMbr.preferredForCoordinator()) {
      sb.append("<ec>");
    }
    int vmViewId = getVmViewId();
    if (vmViewId >= 0) {
      sb.append("<v" + vmViewId + ">");
    }
    sb.append(":");
    sb.append(getPort());

    if (vmKind == ClusterDistributionManager.LONER_DM_TYPE) {
      // add some more info that was added in 4.2.1 for loner bridge clients
      // impact on non-bridge loners is ok
      if (this.uniqueTag != null && this.uniqueTag.length() != 0) {
        sb.append(":").append(this.uniqueTag);
      }
      String name = getName();
      if (name.length() != 0) {
        sb.append(":").append(name);
      }
    }
  }

  private short readVersion(int flags, DataInput in) throws IOException {
    if ((flags & VERSION_BIT) != 0) {
      short version = Version.readOrdinal(in);
      this.versionObj = Version.fromOrdinalNoThrow(version, false);
      return version;
    } else {
      // prior to 7.1 member IDs did not serialize their version information
      Version v = InternalDataSerializer.getVersionForDataStreamOrNull(in);
      if (v != null) {
        this.versionObj = v;
        return v.ordinal();
      }
      return Version.CURRENT_ORDINAL;
    }
  }

  /**
   * For Externalizable
   *
   * @see Externalizable
   */
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    Assert.assertTrue(netMbr.getVmKind() > 0);

    // do it the way we like
    byte[] address = getInetAddress().getAddress();

    out.writeInt(address.length); // IPv6 compatible
    out.write(address);
    out.writeInt(getPort());

    DataSerializer.writeString(netMbr.getHostName(), out);

    int flags = 0;
    if (netMbr.isNetworkPartitionDetectionEnabled())
      flags |= NPD_ENABLED_BIT;
    if (netMbr.preferredForCoordinator())
      flags |= COORD_ENABLED_BIT;
    if (this.isPartial)
      flags |= PARTIAL_ID_BIT;
    // always write product version but enable reading from older versions
    // that do not have it
    flags |= VERSION_BIT;
    out.writeByte((byte) (flags & 0xff));

    out.writeInt(netMbr.getDirectPort());
    out.writeInt(netMbr.getProcessId());
    out.writeInt(netMbr.getVmKind());
    out.writeInt(netMbr.getVmViewId());
    DataSerializer.writeStringArray(netMbr.getGroups(), out);

    DataSerializer.writeString(netMbr.getName(), out);
    DataSerializer.writeString(this.uniqueTag, out);
    DurableClientAttributes attributes = netMbr.getDurableClientAttributes();
    DataSerializer.writeString(attributes == null ? "" : attributes.getId(), out);
    DataSerializer.writeInteger(Integer.valueOf(attributes == null ? 300 : attributes.getTimeout()),
        out);
    Version.writeOrdinal(out, netMbr.getVersionOrdinal(), true);
    netMbr.writeAdditionalData(out);
  }

  /**
   * For Externalizable
   *
   * @see Externalizable
   */
  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    int len = in.readInt(); // IPv6 compatible
    byte addr[] = new byte[len];
    in.readFully(addr);
    InetAddress inetAddr = InetAddress.getByAddress(addr);
    int port = in.readInt();

    String hostName = DataSerializer.readString(in);

    int flags = in.readUnsignedByte();
    boolean sbEnabled = (flags & NPD_ENABLED_BIT) != 0;
    boolean elCoord = (flags & COORD_ENABLED_BIT) != 0;
    this.isPartial = (flags & PARTIAL_ID_BIT) != 0;

    int dcPort = in.readInt();
    int vmPid = in.readInt();
    int vmKind = in.readInt();
    int vmViewId = in.readInt();
    String[] groups = DataSerializer.readStringArray(in);

    String name = DataSerializer.readString(in);
    this.uniqueTag = DataSerializer.readString(in);
    String durableId = DataSerializer.readString(in);
    int durableTimeout = in.readInt();
    DurableClientAttributes durableClientAttributes =
        new DurableClientAttributes(durableId, durableTimeout);

    short version = readVersion(flags, in);

    netMbr = NetMemberFactory.newNetMember(inetAddr, hostName, port, sbEnabled, elCoord, version,
        new MemberAttributes(dcPort, vmPid, vmKind, vmViewId, name, groups,
            durableClientAttributes));
    if (version >= Version.GFE_90.ordinal()) {
      try {
        netMbr.readAdditionalData(in);
      } catch (java.io.EOFException e) {
        // old version
      }
    }

    Assert.assertTrue(netMbr.getVmKind() > 0);
  }

  @Override
  public int getDSFID() {
    return DISTRIBUTED_MEMBER;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    toDataPre_GFE_9_0_0_0(out, context);
    if (netMbr.getVersionOrdinal() >= Version.GFE_90.ordinal()) {
      getNetMember().writeAdditionalData(out);
    }
  }


  public void toDataPre_GFE_9_0_0_0(DataOutput out, SerializationContext context)
      throws IOException {
    // Assert.assertTrue(vmKind > 0);
    // NOTE: If you change the serialized format of this class
    // then bump Connection.HANDSHAKE_VERSION since an
    // instance of this class is sent during Connection handshake.
    DataSerializer.writeInetAddress(getInetAddress(), out);
    out.writeInt(getPort());

    DataSerializer.writeString(netMbr.getHostName(), out);

    int flags = 0;
    if (netMbr.isNetworkPartitionDetectionEnabled())
      flags |= NPD_ENABLED_BIT;
    if (netMbr.preferredForCoordinator())
      flags |= COORD_ENABLED_BIT;
    if (this.isPartial)
      flags |= PARTIAL_ID_BIT;
    // always write product version but enable reading from older versions
    // that do not have it
    flags |= VERSION_BIT;

    out.writeByte((byte) (flags & 0xff));

    out.writeInt(netMbr.getDirectPort());
    out.writeInt(netMbr.getProcessId());
    int vmKind = netMbr.getVmKind();
    out.writeByte(vmKind);
    DataSerializer.writeStringArray(netMbr.getGroups(), out);

    DataSerializer.writeString(netMbr.getName(), out);
    if (vmKind == ClusterDistributionManager.LONER_DM_TYPE) {
      DataSerializer.writeString(this.uniqueTag, out);
    } else { // added in 6.5 for unique identifiers in P2P
      DataSerializer.writeString(String.valueOf(netMbr.getVmViewId()), out);
    }
    DurableClientAttributes durableClientAttributes = netMbr.getDurableClientAttributes();
    DataSerializer
        .writeString(durableClientAttributes == null ? "" : durableClientAttributes.getId(), out);
    DataSerializer.writeInteger(Integer.valueOf(
        durableClientAttributes == null ? 300 : durableClientAttributes.getTimeout()), out);

    short version = netMbr.getVersionOrdinal();
    Version.writeOrdinal(out, version, true);
  }

  public void toDataPre_GFE_7_1_0_0(DataOutput out, SerializationContext context)
      throws IOException {
    Assert.assertTrue(netMbr.getVmKind() > 0);
    // disabled to allow post-connect setting of the port for loner systems
    // Assert.assertTrue(getPort() > 0);
    // if (this.getPort() == 0) {
    // InternalDistributedSystem.getLogger().warning(String.format("%s",
    // "Serializing ID with zero port", new Exception("Stack trace")));
    // }

    // NOTE: If you change the serialized format of this class
    // then bump Connection.HANDSHAKE_VERSION since an
    // instance of this class is sent during Connection handshake.
    DataSerializer.writeInetAddress(getInetAddress(), out);
    out.writeInt(getPort());

    DataSerializer.writeString(netMbr.getHostName(), out);

    int flags = 0;
    if (netMbr.isNetworkPartitionDetectionEnabled())
      flags |= NPD_ENABLED_BIT;
    if (netMbr.preferredForCoordinator())
      flags |= COORD_ENABLED_BIT;
    if (this.isPartial)
      flags |= PARTIAL_ID_BIT;
    out.writeByte((byte) (flags & 0xff));

    out.writeInt(netMbr.getDirectPort());
    out.writeInt(netMbr.getProcessId());
    out.writeByte(netMbr.getVmKind());
    DataSerializer.writeStringArray(netMbr.getGroups(), out);

    DataSerializer.writeString(netMbr.getName(), out);
    int vmKind = netMbr.getVmKind();
    if (vmKind == ClusterDistributionManager.LONER_DM_TYPE) {
      DataSerializer.writeString(this.uniqueTag, out);
    } else { // added in 6.5 for unique identifiers in P2P
      DataSerializer.writeString(String.valueOf(netMbr.getVmViewId()), out);
    }
    DurableClientAttributes durableClientAttributes = netMbr.getDurableClientAttributes();
    DataSerializer
        .writeString(durableClientAttributes == null ? "" : durableClientAttributes.getId(), out);
    DataSerializer.writeInteger(Integer.valueOf(
        durableClientAttributes == null ? 300 : durableClientAttributes.getTimeout()), out);
  }


  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    fromDataPre_GFE_9_0_0_0(in, context);
    // just in case this is just a non-versioned read
    // from a file we ought to check the version
    if (getNetMember().getVersionOrdinal() >= Version.GFE_90.ordinal()) {
      try {
        netMbr.readAdditionalData(in);
      } catch (EOFException e) {
        // nope - it's from a pre-GEODE client or WAN site
      }
    }
  }

  public void fromDataPre_GFE_9_0_0_0(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    InetAddress inetAddr = DataSerializer.readInetAddress(in);
    int port = in.readInt();

    String hostName = DataSerializer.readString(in);

    hostName = SocketCreator.resolve_dns
        ? SocketCreator.getCanonicalHostName(inetAddr, hostName) : inetAddr.getHostAddress();

    int flags = in.readUnsignedByte();
    boolean sbEnabled = (flags & NPD_ENABLED_BIT) != 0;
    boolean elCoord = (flags & COORD_ENABLED_BIT) != 0;
    this.isPartial = (flags & PARTIAL_ID_BIT) != 0;

    int dcPort = in.readInt();
    int vmPid = in.readInt();
    int vmKind = in.readUnsignedByte();
    String[] groups = DataSerializer.readStringArray(in);
    int vmViewId = -1;

    String name = DataSerializer.readString(in);
    if (vmKind == ClusterDistributionManager.LONER_DM_TYPE) {
      this.uniqueTag = DataSerializer.readString(in);
    } else {
      String str = DataSerializer.readString(in);
      if (str != null) { // backward compatibility from earlier than 6.5
        vmViewId = Integer.parseInt(str);
      }
    }

    String durableId = DataSerializer.readString(in);
    int durableTimeout = in.readInt();
    DurableClientAttributes durableClientAttributes =
        durableId.length() > 0 ? new DurableClientAttributes(durableId, durableTimeout) : null;

    short version = readVersion(flags, in);

    MemberAttributes attr = new MemberAttributes(dcPort, vmPid, vmKind, vmViewId, name, groups,
        durableClientAttributes);
    netMbr =
        NetMemberFactory.newNetMember(inetAddr, hostName, port, sbEnabled, elCoord, version, attr);

    Assert.assertTrue(netMbr.getVmKind() > 0);
    // Assert.assertTrue(getPort() > 0);
  }

  public void fromDataPre_GFE_7_1_0_0(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    InetAddress inetAddr = DataSerializer.readInetAddress(in);
    int port = in.readInt();

    String hostName = DataSerializer.readString(in);

    hostName = SocketCreator.resolve_dns
        ? SocketCreator.getCanonicalHostName(inetAddr, hostName) : inetAddr.getHostAddress();

    int flags = in.readUnsignedByte();
    boolean sbEnabled = (flags & NPD_ENABLED_BIT) != 0;
    boolean elCoord = (flags & COORD_ENABLED_BIT) != 0;
    this.isPartial = (flags & PARTIAL_ID_BIT) != 0;

    int dcPort = in.readInt();
    int vmPid = in.readInt();
    int vmKind = in.readUnsignedByte();
    String[] groups = DataSerializer.readStringArray(in);
    int vmViewId = -1;

    String name = DataSerializer.readString(in);
    if (vmKind == ClusterDistributionManager.LONER_DM_TYPE) {
      this.uniqueTag = DataSerializer.readString(in);
    } else {
      String str = DataSerializer.readString(in);
      if (str != null) { // backward compatibility from earlier than 6.5
        vmViewId = Integer.parseInt(str);
      }
    }

    String durableId = DataSerializer.readString(in);
    int durableTimeout = in.readInt();
    DurableClientAttributes durableClientAttributes =
        durableId.length() > 0 ? new DurableClientAttributes(durableId, durableTimeout) : null;

    short version = readVersion(flags, in);

    MemberAttributes attr = new MemberAttributes(dcPort, vmPid, vmKind, vmViewId, name, groups,
        durableClientAttributes);
    netMbr =
        NetMemberFactory.newNetMember(inetAddr, hostName, port, sbEnabled, elCoord, version, attr);

    Assert.assertTrue(netMbr.getVmKind() > 0);
  }

  /** this reads an ID written with writeEssentialData */
  public static InternalDistributedMember readEssentialData(DataInput in)
      throws IOException, ClassNotFoundException {
    final InternalDistributedMember mbr = new InternalDistributedMember();
    mbr._readEssentialData(in);
    return mbr;
  }

  private void _readEssentialData(DataInput in) throws IOException, ClassNotFoundException {
    this.isPartial = true;
    InetAddress inetAddr = DataSerializer.readInetAddress(in);
    int port = in.readInt();

    String hostName =
        SocketCreator.resolve_dns ? SocketCreator.getHostName(inetAddr) : inetAddr.getHostAddress();

    int flags = in.readUnsignedByte();
    boolean sbEnabled = (flags & NPD_ENABLED_BIT) != 0;
    boolean elCoord = (flags & COORD_ENABLED_BIT) != 0;

    int vmKind = in.readUnsignedByte();
    int vmViewId = -1;

    if (vmKind == ClusterDistributionManager.LONER_DM_TYPE) {
      this.uniqueTag = DataSerializer.readString(in);
    } else {
      String str = DataSerializer.readString(in);
      if (str != null) { // backward compatibility from earlier than 6.5
        vmViewId = Integer.parseInt(str);
      }
    }

    String name = DataSerializer.readString(in);

    MemberAttributes attr = new MemberAttributes(-1, -1, vmKind, vmViewId, name, null, null);
    netMbr = NetMemberFactory.newNetMember(inetAddr, hostName, port, sbEnabled, elCoord,
        InternalDataSerializer.getVersionForDataStream(in).ordinal(), attr);

    if (InternalDataSerializer.getVersionForDataStream(in).compareTo(Version.GFE_90) == 0) {
      netMbr.readAdditionalData(in);
    }
  }


  @Override
  public void writeEssentialData(DataOutput out) throws IOException {
    Assert.assertTrue(netMbr.getVmKind() > 0);
    DataSerializer.writeInetAddress(getInetAddress(), out);
    out.writeInt(getPort());

    int flags = 0;
    if (netMbr.isNetworkPartitionDetectionEnabled())
      flags |= NPD_ENABLED_BIT;
    if (netMbr.preferredForCoordinator())
      flags |= COORD_ENABLED_BIT;
    flags |= PARTIAL_ID_BIT;
    out.writeByte((byte) (flags & 0xff));

    // out.writeInt(dcPort);
    byte vmKind = netMbr.getVmKind();
    out.writeByte(vmKind);

    if (vmKind == ClusterDistributionManager.LONER_DM_TYPE) {
      DataSerializer.writeString(this.uniqueTag, out);
    } else { // added in 6.5 for unique identifiers in P2P
      DataSerializer.writeString(String.valueOf(netMbr.getVmViewId()), out);
    }
    // write name last to fix bug 45160
    DataSerializer.writeString(netMbr.getName(), out);

    Version outputVersion = InternalDataSerializer.getVersionForDataStream(out);
    if (0 <= outputVersion.compareTo(Version.GFE_90)
        && outputVersion.compareTo(Version.GEODE_1_1_0) < 0) {
      netMbr.writeAdditionalData(out);
    }
  }

  /**
   * Set the membership port. This is done in loner systems using client/server connection
   * information to help form a unique ID
   */
  public void setPort(int p) {
    assert netMbr.getVmKind() == ClusterDistributionManager.LONER_DM_TYPE;
    this.netMbr.setPort(p);
    cachedToString = null;
  }

  @Override
  public String getHost() {
    return this.netMbr.getInetAddress().getCanonicalHostName();
  }

  @Override
  public int getProcessId() {
    return netMbr.getProcessId();
  }

  @Override
  public String getId() {
    return toString();
  }

  @Override
  public String getUniqueId() {
    StringBuilder sb = new StringBuilder();
    addFixedToString(sb);

    // add version if not current
    short version = netMbr.getVersionOrdinal();
    if (version != Version.CURRENT.ordinal()) {
      sb.append("(version:").append(Version.toString(version)).append(')');
    }

    return sb.toString();
  }

  public void setVersionObjectForTest(Version v) {
    this.versionObj = v;
    netMbr.setVersion(v);
  }

  public Version getVersionObject() {
    return this.versionObj;
  }

  @Override
  public Version[] getSerializationVersions() {
    return dsfidVersions;
  }

  @VisibleForTesting
  void setUniqueTag(String tag) {
    uniqueTag = tag;
  }

  @VisibleForTesting
  public void setIsPartial(boolean value) {
    isPartial = value;
  }

}
