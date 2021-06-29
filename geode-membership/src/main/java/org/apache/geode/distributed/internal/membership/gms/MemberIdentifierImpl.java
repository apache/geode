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

import static org.apache.geode.distributed.internal.membership.gms.GMSMemberData.COORD_ENABLED_BIT;
import static org.apache.geode.distributed.internal.membership.gms.GMSMemberData.NPD_ENABLED_BIT;
import static org.apache.geode.distributed.internal.membership.gms.GMSMemberData.PARTIAL_ID_BIT;
import static org.apache.geode.distributed.internal.membership.gms.GMSMemberData.VERSION_BIT;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.InetAddressValidator;
import org.jetbrains.annotations.NotNull;
import org.jgroups.util.UUID;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.membership.api.MemberData;
import org.apache.geode.distributed.internal.membership.api.MemberDataBuilder;
import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.internal.serialization.Version;
import org.apache.geode.internal.serialization.Versioning;
import org.apache.geode.internal.serialization.VersioningIO;

/**
 * An implementation of {@link MemberIdentifier}
 */
public class MemberIdentifierImpl implements MemberIdentifier, DataSerializableFixedID {
  /** The versions in which this message was modified */
  @Immutable
  private static final KnownVersion[] dsfidVersions =
      new KnownVersion[] {KnownVersion.GFE_90, KnownVersion.GEODE_1_15_0};
  private MemberData memberData; // the underlying member object

  public MemberIdentifierImpl() {}

  public MemberIdentifierImpl(
      MemberData memberData) {
    this.memberData = memberData;
  }

  public void setDurableTimeout(int newValue) {
    memberData.setDurableTimeout(newValue);
  }

  public void setDurableId(String id) {
    memberData.setDurableId(id);
  }

  /**
   * Replace the current member data with the given member data. This can be used to fill out an
   * InternalDistributedMember that was created from a partial data created by
   * readEssentialData.
   *
   * @param memberData the replacement member data
   */
  public void setMemberData(MemberData memberData) {
    this.memberData = memberData;
  }

  /**
   * Return the underlying host address
   *
   * @return the underlying host address
   */
  public InetAddress getInetAddress() {
    return memberData.getInetAddress();
  }

  /**
   * Return the underlying port (membership port)
   *
   * @return the underlying membership port
   */
  public int getMembershipPort() {
    return memberData.getMembershipPort();
  }

  @Override
  public short getVersionOrdinal() {
    return memberData.getVersionOrdinal();
  }

  /**
   * Returns the port on which the direct channel runs
   */
  public int getDirectChannelPort() {
    assert !isPartial();
    return memberData.getDirectChannelPort();
  }

  /**
   * [GemStone] Returns the kind of VM that hosts the distribution manager with this address.
   *
   * @see MemberIdentifier#NORMAL_DM_TYPE
   */
  public int getVmKind() {
    return memberData.getVmKind();
  }

  @Override
  public int getMemberWeight() {
    return memberData.getMemberWeight();
  }

  /**
   * Returns the membership view ID that this member was born in. For backward compatibility reasons
   * this is limited to 16 bits.
   */
  public int getVmViewId() {
    return memberData.getVmViewId();
  }

  @Override
  public boolean preferredForCoordinator() {
    return memberData.isPreferredForCoordinator();
  }

  @Override
  public List<String> getGroups() {
    String[] groups = memberData.getGroups();
    return groups == null ? Collections.emptyList()
        : Collections.unmodifiableList(Arrays.asList(groups));
  }

  @Override
  public void setVmViewId(int p) {
    memberData.setVmViewId(p);
    cachedToString = null;
  }

  @Override
  public void setPreferredForCoordinator(boolean preferred) {
    memberData.setPreferredForCoordinator(preferred);
    cachedToString = null;
  }

  @Override
  public void setDirectChannelPort(int dcPort) {
    memberData.setDirectChannelPort(dcPort);
    cachedToString = null;
  }

  @Override
  public void setVmKind(int dmType) {
    memberData.setVmKind(dmType);
    cachedToString = null;
  }

  public void setGroups(String[] newGroups) {
    memberData.setGroups(newGroups);
    cachedToString = null;
  }

  /**
   * Returns the name of this member's distributed system connection or null if no name was
   * specified.
   */
  public String getName() {
    String result = memberData.getName();
    if (result == null) {
      result = "";
    }
    return result;
  }

  @Override
  public void setName(String name) {
    memberData.setName(name);
  }

  @Override
  public String getUniqueTag() {
    return memberData.getUniqueTag();
  }

  @Override
  public String getDurableId() {
    return memberData.getDurableId();
  }

  @Override
  public int getDurableTimeout() {
    return memberData.getDurableTimeout();
  }

  @Override
  public void setHostName(String hostName) {
    memberData.setHostName(hostName);

  }

  @Override
  public void setProcessId(int id) {
    memberData.setProcessId(id);

  }

  @Override
  public boolean hasUUID() {
    return memberData.hasUUID();
  }

  public int compare(MemberIdentifier other) {
    return compareTo(other, false, true);
  }

  @Override
  public int compareTo(final @NotNull MemberIdentifier other, final boolean compareMemberData,
      final boolean compareViewIds) {
    int c = Integer.compare(getMembershipPort(), other.getMembershipPort());
    if (c != 0) {
      return c;
    }

    final InetAddress myAddr = getInetAddress();
    final InetAddress otherAddr = other.getInetAddress();

    // Discard null cases
    if (myAddr == null && otherAddr == null) {
      return 0;
    } else if (myAddr == null) {
      return -1;
    } else if (otherAddr == null) {
      return 1;
    }

    final byte[] myBytes = myAddr.getAddress();
    final byte[] otherBytes = otherAddr.getAddress();

    if (myBytes != otherBytes) {
      for (int i = 0; i < myBytes.length; i++) {
        if (i >= otherBytes.length) {
          return -1; // same as far as they go, but shorter...
        }
        if (myBytes[i] < otherBytes[i]) {
          return -1;
        }
        if (myBytes[i] > otherBytes[i]) {
          return 1;
        }
      }
      if (myBytes.length > otherBytes.length) {
        return 1; // same as far as they go, but longer...
      }
    }

    if (!(other.isPartial() || isPartial())) {
      c = StringUtils.compare(getName(), other.getName());
      if (c != 0) {
        return c;
      }
    }

    final String uniqueTag = getUniqueTag();
    final String otherUniqueTag = other.getUniqueTag();
    if (uniqueTag == null && otherUniqueTag == null) {
      if (compareViewIds) {
        // not loners, so look at P2P view ID
        final int thisViewId = getVmViewId();
        final int otherViewId = other.getVmViewId();
        if (thisViewId >= 0 && otherViewId >= 0) {
          c = Integer.compare(thisViewId, otherViewId);
          if (c != 0) {
            return c;
          }
        }
      }
    } else {
      c = StringUtils.compare(uniqueTag, otherUniqueTag);
      if (c != 0) {
        return c;
      }
    }

    if (compareMemberData && memberData != null && other.getMemberData() != null) {
      return memberData.compareAdditionalData(other.getMemberData());
    } else {
      return 0;
    }
  }

  /**
   * An InternalDistributedMember created for a test or via readEssentialData will be a Partial ID,
   * possibly not having ancillary info like "name".
   *
   * @return true if this is a partial ID
   */
  public boolean isPartial() {
    return memberData.isPartial();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof MemberIdentifierImpl)) {
      return false;
    }
    MemberIdentifierImpl other = (MemberIdentifierImpl) obj;

    int myPort = getMembershipPort();
    int otherPort = other.getMembershipPort();
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

    if (getUniqueTag() == null && other.getUniqueTag() == null) {
      // not loners, so look at P2P view ID
      int thisViewId = getVmViewId();
      int otherViewId = other.getVmViewId();
      if (thisViewId >= 0 && otherViewId >= 0) {
        if (thisViewId != otherViewId) {
          return false;
        } // else they're the same, so continue
      }
    } else if (!Objects.equals(getUniqueTag(), other.getUniqueTag())) {
      return false;
    }

    if (memberData != null && other.memberData != null) {
      return 0 == memberData.compareAdditionalData(other.memberData);
    }

    // purposely avoid checking roles
    // @todo Add durableClientAttributes to equals

    return true;
  }

  @Override
  public int hashCode() {
    int result = 0;
    result = result + memberData.getInetAddress().hashCode();
    result = result + getMembershipPort();
    return result;
  }

  private String shortName(String hostname) {
    if (hostname == null) {
      return "<null inet_addr hostname>";
    }
    int index = hostname.indexOf('.');

    if (index > 0 && !Character.isDigit(hostname.charAt(0))) {
      return hostname.substring(0, index);
    } else {
      return hostname;
    }
  }


  /** the cached string description of this object */
  private transient String cachedToString;

  @Override
  public String toString() {
    String result = cachedToString;
    if (result == null) {
      final StringBuilder sb = new StringBuilder();
      addFixedToString(sb, false);

      // add version if not current
      short version = memberData.getVersionOrdinal();
      if (version != KnownVersion.CURRENT.ordinal()) {
        sb.append("(version:").append(Versioning.getVersion(version)).append(')');
      }

      // leave out Roles on purpose

      result = sb.toString();
      cachedToString = result;
    }
    return result;
  }

  public void addFixedToString(StringBuilder sb, boolean useIpAddress) {
    // Note: This method is used to generate the HARegion name. If it is changed, memory and GII
    // issues will occur in the case of clients with subscriptions during rolling upgrade.
    String host;

    InetAddress inetAddress = getInetAddress();
    if ((inetAddress != null) && (inetAddress.isMulticastAddress() || useIpAddress)) {
      host = inetAddress.getHostAddress();
    } else {
      String hostName = memberData.getHostName();
      InetAddressValidator inetAddressValidator = InetAddressValidator.getInstance();
      boolean isIpAddress = inetAddressValidator.isValid(hostName);
      host = isIpAddress ? hostName : shortName(hostName);
    }

    sb.append(host);

    String myName = getName();
    int vmPid = memberData.getProcessId();
    int vmKind = memberData.getVmKind();
    if (vmPid > 0 || vmKind != MemberIdentifier.NORMAL_DM_TYPE || !"".equals(myName)) {
      sb.append("(");

      if (!"".equals(myName)) {
        sb.append(myName);
        if (vmPid > 0) {
          sb.append(':');
        }
      }

      if (vmPid > 0) {
        sb.append(vmPid);
      }

      String vmStr = "";
      switch (vmKind) {
        case MemberIdentifier.NORMAL_DM_TYPE:
          // vmStr = ":local"; // let this be silent
          break;
        case MemberIdentifier.LOCATOR_DM_TYPE:
          vmStr = ":locator";
          break;
        case MemberIdentifier.ADMIN_ONLY_DM_TYPE:
          vmStr = ":admin";
          break;
        case MemberIdentifier.LONER_DM_TYPE:
          vmStr = ":loner";
          break;
        default:
          vmStr = ":<unknown:" + vmKind + ">";
          break;
      }
      sb.append(vmStr);
      sb.append(")");
    }
    if (vmKind != MemberIdentifier.LONER_DM_TYPE
        && memberData.isPreferredForCoordinator()) {
      sb.append("<ec>");
    }
    int vmViewId = getVmViewId();
    if (vmViewId >= 0) {
      sb.append("<v").append(vmViewId).append(">");
    }
    sb.append(":");
    sb.append(getMembershipPort());

    if (vmKind == MemberIdentifier.LONER_DM_TYPE) {
      // add some more info that was added in 4.2.1 for loner bridge clients
      // impact on non-bridge loners is ok
      if (getUniqueTag() != null && getUniqueTag().length() != 0) {
        sb.append(":").append(getUniqueTag());
      }
      String name = getName();
      if (name.length() != 0) {
        sb.append(":").append(name);
      }
    }
  }

  private short readVersion(int flags, DataInput in) throws IOException {
    if ((flags & VERSION_BIT) != 0) {
      return VersioningIO.readOrdinal(in);
    } else {
      // prior to 7.1 member IDs did not serialize their version information
      KnownVersion v = StaticSerialization.getVersionForDataStreamOrNull(in);
      if (v != null) {
        return v.ordinal();
      }
      return KnownVersion.CURRENT_ORDINAL;
    }
  }

  /**
   * For Externalizable
   *
   * @see Externalizable
   */
  public void writeExternal(ObjectOutput out) throws IOException {
    assert memberData.getVmKind() > 0;

    // do it the way we like
    byte[] address = getInetAddress().getAddress();

    out.writeInt(address.length); // IPv6 compatible
    out.write(address);
    out.writeInt(getMembershipPort());

    StaticSerialization.writeString(memberData.getHostName(), out);

    int flags = 0;
    if (memberData.isNetworkPartitionDetectionEnabled()) {
      flags |= NPD_ENABLED_BIT;
    }
    if (memberData.isPreferredForCoordinator()) {
      flags |= COORD_ENABLED_BIT;
    }
    if (isPartial()) {
      flags |= PARTIAL_ID_BIT;
    }
    // always write product version but enable reading from older versions
    // that do not have it
    flags |= VERSION_BIT;
    out.writeByte((byte) (flags & 0xff));

    out.writeInt(memberData.getDirectChannelPort());
    out.writeInt(memberData.getProcessId());
    out.writeInt(memberData.getVmKind());
    out.writeInt(memberData.getVmViewId());
    StaticSerialization.writeStringArray(memberData.getGroups(), out);

    StaticSerialization.writeString(memberData.getName(), out);
    StaticSerialization.writeString(memberData.getUniqueTag(), out);
    String durableId = memberData.getDurableId();
    StaticSerialization.writeString(durableId == null ? "" : durableId, out);
    StaticSerialization.writeInteger(durableId == null ? 300 : memberData.getDurableTimeout(), out);
    VersioningIO.writeOrdinal(out, memberData.getVersionOrdinal(), true);
    memberData.writeAdditionalData(out);
  }

  /**
   * For Externalizable
   *
   * @see Externalizable
   */
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    int len = in.readInt(); // IPv6 compatible
    byte[] addr = new byte[len];
    in.readFully(addr);
    InetAddress inetAddr = InetAddress.getByAddress(addr);
    int port = in.readInt();

    String hostName = StaticSerialization.readString(in);

    int flags = in.readUnsignedByte();
    boolean sbEnabled = (flags & NPD_ENABLED_BIT) != 0;
    boolean elCoord = (flags & COORD_ENABLED_BIT) != 0;
    boolean isPartial = (flags & PARTIAL_ID_BIT) != 0;

    int dcPort = in.readInt();
    int vmPid = in.readInt();
    int vmKind = in.readInt();
    int vmViewId = in.readInt();
    String[] groups = StaticSerialization.readStringArray(in);

    String name = StaticSerialization.readString(in);
    String uniqueTag = StaticSerialization.readString(in);
    String durableId = StaticSerialization.readString(in);
    int durableTimeout = in.readInt();

    short version = readVersion(flags, in);

    memberData = MemberDataBuilder.newBuilder(inetAddr, hostName)
        .setMembershipPort(port)
        .setDirectChannelPort(dcPort)
        .setName(name)
        .setNetworkPartitionDetectionEnabled(sbEnabled)
        .setPreferredForCoordinator(elCoord)
        .setVersionOrdinal(version)
        .setVmPid(vmPid)
        .setVmKind(vmKind)
        .setVmViewId(vmViewId)
        .setGroups(groups)
        .setDurableId(durableId)
        .setDurableTimeout(durableTimeout)
        .setIsPartial(isPartial)
        .setUniqueTag(uniqueTag)
        .build();
    if (version >= KnownVersion.GFE_90.ordinal()) {
      try {
        memberData.readAdditionalData(in);
      } catch (java.io.EOFException e) {
        // old version
      }
    }
    assert memberData.getVmKind() > 0;
  }

  @Override
  public int getDSFID() {
    return MEMBER_IDENTIFIER;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    toDataPre_GFE_9_0_0_0(out, context);
    if (memberData.getVersionOrdinal() >= KnownVersion.GFE_90.ordinal()) {
      memberData.writeAdditionalData(out);
    }
  }

  /**
   * This method doesn't convert memberData's hostname back to an IP address
   * (see {@link MemberIdentifier#fromDataPre_GEODE_1_15_0_0}). Pre Geode 1.15
   * member may store an IP address instead of its hostname to achieve better performance
   * in case of a network partition which is not a significant concern here because these
   * members will be upgraded.
   */
  public void toDataPre_GEODE_1_15_0_0(DataOutput out, SerializationContext context)
      throws IOException {
    toDataPre_GFE_9_0_0_0(out, context);
    memberData.writeAdditionalData(out);
  }

  public void toDataPre_GFE_9_0_0_0(DataOutput out, SerializationContext context)
      throws IOException {
    StaticSerialization.writeInetAddress(getInetAddress(), out);
    out.writeInt(getMembershipPort());

    StaticSerialization.writeString(memberData.getHostName(), out);

    int flags = 0;
    if (memberData.isNetworkPartitionDetectionEnabled()) {
      flags |= NPD_ENABLED_BIT;
    }
    if (memberData.isPreferredForCoordinator()) {
      flags |= COORD_ENABLED_BIT;
    }
    if (isPartial()) {
      flags |= PARTIAL_ID_BIT;
    }
    // always write product version but enable reading from older versions
    // that do not have it
    flags |= VERSION_BIT;

    out.writeByte((byte) (flags & 0xff));

    out.writeInt(memberData.getDirectChannelPort());
    out.writeInt(memberData.getProcessId());
    int vmKind = memberData.getVmKind();
    out.writeByte(vmKind);
    StaticSerialization.writeStringArray(memberData.getGroups(), out);

    StaticSerialization.writeString(memberData.getName(), out);
    if (vmKind == MemberIdentifier.LONER_DM_TYPE) {
      StaticSerialization.writeString(memberData.getUniqueTag(), out);
    } else { // added in 6.5 for unique identifiers in P2P
      StaticSerialization.writeString(String.valueOf(memberData.getVmViewId()), out);
    }
    String durableId = memberData.getDurableId();
    StaticSerialization.writeString(durableId == null ? "" : durableId, out);
    StaticSerialization.writeInteger(
        durableId == null ? 300 : memberData.getDurableTimeout(),
        out);

    short version = memberData.getVersionOrdinal();
    VersioningIO.writeOrdinal(out, version, true);
  }

  @Override
  public void fromData(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    readMemberData(in);
    // just in case this is just a non-versioned read
    // from a file we ought to check the version
    if (memberData.getVersionOrdinal() >= KnownVersion.GFE_90.ordinal()) {
      try {
        memberData.readAdditionalData(in);
      } catch (EOFException e) {
        // nope - it's from a pre-GEODE client or WAN site
      }
    }
    convertIpToHostnameIfNeeded();
  }

  public void fromDataPre_GEODE_1_15_0_0(DataInput in, DeserializationContext context)
      throws IOException, ClassNotFoundException {
    readMemberData(in);
    memberData.readAdditionalData(in);
    convertIpToHostnameIfNeeded();
  }

  public void fromDataPre_GFE_9_0_0_0(DataInput in, DeserializationContext context)
      throws IOException {
    readMemberData(in);
    convertIpToHostnameIfNeeded();
  }

  /**
   * In older versions of Geode {@link MemberData#getHostName()} can return an IP address
   * if network partition is enabled. Also, older versions of Geode don't use bind-address for
   * SNI endpoint identification and do a reverse lookup to find the fully qualified hostname.
   * This can become a problem if TLS certificate doesn't have the fully qualified name in it
   * as a Subject Alternate Name, therefore this behavior was changed to preserve the bind-address.
   *
   * During a rolling upgrade, we need to ensure that {@link MemberData#getHostName()} returns
   * a hostname because it might be later used for SNI endpoint identification.
   */
  private void convertIpToHostnameIfNeeded() {
    if (memberData.isNetworkPartitionDetectionEnabled()) {
      final InetAddressValidator inetAddressValidator = InetAddressValidator.getInstance();
      final boolean isIpAddress = inetAddressValidator.isValid(memberData.getHostName());
      if (isIpAddress) {
        memberData.setHostName(getHost());
      }
    }
  }

  private void readMemberData(DataInput in) throws IOException {
    InetAddress inetAddr = StaticSerialization.readInetAddress(in);
    int port = in.readInt();

    String hostName = StaticSerialization.readString(in);

    int flags = in.readUnsignedByte();
    boolean sbEnabled = (flags & NPD_ENABLED_BIT) != 0;
    boolean elCoord = (flags & COORD_ENABLED_BIT) != 0;
    boolean isPartial = (flags & PARTIAL_ID_BIT) != 0;

    int dcPort = in.readInt();
    int vmPid = in.readInt();
    int vmKind = in.readUnsignedByte();
    String[] groups = StaticSerialization.readStringArray(in);
    int vmViewId = -1;

    String name = StaticSerialization.readString(in);
    String uniqueTag = null;
    if (vmKind == MemberIdentifier.LONER_DM_TYPE) {
      uniqueTag = StaticSerialization.readString(in);
    } else {
      String str = StaticSerialization.readString(in);
      if (str != null) { // backward compatibility from earlier than 6.5
        vmViewId = Integer.parseInt(str);
      }
    }

    String durableId = StaticSerialization.readString(in);
    int durableTimeout = in.readInt();

    short version = readVersion(flags, in);

    memberData = MemberDataBuilder.newBuilder(inetAddr, hostName)
        .setMembershipPort(port)
        .setDirectChannelPort(dcPort)
        .setName(name)
        .setNetworkPartitionDetectionEnabled(sbEnabled)
        .setPreferredForCoordinator(elCoord)
        .setVersionOrdinal(version)
        .setVmPid(vmPid)
        .setVmKind(vmKind)
        .setVmViewId(vmViewId)
        .setGroups(groups)
        .setDurableId(durableId)
        .setDurableTimeout(durableTimeout)
        .setIsPartial(isPartial)
        .setUniqueTag(uniqueTag)
        .build();

    assert memberData.getVmKind() > 0;
  }

  public void _readEssentialData(DataInput in, Function<InetAddress, String> hostnameResolver)
      throws IOException, ClassNotFoundException {
    InetAddress inetAddr = StaticSerialization.readInetAddress(in);
    int port = in.readInt();

    String hostName = hostnameResolver.apply(inetAddr);

    int flags = in.readUnsignedByte();
    boolean sbEnabled = (flags & NPD_ENABLED_BIT) != 0;
    boolean elCoord = (flags & COORD_ENABLED_BIT) != 0;

    int vmKind = in.readUnsignedByte();
    int vmViewId = -1;

    String uniqueTag = null;
    if (vmKind == MemberIdentifier.LONER_DM_TYPE) {
      uniqueTag = StaticSerialization.readString(in);
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
        .setIsPartial(true)
        .setUniqueTag(uniqueTag)
        .build();

    if (StaticSerialization.getVersionForDataStream(in) == KnownVersion.GFE_90) {
      memberData.readAdditionalData(in);
    }
  }

  public void writeEssentialData(DataOutput out) throws IOException {
    assert memberData.getVmKind() > 0;
    StaticSerialization.writeInetAddress(getInetAddress(), out);
    out.writeInt(getMembershipPort());

    int flags = 0;
    if (memberData.isNetworkPartitionDetectionEnabled()) {
      flags |= NPD_ENABLED_BIT;
    }
    if (memberData.isPreferredForCoordinator()) {
      flags |= COORD_ENABLED_BIT;
    }
    flags |= PARTIAL_ID_BIT;
    out.writeByte((byte) (flags & 0xff));

    // out.writeInt(dcPort);
    byte vmKind = memberData.getVmKind();
    out.writeByte(vmKind);

    if (vmKind == MemberIdentifier.LONER_DM_TYPE) {
      StaticSerialization.writeString(memberData.getUniqueTag(), out);
    } else { // added in 6.5 for unique identifiers in P2P
      StaticSerialization.writeString(String.valueOf(memberData.getVmViewId()), out);
    }
    // write name last to fix bug 45160
    StaticSerialization.writeString(memberData.getName(), out);

    KnownVersion outputVersion = StaticSerialization.getVersionForDataStream(out);
    if (outputVersion.isOlderThan(KnownVersion.GEODE_1_1_0)
        && outputVersion.isNotOlderThan(KnownVersion.GFE_90)) {
      memberData.writeAdditionalData(out);
    }
  }



  /**
   * Set the membership port. This is done in loner systems using client/server connection
   * information to help form a unique ID
   */
  @Override
  public void setPort(int p) {
    assert memberData.getVmKind() == MemberIdentifier.LONER_DM_TYPE;
    memberData.setPort(p);
    cachedToString = null;
  }

  @Override
  public MemberData getMemberData() {
    return memberData;
  }

  @Override
  public String getHostName() {
    return memberData.getHostName();
  }

  @Override
  public String getHost() {
    return memberData.getInetAddress().getCanonicalHostName();
  }

  @Override
  public int getProcessId() {
    return memberData.getProcessId();
  }

  @Override
  public String getId() {
    return toString();
  }

  @Override
  public String getUniqueId() {
    StringBuilder sb = new StringBuilder();
    addFixedToString(sb, false);

    // add version if not current
    short version = memberData.getVersionOrdinal();
    if (version != KnownVersion.CURRENT.ordinal()) {
      sb.append("(version:").append(Versioning.getVersion(version)).append(')');
    }

    return sb.toString();
  }

  @Override
  public void setVersionForTest(Version v) {
    memberData.setVersion(v);
    cachedToString = null;
  }

  @Override
  public Version getVersion() {
    return memberData.getVersion();
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return dsfidVersions;
  }

  @VisibleForTesting
  @Override
  public void setUniqueTag(String tag) {
    memberData.setUniqueTag(tag);
  }

  @Override
  public void setIsPartial(boolean value) {
    memberData.setIsPartial(value);
  }

  @Override
  public long getUuidLeastSignificantBits() {
    return memberData.getUuidLeastSignificantBits();
  }

  @Override
  public long getUuidMostSignificantBits() {
    return memberData.getUuidMostSignificantBits();
  }

  @Override
  public boolean isNetworkPartitionDetectionEnabled() {
    return memberData.isNetworkPartitionDetectionEnabled();
  }

  @Override
  public void setUUID(UUID uuid) {
    memberData.setUUID(uuid);

  }

  @Override
  public void setMemberWeight(byte b) {
    memberData.setMemberWeight(b);
  }

  @Override
  public void setUdpPort(int port) {
    memberData.setUdpPort(port);

  }

  @Override
  public UUID getUUID() {
    return memberData.getUUID();
  }
}
