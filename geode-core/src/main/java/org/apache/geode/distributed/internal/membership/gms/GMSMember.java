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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;

import org.jgroups.util.UUID;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.internal.serialization.Version;

/**
 * GMSMember is the membership identifier class for Group Membership Services.
 */
public class GMSMember implements DataSerializableFixedID {
  /** The type for regular members */
  public static final int NORMAL_DM_TYPE = 10;

  /** The DM type for locator members */
  public static final int LOCATOR_DM_TYPE = 11;

  /** The DM type for deprecated admin-only members */
  public static final int ADMIN_ONLY_DM_TYPE = 12;

  /** The DM type for stand-alone members (usually clients) */
  public static final int LONER_DM_TYPE = 13;

  private String hostName;

  private int udpPort = 0;
  private boolean preferredForCoordinator;
  private boolean networkPartitionDetectionEnabled;
  private byte memberWeight;
  private InetAddress inetAddr;
  private int processId;
  private byte vmKind;
  private int vmViewId = -1;
  private int directPort;
  private String name;
  private String[] groups;
  private short versionOrdinal = Version.getCurrentVersion().ordinal();
  private long uuidLSBs;
  private long uuidMSBs;
  private String durableId;
  private int durableTimeout;

  private boolean isPartial; // transient state - created with readEssentialData

  public boolean isPartial() {
    return isPartial;
  }

  // Used only by Externalization
  public GMSMember() {}

  @VisibleForTesting
  public GMSMember(String localhost, int udpPort, Version version) {
    this.hostName = localhost;
    this.inetAddr = SocketCreator.toInetAddress(localhost);
    this.udpPort = udpPort;
    this.versionOrdinal = version.ordinal();
    this.vmKind = NORMAL_DM_TYPE;
    this.preferredForCoordinator = true;
    this.vmViewId = -1;
    this.processId = -1;
    this.directPort = -1;
    setUUID(UUID.randomUUID());
  }


  /**
   * Create a CacheMember referring to the current host (as defined by the given string).
   *
   * @param i the hostname, must be for the current host
   * @param p the membership listening port
   */
  @VisibleForTesting
  public GMSMember(String i, int p) {
    this(i, p, Version.getCurrentVersion());
  }

  /**
   * Create a CacheMember referring to the current host (as defined by the given string).
   *
   * @param i the hostname, must be for the current host
   * @param p the membership listening port
   * @param networkPartitionDetectionEnabled whether the member has network partition detection
   *        enabled
   * @param preferredForCoordinator whether the member can be group coordinator
   * @param version the member's version ordinal
   * @param msbs - most significant bytes of UUID
   * @param lsbs - least significant bytes of UUID
   */
  public GMSMember(InetAddress i, String hostName, int p, int processId, byte vmKind,
      int directPort, int vmViewId,
      String name, String[] groups,
      String durableId, int durableTimeout,
      boolean networkPartitionDetectionEnabled, boolean preferredForCoordinator, short version,
      long msbs, long lsbs) {
    this.inetAddr = i;
    this.hostName = hostName;
    this.udpPort = p;
    this.processId = processId;
    this.vmKind = vmKind;
    this.directPort = directPort;
    this.vmViewId = vmViewId;
    this.name = name;
    this.groups = groups;
    this.durableId = durableId;
    this.durableTimeout = durableTimeout;
    this.networkPartitionDetectionEnabled = networkPartitionDetectionEnabled;
    this.preferredForCoordinator = preferredForCoordinator;
    this.versionOrdinal = version;
    this.uuidMSBs = msbs;
    this.uuidLSBs = lsbs;
  }

  public GMSMember(InetAddress i, int p, short version, long msbs, long lsbs, int viewId) {
    this.inetAddr = i;
    this.hostName = i.getHostName();
    this.udpPort = p;
    this.versionOrdinal = version;
    this.uuidMSBs = msbs;
    this.uuidLSBs = lsbs;
    this.vmViewId = viewId;
  }


  /**
   * Clone a GMSMember
   *
   * @param other the member to create a copy of
   */
  public GMSMember(GMSMember other) {
    this.hostName = other.hostName;
    this.udpPort = other.udpPort;
    this.preferredForCoordinator = other.preferredForCoordinator;
    this.networkPartitionDetectionEnabled = other.networkPartitionDetectionEnabled;
    this.memberWeight = other.memberWeight;
    this.inetAddr = other.inetAddr;
    this.processId = other.processId;
    this.vmKind = other.vmKind;
    this.vmViewId = other.vmViewId;
    this.directPort = other.directPort;
    this.name = other.name;
    this.durableId = other.durableId;
    this.durableTimeout = other.durableTimeout;
    this.groups = other.groups;
    this.versionOrdinal = other.versionOrdinal;
    this.uuidLSBs = other.uuidLSBs;
    this.uuidMSBs = other.uuidMSBs;
  }


  public int getPort() {
    return this.udpPort;
  }


  public boolean preferredForCoordinator() {
    return this.preferredForCoordinator;
  }


  public void setPreferredForCoordinator(boolean preferred) {
    this.preferredForCoordinator = preferred;
  }


  public String getDurableId() {
    return durableId;
  }

  public int getDurableTimeout() {
    return durableTimeout;
  }

  public InetAddress getInetAddress() {
    return this.inetAddr;
  }


  public short getVersionOrdinal() {
    return this.versionOrdinal;
  }

  public void setVersionOrdinal(short versionOrdinal) {
    this.versionOrdinal = versionOrdinal;
  }

  public void setUUID(UUID u) {
    this.uuidLSBs = u.getLeastSignificantBits();
    this.uuidMSBs = u.getMostSignificantBits();
  }

  /**
   * return the jgroups logical address for this member, if it's been established
   */
  public UUID getUUID() {
    if (this.uuidLSBs == 0 && this.uuidMSBs == 0) {
      return null;
    }
    return new UUID(this.uuidMSBs, this.uuidLSBs);
  }

  public long getUuidMSBs() {
    return this.uuidMSBs;
  }

  public long getUuidLSBs() {
    return this.uuidLSBs;
  }

  /*
   * implements the java.lang.Comparable interface
   *
   * @see java.lang.Comparable
   *
   * @param o - the Object to be compared
   *
   * @return a negative integer, zero, or a positive integer as this object is less than, equal to,
   * or greater than the specified object.
   *
   * @exception java.lang.ClassCastException - if the specified object's type prevents it from being
   * compared to this Object.
   */

  public int compareTo(GMSMember o) {
    return compareTo(o, true);
  }

  public int compareTo(GMSMember o, boolean compareUUIDs) {
    if (o == this) {
      return 0;
    }
    // obligatory type check
    if (o == null) {
      throw new ClassCastException(
          "GMSMember.compareTo(): comparison between different classes");
    }
    byte[] myAddr = inetAddr.getAddress();
    GMSMember his = o;
    byte[] hisAddr = his.inetAddr.getAddress();
    if (myAddr != hisAddr) {
      for (int idx = 0; idx < myAddr.length; idx++) {
        if (idx >= hisAddr.length) {
          return 1;
        } else if (myAddr[idx] > hisAddr[idx]) {
          return 1;
        } else if (myAddr[idx] < hisAddr[idx]) {
          return -1;
        }
      }
      // After checking both addresses we have only gone up to myAddr.length, their address could be
      // longer
      if (hisAddr.length > myAddr.length) {
        return -1;
      }
    }
    if (udpPort < his.udpPort)
      return -1;
    if (his.udpPort < udpPort)
      return 1;
    int result = 0;

    // bug #41983, address of kill-9'd member is reused
    // before it can be ejected from membership
    if (this.vmViewId >= 0 && his.vmViewId >= 0) {
      if (this.vmViewId < his.vmViewId) {
        result = -1;
      } else if (his.vmViewId < this.vmViewId) {
        result = 1;
      }
    }
    if (compareUUIDs && result == 0 && this.uuidMSBs != 0 && his.uuidMSBs != 0) {
      if (this.uuidMSBs < his.uuidMSBs) {
        result = -1;
      } else if (his.uuidMSBs < this.uuidMSBs) {
        result = 1;
      } else if (this.uuidLSBs < his.uuidLSBs) {
        result = -1;
      } else if (his.uuidLSBs < this.uuidLSBs) {
        result = 1;
      }
    }
    return result;
  }


  public int compareAdditionalData(GMSMember his) {
    int result = 0;
    if (this.uuidMSBs != 0 && his.uuidMSBs != 0) {
      if (this.uuidMSBs < his.uuidMSBs) {
        result = -1;
      } else if (his.uuidMSBs < this.uuidMSBs) {
        result = 1;
      } else if (this.uuidLSBs < his.uuidLSBs) {
        result = -1;
      } else if (his.uuidLSBs < this.uuidLSBs) {
        result = 1;
      }
    }
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    // GemStone fix for 29125
    if ((obj == null) || !(obj instanceof GMSMember)) {
      return false;
    }
    return compareTo((GMSMember) obj) == 0;
  }

  @Override
  public int hashCode() {
    if (this.inetAddr == null) {
      return this.udpPort;
    }
    return this.udpPort + inetAddr.hashCode();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(100);

    sb.append("GMSMember[");
    if (name != null && name.length() > 0) {
      sb.append("name=").append(name);
    }
    sb.append(";addr=").append(inetAddr).append(";port=").append(udpPort)
        .append(";kind=").append(vmKind).append(";processId=").append(processId)
        .append(";viewId=").append(vmViewId);
    if (versionOrdinal != Version.CURRENT_ORDINAL) {
      sb.append(";version=").append(versionOrdinal);
    }
    sb.append("]");
    return sb.toString();
  }


  public boolean isNetworkPartitionDetectionEnabled() {
    return networkPartitionDetectionEnabled;
  }


  public byte getMemberWeight() {
    return memberWeight;
  }

  public InetAddress getInetAddr() {
    return inetAddr;
  }


  public int getProcessId() {
    return processId;
  }


  public byte getVmKind() {
    return vmKind;
  }


  public int getVmViewId() {
    return vmViewId;
  }


  public void setVmViewId(int id) {
    this.vmViewId = id;
  }


  public int getDirectPort() {
    return directPort;
  }


  public String getName() {
    return name;
  }


  public String[] getRoles() {
    return groups;
  }

  public void setUdpPort(int udpPort) {
    this.udpPort = udpPort;
  }


  public void setNetworkPartitionDetectionEnabled(boolean networkPartitionDetectionEnabled) {
    this.networkPartitionDetectionEnabled = networkPartitionDetectionEnabled;
  }

  public void setMemberWeight(byte memberWeight) {
    this.memberWeight = memberWeight;
  }

  public void setInetAddr(InetAddress inetAddr) {
    this.inetAddr = inetAddr;
  }


  public void setProcessId(int processId) {
    this.processId = processId;
  }


  public void setVmKind(int vmKind) {
    this.vmKind = (byte) vmKind;
  }


  public void setVersion(Version v) {
    this.versionOrdinal = v.ordinal();
  }

  public void setBirthViewId(int birthViewId) {
    this.vmViewId = birthViewId;
  }


  public void setDirectPort(int directPort) {
    this.directPort = directPort;
  }


  public void setName(String name) {
    this.name = name;
  }


  public String[] getGroups() {
    return groups;
  }


  public void setGroups(String[] groups) {
    this.groups = groups;
  }


  public void setPort(int p) {
    this.udpPort = p;
  }

  /**
   * checks to see if this address has UUID information needed to send messages via JGroups
   */
  public boolean hasUUID() {
    return !(this.uuidLSBs == 0 && this.uuidMSBs == 0);
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return GMSMEMBER;
  }

  static final int NPD_ENABLED_BIT = 0x01;
  static final int PREFERRED_FOR_COORD_BIT = 0x02;
  static final int VERSION_BIT = 0x8;

  static final int LONER_VM_TYPE = 13; // from ClusterDistributionManager

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    StaticSerialization.writeInetAddress(getInetAddress(), out);
    out.writeInt(getPort());

    StaticSerialization.writeString(hostName, out);

    int flags = 0;
    if (isNetworkPartitionDetectionEnabled())
      flags |= NPD_ENABLED_BIT;
    if (preferredForCoordinator())
      flags |= PREFERRED_FOR_COORD_BIT;
    // always write product version but enable reading from older versions
    // that do not have it
    flags |= VERSION_BIT;

    out.writeByte((byte) (flags & 0xff));

    out.writeInt(getDirectPort());
    out.writeInt(getProcessId());
    int vmKind = getVmKind();
    out.writeByte(vmKind);
    StaticSerialization.writeStringArray(getGroups(), out);

    StaticSerialization.writeString(getName(), out);
    if (vmKind == LONER_VM_TYPE) {
      StaticSerialization.writeString("", out);
    } else { // added in 6.5 for unique identifiers in P2P
      StaticSerialization.writeString(String.valueOf(getVmViewId()), out);
    }
    StaticSerialization
        .writeString(durableId == null ? "" : durableId, out);
    out.writeInt(durableId == null ? 300 : durableTimeout);

    Version.writeOrdinal(out, versionOrdinal, true);

    if (versionOrdinal >= Version.GFE_90.ordinal()) {
      writeAdditionalData(out);
    }
  }

  public void writeEssentialData(DataOutput out,
      SerializationContext context) throws IOException {
    Version.writeOrdinal(out, this.versionOrdinal, true);

    int flags = 0;
    if (networkPartitionDetectionEnabled)
      flags |= NPD_ENABLED_BIT;
    if (preferredForCoordinator)
      flags |= PREFERRED_FOR_COORD_BIT;
    out.writeShort(flags);

    StaticSerialization.writeInetAddress(inetAddr, out);
    out.writeInt(udpPort);
    out.writeInt(vmViewId);
    out.writeLong(uuidMSBs);
    out.writeLong(uuidLSBs);
    if (context.getSerializationVersion().ordinal() >= Version.GEODE_1_2_0.ordinal()) {
      out.writeByte(vmKind);
    }
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    inetAddr = StaticSerialization.readInetAddress(in);
    udpPort = in.readInt();

    this.hostName = StaticSerialization.readString(in);

    int flags = in.readUnsignedByte();
    preferredForCoordinator = (flags & PREFERRED_FOR_COORD_BIT) != 0;
    this.networkPartitionDetectionEnabled = (flags & NPD_ENABLED_BIT) != 0;

    directPort = in.readInt();
    processId = in.readInt();
    vmKind = (byte) in.readUnsignedByte();
    groups = StaticSerialization.readStringArray(in);
    vmViewId = -1;

    name = StaticSerialization.readString(in);
    if (vmKind == LONER_DM_TYPE) {
      StaticSerialization.readString(in);
    } else {
      String str = StaticSerialization.readString(in);
      if (str != null) { // backward compatibility from earlier than 6.5
        vmViewId = Integer.parseInt(str);
      }
    }

    durableId = StaticSerialization.readString(in);
    durableTimeout = in.readInt();

    versionOrdinal = readVersion(flags, in, context);

    if (versionOrdinal >= Version.GFE_90.ordinal()) {
      readAdditionalData(in);
    }
  }

  private short readVersion(int flags, DataInput in,
      DeserializationContext context) throws IOException {
    if ((flags & VERSION_BIT) != 0) {
      return Version.readOrdinal(in);
    } else {
      // prior to 7.1 member IDs did not serialize their version information
      Version v = context.getSerializationVersion();
      return v.ordinal();
    }
  }

  public String getHostName() {
    return hostName;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public void readEssentialData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    this.versionOrdinal = Version.readOrdinal(in);

    int flags = in.readShort();
    this.networkPartitionDetectionEnabled = (flags & NPD_ENABLED_BIT) != 0;
    this.preferredForCoordinator = (flags & PREFERRED_FOR_COORD_BIT) != 0;

    this.inetAddr = StaticSerialization.readInetAddress(in);
    if (this.inetAddr != null) {
      this.hostName =
          SocketCreator.resolve_dns ? SocketCreator.getHostName(inetAddr)
              : inetAddr.getHostAddress();
    }
    this.udpPort = in.readInt();
    this.vmViewId = in.readInt();
    this.uuidMSBs = in.readLong();
    this.uuidLSBs = in.readLong();
    if (context.getSerializationVersion().ordinal() >= Version.GEODE_1_2_0.ordinal()) {
      this.vmKind = in.readByte();
    }
    this.isPartial = true;
  }


  public boolean hasAdditionalData() {
    return uuidMSBs != 0 || uuidLSBs != 0 || memberWeight != 0;
  }


  public void writeAdditionalData(DataOutput out) throws IOException {
    out.writeLong(uuidMSBs);
    out.writeLong(uuidLSBs);
    out.write(memberWeight);
  }


  public void readAdditionalData(DataInput in) throws ClassNotFoundException, IOException {
    try {
      this.uuidMSBs = in.readLong();
      this.uuidLSBs = in.readLong();
      memberWeight = (byte) (in.readByte() & 0xFF);
    } catch (EOFException e) {
      // some IDs do not have UUID or membership weight information
    }
  }

  private String formatUUID() {
    UUID uuid = getUUID();
    return ";uuid=" + (uuid == null ? "none" : getUUID().toStringLong());
  }

  public void setDurableTimeout(int newValue) {
    durableTimeout = newValue;
  }

  public void setDurableId(String id) {
    durableId = id;
  }


  public static class GMSMemberWrapper {
    GMSMember mbr;

    public GMSMemberWrapper(GMSMember m) {
      this.mbr = m;
    }

    public GMSMember getMbr() {
      return mbr;
    }

    @Override
    public int hashCode() {
      return mbr.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof GMSMemberWrapper)) {
        return false;
      }
      GMSMember other = ((GMSMemberWrapper) obj).mbr;
      return mbr.compareTo(other) == 0;
    }

    @Override
    public String toString() {
      return "GMSMemberWrapper [mbr=" + mbr + "]";
    }
  }


}
