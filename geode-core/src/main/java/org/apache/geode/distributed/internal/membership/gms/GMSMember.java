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
import java.net.UnknownHostException;

import org.jgroups.util.UUID;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.membership.MemberAttributes;
import org.apache.geode.distributed.internal.membership.NetMember;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;

/**
 * This is the fundamental representation of a member of a GemFire distributed system.
 *
 * Unfortunately, this class serves two distinct functions. First, it is the fundamental element of
 * membership in the GemFire distributed system. As such, it is used in enumerations and properly
 * responds to hashing and equals() comparisons.
 *
 * Second, it is used as a cheap way of representing an address. This is unfortunate, because as a
 * NetMember, it holds two separate port numbers: the "membership" descriptor as well as a direct
 * communication channel.
 *
 */
public class GMSMember implements NetMember, DataSerializableFixedID {
  // whether to show UUID info in toString()
  private static final boolean SHOW_UUIDS =
      Boolean.getBoolean(DistributionConfig.GEMFIRE_PREFIX + "show_UUIDs");

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
  private DurableClientAttributes durableClientAttributes;
  private String[] groups;
  private short versionOrdinal = Version.CURRENT_ORDINAL;
  private long uuidLSBs;
  private long uuidMSBs;



  // Used only by Externalization
  public GMSMember() {}

  public MemberAttributes getAttributes() {
    return new MemberAttributes(directPort, processId, vmKind, vmViewId, name, groups,
        durableClientAttributes);
  }

  public void setAttributes(MemberAttributes p_attr) {
    MemberAttributes attr = p_attr;
    if (attr == null) {
      attr = MemberAttributes.INVALID;
    }
    processId = attr.getVmPid();
    vmKind = (byte) attr.getVmKind();
    directPort = attr.getPort();
    vmViewId = attr.getVmViewId();
    name = attr.getName();
    groups = attr.getGroups();
    durableClientAttributes = attr.getDurableClientAttributes();
  }

  /**
   * Create a CacheMember referring to the current host (as defined by the given string).
   *
   * @param i the hostname, must be for the current host
   * @param p the membership listening port
   */
  public GMSMember(String i, int p) {
    udpPort = p;
    try {
      inetAddr = InetAddress.getByName(i);
    } catch (UnknownHostException e) {
      // oops
    }
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
  public GMSMember(MemberAttributes attr, InetAddress i, int p,
      boolean networkPartitionDetectionEnabled, boolean preferredForCoordinator, short version,
      long msbs, long lsbs) {
    setAttributes(attr);
    this.inetAddr = i;
    this.udpPort = p;
    this.networkPartitionDetectionEnabled = networkPartitionDetectionEnabled;
    this.preferredForCoordinator = preferredForCoordinator;
    this.versionOrdinal = version;
    this.uuidMSBs = msbs;
    this.uuidLSBs = lsbs;
  }

  public GMSMember(InetAddress i, int p, short version, long msbs, long lsbs, int viewId) {
    this.inetAddr = i;
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
    this.durableClientAttributes = other.durableClientAttributes;
    this.groups = other.groups;
    this.versionOrdinal = other.versionOrdinal;
    this.uuidLSBs = other.uuidLSBs;
    this.uuidMSBs = other.uuidMSBs;
  }

  public int getPort() {
    return this.udpPort;
  }

  public boolean isMulticastAddress() {
    return false;
  }

  public boolean preferredForCoordinator() {
    return this.preferredForCoordinator;
  }

  public void setPreferredForCoordinator(boolean preferred) {
    this.preferredForCoordinator = preferred;
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
  public int compareTo(NetMember o) {
    if (o == this) {
      return 0;
    }
    // obligatory type check
    if (o == null || !(o instanceof GMSMember)) {
      throw new ClassCastException(
          "NetMember.compareTo(): comparison between different classes");
    }
    byte[] myAddr = inetAddr.getAddress();
    GMSMember his = (GMSMember) o;
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
    if (result == 0 && this.uuidMSBs != 0 && his.uuidMSBs != 0) {
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
  public int compareAdditionalData(NetMember other) {
    GMSMember his = (GMSMember) other;
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
    String uuid = SHOW_UUIDS ? (";uuid=" + getUUID().toStringLong())
        : ((this.uuidLSBs == 0 && this.uuidMSBs == 0) ? "; no uuid" : "; uuid set");

    sb.append("GMSMember[addr=").append(inetAddr).append(";port=").append(udpPort)
        .append(";processId=").append(processId).append(";name=").append(name).append(uuid)
        .append("]");
    return sb.toString();
  }


  public int getUdpPort() {
    return udpPort;
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

  @Override
  public void setVmViewId(int id) {
    this.vmViewId = id;
  }

  public int getDirectPort() {
    return directPort;
  }

  public String getName() {
    return name;
  }

  public DurableClientAttributes getDurableClientAttributes() {
    return durableClientAttributes;
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

  public void setDurableClientAttributes(DurableClientAttributes durableClientAttributes) {
    this.durableClientAttributes = durableClientAttributes;
  }

  @Override
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

  @Override
  public void toData(DataOutput out) throws IOException {
    writeEssentialData(out);
    out.writeInt(directPort);
    out.writeByte(memberWeight);
    out.writeByte(vmKind);
    out.writeInt(processId);

    DataSerializer.writeString(name, out);
    DataSerializer.writeStringArray(groups, out);
  }

  public void writeEssentialData(DataOutput out) throws IOException {
    Version.writeOrdinal(out, this.versionOrdinal, true);

    int flags = 0;
    if (networkPartitionDetectionEnabled)
      flags |= NPD_ENABLED_BIT;
    if (preferredForCoordinator)
      flags |= PREFERRED_FOR_COORD_BIT;
    out.writeShort(flags);

    DataSerializer.writeInetAddress(inetAddr, out);
    out.writeInt(udpPort);
    out.writeInt(vmViewId);
    out.writeLong(uuidMSBs);
    out.writeLong(uuidLSBs);
    if (InternalDataSerializer.getVersionForDataStream(out).compareTo(Version.GEODE_120) >= 0) {
      out.writeByte(vmKind);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    readEssentialData(in);
    this.directPort = in.readInt();
    this.memberWeight = in.readByte();
    this.vmKind = in.readByte();
    this.processId = in.readInt();

    this.name = DataSerializer.readString(in);
    this.groups = DataSerializer.readStringArray(in);
  }

  public void readEssentialData(DataInput in) throws IOException, ClassNotFoundException {
    this.versionOrdinal = Version.readOrdinal(in);

    int flags = in.readShort();
    this.networkPartitionDetectionEnabled = (flags & NPD_ENABLED_BIT) != 0;
    this.preferredForCoordinator = (flags & PREFERRED_FOR_COORD_BIT) != 0;

    this.inetAddr = DataSerializer.readInetAddress(in);
    this.udpPort = in.readInt();
    this.vmViewId = in.readInt();
    this.uuidMSBs = in.readLong();
    this.uuidLSBs = in.readLong();
    if (InternalDataSerializer.getVersionForDataStream(in).compareTo(Version.GEODE_120) >= 0) {
      this.vmKind = in.readByte();
    }
  }

  @Override
  public boolean hasAdditionalData() {
    return uuidMSBs != 0 || uuidLSBs != 0 || memberWeight != 0;
  }

  @Override
  public void writeAdditionalData(DataOutput out) throws IOException {
    out.writeLong(uuidMSBs);
    out.writeLong(uuidLSBs);
    out.write(memberWeight);
  }

  @Override
  public void readAdditionalData(DataInput in) throws ClassNotFoundException, IOException {
    try {
      this.uuidMSBs = in.readLong();
      this.uuidLSBs = in.readLong();
      memberWeight = (byte) (in.readByte() & 0xFF);
    } catch (EOFException e) {
      // some IDs do not have UUID or membership weight information
    }
  }
}
