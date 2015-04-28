/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.membership.jgroup;

import java.net.InetAddress;
import java.io.*;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.distributed.DurableClientAttributes;
import com.gemstone.gemfire.distributed.internal.membership.NetMember;
import com.gemstone.gemfire.distributed.internal.membership.MemberAttributes;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.stack.IpAddress;

/**
 * This is the fundamental representation of a member of a GemFire distributed system.
 * 
 * Unfortunately, this class serves two distinct functions.  First, it is the
 * fundamental element of membership in the GemFire distributed system.  As such,
 * it is used in enumerations and properly responds to hashing and equals() comparisons.
 * 
 * Second, it is used as a cheap way of representing an address.  This is
 * unfortunate, because as a NetMember, it holds two separate port numbers: the
 * "membership" descriptor as well as a direct communication channel.
 * 
 * TODO fix this.
 */
public class JGroupMember implements NetMember {
  private transient IpAddress ipAddr;
  
  
  // Used only by Externalization
  public JGroupMember() {
  }
  
  /** the JGroups address object */
  public IpAddress getAddress() {
    return ipAddr;
  }

  public MemberAttributes getAttributes() {
    return new MemberAttributes(ipAddr.getDirectPort(), ipAddr.getProcessId(),
        ipAddr.getVmKind(), ipAddr.getBirthViewId(), ipAddr.getName(), ipAddr.getRoles(),
        (DurableClientAttributes)ipAddr.getDurableClientAttributes());
  }

  public void setAttributes(MemberAttributes p_attr) {
    MemberAttributes attr = p_attr;
    if (attr == null) {
      attr = MemberAttributes.INVALID;
    }
    GFJGBasicAdapter.insertGemFireAttributes(ipAddr, attr);
  }
  
  /**
   * This constructor used internally, esp. for views
   * 
   * @param base
   */
  public JGroupMember(IpAddress base) {
    ipAddr = base;  
  }
  
  /**
   * This is the only constructor to refer to a CacheMember other
   * than the current host.
   */
  public JGroupMember(JGroupMember m) {
    ipAddr = m.ipAddr; // This should be ok, since they shouldn't change
  }

  /**
   * Create a CacheMember referring to the current host (as defined by
   * the given string).
   * 
   * @param i the hostname, must be for the current host
   * @param p the membership listening port
   */
  public JGroupMember(String i, int p) {
    ipAddr = new IpAddress(i, p);
//    ipAddr.splitBrainEnabled(false);
//    ipAddr.cantBeCoordinator(false);
  }

  /**
   * Create a CacheMember referring to the current host (as defined by
   * the given string).
   * 
   * @param i the hostname, must be for the current host
   * @param p the membership listening port
   * @param splitBrainEnabled whether the member has network partition detection enabled
   * @param canBeCoordinator whether the member can be group coordinator
   */
  public JGroupMember(InetAddress i, int p, boolean splitBrainEnabled, boolean canBeCoordinator) {
    ipAddr = new IpAddress(i, p);
    ipAddr.splitBrainEnabled(splitBrainEnabled);
    ipAddr.shouldntBeCoordinator(!canBeCoordinator);
  }

  /**
   * Create a CacheMember referring to the current host
   * 
   * @param port the membership listening port
   */
  public JGroupMember(int port) {
    ipAddr = new IpAddress(port);
  }

  public InetAddress getIpAddress() {
    return ipAddr.getIpAddress();
  }

  public int getPort() {
    return ipAddr.getPort();
  }

  public boolean isMulticastAddress() {
    return ipAddr.isMulticastAddress();
  }
  
  public boolean splitBrainEnabled() {
    return ipAddr.splitBrainEnabled();
  }
  
  public boolean canBeCoordinator() {
    return ipAddr.preferredForCoordinator();
  }

  /**
   * Establishes an order between 2 addresses. Assumes other contains non-null IpAddress.
   * Excludes channel_name from comparison.
   * @return 0 for equality, value less than 0 if smaller, greater than 0 if greater.
   */
  public int compare(NetMember other) {
    return compareTo(other);
  }

  /**
   * implements the java.lang.Comparable interface
   * @see java.lang.Comparable
   * @param o - the Object to be compared
   * @return a negative integer, zero, or a positive integer as this object is less than,
   *         equal to, or greater than the specified object.
   * @exception java.lang.ClassCastException - if the specified object's type prevents it
   *            from being compared to this Object.
   */
  public int compareTo(Object o) {
    if (o == this) {
      return 0;
    }
    // obligatory type check
    if ((o == null) || !(o instanceof JGroupMember))
      throw new ClassCastException(LocalizedStrings.JGroupMember_JGROUPMEMBERCOMPARETO_COMPARISON_BETWEEN_DIFFERENT_CLASSES.toLocalizedString());
    return ipAddr.compareTo(((JGroupMember)o).ipAddr);
  }

  @Override
  public boolean equals(Object obj) {
    // GemStone fix for 29125
    if ((obj == null) || !(obj instanceof JGroupMember)) {
      return false;
    }
    return compareTo(obj) == 0;
  }

  @Override
  public int hashCode() {
    return ipAddr.hashCode();
  }

  @Override
  public String toString() {
    return ipAddr.toString();
  }

  
  /**
   * For Externalizable
   * 
   * @see Externalizable
   */
  public void writeExternal(ObjectOutput out) throws IOException {
    if (ipAddr == null)
      throw new InternalGemFireError(LocalizedStrings.JGroupMember_ATTEMPT_TO_EXTERNALIZE_NULL_IP_ADDRESS.toLocalizedString());

//    ipAddr.writeExternal(out);
    // do it the way we like
    byte[] address = ipAddr.getIpAddress().getAddress();
    
    out.writeInt(address.length); // IPv6 compatible
    out.write(address);
    out.writeInt(ipAddr.getPort());
    out.write(ipAddr.getFlags());
    Version.writeOrdinal(out, ipAddr.getVersionOrdinal(), true);
    byte bytes[] = new MemberAttributes(ipAddr.getDirectPort(), ipAddr.getProcessId(),
        ipAddr.getVmKind(), ipAddr.getBirthViewId(), ipAddr.getName(), ipAddr.getRoles(),
        (DurableClientAttributes)ipAddr.getDurableClientAttributes()).toByteArray();
    if (bytes == null)
      out.writeInt(0);
    else {
      out.writeInt(bytes.length);
      out.write(bytes);
    }
  }
  
  /**
   * For Externalizable
   * 
   * @see Externalizable
   */
  public void readExternal(ObjectInput in) throws IOException,
          ClassNotFoundException {
//    ipAddr = new IpAddress();
//    ipAddr.readExternal(in);
    // do it the way we like
    int len = in.readInt(); // IPv6 compatible
    byte addr[] = new byte[len];
    in.readFully(addr);
    InetAddress ia = InetAddress.getByAddress(addr);
    int port = in.readInt();
    byte flags = in.readByte();
    ipAddr = new IpAddress(ia, port);
    ipAddr.setFlags(flags);
    ipAddr.readVersion(flags, in);
    len = in.readInt();
    if (len != 0) {
      byte bytes[] = new byte[len];
      in.readFully(bytes);
      GFJGBasicAdapter.insertGemFireAttributes(ipAddr, new MemberAttributes(bytes));
    }
  }

  public void setPort(int p) {
    IpAddress i = new IpAddress(ipAddr.getIpAddress(), p);
    i.setFlags(ipAddr.getFlags());
    i.setVmKind(ipAddr.getVmKind());
    i.setDirectPort(ipAddr.getDirectPort());
    i.setProcessId(ipAddr.getProcessId());
    i.setRoles(ipAddr.getRoles());
    i.setDurableClientAttributes(ipAddr.getDurableClientAttributes());
    i.setVersionOrdinal(ipAddr.getVersionOrdinal());
    this.ipAddr = i;
  }
 }
