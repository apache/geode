/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal.membership;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.distributed.DurableClientAttributes;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.*;
import java.util.*;

/**
 * The attributes of a distributed member. These attributes are stored as
 * the AdditionalBytes in JGroups' IpAddress.
 *
 * @author Kirk Lund
 * @since 5.0
 */
public class MemberAttributes implements DataSerializable {
  private static final long serialVersionUID = -3257772958884802693L;
  
  public static final MemberAttributes INVALID = new MemberAttributes(-1, -1, -1, -1, null, null, null);
  public static MemberAttributes DEFAULT = INVALID;
  
  public static void setDefaults(int dcPort, int vmPid, int vmKind, int vmViewId, String name, String[] groups, DurableClientAttributes durableClientAttributes) {
    DEFAULT = new MemberAttributes(dcPort, vmPid, vmKind, vmViewId, name, groups, durableClientAttributes);
  }
  
  private int dcPort;
  private int vmPid;
  private int vmKind;
  private int vmViewId;
  private String name;
  private String[] groups;
  private DurableClientAttributes durableClientAttributes;
  
  /** Constructs new MemberAttributes from parameters.  */
  public MemberAttributes(int dcPort, int vmPid, int vmKind, int vmViewId,
      String p_name, String[] p_groups, 
      DurableClientAttributes durableClientAttributes) {
    String[] l_groups = p_groups;
    this.dcPort = dcPort;
    this.vmPid = vmPid;
    this.vmKind = vmKind;
    this.vmViewId = vmViewId;
    if (l_groups == null) {
      l_groups = new String[0];
    }
    if (p_name == null) {
      this.name = "";
    } else {
      this.name = p_name;
    }
    this.groups = l_groups;
    this.durableClientAttributes = durableClientAttributes;
  }
  
  /** Constructs new MemberAttributes from DataInput.  */
  public MemberAttributes(byte[] b) throws IOException, ClassNotFoundException {
    this.byteInfo = b;
    DataInputStream in = 
      new DataInputStream(new ByteArrayInputStream(b));
    fromData(in);
  }
  
  public MemberAttributes(MemberAttributes other) {
    this.dcPort = other.dcPort;
    this.vmPid = other.vmPid;
    this.vmKind = other.vmKind;
    this.name = other.name;
    this.groups = other.groups;
    this.durableClientAttributes = other.durableClientAttributes;
  }
  
  /** Returns direct channel port. */
  public int getPort() {
    return this.dcPort;
  }
  
  /** Returns VM PID. */
  public int getVmPid() {
    return this.vmPid;
  }
  
  /** Returns VM Kind (enumerated constants in DistributionManager). */
  public int getVmKind() {
    return this.vmKind;
  }
  
  /** Returns the name of the member. */
  public String getName() {
    return this.name;
  }
  
  /** Returns the groups of the member. */
  public String[] getGroups() {
    return this.groups;
  }
  
  /** Returns the durable client attributes. */
  public DurableClientAttributes getDurableClientAttributes() {
    return this.durableClientAttributes;
  }

  /** Parses comma-separated-values into array of groups (strings). */
  public static String[] parseGroups(String csv) {
    if (csv == null || csv.length() == 0) {
      return new String[0];
    }
    List groups = new ArrayList();
    StringTokenizer st = new StringTokenizer(csv, ",");
    while (st.hasMoreTokens()) {
      String groupName = st.nextToken().trim();
      // TODO make case insensitive
      if (!groups.contains(groupName)) { // only add each group once
        groups.add(groupName);
      }
    }
    return (String[]) groups.toArray(new String[groups.size()]);
  }
  /** Parses comma-separated-roles/groups into array of groups (strings). */
  public static String[] parseGroups(String csvRoles, String csvGroups) {
    List<String> groups = new ArrayList<String>();
    parseCsv(groups, csvRoles);
    parseCsv(groups, csvGroups);
    return (String[]) groups.toArray(new String[groups.size()]);
  }
  private static void parseCsv(List<String> groups, String csv) {
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
  
  /** Writes the contents of this object to the given output. */
  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.dcPort);
    out.writeInt(this.vmPid);
    out.writeInt(this.vmKind);
    DataSerializer.writeString(this.name, out);
    DataSerializer.writeStringArray(this.groups, out);
    DataSerializer.writeString(this.durableClientAttributes==null ? "" : this.durableClientAttributes.getId(), out);
    DataSerializer.writeInteger(Integer.valueOf(this.durableClientAttributes==null ? 300 : this.durableClientAttributes.getTimeout()), out);
  }

  /** Reads the contents of this object from the given input. */
  public void fromData(DataInput in)
  throws IOException, ClassNotFoundException {
    this.dcPort = in.readInt();
    this.vmPid = in.readInt();
    this.vmKind = in.readInt();
    this.name = DataSerializer.readString(in);
    this.groups = DataSerializer.readStringArray(in);
    String durableId = DataSerializer.readString(in);
    int durableTimeout = DataSerializer.readInteger(in).intValue();
    this.durableClientAttributes = new DurableClientAttributes(durableId, durableTimeout);
  }
  
  private byte[] byteInfo;
  
  /** Returns the contents of this objects serialized as a byte array. */
  public byte[] toByteArray() {
    if (byteInfo != null) {
      return byteInfo;
    }
    try {
      HeapDataOutputStream hdos = new HeapDataOutputStream(Version.CURRENT);
      toData(hdos);
      byteInfo = hdos.toByteArray();
      return byteInfo;
    }
    catch (IOException e) {
      throw new InternalGemFireError(LocalizedStrings.MemberAttributes_IOEXCEPTION_ON_A_BYTE_ARRAY_0.toLocalizedString(e));
    }
  }
  
  public static MemberAttributes fromByteArray(byte[] bytes) {
    try {
      return new MemberAttributes(bytes);
    }
    catch (IOException e) {
      throw new InternalGemFireError(LocalizedStrings.MemberAttributes_IOEXCEPTION_ON_A_BYTE_ARRAY_0.toLocalizedString(e));
    }
    catch (ClassNotFoundException e) {
      throw new InternalGemFireError(LocalizedStrings.MemberAttributes_CLASSNOTFOUNDEXCEPTION_IN_DESERIALIZATION_0.toLocalizedString(e));
    }
  }

	/**
	 * Returns a string representation of the object.
	 * 
	 * @return a string representation of the object
	 */
  @Override
	public String toString() {
		final StringBuffer sb = new StringBuffer("[MemberAttributes: ");
		sb.append("dcPort=").append(this.dcPort);
		sb.append(", vmPid=").append(this.vmPid);
		sb.append(", vmKind=").append(this.vmKind);
		sb.append(", name=").append(this.name);
		sb.append(", groups=").append("(");
    for (int i = 0; i < groups.length; i++) {
      sb.append(groups[i]);
    }
    sb.append(")");
    sb.append(", durableClientAttributes=").append(this.durableClientAttributes);
    sb.append("]");
		return sb.toString();
	}

  /**
   * Set the VmPid to be the given value.  This may be done by JGroups UDP
   * protocol if there is no PID available to augment its membership port number.
   * This functionality was added by us for bug #41983
   * @param uniqueID
   */
  public static void setDefaultVmPid(int uniqueID) {
    // note: JGroupMembershipManager establishes DEFAULT before attempting to
    // create a JGroups channel, so we know it isn't INVALID here
    setDefaults(DEFAULT.dcPort, uniqueID, DEFAULT.vmKind, DEFAULT.vmViewId, DEFAULT.name,
        DEFAULT.groups, DEFAULT.durableClientAttributes);
  }

  /**
   * @return the membership view number in which this member was born
   */
  public int getVmViewId() {
    return this.vmViewId;
  }
}

