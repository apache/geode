/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
    this.groups = l_groups;
    if (p_name == null) {
      this.name = "";
    } else {
      this.name = p_name;
    }
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
   * @return the membership view number in which this member was born
   */
  public int getVmViewId() {
    return this.vmViewId;
  }
}

