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
package org.apache.geode.distributed.internal.membership;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.geode.distributed.DurableClientAttributes;

/**
 * The attributes of a distributed member.  This is largely deprecated as
 * GMSMember holds all of this information.
 *
 * @since GemFire 5.0
 */
public class MemberAttributes {
  
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
  
  /**
   * @return the membership view number in which this member was born
   */
  public int getVmViewId() {
    return this.vmViewId;
  }
}

