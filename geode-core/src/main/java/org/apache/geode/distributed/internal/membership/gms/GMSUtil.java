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
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.distributed.internal.membership.gms.membership.HostAddress;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.SerializationVersion;
import org.apache.geode.internal.serialization.StaticSerialization;

public class GMSUtil {
  private static final Logger logger = LogService.getLogger();

  /**
   * parse locators & check that the resulting address is compatible with the given address
   *
   * @param locatorsString a DistributionConfig "locators" string
   * @param bindAddress optional address to check for loopback compatibility
   * @return addresses of locators
   */
  public static List<HostAddress> parseLocators(String locatorsString, String bindAddress) {
    InetAddress addr = null;

    try {
      if (bindAddress == null || bindAddress.trim().length() == 0) {
        addr = SocketCreator.getLocalHost();
      } else {
        addr = InetAddress.getByName(bindAddress);
      }
    } catch (UnknownHostException e) {
      // ignore
    }
    return parseLocators(locatorsString, addr);
  }

  public static GMSMember readMemberID(DataInput in,
      SerializationContext context) throws IOException, ClassNotFoundException {
    Object id = context.getDSFIDSerializer().getDataSerializer().readObject(in);
    if (id == null || id instanceof GMSMember) {
      return (GMSMember) id;
    }
    // return ((GMSMemberAdapter)((InternalDistributedMember)id).getNetMember()).getGmsMember();
    try {
      Method getNetMember = id.getClass().getMethod("getNetMember");
      Object netMember = getNetMember.invoke(id);
      Method getGmsMember = netMember.getClass().getMethod("getGmsMember");
      return (GMSMember) getGmsMember.invoke(netMember);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new IllegalStateException("Unable to deserialize a member ID", e);
    }
  }

  public static Set<GMSMember> readHashSetOfMemberIDs(DataInput in,
      SerializationContext context)
      throws IOException, ClassNotFoundException {
    int size = StaticSerialization.readArrayLength(in);
    if (size == -1) {
      return null;
    }
    Set<GMSMember> result = new HashSet<>();
    for (int i = 0; i < size; i++) {
      result.add(readMemberID(in, context));
    }
    return result;
  }

  /**
   * parse locators & check that the resulting address is compatible with the given address
   *
   * @param locatorsString a DistributionConfig "locators" string
   * @param bindAddress optional address to check for loopback compatibility
   * @return addresses of locators
   */
  public static List<HostAddress> parseLocators(String locatorsString, InetAddress bindAddress) {
    List<HostAddress> result = new ArrayList<>(2);
    Set<InetSocketAddress> inetAddresses = new HashSet<>();
    String host;
    int port;
    boolean checkLoopback = (bindAddress != null);
    boolean isLoopback = (checkLoopback && bindAddress.isLoopbackAddress());

    StringTokenizer parts = new StringTokenizer(locatorsString, ",");
    while (parts.hasMoreTokens()) {
      try {
        String str = parts.nextToken();
        host = str.substring(0, str.indexOf('['));
        int idx = host.lastIndexOf('@');
        if (idx < 0) {
          idx = host.lastIndexOf(':');
        }
        String start = host.substring(0, idx > -1 ? idx : host.length());
        if (start.indexOf(':') >= 0) { // a single numeric ipv6 address
          idx = host.lastIndexOf('@');
        }
        if (idx >= 0) {
          host = host.substring(idx + 1, host.length());
        }

        int startIdx = str.indexOf('[') + 1;
        int endIdx = str.indexOf(']');
        port = Integer.parseInt(str.substring(startIdx, endIdx));
        if (port <= 0) {
          continue;
        }
        InetSocketAddress isa = new InetSocketAddress(host, port);

        if (checkLoopback) {
          if (isLoopback && !isa.getAddress().isLoopbackAddress()) {
            throw new GemFireConfigException(
                "This process is attempting to join with a loopback address (" + bindAddress
                    + ") using a locator that does not have a local address (" + isa
                    + ").  On Unix this usually means that /etc/hosts is misconfigured.");
          }
        }
        HostAddress la = new HostAddress(isa, host);
        if (!inetAddresses.contains(isa)) {
          inetAddresses.add(isa);
          result.add(la);
        }
      } catch (NumberFormatException e) {
        // this shouldn't happen because the config has already been parsed and
        // validated
      }
    }

    return result;
  }

  /** Parses comma-separated-roles/groups into array of groups (strings). */
  public static String[] parseGroups(String csvRoles, String csvGroups) {
    List<String> groups = new ArrayList<String>();
    parseCsv(groups, csvRoles);
    parseCsv(groups, csvGroups);
    return groups.toArray(new String[groups.size()]);
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
   * replaces all occurrences of a given string in the properties argument with the given value
   */
  public static String replaceStrings(String properties, String property, String value) {
    StringBuilder sb = new StringBuilder();
    int start = 0;
    int index = properties.indexOf(property);
    while (index != -1) {
      sb.append(properties.substring(start, index));
      sb.append(value);

      start = index + property.length();
      index = properties.indexOf(property, start);
    }
    sb.append(properties.substring(start));
    return sb.toString();
  }

  public static List<GMSMember> readArrayOfIDs(DataInput in,
      SerializationContext context)
      throws IOException, ClassNotFoundException {
    int size = StaticSerialization.readArrayLength(in);
    if (size == -1) {
      return null;
    }
    List<GMSMember> result = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      result.add(readMemberID(in, context));
    }
    return result;
  }

  private static void writeAsInternalDistributedMember(GMSMember suspect, DataOutput out,
      SerializationContext context)
      throws IOException {
    context.getDSFIDSerializer().writeDSFID(suspect, DataSerializableFixedID.DISTRIBUTED_MEMBER,
        out);
  }

  public static void writeMemberID(GMSMember id, DataOutput out,
      SerializationContext context) throws IOException {
    if (id == null) {
      context.getDSFIDSerializer().getDataSerializer().writeObject(id, out);
      return;
    }
    short ordinal = context.getSerializationVersion().ordinal();
    if (ordinal <= SerializationVersion.GEODE_1_10_0_ORDINAL) {
      writeAsInternalDistributedMember(id, out, context);
    } else {
      context.getDSFIDSerializer().writeDSFID(id, out);
    }
  }

  public static void writeSetOfMemberIDs(Set<GMSMember> set, DataOutput out,
      SerializationContext context) throws IOException {
    int size;
    if (set == null) {
      size = -1;
    } else {
      size = set.size();
    }
    StaticSerialization.writeArrayLength(size, out);
    if (size > 0) {
      for (GMSMember member : set) {
        GMSUtil.writeMemberID(member, out, context);
      }
    }
  }
}
