/*
 *
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
 *
 */
package org.apache.geode.distributed.internal.membership.gms;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.geode.distributed.internal.membership.api.MemberIdentifier;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;

/**
 * GMSUtil contains a few static utility methods that should probably reside in other classes
 */
public class GMSUtil {

  public static <ID extends MemberIdentifier> Set<ID> readHashSetOfMemberIDs(DataInput in,
      DeserializationContext context)
      throws IOException, ClassNotFoundException {
    int size = StaticSerialization.readArrayLength(in);
    if (size == -1) {
      return null;
    }
    Set<ID> result = new HashSet<>();
    for (int i = 0; i < size; i++) {
      result.add(context.getDeserializer().readObject(in));
    }
    return result;
  }

  /**
   * Parses comma-separated-roles/groups into array of groups (strings).
   */
  public static String[] parseGroups(String csvRoles, String csvGroups) {
    List<String> groups = new ArrayList<>();
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
      sb.append(properties, start, index);
      sb.append(value);

      start = index + property.length();
      index = properties.indexOf(property, start);
    }
    sb.append(properties.substring(start));
    return sb.toString();
  }

  public static <ID extends MemberIdentifier> List<ID> readArrayOfIDs(DataInput in,
      DeserializationContext context)
      throws IOException, ClassNotFoundException {
    int size = StaticSerialization.readArrayLength(in);
    if (size == -1) {
      return null;
    }
    List<ID> result = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      result.add(context.getDeserializer().readObject(in));
    }
    return result;
  }

  public static <ID extends MemberIdentifier> void writeSetOfMemberIDs(Set<ID> set, DataOutput out,
      SerializationContext context) throws IOException {
    int size;
    if (set == null) {
      size = -1;
    } else {
      size = set.size();
    }
    StaticSerialization.writeArrayLength(size, out);
    if (size > 0) {
      for (ID member : set) {
        context.getSerializer().writeObject(member, out);
      }
    }
  }
}
