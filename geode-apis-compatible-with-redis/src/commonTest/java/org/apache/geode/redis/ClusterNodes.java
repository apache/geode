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

package org.apache.geode.redis;

import static org.apache.geode.redis.internal.netty.Coder.bytesToString;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.Pair;

public class ClusterNodes {

  private static final Pattern ipPortCportRE = Pattern.compile("([^:]*):([0-9]*)@([0-9]*)");

  private final List<ClusterNode> nodes = new ArrayList<>();

  private ClusterNodes() {}

  @SuppressWarnings("unchecked")
  public static ClusterNodes parseClusterSlots(List<Object> rawSlots) {
    ClusterNodes result = new ClusterNodes();

    for (Object obj : rawSlots) {
      List<Object> firstLevel = (List<Object>) obj;
      long slotStart = (long) firstLevel.get(0);
      long slotEnd = (long) firstLevel.get(1);

      List<Object> primary = (List<Object>) firstLevel.get(2);
      String primaryIp = bytesToString((byte[]) primary.get(0));
      long primaryPort = (long) primary.get(1);
      String primaryGUID = primary.size() > 2 ? bytesToString((byte[]) primary.get(2)) : "";

      result.addSlot(primaryGUID, primaryIp, primaryPort, slotStart, slotEnd);
    }

    return result;
  }

  public static ClusterNodes parseClusterNodes(String rawInput) {
    ClusterNodes result = new ClusterNodes();

    for (String line : rawInput.split("\\n")) {
      result.nodes.add(parseOneClusterNodeLine(line));
    }

    return result;
  }

  private static ClusterNode parseOneClusterNodeLine(String line) {
    String[] parts = line.split(" ");

    Matcher addressMatcher = ipPortCportRE.matcher(parts[1]);
    if (!addressMatcher.matches()) {
      throw new IllegalArgumentException("Unable to extract ip:port@cport from " + line);
    }

    boolean primary = parts[2].contains("master");

    List<Pair<Long, Long>> slots = new ArrayList<>();
    if (primary) {
      // Sometimes we see a 'primary' without slots which seems to imply it hasn't yet transitioned
      // to being a 'replica'.
      if (parts.length > 8) {
        for (int i = 8; i < parts.length; i++) {
          String[] startEnd = parts[i].split("-");
          long slotStart = Long.parseLong(startEnd[0]);
          long slotEnd;
          if (startEnd.length > 1) {
            slotEnd = Long.parseLong(startEnd[1]);
          } else {
            slotEnd = slotStart;
          }
          slots.add(Pair.of(slotStart, slotEnd));
        }
      } else {
        primary = false;
      }
    }

    return new ClusterNode(
        parts[0],
        addressMatcher.group(1),
        Integer.parseInt(addressMatcher.group(2)),
        primary,
        slots);
  }

  public ClusterNode getNode(String ip, long port) {
    for (ClusterNode node : nodes) {
      if (node.ipAddress.equals(ip) && node.port == port) {
        return node;
      }
    }
    return null;
  }

  public List<ClusterNode> getNodes() {
    return nodes;
  }

  private void addSlot(String guid, String ip, long port, long slotStart, long slotEnd) {
    ClusterNode node = getNode(ip, port);
    if (node == null) {
      List<Pair<Long, Long>> slots = new ArrayList<>();
      node = new ClusterNode(guid, ip, port, true, slots);
      nodes.add(node);
    }

    node.slots.add(Pair.of(slotStart, slotEnd));
  }

}
