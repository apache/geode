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

package org.apache.geode.management.internal.cli.commands;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

public class RegionCommandsUtils {

  public static final Set<RegionShortcut> PERSISTENT_OVERFLOW_SHORTCUTS = new TreeSet<>();

  static {
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.PARTITION_PERSISTENT);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.PARTITION_REDUNDANT_PERSISTENT);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.PARTITION_OVERFLOW);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.PARTITION_REDUNDANT_OVERFLOW);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.PARTITION_PERSISTENT_OVERFLOW);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.PARTITION_REDUNDANT_PERSISTENT_OVERFLOW);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.REPLICATE_PERSISTENT);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.REPLICATE_OVERFLOW);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.REPLICATE_PERSISTENT_OVERFLOW);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.LOCAL_PERSISTENT);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.LOCAL_OVERFLOW);
    PERSISTENT_OVERFLOW_SHORTCUTS.add(RegionShortcut.LOCAL_PERSISTENT_OVERFLOW);
  }

  static void validateGroups(InternalCache cache, String[] groups) {
    if (groups != null && groups.length != 0) {
      Set<String> existingGroups = new HashSet<>();
      Set<DistributedMember> members = CliUtil.getAllNormalMembers(cache);
      for (DistributedMember distributedMember : members) {
        List<String> memberGroups = distributedMember.getGroups();
        existingGroups.addAll(memberGroups);
      }
      List<String> groupsList = new ArrayList<>(Arrays.asList(groups));
      groupsList.removeAll(existingGroups);

      if (!groupsList.isEmpty()) {
        throw new IllegalArgumentException(
            CliStrings.format(CliStrings.CREATE_REGION__MSG__GROUPS_0_ARE_INVALID,
                new Object[] {String.valueOf(groupsList)}));
      }
    }
  }

}
