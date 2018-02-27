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

package org.apache.geode.management.internal.cli;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.cache.Region;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.exceptions.UserErrorException;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

/**
 * A utility class to provide easy access to members, particularly in Gfsh commands and functions,
 * provided cache access is already available. As an interface, an anonymous class can be
 * instantiated via lambda, providing a {@link java.util.function.Supplier} for the
 * {@link CacheMembers#getCache()} method.
 */
public interface CacheMembers {
  InternalCache getCache();

  /**
   * Gets all members in the GemFire distributed system/cache, including locators
   */
  default Set<DistributedMember> getAllMembers() {
    return getAllMembers(getCache());
  }

  default Set<DistributedMember> getAllMembers(final InternalCache cache) {
    return getAllMembers(cache.getInternalDistributedSystem());
  }

  default Set<DistributedMember> getAllMembers(InternalDistributedSystem internalDS) {
    return new HashSet<DistributedMember>(
        internalDS.getDistributionManager().getDistributionManagerIds());
  }

  /**
   * Get All members, excluding locators
   */
  default Set<DistributedMember> getAllNormalMembers(InternalCache cache) {
    return new HashSet<DistributedMember>(cache.getInternalDistributedSystem()
        .getDistributionManager().getNormalDistributionManagerIds());
  }

  /**
   * this either returns a non-null member or throw an exception if member is not found.
   */
  default DistributedMember getMember(final String memberName) {
    DistributedMember member = findMember(memberName);

    if (member == null) {
      throw new EntityNotFoundException(
          CliStrings.format(CliStrings.MEMBER_NOT_FOUND_ERROR_MESSAGE, memberName));
    }
    return member;
  }

  /**
   * if no members matches these names, a UserErrorException will be thrown
   */
  default Set<DistributedMember> getMembers(String[] groups, String[] members) {
    Set<DistributedMember> matchingMembers = findMembers(groups, members);
    if (matchingMembers.size() == 0) {
      throw new EntityNotFoundException(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }
    return matchingMembers;
  }

  /**
   * if no members matches these names, a UserErrorException will be thrown
   */
  default Set<DistributedMember> getMembersIncludingLocators(String[] groups, String[] members) {
    Set<DistributedMember> matchingMembers = findMembersIncludingLocators(groups, members);
    if (matchingMembers.size() == 0) {
      throw new EntityNotFoundException(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
    }
    return matchingMembers;
  }

  /**
   * this will return the member found or null if no member with that name
   */
  default DistributedMember findMember(final String memberName) {
    return findMember(memberName, getCache());
  }

  default DistributedMember findMember(final String memberName, InternalCache cache) {
    if (memberName == null) {
      return null;
    }

    Set<DistributedMember> memberSet = getAllMembers(cache);
    return memberSet.stream().filter(member -> memberName.equalsIgnoreCase(member.getId())
        || memberName.equalsIgnoreCase(member.getName())).findFirst().orElse(null);
  }

  /**
   * if no members matches these names, an empty set would return, this does not include locators
   */
  default Set<DistributedMember> findMembers(String[] groups, String[] members) {
    return findMembers(groups, members, getCache());
  }

  default Set<DistributedMember> findMembers(String[] groups, String[] members,
      InternalCache cache) {
    Set<DistributedMember> allNormalMembers = getAllNormalMembers(cache);

    return findMembers(allNormalMembers, groups, members);
  }

  default Set<DistributedMember> findMembers(Set<DistributedMember> membersToConsider,
      String[] groups, String[] members) {
    if (groups == null) {
      groups = new String[] {};
    }

    if (members == null) {
      members = new String[] {};
    }

    if ((members.length > 0) && (groups.length > 0)) {
      throw new UserErrorException(CliStrings.PROVIDE_EITHER_MEMBER_OR_GROUP_MESSAGE);
    }

    if (members.length == 0 && groups.length == 0) {
      return membersToConsider;
    }

    Set<DistributedMember> matchingMembers = new HashSet<>();
    // it will either go into this loop or the following loop, not both.
    for (String memberNameOrId : members) {
      for (DistributedMember member : membersToConsider) {
        if (memberNameOrId.equalsIgnoreCase(member.getId())
            || memberNameOrId.equalsIgnoreCase(member.getName())) {
          matchingMembers.add(member);
        }
      }
    }

    for (String group : groups) {
      for (DistributedMember member : membersToConsider) {
        if (member.getGroups().contains(group)) {
          matchingMembers.add(member);
        }
      }
    }
    return matchingMembers;
  }

  /**
   * if no members matches these names, an empty set would return
   */
  default Set<DistributedMember> findMembersIncludingLocators(String[] groups, String[] members) {
    Set<DistributedMember> allMembers = getAllMembers(getCache());
    return findMembers(allMembers, groups, members);
  }

  default Set<DistributedMember> findMembersForRegion(InternalCache cache, String regionPath) {
    return CliUtil.getRegionAssociatedMembers(regionPath, cache, true);
  }

  default Set<DistributedMember> findAnyMembersForRegion(InternalCache cache, String regionPath) {
    return getRegionAssociatedMembers(regionPath, cache, false);
  }

  default Set<DistributedMember> getRegionAssociatedMembers(String region,
      final InternalCache cache, boolean returnAll) {
    if (region == null || region.isEmpty()) {
      return Collections.emptySet();
    }

    if (!region.startsWith(Region.SEPARATOR)) {
      region = Region.SEPARATOR + region;
    }

    DistributedRegionMXBean regionMXBean =
        ManagementService.getManagementService(cache).getDistributedRegionMXBean(region);

    if (regionMXBean == null) {
      return Collections.emptySet();
    }

    String[] regionAssociatedMemberNames = regionMXBean.getMembers();
    Set<DistributedMember> matchedMembers = new HashSet<>();
    Set<DistributedMember> allClusterMembers = new HashSet<>(cache.getMembers());
    allClusterMembers.add(cache.getDistributedSystem().getDistributedMember());

    for (DistributedMember member : allClusterMembers) {
      for (String regionAssociatedMemberName : regionAssociatedMemberNames) {
        String name = MBeanJMXAdapter.getMemberNameOrId(member);
        if (name.equals(regionAssociatedMemberName)) {
          matchedMembers.add(member);
          if (!returnAll) {
            return matchedMembers;
          }
        }
      }
    }
    return matchedMembers;
  }

  default Set<DistributedMember> findMembersWithAsyncEventQueue(String queueId) {
    InternalCache cache = getCache();
    Set<DistributedMember> members = findMembers(null, null, cache);
    return members.stream().filter(m -> CliUtil.getAsyncEventQueueIds(cache, m).contains(queueId))
        .collect(Collectors.toSet());
  }

}
