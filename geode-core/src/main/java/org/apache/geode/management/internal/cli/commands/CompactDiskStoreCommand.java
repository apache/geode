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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.messages.CompactRequest;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class CompactDiskStoreCommand implements GfshCommand {
  @CliCommand(value = CliStrings.COMPACT_DISK_STORE, help = CliStrings.COMPACT_DISK_STORE__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DISKSTORE})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE, target = ResourcePermission.Target.DISK)
  public Result compactDiskStore(
      @CliOption(key = CliStrings.COMPACT_DISK_STORE__NAME, mandatory = true,
          optionContext = ConverterHint.DISKSTORE,
          help = CliStrings.COMPACT_DISK_STORE__NAME__HELP) String diskStoreName,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.COMPACT_DISK_STORE__GROUP__HELP) String[] groups) {
    Result result;

    try {
      // disk store exists validation
      if (!diskStoreExists(diskStoreName)) {
        result = ResultBuilder.createUserErrorResult(
            CliStrings.format(CliStrings.COMPACT_DISK_STORE__DISKSTORE_0_DOES_NOT_EXIST,
                new Object[] {diskStoreName}));
      } else {
        InternalDistributedSystem ds = getCache().getInternalDistributedSystem();

        Map<DistributedMember, PersistentID> overallCompactInfo = new HashMap<>();

        Set<?> otherMembers = ds.getDistributionManager().getOtherNormalDistributionManagerIds();
        Set<InternalDistributedMember> allMembers = new HashSet<>();

        for (Object member : otherMembers) {
          allMembers.add((InternalDistributedMember) member);
        }
        allMembers.add(ds.getDistributedMember());

        String groupInfo = "";
        // if groups are specified, find members in the specified group
        if (groups != null && groups.length > 0) {
          groupInfo = CliStrings.format(CliStrings.COMPACT_DISK_STORE__MSG__FOR_GROUP,
              new Object[] {Arrays.toString(groups) + "."});
          final Set<InternalDistributedMember> selectedMembers = new HashSet<>();
          List<String> targetedGroups = Arrays.asList(groups);
          for (InternalDistributedMember member : allMembers) {
            List<String> memberGroups = member.getGroups();
            if (!Collections.disjoint(targetedGroups, memberGroups)) {
              selectedMembers.add(member);
            }
          }

          allMembers = selectedMembers;
        }

        // allMembers should not be empty when groups are not specified - it'll
        // have at least one member
        if (allMembers.isEmpty()) {
          result = ResultBuilder.createUserErrorResult(
              CliStrings.format(CliStrings.COMPACT_DISK_STORE__NO_MEMBERS_FOUND_IN_SPECIFED_GROUP,
                  new Object[] {Arrays.toString(groups)}));
        } else {
          // first invoke on local member if it exists in the targeted set
          if (allMembers.remove(ds.getDistributedMember())) {
            PersistentID compactedDiskStoreId = CompactRequest.compactDiskStore(diskStoreName);
            if (compactedDiskStoreId != null) {
              overallCompactInfo.put(ds.getDistributedMember(), compactedDiskStoreId);
            }
          }

          // was this local member the only one? Then don't try to send
          // CompactRequest. Otherwise, send the request to others
          if (!allMembers.isEmpty()) {
            // Invoke compact on all 'other' members
            Map<DistributedMember, PersistentID> memberCompactInfo =
                CompactRequest.send(ds.getDistributionManager(), diskStoreName, allMembers);
            if (memberCompactInfo != null && !memberCompactInfo.isEmpty()) {
              overallCompactInfo.putAll(memberCompactInfo);
              memberCompactInfo.clear();
            }
            String notExecutedMembers = CompactRequest.getNotExecutedMembers();
            if (notExecutedMembers != null && !notExecutedMembers.isEmpty()) {
              LogWrapper.getInstance()
                  .info("compact disk-store \"" + diskStoreName
                      + "\" message was scheduled to be sent to but was not send to "
                      + notExecutedMembers);
            }
          }

          // If compaction happened at all, then prepare the summary
          if (overallCompactInfo != null && !overallCompactInfo.isEmpty()) {
            CompositeResultData compositeResultData = ResultBuilder.createCompositeResultData();
            CompositeResultData.SectionResultData section;

            Set<Map.Entry<DistributedMember, PersistentID>> entries = overallCompactInfo.entrySet();

            for (Map.Entry<DistributedMember, PersistentID> entry : entries) {
              String memberId = entry.getKey().getId();
              section = compositeResultData.addSection(memberId);
              section.addData("On Member", memberId);

              PersistentID persistentID = entry.getValue();
              if (persistentID != null) {
                CompositeResultData.SectionResultData subSection =
                    section.addSection("DiskStore" + memberId);
                subSection.addData("UUID", persistentID.getUUID());
                subSection.addData("Host", persistentID.getHost().getHostName());
                subSection.addData("Directory", persistentID.getDirectory());
              }
            }
            compositeResultData.setHeader("Compacted " + diskStoreName + groupInfo);
            result = ResultBuilder.buildResult(compositeResultData);
          } else {
            result = ResultBuilder.createInfoResult(
                CliStrings.COMPACT_DISK_STORE__COMPACTION_ATTEMPTED_BUT_NOTHING_TO_COMPACT);
          }
        } // all members' if
      } // disk store exists' if
    } catch (RuntimeException e) {
      LogWrapper.getInstance().info(e.getMessage(), e);
      result = ResultBuilder.createGemFireErrorResult(
          CliStrings.format(CliStrings.COMPACT_DISK_STORE__ERROR_WHILE_COMPACTING_REASON_0,
              new Object[] {e.getMessage()}));
    }
    return result;
  }

  private boolean diskStoreExists(String diskStoreName) {
    InternalCache cache = getCache();
    ManagementService managementService = ManagementService.getExistingManagementService(cache);
    DistributedSystemMXBean dsMXBean = managementService.getDistributedSystemMXBean();
    Map<String, String[]> diskstore = dsMXBean.listMemberDiskstore();

    Set<Map.Entry<String, String[]>> entrySet = diskstore.entrySet();

    for (Map.Entry<String, String[]> entry : entrySet) {
      String[] value = entry.getValue();
      if (CliUtil.contains(value, diskStoreName)) {
        return true;
      }
    }
    return false;
  }
}
