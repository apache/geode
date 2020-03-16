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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.functions.GarbageCollectionFunction;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.management.internal.util.ManagementUtils;
import org.apache.geode.security.ResourcePermission;

public class GCCommand extends GfshCommand {
  @CliCommand(value = CliStrings.GC, help = CliStrings.GC__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DEBUG_UTIL})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel gc(
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.GC__GROUP__HELP) String[] groups,
      @CliOption(key = CliStrings.MEMBER, optionContext = ConverterHint.ALL_MEMBER_IDNAME,
          help = CliStrings.GC__MEMBER__HELP) String memberId) {
    Cache cache = getCache();
    ResultModel result = new ResultModel();
    TabularResultModel summary = result.addTable("summary");
    InfoResultModel errors = result.addInfo("errors");
    String headerText = "GC Summary";
    summary.setHeader(headerText);
    Set<DistributedMember> dsMembers = new HashSet<>();
    if (memberId != null && memberId.length() > 0) {
      DistributedMember member = getMember(memberId);
      dsMembers.add(member);
    } else if (groups != null && groups.length > 0) {
      for (String group : groups) {
        dsMembers.addAll(cache.getDistributedSystem().getGroupMembers(group));
      }
    } else {
      // gc on entire cluster
      // exclude locators
      dsMembers = getAllNormalMembers();
    }

    GarbageCollectionFunction garbageCollectionFunction = new GarbageCollectionFunction();
    List<?> resultList =
        (List<?>) ManagementUtils.executeFunction(garbageCollectionFunction, null, dsMembers)
            .getResult();

    for (Object object : resultList) {
      if (object == null) {
        errors.addLine("ResultMap was null");
      } else if (object instanceof Throwable) {
        errors.addLine("Exception in GC " + ((Throwable) object).getMessage());
        LogWrapper.getInstance(getCache())
            .fine("Exception in GC " + ((Throwable) object).getMessage(), ((Throwable) object));
      } else if (object instanceof String) {
        // unexpected exception string - cache may be closed or something
        errors.addLine((String) object);
      } else {
        @SuppressWarnings("unchecked")
        Map<String, String> resultMap = (Map<String, String>) object;
        summary
            .accumulate(CliStrings.GC__MSG__MEMBER_NAME, resultMap.get("MemberId"));
        summary.accumulate(CliStrings.GC__MSG__HEAP_SIZE_BEFORE_GC,
            resultMap.get("HeapSizeBeforeGC"));
        summary
            .accumulate(CliStrings.GC__MSG__HEAP_SIZE_AFTER_GC, resultMap.get("HeapSizeAfterGC"));
        summary
            .accumulate(CliStrings.GC__MSG__TOTAL_TIME_IN_GC, resultMap.get("TimeSpentInGC"));
      }
    }

    return result;
  }

}
