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

import org.apache.geode.cache.execute.Function;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.functions.GarbageCollectionFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class GCCommand implements GfshCommand {
  @CliCommand(value = CliStrings.GC, help = CliStrings.GC__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_DEBUG_UTIL})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public Result gc(
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.GC__GROUP__HELP) String[] groups,
      @CliOption(key = CliStrings.MEMBER, optionContext = ConverterHint.ALL_MEMBER_IDNAME,
          help = CliStrings.GC__MEMBER__HELP) String memberId) {
    InternalCache cache = getCache();
    Result result;
    CompositeResultData gcResultTable = ResultBuilder.createCompositeResultData();
    TabularResultData resultTable = gcResultTable.addSection().addTable("Table1");
    String headerText = "GC Summary";
    resultTable.setHeader(headerText);
    Set<DistributedMember> dsMembers = new HashSet<>();
    if (memberId != null && memberId.length() > 0) {
      DistributedMember member = getMember(memberId);
      dsMembers.add(member);
      result = executeAndBuildResult(resultTable, dsMembers);
    } else if (groups != null && groups.length > 0) {
      for (String group : groups) {
        dsMembers.addAll(cache.getDistributedSystem().getGroupMembers(group));
      }
      result = executeAndBuildResult(resultTable, dsMembers);

    } else {
      // gc on entire cluster
      // exclude locators
      dsMembers = CliUtil.getAllNormalMembers(cache);
      result = executeAndBuildResult(resultTable, dsMembers);

    }
    return result;
  }

  private Result executeAndBuildResult(TabularResultData resultTable,
      Set<DistributedMember> dsMembers) {

    List<?> resultList;
    Function garbageCollectionFunction = new GarbageCollectionFunction();
    resultList =
        (List<?>) CliUtil.executeFunction(garbageCollectionFunction, null, dsMembers).getResult();

    for (Object object : resultList) {
      if (object instanceof Exception) {
        LogWrapper.getInstance(CliUtil.getCacheIfExists(this::getCache))
            .fine("Exception in GC " + ((Throwable) object).getMessage(), ((Throwable) object));
        continue;
      } else if (object instanceof Throwable) {
        LogWrapper.getInstance(CliUtil.getCacheIfExists(this::getCache))
            .fine("Exception in GC " + ((Throwable) object).getMessage(), ((Throwable) object));
        continue;
      }

      if (object != null) {
        if (object instanceof String) {
          // unexpected exception string - cache may be closed or something
          return ResultBuilder.createUserErrorResult((String) object);
        } else {
          Map<String, String> resultMap = (Map<String, String>) object;
          toTabularResultData(resultTable, resultMap.get("MemberId"),
              resultMap.get("HeapSizeBeforeGC"), resultMap.get("HeapSizeAfterGC"),
              resultMap.get("TimeSpentInGC"));
        }
      } else {
        LogWrapper.getInstance(CliUtil.getCacheIfExists(this::getCache))
            .fine("ResultMap was null ");
      }
    }

    return ResultBuilder.buildResult(resultTable);
  }

  private void toTabularResultData(TabularResultData table, String memberId, String heapSizeBefore,
      String heapSizeAfter, String timeTaken) {
    table.accumulate(CliStrings.GC__MSG__MEMBER_NAME, memberId);
    table.accumulate(CliStrings.GC__MSG__HEAP_SIZE_BEFORE_GC, heapSizeBefore);
    table.accumulate(CliStrings.GC__MSG__HEAP_SIZE_AFTER_GC, heapSizeAfter);
    table.accumulate(CliStrings.GC__MSG__TOTAL_TIME_IN_GC, timeTaken);
  }
}
