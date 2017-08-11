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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.domain.MemberResult;
import org.apache.geode.management.internal.cli.domain.SubscriptionQueueSizeResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;

public class DurableClientCommandsResultBuilder {
  public Result buildResult(List<MemberResult> results, String successHeader,
      String failureHeader) {
    Result result;
    boolean failure = true;
    boolean partialFailure = false;
    Map<String, List<String>> errorMap = new HashMap<>();
    Map<String, List<String>> successMap = new HashMap<>();
    Map<String, List<String>> exceptionMap = new HashMap<>();

    // Aggregate the results from the members
    for (MemberResult memberResult : results) {
      if (memberResult.isSuccessful()) {
        failure = false;
        groupByMessage(memberResult.getSuccessMessage(), memberResult.getMemberNameOrId(),
            successMap);
      } else {
        if (memberResult.isOpPossible()) {
          partialFailure = true;
          groupByMessage(memberResult.getExceptionMessage(), memberResult.getMemberNameOrId(),
              exceptionMap);
        } else {
          groupByMessage(memberResult.getErrorMessage(), memberResult.getMemberNameOrId(),
              errorMap);
        }
      }
    }

    if (!failure && !partialFailure) {
      result = ResultBuilder.buildResult(buildSuccessData(successMap));
    } else {
      result = ResultBuilder
          .buildResult(buildFailureData(successMap, exceptionMap, errorMap, failureHeader));
    }
    return result;
  }

  Result buildTableResultForQueueSize(List<SubscriptionQueueSizeResult> results,
      String queueSizeColumnName) {
    Result result;
    boolean failure = true;

    Map<String, List<String>> failureMap = new HashMap<>();
    Map<String, Long> memberQueueSizeTable = new TreeMap<>();

    // Aggregate the results from the members
    for (SubscriptionQueueSizeResult memberResult : results) {
      if (memberResult.isSuccessful()) {
        failure = false;
        memberQueueSizeTable.put(memberResult.getMemberNameOrId(),
            memberResult.getSubscriptionQueueSize());
      } else {
        groupByMessage(memberResult.getErrorMessage(), memberResult.getMemberNameOrId(),
            failureMap);
      }
    }

    if (!failure) {
      TabularResultData table = ResultBuilder.createTabularResultData();
      Set<String> members = memberQueueSizeTable.keySet();

      for (String member : members) {
        long queueSize = memberQueueSizeTable.get(member);
        table.accumulate(CliStrings.MEMBER, member);
        table.accumulate(queueSizeColumnName, queueSize);
      }
      result = ResultBuilder.buildResult(table);
    } else {
      ErrorResultData erd = ResultBuilder.createErrorResultData();
      buildErrorResult(erd, failureMap);
      result = ResultBuilder.buildResult(erd);
    }
    return result;
  }

  void groupByMessage(String message, String memberNameOrId, Map<String, List<String>> map) {
    List<String> members = map.get(message);
    if (members == null) {
      members = new LinkedList<>();
    }
    members.add(memberNameOrId);
    map.put(message, members);
  }


  private InfoResultData buildSuccessData(Map<String, List<String>> successMap) {
    InfoResultData ird = ResultBuilder.createInfoResultData();
    Set<String> successMessages = successMap.keySet();

    for (String successMessage : successMessages) {
      ird.addLine(CliStrings.format(CliStrings.ACTION_SUCCEEDED_ON_MEMBER, successMessage));
      List<String> successfulMembers = successMap.get(successMessage);
      int num = 0;
      for (String member : successfulMembers) {
        ird.addLine("" + ++num + "." + member);
      }
      ird.addLine("\n");
    }
    return ird;
  }

  ErrorResultData buildFailureData(Map<String, List<String>> successMap,
      Map<String, List<String>> exceptionMap, Map<String, List<String>> errorMap,
      String errorHeader) {
    ErrorResultData erd = ResultBuilder.createErrorResultData();
    buildErrorResult(erd, successMap);
    erd.addLine("\n");
    erd.addLine(errorHeader);
    buildErrorResult(erd, exceptionMap);
    buildErrorResult(erd, errorMap);
    return erd;
  }

  private void buildErrorResult(ErrorResultData erd, Map<String, List<String>> resultMap) {
    if (resultMap != null && !resultMap.isEmpty()) {
      Set<String> messages = resultMap.keySet();

      for (String message : messages) {
        erd.addLine("\n");
        erd.addLine(message);
        erd.addLine(CliStrings.OCCURRED_ON_MEMBERS);
        List<String> members = resultMap.get(message);
        int num = 0;
        for (String member : members) {
          ++num;
          erd.addLine("" + num + "." + member);
        }
      }
    }
  }
}
