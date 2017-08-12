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

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.domain.DurableCqNamesResult;
import org.apache.geode.management.internal.cli.domain.MemberResult;
import org.apache.geode.management.internal.cli.domain.SubscriptionQueueSizeResult;
import org.apache.geode.management.internal.cli.functions.CloseDurableClientFunction;
import org.apache.geode.management.internal.cli.functions.CloseDurableCqFunction;
import org.apache.geode.management.internal.cli.functions.GetSubscriptionQueueSizeFunction;
import org.apache.geode.management.internal.cli.functions.ListDurableCqNamesFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;

/**
 * The DurableClientCommands class encapsulates all GemFire shell (Gfsh) commands related to durable
 * clients and cqs defined in GemFire.
 * </p>
 */
@SuppressWarnings("unused")
public class DurableClientCommands implements GfshCommand {

  private static final ListDurableCqNamesFunction listDurableCqNamesFunction =
      new ListDurableCqNamesFunction();
  private static final CloseDurableClientFunction closeDurableClientFunction =
      new CloseDurableClientFunction();
  private static final CloseDurableCqFunction closeDurableCqFunction = new CloseDurableCqFunction();
  private static final GetSubscriptionQueueSizeFunction countDurableCqEvents =
      new GetSubscriptionQueueSizeFunction();

  @CliCommand(value = CliStrings.LIST_DURABLE_CQS, help = CliStrings.LIST_DURABLE_CQS__HELP)
  @CliMetaData()
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result listDurableClientCqs(
      @CliOption(key = CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID, mandatory = true,
          help = CliStrings.LIST_DURABLE_CQS__DURABLECLIENTID__HELP) final String durableClientId,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          help = CliStrings.LIST_DURABLE_CQS__MEMBER__HELP,
          optionContext = ConverterHint.MEMBERIDNAME) final String[] memberNameOrId,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.LIST_DURABLE_CQS__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) final String[] group) {
    Result result;
    try {

      boolean noResults = true;
      Set<DistributedMember> targetMembers = CliUtil.findMembers(group, memberNameOrId);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      final ResultCollector<?, ?> rc =
          CliUtil.executeFunction(new ListDurableCqNamesFunction(), durableClientId, targetMembers);
      final List<DurableCqNamesResult> results = (List<DurableCqNamesResult>) rc.getResult();
      Map<String, List<String>> memberCqNamesMap = new TreeMap<>();
      Map<String, List<String>> errorMessageNodes = new HashMap<>();
      Map<String, List<String>> exceptionMessageNodes = new HashMap<>();

      for (DurableCqNamesResult memberResult : results) {
        if (memberResult != null) {
          if (memberResult.isSuccessful()) {
            memberCqNamesMap.put(memberResult.getMemberNameOrId(), memberResult.getCqNamesList());
          } else {
            if (memberResult.isOpPossible()) {
              groupByMessage(memberResult.getExceptionMessage(), memberResult.getMemberNameOrId(),
                  exceptionMessageNodes);
            } else {
              groupByMessage(memberResult.getErrorMessage(), memberResult.getMemberNameOrId(),
                  errorMessageNodes);
            }
          }
        }
      }

      if (!memberCqNamesMap.isEmpty()) {
        TabularResultData table = ResultBuilder.createTabularResultData();
        Set<String> members = memberCqNamesMap.keySet();

        for (String member : members) {
          boolean isFirst = true;
          List<String> cqNames = memberCqNamesMap.get(member);
          for (String cqName : cqNames) {
            if (isFirst) {
              isFirst = false;
              table.accumulate(CliStrings.MEMBER, member);
            } else {
              table.accumulate(CliStrings.MEMBER, "");
            }
            table.accumulate(CliStrings.LIST_DURABLE_CQS__NAME, cqName);
          }
        }
        result = ResultBuilder.buildResult(table);
      } else {
        String errorHeader =
            CliStrings.format(CliStrings.LIST_DURABLE_CQS__FAILURE__HEADER, durableClientId);
        result = ResultBuilder.buildResult(
            buildFailureData(null, exceptionMessageNodes, errorMessageNodes, errorHeader));
      }
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    }

    return result;
  }

  @CliCommand(value = CliStrings.COUNT_DURABLE_CQ_EVENTS,
      help = CliStrings.COUNT_DURABLE_CQ_EVENTS__HELP)
  @CliMetaData()
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public Result countDurableCqEvents(
      @CliOption(key = CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID, mandatory = true,
          help = CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CLIENT__ID__HELP) final String durableClientId,
      @CliOption(key = CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CQ__NAME,
          help = CliStrings.COUNT_DURABLE_CQ_EVENTS__DURABLE__CQ__NAME__HELP) final String cqName,
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          help = CliStrings.COUNT_DURABLE_CQ_EVENTS__MEMBER__HELP,
          optionContext = ConverterHint.MEMBERIDNAME) final String[] memberNameOrId,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.COUNT_DURABLE_CQ_EVENTS__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) final String[] group) {

    Result result;
    try {
      Set<DistributedMember> targetMembers = CliUtil.findMembers(group, memberNameOrId);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      String[] params = new String[2];
      params[0] = durableClientId;
      params[1] = cqName;
      final ResultCollector<?, ?> rc =
          CliUtil.executeFunction(new GetSubscriptionQueueSizeFunction(), params, targetMembers);
      final List<SubscriptionQueueSizeResult> funcResults =
          (List<SubscriptionQueueSizeResult>) rc.getResult();

      String queueSizeColumnName;

      if (cqName != null && !cqName.isEmpty()) {
        queueSizeColumnName = CliStrings
            .format(CliStrings.COUNT_DURABLE_CQ_EVENTS__SUBSCRIPTION__QUEUE__SIZE__CLIENT, cqName);
      } else {
        queueSizeColumnName = CliStrings.format(
            CliStrings.COUNT_DURABLE_CQ_EVENTS__SUBSCRIPTION__QUEUE__SIZE__CLIENT, durableClientId);
      }
      result = buildTableResultForQueueSize(funcResults, queueSizeColumnName);
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    }

    return result;
  }

  @CliCommand(value = CliStrings.CLOSE_DURABLE_CLIENTS,
      help = CliStrings.CLOSE_DURABLE_CLIENTS__HELP)
  @CliMetaData()
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.QUERY)
  public Result closeDurableClient(
      @CliOption(key = CliStrings.CLOSE_DURABLE_CLIENTS__CLIENT__ID, mandatory = true,
          help = CliStrings.CLOSE_DURABLE_CLIENTS__CLIENT__ID__HELP) final String durableClientId,
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          help = CliStrings.CLOSE_DURABLE_CLIENTS__MEMBER__HELP,
          optionContext = ConverterHint.MEMBERIDNAME) final String[] memberNameOrId,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.COUNT_DURABLE_CQ_EVENTS__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) final String[] group) {

    Result result;
    try {

      Set<DistributedMember> targetMembers = CliUtil.findMembers(group, memberNameOrId);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      final ResultCollector<?, ?> rc =
          CliUtil.executeFunction(new CloseDurableClientFunction(), durableClientId, targetMembers);
      final List<MemberResult> results = (List<MemberResult>) rc.getResult();
      String failureHeader =
          CliStrings.format(CliStrings.CLOSE_DURABLE_CLIENTS__FAILURE__HEADER, durableClientId);
      String successHeader =
          CliStrings.format(CliStrings.CLOSE_DURABLE_CLIENTS__SUCCESS, durableClientId);
      result = buildResult(results, successHeader, failureHeader);
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    }
    return result;
  }


  @CliCommand(value = CliStrings.CLOSE_DURABLE_CQS, help = CliStrings.CLOSE_DURABLE_CQS__HELP)
  @CliMetaData()
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.MANAGE,
      target = Target.QUERY)
  public Result closeDurableCqs(@CliOption(key = CliStrings.CLOSE_DURABLE_CQS__DURABLE__CLIENT__ID,
      mandatory = true,
      help = CliStrings.CLOSE_DURABLE_CQS__DURABLE__CLIENT__ID__HELP) final String durableClientId,

      @CliOption(key = CliStrings.CLOSE_DURABLE_CQS__NAME, mandatory = true,
          help = CliStrings.CLOSE_DURABLE_CQS__NAME__HELP) final String cqName,

      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          help = CliStrings.CLOSE_DURABLE_CQS__MEMBER__HELP,
          optionContext = ConverterHint.MEMBERIDNAME) final String[] memberNameOrId,

      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          help = CliStrings.CLOSE_DURABLE_CQS__GROUP__HELP,
          optionContext = ConverterHint.MEMBERGROUP) final String[] group) {
    Result result;
    try {
      Set<DistributedMember> targetMembers = CliUtil.findMembers(group, memberNameOrId);

      if (targetMembers.isEmpty()) {
        return ResultBuilder.createUserErrorResult(CliStrings.NO_MEMBERS_FOUND_MESSAGE);
      }

      String[] params = new String[2];
      params[0] = durableClientId;
      params[1] = cqName;

      final ResultCollector<?, ?> rc =
          CliUtil.executeFunction(new CloseDurableCqFunction(), params, targetMembers);
      final List<MemberResult> results = (List<MemberResult>) rc.getResult();
      String failureHeader =
          CliStrings.format(CliStrings.CLOSE_DURABLE_CQS__FAILURE__HEADER, cqName, durableClientId);
      String successHeader =
          CliStrings.format(CliStrings.CLOSE_DURABLE_CQS__SUCCESS, cqName, durableClientId);
      result = buildResult(results, successHeader, failureHeader);
    } catch (Exception e) {
      result = ResultBuilder.createGemFireErrorResult(e.getMessage());
    }
    return result;
  }


  private Result buildResult(List<MemberResult> results, String successHeader,
      String failureHeader) {
    Result result;
    boolean failure = true;
    boolean partialFailure = false;
    Map<String, List<String>> errorMap = new HashMap<>();
    Map<String, List<String>> successMap = new HashMap<>();
    Map<String, List<String>> exceptionMap = new HashMap<>();

    /*
     * Aggregate the results from the members
     */
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

  private Result buildTableResultForQueueSize(List<SubscriptionQueueSizeResult> results,
      String queueSizeColumnName) {
    Result result;
    boolean failure = true;

    Map<String, List<String>> failureMap = new HashMap<>();
    Map<String, Long> memberQueueSizeTable = new TreeMap<>();

    /*
     * Aggregate the results from the members
     */
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

  private void groupByMessage(String message, String memberNameOrId,
      Map<String, List<String>> map) {
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

  private ErrorResultData buildFailureData(Map<String, List<String>> successMap,
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

