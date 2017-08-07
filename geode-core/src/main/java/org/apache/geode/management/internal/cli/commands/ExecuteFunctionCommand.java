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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.lang.BooleanUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.DistributedRegionMXBean;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.MBeanJMXAdapter;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.functions.UserFunctionExecution;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ErrorResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ExecuteFunctionCommand implements GfshCommand {
  @CliCommand(value = CliStrings.EXECUTE_FUNCTION, help = CliStrings.EXECUTE_FUNCTION__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_GEODE_FUNCTION})
  @ResourceOperation(resource = ResourcePermission.Resource.DATA,
      operation = ResourcePermission.Operation.WRITE)
  public Result executeFunction(
      // TODO: Add optioncontext for functionID
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__ID, mandatory = true,
          help = CliStrings.EXECUTE_FUNCTION__ID__HELP) String functionId,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS},
          optionContext = ConverterHint.MEMBERGROUP,
          help = CliStrings.EXECUTE_FUNCTION__ONGROUPS__HELP) String[] onGroups,
      @CliOption(key = CliStrings.MEMBER, optionContext = ConverterHint.MEMBERIDNAME,
          help = CliStrings.EXECUTE_FUNCTION__ONMEMBER__HELP) String onMember,
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__ONREGION,
          optionContext = ConverterHint.REGION_PATH,
          help = CliStrings.EXECUTE_FUNCTION__ONREGION__HELP) String onRegion,
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__ARGUMENTS,
          help = CliStrings.EXECUTE_FUNCTION__ARGUMENTS__HELP) String[] arguments,
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__RESULTCOLLECTOR,
          help = CliStrings.EXECUTE_FUNCTION__RESULTCOLLECTOR__HELP) String resultCollector,
      @CliOption(key = CliStrings.EXECUTE_FUNCTION__FILTER,
          help = CliStrings.EXECUTE_FUNCTION__FILTER__HELP) String filterString) {
    CompositeResultData executeFunctionResultTable = ResultBuilder.createCompositeResultData();
    TabularResultData resultTable = executeFunctionResultTable.addSection().addTable("Table1");
    String headerText = "Execution summary";
    resultTable.setHeader(headerText);
    ResultCollector resultCollectorInstance = null;
    Set<String> filters = new HashSet<>();
    Execution execution;
    if (functionId != null) {
      functionId = functionId.trim();
    }
    if (onRegion != null) {
      onRegion = onRegion.trim();
    }
    if (onMember != null) {
      onMember = onMember.trim();
    }
    if (filterString != null) {
      filterString = filterString.trim();
    }

    try {
      // validate otherwise return right away. no need to process anything
      if (functionId == null || functionId.length() == 0) {
        ErrorResultData errorResultData =
            ResultBuilder.createErrorResultData().setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
                .addLine(CliStrings.EXECUTE_FUNCTION__MSG__MISSING_FUNCTIONID);
        return ResultBuilder.buildResult(errorResultData);
      }

      if (moreThanOneIsTrue(onRegion != null, onMember != null, onGroups != null)) {
        // Provide Only one of region/member/groups
        ErrorResultData errorResultData =
            ResultBuilder.createErrorResultData().setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
                .addLine(CliStrings.EXECUTE_FUNCTION__MSG__OPTIONS);
        return ResultBuilder.buildResult(errorResultData);
      } else if ((onRegion == null || onRegion.length() == 0) && (filterString != null)) {
        ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
            .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT)
            .addLine(CliStrings.EXECUTE_FUNCTION__MSG__MEMBER_SHOULD_NOT_HAVE_FILTER_FOR_EXECUTION);
        return ResultBuilder.buildResult(errorResultData);
      }

      InternalCache cache = getCache();

      if (resultCollector != null) {
        resultCollectorInstance =
            (ResultCollector) ClassPathLoader.getLatest().forName(resultCollector).newInstance();
      }

      if (filterString != null && filterString.length() > 0) {
        filters.add(filterString);
      }

      if (onRegion == null && onMember == null && onGroups == null) {
        // run function on all the members excluding locators bug#46113
        // if user wish to execute on locator then he can choose --member or --group option
        Set<DistributedMember> dsMembers = CliUtil.getAllNormalMembers(cache);
        if (dsMembers.size() > 0) {
          new UserFunctionExecution();
          LogWrapper.getInstance().info(CliStrings
              .format(CliStrings.EXECUTE_FUNCTION__MSG__EXECUTING_0_ON_ENTIRE_DS, functionId));
          for (DistributedMember member : dsMembers) {
            executeAndGetResults(functionId, null, resultCollector, arguments, member, resultTable,
                null);
          }
          return ResultBuilder.buildResult(resultTable);
        } else {
          return ResultBuilder
              .createUserErrorResult(CliStrings.EXECUTE_FUNCTION__MSG__DS_HAS_NO_MEMBERS);
        }
      } else if (onRegion != null && onRegion.length() > 0) {
        if (cache.getRegion(onRegion) == null) {
          // find a member where region is present
          DistributedRegionMXBean bean = ManagementService.getManagementService(getCache())
              .getDistributedRegionMXBean(onRegion);
          if (bean == null) {
            bean = ManagementService.getManagementService(getCache())
                .getDistributedRegionMXBean(Region.SEPARATOR + onRegion);

            if (bean == null) {
              return ResultBuilder.createGemFireErrorResult(CliStrings
                  .format(CliStrings.EXECUTE_FUNCTION__MSG__MXBEAN_0_FOR_NOT_FOUND, onRegion));
            }
          }

          DistributedMember member = null;
          String[] membersName = bean.getMembers();
          Set<DistributedMember> dsMembers = CliUtil.getAllMembers(cache);
          Iterator it = dsMembers.iterator();
          boolean matchFound = false;

          if (membersName.length > 0) {
            while (it.hasNext() && !matchFound) {
              DistributedMember dsmember = (DistributedMember) it.next();
              for (String memberName : membersName) {
                if (MBeanJMXAdapter.getMemberNameOrId(dsmember).equals(memberName)) {
                  member = dsmember;
                  matchFound = true;
                  break;
                }
              }
            }
          }
          if (matchFound) {
            executeAndGetResults(functionId, filterString, resultCollector, arguments, member,
                resultTable, onRegion);
            return ResultBuilder.buildResult(resultTable);
          } else {
            return ResultBuilder.createGemFireErrorResult(CliStrings.format(
                CliStrings.EXECUTE_FUNCTION__MSG__NO_ASSOCIATED_MEMBER_REGION, " " + onRegion));
          }
        } else {
          execution = FunctionService.onRegion(cache.getRegion(onRegion));
          if (execution != null) {
            if (resultCollectorInstance != null) {
              execution = execution.withCollector(resultCollectorInstance);
            }
            if (filters.size() > 0) {
              execution = execution.withFilter(filters);
            }
            if (arguments != null && arguments.length > 0) {
              execution = execution.setArguments(arguments);
            }

            try {
              List<Object> results = (List<Object>) execution.execute(functionId).getResult();
              if (results.size() > 0) {
                StringBuilder strResult = new StringBuilder();
                for (Object obj : results) {
                  strResult.append(obj);
                }
                toTabularResultData(resultTable,
                    cache.getDistributedSystem().getDistributedMember().getId(),
                    strResult.toString());
              }
              return ResultBuilder.buildResult(resultTable);
            } catch (FunctionException e) {
              return ResultBuilder.createGemFireErrorResult(CliStrings.format(
                  CliStrings.EXECUTE_FUNCTION__MSG__ERROR_IN_EXECUTING_0_ON_REGION_1_DETAILS_2,
                  functionId, onRegion, e.getMessage()));
            }
          } else {
            return ResultBuilder.createGemFireErrorResult(CliStrings.format(
                CliStrings.EXECUTE_FUNCTION__MSG__ERROR_IN_EXECUTING_0_ON_REGION_1_DETAILS_2,
                functionId, onRegion,
                CliStrings.EXECUTE_FUNCTION__MSG__ERROR_IN_RETRIEVING_EXECUTOR));
          }
        }
      } else if (onGroups != null) {
        // execute on group members
        Set<DistributedMember> dsMembers = new HashSet<>();
        for (String grp : onGroups) {
          dsMembers.addAll(cache.getDistributedSystem().getGroupMembers(grp));
        }

        if (dsMembers.size() > 0) {
          for (DistributedMember member : dsMembers) {
            executeAndGetResults(functionId, filterString, resultCollector, arguments, member,
                resultTable, onRegion);
          }
          return ResultBuilder.buildResult(resultTable);
        } else {
          StringBuilder grps = new StringBuilder();
          for (String grp : onGroups) {
            grps.append(grp);
            grps.append(", ");
          }
          return ResultBuilder.createUserErrorResult(
              CliStrings.format(CliStrings.EXECUTE_FUNCTION__MSG__GROUPS_0_HAS_NO_MEMBERS,
                  grps.toString().substring(0, grps.toString().length() - 1)));
        }
      } else if (onMember != null && onMember.length() > 0) {
        DistributedMember member = CliUtil.getDistributedMemberByNameOrId(onMember); // fix for bug
        // 45658
        if (member != null) {
          executeAndGetResults(functionId, filterString, resultCollector, arguments, member,
              resultTable, onRegion);
        } else {
          toTabularResultData(resultTable, onMember, CliStrings
              .format(CliStrings.EXECUTE_FUNCTION__MSG__NO_ASSOCIATED_MEMBER + " " + onMember));
        }
        return ResultBuilder.buildResult(resultTable);
      }
    } catch (Exception e) {
      ErrorResultData errorResultData = ResultBuilder.createErrorResultData()
          .setErrorCode(ResultBuilder.ERRORCODE_DEFAULT).addLine(e.getMessage());
      return ResultBuilder.buildResult(errorResultData);
    }
    return null;
  }

  private boolean moreThanOneIsTrue(Boolean... values) {
    return Stream.of(values).mapToInt(BooleanUtils::toInteger).sum() > 1;
  }

  private void executeAndGetResults(String functionId, String filterString, String resultCollector,
      String[] arguments, DistributedMember member, TabularResultData resultTable,
      String onRegion) {
    StringBuilder resultMessage = new StringBuilder();
    try {
      Function function = new UserFunctionExecution();
      Object[] args = new Object[5];
      args[0] = functionId;
      if (filterString != null) {
        args[1] = filterString;
      }
      if (resultCollector != null) {
        args[2] = resultCollector;
      }
      if (arguments != null && arguments.length > 0) {
        args[3] = "";
        for (String str : arguments) {
          // send via CSV separated value format
          if (str != null) {
            args[3] = args[3] + str + ",";
          }
        }
      }
      args[4] = onRegion;

      Execution execution = FunctionService.onMember(member).setArguments(args);
      if (execution != null) {
        List<Object> results = (List<Object>) execution.execute(function).getResult();
        if (results != null) {
          for (Object resultObj : results) {
            if (resultObj != null) {
              if (resultObj instanceof String) {
                resultMessage.append(((String) resultObj));
              } else if (resultObj instanceof Exception) {
                resultMessage.append(((Exception) resultObj).getMessage());
              } else {
                resultMessage.append(resultObj);
              }
            }
          }
        }
        toTabularResultData(resultTable, member.getId(), resultMessage.toString());
      } else {
        toTabularResultData(resultTable, member.getId(),
            CliStrings.EXECUTE_FUNCTION__MSG__ERROR_IN_RETRIEVING_EXECUTOR);
      }
    } catch (Exception e) {
      resultMessage.append(CliStrings.format(
          CliStrings.EXECUTE_FUNCTION__MSG__COULD_NOT_EXECUTE_FUNCTION_0_ON_MEMBER_1_ERROR_2,
          functionId, member.getId(), e.getMessage()));
      toTabularResultData(resultTable, member.getId(), resultMessage.toString());
    }
  }

  private void toTabularResultData(TabularResultData table, String memberId, String memberResult) {
    table.accumulate("Member ID/Name", memberId);
    table.accumulate("Function Execution Result", memberResult);
  }
}
