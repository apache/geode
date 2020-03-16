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

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.logging.internal.log4j.LogLevel;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.ChangeLogLevelFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ChangeLogLevelCommand extends GfshCommand {
  private static final Logger logger = LogService.getLogger();

  @CliCommand(value = CliStrings.CHANGE_LOGLEVEL, help = CliStrings.CHANGE_LOGLEVEL__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_LOGS},
      interceptor = "org.apache.geode.management.internal.cli.commands.ChangeLogLevelCommand$ChangeLogLevelCommandInterceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.WRITE)
  public ResultModel changeLogLevel(
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          help = CliStrings.CHANGE_LOGLEVEL__MEMBER__HELP) String[] memberIds,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS}, unspecifiedDefaultValue = "",
          help = CliStrings.CHANGE_LOGLEVEL__GROUPS__HELP) String[] grps,
      @CliOption(key = CliStrings.CHANGE_LOGLEVEL__LOGLEVEL,
          optionContext = ConverterHint.LOG_LEVEL, mandatory = true, unspecifiedDefaultValue = "",
          help = CliStrings.CHANGE_LOGLEVEL__LOGLEVEL__HELP) String logLevel) {

    if ((memberIds == null || memberIds.length == 0) && (grps == null || grps.length == 0)) {
      return ResultModel.createError(CliStrings.CHANGE_LOGLEVEL__MSG__SPECIFY_GRP_OR_MEMBER);
    }

    Cache cache = getCache();

    Set<DistributedMember> dsMembers = new HashSet<>();
    Set<DistributedMember> ds = getAllMembers();

    if (grps != null && grps.length > 0) {
      for (String grp : grps) {
        dsMembers.addAll(cache.getDistributedSystem().getGroupMembers(grp));
      }
    }

    if (memberIds != null && memberIds.length > 0) {
      for (String member : memberIds) {
        for (DistributedMember mem : ds) {
          if (mem.getName() != null
              && (mem.getName().equals(member) || mem.getId().equals(member))) {
            dsMembers.add(mem);
            break;
          }
        }
      }
    }

    if (dsMembers.size() == 0) {
      return ResultModel.createError(
          "No members were found matching the given member IDs or groups.");
    }

    ChangeLogLevelFunction logFunction = new ChangeLogLevelFunction();
    FunctionService.registerFunction(logFunction);
    Object[] functionArgs = new Object[1];
    functionArgs[0] = logLevel;

    ResultModel result = new ResultModel();

    @SuppressWarnings("unchecked")
    Execution<?, ?, ?> execution = FunctionService.onMembers(dsMembers).setArguments(functionArgs);
    if (execution == null) {
      return ResultModel.createError(CliStrings.CHANGE_LOGLEVEL__MSG__CANNOT_EXECUTE);
    }
    List<?> resultList =
        (List<?>) this.executeFunction(logFunction, functionArgs, dsMembers).getResult();


    TabularResultModel tableInfo = result.addTable("result");
    tableInfo.setColumnHeader(CliStrings.CHANGE_LOGLEVEL__COLUMN_MEMBER,
        CliStrings.CHANGE_LOGLEVEL__COLUMN_STATUS);
    for (Object object : resultList) {
      try {
        if (object instanceof Throwable) {
          logger.warn("Exception in ChangeLogLevelFunction " + ((Throwable) object).getMessage(),
              ((Throwable) object));
          continue;
        }

        if (object != null) {
          @SuppressWarnings("unchecked")
          Map<String, String> resultMap = (Map<String, String>) object;
          Map.Entry<String, String> entry = resultMap.entrySet().iterator().next();

          if (entry.getValue().contains("ChangeLogLevelFunction exception")) {
            tableInfo.addRow(entry.getKey(), "false");
          } else {
            tableInfo.addRow(entry.getKey(), "true");
          }

        }
      } catch (Exception ex) {
        logger.warn("change log level command exception " + ex);
      }
    }

    logger.info("change log-level command result=" + result);
    return result;

  }

  public static class ChangeLogLevelCommandInterceptor extends AbstractCliAroundInterceptor {
    @Override
    public ResultModel preExecution(GfshParseResult parseResult) {
      // validate log level
      String logLevel = parseResult.getParamValueAsString("loglevel");
      if (StringUtils.isBlank(logLevel) || LogLevel.getLevel(logLevel) == null) {
        return ResultModel.createError("Invalid log level: " + logLevel);
      }

      return ResultModel.createInfo("");
    }
  }
}
