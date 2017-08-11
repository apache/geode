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

import org.apache.commons.lang.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.LogWriter;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.log4j.LogLevel;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.CliUtil;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.functions.ChangeLogLevelFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CompositeResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.cli.result.TabularResultData;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class ChangeLogLevelCommand implements GfshCommand {
  @CliCommand(value = CliStrings.CHANGE_LOGLEVEL, help = CliStrings.CHANGE_LOGLEVEL__HELP)
  @CliMetaData(relatedTopic = {CliStrings.TOPIC_LOGS},
      interceptor = "org.apache.geode.management.internal.cli.commands.ChangeLogLevelCommand$ChangeLogLevelCommandInterceptor")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.WRITE)
  public Result changeLogLevel(
      @CliOption(key = {CliStrings.MEMBER, CliStrings.MEMBERS},
          help = CliStrings.CHANGE_LOGLEVEL__MEMBER__HELP) String[] memberIds,
      @CliOption(key = {CliStrings.GROUP, CliStrings.GROUPS}, unspecifiedDefaultValue = "",
          help = CliStrings.CHANGE_LOGLEVEL__GROUPS__HELP) String[] grps,
      @CliOption(key = CliStrings.CHANGE_LOGLEVEL__LOGLEVEL,
          optionContext = ConverterHint.LOG_LEVEL, mandatory = true, unspecifiedDefaultValue = "",
          help = CliStrings.CHANGE_LOGLEVEL__LOGLEVEL__HELP) String logLevel) {
    try {
      if ((memberIds == null || memberIds.length == 0) && (grps == null || grps.length == 0)) {
        return ResultBuilder
            .createUserErrorResult(CliStrings.CHANGE_LOGLEVEL__MSG__SPECIFY_GRP_OR_MEMBER);
      }

      InternalCache cache = GemFireCacheImpl.getInstance();
      LogWriter logger = cache.getLogger();

      Set<DistributedMember> dsMembers = new HashSet<>();
      Set<DistributedMember> ds = CliUtil.getAllMembers(cache);

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
        return ResultBuilder.createGemFireErrorResult(CliStrings.CHANGE_LOGLEVEL__MSG_NO_MEMBERS);
      }

      Function logFunction = new ChangeLogLevelFunction();
      FunctionService.registerFunction(logFunction);
      Object[] functionArgs = new Object[1];
      functionArgs[0] = logLevel;

      CompositeResultData compositeResultData = ResultBuilder.createCompositeResultData();
      CompositeResultData.SectionResultData section = compositeResultData.addSection("section");
      TabularResultData resultTable = section.addTable("ChangeLogLevel");
      resultTable = resultTable.setHeader("Summary");

      Execution execution = FunctionService.onMembers(dsMembers).setArguments(functionArgs);
      if (execution == null) {
        return ResultBuilder.createUserErrorResult(CliStrings.CHANGE_LOGLEVEL__MSG__CANNOT_EXECUTE);
      }
      List<?> resultList = (List<?>) execution.execute(logFunction).getResult();

      for (Object object : resultList) {
        try {
          if (object instanceof Throwable) {
            logger.warning(
                "Exception in ChangeLogLevelFunction " + ((Throwable) object).getMessage(),
                ((Throwable) object));
            continue;
          }

          if (object != null) {
            Map<String, String> resultMap = (Map<String, String>) object;
            Map.Entry<String, String> entry = resultMap.entrySet().iterator().next();

            if (entry.getValue().contains("ChangeLogLevelFunction exception")) {
              resultTable.accumulate(CliStrings.CHANGE_LOGLEVEL__COLUMN_MEMBER, entry.getKey());
              resultTable.accumulate(CliStrings.CHANGE_LOGLEVEL__COLUMN_STATUS, "false");
            } else {
              resultTable.accumulate(CliStrings.CHANGE_LOGLEVEL__COLUMN_MEMBER, entry.getKey());
              resultTable.accumulate(CliStrings.CHANGE_LOGLEVEL__COLUMN_STATUS, "true");
            }

          }
        } catch (Exception ex) {
          LogWrapper.getInstance().warning("change log level command exception " + ex);
        }
      }

      Result result = ResultBuilder.buildResult(compositeResultData);
      logger.info("change log-level command result=" + result);
      return result;
    } catch (Exception ex) {
      GemFireCacheImpl.getInstance().getLogger().error("GFSH Changeloglevel exception: " + ex);
      return ResultBuilder.createUserErrorResult(ex.getMessage());
    }
  }

  public static class ChangeLogLevelCommandInterceptor extends AbstractCliAroundInterceptor {
    @Override
    public Result preExecution(GfshParseResult parseResult) {
      Map<String, String> arguments = parseResult.getParamValueStrings();
      // validate log level
      String logLevel = arguments.get("loglevel");
      if (StringUtils.isBlank(logLevel) || LogLevel.getLevel(logLevel) == null) {
        return ResultBuilder.createUserErrorResult("Invalid log level: " + logLevel);
      }

      return ResultBuilder.createInfoResult("");
    }
  }
}
