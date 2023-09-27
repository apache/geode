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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.ConverterHint;
import org.apache.geode.management.cli.GfshCommand;
import org.apache.geode.management.internal.cli.functions.SetThreadLimitingProxyRequestFunction;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.i18n.CliStrings;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class SetThreadLimitingProxyRequestCommand extends GfshCommand {

  private static final Logger logger = LogService.getLogger();

  @CliAvailabilityIndicator({"set-thread-limit-proxy"})
  public boolean commandAvailable() {
    return isOnlineCommandAvailable();
  }

  @CliCommand(value = "set-thread-limit-proxy",
      help = "Set a maximum number of threads to be used to proxy operations to another server")
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)

  public ResultModel setThreadLimitingProxyRequest(
      @CliOption(key = "max-threads-to-destination",
          optionContext = ConverterHint.LOG_LEVEL, mandatory = true, unspecifiedDefaultValue = "0",
          help = "The maximum number of threads to a specific destination") int maxThreads) {


    Set<DistributedMember> dsMembers = getAllNormalMembers();

    SetThreadLimitingProxyRequestFunction logFunction = new SetThreadLimitingProxyRequestFunction();
    FunctionService.registerFunction(logFunction);
    Object[] functionArgs = new Object[1];
    functionArgs[0] = maxThreads;

    ResultModel result = new ResultModel();

    @SuppressWarnings("unchecked")
    Execution<?, ?, ?> execution = FunctionService.onMembers(dsMembers).setArguments(functionArgs);
    if (execution == null) {
      return ResultModel.createError(CliStrings.CHANGE_LOGLEVEL__MSG__CANNOT_EXECUTE);
    }
    List<?> resultList =
        (List<?>) executeFunction(logFunction, functionArgs, dsMembers).getResult();

    TabularResultModel tableInfo = result.addTable("result");
    tableInfo.setColumnHeader("Member",
        "thread limited");
    for (Object object : resultList) {
      try {
        if (object instanceof Throwable) {
          logger.warn("Exception in set thread limit for proxy requests "
              + ((Throwable) object).getMessage(),
              ((Throwable) object));
          continue;
        }

        if (object != null) {
          @SuppressWarnings("unchecked")
          Map<String, String> resultMap = (Map<String, String>) object;
          Map.Entry<String, String> entry = resultMap.entrySet().iterator().next();

          if (entry.getValue().contains("Exception")) {
            tableInfo.addRow(entry.getKey(), "false");
          } else {
            tableInfo.addRow(entry.getKey(), "true");
          }

        }
      } catch (Exception ex) {
        logger.warn("command exception " + ex);
      }
    }

    logger.info("command result=" + result);
    return result;

  }
}
