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
package org.apache.geode.management.internal.cli.remote;

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Logger;
import org.springframework.util.ReflectionUtils;

import org.apache.geode.SystemFailure;
import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.exceptions.UserErrorException;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.security.NotAuthorizedException;

/**
 * this executes the command using method reflection. It logs all possible exceptions, and generates
 * GemfireErrorResult based on the exceptions.
 *
 * For AuthorizationExceptions, it logs it and then rethrow it.
 */
public class CommandExecutor {
  public static final String RUN_ON_MEMBER_CHANGE_NOT_PERSISTED =
      "Configuration change is not persisted because the command is executed on specific member.";
  public static final String SERVICE_NOT_RUNNING_CHANGE_NOT_PERSISTED =
      "Cluster configuration service is not running. Configuration change is not persisted.";

  private Logger logger = LogService.getLogger();

  // used by the product
  public Object execute(GfshParseResult parseResult) {
    return execute(null, parseResult);
  }

  // used by the GfshParserRule to pass in a mock command
  public Object execute(Object command, GfshParseResult parseResult) {
    try {
      Object result = invokeCommand(command, parseResult);

      if (result == null) {
        return ResultModel.createError("Command returned null: " + parseResult);
      }
      return result;
    }

    // for Authorization Exception, we need to throw them for higher level code to catch
    catch (NotAuthorizedException e) {
      logger.error("Not authorized to execute \"" + parseResult + "\".", e);
      throw e;
    }

    // for these exceptions, needs to create a UserErrorResult (still reported as error by gfsh)
    // no need to log since this is a user error
    catch (UserErrorException | IllegalStateException | IllegalArgumentException e) {
      return ResultModel.createError(e.getMessage());
    }

    // if entity not found, depending on the thrower's intention, report either as success or error
    // no need to log since this is a user error
    catch (EntityNotFoundException e) {
      if (e.isStatusOK()) {
        return ResultModel.createInfo("Skipping: " + e.getMessage());
      } else {
        return ResultModel.createError(e.getMessage());
      }
    }

    // all other exceptions, log it and build an error result.
    catch (Exception e) {
      logger.error("Could not execute \"" + parseResult + "\".", e);
      return ResultModel.createError(
          "Error while processing command <" + parseResult + "> Reason : " + e.getMessage());
    }

    // for errors more lower-level than Exception, just throw them.
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      SystemFailure.checkFailure();
      throw t;
    }
  }

  protected Object invokeCommand(Object command, GfshParseResult parseResult) {
    // if no command instance is passed in, use the one in the parseResult
    if (command == null) {
      command = parseResult.getInstance();
    }

    Object result =
        ReflectionUtils.invokeMethod(parseResult.getMethod(), command, parseResult.getArguments());

    if (!(command instanceof SingleGfshCommand)) {
      return result;
    }

    SingleGfshCommand gfshCommand = (SingleGfshCommand) command;
    ResultModel resultModel = (ResultModel) result;
    if (resultModel.getStatus() == Result.Status.ERROR) {
      return result;
    }

    // if command result is ok, we will need to see if we need to update cluster configuration
    InfoResultModel infoResultModel = resultModel.addInfo(ResultModel.INFO_SECTION);
    InternalConfigurationPersistenceService ccService =
        (InternalConfigurationPersistenceService) gfshCommand.getConfigurationPersistenceService();
    if (ccService == null) {
      infoResultModel.addLine(SERVICE_NOT_RUNNING_CHANGE_NOT_PERSISTED);
      return resultModel;
    }

    if (parseResult.getParamValue("member") != null) {
      infoResultModel.addLine(RUN_ON_MEMBER_CHANGE_NOT_PERSISTED);
      return resultModel;
    }

    String groupInput = parseResult.getParamValueAsString("group");
    if (groupInput == null) {
      groupInput = "cluster";
    }
    String[] groups = groupInput.split(",");
    for (String group : groups) {
      ccService.updateCacheConfig(group, cc -> {
        try {
          if (gfshCommand.updateConfigForGroup(group, cc, resultModel.getConfigObject())) {
            infoResultModel
                .addLine("Changes to configuration for group '" + group + "' are persisted.");
          } else {
            infoResultModel
                .addLine("No changes were made to the configuration for group '" + group + "'");
          }
        } catch (Exception e) {
          String message = "failed to update cluster config for " + group;
          logger.error(message, e);
          // for now, if one cc update failed, we will set this flag. Will change this when we can
          // add lines to the result returned by the command
          infoResultModel.addLine(message + ". Reason: " + e.getMessage());
          return null;
        }
        return cc;
      });
    }

    Map<String, CacheConfig> configMap = ccService.getGroups()
        .stream()
        .filter(group -> ccService.getCacheConfig(group) != null)
        .collect(Collectors.toMap(group -> group, group -> ccService.getCacheConfig(group)));

    if (gfshCommand.updateAllConfigs(configMap, resultModel)) {
      ccService.getGroups()
          .forEach(group -> ccService.replaceCacheConfig(group, configMap.get(group)));
    }

    return resultModel;
  }
}
