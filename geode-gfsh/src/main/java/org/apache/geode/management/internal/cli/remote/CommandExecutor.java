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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.springframework.util.ReflectionUtils;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.util.ArgumentRedactor;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.cli.UpdateAllConfigurationGroupsMarker;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.exceptions.UserErrorException;
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

  /**
   *
   * @return always return ResultModel for online command, for offline command, return either
   *         ResultModel or ExitShellRequest
   */
  public Object execute(GfshParseResult parseResult) {
    return execute(null, parseResult);
  }

  /**
   * @return always return ResultModel for online command, for offline command, return either
   *         ResultModel or ExitShellRequest
   */
  @VisibleForTesting
  @SuppressWarnings("deprecation")
  public Object execute(Object command, GfshParseResult parseResult) {
    String userInput = parseResult.getUserInput();
    if (userInput != null) {
      logger.info("Executing command: " + ArgumentRedactor.redact(userInput));
    }

    try {
      Object result = invokeCommand(command, parseResult);

      // if some custom command returns Result instead of ResultModel, we need to turn that
      // into ResultModel in order to be processed later on.
      if (result instanceof CommandResult) {
        result = ((CommandResult) result).getResultData();
      } else if (result instanceof Result) {
        Result customResult = (Result) result;
        result = new ResultModel();
        InfoResultModel info = ((ResultModel) result).addInfo();
        while (customResult.hasNextLine()) {
          info.addLine(customResult.nextLine());
        }
        customResult.resetToFirstLine();
      }

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
      org.apache.geode.SystemFailure.initiateFailure(e);
      throw e;
    } catch (Throwable t) {
      org.apache.geode.SystemFailure.checkFailure();
      throw t;
    }
  }

  protected Object callInvokeMethod(Object command, GfshParseResult parseResult) {
    return ReflectionUtils.invokeMethod(parseResult.getMethod(), command,
        parseResult.getArguments());
  }

  protected Object invokeCommand(Object command, GfshParseResult parseResult) {
    // if no command instance is passed in, use the one in the parseResult
    if (command == null) {
      command = parseResult.getInstance();
    }

    Object result = callInvokeMethod(command, parseResult);

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
        gfshCommand.getConfigurationPersistenceService();
    if (ccService == null) {
      infoResultModel.addLine(SERVICE_NOT_RUNNING_CHANGE_NOT_PERSISTED);
      return resultModel;
    }

    if (parseResult.getParamValue("member") != null) {
      infoResultModel.addLine(RUN_ON_MEMBER_CHANGE_NOT_PERSISTED);
      return resultModel;
    }

    List<String> groupsToUpdate;
    String groupInput = parseResult.getParamValueAsString("group");

    if (!StringUtils.isBlank(groupInput)) {
      groupsToUpdate = Arrays.asList(groupInput.split(","));
    } else if (gfshCommand instanceof UpdateAllConfigurationGroupsMarker) {
      groupsToUpdate = new ArrayList<>(ccService.getGroups());
    } else {
      groupsToUpdate = Collections.singletonList("cluster");
    }

    for (String group : groupsToUpdate) {
      ccService.updateCacheConfig(group, cacheConfig -> {
        try {
          if (gfshCommand.updateConfigForGroup(group, cacheConfig, resultModel.getConfigObject())) {
            infoResultModel
                .addLine("Cluster configuration for group '" + group + "' is updated.");
          } else {
            infoResultModel
                .addLine("Cluster configuration for group '" + group + "' is not updated.");
          }
        } catch (Exception e) {
          String message = "Failed to update cluster configuration for " + group + ".";
          logger.error(message, e);
          // for now, if one cc update failed, we will set this flag. Will change this when we can
          // add lines to the result returned by the command
          infoResultModel.addLine(message + ". Reason: " + e.getMessage());
          return null;
        }
        return cacheConfig;
      });
    }

    return resultModel;
  }
}
