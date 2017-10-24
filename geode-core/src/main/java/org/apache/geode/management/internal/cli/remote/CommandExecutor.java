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

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.Logger;
import org.springframework.shell.event.ParseResult;
import org.springframework.util.ReflectionUtils;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.security.NotAuthorizedException;

/**
 * this executes the command using method reflection. It logs all possible exceptions, and generates
 * GemfireErrorResult based on the exceptions.
 *
 * For AuthorizationExceptions, it logs it and then rethrow it.
 */
public class CommandExecutor {
  private Logger logger = LogService.getLogger();

  public Object execute(ParseResult parseResult) {
    try {
      Object result = invokeCommand(parseResult);

      if (result == null) {
        return ResultBuilder.createGemFireErrorResult("Command returned null: " + parseResult);
      }
      return result;
    } catch (NotAuthorizedException e) {
      logger.error("Not authorized to execute \"" + parseResult + "\".", e);
      throw e;
    } catch (Exception e) {
      logger.error("Could not execute \"" + parseResult + "\".", e);
      return ResultBuilder.createGemFireErrorResult(
          "Error while processing command <" + parseResult + "> Reason : " + e.getMessage());
    }
  }

  protected Object invokeCommand(ParseResult parseResult) {
    return ReflectionUtils.invokeMethod(parseResult.getMethod(), parseResult.getInstance(),
        parseResult.getArguments());
  }

}
