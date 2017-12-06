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

import org.apache.logging.log4j.Logger;
import org.springframework.shell.event.ParseResult;
import org.springframework.util.ReflectionUtils;

import org.apache.geode.SystemFailure;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.exceptions.UserErrorException;
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

  // used by the product
  public Object execute(ParseResult parseResult) {
    return execute(null, parseResult);
  }

  // used by the GfshParserRule to pass in a mock command
  public Object execute(Object command, ParseResult parseResult) {
    try {
      Object result = invokeCommand(command, parseResult);

      if (result == null) {
        return ResultBuilder.createGemFireErrorResult("Command returned null: " + parseResult);
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
      return ResultBuilder.createUserErrorResult(e.getMessage());
    }

    // if entity not found, depending on the thrower's intention, report either as success or error
    // no need to log since this is a user error
    catch (EntityNotFoundException e) {
      if (e.isStatusOK()) {
        return ResultBuilder.createInfoResult("Skipping: " + e.getMessage());
      } else {
        return ResultBuilder.createUserErrorResult(e.getMessage());
      }
    }

    // all other exceptions, log it and build an error result.
    catch (Exception e) {
      logger.error("Could not execute \"" + parseResult + "\".", e);
      return ResultBuilder.createGemFireErrorResult(
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

  protected Object invokeCommand(Object command, ParseResult parseResult) {
    // if no command instance is passed in, use the one in the parseResult
    if (command == null) {
      command = parseResult.getInstance();
    }
    return ReflectionUtils.invokeMethod(parseResult.getMethod(), command,
        parseResult.getArguments());
  }
}
