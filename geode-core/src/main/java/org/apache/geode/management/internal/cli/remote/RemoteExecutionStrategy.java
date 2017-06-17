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

import org.apache.commons.lang.StringUtils;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.management.ManagementService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.CliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.multistep.MultiStepCommand;
import org.apache.geode.management.internal.cli.result.FileResult;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.springframework.shell.event.ParseResult;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * 
 * 
 * @since GemFire 7.0
 */
// Doesn't have to be org.springframework.roo.shell.ExecutionStrategy
public class RemoteExecutionStrategy {
  private LogWrapper logWrapper = LogWrapper.getInstance();

  public Object execute(ParseResult parseResult) throws RuntimeException {
    Result result = null;
    try {

      Assert.notNull(parseResult, "Parse result required");
      if (!GfshParseResult.class.isInstance(parseResult)) {
        // Remote command means implemented for Gfsh and ParseResult should be GfshParseResult.
        // TODO: should this message be more specific?
        throw new IllegalArgumentException("Command Configuration/Definition error.");
      }

      GfshParseResult gfshParseResult = (GfshParseResult) parseResult;

      Method method = gfshParseResult.getMethod();

      if (!isShellOnly(method, gfshParseResult)) {
        Boolean fromShell = CommandExecutionContext.isShellRequest();
        boolean sentFromShell = fromShell != null && fromShell.booleanValue();
        String interceptorClass = getInterceptor(gfshParseResult.getMethod());
        CliAroundInterceptor interceptor = null;

        // 1. Pre Execution
        if (!sentFromShell && !CliMetaData.ANNOTATION_NULL_VALUE.equals(interceptorClass)) {
          try {
            interceptor = (CliAroundInterceptor) ClassPathLoader.getLatest()
                .forName(interceptorClass).newInstance();
          } catch (InstantiationException e) {
            logWrapper.info(e.getMessage());
          } catch (IllegalAccessException e) {
            logWrapper.info(e.getMessage());
          } catch (ClassNotFoundException e) {
            logWrapper.info(e.getMessage());
          }
          if (interceptor != null) {
            Result preExecResult = interceptor.preExecution(gfshParseResult);
            if (Status.ERROR.equals(preExecResult.getStatus())) {
              return preExecResult;
            } else if (preExecResult instanceof FileResult) {
              FileResult fileResult = (FileResult) preExecResult;
              byte[][] fileData = fileResult.toBytes();
              CommandExecutionContext.setBytesFromShell(fileData);
            }
          } else {
            return ResultBuilder
                .createBadConfigurationErrorResult("Interceptor Configuration Error");
          }
        }
        logWrapper.info("Executing " + gfshParseResult.getUserInput());

        result = (Result) ReflectionUtils.invokeMethod(gfshParseResult.getMethod(),
            gfshParseResult.getInstance(), gfshParseResult.getArguments());

        if (result != null && Status.ERROR.equals(result.getStatus())) {
          logWrapper
              .info("Error occurred while executing \"" + gfshParseResult.getUserInput() + "\".");
        }

        if (interceptor != null) {
          Result postExecResult = interceptor.postExecution(gfshParseResult, result, null);
          if (postExecResult != null) {
            if (Status.ERROR.equals(postExecResult.getStatus())) {
              logWrapper.warning(postExecResult.toString(), null);
            } else if (logWrapper.fineEnabled()) {
              logWrapper.fine(String.valueOf(postExecResult));
            }
            result = postExecResult;
          }
          CommandExecutionContext.setBytesFromShell(null); // for remote commands with bytes
        }
      } else {
        throw new IllegalArgumentException(
            "Only Remote command can be executed through " + ManagementService.class.getSimpleName()
                + ".processCommand() or ManagementMBean's processCommand "
                + "operation. Please refer documentation for the list of " + "commands.");
      }
    } catch (RuntimeException e) {
      throw e;
    }
    return result;
  }

  private boolean isShellOnly(Method method, GfshParseResult result) {
    CliMetaData cliMetadata = method.getAnnotation(CliMetaData.class);
    if (cliMetadata == null) {
      return false;
    }
    if (cliMetadata.shellOnly()) {
      return true;
    }
    MultiStepCommand stepCommand = method.getAnnotation(MultiStepCommand.class);
    if (stepCommand == null) {
      return false;
    }
    String step = result.getParamValue(MultiStepCommand.STEP_PARAMETER_NAME);
    if (StringUtils.isBlank(step)) {
      return false;
    }
    if (Arrays.asList(stepCommand.shellOnlyStep()).contains(step)) {
      return true;
    }
    return false;
  }

  private String getInterceptor(Method method) {
    CliMetaData cliMetadata = method.getAnnotation(CliMetaData.class);
    return cliMetadata != null ? cliMetadata.interceptor() : CliMetaData.ANNOTATION_NULL_VALUE;
  }

  public void terminate() {}
}
