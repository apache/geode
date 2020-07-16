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
package org.apache.geode.management.internal.cli.shell;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.springframework.shell.core.ExecutionStrategy;
import org.springframework.shell.core.Shell;
import org.springframework.shell.event.ParseResult;
import org.springframework.util.Assert;

import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result.Status;
import org.apache.geode.management.internal.cli.CliAroundInterceptor;
import org.apache.geode.management.internal.cli.CommandRequest;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.LogWrapper;
import org.apache.geode.management.internal.cli.remote.CommandExecutor;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.security.NotAuthorizedException;

/**
 * Defines the {@link ExecutionStrategy} for commands that are executed in GemFire Shell (gfsh).
 *
 * @since GemFire 7.0
 */
public class GfshExecutionStrategy implements ExecutionStrategy {
  private Class<?> mutex = GfshExecutionStrategy.class;
  private Gfsh shell;
  private LogWrapper logWrapper;

  GfshExecutionStrategy(Gfsh shell) {
    this.shell = shell;
    this.logWrapper = shell.getGfshFileLogger();
  }

  /**
   * Executes the method indicated by the {@link ParseResult} which would always be
   * {@link GfshParseResult} for GemFire defined commands. If the command Method is decorated with
   * {@link CliMetaData#shellOnly()} set to <code>false</code>, {@link OperationInvoker} is used to
   * send the command for processing on a remote GemFire node.
   *
   * @param parseResult that should be executed (never presented as null)
   * @return an object which will be rendered by the {@link Shell} implementation (may return null)
   *         this returns a CommandResult for all the online commands. For offline-commands,
   *         this can return either a CommandResult or ExitShellRequest
   * @throws RuntimeException which is handled by the {@link Shell} implementation
   */
  @Override
  public Object execute(ParseResult parseResult) {
    Method method = parseResult.getMethod();

    // check if it's a shell only command
    if (isShellOnly(method)) {
      Assert.notNull(parseResult, "Parse result required");
      synchronized (mutex) {
        Assert.isTrue(isReadyForCommands(), "Not yet ready for commands");

        Object exeuctionResult = new CommandExecutor(null).execute((GfshParseResult) parseResult);
        if (exeuctionResult instanceof ResultModel) {
          return new CommandResult((ResultModel) exeuctionResult);
        }
        return exeuctionResult;

      }
    }

    // check if it's a GfshParseResult
    if (!GfshParseResult.class.isInstance(parseResult)) {
      throw new IllegalStateException("Configuration error!");
    }

    ResultModel resultModel = executeOnRemote((GfshParseResult) parseResult);

    if (resultModel == null) {
      return null;
    }
    return new CommandResult(resultModel);
  }

  /**
   * Whether the command is available only at the shell or on GemFire member too.
   *
   * @param method the method to check the associated annotation
   * @return true if CliMetaData is added to the method & CliMetaData.shellOnly is set to true,
   *         false otherwise
   */
  private boolean isShellOnly(Method method) {
    CliMetaData cliMetadata = method.getAnnotation(CliMetaData.class);
    return cliMetadata != null && cliMetadata.shellOnly();
  }

  private String getInterceptor(Method method) {
    CliMetaData cliMetadata = method.getAnnotation(CliMetaData.class);
    return cliMetadata != null ? cliMetadata.interceptor() : CliMetaData.ANNOTATION_NULL_VALUE;
  }

  /**
   * Indicates commands are able to be presented. This generally means all important system startup
   * activities have completed. Copied from {@link ExecutionStrategy#isReadyForCommands()}.
   *
   * @return whether commands can be presented for processing at this time
   */
  @Override
  public boolean isReadyForCommands() {
    return true;
  }

  /**
   * Indicates the execution runtime should be terminated. This allows it to cleanup before
   * returning control flow to the caller. Necessary for clean shutdowns. Copied from
   * {@link ExecutionStrategy#terminate()}.
   */
  @Override
  public void terminate() {
    shell = null;
  }

  /**
   * Sends the user input (command string) via {@link OperationInvoker} to a remote GemFire node for
   * processing & execution.
   *
   * @return result of execution/processing of the command
   * @throws IllegalStateException if gfsh doesn't have an active connection.
   */
  private ResultModel executeOnRemote(GfshParseResult parseResult) {
    Path tempFile = null;

    if (!shell.isConnectedAndReady()) {
      shell.logWarning(
          "Can't execute a remote command without connection. Use 'connect' first to connect.",
          null);
      logWrapper.info("Can't execute a remote command \"" + parseResult.getUserInput()
          + "\" without connection. Use 'connect' first to connect to GemFire.");
      return null;
    }

    List<File> fileData = null;
    CliAroundInterceptor interceptor = null;

    String interceptorClass = getInterceptor(parseResult.getMethod());

    // 1. Pre Remote Execution
    if (!CliMetaData.ANNOTATION_NULL_VALUE.equals(interceptorClass)) {
      try {
        interceptor = (CliAroundInterceptor) ClassPathLoader.getLatest().forName(interceptorClass)
            .newInstance();
      } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
        shell.logWarning("Configuration error", e);
      }

      if (interceptor == null) {
        return ResultModel.createError("Interceptor Configuration Error");
      }

      ResultModel preExecResult = interceptor.preExecution(parseResult);
      if (preExecResult.getStatus() != Status.OK) {
        return preExecResult;
      }
      fileData = preExecResult.getFileList();
    }

    // 2. Remote Execution
    Object response = null;
    final Map<String, String> env = shell.getEnv();
    try {
      response = shell.getOperationInvoker()
          .processCommand(new CommandRequest(parseResult, env, fileData));

      if (response == null) {
        return ResultModel.createError("Response was null for: " + parseResult.getUserInput());
      }
    } catch (NotAuthorizedException e) {
      return ResultModel.createError("Unauthorized. Reason : " + e.getMessage());
    } catch (Exception e) {
      shell.logSevere(e.getMessage(), e);
      return ResultModel.createError(
          "Error occurred while executing \"" + parseResult.getUserInput() + "\" on manager.");
    } finally {
      env.clear();
    }

    // the response could be a string which is a json representation of the
    // ResultModel object
    // it can also be a Path to a temp file downloaded from the rest http request
    ResultModel commandResult = null;
    if (response instanceof String) {
      try {
        commandResult = ResultModel.fromJson((String) response);
      } catch (Exception e) {
        logWrapper.severe("Unable to parse the remote response.", e);
        String clientVersion = GemFireVersion.getGemFireVersion();
        String remoteVersion = null;
        try {
          remoteVersion = shell.getOperationInvoker().getRemoteVersion();
        } catch (Exception exception) {
          // unable to get the remote version (pre-1.5.0 manager does not have this capability)
          // ignore
        }
        String message = "Unable to parse the remote response. This might due to gfsh client "
            + "version(" + clientVersion + ") mismatch with the remote cluster version"
            + ((remoteVersion == null) ? "." : "(" + remoteVersion + ").");
        return ResultModel.createError(message);
      }
    } else if (response instanceof Path) {
      tempFile = (Path) response;
    }

    // 3. Post Remote Execution
    if (interceptor != null) {
      try {
        commandResult =
            interceptor.postExecution(parseResult, commandResult, tempFile);

      } catch (Exception e) {
        logWrapper.severe("error running post interceptor", e);
        commandResult = ResultModel.createError(e.getMessage());
      }
    }

    if (commandResult == null) {
      commandResult =
          ResultModel.createError("Unable to build ResultModel using the remote response.");
    }

    return commandResult;
  }
}
