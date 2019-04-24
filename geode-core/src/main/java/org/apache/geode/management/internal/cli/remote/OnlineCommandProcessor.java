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

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.springframework.shell.core.Parser;
import org.springframework.shell.event.ParseResult;
import org.springframework.util.StringUtils;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.CommandProcessingException;
import org.apache.geode.management.internal.cli.CommandManager;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.util.CommentSkipHelper;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

/**
 * @since GemFire 7.0
 */
public class OnlineCommandProcessor {
  protected final CommandExecutor executor;
  private final GfshParser gfshParser;

  // Lock to synchronize getters & stop
  private final Object LOCK = new Object();

  private final SecurityService securityService;

  public OnlineCommandProcessor(Properties cacheProperties, SecurityService securityService,
      InternalCache cache) {
    this(cacheProperties, securityService, new CommandExecutor(), cache);
  }

  @VisibleForTesting
  public OnlineCommandProcessor(Properties cacheProperties, SecurityService securityService,
      CommandExecutor commandExecutor, InternalCache cache) {
    this.gfshParser = new GfshParser(new CommandManager(cacheProperties, cache));
    this.executor = commandExecutor;
    this.securityService = securityService;
  }

  protected CommandExecutor getCommandExecutor() {
    synchronized (LOCK) {
      return executor;
    }
  }

  protected Parser getParser() {
    synchronized (LOCK) {
      return gfshParser;
    }
  }

  public ParseResult parseCommand(String commentLessLine)
      throws CommandProcessingException, IllegalStateException {
    if (commentLessLine != null) {
      return getParser().parse(commentLessLine);
    }
    throw new IllegalStateException("Command String should not be null.");
  }

  public ResultModel executeCommand(String command) {
    return executeCommand(command, Collections.emptyMap(), null);
  }

  public ResultModel executeCommand(String command, Map<String, String> env,
      List<String> stagedFilePaths) {
    CommentSkipHelper commentSkipper = new CommentSkipHelper();
    String commentLessLine = commentSkipper.skipComments(command);
    if (StringUtils.isEmpty(commentLessLine)) {
      return null;
    }

    CommandExecutionContext.setShellEnv(env);
    CommandExecutionContext.setFilePathToShell(stagedFilePaths);

    final CommandExecutor commandExecutor = getCommandExecutor();
    ParseResult parseResult = parseCommand(commentLessLine);

    if (parseResult == null) {
      return ResultModel.createError("Could not parse command string. " + command);
    }

    Method method = parseResult.getMethod();

    // do general authorization check here
    ResourceOperation resourceOperation = method.getAnnotation(ResourceOperation.class);
    if (resourceOperation != null) {
      this.securityService.authorize(resourceOperation.resource(), resourceOperation.operation(),
          resourceOperation.target(), ResourcePermission.ALL);
    }

    // this command processor does not execute commands that need fileData passed from client
    CliMetaData metaData = method.getAnnotation(CliMetaData.class);
    if (metaData != null && metaData.isFileUploaded() && stagedFilePaths == null) {
      return ResultModel
          .createError(command + " can not be executed only from server side");
    }

    // we can do a direct cast because this only process online commands
    return (ResultModel) commandExecutor.execute((GfshParseResult) parseResult);
  }
}
