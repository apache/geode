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

import static org.apache.geode.management.internal.api.LocatorClusterManagementService.CMS_DLOCK_SERVICE_NAME;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.springframework.shell.core.Parser;
import org.springframework.shell.event.ParseResult;
import org.springframework.util.StringUtils;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.internal.CommandProcessor;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.CommandProcessingException;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;
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
public class OnlineCommandProcessor implements CommandProcessor {
  protected CommandExecutor executor;
  private GfshParser gfshParser;

  // Lock to synchronize getters & stop
  private final Object LOCK = new Object();

  private SecurityService securityService;

  public OnlineCommandProcessor() {}

  @VisibleForTesting
  public OnlineCommandProcessor(GfshParser gfshParser,
      SecurityService securityService,
      CommandExecutor commandExecutor) {
    this.gfshParser = gfshParser;
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
      String version = GemFireVersion.getGemFireVersion();
      String message = "Could not parse command string. " + command + "." + System.lineSeparator() +
          "The command or some options in this command may not be supported by this locator(" +
          version + ") gfsh is connected with.";
      return ResultModel.createError(message);
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

  @Override
  public String executeCommandReturningJson(String command, Map<String, String> env,
      List<String> stagedFilePaths) {
    return executeCommand(command, env, stagedFilePaths).toJson();
  }

  @Override
  public boolean init(Cache cache) {
    Properties cacheProperties = cache.getDistributedSystem().getProperties();
    this.securityService = ((InternalCache) cache).getSecurityService();
    this.gfshParser = new GfshParser(new CommandManager(cacheProperties, (InternalCache) cache));
    DistributedLockService cmsDlockService = DLockService.getOrCreateService(
        CMS_DLOCK_SERVICE_NAME,
        ((InternalCache) cache).getInternalDistributedSystem());
    this.executor = new CommandExecutor(cmsDlockService);

    return true;
  }

  @Override
  public Class<? extends CacheService> getInterface() {
    return CommandProcessor.class;
  }

  @Override
  public CacheServiceMBeanBase getMBean() {
    return null;
  }
}
