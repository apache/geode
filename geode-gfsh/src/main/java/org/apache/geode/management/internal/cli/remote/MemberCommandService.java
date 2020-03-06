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

import org.apache.geode.cache.internal.CommandProcessor;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.cli.CommandServiceException;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.model.ResultModel;

/**
 * @deprecated since 1.3 use OnlineCommandProcessor directly
 */
@Deprecated
public class MemberCommandService extends org.apache.geode.management.cli.CommandService {
  private final Object modLock = new Object();

  private InternalCache cache;
  private CommandProcessor commandProcessor;

  public MemberCommandService(InternalCache cache) throws CommandServiceException {
    this.cache = cache;
    try {
      this.commandProcessor = cache.getService(CommandProcessor.class);
      this.commandProcessor.init(cache);
    } catch (Exception e) {
      throw new CommandServiceException("Could not load commands.", e);
    }
  }

  @Override
  public Result processCommand(String commandString) {
    return this.processCommand(commandString, EMPTY_ENV);
  }

  @Override
  public Result processCommand(String commandString, Map<String, String> env) {
    ResultModel resultModel = ResultModel.fromJson(
        commandProcessor.executeCommandReturningJson(commandString, env, null));
    return new CommandResult(resultModel);
  }

  @Override
  @Deprecated
  public org.apache.geode.management.cli.CommandStatement createCommandStatement(
      String commandString) {
    return this.createCommandStatement(commandString, EMPTY_ENV);
  }

  @Override
  @Deprecated
  public org.apache.geode.management.cli.CommandStatement createCommandStatement(
      String commandString, Map<String, String> env) {
    if (!isUsable()) {
      throw new IllegalStateException("Cache instance is not available.");
    }
    synchronized (modLock) {
      return new CommandStatementImpl(commandString, env, commandProcessor);
    }
  }

  @Override
  public boolean isUsable() {
    return (this.cache != null && !this.cache.isClosed());
  }

}
