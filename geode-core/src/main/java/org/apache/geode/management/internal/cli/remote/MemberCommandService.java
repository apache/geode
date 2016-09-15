/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli.remote;

import java.io.IOException;
import java.util.Map;

import org.apache.geode.cache.Cache;
import org.apache.geode.management.cli.CommandService;
import org.apache.geode.management.cli.CommandServiceException;
import org.apache.geode.management.cli.CommandStatement;
import org.apache.geode.management.cli.Result;

/**
 */
public class MemberCommandService extends CommandService {
  private final Object modLock = new Object();
  
  private Cache            cache;
  private CommandProcessor commandProcessor;

  public MemberCommandService(Cache cache) throws CommandServiceException {
    this.cache = cache;
    try {
      this.commandProcessor = new CommandProcessor(cache.getDistributedSystem().getProperties());
    } catch (ClassNotFoundException e) {
      throw new CommandServiceException("Could not load commands.", e);
    } catch (IOException e) {
      throw new CommandServiceException("Could not load commands.", e);
    } catch (IllegalStateException e) {
      throw new CommandServiceException(e.getMessage(), e);
    }
  }
  
  public Result processCommand(String commandString) {
    return this.processCommand(commandString, EMPTY_ENV);
  }
  
  public Result processCommand(String commandString, Map<String, String> env) {
    return createCommandStatement(commandString, env).process();
  }
  
  public CommandStatement createCommandStatement(String commandString) {
    return this.createCommandStatement(commandString, EMPTY_ENV);
  }
  
  public CommandStatement createCommandStatement(String commandString, Map<String, String> env) {
    if (!isUsable()) {
      throw new IllegalStateException("Cache instance is not available.");
    }
    synchronized (modLock) {
      return commandProcessor.createCommandStatement(commandString, env);
    }
  }

  @Override
  public boolean isUsable() {
    return (this.cache != null && !this.cache.isClosed());
  }

//  @Override
//  public void stop() {
//    cache = null;
//    synchronized (modLock) {
//      this.commandProcessor.stop();
//      this.commandProcessor = null;
//    }
//  }  
}
