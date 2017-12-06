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

import org.apache.geode.management.cli.CommandStatement;
import org.apache.geode.management.cli.Result;

/**
 *
 *
 * @since GemFire 7.0
 *
 * @deprecated since Geode 1.3. simply use commandProcessor to process the command
 */
public class CommandStatementImpl implements CommandStatement {

  private OnlineCommandProcessor cmdProcessor;
  private String commandString;
  private Map<String, String> env;

  CommandStatementImpl(String commandString, Map<String, String> env,
      OnlineCommandProcessor cmdProcessor) {
    this.commandString = commandString;
    this.env = env;
    this.cmdProcessor = cmdProcessor;
  }

  @Override
  public String getCommandString() {
    return commandString;
  }

  @Override
  public Map<String, String> getEnv() {
    return env;
  }

  @Override
  public Result process() {
    return cmdProcessor.executeCommand(commandString, env, null);
  }

  public boolean validate() {
    return true;
  }

  @Override
  public String toString() {
    return CommandStatement.class.getSimpleName() + "[commandString=" + commandString + ", env="
        + env + "]";
  }
}
