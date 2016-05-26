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
package com.gemstone.gemfire.management.internal.cli.remote;

import java.util.Collections;
import java.util.Map;

import org.springframework.shell.event.ParseResult;

import com.gemstone.gemfire.management.cli.CommandStatement;
import com.gemstone.gemfire.management.cli.Result;

/**
 * 
 * 
 * @since GemFire 7.0
 */
public class CommandStatementImpl implements CommandStatement {
  
  private CommandProcessor cmdProcessor;
  private String commandString;
  private Map<String, String> env;
  private ParseResult parseResult;

  CommandStatementImpl(String commandString, Map<String, String> env, CommandProcessor cmdProcessor) {
    this.commandString = commandString;
    this.env           = env;
    this.cmdProcessor  = cmdProcessor;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.management.internal.cli.remote.CommandStatement#getCommandString()
   */
  @Override
  public String getCommandString() {
    return commandString;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.management.internal.cli.remote.CommandStatement#getEnv()
   */
  @Override
  public Map<String, String> getEnv() {
    return env;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.management.internal.cli.remote.CommandStatement#process()
   */
  @Override
  public Result process() {
    return cmdProcessor.executeCommand(this);
  }

  /**
   * @return the parseResult
   */
  ParseResult getParseResult() {
    return parseResult;
  }

  /**
   * @param parseResult the parseResult to set
   */
  void setParseResult(ParseResult parseResult) {
    this.parseResult = parseResult;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.management.internal.cli.remote.CommandStatement#validate()
   */
  @Override
  public boolean validate() {
    //TODO-Abhishek: is not null check enough?
    return parseResult != null;
  }

  /* (non-Javadoc)
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return CommandStatement.class.getSimpleName() + "[commandString=" + commandString + ", env=" + env + "]";
  }
}
